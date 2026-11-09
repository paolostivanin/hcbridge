package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// pollDebounce: edge-triggered polls never run closer than this, even if
	// many LocalControlActive transitions arrive in a burst.
	pollDebounce = 5 * time.Second

	// streamResyncMinGap: after an SSE connect, resync unless one succeeded
	// this recently. That skips the first connect right after a successful
	// startup resync, retries a startup resync that failed (e.g. restarted
	// inside a Bosch block), and caps a flapping stream at one resync per
	// gap instead of one per reconnect.
	streamResyncMinGap = 5 * time.Minute

	// cloudOfflineGrace: how long the event stream may be down before the
	// data entities are marked unavailable. Bosch closes the stream about
	// once a day and the bridge reconnects within seconds; the grace keeps
	// that routine reconnect from flapping every entity.
	cloudOfflineGrace = 90 * time.Second

	// resyncEvery: periodic snapshot as a drift safety net. SSE delivers
	// every state change and every SSE connect resyncs, so this only
	// catches a silently missed event. 2 calls with the hob off.
	resyncEvery = 6 * time.Hour

	statusEvery    = 30 * time.Second // rate-limit state, call count, tier
	heartbeatEvery = 5 * time.Minute  // MQTT heartbeat for HA
	aliveEvery     = time.Minute      // liveness file for the OpenRC healthcheck
	quotaSaveEvery = 5 * time.Minute  // persisted call log
)

// controller drives the bridge: the SSE event handler, the resyncs and the
// polling schedule. --mode=run and --mode=watch share it; watch has no MQTT
// client (the bridge prints [PUB] lines instead), prints [SSE] and [POLL]
// trace lines, and persists nothing.
type controller struct {
	cfg    *Config
	api    *APIClient
	auth   *Authenticator
	quota  *Quota
	bridge *Bridge
	log    *Logger
	watch  bool
	out    io.Writer

	fast fastPoller

	pollMu     sync.Mutex
	lastPollAt time.Time
	lastEdgeAt time.Time

	resyncMu     sync.Mutex
	lastResyncOK time.Time

	streamMu     sync.Mutex
	streamUp     bool
	offlineTimer *time.Timer
	offlineGrace time.Duration // cloudOfflineGrace; a field so tests can shrink it

	statsMu sync.Mutex
	stats   map[string]*pollStat
}

type pollStat struct{ runs, changed int }

func newController(cfg *Config, api *APIClient, auth *Authenticator, quota *Quota, bridge *Bridge, log *Logger, watch bool) *controller {
	return &controller{
		cfg: cfg, api: api, auth: auth, quota: quota, bridge: bridge, log: log,
		watch: watch, out: os.Stdout, stats: map[string]*pollStat{},
		offlineGrace: cloudOfflineGrace,
	}
}

func (c *controller) quotaPath() string { return filepath.Join(c.cfg.StateDir(), "calls.json") }
func (c *controller) alivePath() string { return filepath.Join(c.cfg.StateDir(), "alive") }

func (c *controller) trace(format string, args ...interface{}) {
	if c.watch {
		fmt.Fprintf(c.out, "[%s] [POLL]   %s\n", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
		return
	}
	c.log.Debug(format, args...)
}

// run blocks until a signal arrives on sigCh, then cancels and returns.
func (c *controller) run(ctx context.Context, cancel context.CancelFunc, sigCh <-chan os.Signal) {
	// The stream is not up yet; mark the data entities unavailable unless it
	// comes up within the grace period (the normal case).
	c.streamDown()
	c.resync(ctx, "startup")

	sse := NewSSEClient(c.cfg, c.auth, c.quota, c.log)
	go sse.Run(ctx, c.cfg.Appliance.HaID, func(ev SSEEvent) { c.handleEvent(ctx, ev) })

	pollTicker := time.NewTicker(pollTick)
	defer pollTicker.Stop()
	resyncTicker := time.NewTicker(resyncEvery)
	defer resyncTicker.Stop()
	statusTicker := time.NewTicker(statusEvery)
	defer statusTicker.Stop()
	heartbeatTicker := time.NewTicker(heartbeatEvery)
	defer heartbeatTicker.Stop()
	aliveTicker := time.NewTicker(aliveEvery)
	defer aliveTicker.Stop()
	saveTicker := time.NewTicker(quotaSaveEvery)
	defer saveTicker.Stop()

	lastTier := tierNormal
	c.publishStatus(&lastTier)
	if !c.watch {
		c.bridge.PublishHeartbeat(time.Now())
		c.touchAlive()
	}

	for {
		select {
		case sig := <-sigCh:
			c.log.Info("got signal %s, shutting down", sig)
			cancel()
			if !c.watch {
				if err := c.quota.Save(c.quotaPath()); err != nil {
					c.log.Warn("save API call log: %v", err)
				}
			}
			time.Sleep(500 * time.Millisecond)
			return
		case now := <-pollTicker.C:
			if !c.bridge.CanPoll() || c.fast.Active() {
				break
			}
			c.pollMu.Lock()
			lastPoll, lastEdge := c.lastPollAt, c.lastEdgeAt
			c.pollMu.Unlock()
			if periodicPollDue(now, lastPoll, lastEdge, currentTier(c.quota)) {
				c.pollNow(ctx, "periodic")
			}
		case <-resyncTicker.C:
			if currentTier(c.quota) == tierStop {
				c.log.Info("periodic resync skipped: API budget (%d calls in 24 h)", c.quota.Count24h())
				break
			}
			c.resync(ctx, "periodic")
		case <-statusTicker.C:
			c.publishStatus(&lastTier)
		case now := <-heartbeatTicker.C:
			if !c.watch {
				c.bridge.PublishHeartbeat(now)
			}
		case <-aliveTicker.C:
			if !c.watch {
				c.touchAlive()
			}
		case <-saveTicker.C:
			if !c.watch {
				if err := c.quota.Save(c.quotaPath()); err != nil {
					c.log.Warn("save API call log: %v", err)
				}
			}
		}
	}
}

func (c *controller) publishStatus(lastTier *budgetTier) {
	c.bridge.PublishRateLimit(c.quota.Blocked())
	n := c.quota.Count24h()
	c.bridge.PublishAPICalls(n)
	if t := currentTier(c.quota); t != *lastTier {
		c.log.Warn("API budget tier %s -> %s (%d calls in 24 h)", *lastTier, t, n)
		*lastTier = t
	}
}

// touchAlive refreshes the liveness file the OpenRC healthcheck watches. It
// is written from the main loop, so a wedged loop stops refreshing it.
func (c *controller) touchAlive() {
	if err := os.WriteFile(c.alivePath(), []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o600); err != nil {
		c.log.Warn("write liveness file: %v", err)
	}
}

// resync re-reads /status + /settings (2 calls; 1 if the appliance is
// offline) and, while the hob is on, the focused-zone options (1 call).
func (c *controller) resync(ctx context.Context, reason string) bool {
	c.log.Info("snapshot resync (%s, %d API calls in 24 h)", reason, c.quota.Count24h())
	rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
	defer rcancel()
	startedAt := time.Now()
	snap, online, err := c.api.FetchAll(rctx, c.cfg.Appliance.HaID)
	if err != nil {
		c.log.Warn("snapshot resync failed: %v", err)
		return false
	}
	if !online {
		c.log.Warn("snapshot: appliance is not connected to Home Connect")
	}
	if c.watch && reason == "startup" {
		c.printSnapshot(snap)
	}
	c.bridge.ApplySnapshot(snap, online, startedAt)
	if c.bridge.CanPoll() {
		c.doPoll(rctx, "resync")
	}
	c.resyncMu.Lock()
	c.lastResyncOK = time.Now()
	c.resyncMu.Unlock()
	return true
}

func (c *controller) resyncOnStreamUp(ctx context.Context) {
	c.resyncMu.Lock()
	since := time.Since(c.lastResyncOK)
	c.resyncMu.Unlock()
	if since < streamResyncMinGap {
		c.log.Debug("stream up: resync skipped (last one %s ago)", since.Round(time.Second))
		return
	}
	c.resync(ctx, "sse connect")
}

func (c *controller) streamUpNow() {
	c.streamMu.Lock()
	c.streamUp = true
	if c.offlineTimer != nil {
		c.offlineTimer.Stop()
		c.offlineTimer = nil
	}
	c.streamMu.Unlock()
	c.bridge.SetCloud(true)
}

// streamDown starts the grace period after which the data entities are
// marked unavailable, unless the stream is back by then.
func (c *controller) streamDown() {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	c.streamUp = false
	if c.offlineTimer != nil {
		return
	}
	c.offlineTimer = time.AfterFunc(c.offlineGrace, func() {
		c.streamMu.Lock()
		up := c.streamUp
		c.offlineTimer = nil
		c.streamMu.Unlock()
		if !up {
			c.log.Warn("event stream down for %s; marking cooktop data unavailable", c.offlineGrace)
			c.bridge.SetCloud(false)
		}
	})
}

// doPoll runs one options poll and records it in the session stats.
func (c *controller) doPoll(ctx context.Context, reason string) {
	c.pollMu.Lock()
	c.lastPollAt = time.Now()
	c.pollMu.Unlock()
	c.trace("/programs/*/options (%s)", reason)
	pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
	changed := c.bridge.PollActiveOptions(pctx)
	pcancel()
	c.statsMu.Lock()
	s := c.stats[reason]
	if s == nil {
		s = &pollStat{}
		c.stats[reason] = s
	}
	s.runs++
	if changed {
		s.changed++
	}
	c.statsMu.Unlock()
}

// pollNow runs an edge-triggered or periodic poll, debounced and subject to
// the budget.
func (c *controller) pollNow(ctx context.Context, reason string) {
	if !c.bridge.CanPoll() {
		return
	}
	if currentTier(c.quota) == tierStop {
		c.trace("poll (%s) skipped: API budget", reason)
		return
	}
	c.pollMu.Lock()
	if time.Since(c.lastPollAt) < pollDebounce {
		c.pollMu.Unlock()
		return
	}
	c.lastPollAt = time.Now() // claim the slot before releasing the lock
	c.pollMu.Unlock()
	c.doPoll(ctx, reason)
}

// fastTick is one fast-poll step: every 2 s while the panel is touched, so
// each zone the user taps in turn is caught (the API reports only the
// focused one). Only in the normal tier.
func (c *controller) fastTick(ctx context.Context) {
	if currentTier(c.quota) != tierNormal || !c.bridge.CanPoll() {
		return
	}
	c.doPoll(ctx, "fast")
}

// isEdgeKey reports whether an SSE key signals that a zone may have changed.
// The program countdown keys are pushed several times a minute while a timer
// runs and say nothing about zones, so they don't keep the poll fast.
func isEdgeKey(key string) bool {
	switch key {
	case "BSH.Common.Option.RemainingProgramTime",
		"BSH.Common.Option.ElapsedProgramTime",
		"BSH.Common.Option.ProgramProgress",
		"BSH.Common.Setting.AlarmClock":
		return false
	}
	return true
}

func (c *controller) handleEvent(ctx context.Context, ev SSEEvent) {
	switch ev.Event {
	case "STREAM_UP":
		if c.watch {
			fmt.Fprintf(c.out, "[%s] STREAM_UP\n", time.Now().Format("15:04:05.000"))
		}
		c.resyncOnStreamUp(ctx)
		c.streamUpNow()
		return
	case "STREAM_DOWN":
		if c.watch {
			fmt.Fprintf(c.out, "[%s] STREAM_DOWN\n", time.Now().Format("15:04:05.000"))
		}
		c.streamDown()
		return
	case "CONNECTED":
		// The appliance reconnected to Home Connect: whatever changed while
		// it was away was never streamed, so take a fresh snapshot.
		c.bridge.ApplyEvent(ev)
		c.resync(ctx, "appliance connected")
		return
	case "DISCONNECTED", "PAIRED", "DEPAIRED":
		if c.watch {
			fmt.Fprintf(c.out, "[%s] %s\n", time.Now().Format("15:04:05.000"), ev.Event)
		}
		c.bridge.ApplyEvent(ev)
		if ev.Event == "DISCONNECTED" {
			c.fast.Stop()
		}
		return
	}

	if c.watch {
		c.printItems(ev)
	}
	c.bridge.ApplyEvent(ev)

	now := time.Now()
	for _, it := range ev.Items {
		if isEdgeKey(it.Key) {
			c.pollMu.Lock()
			c.lastEdgeAt = now
			c.pollMu.Unlock()
		}
		switch it.Key {
		case "BSH.Common.Status.LocalControlActive":
			if v, ok := it.Value.(bool); ok {
				if v {
					go c.pollNow(ctx, "panel touched")
					// Outside the normal tier the fast poller would do
					// nothing yet still suppress the periodic poll.
					if currentTier(c.quota) == tierNormal {
						c.fast.Start(ctx, fastPollEvery, fastPollMax, c.fastTick)
					}
				} else {
					c.fast.Stop()
				}
			}
		case "BSH.Common.Root.ActiveProgram":
			if it.Value != nil {
				go c.pollNow(ctx, "program started")
			}
		case "BSH.Common.Setting.PowerState":
			// Hob off → no point fast-polling an inactive appliance, even if
			// LocalControlActive hasn't transitioned to false yet.
			if s, ok := it.Value.(string); ok && strings.HasSuffix(s, ".Off") {
				c.fast.Stop()
				c.logSessionStats()
			}
		case "BSH.Common.Event.ProgramFinished", "BSH.Common.Event.ProgramAborted":
			// A program ending means Bosch has turned a zone off physically,
			// but SSE doesn't carry that PowerLevel change. Filter on
			// .Present so we fire on the event, not the 10 s-later .Off clear.
			if s, ok := it.Value.(string); ok && strings.HasSuffix(s, ".Present") {
				go c.pollNow(ctx, "program ended")
			}
		}
	}
}

// logSessionStats logs, once per cooking session, how many polls each
// trigger ran and how many of them changed a value - the data for tuning
// the poll schedule.
func (c *controller) logSessionStats() {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	if len(c.stats) == 0 {
		return
	}
	keys := make([]string, 0, len(c.stats))
	for k := range c.stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s %d (%d changed)", k, c.stats[k].runs, c.stats[k].changed))
	}
	c.log.Info("session polls: %s; %d API calls in 24 h", strings.Join(parts, ", "), c.quota.Count24h())
	c.stats = map[string]*pollStat{}
}

func (c *controller) printSnapshot(snap map[string]interface{}) {
	fmt.Fprintln(c.out, "=== snapshot ===")
	keys := make([]string, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(c.out, "%s%-50s -> %-32s = %v\n", mappedMarker(k), k, suffixFor(k), snap[k])
	}
	fmt.Fprintln(c.out, "\n--- applying snapshot through bridge ([PUB] = what run mode would publish) ---")
}

func (c *controller) printItems(ev SSEEvent) {
	ts := time.Now().Format("15:04:05.000")
	for _, it := range ev.Items {
		vJSON, _ := json.Marshal(it.Value)
		fmt.Fprintf(c.out, "[%s] [SSE]    %s%-50s -> %-32s = %s\n",
			ts, mappedMarker(it.Key), it.Key, suffixFor(it.Key), string(vJSON))
	}
}

func suffixFor(key string) string {
	if s, ok := keyToSuffix[key]; ok {
		return s
	}
	if s, ok := eventKeyToSuffix[key]; ok {
		return s
	}
	if key == "BSH.Common.Option.RemainingProgramTime" {
		return "status/program_timer_deadline"
	}
	return "(unmapped)"
}

func mappedMarker(key string) string {
	if suffixFor(key) == "(unmapped)" {
		return "* "
	}
	return "  "
}
