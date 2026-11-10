package main

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	hc "hcbridge/src/internal/homeconnect"
)

const (
	// linkOfflineGrace: how long the session may be down before the data
	// entities are marked unavailable. A routine reconnect takes seconds;
	// the grace keeps it from flapping every entity.
	linkOfflineGrace = 30 * time.Second

	heartbeatEvery = 5 * time.Minute  // MQTT heartbeat for HA
	aliveEvery     = time.Minute      // liveness file for the OpenRC healthcheck
	linkEvalEvery  = 30 * time.Second // power_cycle_needed threshold check

	shutdownWait = 5 * time.Second // for the client's Close handshake
)

// controller connects the local client to the bridge: values go to MQTT,
// link states go through the power-cycle monitor, and the main loop keeps
// the heartbeat and the liveness file fresh.
type controller struct {
	cfg      *Config
	prof     *hc.Profile
	deviceID string
	bridge   *Bridge
	log      *Logger

	// linkMu is held across every link decision, its link.json write and its
	// publish, so the three happen in the same order for every goroutine
	// (the dispatcher, the main loop's threshold check, the dial loop). The
	// bridge's publishes only queue, so holding it there never waits on MQTT.
	linkMu   sync.Mutex
	mon      linkMonitor
	lastDial time.Time

	// streamMu likewise covers each availability decision and its publish.
	streamMu     sync.Mutex
	streamUp     bool
	streamGen    uint64 // bumped on every up and every grace started: identifies the live grace timer
	offlineTimer *time.Timer
	offlineGrace time.Duration // linkOfflineGrace; a field so tests can shrink it
	evalEvery    time.Duration

	optsHook func(*hc.Options) // tests shorten the client's timings
	now      func() time.Time
}

func newController(cfg *Config, prof *hc.Profile, deviceID string, bridge *Bridge, log *Logger) *controller {
	return &controller{
		cfg: cfg, prof: prof, deviceID: deviceID, bridge: bridge, log: log,
		mon:          linkMonitor{threshold: cfg.Link.PowerCycleAfter},
		offlineGrace: linkOfflineGrace,
		evalEvery:    linkEvalEvery,
		now:          time.Now,
	}
}

func (c *controller) alivePath() string { return filepath.Join(c.cfg.StateDir(), "alive") }

// run blocks until a signal arrives on sigCh, then closes the session
// cleanly and returns.
func (c *controller) run(ctx context.Context, cancel context.CancelFunc, sigCh <-chan os.Signal) {
	// Carry a link episode over from the previous run: a restart in the
	// middle of a 503 episode keeps its power-cycle clock, and the first
	// dial respects the previous one.
	rec, err := loadLinkRecord(c.cfg.StateDir())
	if err != nil {
		c.log.Warn("read link.json: %v (starting fresh)", err)
	}
	c.linkMu.Lock()
	c.mon.restore(rec)
	c.lastDial = rec.LastDial
	pc := c.mon.powerCycleNeeded(c.now())
	c.linkMu.Unlock()
	if rec.State != "" {
		c.bridge.PublishLink(hc.LinkState(rec.State), rec.Since, pc)
	}
	notBefore := time.Time{}
	if !rec.LastDial.IsZero() {
		gap := 5 * time.Second
		if rec.State != "" && hc.LinkState(rec.State) != hc.LinkConnected {
			gap = 30 * time.Second
		}
		notBefore = rec.LastDial.Add(gap)
	}

	// The session is not up yet, and nothing says the broker's retained
	// values are current: the data entities are unavailable until it is.
	c.streamMu.Lock()
	c.streamUp = false
	c.bridge.SetLink(false)
	c.streamMu.Unlock()

	opts := hc.Options{
		Host:      c.cfg.Appliance.Host,
		DeviceID:  c.deviceID,
		Log:       c.log,
		NotBefore: notBefore,
		OnDial:    c.onDial,
		Resume:    hc.LinkStatus{State: hc.LinkState(rec.State), Since: rec.Since},
	}
	if c.optsHook != nil {
		c.optsHook(&opts)
	}
	client, err := hc.NewClient(opts, c.prof, c.handle)
	if err != nil {
		c.log.Error("local client: %v", err)
		return
	}
	done := make(chan struct{})
	go func() {
		client.Run(ctx)
		close(done)
	}()

	heartbeatTicker := time.NewTicker(heartbeatEvery)
	defer heartbeatTicker.Stop()
	aliveTicker := time.NewTicker(aliveEvery)
	defer aliveTicker.Stop()
	evalTicker := time.NewTicker(c.evalEvery)
	defer evalTicker.Stop()

	c.bridge.PublishHeartbeat(c.now())
	c.touchAlive()

	for {
		select {
		case sig := <-sigCh:
			c.log.Info("got signal %s, shutting down", sig)
			cancel()
			select {
			case <-done:
			case <-time.After(shutdownWait):
				c.log.Warn("local session did not close within %s", shutdownWait)
			}
			c.linkMu.Lock()
			c.saveLinkLocked()
			c.linkMu.Unlock()
			return
		case now := <-heartbeatTicker.C:
			c.bridge.PublishHeartbeat(now)
		case <-aliveTicker.C:
			// Refreshed whatever the link state: a hob refusing connections is
			// not a reason for the supervisor to restart the bridge.
			c.touchAlive()
		case <-evalTicker.C:
			c.evaluateLink()
		}
	}
}

// handle receives the client's events, one at a time, in wire order.
func (c *controller) handle(ev hc.Event) {
	switch ev.Kind {
	case hc.EvValues:
		c.bridge.ApplyValues(ev.Values, ev.Snapshot)
	case hc.EvDescription:
		c.bridge.ApplyDescriptions(ev.Changes)
	case hc.EvLink:
		c.onLink(ev.Link)
	case hc.EvInfo:
		c.onInfo(ev.Info)
	}
}

func (c *controller) onLink(st hc.LinkStatus) {
	c.linkMu.Lock()
	c.mon.update(st)
	pc := c.mon.powerCycleNeeded(c.now())
	c.saveLinkLocked() // persist before announcing
	c.bridge.PublishLink(st.State, st.Since, pc)
	c.linkMu.Unlock()
	if st.State == hc.LinkConnected {
		c.streamUpNow()
	} else {
		c.streamDown()
	}
}

// evaluateLink re-checks the power-cycle threshold: retries in the same
// state produce no events, so the clock is checked on a timer.
//
// The whole check runs under linkMu: a session connecting meanwhile cannot
// publish connected/false only to be overwritten by a refused/true decided
// a moment before.
func (c *controller) evaluateLink() {
	c.linkMu.Lock()
	defer c.linkMu.Unlock()
	was := c.mon.latched
	if pc := c.mon.powerCycleNeeded(c.now()); pc && !was {
		c.log.Warn("the hob has refused local connections for %s; power cycle it (breaker off ~30 s)", c.cfg.Link.PowerCycleAfter)
		c.saveLinkLocked() // persist before announcing
		c.bridge.PublishLink(c.mon.state, c.mon.since, pc)
	}
}

func (c *controller) onDial(at time.Time) {
	c.linkMu.Lock()
	defer c.linkMu.Unlock()
	c.lastDial = at
	c.saveLinkLocked()
}

// saveLinkLocked writes link.json; call with linkMu held, which also keeps
// the writes in order (the last one written is the latest state).
func (c *controller) saveLinkLocked() {
	if err := saveLinkRecord(c.cfg.StateDir(), c.mon.record(c.lastDial)); err != nil {
		c.log.Warn("write link.json: %v", err)
	}
}

func (c *controller) onInfo(info *hc.DeviceInfo) {
	meta := c.bridge.Meta()
	meta.Brand, meta.VIB, meta.Serial = c.prof.Brand, c.prof.VIB, c.prof.Serial
	if info.Brand != "" {
		meta.Brand = info.Brand
	}
	if info.VIB != "" {
		meta.VIB = info.VIB
	}
	meta.HaVersion, meta.SwVersion, meta.HwVersion = info.HaVersion, info.SwVersion, info.HwVersion
	meta.SeenAt = c.now().UTC()
	rssi := "?"
	if info.RSSI != nil {
		rssi = strconv.Itoa(*info.RSSI) + " dBm"
	}
	c.log.Info("connected to %s %s: appliance %s, network module %s (hw %s), Wi-Fi %s",
		meta.Brand, info.ENumber, info.HaVersion, info.SwVersion, info.HwVersion, rssi)
	if c.bridge.SetDeviceInfo(meta) {
		c.log.Info("firmware versions changed; republishing discovery")
	}
	if err := saveDeviceMeta(c.cfg.StateDir(), meta); err != nil {
		c.log.Warn("write device.json: %v", err)
	}
}

// touchAlive refreshes the liveness file the OpenRC healthcheck watches. It
// is written from the main loop, so a wedged loop stops refreshing it.
func (c *controller) touchAlive() {
	if err := os.WriteFile(c.alivePath(), []byte(c.now().UTC().Format(time.RFC3339)+"\n"), 0o600); err != nil {
		c.log.Warn("write liveness file: %v", err)
	}
}

func (c *controller) streamUpNow() {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	c.streamUp = true
	c.streamGen++ // a grace callback already running finds itself superseded
	if c.offlineTimer != nil {
		c.offlineTimer.Stop()
		c.offlineTimer = nil
	}
	c.bridge.SetLink(true)
}

// streamDown starts the grace period after which the data entities are
// marked unavailable, unless the session is back by then.
func (c *controller) streamDown() {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	c.streamUp = false
	if c.offlineTimer != nil {
		return
	}
	c.streamGen++
	gen := c.streamGen
	c.offlineTimer = time.AfterFunc(c.offlineGrace, func() { c.offlineExpired(gen) })
}

// offlineExpired ends the grace period gen. Decision and publish are one
// critical section, and a grace that the session coming back (or a newer
// grace) superseded does nothing: the expired timer of an old outage can
// neither mark a recovered session unavailable nor drop a newer timer.
func (c *controller) offlineExpired(gen uint64) {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	if gen != c.streamGen {
		return
	}
	c.offlineTimer = nil
	if c.streamUp {
		return
	}
	c.log.Warn("local session down for %s; marking cooktop data unavailable", c.offlineGrace)
	c.bridge.SetLink(false)
}
