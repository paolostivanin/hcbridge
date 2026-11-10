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

	linkMu   sync.Mutex
	mon      linkMonitor
	lastDial time.Time

	streamMu     sync.Mutex
	streamUp     bool
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

	// The session is not up yet; mark the data entities unavailable unless
	// it comes up within the grace period (the normal case).
	c.streamDown()

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
			c.saveLink()
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
	c.linkMu.Unlock()
	c.saveLink() // persist before announcing
	c.bridge.PublishLink(st.State, st.Since, pc)
	if st.State == hc.LinkConnected {
		c.streamUpNow()
	} else {
		c.streamDown()
	}
}

// evaluateLink re-checks the power-cycle threshold: retries in the same
// state produce no events, so the clock is checked on a timer.
func (c *controller) evaluateLink() {
	c.linkMu.Lock()
	was := c.mon.latched
	pc := c.mon.powerCycleNeeded(c.now())
	state, since := c.mon.state, c.mon.since
	c.linkMu.Unlock()
	if pc && !was {
		c.log.Warn("the hob has refused local connections for %s; power cycle it (breaker off ~30 s)", c.cfg.Link.PowerCycleAfter)
		c.saveLink() // persist before announcing
		c.bridge.PublishLink(state, since, pc)
	}
}

func (c *controller) onDial(at time.Time) {
	c.linkMu.Lock()
	c.lastDial = at
	c.linkMu.Unlock()
	c.saveLink()
}

func (c *controller) saveLink() {
	c.linkMu.Lock()
	rec := c.mon.record(c.lastDial)
	c.linkMu.Unlock()
	if err := saveLinkRecord(c.cfg.StateDir(), rec); err != nil {
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
	c.streamUp = true
	if c.offlineTimer != nil {
		c.offlineTimer.Stop()
		c.offlineTimer = nil
	}
	c.streamMu.Unlock()
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
	c.offlineTimer = time.AfterFunc(c.offlineGrace, func() {
		c.streamMu.Lock()
		up := c.streamUp
		c.offlineTimer = nil
		c.streamMu.Unlock()
		if !up {
			c.log.Warn("local session down for %s; marking cooktop data unavailable", c.offlineGrace)
			c.bridge.SetLink(false)
		}
	})
}
