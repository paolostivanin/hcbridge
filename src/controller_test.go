package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	hc "hcbridge/src/internal/homeconnect"
	"hcbridge/src/internal/homeconnect/hcfake"
)

func TestLinkGraceSurvivesQuickReconnect(t *testing.T) {
	cfg := testConfig()
	cfg.StateDirectory = t.TempDir()
	c := newController(cfg, hcfake.Profile(), "0a1b2c3d", newTestBridge(), quietLogger())
	c.offlineGrace = 50 * time.Millisecond
	c.streamUpNow()
	c.streamDown()
	time.Sleep(10 * time.Millisecond)
	c.streamUpNow() // routine reconnect inside the grace
	time.Sleep(80 * time.Millisecond)
	if got, _ := c.bridge.get("link"); got != "online" {
		t.Fatalf("link = %q after a reconnect inside the grace, want online", got)
	}
	c.streamDown()
	if !waitFor(t, time.Second, func() bool { v, _ := c.bridge.get("link"); return v == "offline" }) {
		t.Fatal("link never went offline after the grace")
	}
}

type ctlHarness struct {
	c     *controller
	b     *Bridge
	srv   *hcfake.Server
	dir   string
	sigCh chan os.Signal
	done  chan struct{}
}

func startController(t *testing.T, srv *hcfake.Server) *ctlHarness {
	t.Helper()
	dir := t.TempDir()
	cfg, err := parseConfig([]byte("appliance:\n  host: " + srv.Host() + "\n  profile: /unused\nstate_dir: " + dir + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	b := newTestBridge()
	c := newController(cfg, hcfake.Profile(), "0a1b2c3d", b, quietLogger())
	c.offlineGrace = 50 * time.Millisecond
	c.evalEvery = 20 * time.Millisecond
	c.mon.threshold = 200 * time.Millisecond
	c.optsHook = func(o *hc.Options) {
		o.BackoffMin, o.BackoffMax, o.MinDialGap = 20*time.Millisecond, 50*time.Millisecond, 10*time.Millisecond
	}
	h := &ctlHarness{c: c, b: b, srv: srv, dir: dir, sigCh: make(chan os.Signal, 1), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { c.run(ctx, cancel, h.sigCh); close(h.done) }()
	t.Cleanup(h.stop)
	return h
}

func (h *ctlHarness) stop() {
	select {
	case <-h.done:
		return
	default:
	}
	h.sigCh <- syscall.SIGTERM
	<-h.done
}

func (h *ctlHarness) wait(t *testing.T, suffix, value string) {
	t.Helper()
	if !waitFor(t, 5*time.Second, func() bool { v, _ := h.b.get(suffix); return v == value }) {
		got, _ := h.b.get(suffix)
		t.Fatalf("%s = %q, want %q", suffix, got, value)
	}
}

func TestControllerEndToEndWithFakeHob(t *testing.T) {
	srv := hcfake.New()
	defer srv.Close()
	srv.SetValues(map[int]any{4409: 18, 8204: 13, 8202: 2, 531: 2})
	h := startController(t, srv)

	h.wait(t, "link", "online")
	h.wait(t, "status/link_state", "connected")
	h.wait(t, "status/power_cycle_needed", "false")
	h.wait(t, "status/fan_level", "4.5")
	h.wait(t, "zone/front_left/power", "6.5")
	h.wait(t, "zone/front_left/state", "active")
	h.wait(t, "status/power_state", "On")

	// Values that only come inside access changes.
	srv.NotifyDescription(4409, "READ", 101)
	h.wait(t, "status/fan_level", "afterrun")

	// The device info lands in the HA device block and in device.json.
	if !waitFor(t, 2*time.Second, func() bool { return h.b.Meta().SwVersion == hcfake.SwVersion }) {
		t.Fatalf("meta %+v", h.b.Meta())
	}
	if d := deviceBlock(h.c.cfg, h.b.Meta()); d["sw_version"] != hcfake.HaVersion+"-"+hcfake.SwVersion || d["model"] != "TESTHOB01" {
		t.Fatalf("device block %v", d)
	}
	meta, err := loadDeviceMeta(h.dir)
	if err != nil || meta.SwVersion != hcfake.SwVersion {
		t.Fatalf("device.json %+v %v", meta, err)
	}

	h.stop()
	if codes := srv.CloseCodes(); len(codes) != 1 || codes[0] != 1000 {
		t.Fatalf("close codes %v", codes)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "alive")); err != nil {
		t.Fatalf("alive file: %v", err)
	}
	rec, err := loadLinkRecord(h.dir)
	if err != nil || rec.State != "connected" || rec.LastDial.IsZero() {
		t.Fatalf("link.json %+v %v", rec, err)
	}
}

func TestControllerRefusalRaisesPowerCycleFlag(t *testing.T) {
	srv := hcfake.New()
	defer srv.Close()
	srv.Refuse(503, http.Header{"Server": {"WebSocket++/0.8.2"}}, "")
	h := startController(t, srv)

	h.wait(t, "status/link_state", "refused")
	h.wait(t, "status/power_cycle_needed", "true") // after the (shrunk) threshold
	h.wait(t, "link", "offline")                   // data entities unavailable
	if since, _ := h.b.get("status/link_since"); since == "" {
		t.Fatal("link_since empty")
	}
	rec, _ := loadLinkRecord(h.dir)
	if !rec.Latched || rec.RefusedSince.IsZero() {
		t.Fatalf("link.json %+v", rec)
	}

	srv.Refuse(0, nil, "") // the breaker was flipped
	h.wait(t, "status/link_state", "connected")
	h.wait(t, "status/power_cycle_needed", "false")
	h.wait(t, "link", "online")
}

// A restart mid-episode keeps the clock and waits before the first dial.
func TestControllerRestoresEpisode(t *testing.T) {
	srv := hcfake.New()
	defer srv.Close()
	srv.Refuse(503, nil, "")
	dir := t.TempDir()
	lastDial := time.Now()
	rec := linkRecord{State: "refused", Since: time.Now().Add(-time.Hour), RefusedSince: time.Now().Add(-time.Hour), Latched: true, LastDial: lastDial}
	b, _ := json.Marshal(rec)
	os.WriteFile(filepath.Join(dir, "link.json"), b, 0o600)

	cfg, _ := parseConfig([]byte(fmt.Sprintf("appliance:\n  host: %s\n  profile: /unused\nstate_dir: %s\n", srv.Host(), dir)))
	br := newTestBridge()
	c := newController(cfg, hcfake.Profile(), "0a1b2c3d", br, quietLogger())
	sigCh := make(chan os.Signal, 1)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { c.run(ctx, cancel, sigCh); close(done) }()
	defer func() { sigCh <- syscall.SIGTERM; <-done }()

	if !waitFor(t, time.Second, func() bool { v, _ := br.get("status/power_cycle_needed"); return v == "true" }) {
		t.Fatal("restored episode did not republish the flag")
	}
	time.Sleep(200 * time.Millisecond)
	if n := len(srv.DialTimes()); n != 0 {
		t.Fatalf("dialed %d times within the restart gap", n)
	}
	if got, _ := br.get("status/link_state"); !strings.Contains(got, "refused") {
		t.Fatalf("link_state %q", got)
	}
}
