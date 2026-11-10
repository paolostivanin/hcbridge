package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hc "hcbridge/src/internal/homeconnect"
	"hcbridge/src/internal/homeconnect/hcfake"
)

// Small units and failure paths that the scenario tests do not reach.

func TestValueFormatting(t *testing.T) {
	cases := []struct {
		got, want string
	}{
		{cleanEnum(3.0), "3"},
		{cleanEnum("BSH.Common.EnumType.PowerState.On"), "On"},
		{cleanEnum("Plain"), "Plain"},
		{powerLevelAPIToLabel(42.0), ""},
		{powerLevelAPIToLabel("Cooking.Hob.EnumType.PowerLevel.65"), "6.5"},
		{fanLabel(nil), ""},
		{fanLabel(true), "true"},
		{zoneStateLabel("Active", "ActionRequired"), "action_required"},
		{zoneStateLabel("Warming", ""), "warming"},
		{formatValue("x", 2.5), "2.5"},
		{formatValue("x", []any{1}), "[1]"},
		{formatValue("x.PowerLevel", 7.0), "7"},
	}
	for i, c := range cases {
		if c.got != c.want {
			t.Errorf("case %d: %q, want %q", i, c.got, c.want)
		}
	}
	if isIgnored("Some.Brand.New.Key") {
		t.Error("an unknown key counted as ignored")
	}
}

func TestBridgeEdgeValues(t *testing.T) {
	b := newTestBridge()
	// A zone power with no value reads off; an attribute the bridge does
	// not publish is accepted silently; a key nobody maps is logged once.
	b.ApplyValues([]hc.Value{{UID: 1, Name: "Cooking.Hob.Status.Zone.100.PowerLevel"}}, false)
	want(t, b, "zone/front_left/power", "off")
	apply(b, num("Cooking.Hob.Status.Zone.100.ElapsedProgramTime", 3))
	apply(b, num("Some.Brand.New.Key", 1))
	if !b.unmapped["Some.Brand.New.Key"] {
		t.Error("new key not noted")
	}
	// JoinZone before any focus: no side to attribute it to.
	apply(b, boolv("Cooking.Hob.Option.JoinZone", true))
	want(t, b, "joined_left", "false")
	if sideOf("Center") != "" {
		t.Error("a zone with no side")
	}
}

func TestDeviceBlockAndTitles(t *testing.T) {
	cfg := testConfig()
	if d := deviceBlock(cfg, deviceMeta{SwVersion: "2.17"}); d["sw_version"] != "2.17" {
		t.Errorf("network module only: %v", d["sw_version"])
	}
	if titleCase("") != "" || titleCase("bOSCH") != "Bosch" {
		t.Error("titleCase")
	}
	if err := PublishDiscovery(cfg, deviceMeta{}, nil, quietLogger()); err != nil {
		t.Errorf("no client: %v", err)
	}
	f := newFakeMQTT()
	f.noAck = map[string]int{}
	topic, id := discoveryTopicOf(cfg, 0)
	f.noAck[topic] = 99
	if err := PublishDiscovery(cfg, deviceMeta{}, f, quietLogger()); err == nil || !strings.Contains(err.Error(), id) {
		t.Fatalf("an unacked entity must be reported: %v", err)
	}
}

func TestConfigEdges(t *testing.T) {
	if _, err := parseConfig([]byte(testConfigBase + "mqtt:\n  discovery_topic: 'ha/#'\n")); err == nil || !strings.Contains(err.Error(), "wildcards") {
		t.Errorf("wildcard discovery topic: %v", err)
	}
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Error("missing config file accepted")
	}
}

// Files the state directory cannot hold: each is an error, never a silent
// fresh start (apart from the link record, which only carries timing).
func TestStateFileFailures(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "file")
	os.WriteFile(notADir, nil, 0o600)
	under := filepath.Join(notADir, "x")

	if _, err := acquireLock(under); err == nil {
		t.Error("lock under a file")
	}
	lockDir := t.TempDir()
	os.Mkdir(filepath.Join(lockDir, "lock"), 0o700)
	if _, err := acquireLock(lockDir); err == nil {
		t.Error("lock that is a directory")
	}

	isDir := t.TempDir()
	for _, name := range []string{"device_id", "device.json", "link.json"} {
		os.Mkdir(filepath.Join(isDir, name), 0o700)
	}
	if _, _, err := loadOrCreateDeviceID(isDir); err == nil {
		t.Error("unreadable device_id accepted")
	}
	if _, err := loadDeviceMeta(isDir); err == nil {
		t.Error("unreadable device.json accepted")
	}
	if _, err := loadLinkRecord(isDir); err == nil {
		t.Error("unreadable link.json accepted")
	}
	if _, _, err := loadOrCreateDeviceID(under); err == nil {
		t.Error("device ID written under a file")
	}
	if err := saveDeviceMeta(under, deviceMeta{}); err == nil {
		t.Error("device.json written under a file")
	}
	if err := saveLinkRecord(under, linkRecord{}); err == nil {
		t.Error("link.json written under a file")
	}

	// writeFileAtomic: the target is a directory (the rename fails), and the
	// directory is read-only (no temp file).
	target := filepath.Join(t.TempDir(), "target")
	os.MkdirAll(filepath.Join(target, "child"), 0o700)
	if err := writeFileAtomic(target, []byte("x"), 0o600); err == nil {
		t.Error("rename over a directory succeeded")
	}
	if left, _ := filepath.Glob(filepath.Join(filepath.Dir(target), ".target.*.tmp")); len(left) != 0 {
		t.Errorf("temp files left behind: %v", left)
	}
	if os.Geteuid() != 0 {
		ro := t.TempDir()
		os.Chmod(ro, 0o500)
		defer os.Chmod(ro, 0o700)
		if err := writeFileAtomic(filepath.Join(ro, "f"), []byte("x"), 0o600); err == nil {
			t.Error("wrote into a read-only directory")
		}
	}
}

// A damaged link.json only costs the episode: the bridge starts fresh. A
// link.json it cannot write is logged, not fatal.
func TestLinkBookFailures(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "link.json"), []byte("{broken"), 0o600)
	lb := openLinkBook(dir, time.Minute, quietLogger())
	if lb.loaded.State != "" || !lb.lastDial.IsZero() {
		t.Fatalf("broken link.json gave %+v", lb.loaded)
	}
	file := filepath.Join(t.TempDir(), "file")
	os.WriteFile(file, nil, 0o600)
	lb.dir = filepath.Join(file, "x")
	lb.onDial(time.Now()) // must not panic or block
	lb.record(hc.LinkStatus{State: hc.LinkRefused, Since: time.Now()}, time.Now())
}

func TestControllerFailures(t *testing.T) {
	// No profile: the client cannot be built and run returns at once.
	cfg := testConfig()
	cfg.StateDirectory = t.TempDir()
	c := newController(cfg, nil, "0a1b2c3d", newTestBridge(), quietLogger())
	done := make(chan struct{})
	go func() { c.run(context.Background(), func() {}, make(chan os.Signal)); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run kept going without a client")
	}

	// Files the controller cannot write are logged, not fatal.
	file := filepath.Join(t.TempDir(), "file")
	os.WriteFile(file, nil, 0o600)
	cfg2 := testConfig()
	cfg2.StateDirectory = filepath.Join(file, "x")
	c2 := newController(cfg2, hcfake.Profile(), "0a1b2c3d", newTestBridge(), quietLogger())
	c2.touchAlive()
	c2.onInfo(&hc.DeviceInfo{Brand: "BOSCH", VIB: "X", SwVersion: "1"})
}

// The main loop keeps the heartbeat and the liveness file fresh.
func TestControllerHeartbeatAndAlive(t *testing.T) {
	srv := hcfake.New()
	defer srv.Close()
	h := startController(t, srv)
	h.wait(t, "link", "online")
	first, _ := h.b.get("status/heartbeat")
	alive := filepath.Join(h.dir, "alive")
	st, err := os.Stat(alive)
	if err != nil {
		t.Fatal(err)
	}
	os.Chtimes(alive, st.ModTime().Add(-time.Hour), st.ModTime().Add(-time.Hour))
	if !waitFor(t, 3*time.Second, func() bool {
		v, _ := h.b.get("status/heartbeat")
		st, err := os.Stat(alive)
		return v != first && err == nil && time.Since(st.ModTime()) < time.Minute
	}) {
		t.Fatal("heartbeat or liveness file not refreshed")
	}
}

func TestLocalWatchFailures(t *testing.T) {
	cfg := localTestConfig(t, "192.0.2.1")
	noHost := *cfg
	noHost.Appliance.Host = ""
	if err := runLocalWatch(context.Background(), &noHost, false, "", &syncBuffer{}); err == nil || !strings.Contains(err.Error(), "required") {
		t.Errorf("no host: %v", err)
	}
	badProf := *cfg
	badProf.Appliance.Profile = filepath.Join(t.TempDir(), "missing")
	if err := runLocalWatch(context.Background(), &badProf, false, "", &syncBuffer{}); err == nil {
		t.Error("missing profile accepted")
	}
	if err := runLocalWatch(context.Background(), cfg, false, t.TempDir(), &syncBuffer{}); err == nil {
		t.Error("a directory accepted as the recording file")
	}
	file := filepath.Join(t.TempDir(), "file")
	os.WriteFile(file, nil, 0o600)
	badDir := *cfg
	badDir.StateDirectory = filepath.Join(file, "state")
	if err := runLocalWatch(context.Background(), &badDir, false, "", &syncBuffer{}); err == nil {
		t.Error("unusable state dir accepted")
	}
	badID := localTestConfig(t, "192.0.2.1")
	os.MkdirAll(badID.StateDir(), 0o700)
	os.WriteFile(filepath.Join(badID.StateDir(), "device_id"), []byte("nope\n"), 0o600)
	if err := runLocalWatch(context.Background(), badID, false, "", &syncBuffer{}); err == nil {
		t.Error("damaged device ID accepted")
	}
}

// What local-watch prints for a refusing hob, raw frames, and access
// changes for UIDs the profile does not name.
func TestLocalWatchPrintsLinkErrorsRawFramesAndDescriptions(t *testing.T) {
	out := &syncBuffer{}
	w := &watchPrinter{out: out}
	w.handle(hc.Event{Kind: hc.EvLink, Link: hc.LinkStatus{State: hc.LinkRefused, Err: errors.New("HTTP 503")}})
	w.handle(hc.Event{Kind: hc.EvRaw, Raw: &hc.RawFrame{Dir: "rx", Plain: []byte(`{"action":"NOTIFY"}`)}})
	avail := true
	w.handle(hc.Event{Kind: hc.EvDescription, Changes: []hc.Description{
		{UID: 77777, Access: "read", Available: &avail, Value: &hc.Value{Raw: json.RawMessage("5")}},
	}})
	got := out.String()
	for _, s := range []string{"[LINK]   refused: HTTP 503", `[RAW]    rx {"action":"NOTIFY"}`, "77777 (not in profile) access=read available=true value=5"} {
		if !strings.Contains(got, s) {
			t.Errorf("output lacks %q:\n%s", s, got)
		}
	}
	if formatWatchValue(hc.Value{UID: 5, Raw: json.RawMessage("1")}) != "     5 (not in profile) = 1" {
		t.Errorf("unnamed value: %q", formatWatchValue(hc.Value{UID: 5, Raw: json.RawMessage("1")}))
	}
}

func TestWorldReadableProfileZip(t *testing.T) {
	zip := filepath.Join(t.TempDir(), "profile.zip")
	os.WriteFile(zip, []byte("PK"), 0o644)
	if got := worldReadableKeyFiles(zip); len(got) != 1 || got[0] != zip {
		t.Errorf("world-readable zip: %v", got)
	}
	os.Chmod(zip, 0o640)
	if got := worldReadableKeyFiles(zip); len(got) != 0 {
		t.Errorf("0640 zip flagged: %v", got)
	}
	if got := worldReadableKeyFiles(filepath.Join(t.TempDir(), "missing")); got != nil {
		t.Errorf("missing profile: %v", got)
	}
}
