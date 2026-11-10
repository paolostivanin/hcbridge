package homeconnect_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	hc "hcbridge/src/internal/homeconnect"
	"hcbridge/src/internal/homeconnect/hcfake"
)

const testDeviceID = "0a1b2c3d"

type testLogger struct {
	t     *testing.T
	mu    sync.Mutex
	lines []string
}

func (l *testLogger) add(lvl, f string, a ...any) {
	l.mu.Lock()
	l.lines = append(l.lines, lvl+" "+fmt.Sprintf(f, a...))
	l.mu.Unlock()
}
func (l *testLogger) Debug(f string, a ...any) { l.add("DEBUG", f, a...) }
func (l *testLogger) Info(f string, a ...any)  { l.add("INFO", f, a...) }
func (l *testLogger) Warn(f string, a ...any)  { l.add("WARN", f, a...) }
func (l *testLogger) Error(f string, a ...any) { l.add("ERROR", f, a...) }

func (l *testLogger) has(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.lines {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

type recorder struct {
	mu  sync.Mutex
	evs []hc.Event
}

func (r *recorder) handle(ev hc.Event) {
	r.mu.Lock()
	r.evs = append(r.evs, ev)
	r.mu.Unlock()
}

func (r *recorder) events() []hc.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]hc.Event(nil), r.evs...)
}

func (r *recorder) links() []hc.LinkStatus {
	var out []hc.LinkStatus
	for _, ev := range r.events() {
		if ev.Kind == hc.EvLink {
			out = append(out, ev.Link)
		}
	}
	return out
}

func (r *recorder) linkStates() []hc.LinkState {
	var out []hc.LinkState
	for _, l := range r.links() {
		out = append(out, l.State)
	}
	return out
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (r *recorder) waitLink(t *testing.T, state hc.LinkState, n int) {
	t.Helper()
	waitFor(t, 5*time.Second, fmt.Sprintf("%d× link %s", n, state), func() bool {
		c := 0
		for _, s := range r.linkStates() {
			if s == state {
				c++
			}
		}
		return c >= n
	})
}

type harness struct {
	srv  *hcfake.Server
	rec  *recorder
	log  *testLogger
	stop func()
}

// start runs a client against srv with fast timings.
func start(t *testing.T, srv *hcfake.Server, mutate func(*hc.Options)) *harness {
	t.Helper()
	log := &testLogger{t: t}
	o := hc.Options{
		Host: srv.Host(), DeviceID: testDeviceID, Log: log,
		PingEvery: time.Hour, PongTimeout: time.Hour,
		BackoffMin: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond,
		MinDialGap: 10 * time.Millisecond, StableAfter: time.Hour,
		RequestTimeout: 2 * time.Second, InitTimeout: 2 * time.Second,
	}
	hc.SetJitter(&o, func() float64 { return 0.5 })
	if mutate != nil {
		mutate(&o)
	}
	rec := &recorder{}
	c, err := hc.NewClient(o, hcfake.Profile(), rec.handle)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("Run did not return after cancel")
			}
		})
	}
	t.Cleanup(stop)
	return &harness{srv: srv, rec: rec, log: log, stop: stop}
}

func newServer(t *testing.T) *hcfake.Server {
	srv := hcfake.New()
	t.Cleanup(srv.Close)
	return srv
}

func TestHandshakeSequence(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)

	got := srv.Received()
	type step struct {
		action, resource string
		msgID            int64
		version          int
	}
	want := []step{
		{"RESPONSE", "/ei/initialValues", 500, 2},
		{"GET", "/ci/services", 7000, 1},
		{"GET", "/ci/authentication", 7001, 2},
		{"GET", "/ci/info", 7002, 2},
		{"NOTIFY", "/ei/deviceReady", 7003, 2},
		{"GET", "/ni/info", 7004, 1},
		{"GET", "/ro/allDescriptionChanges", 7005, 1},
		{"GET", "/ro/allMandatoryValues", 7006, 1},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d messages, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		if g.Action != w.action || g.Resource != w.resource || g.MsgID != w.msgID || g.Version != w.version || g.SID != 1001 {
			t.Errorf("message %d = %s %s msgID %d v%d sID %d, want %+v", i, g.Action, g.Resource, g.MsgID, g.Version, g.SID, w)
		}
	}
	wantReply := `"data":[{"deviceType":"Application","deviceName":"hcbridge","deviceID":"0a1b2c3d"}]`
	if !strings.Contains(string(got[0].Raw), wantReply) {
		t.Errorf("initialValues reply %s does not contain %s", got[0].Raw, wantReply)
	}
	var auth []map[string]string
	if err := json.Unmarshal(got[2].Data, &auth); err != nil || len(auth) != 1 || len(auth[0]["nonce"]) != 43 {
		t.Errorf("authentication nonce: %s", got[2].Data)
	}

	var info *hc.DeviceInfo
	for _, ev := range h.rec.events() {
		if ev.Kind == hc.EvInfo {
			info = ev.Info
		}
	}
	if info == nil || info.SwVersion != hcfake.SwVersion || info.HaVersion != hcfake.HaVersion || info.RSSI == nil || *info.RSSI != -61 {
		t.Fatalf("device info %+v", info)
	}
}

// The bridge must never write to the appliance: the only verbs it sends
// are GETs, the initialValues answer and the deviceReady notification.
func TestOnlyReadOnlyVerbs(t *testing.T) {
	srv := newServer(t)
	srv.SetServices(2, 2, true, true)
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)
	for _, m := range srv.Received() {
		ok := m.Action == "GET" ||
			(m.Action == "RESPONSE" && m.Resource == "/ei/initialValues") ||
			(m.Action == "NOTIFY" && m.Resource == "/ei/deviceReady")
		if !ok {
			t.Errorf("non-read-only message sent: %s %s", m.Action, m.Resource)
		}
	}
}

func TestUpgradeCarriesNoOrigin(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)
	hdr := srv.UpgradeHeaders()[0]
	for _, k := range []string{"Origin", "Sec-Websocket-Extensions", "Sec-Websocket-Protocol"} {
		if _, ok := hdr[k]; ok {
			t.Errorf("upgrade request carries %s: %v", k, hdr[k])
		}
	}
}

func TestInitVersion1UsesNumericDeviceType(t *testing.T) {
	srv := newServer(t)
	srv.SetInitVersion(1)
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)
	if raw := string(srv.Received()[0].Raw); !strings.Contains(raw, `[{"deviceType":2,"deviceName":"hcbridge","deviceID":"0a1b2c3d"}]`) {
		t.Fatalf("reply %s", raw)
	}
}

func TestCIv3SkipsAuthentication(t *testing.T) {
	srv := newServer(t)
	srv.SetServices(3, 2, true, false)
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)
	var seen []string
	for _, m := range srv.Received() {
		seen = append(seen, m.Resource)
	}
	all := strings.Join(seen, " ")
	if strings.Contains(all, "/ci/authentication") || strings.Contains(all, "/ci/info") || strings.Contains(all, "/ni/info") || !strings.Contains(all, "/iz/info") {
		t.Fatalf("requests: %s", all)
	}
}

func valueEvents(r *recorder) []hc.Event {
	var out []hc.Event
	for _, ev := range r.events() {
		if ev.Kind == hc.EvValues {
			out = append(out, ev)
		}
	}
	return out
}

func TestSnapshotThenNotify(t *testing.T) {
	srv := newServer(t)
	srv.SetValues(map[int]any{4409: 18, 8204: 13})
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)
	srv.Notify(map[int]any{4409: 95})
	waitFor(t, 2*time.Second, "notify", func() bool { return len(valueEvents(h.rec)) >= 2 })

	evs := valueEvents(h.rec)
	if !evs[0].Snapshot || len(evs[0].Values) != 2 {
		t.Fatalf("first values event %+v", evs[0])
	}
	syms := map[string]string{}
	for _, v := range evs[0].Values {
		syms[v.Name] = v.Symbol
	}
	if syms["Cooking.Hob.Setting.Ventilation"] != "Level08" || syms["Cooking.Hob.Status.Zone.100.PowerLevel"] != "65" {
		t.Fatalf("snapshot %v", syms)
	}
	if evs[1].Snapshot || evs[1].Values[0].Symbol != "BoostLevel1" {
		t.Fatalf("notify %+v", evs[1])
	}
}

func TestMandatoryValuesRetryAndError(t *testing.T) {
	srv := newServer(t)
	srv.FailMandatory(0, true) // first request unanswered
	h := start(t, srv, func(o *hc.Options) { o.RequestTimeout = 200 * time.Millisecond })
	h.rec.waitLink(t, hc.LinkConnected, 1)
	if srv.Sessions() != 1 || !h.log.has("retrying once") {
		t.Fatalf("sessions %d", srv.Sessions())
	}

	srv2 := newServer(t)
	srv2.FailMandatory(500, false)
	h2 := start(t, srv2, nil)
	h2.rec.waitLink(t, hc.LinkConnected, 1)
	if !h2.log.has("continuing without a snapshot") {
		t.Fatal("500 not reported")
	}
}

func TestMalformedMessageKeepsSession(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)
	srv.SendRaw([]byte(`{"sID":1,"msgID":2,"resource":"/ro/values","action":"NOTIFY","data":[{"uid":4409,`))
	srv.Notify(map[int]any{4409: 1})
	waitFor(t, 2*time.Second, "value after garbage", func() bool { return len(valueEvents(h.rec)) >= 2 })
	if srv.Sessions() != 1 || !h.log.has("skipping undecodable message") {
		t.Fatalf("sessions %d", srv.Sessions())
	}
}

// A corrupted frame ends the session with a clean close, and the client
// comes back with the same device ID and never more than one connection.
func TestDesyncReconnectsCleanly(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)
	srv.CorruptNextMAC()
	srv.Notify(map[int]any{4409: 12})
	h.rec.waitLink(t, hc.LinkConnected, 2)

	if got := h.rec.linkStates(); fmt.Sprint(got) != "[connecting connected connecting connected]" {
		t.Fatalf("link states %v", got)
	}
	if codes := srv.CloseCodes(); len(codes) == 0 || codes[0] != 1000 {
		t.Fatalf("close codes %v", codes)
	}
	if srv.MaxConcurrent() != 1 {
		t.Fatalf("max concurrent %d", srv.MaxConcurrent())
	}
	var ids []string
	for _, m := range srv.Received() {
		if m.Resource == "/ei/initialValues" {
			var d []map[string]any
			json.Unmarshal(m.Data, &d)
			ids = append(ids, fmt.Sprint(d[0]["deviceID"]))
		}
	}
	if fmt.Sprint(ids) != "[0a1b2c3d 0a1b2c3d]" {
		t.Fatalf("device IDs %v", ids)
	}
}

func TestPongTimeoutReconnects(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, func(o *hc.Options) {
		o.PingEvery = 50 * time.Millisecond
		o.PongTimeout = 100 * time.Millisecond
	})
	h.rec.waitLink(t, hc.LinkConnected, 1)
	srv.SetPong(false)
	h.rec.waitLink(t, hc.LinkConnecting, 2)
	if !h.log.has("no answer to ping") {
		t.Fatal("pong timeout not logged")
	}
	if codes := srv.CloseCodes(); len(codes) == 0 || codes[0] != 1000 {
		t.Fatalf("close codes %v", codes)
	}
}

// Pongs keep a quiet session alive; there is no idle-read timeout.
func TestQuietSessionStaysUp(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, func(o *hc.Options) {
		o.PingEvery = 40 * time.Millisecond
		o.PongTimeout = 100 * time.Millisecond
	})
	h.rec.waitLink(t, hc.LinkConnected, 1)
	time.Sleep(500 * time.Millisecond)
	if srv.Sessions() != 1 || srv.Pings() < 5 {
		t.Fatalf("sessions %d pings %d", srv.Sessions(), srv.Pings())
	}
}

func TestShutdownSendsClose1000(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)
	h.stop()
	if codes := srv.CloseCodes(); fmt.Sprint(codes) != "[1000]" {
		t.Fatalf("close codes %v", codes)
	}
}

func TestRefused503(t *testing.T) {
	srv := newServer(t)
	srv.Refuse(503, http.Header{"Server": {"WebSocket++/0.8.2"}}, "busy")
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkRefused, 1)
	waitFor(t, 3*time.Second, "several refused attempts", func() bool { return len(srv.DialTimes()) >= 4 })

	links := h.rec.links()
	if fmt.Sprint(h.rec.linkStates()) != "[connecting refused]" {
		t.Fatalf("link states %v (retries must not re-emit)", h.rec.linkStates())
	}
	var ue *hc.UpgradeError
	if !errors.As(links[1].Err, &ue) || ue.Status != 503 || string(ue.Body) != "busy" || links[1].Since.IsZero() {
		t.Fatalf("refused status %+v", links[1])
	}
	if !h.log.has("Server: WebSocket++/0.8.2") {
		t.Fatal("refusal details not logged")
	}

	srv.Refuse(0, nil, "")
	h.rec.waitLink(t, hc.LinkConnected, 1)
	if !h.log.has("connected after refused") {
		t.Fatal("recovery not logged")
	}
}

func TestUnreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there now

	log := &testLogger{t: t}
	o := hc.Options{Host: addr, DeviceID: testDeviceID, Log: log, BackoffMin: 20 * time.Millisecond, MinDialGap: 10 * time.Millisecond}
	rec := &recorder{}
	c, _ := hc.NewClient(o, hcfake.Profile(), rec.handle)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	rec.waitLink(t, hc.LinkUnreachable, 1)
}

// A stale key: the first frame fails authentication on every connection;
// after three in a row the state becomes key_error.
func TestWrongKeyBecomesKeyError(t *testing.T) {
	srv := newServer(t)
	srv.SetKey(make([]byte, 32))
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkKeyError, 1)
	if got := fmt.Sprint(h.rec.linkStates()); got != "[connecting error key_error]" {
		t.Fatalf("link states %s", got)
	}
	if len(srv.DialTimes()) < 3 {
		t.Fatalf("dials %d", len(srv.DialTimes()))
	}
}

// Failed attempts back off exponentially up to the cap.
func TestBackoffGrows(t *testing.T) {
	srv := newServer(t)
	srv.Refuse(503, nil, "")
	start(t, srv, func(o *hc.Options) {
		o.BackoffMin = 30 * time.Millisecond
		o.BackoffMax = 120 * time.Millisecond
		o.MinDialGap = time.Millisecond
	})
	waitFor(t, 3*time.Second, "6 dials", func() bool { return len(srv.DialTimes()) >= 6 })
	d := srv.DialTimes()
	want := []time.Duration{30, 60, 120, 120, 120}
	for i, w := range want {
		gap := d[i+1].Sub(d[i])
		w *= time.Millisecond
		if gap < w || gap > w+80*time.Millisecond {
			t.Errorf("gap %d = %v, want ~%v", i, gap, w)
		}
	}
}

func TestNotBeforeDelaysFirstDial(t *testing.T) {
	srv := newServer(t)
	nb := time.Now().Add(300 * time.Millisecond)
	var dialed []time.Time
	var mu sync.Mutex
	h := start(t, srv, func(o *hc.Options) {
		o.NotBefore = nb
		o.OnDial = func(at time.Time) { mu.Lock(); dialed = append(dialed, at); mu.Unlock() }
	})
	h.rec.waitLink(t, hc.LinkConnected, 1)
	mu.Lock()
	defer mu.Unlock()
	if len(dialed) != 1 || dialed[0].Before(nb) {
		t.Fatalf("dialed %v, not before %v", dialed, nb)
	}
}

// A resumed failure episode keeps its state and start time: no
// "connecting" announcement, and retries in the same state stay silent.
func TestResumeEpisode(t *testing.T) {
	srv := newServer(t)
	srv.Refuse(503, nil, "")
	since := time.Now().Add(-time.Hour)
	h := start(t, srv, func(o *hc.Options) { o.Resume = hc.LinkStatus{State: hc.LinkRefused, Since: since} })
	waitFor(t, 3*time.Second, "a few refused dials", func() bool { return len(srv.DialTimes()) >= 3 })
	if got := h.rec.linkStates(); len(got) != 0 {
		t.Fatalf("resumed episode emitted %v", got)
	}
	srv.Refuse(0, nil, "")
	h.rec.waitLink(t, hc.LinkConnected, 1)
	if !h.log.has("connected after refused for 1h") {
		t.Fatal("recovery log does not span the resumed episode")
	}
}
