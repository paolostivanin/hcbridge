package homeconnect_test

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	hc "hcbridge/src/internal/homeconnect"
	"hcbridge/src/internal/homeconnect/hcfake"
)

// Session paths the scenario tests in client_test.go do not reach.

// Error answers to the informational handshake requests are tolerated: the
// session comes up all the same.
func TestHandshakeToleratesErrorAnswers(t *testing.T) {
	srv := newServer(t)
	srv.SetServices(1, 2, true, true) // ci v1: authentication; iz present
	for _, r := range []string{"/ci/authentication", "/ci/info", "/iz/info", "/ni/info", "/ro/allDescriptionChanges"} {
		srv.FailResource(r, 500)
	}
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)
	if srv.Sessions() != 1 {
		t.Fatalf("sessions %d", srv.Sessions())
	}
}

// A handshake request that goes unanswered ends the attempt (as error),
// whichever request it is, and the client tries again.
func TestHandshakeTimeoutsFailTheAttempt(t *testing.T) {
	for _, r := range []string{"/ci/services", "/ci/authentication", "/ci/info", "/iz/info", "/ni/info", "/ro/allDescriptionChanges"} {
		srv := newServer(t)
		srv.SetServices(1, 2, true, true)
		srv.FailResource(r, -1)
		h := start(t, srv, func(o *hc.Options) { o.RequestTimeout = 50 * time.Millisecond })
		h.rec.waitLink(t, hc.LinkError, 1)
		waitFor(t, 2*time.Second, r+": a second attempt", func() bool { return len(srv.DialTimes()) >= 2 })
		h.stop()
		if !h.log.has("handshake") {
			t.Errorf("%s: failure not reported as a handshake error", r)
		}
	}
}

func TestNoInitialValuesTimesOut(t *testing.T) {
	srv := newServer(t)
	srv.SetSendInit(false)
	h := start(t, srv, func(o *hc.Options) { o.InitTimeout = 50 * time.Millisecond })
	h.rec.waitLink(t, hc.LinkError, 1)
	if codes := srv.CloseCodes(); len(codes) == 0 || codes[0] != 1000 {
		t.Fatalf("close codes %v: give up with a clean close", codes)
	}
}

// Without an edMsgID the client numbers its messages after the appliance's.
func TestInitWithoutEdMsgID(t *testing.T) {
	srv := newServer(t)
	srv.SetInitEdMsgID(0)
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)
	for _, m := range srv.Received() {
		if m.Resource == "/ci/services" && m.MsgID != 501 {
			t.Fatalf("first request msgID %d, want 501 (after the appliance's 500)", m.MsgID)
		}
	}
}

// The appliance restarts the handshake on the live socket: the client
// redoes it; when that fails, it closes cleanly and reconnects.
func TestRehandshake(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, func(o *hc.Options) { o.RequestTimeout = 100 * time.Millisecond })
	h.rec.waitLink(t, hc.LinkConnected, 1)
	srv.Reinit()
	waitFor(t, 2*time.Second, "a second device info", func() bool {
		n := 0
		for _, ev := range h.rec.events() {
			if ev.Kind == hc.EvInfo {
				n++
			}
		}
		return n == 2
	})
	if srv.Sessions() != 1 || !h.log.has("redoing the handshake") {
		t.Fatalf("sessions %d", srv.Sessions())
	}

	srv.FailResource("/ci/services", -1)
	srv.Reinit()
	waitFor(t, 3*time.Second, "a reconnect", func() bool { return len(srv.DialTimes()) >= 2 })
	if codes := srv.CloseCodes(); len(codes) == 0 || codes[0] != 1000 || !h.log.has("re-handshake") {
		t.Fatalf("close codes %v", codes)
	}
}

// A corrupted frame in the middle of the handshake is a desync, reported as
// such (not as a key error).
func TestDesyncDuringHandshake(t *testing.T) {
	srv := newServer(t)
	srv.CorruptResponseTo("/ci/services")
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkError, 1)
	for _, st := range h.rec.linkStates() {
		if st == hc.LinkKeyError {
			t.Fatal("a desync after the first frame counted as a key error")
		}
	}
}

// Frames that are not part of the protocol, requests the read-only client
// never answers, and malformed values are all survived.
func TestOddFramesKeepTheSession(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, func(o *hc.Options) { o.Raw = true })
	h.rec.waitLink(t, hc.LinkConnected, 1)
	srv.SendText("hello")
	srv.PingClient()
	srv.SendRaw([]byte(`{"sID":1,"msgID":9999,"resource":"/ro/values","version":1,"action":"RESPONSE","data":[]}`))
	srv.SendRaw([]byte(`{"sID":1,"msgID":1,"resource":"/ei/somethingElse","version":1,"action":"NOTIFY","data":[]}`))
	srv.SendRaw([]byte(`{"sID":1,"msgID":2,"resource":"/ro/values","version":1,"action":"POST","data":[]}`))
	srv.SendRaw([]byte(`{"sID":1,"msgID":3,"resource":"/ro/values","version":1,"action":"DELETE","data":[]}`))
	srv.SendRaw([]byte(`{"sID":1,"msgID":4,"resource":"/ro/values","version":1,"action":"NOTIFY","data":[{"uid":"x"},{"uid":4409,"value":1}]}`))
	srv.SendRaw([]byte(`{"broken": "` + strings.Repeat("x", 400) + `"`))
	waitFor(t, 2*time.Second, "the last frame", func() bool { return h.log.has("skipping undecodable message") })
	if srv.Sessions() != 1 {
		t.Fatalf("sessions %d", srv.Sessions())
	}
	for _, s := range []string{"not answering (read-only)", "skipped 1 undecodable", "skipping undecodable message"} {
		if !h.log.has(s) {
			t.Errorf("log lacks %q", s)
		}
	}
	raw := 0
	for _, ev := range h.rec.events() {
		if ev.Kind == hc.EvRaw {
			raw++
		}
	}
	if raw == 0 {
		t.Error("no raw frames with Options.Raw")
	}
}

// The resync request is cut off by the connection dropping: the session
// ends and the client reconnects.
func TestResyncCutOffByADrop(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, func(o *hc.Options) { o.ResyncEvery = 50 * time.Millisecond })
	h.rec.waitLink(t, hc.LinkConnected, 1)
	srv.FailResource("/ro/allMandatoryValues", -1)
	reads := func() int {
		n := 0
		for _, m := range srv.Received() {
			if m.Resource == "/ro/allMandatoryValues" {
				n++
			}
		}
		return n
	}
	waitFor(t, 2*time.Second, "the resync request", func() bool { return reads() >= 2 })
	srv.FailResource("/ro/allMandatoryValues", 0)
	srv.Drop()
	h.rec.waitLink(t, hc.LinkConnected, 2)
}

// A session that lasted resets the backoff; a stop while waiting for the
// first dial returns at once.
func TestStableSessionAndStopBeforeDial(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, func(o *hc.Options) { o.StableAfter = 20 * time.Millisecond })
	h.rec.waitLink(t, hc.LinkConnected, 1)
	time.Sleep(50 * time.Millisecond)
	srv.Drop()
	h.rec.waitLink(t, hc.LinkConnected, 2)

	srv2 := newServer(t)
	h2 := start(t, srv2, func(o *hc.Options) { o.NotBefore = time.Now().Add(time.Hour) })
	began := time.Now()
	h2.stop()
	if time.Since(began) > time.Second || len(srv2.DialTimes()) != 0 {
		t.Fatalf("stop before the first dial took %s, dials %d", time.Since(began), len(srv2.DialTimes()))
	}
}

// An appliance that never answers our Close: the client gives up waiting
// after closeWait and closes the socket.
func TestCloseWithoutReply(t *testing.T) {
	srv := newServer(t)
	srv.SetCloseReply(false)
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)
	began := time.Now()
	h.stop()
	if d := time.Since(began); d < time.Second || d > 8*time.Second {
		t.Fatalf("stop took %s, want about the Close wait", d)
	}
}

// A handler that cannot keep up: the queue fills, the client says so, and
// nothing is lost.
func TestSlowHandler(t *testing.T) {
	srv := newServer(t)
	log := &testLogger{t: t}
	gate := make(chan struct{})
	var mu sync.Mutex
	values := 0
	o := hc.Options{Host: srv.Host(), DeviceID: testDeviceID, Log: log, PingEvery: time.Hour, PongTimeout: time.Hour}
	c, err := hc.NewClient(o, hcfake.Profile(), func(ev hc.Event) {
		if ev.Kind == hc.EvValues {
			<-gate
			mu.Lock()
			values++
			mu.Unlock()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	waitFor(t, 2*time.Second, "the session", func() bool { return srv.Sessions() == 1 })
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 1100; i++ {
		srv.Notify(map[int]any{4409: i % 10})
	}
	waitFor(t, 5*time.Second, "the full-queue warning", func() bool { return log.has("event queue full") })
	close(gate)
	waitFor(t, 5*time.Second, "every value", func() bool { mu.Lock(); defer mu.Unlock(); return values >= 1101 })
	cancel()
	<-done
}

func TestNewClientValidation(t *testing.T) {
	log := &testLogger{t: t}
	if _, err := hc.NewClient(hc.Options{Host: "192.0.2.1", DeviceID: testDeviceID, Log: log}, nil, func(hc.Event) {}); err == nil {
		t.Error("no profile accepted")
	}
	if _, err := hc.NewClient(hc.Options{Host: "192.0.2.1", DeviceID: testDeviceID}, hcfake.Profile(), func(hc.Event) {}); err == nil {
		t.Error("no logger accepted")
	}
}

// An upgrade answer net/http cannot parse (the reported "TTP/1.1 503") is
// an error, not a refusal; the bridge keeps the refusal clock running
// through it (see linkMonitor).
func TestUnparseableUpgradeAnswerIsAnError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				bufio.NewReader(c).ReadString('\n') // the request line
				c.Write([]byte("TTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"))
			}()
		}
	}()
	log := &testLogger{t: t}
	rec := &recorder{}
	o := hc.Options{Host: ln.Addr().String(), DeviceID: testDeviceID, Log: log, BackoffMin: time.Hour}
	c, err := hc.NewClient(o, hcfake.Profile(), rec.handle)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	rec.waitLink(t, hc.LinkError, 1)
	cancel()
	<-done
	if !log.has("websocket upgrade") || !log.has("malformed HTTP") {
		t.Fatal("the unparseable answer was not reported as such")
	}
}

// After a corrupted frame the session discards whatever else arrives and
// reconnects cleanly.
func TestFramesAfterADesyncAreDiscarded(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, nil)
	h.rec.waitLink(t, hc.LinkConnected, 1)
	before := len(valueEvents(h.rec))
	srv.CorruptNextMAC()
	for i := 0; i < 5; i++ {
		srv.Notify(map[int]any{4409: i})
	}
	h.rec.waitLink(t, hc.LinkConnected, 2)
	for _, ev := range valueEvents(h.rec)[before:] {
		if !ev.Snapshot {
			t.Fatalf("a value after the desync was delivered: %+v", ev)
		}
	}
}

// Whether a failed attempt got past the upgrade is reported, and a change
// of it alone produces an event (the state stays "error", Since unchanged).
func TestUpgradedIsReported(t *testing.T) {
	srv := newServer(t)
	srv.SetSendInit(false) // accepted, then no initialValues: error, upgraded
	h := start(t, srv, func(o *hc.Options) { o.InitTimeout = 30 * time.Millisecond })
	waitFor(t, 3*time.Second, "an upgraded error", func() bool {
		for _, l := range h.rec.links() {
			if l.State == hc.LinkError && l.Upgraded {
				return true
			}
		}
		return false
	})
	srv.Refuse(500, nil, "") // not accepted: still error, not upgraded
	waitFor(t, 3*time.Second, "the change reported", func() bool {
		ls := h.rec.links()
		last := ls[len(ls)-1]
		return last.State == hc.LinkError && !last.Upgraded
	})
	var since []time.Time
	for _, l := range h.rec.links() {
		if l.State == hc.LinkError {
			since = append(since, l.Since)
		}
	}
	if len(since) < 2 || !since[0].Equal(since[len(since)-1]) {
		t.Fatalf("error episode start moved: %v", since)
	}
}

// Two snapshot timeouts at connect no longer drop the session: it stays up
// and the read is retried on it.
func TestSnapshotTimeoutsKeepTheSession(t *testing.T) {
	srv := newServer(t)
	srv.FailResource("/ro/allMandatoryValues", -1)
	h := start(t, srv, func(o *hc.Options) {
		o.RequestTimeout = 40 * time.Millisecond
		o.SnapshotRetryMin, o.SnapshotRetryMax = 30*time.Millisecond, 60*time.Millisecond
	})
	h.rec.waitLink(t, hc.LinkConnected, 1)
	srv.SetValues(map[int]any{4409: 18})
	srv.FailResource("/ro/allMandatoryValues", 0)
	waitFor(t, 3*time.Second, "the snapshot", func() bool {
		for _, ev := range valueEvents(h.rec) {
			if ev.Snapshot && len(ev.Values) > 0 {
				return true
			}
		}
		return false
	})
	if srv.Sessions() != 1 || len(srv.CloseCodes()) != 0 {
		t.Fatalf("sessions %d, closes %v: the session must survive slow snapshots", srv.Sessions(), srv.CloseCodes())
	}
}

func mandatoryReads(srv *hcfake.Server) int {
	n := 0
	for _, m := range srv.Received() {
		if m.Resource == "/ro/allMandatoryValues" {
			n++
		}
	}
	return n
}

// The retries back off: with 20 ms doubling to 160 ms, 500 ms hold about
// 5 reads, not the 25 a fixed 20 ms would give.
func TestSnapshotRetriesBackOff(t *testing.T) {
	srv := newServer(t)
	srv.FailMandatory(500, false)
	h := start(t, srv, func(o *hc.Options) {
		o.SnapshotRetryMin, o.SnapshotRetryMax = 20*time.Millisecond, 160*time.Millisecond
	})
	h.rec.waitLink(t, hc.LinkConnected, 1)
	time.Sleep(500 * time.Millisecond)
	if n := mandatoryReads(srv); n < 3 || n > 9 {
		t.Fatalf("%d reads in 500 ms, want about 5 (doubling)", n)
	}
}

// A re-handshake whose snapshot fails schedules a retry, even when the
// session had a good snapshot (and so a 6-hour resync) before.
func TestRehandshakeSnapshotFailureIsRetried(t *testing.T) {
	srv := newServer(t)
	h := start(t, srv, func(o *hc.Options) {
		o.SnapshotRetryMin, o.SnapshotRetryMax = 20*time.Millisecond, 40*time.Millisecond
	})
	h.rec.waitLink(t, hc.LinkConnected, 1)
	srv.FailMandatory(500, false)
	srv.Reinit()
	waitFor(t, 2*time.Second, "retries after the re-handshake", func() bool { return mandatoryReads(srv) >= 4 })
}

// After a cancelled upgrade the client sends nothing but the Close. (The
// race the check guards, initialValues already decoded when the select
// runs, is rare; this test describes the behaviour more than it catches a
// regression.)
func TestNoHandshakeAfterACancelledUpgrade(t *testing.T) {
	srv := newServer(t)
	srv.SetUpgradeDelay(150 * time.Millisecond)
	h := start(t, srv, nil)
	waitFor(t, 2*time.Second, "the dial", func() bool { return len(srv.DialTimes()) == 1 })
	h.stop()
	if got := srv.Received(); len(got) != 0 {
		t.Fatalf("the client sent %d messages after shutdown was requested: %s", len(got), got[0].Resource)
	}
}

// ResumeBackoff above BackoffMax is capped.
func TestResumeBackoffIsCapped(t *testing.T) {
	srv := newServer(t)
	srv.Refuse(503, nil, "")
	h := start(t, srv, func(o *hc.Options) {
		o.BackoffMin, o.BackoffMax, o.ResumeBackoff = 10*time.Millisecond, 60*time.Millisecond, time.Hour
	})
	waitFor(t, 3*time.Second, "two dials", func() bool { return len(srv.DialTimes()) >= 2 })
	h.stop()
	d := srv.DialTimes()
	if gap := d[1].Sub(d[0]); gap > 500*time.Millisecond {
		t.Fatalf("second dial %s after the first; ResumeBackoff must be capped at BackoffMax", gap)
	}
}
