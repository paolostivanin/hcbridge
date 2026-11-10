package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	hc "hcbridge/src/internal/homeconnect"
)

func TestLinkMonitorLatch(t *testing.T) {
	t0 := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	m := linkMonitor{threshold: 30 * time.Minute}
	m.update(hc.LinkStatus{State: hc.LinkRefused, Since: t0})
	if m.powerCycleNeeded(t0.Add(29 * time.Minute)) {
		t.Fatal("latched before the threshold")
	}
	if !m.powerCycleNeeded(t0.Add(30 * time.Minute)) {
		t.Fatal("not latched at the threshold")
	}
	// The breaker is off: unreachable keeps the flag.
	m.update(hc.LinkStatus{State: hc.LinkUnreachable, Since: t0.Add(40 * time.Minute)})
	if !m.powerCycleNeeded(t0.Add(41 * time.Minute)) {
		t.Fatal("unreachable cleared the flag")
	}
	m.update(hc.LinkStatus{State: hc.LinkConnecting, Since: t0.Add(42 * time.Minute)})
	if !m.powerCycleNeeded(t0.Add(42 * time.Minute)) {
		t.Fatal("connecting cleared the flag")
	}
	m.update(hc.LinkStatus{State: hc.LinkConnected, Since: t0.Add(43 * time.Minute)})
	if m.powerCycleNeeded(t0.Add(43 * time.Minute)) {
		t.Fatal("connected did not clear the flag")
	}
}

// An unreachable blip during a refusal episode restarts the clock: the hob
// was off the network, which is what a power cycle looks like.
func TestLinkMonitorUnreachableRestartsTheEpisode(t *testing.T) {
	t0 := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	m := linkMonitor{threshold: 30 * time.Minute}
	m.update(hc.LinkStatus{State: hc.LinkRefused, Since: t0})
	m.update(hc.LinkStatus{State: hc.LinkUnreachable, Since: t0.Add(20 * time.Minute)})
	m.update(hc.LinkStatus{State: hc.LinkRefused, Since: t0.Add(21 * time.Minute)})
	if m.powerCycleNeeded(t0.Add(40 * time.Minute)) {
		t.Fatal("latched across an interrupted episode")
	}
	if !m.powerCycleNeeded(t0.Add(51 * time.Minute)) {
		t.Fatal("not latched 30 min into the new episode")
	}
}

func TestLinkMonitorRestore(t *testing.T) {
	t0 := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	m := linkMonitor{threshold: 30 * time.Minute}
	m.update(hc.LinkStatus{State: hc.LinkRefused, Since: t0})
	if err := saveLinkRecord(dir, m.record(t0.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	r, err := loadLinkRecord(dir)
	if err != nil || r.LastDial != t0.Add(time.Minute) {
		t.Fatalf("record %+v %v", r, err)
	}
	// After a restart 31 minutes in, the episode's clock carries on.
	m2 := linkMonitor{threshold: 30 * time.Minute}
	m2.restore(r)
	if !m2.powerCycleNeeded(t0.Add(31 * time.Minute)) {
		t.Fatal("restart reset the power-cycle clock")
	}
}

// Failed attempts that are not refusals (a garbled or dropped upgrade
// answer, a timeout) say nothing either way: they must not restart the
// clock, or a hob that alternates them with 503s would never raise the flag.
func TestLinkMonitorErrorsDoNotRestartTheEpisode(t *testing.T) {
	t0 := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	m := linkMonitor{threshold: 30 * time.Minute}
	m.update(hc.LinkStatus{State: hc.LinkRefused, Since: t0})
	m.update(hc.LinkStatus{State: hc.LinkError, Since: t0.Add(10 * time.Minute)})
	if m.powerCycleNeeded(t0.Add(31 * time.Minute)) {
		t.Fatal("latched while the state is error, not refused")
	}
	m.update(hc.LinkStatus{State: hc.LinkRefused, Since: t0.Add(32 * time.Minute)})
	if !m.powerCycleNeeded(t0.Add(32 * time.Minute)) {
		t.Fatal("an error attempt restarted the refusal clock")
	}

	// A client starting up ("connecting", as local-watch does) continues a
	// restored episode too.
	m2 := linkMonitor{threshold: 30 * time.Minute}
	m2.restore(linkRecord{State: "refused", Since: t0, RefusedSince: t0})
	m2.update(hc.LinkStatus{State: hc.LinkConnecting, Since: t0.Add(20 * time.Minute)})
	m2.update(hc.LinkStatus{State: hc.LinkRefused, Since: t0.Add(21 * time.Minute)})
	if !m2.powerCycleNeeded(t0.Add(30 * time.Minute)) {
		t.Fatal("connecting restarted a restored episode")
	}

	// A session in between does end it.
	m.update(hc.LinkStatus{State: hc.LinkConnected, Since: t0.Add(40 * time.Minute)})
	m.update(hc.LinkStatus{State: hc.LinkRefused, Since: t0.Add(41 * time.Minute)})
	if m.powerCycleNeeded(t0.Add(50 * time.Minute)) {
		t.Fatal("the episode survived a session")
	}
}

// The first dial after a restart (or a local-watch run) keeps the spacing
// the previous run had reached.
func TestLinkBookSpacing(t *testing.T) {
	t0 := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		rec     linkRecord
		gap     time.Duration // from the last dial; -1: no constraint
		backoff time.Duration
	}{
		{"fresh install", linkRecord{}, -1, 0},
		{"ended connected", linkRecord{State: "connected", Since: t0.Add(-time.Hour), LastDial: t0}, 5 * time.Second, 0},
		{"short failure", linkRecord{State: "error", Since: t0.Add(-10 * time.Second), LastDial: t0}, 30 * time.Second, 30 * time.Second},
		{"failing for 2 min", linkRecord{State: "unreachable", Since: t0.Add(-2 * time.Minute), LastDial: t0}, 2 * time.Minute, 2 * time.Minute},
		{"refused for hours", linkRecord{State: "refused", Since: t0.Add(-3 * time.Hour), LastDial: t0}, 5 * time.Minute, 5 * time.Minute},
	}
	for _, tc := range cases {
		lb := &linkBook{loaded: tc.rec}
		nb, backoff := lb.spacing()
		if tc.gap < 0 {
			if !nb.IsZero() || backoff != 0 {
				t.Errorf("%s: not before %v, backoff %s; want none", tc.name, nb, backoff)
			}
			continue
		}
		if got := nb.Sub(tc.rec.LastDial); got != tc.gap || backoff != tc.backoff {
			t.Errorf("%s: gap %s backoff %s, want %s and %s", tc.name, got, backoff, tc.gap, tc.backoff)
		}
	}
}

// The regression both reviews found: local-watch (no Resume) records
// "connecting" and its own refusals over the service's episode. The time
// without a session must survive that, so the service's next first dial
// keeps the 5 min spacing in both cases.
func TestLinkBookSpacingSurvivesLocalWatch(t *testing.T) {
	now := time.Now()
	start := now.Add(-3 * time.Hour)
	seed := linkRecord{State: "refused", Since: start, RefusedSince: start, DownSince: start, LastDial: now.Add(-10 * time.Second)}
	for _, tc := range []struct {
		name string
		run  func(lb *linkBook)
	}{
		{"aborted before dialling", func(lb *linkBook) {
			lb.record(hc.LinkStatus{State: hc.LinkConnecting, Since: time.Now()}, time.Now())
		}},
		{"refused once", func(lb *linkBook) {
			lb.record(hc.LinkStatus{State: hc.LinkConnecting, Since: time.Now()}, time.Now())
			lb.onDial(time.Now())
			lb.record(hc.LinkStatus{State: hc.LinkRefused, Since: time.Now()}, time.Now())
		}},
	} {
		dir := t.TempDir()
		saveLinkRecord(dir, seed)
		lw := openLinkBook(dir, 30*time.Minute, quietLogger()) // local-watch
		tc.run(lw)
		lw.save()
		svc := openLinkBook(dir, 30*time.Minute, quietLogger()) // the service afterwards
		nb, backoff := svc.spacing()
		if gap := nb.Sub(svc.loaded.LastDial); gap != 5*time.Minute || backoff != 5*time.Minute {
			t.Errorf("%s: service spacing %s / backoff %s, want 5m0s / 5m0s", tc.name, gap, backoff)
		}
	}

	// A file from before down_since existed: the state's start stands in.
	old := &linkBook{loaded: linkRecord{State: "refused", Since: now.Add(-time.Hour), LastDial: now}}
	if nb, _ := old.spacing(); nb.Sub(now) != 5*time.Minute {
		t.Errorf("old record: gap %s", nb.Sub(now))
	}
}

// The refusal clock pauses while nothing watches: 5 minutes of refusal
// before a 2-hour stop do not raise the flag at startup.
func TestLinkBookPausesTheClockWhileStopped(t *testing.T) {
	dir := t.TempDir()
	lastDial := time.Now().Add(-2 * time.Hour)
	saveLinkRecord(dir, linkRecord{State: "refused", Since: lastDial.Add(-5 * time.Minute), RefusedSince: lastDial.Add(-5 * time.Minute), LastDial: lastDial})
	lb := openLinkBook(dir, 30*time.Minute, quietLogger())
	if lb.mon.powerCycleNeeded(time.Now()) {
		t.Fatal("the stop counted as refusal")
	}
	if !lb.mon.powerCycleNeeded(time.Now().Add(26 * time.Minute)) {
		t.Fatal("the observed 5 minutes were lost: 26 more should reach 30")
	}
	// A latched flag stays latched.
	saveLinkRecord(dir, linkRecord{State: "refused", Since: lastDial, RefusedSince: lastDial, Latched: true, LastDial: lastDial})
	if lb := openLinkBook(dir, 30*time.Minute, quietLogger()); !lb.mon.powerCycleNeeded(time.Now()) {
		t.Fatal("a latched flag was dropped")
	}
}

func TestLinkBookSanitisesTheRecord(t *testing.T) {
	// A last dial in the future (a clock that was ahead) does not hold the
	// first dial back by that much.
	dir := t.TempDir()
	saveLinkRecord(dir, linkRecord{State: "connected", Since: time.Now().Add(-time.Hour), LastDial: time.Now().Add(24 * time.Hour)})
	if nb, _ := openLinkBook(dir, time.Minute, quietLogger()).spacing(); time.Until(nb) > time.Minute {
		t.Fatalf("first dial in %s", time.Until(nb))
	}
	// A record that does not parse is not half-used.
	os.WriteFile(filepath.Join(dir, "link.json"), []byte(`{"state":"refused","power_cycle_needed":true,"since":"bogus"}`), 0o600)
	lb := openLinkBook(dir, time.Minute, quietLogger())
	if lb.mon.latched || lb.mon.state != "" {
		t.Fatalf("partly decoded record used: %+v", lb.mon)
	}
}

// An attempt the hob accepted (HTTP 101) ends the refusal episode even
// though the state stays "error".
func TestLinkMonitorAcceptedUpgradeEndsTheEpisode(t *testing.T) {
	t0 := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	m := linkMonitor{threshold: 30 * time.Minute}
	m.update(hc.LinkStatus{State: hc.LinkRefused, Since: t0})
	m.update(hc.LinkStatus{State: hc.LinkError, Since: t0.Add(10 * time.Minute), Upgraded: true})
	m.update(hc.LinkStatus{State: hc.LinkRefused, Since: t0.Add(20 * time.Minute)})
	if m.powerCycleNeeded(t0.Add(40 * time.Minute)) {
		t.Fatal("refusal before an accepted session still counted")
	}
	if !m.powerCycleNeeded(t0.Add(50 * time.Minute)) {
		t.Fatal("the new episode never latched")
	}
}

func TestLinkBookClientOptions(t *testing.T) {
	now := time.Now()
	lb := &linkBook{loaded: linkRecord{State: "refused", Since: now.Add(-time.Hour), LastDial: now}}
	var o hc.Options
	lb.clientOptions(&o, false)
	if o.ResumeBackoff != 5*time.Minute || o.NotBefore != now.Add(5*time.Minute) || o.OnDial == nil || o.Resume.State != "" {
		t.Fatalf("local-watch options %+v", o)
	}
	lb.clientOptions(&o, true)
	if o.Resume.State != hc.LinkRefused || !o.Resume.Since.Equal(now.Add(-time.Hour)) {
		t.Fatalf("service resume %+v", o.Resume)
	}
	// A session that had only just connected (a crash loop) is spaced like a
	// failure.
	crash := &linkBook{loaded: linkRecord{State: "connected", Since: now.Add(-20 * time.Second), LastDial: now.Add(-25 * time.Second)}}
	if nb, backoff := crash.spacing(); nb.Sub(crash.loaded.LastDial) != 30*time.Second || backoff != 30*time.Second {
		t.Fatalf("crash loop spacing %s / %s", nb.Sub(crash.loaded.LastDial), backoff)
	}
}
