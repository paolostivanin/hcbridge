package main

import (
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

// An unreachable blip during a refusal episode restarts the clock: the
// episode that counts is continuous refusal.
func TestLinkMonitorNeedsContinuousRefusal(t *testing.T) {
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
