package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPeriodicPollDue(t *testing.T) {
	now := time.Now()
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	cases := []struct {
		name     string
		lastPoll time.Time
		lastEdge time.Time
		tier     budgetTier
		want     bool
	}{
		{"recent edge, 30 s since poll", ago(30 * time.Second), ago(time.Minute), tierNormal, true},
		{"recent edge, 20 s since poll", ago(20 * time.Second), ago(time.Minute), tierNormal, false},
		{"quiet hob, 60 s since poll", ago(time.Minute), ago(10 * time.Minute), tierNormal, false},
		{"quiet hob, 3 min since poll", ago(3 * time.Minute), ago(10 * time.Minute), tierNormal, true},
		{"slow tier stretches active cadence", ago(time.Minute), ago(time.Minute), tierSlow, false},
		{"slow tier, 2 min since poll", ago(2 * time.Minute), ago(time.Minute), tierSlow, true},
		{"slow tier keeps the idle cadence", ago(2 * time.Minute), ago(10 * time.Minute), tierSlow, false},
		{"stop tier never polls", ago(time.Hour), ago(time.Second), tierStop, false},
		{"tick jitter tolerated", ago(30*time.Second - time.Second), ago(time.Minute), tierNormal, true},
	}
	for _, c := range cases {
		if got := periodicPollDue(now, c.lastPoll, c.lastEdge, c.tier); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestFastPollerCapsDuration(t *testing.T) {
	var f fastPoller
	var ticks atomic.Int32
	f.Start(context.Background(), 5*time.Millisecond, 40*time.Millisecond, func(context.Context) { ticks.Add(1) })
	if !f.Active() {
		t.Fatal("not active right after Start")
	}
	if !waitFor(t, time.Second, func() bool { return !f.Active() }) {
		t.Fatal("still active long after maxDur")
	}
	n := ticks.Load()
	if n == 0 {
		t.Fatal("never ticked")
	}
	time.Sleep(30 * time.Millisecond)
	if ticks.Load() != n {
		t.Fatal("kept ticking after maxDur")
	}
}

func TestFastPollerOldRunDoesNotClearNew(t *testing.T) {
	var f fastPoller
	inTick := make(chan struct{})
	release := make(chan struct{})
	first := true
	// Run 1 is stuck inside its first tick when it is stopped and replaced.
	f.Start(context.Background(), time.Millisecond, time.Hour, func(context.Context) {
		if first {
			first = false
			close(inTick)
			<-release
		}
	})
	select {
	case <-inTick:
	case <-time.After(time.Second):
		t.Fatal("run 1 never ticked")
	}
	f.Stop()
	f.Start(context.Background(), time.Hour, time.Hour, func(context.Context) {})
	close(release) // run 1 returns and calls finish with its old generation
	time.Sleep(20 * time.Millisecond)
	if !f.Active() {
		t.Fatal("run 1 finishing cleared run 2")
	}
	f.Stop()
}
