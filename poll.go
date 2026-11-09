package main

import (
	"context"
	"sync"
	"time"
)

// Self-imposed API budget against Bosch's limits (see Quota). A single long
// cooking session at the original fixed 30 s cadence (720 calls in 6 h) was
// enough to hit the daily quota on its own, and once blocked the bridge is
// blind for up to a day. So polling degrades before Bosch blocks:
//
//   - normal: adaptive periodic poll (see periodicPollDue), 2 s fast poll
//     while the panel is touched, edge-triggered polls
//   - slow (budgetSlow calls in 24 h): periodic poll at most every
//     slowPollInterval, no fast poll; edge polls and resyncs continue
//   - stop (budgetStop calls in 24 h, perMinuteStop calls in the last
//     minute, or a run of error responses): no discretionary polls or
//     periodic resyncs; only the throttled resync after an SSE connect
//     still goes out
//
// The gap above budgetStop is the reserve for SSE reconnects and their
// resyncs, so the daily stream close never finds the quota exhausted.
const (
	budgetSlow       = 700
	budgetStop       = 900
	slowPollInterval = 2 * time.Minute
)

type budgetTier int

const (
	tierNormal budgetTier = iota
	tierSlow
	tierStop
)

func currentTier(q *Quota) budgetTier {
	n := q.Count24h()
	switch {
	case n >= budgetStop, q.countSince(time.Minute) >= perMinuteStop, q.errorPaused():
		return tierStop
	case n >= budgetSlow:
		return tierSlow
	}
	return tierNormal
}

func (t budgetTier) String() string {
	switch t {
	case tierSlow:
		return "slow"
	case tierStop:
		return "stop"
	}
	return "normal"
}

// Adaptive periodic poll. The cloud API never pushes the per-zone options
// (ZoneSelector / PowerLevel / JoinZone), so they must be polled, but a zone
// only changes after something SSE does report: a panel touch
// (LocalControlActive), a program start or end, an OperationState change.
// Right after such an edge the periodic poll runs every activePollInterval;
// once the hob has been quiet for edgeWindow it drops to idlePollInterval,
// which only guards against changes the hob makes on its own (boost
// timeout, pan removed). A 6 h simmer costs ~120 periodic calls instead of
// the original ~720.
const (
	activePollInterval = 30 * time.Second
	idlePollInterval   = 3 * time.Minute
	edgeWindow         = 3 * time.Minute
	pollTick           = 5 * time.Second // main loop granularity
)

// periodicPollDue decides whether the periodic poll should run at `now`,
// given the last poll of any kind, the last SSE edge and the budget tier.
// Pure, so the schedule is unit-testable.
func periodicPollDue(now, lastPoll, lastEdge time.Time, tier budgetTier) bool {
	if tier == tierStop {
		return false
	}
	interval := idlePollInterval
	if now.Sub(lastEdge) < edgeWindow {
		interval = activePollInterval
	}
	if tier == tierSlow && interval < slowPollInterval {
		interval = slowPollInterval
	}
	// Half a tick of slack so a ticker that fires a hair early does not
	// push the poll a whole tick late.
	return now.Sub(lastPoll) >= interval-pollTick/2
}

// Fast-poll cadence and lifetime. The fast poll used to run until an SSE
// LocalControlActive=false or PowerState=Off arrived; if that event was lost
// (stream drop, rate-limit block) it would poll every 2 s indefinitely, which
// alone exhausts the quota in under an hour. fastPollMax caps one activation;
// a new LocalControlActive=true edge starts a fresh one, and the periodic
// poll covers a panel that stays active longer.
const (
	fastPollEvery = 2 * time.Second
	fastPollMax   = 90 * time.Second
)

// fastPoller runs tick every `every` until Stop, the parent context ends, or
// maxDur elapses - whichever comes first. At most one run is active.
type fastPoller struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	gen    uint64
}

func (f *fastPoller) Start(parent context.Context, every, maxDur time.Duration, tick func(context.Context)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cancel != nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, maxDur)
	f.cancel = cancel
	f.gen++
	gen := f.gen
	go func() {
		defer f.finish(gen)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			tick(ctx)
		}
	}()
}

// finish clears the active run when it ends on its own (timeout or parent
// cancel). The generation check stops a run that was already replaced from
// clearing its successor.
func (f *fastPoller) finish(gen uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gen == gen && f.cancel != nil {
		f.cancel()
		f.cancel = nil
	}
}

func (f *fastPoller) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cancel != nil {
		f.cancel()
		f.cancel = nil
	}
}

func (f *fastPoller) Active() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancel != nil
}
