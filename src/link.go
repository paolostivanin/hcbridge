package main

import (
	"sync"
	"time"

	hc "hcbridge/src/internal/homeconnect"
)

// linkMonitor turns the client's link states into the power_cycle_needed
// signal. The hob's firmware sometimes refuses every local session (HTTP
// 503 on the upgrade) until it is power cycled; once the link has been
// refused for the threshold, the flag latches on. It stays on through
// "unreachable" (what the breaker being off looks like) and clears only
// when a session connects, so "on" means "go flip the breaker" and "off
// again" means "it worked".
//
// What counts is refusal with no session accepted in between. An "error"
// attempt that failed before the upgrade was accepted (a dropped or garbled
// answer, a timeout) and "connecting" say nothing about whether the
// firmware still refuses, so they neither end nor restart the episode; the
// flag only latches while the state is refused. An attempt the appliance
// accepted (HTTP 101) but that failed later, "unreachable" (the hob is off
// the network) and a key error do end it.
type linkMonitor struct {
	threshold    time.Duration
	state        hc.LinkState
	since        time.Time
	refusedSince time.Time
	downSince    time.Time // start of the current run without a session; zero while connected
	latched      bool
}

func (m *linkMonitor) update(st hc.LinkStatus) {
	m.state, m.since = st.State, st.Since
	if st.State == hc.LinkConnected {
		m.downSince = time.Time{}
	} else if m.downSince.IsZero() {
		m.downSince = st.Since
	}
	switch st.State {
	case hc.LinkRefused:
		if m.refusedSince.IsZero() {
			m.refusedSince = st.Since
		}
	case hc.LinkConnected:
		m.refusedSince = time.Time{}
		m.latched = false
	case hc.LinkError:
		if st.Upgraded {
			m.refusedSince = time.Time{} // the appliance accepted a session: not refusing
		}
	case hc.LinkConnecting:
		// Neutral: the episode carries on.
	default:
		m.refusedSince = time.Time{}
	}
}

// powerCycleNeeded evaluates (and latches) the flag at now.
func (m *linkMonitor) powerCycleNeeded(now time.Time) bool {
	if !m.latched && m.state == hc.LinkRefused && !m.refusedSince.IsZero() && now.Sub(m.refusedSince) >= m.threshold {
		m.latched = true
	}
	return m.latched
}

func (m *linkMonitor) restore(r linkRecord) {
	m.state = hc.LinkState(r.State)
	m.since = r.Since
	m.refusedSince = r.RefusedSince
	m.downSince = r.downSince()
	m.latched = r.Latched
}

func (m *linkMonitor) record(lastDial time.Time) linkRecord {
	return linkRecord{State: string(m.state), Since: m.since, RefusedSince: m.refusedSince, DownSince: m.downSince, Latched: m.latched, LastDial: lastDial}
}

// Dial spacing carried over from link.json (see linkBook.spacing).
const (
	resumeGapConnected = 5 * time.Second // after a run that ended connected
	resumeGapMin       = 30 * time.Second
	resumeGapMax       = 5 * time.Minute // the client's backoff cap
	// A session younger than this when the record was read probably ended
	// with the process (a crash loop): it is spaced like a failure.
	resumeUnstable = 2 * time.Minute
)

// linkBook is the link episode the two local clients share through
// link.json: the power-cycle monitor and the last dial. The service and
// local-watch (never both at once: the state-dir lock) each load it at
// start and write it on every dial and link change, so the spacing of dials
// and the refusal episode carry over from one to the other, and across
// restarts.
type linkBook struct {
	dir string
	log *Logger

	// mu is held across every decision, its link.json write and (in the
	// service) its publish, so the three happen in the same order for every
	// goroutine; it also keeps the writes in order.
	mu       sync.Mutex
	mon      linkMonitor
	lastDial time.Time
	loaded   linkRecord // as read at start
}

func openLinkBook(dir string, threshold time.Duration, log *Logger) *linkBook {
	rec, err := loadLinkRecord(dir)
	if err != nil {
		log.Warn("read link.json: %v (starting fresh)", err)
		rec = linkRecord{}
	}
	now := time.Now()
	if rec.LastDial.After(now) {
		rec.LastDial = now // a clock that was ahead must not hold the first dial back
	}
	lb := &linkBook{dir: dir, log: log, mon: linkMonitor{threshold: threshold}, lastDial: rec.LastDial, loaded: rec}
	lb.mon.restore(rec)
	// The refusal clock only runs while a client watches: the time since
	// the last dial (the bridge stopped, or between the service and
	// local-watch) is not counted. A refusal seen before a long stop must
	// not raise the flag at startup against a hob that may have recovered.
	if !lb.mon.latched && !lb.mon.refusedSince.IsZero() && !rec.LastDial.IsZero() {
		lb.mon.refusedSince = lb.mon.refusedSince.Add(now.Sub(rec.LastDial))
	}
	return lb
}

// saveLocked writes link.json; call with mu held.
func (lb *linkBook) saveLocked() {
	if err := saveLinkRecord(lb.dir, lb.mon.record(lb.lastDial)); err != nil {
		lb.log.Warn("write link.json: %v", err)
	}
}

func (lb *linkBook) save() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.saveLocked()
}

// onDial records a dial attempt (hc.Options.OnDial).
func (lb *linkBook) onDial(at time.Time) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.lastDial = at
	lb.saveLocked()
}

// record applies and persists a link change, for local-watch; the service
// does the same under mu and publishes too.
func (lb *linkBook) record(st hc.LinkStatus, now time.Time) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.mon.update(st)
	lb.mon.powerCycleNeeded(now)
	lb.saveLocked()
}

// spacing is when the first dial may happen and the backoff to carry on
// with, from the record loaded at start: at least resumeGapConnected after
// the last dial, and after failed attempts as far apart as their retries
// had grown by then (the time without a session up to the last dial, from
// resumeGapMin up to the backoff cap). So neither a restart nor local-watch
// after the service (nor the service after local-watch) starts the backoff
// over at 5 s against a hob that refuses. A session that had only just
// connected when the record was read (a crash loop) counts as a failure.
func (lb *linkBook) spacing() (notBefore time.Time, backoff time.Duration) {
	r := lb.loaded
	if r.LastDial.IsZero() {
		return time.Time{}, 0
	}
	if r.State == "" || hc.LinkState(r.State) == hc.LinkConnected {
		if time.Since(r.Since) < resumeUnstable {
			return r.LastDial.Add(resumeGapMin), resumeGapMin
		}
		return r.LastDial.Add(resumeGapConnected), 0
	}
	gap := min(max(r.LastDial.Sub(r.downSince()), resumeGapMin), resumeGapMax)
	return r.LastDial.Add(gap), gap
}

// clientOptions sets the dial spacing and the dial hook in o; with resume,
// the client also continues the loaded episode's state (the service: its
// retries in that state stay silent and keep the episode's start time).
func (lb *linkBook) clientOptions(o *hc.Options, resume bool) {
	o.NotBefore, o.ResumeBackoff = lb.spacing()
	o.OnDial = lb.onDial
	if resume {
		o.Resume = hc.LinkStatus{State: hc.LinkState(lb.loaded.State), Since: lb.loaded.Since}
	}
}
