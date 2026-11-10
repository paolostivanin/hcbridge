package main

import (
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
// What counts is refusal without a session in between. An "error" attempt
// (a dropped or garbled upgrade answer, a timeout) and "connecting" say
// nothing about whether the firmware still refuses, so they neither end nor
// restart the episode; the flag only latches while the state is refused.
// "unreachable" (the hob is off the network) and a key error do restart it.
type linkMonitor struct {
	threshold    time.Duration
	state        hc.LinkState
	since        time.Time
	refusedSince time.Time
	latched      bool
}

func (m *linkMonitor) update(st hc.LinkStatus) {
	m.state, m.since = st.State, st.Since
	switch st.State {
	case hc.LinkRefused:
		if m.refusedSince.IsZero() {
			m.refusedSince = st.Since
		}
	case hc.LinkConnected:
		m.refusedSince = time.Time{}
		m.latched = false
	case hc.LinkError, hc.LinkConnecting:
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
	m.latched = r.Latched
}

func (m *linkMonitor) record(lastDial time.Time) linkRecord {
	return linkRecord{State: string(m.state), Since: m.since, RefusedSince: m.refusedSince, Latched: m.latched, LastDial: lastDial}
}
