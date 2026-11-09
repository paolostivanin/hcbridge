package main

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"
)

// Home Connect limits the bridge has to live within
// (https://api-docs.home-connect.com/general/#rate-limiting):
//   - 1000 calls per client and user per day, counted over a rolling window.
//     Once reached, everything (SSE reconnects included) is rejected with a
//     429 whose body announces "the remaining period of N seconds" - seen as
//     high as 81000. Rejected calls do not appear to count: the announced
//     countdown kept falling steadily across hourly rejected retries.
//   - 50 calls per minute.
//   - 10 successive error responses block the client for 10 minutes; every
//     request counts "regardless of success or failure".
//
// Quota tracks all three from the bridge's own requests and turns them into
// a polling tier (see poll.go). It replaces the old package-level
// rateLimitedUntil/apiCalls globals so every client shares one instance and
// tests can use a fresh one.
type Quota struct {
	mu           sync.Mutex
	calls        []time.Time // sent requests, oldest first
	blockedUntil time.Time   // a 429 block announced by Bosch
	consecErrs   int         // consecutive error responses
	lastErrAt    time.Time
}

const (
	perMinuteStop  = 40               // stay clear of the 50/min limit
	errorPauseRun  = 8                // stay clear of the 10-errors limit
	errorPauseSpan = 10 * time.Minute // how long that limit lasts
)

func NewQuota() *Quota { return &Quota{} }

// RecordCall notes a request that is about to be sent (REST or SSE connect).
// Requests short-circuited by a local block never get here.
func (q *Quota) RecordCall() {
	now := time.Now()
	q.mu.Lock()
	q.calls = append(q.calls, now)
	q.pruneLocked(now)
	q.mu.Unlock()
}

// RecordResult notes the HTTP status of a sent request. Any 2xx clears the
// error run; anything >= 400 extends it. Transport errors (no response) are
// not recorded: Bosch never saw them.
func (q *Quota) RecordResult(status int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	switch {
	case status >= 200 && status < 300:
		q.consecErrs = 0
	case status >= 400:
		q.consecErrs++
		q.lastErrAt = time.Now()
	}
}

// Block records a 429 block lasting until `until`.
func (q *Quota) Block(until time.Time) {
	q.mu.Lock()
	q.blockedUntil = until
	q.mu.Unlock()
}

// Blocked reports whether a 429 block is in effect and when it ends.
func (q *Quota) Blocked() (bool, time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return time.Now().Before(q.blockedUntil), q.blockedUntil
}

// Count24h returns the number of requests sent in the last 24 hours.
func (q *Quota) Count24h() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pruneLocked(time.Now())
	return len(q.calls)
}

// countSince returns the number of requests sent in the last d.
func (q *Quota) countSince(d time.Duration) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	cutoff := time.Now().Add(-d)
	n := 0
	for i := len(q.calls) - 1; i >= 0 && q.calls[i].After(cutoff); i-- {
		n++
	}
	return n
}

// errorPaused reports whether the bridge should hold off because its recent
// responses were a run of errors close to Bosch's 10-error block.
func (q *Quota) errorPaused() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consecErrs >= errorPauseRun && time.Since(q.lastErrAt) < errorPauseSpan
}

func (q *Quota) pruneLocked(now time.Time) {
	cutoff := now.Add(-24 * time.Hour)
	i := 0
	for i < len(q.calls) && !q.calls[i].After(cutoff) {
		i++
	}
	if i > 0 {
		q.calls = append(q.calls[:0], q.calls[i:]...)
	}
}

type quotaFile struct {
	Calls        []time.Time `json:"calls"`
	BlockedUntil time.Time   `json:"blocked_until"`
}

// Save persists the call log and any active block so a restart neither
// starts counting from zero (and walks into a Bosch block) nor retries
// through a block it already knows about.
func (q *Quota) Save(path string) error {
	q.mu.Lock()
	q.pruneLocked(time.Now())
	data, err := json.Marshal(quotaFile{Calls: q.calls, BlockedUntil: q.blockedUntil})
	q.mu.Unlock()
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0o600)
}

// Load restores a saved call log. A missing file is not an error.
func (q *Quota) Load(path string) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var f quotaFile
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}
	now := time.Now()
	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls = q.calls[:0]
	for _, t := range f.Calls {
		if !t.After(now) { // ignore timestamps from a clock that was ahead
			q.calls = append(q.calls, t)
		}
	}
	q.pruneLocked(now)
	if f.BlockedUntil.After(q.blockedUntil) {
		q.blockedUntil = f.BlockedUntil
	}
	return nil
}
