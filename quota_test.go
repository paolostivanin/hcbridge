package main

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fillCalls(q *Quota, n int, at time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := 0; i < n; i++ {
		q.calls = append(q.calls, at)
	}
}

func TestQuotaRollingWindow(t *testing.T) {
	q := NewQuota()
	now := time.Now()
	q.calls = []time.Time{now.Add(-25 * time.Hour), now.Add(-24*time.Hour - time.Second), now.Add(-23 * time.Hour), now.Add(-time.Minute)}
	if got := q.Count24h(); got != 2 {
		t.Fatalf("Count24h = %d, want 2", got)
	}
	q.RecordCall()
	if got := q.Count24h(); got != 3 {
		t.Fatalf("after RecordCall = %d, want 3", got)
	}
}

func TestQuotaTiers(t *testing.T) {
	old := time.Now().Add(-2 * time.Hour) // inside 24 h, outside the minute
	for _, c := range []struct {
		n    int
		want budgetTier
	}{{0, tierNormal}, {budgetSlow - 1, tierNormal}, {budgetSlow, tierSlow}, {budgetStop - 1, tierSlow}, {budgetStop, tierStop}} {
		q := NewQuota()
		fillCalls(q, c.n, old)
		if got := currentTier(q); got != c.want {
			t.Errorf("%d calls: tier %s, want %s", c.n, got, c.want)
		}
	}
	q := NewQuota()
	fillCalls(q, perMinuteStop, time.Now())
	if got := currentTier(q); got != tierStop {
		t.Errorf("%d calls in the last minute: tier %s, want stop", perMinuteStop, got)
	}
}

func TestQuotaErrorPause(t *testing.T) {
	q := NewQuota()
	for i := 0; i < errorPauseRun-1; i++ {
		q.RecordResult(409)
	}
	if currentTier(q) == tierStop {
		t.Fatal("paused before the error run limit")
	}
	q.RecordResult(404)
	if currentTier(q) != tierStop {
		t.Fatal("not paused after an error run")
	}
	q.RecordResult(200)
	if currentTier(q) != tierNormal {
		t.Fatal("a success did not clear the error run")
	}
	for i := 0; i < errorPauseRun; i++ {
		q.RecordResult(500)
	}
	q.mu.Lock()
	q.lastErrAt = time.Now().Add(-errorPauseSpan - time.Second)
	q.mu.Unlock()
	if currentTier(q) != tierNormal {
		t.Fatal("error pause outlived its span")
	}
}

func TestQuotaSaveLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "calls.json")
	q := NewQuota()
	now := time.Now()
	q.calls = []time.Time{now.Add(-25 * time.Hour), now.Add(-time.Hour), now.Add(-time.Minute)}
	q.Block(now.Add(time.Hour))
	if err := q.Save(path); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %o, want 600", fi.Mode().Perm())
	}
	r := NewQuota()
	if err := r.Load(path); err != nil {
		t.Fatal(err)
	}
	if got := r.Count24h(); got != 2 {
		t.Errorf("loaded Count24h = %d, want 2", got)
	}
	if blocked, _ := r.Blocked(); !blocked {
		t.Error("persisted block not restored")
	}
	if err := NewQuota().Load(filepath.Join(t.TempDir(), "missing.json")); err != nil {
		t.Errorf("missing file: %v", err)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Errorf("temp files left behind: %v", entries)
	}
}

func TestRetryAfterSeconds(t *testing.T) {
	body := []byte(`{"error":{"key":"429","description":"The rate limit \"1000 calls in 1 day\" was reached. Requests are blocked during the remaining period of 54434 seconds."}}`)
	if got := retryAfterSeconds(http.Header{}, body); got != 54434 {
		t.Errorf("body form: got %d, want 54434", got)
	}
	h := http.Header{}
	h.Set("Retry-After", "120")
	if got := retryAfterSeconds(h, []byte(`{"error":{"description":"nope"}}`)); got != 120 {
		t.Errorf("header form: got %d, want 120", got)
	}
	if got := retryAfterSeconds(http.Header{}, []byte(`garbage`)); got != 0 {
		t.Errorf("unparseable: got %d, want 0", got)
	}
}
