package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sseHarness serves a scripted sequence of responses on /events.
func sseHarness(t *testing.T, steps []func(w http.ResponseWriter)) (*SSEClient, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(hits.Add(1)) - 1
		if i >= len(steps) {
			w.WriteHeader(503)
			return
		}
		steps[i](w)
	}))
	t.Cleanup(srv.Close)
	cfg := testConfig()
	cfg.apiBase = srv.URL
	auth := NewAuthenticator(cfg, quietLogger())
	auth.token = &Token{AccessToken: "x", RefreshToken: "y", ExpiresAt: time.Now().Add(time.Hour)}
	s := NewSSEClient(cfg, auth, NewQuota(), quietLogger())
	s.backoffInitial = 10 * time.Millisecond
	s.backoffMax = 40 * time.Millisecond
	return s, &hits
}

func okStream(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(200)
	fmt.Fprint(w, "event: STATUS\ndata: {\"haId\":\"HAID\",\"items\":[{\"key\":\"BSH.Common.Status.OperationState\",\"value\":\"BSH.Common.EnumType.OperationState.Ready\"}]}\n\n")
	// returning closes the stream -> EOF
}

func failStream(w http.ResponseWriter) { w.WriteHeader(503) }

// runSSE runs s until cond holds (or 2 s), then stops it and returns the
// events the handler saw.
func runSSE(t *testing.T, s *SSEClient, cond func() bool) []string {
	t.Helper()
	var mu sync.Mutex
	var events []string
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx, "HAID", func(ev SSEEvent) {
			mu.Lock()
			events = append(events, ev.Event)
			mu.Unlock()
		})
		close(done)
	}()
	waitFor(t, 2*time.Second, cond)
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	return append([]string(nil), events...)
}

func count(events []string, name string) int {
	n := 0
	for _, e := range events {
		if e == name {
			n++
		}
	}
	return n
}

func TestSSEStreamEventsOnlyForEstablishedSessions(t *testing.T) {
	s, hits := sseHarness(t, []func(http.ResponseWriter){okStream, failStream, failStream, okStream})
	events := runSSE(t, s, func() bool { return hits.Load() >= 4 })
	if up, down := count(events, "STREAM_UP"), count(events, "STREAM_DOWN"); up != 2 || down != 2 {
		t.Fatalf("STREAM_UP=%d STREAM_DOWN=%d, want 2/2; events=%v", up, down, events)
	}
	if s.quota.Count24h() < 4 {
		t.Fatalf("SSE connects not counted: %d", s.quota.Count24h())
	}
}

func TestSSEFlappingStreamKeepsBackingOff(t *testing.T) {
	steps := make([]func(http.ResponseWriter), 200)
	for i := range steps {
		steps[i] = okStream // 200, one event, immediate close
	}
	s, hits := sseHarness(t, steps)
	s.backoffMax = 80 * time.Millisecond
	s.minStableSession = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx, "HAID", func(SSEEvent) {}); close(done) }()
	time.Sleep(500 * time.Millisecond)
	cancel()
	<-done
	// waits 10+20+40+80+80+... ms: about 8 connects in 500 ms. Resetting
	// the backoff on every 200 reached ~40.
	if n := hits.Load(); n > 12 {
		t.Fatalf("flapping stream reconnected %d times in 500 ms; backoff is resetting", n)
	}
}

func TestSSE429SetsBlockAndStopsReconnecting(t *testing.T) {
	limited := func(w http.ResponseWriter) {
		w.WriteHeader(429)
		fmt.Fprint(w, `{"error":{"key":"429","description":"Requests are blocked during the remaining period of 3600 seconds."}}`)
	}
	s, hits := sseHarness(t, []func(http.ResponseWriter){limited, okStream})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx, "HAID", func(SSEEvent) {}); close(done) }()
	time.Sleep(200 * time.Millisecond) // ~20 backoff periods
	cancel()
	<-done
	if blocked, until := s.quota.Blocked(); !blocked || time.Until(until) < 59*time.Minute {
		t.Fatalf("block not set from SSE 429: blocked=%v until=%s", blocked, until)
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("SSE reconnected %d times during the block, want 1 attempt", n)
	}
}

// stable keeps the stream open for 50 ms before closing it.
func stable(next func(http.ResponseWriter)) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		next(w)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestSSEBackoffResetsAfterStableSession(t *testing.T) {
	var mu sync.Mutex
	var at []time.Time
	stamp := func(next func(http.ResponseWriter)) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			mu.Lock()
			at = append(at, time.Now())
			mu.Unlock()
			next(w)
		}
	}
	// fail x3 grows the backoff to 80 ms; a 50 ms session counts as stable
	// (minStableSession 30 ms) and the next reconnect waits 20 ms again.
	s, hits := sseHarness(t, []func(http.ResponseWriter){stamp(failStream), stamp(failStream), stamp(failStream), stamp(stable(okStream)), stamp(okStream)})
	s.backoffInitial = 20 * time.Millisecond
	s.backoffMax = 200 * time.Millisecond
	s.minStableSession = 30 * time.Millisecond
	runSSE(t, s, func() bool { return hits.Load() >= 5 })
	mu.Lock()
	defer mu.Unlock()
	if len(at) < 5 {
		t.Fatalf("only %d attempts", len(at))
	}
	if gap := at[3].Sub(at[2]); gap < 70*time.Millisecond {
		t.Fatalf("backoff did not grow: %s", gap)
	}
	if gap := at[4].Sub(at[3]) - 50*time.Millisecond; gap > 60*time.Millisecond {
		t.Fatalf("backoff not reset after a stable session: %s", gap)
	}
}

func TestSSEStalledStreamIsDropped(t *testing.T) {
	silent := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(2 * time.Second) // headers, then nothing
	}
	s, hits := sseHarness(t, []func(http.ResponseWriter){silent, okStream})
	s.idleTimeout = 50 * time.Millisecond
	events := runSSE(t, s, func() bool { return hits.Load() >= 2 })
	if hits.Load() < 2 {
		t.Fatal("stalled stream was never abandoned")
	}
	if count(events, "STREAM_DOWN") < 1 {
		t.Fatalf("no STREAM_DOWN after the stall; events=%v", events)
	}
}

func TestReadLineCap(t *testing.T) {
	br := bufio.NewReaderSize(strings.NewReader(strings.Repeat("a", 100)+"\nok\n"), 16)
	if _, err := readLine(br, 50); err == nil {
		t.Fatal("over-long line accepted")
	}
	br = bufio.NewReaderSize(strings.NewReader(strings.Repeat("b", 40)+"\n"), 16)
	line, err := readLine(br, 50)
	if err != nil || len(line) != 41 {
		t.Fatalf("line across buffer refills: %q %v", line, err)
	}
}
