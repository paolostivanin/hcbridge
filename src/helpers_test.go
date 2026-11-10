package main

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	hc "hcbridge/src/internal/homeconnect"
)

func quietLogger() *Logger { return NewLogger("/dev/null", "ERROR") }

const testConfigBase = "appliance:\n  host: 192.0.2.1\n  profile: /nonexistent/profile.zip\n"

func testConfig() *Config {
	cfg, err := parseConfig([]byte(testConfigBase))
	if err != nil {
		panic(err)
	}
	return cfg
}

// newTestBridge returns a dry-run bridge (no MQTT, output discarded).
func newTestBridge() *Bridge {
	b := NewBridge(testConfig(), deviceMeta{}, quietLogger())
	b.out = io.Discard
	b.powerOffDelay = 0 // reset synchronously; the graced path has its own tests
	return b
}

// capture redirects the dry-run output of b into a buffer.
func capture(b *Bridge) *syncBuffer {
	buf := &syncBuffer{}
	b.out = buf
	return buf
}

func (b *Bridge) get(suffix string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.lastPub[suffix]
	return v, ok
}

// Resolved values as the client delivers them.
func sym(name, symbol string) hc.Value    { return hc.Value{UID: 1, Name: name, Symbol: symbol} }
func num(name string, v float64) hc.Value { return hc.Value{UID: 1, Name: name, V: v} }
func boolv(name string, v bool) hc.Value  { return hc.Value{UID: 1, Name: name, V: v} }

func apply(b *Bridge, vals ...hc.Value) { b.ApplyValues(vals, false) }

func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
