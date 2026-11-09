package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func quietLogger() *Logger { return NewLogger("/dev/null", "ERROR") }

func testConfig() *Config {
	cfg, err := parseConfig([]byte("oauth:\n  client_id: test\nappliance:\n  ha_id: HAID\n"))
	if err != nil {
		panic(err)
	}
	return cfg
}

// newTestBridge returns a dry-run bridge (no MQTT, output discarded) whose
// hob is powered on.
func newTestBridge() *Bridge {
	b := NewBridge(testConfig(), nil, quietLogger(), "HAID")
	b.out = io.Discard
	b.applyKV("BSH.Common.Setting.PowerState", "BSH.Common.EnumType.PowerState.On")
	return b
}

// apply runs applyOptions as a poll that started now (current epoch).
func apply(b *Bridge, items []apiKV, fromActive bool) bool {
	return b.applyOptions(items, fromActive, b.zoneEpoch.Load())
}

func (b *Bridge) get(suffix string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.lastPub[suffix]
	return v, ok
}

// apiServer is an httptest server that records request paths and answers
// from a handler; it returns an APIClient and Authenticator wired to it.
type apiServer struct {
	mu    sync.Mutex
	paths []string
	srv   *httptest.Server
}

func newAPIServer(t *testing.T, h func(w http.ResponseWriter, r *http.Request)) (*apiServer, *Config, *Authenticator, *Quota) {
	t.Helper()
	s := &apiServer{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.paths = append(s.paths, r.URL.Path)
		s.mu.Unlock()
		h(w, r)
	}))
	t.Cleanup(s.srv.Close)
	cfg := testConfig()
	cfg.apiBase = s.srv.URL
	cfg.OAuth.TokenFile = t.TempDir() + "/token.json"
	auth := NewAuthenticator(cfg, quietLogger())
	auth.token = &Token{AccessToken: "x", RefreshToken: "y", ExpiresAt: time.Now().Add(time.Hour)}
	return s, cfg, auth, NewQuota()
}

func (s *apiServer) Paths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.paths...)
}

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
