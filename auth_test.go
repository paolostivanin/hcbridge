package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func tokenServer(t *testing.T, h http.HandlerFunc) (*Authenticator, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	cfg := testConfig()
	cfg.apiBase = srv.URL
	cfg.OAuth.TokenFile = filepath.Join(t.TempDir(), "token.json")
	a := NewAuthenticator(cfg, quietLogger())
	return a, &hits
}

func okToken(w http.ResponseWriter, r *http.Request) {
	time.Sleep(30 * time.Millisecond) // widen the race window
	fmt.Fprint(w, `{"access_token":"new","refresh_token":"r2","expires_in":86400}`)
}

func TestRefreshIsSingleFlight(t *testing.T) {
	a, hits := tokenServer(t, okToken)
	a.token = &Token{AccessToken: "old", RefreshToken: "r1", ExpiresAt: time.Now().Add(time.Minute)} // inside refreshEarly
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tok, err := a.AccessToken(context.Background()); err != nil || tok != "new" {
				t.Errorf("AccessToken = %q, %v", tok, err)
			}
		}()
	}
	wg.Wait()
	if n := hits.Load(); n != 1 {
		t.Fatalf("%d refresh requests, want 1", n)
	}
}

func TestForceRefreshAfterSomeoneElseRefreshed(t *testing.T) {
	a, hits := tokenServer(t, okToken)
	a.token = &Token{AccessToken: "old", RefreshToken: "r1", ExpiresAt: time.Now().Add(time.Hour)}
	if _, err := a.ForceRefresh(context.Background(), "old"); err != nil {
		t.Fatal(err)
	}
	// A second caller whose request also failed with the old token.
	tok, err := a.ForceRefresh(context.Background(), "old")
	if err != nil || tok != "new" || hits.Load() != 1 {
		t.Fatalf("second ForceRefresh: tok=%q err=%v requests=%d", tok, err, hits.Load())
	}
}

func TestRejectedRefreshIsSuppressedAndValidTokenKept(t *testing.T) {
	a, hits := tokenServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		fmt.Fprint(w, `{"error":"invalid_grant","error_description":"revoked"}`)
	})
	a.token = &Token{AccessToken: "old", RefreshToken: "r1", ExpiresAt: time.Now().Add(time.Minute)}
	for i := 0; i < 3; i++ {
		tok, err := a.AccessToken(context.Background())
		if err != nil || tok != "old" {
			t.Fatalf("AccessToken while still valid = %q, %v", tok, err)
		}
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("%d refresh requests after a rejection, want 1", n)
	}
	a.token.ExpiresAt = time.Now().Add(-time.Second)
	if _, err := a.AccessToken(context.Background()); err == nil {
		t.Fatal("expired token with a rejected refresh returned no error")
	}
}

func TestTokenSaveIsPrivateAndClean(t *testing.T) {
	a, _ := tokenServer(t, okToken)
	path := a.cfg.OAuth.TokenFile
	// A stale world-readable temp file from an older version must not be reused.
	if err := os.WriteFile(path+".tmp", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := a.save(&Token{AccessToken: "a", RefreshToken: "b"}); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("token mode %o, want 600", fi.Mode().Perm())
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	for _, e := range entries {
		if e.Name() != "token.json" && e.Name() != "token.json.tmp" {
			t.Errorf("leftover file %s", e.Name())
		}
	}
}

func TestCheckTokenOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token.json")
	if err := checkTokenOwner(path); err != nil {
		t.Fatalf("missing file: %v", err)
	}
	if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkTokenOwner(path); err != nil {
		t.Fatalf("own file: %v", err)
	}
}
