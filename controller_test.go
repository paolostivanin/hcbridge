package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

func newTestController(t *testing.T, h func(w http.ResponseWriter, r *http.Request)) (*controller, *apiServer) {
	t.Helper()
	srv, cfg, auth, q := newAPIServer(t, h)
	api := NewAPIClient(cfg, auth, q)
	b := NewBridge(cfg, api, quietLogger(), "HAID")
	b.out = io.Discard
	c := newController(cfg, api, auth, q, b, quietLogger(), false)
	c.out = io.Discard
	return c, srv
}

func TestCloudGraceSurvivesQuickReconnect(t *testing.T) {
	c, _ := newTestController(t, func(w http.ResponseWriter, r *http.Request) {})
	c.offlineGrace = 50 * time.Millisecond
	c.streamUpNow()
	c.streamDown()
	time.Sleep(10 * time.Millisecond)
	c.streamUpNow() // routine reconnect inside the grace
	time.Sleep(80 * time.Millisecond)
	if got, _ := c.bridge.get("cloud"); got != "online" {
		t.Fatalf("cloud = %q after a reconnect inside the grace, want online", got)
	}
	c.streamDown()
	if !waitFor(t, time.Second, func() bool { v, _ := c.bridge.get("cloud"); return v == "offline" }) {
		t.Fatal("cloud never went offline after the grace")
	}
}

func TestStreamUpResyncThrottle(t *testing.T) {
	c, srv := newTestController(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"status":[],"settings":[]}}`)
	})
	ctx := context.Background()
	c.handleEvent(ctx, SSEEvent{Event: "STREAM_UP"}) // no resync yet: resyncs
	n := len(srv.Paths())
	if n == 0 {
		t.Fatal("first STREAM_UP did not resync")
	}
	c.handleEvent(ctx, SSEEvent{Event: "STREAM_UP"}) // flapping: throttled
	if len(srv.Paths()) != n {
		t.Fatal("second STREAM_UP within the gap resynced again")
	}
	if got, _ := c.bridge.get("cloud"); got != "online" {
		t.Fatalf("cloud = %q after STREAM_UP", got)
	}
}

func TestApplianceReconnectResyncs(t *testing.T) {
	c, srv := newTestController(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"status":[],"settings":[]}}`)
	})
	c.handleEvent(context.Background(), SSEEvent{Event: "CONNECTED"})
	if len(srv.Paths()) == 0 {
		t.Fatal("appliance CONNECTED did not resync")
	}
}

func TestEdgeKeysAndFastPollGate(t *testing.T) {
	if isEdgeKey("BSH.Common.Option.RemainingProgramTime") || isEdgeKey("BSH.Common.Option.ProgramProgress") {
		t.Error("countdown keys counted as edges")
	}
	if !isEdgeKey("BSH.Common.Status.LocalControlActive") || !isEdgeKey("BSH.Common.Status.OperationState") {
		t.Error("real edges not counted")
	}

	c, _ := newTestController(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) })
	c.bridge.applyKV("BSH.Common.Setting.PowerState", "BSH.Common.EnumType.PowerState.On")
	lca := SSEEvent{Event: "STATUS", Items: []SSEItem{{Key: "BSH.Common.Status.LocalControlActive", Value: true}}}

	fillCalls(c.quota, budgetSlow, time.Now().Add(-time.Hour))
	c.handleEvent(context.Background(), lca)
	if c.fast.Active() {
		t.Fatal("fast poller started outside the normal tier")
	}

	// Back under budget. Clear in place: the poll goroutine started by the
	// first event may still be reading the quota.
	c.quota.mu.Lock()
	c.quota.calls = nil
	c.quota.mu.Unlock()
	c.handleEvent(context.Background(), lca)
	if !c.fast.Active() {
		t.Fatal("fast poller not started in the normal tier")
	}
	c.handleEvent(context.Background(), SSEEvent{Event: "STATUS", Items: []SSEItem{{Key: "BSH.Common.Setting.PowerState", Value: "BSH.Common.EnumType.PowerState.Off"}}})
	if c.fast.Active() {
		t.Fatal("fast poller survived PowerState=Off")
	}
	c.pollMu.Lock()
	edge := c.lastEdgeAt
	c.pollMu.Unlock()
	if time.Since(edge) > time.Second {
		t.Fatal("edge time not recorded")
	}
}
