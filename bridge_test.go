package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestApplyOptionsPublishesFocusedZone(t *testing.T) {
	b := newTestBridge()
	if !apply(b, []apiKV{
		{Key: "Cooking.Hob.Option.ZoneSelector", Value: "Cooking.Hob.EnumType.ZoneSelector.FrontRight"},
		{Key: "Cooking.Hob.Option.PowerLevel", Value: "Cooking.Hob.EnumType.PowerLevel.65"},
		{Key: "Cooking.Hob.Option.JoinZone", Value: false},
		{Key: "BSH.Common.Option.ElapsedProgramTime", Value: float64(90)},
	}, true) {
		t.Fatal("first poll reported no change")
	}
	want := map[string]string{
		"status/focused_zone":         "FrontRight",
		"zone/front_right/power":      "6.5",
		"joined_right":                "false",
		"status/elapsed_program_time": "90",
	}
	for k, v := range want {
		if got, _ := b.get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	for _, gone := range []string{"status/focused_zone_power", "status/focused_zone_joined"} {
		if _, ok := b.get(gone); ok {
			t.Errorf("removed topic %s published", gone)
		}
	}
	if apply(b, []apiKV{
		{Key: "Cooking.Hob.Option.ZoneSelector", Value: "Cooking.Hob.EnumType.ZoneSelector.FrontRight"},
		{Key: "Cooking.Hob.Option.PowerLevel", Value: "Cooking.Hob.EnumType.PowerLevel.65"},
	}, true) {
		t.Error("identical poll reported a change")
	}
}

func TestApplyOptionsSelectedSkipsProgramOptions(t *testing.T) {
	b := newTestBridge()
	apply(b, []apiKV{
		{Key: "Cooking.Hob.Option.ZoneSelector", Value: "Cooking.Hob.EnumType.ZoneSelector.RearLeft"},
		{Key: "BSH.Common.Option.Duration", Value: float64(600)},
	}, false)
	if got, _ := b.get("status/focused_zone"); got != "RearLeft" {
		t.Errorf("focused_zone = %q, want RearLeft", got)
	}
	if got, _ := b.get("zone/rear_left/power"); got != "off" {
		t.Errorf("rear_left = %q, want off (no PowerLevel)", got)
	}
	if _, ok := b.get("status/program_duration"); ok {
		t.Error("program_duration published from /selected")
	}
}

func TestApplyOptionsMirrorsJoinedPartner(t *testing.T) {
	b := newTestBridge()
	apply(b, []apiKV{
		{Key: "Cooking.Hob.Option.ZoneSelector", Value: "Cooking.Hob.EnumType.ZoneSelector.FrontLeft"},
		{Key: "Cooking.Hob.Option.PowerLevel", Value: "Cooking.Hob.EnumType.PowerLevel.40"},
		{Key: "Cooking.Hob.Option.JoinZone", Value: true},
	}, true)
	if got, _ := b.get("zone/rear_left/power"); got != "4" {
		t.Errorf("partner rear_left = %q, want 4", got)
	}
	apply(b, []apiKV{
		{Key: "Cooking.Hob.Option.ZoneSelector", Value: "Cooking.Hob.EnumType.ZoneSelector.FrontLeft"},
		{Key: "Cooking.Hob.Option.PowerLevel", Value: "Cooking.Hob.EnumType.PowerLevel.40"},
		{Key: "Cooking.Hob.Option.JoinZone", Value: false},
	}, true)
	if got, _ := b.get("zone/rear_left/power"); got != "off" {
		t.Errorf("partner after un-join = %q, want off", got)
	}
}

func TestInFlightPollDroppedAfterPowerOff(t *testing.T) {
	b := newTestBridge()
	epoch := b.zoneEpoch.Load() // poll starts its GET
	b.applyKV("BSH.Common.Setting.PowerState", "BSH.Common.EnumType.PowerState.Off")
	b.applyOptions([]apiKV{ // ...and its response lands after the power-off
		{Key: "Cooking.Hob.Option.ZoneSelector", Value: "Cooking.Hob.EnumType.ZoneSelector.FrontLeft"},
		{Key: "Cooking.Hob.Option.PowerLevel", Value: "Cooking.Hob.EnumType.PowerLevel.50"},
	}, true, epoch)
	if got, _ := b.get("zone/front_left/power"); got != "off" {
		t.Fatalf("front_left = %q after power-off, want off", got)
	}
}

func TestPowerOnAgainAcceptsNewPolls(t *testing.T) {
	b := newTestBridge()
	b.applyKV("BSH.Common.Setting.PowerState", "BSH.Common.EnumType.PowerState.Off")
	b.applyKV("BSH.Common.Setting.PowerState", "BSH.Common.EnumType.PowerState.On")
	apply(b, []apiKV{
		{Key: "Cooking.Hob.Option.ZoneSelector", Value: "Cooking.Hob.EnumType.ZoneSelector.RearRight"},
		{Key: "Cooking.Hob.Option.PowerLevel", Value: "Cooking.Hob.EnumType.PowerLevel.90"},
	}, true)
	if got, _ := b.get("zone/rear_right/power"); got != "9" {
		t.Fatalf("rear_right = %q, want 9", got)
	}
}

func TestUnknownZoneIgnored(t *testing.T) {
	b := newTestBridge()
	apply(b, []apiKV{{Key: "Cooking.Hob.Option.ZoneSelector", Value: nil}}, true)
	if _, ok := b.get("zone/<nil>/power"); ok {
		t.Fatal("published a zone/<nil>/power topic")
	}
	if _, ok := b.get("status/focused_zone"); ok {
		t.Fatal("published focused_zone for a null ZoneSelector")
	}
}

func TestApplianceConnectedEvents(t *testing.T) {
	b := newTestBridge()
	b.ApplyEvent(SSEEvent{Event: "DISCONNECTED"})
	if got, _ := b.get("status/appliance_online"); got != "false" || b.CanPoll() {
		t.Fatalf("after DISCONNECTED: online=%q canPoll=%v", got, b.CanPoll())
	}
	b.ApplyEvent(SSEEvent{Event: "CONNECTED"})
	if got, _ := b.get("status/appliance_online"); got != "true" || !b.CanPoll() {
		t.Fatalf("after CONNECTED: online=%q canPoll=%v", got, b.CanPoll())
	}
}

func TestStandbyIsNotActive(t *testing.T) {
	b := newTestBridge()
	b.applyKV("BSH.Common.Setting.PowerState", "BSH.Common.EnumType.PowerState.Standby")
	if b.IsHobActive() {
		t.Fatal("Standby counted as active")
	}
}

func TestSnapshotDoesNotOverwriteNewerEvent(t *testing.T) {
	b := newTestBridge()
	b.applyKV("BSH.Common.Setting.PowerState", "BSH.Common.EnumType.PowerState.Off")
	startedAt := time.Now()
	time.Sleep(time.Millisecond)
	// PowerState=On arrives over SSE while the snapshot request is in flight.
	b.ApplyEvent(SSEEvent{Event: "STATUS", Items: []SSEItem{{Key: "BSH.Common.Setting.PowerState", Value: "BSH.Common.EnumType.PowerState.On"}}})
	b.ApplySnapshot(map[string]interface{}{
		"BSH.Common.Setting.PowerState":    "BSH.Common.EnumType.PowerState.Off", // stale
		"BSH.Common.Status.OperationState": "BSH.Common.EnumType.OperationState.Ready",
	}, true, startedAt)
	if got, _ := b.get("status/power_state"); got != "On" {
		t.Fatalf("snapshot overwrote a newer event: power_state = %q", got)
	}
	if got, _ := b.get("status/operation_state"); got != "Ready" {
		t.Fatalf("snapshot keys without newer events not applied: %q", got)
	}
}

func TestSnapshotOfflineAppliance(t *testing.T) {
	b := newTestBridge()
	b.ApplySnapshot(map[string]interface{}{}, false, time.Now())
	if got, _ := b.get("status/appliance_online"); got != "false" {
		t.Fatalf("appliance_online = %q, want false", got)
	}
}

func TestProgramTimerJitter(t *testing.T) {
	b := newTestBridge()
	if !b.updateProgramTimer(600) {
		t.Fatal("first deadline not published")
	}
	if b.updateProgramTimer(599) {
		t.Error("1 s wobble republished")
	}
	if !b.updateProgramTimer(300) {
		t.Error("real change not published")
	}
	if !b.updateProgramTimer(0) {
		t.Error("timer clear not published")
	}
	if got, _ := b.get("status/program_timer_deadline"); got != "" {
		t.Errorf("cleared deadline = %q", got)
	}
}

func TestRemainingProgramTimeOnlyFeedsTimer(t *testing.T) {
	b := newTestBridge()
	b.applyKV("BSH.Common.Option.RemainingProgramTime", float64(120))
	if got, _ := b.get("status/program_timer_deadline"); got == "" {
		t.Error("no deadline from RemainingProgramTime")
	}
	if _, ok := b.get("status/remaining_program_time"); ok {
		t.Error("removed remaining_program_time topic published")
	}
}

func TestFailedPublishIsRetried(t *testing.T) {
	b := newTestBridge()
	if !b.publish("status/x", "1") {
		t.Fatal("first publish skipped")
	}
	if b.publish("status/x", "1") {
		t.Fatal("unchanged value republished")
	}
	b.onFail("status/x", "1")() // the broker never acked it
	if !b.publish("status/x", "1") {
		t.Fatal("value from a failed publish was deduped")
	}
}

func TestPollEndpointSelection(t *testing.T) {
	var activeCode atomic.Int32
	activeCode.Store(200)
	srv, cfg, auth, q := newAPIServer(t, func(w http.ResponseWriter, r *http.Request) {
		if c := int(activeCode.Load()); strings.HasSuffix(r.URL.Path, "/active/options") && c != 200 {
			w.WriteHeader(c)
			return
		}
		fmt.Fprint(w, `{"data":{"options":[{"key":"Cooking.Hob.Option.ZoneSelector","value":"Cooking.Hob.EnumType.ZoneSelector.FrontLeft"}]}}`)
	})
	b := NewBridge(cfg, NewAPIClient(cfg, auth, q), quietLogger(), "HAID")
	b.out = io.Discard
	b.applyKV("BSH.Common.Setting.PowerState", "BSH.Common.EnumType.PowerState.On")

	b.applyKV("BSH.Common.Status.OperationState", "BSH.Common.EnumType.OperationState.Ready")
	b.PollActiveOptions(context.Background())
	b.applyKV("BSH.Common.Status.OperationState", "BSH.Common.EnumType.OperationState.Run")
	b.PollActiveOptions(context.Background())
	activeCode.Store(404)
	b.PollActiveOptions(context.Background())

	got := srv.Paths()
	want := []string{"selected/options", "active/options", "active/options", "selected/options"}
	if len(got) != len(want) {
		t.Fatalf("paths %v, want suffixes %v", got, want)
	}
	for i := range want {
		if !strings.HasSuffix(got[i], want[i]) {
			t.Fatalf("request %d = %s, want .../%s (all: %v)", i, got[i], want[i], got)
		}
	}

	b.ApplyEvent(SSEEvent{Event: "DISCONNECTED"})
	b.PollActiveOptions(context.Background())
	if n := len(srv.Paths()); n != len(want) {
		t.Fatalf("polled a disconnected appliance (%d requests)", n)
	}
}
