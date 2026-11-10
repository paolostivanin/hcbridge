package main

import (
	"strings"
	"testing"
	"time"

	hc "hcbridge/src/internal/homeconnect"
)

func want(t *testing.T, b *Bridge, suffix, value string) {
	t.Helper()
	if got, ok := b.get(suffix); !ok || got != value {
		t.Errorf("%s = %q (published %v), want %q", suffix, got, ok, value)
	}
}

func TestProgramLevelMapping(t *testing.T) {
	b := newTestBridge()
	apply(b,
		sym("BSH.Common.Status.OperationState", "Run"),
		sym("BSH.Common.Setting.PowerState", "On"),
		boolv("BSH.Common.Setting.ChildLock", true),
		boolv("BSH.Common.Status.LocalControlActive", false),
		sym("BSH.Common.Root.ActiveProgram", "Cooking.Hob.Program.PowerLevelMode"),
		num("BSH.Common.Option.Duration", 124),
		sym("Cooking.Hob.Option.ZoneSelector", "RearRight"),
		num("BSH.Common.Status.WiFiSignalStrength", -44),
	)
	want(t, b, "status/operation_state", "Run")
	want(t, b, "status/power_state", "On")
	want(t, b, "status/child_lock", "true")
	want(t, b, "status/local_control", "false")
	want(t, b, "status/active_program", "Cooking.Hob.Program.PowerLevelMode")
	want(t, b, "status/program_duration", "124")
	want(t, b, "status/focused_zone", "RearRight")
	want(t, b, "status/wifi_signal", "-44")

	// ActiveProgram 0 (no program) decodes to nil: an empty payload.
	b.ApplyValues([]hc.Value{{UID: 256, Name: "BSH.Common.Root.ActiveProgram"}}, false)
	want(t, b, "status/active_program", "")
}

func TestZonePowerAndState(t *testing.T) {
	b := newTestBridge()
	apply(b,
		sym("Cooking.Hob.Status.Zone.100.State", "Active"),
		sym("Cooking.Hob.Status.Zone.100.OperationState", "Run"),
		sym("Cooking.Hob.Status.Zone.100.PowerLevel", "65"),
		sym("Cooking.Hob.Status.Zone.300.PowerLevel", "Boost2"),
		sym("Cooking.Hob.Status.Zone.400.State", "ResiduelHeat"),
	)
	want(t, b, "zone/front_left/power", "6.5")
	want(t, b, "zone/front_left/state", "active")
	want(t, b, "zone/rear_right/power", "Boost2")
	want(t, b, "zone/front_right/state", "residual_heat")

	apply(b, sym("Cooking.Hob.Status.Zone.100.OperationState", "Pause"))
	want(t, b, "zone/front_left/state", "paused")
	apply(b, sym("Cooking.Hob.Status.Zone.100.OperationState", "Run"))
	want(t, b, "zone/front_left/state", "active")

	// Combined and venting zones carry nothing on this hob: ignored.
	apply(b, sym("Cooking.Hob.Status.Zone.120.PowerLevel", "50"), sym("Cooking.Hob.Status.Zone.F55.State", "Active"))
	for suffix := range b.lastPub {
		if strings.Contains(suffix, "120") || strings.Contains(suffix, "f55") {
			t.Errorf("unexpected topic %s", suffix)
		}
	}
}

// Observed on the PXX801D67E: a flex-joined pair is both base zones active
// at the same level, and Option.JoinZone true while the focus is on that side.
func TestFlexJoin(t *testing.T) {
	b := newTestBridge()
	join := func(on bool) { apply(b, boolv("Cooking.Hob.Option.JoinZone", on)) }

	apply(b, sym("Cooking.Hob.Option.ZoneSelector", "RearLeft"), sym("Cooking.Hob.Option.ZoneSelector", "FrontLeft"))
	join(true)
	want(t, b, "joined_left", "false") // flagged, but nothing cooks yet
	apply(b,
		sym("Cooking.Hob.Status.Zone.100.State", "Active"), sym("Cooking.Hob.Status.Zone.100.PowerLevel", "35"),
		sym("Cooking.Hob.Status.Zone.200.State", "Active"), sym("Cooking.Hob.Status.Zone.200.PowerLevel", "35"),
	)
	want(t, b, "joined_left", "true")
	want(t, b, "joined_right", "false")
	want(t, b, "zone/front_left/power", "3.5")
	want(t, b, "zone/rear_left/power", "3.5")

	// Focus moves to the right side, which is not joined: the left stays joined.
	apply(b, sym("Cooking.Hob.Option.ZoneSelector", "RearRight"))
	join(false)
	want(t, b, "joined_left", "true")

	// Unjoin on the left: the flag goes false and front-left switches off.
	apply(b, sym("Cooking.Hob.Option.ZoneSelector", "FrontLeft"))
	join(false)
	apply(b, sym("Cooking.Hob.Status.Zone.100.State", "Off"), sym("Cooking.Hob.Status.Zone.100.PowerLevel", "Off"))
	want(t, b, "joined_left", "false")
	want(t, b, "zone/front_left/power", "off")
	want(t, b, "zone/rear_left/power", "3.5")

	// A stale flag cannot outlive the cooking.
	join(true)
	want(t, b, "joined_left", "false")
}

func TestTimers(t *testing.T) {
	b := newTestBridge()
	start := time.Now()
	apply(b, num("Cooking.Hob.Status.Zone.200.RemainingProgramTime", 86))
	got, _ := b.get("zone/rear_left/timer_deadline")
	deadline, err := time.Parse(time.RFC3339, got)
	if err != nil || deadline.Sub(start) < 84*time.Second || deadline.Sub(start) > 88*time.Second {
		t.Fatalf("zone timer %q (%v)", got, err)
	}
	// The next tick a second later lands on the same deadline: no republish.
	b.mu.Lock()
	before := b.lastAt["zone/rear_left/timer_deadline"]
	b.mu.Unlock()
	apply(b, num("Cooking.Hob.Status.Zone.200.RemainingProgramTime", 85))
	b.mu.Lock()
	after := b.lastAt["zone/rear_left/timer_deadline"]
	b.mu.Unlock()
	if !after.Equal(before) {
		t.Error("timer tick republished the deadline")
	}
	apply(b, num("Cooking.Hob.Status.Zone.200.RemainingProgramTime", 0))
	want(t, b, "zone/rear_left/timer_deadline", "")

	apply(b, num("BSH.Common.Option.RemainingProgramTime", 600))
	if got, _ := b.get("status/program_timer_deadline"); got == "" {
		t.Fatal("program timer not published")
	}
	if _, ok := b.get("status/remaining_program_time"); ok {
		t.Fatal("remaining time published as its own topic")
	}
}

func TestEventsAreNotRetainedAndSkippedInSnapshots(t *testing.T) {
	b := newTestBridge()
	out := capture(b)
	b.ApplyValues([]hc.Value{sym("BSH.Common.Event.ProgramFinished", "Off")}, true)
	if strings.Contains(out.String(), "event/") {
		t.Fatalf("snapshot fired an event:\n%s", out)
	}
	apply(b, sym("BSH.Common.Event.ProgramFinished", "Present"))
	line := out.String()
	if !strings.Contains(line, "homeconnect/cooktop/event/program_finished") || !strings.Contains(line, "= Present") || strings.Contains(line, "[retained]") {
		t.Fatalf("event publish: %s", line)
	}
}

func TestFanLevel(t *testing.T) {
	b := newTestBridge()
	apply(b, sym("Cooking.Hob.Setting.Ventilation", "Level08"))
	want(t, b, "status/fan_level", "4.5")
	// After power-off the hob reports the after-run inside an access change.
	b.ApplyDescriptions([]hc.Description{{UID: 4409, Name: "Cooking.Hob.Setting.Ventilation", Value: &hc.Value{Name: "Cooking.Hob.Setting.Ventilation", Symbol: "AfterRun"}}})
	want(t, b, "status/fan_level", "afterrun")
}

func TestCarbonFilter(t *testing.T) {
	b := newTestBridge()
	sat := func(pct float64) {
		b.ApplyDescriptions([]hc.Description{{UID: 4096, Name: "Cooking.Hob.Status.CarbonFilterSaturation", Access: "read",
			Value: &hc.Value{Name: "Cooking.Hob.Status.CarbonFilterSaturation", V: pct}}})
	}
	if _, ok := b.get("status/carbon_filter_replace"); ok {
		t.Fatal("replace flag published before anything is known")
	}
	sat(96)
	want(t, b, "status/carbon_filter_saturation", "96")
	want(t, b, "status/carbon_filter_replace", "true")
	sat(40)
	want(t, b, "status/carbon_filter_replace", "false")

	// The hob's own event latches the flag until the saturation reads low.
	b2 := newTestBridge()
	apply(b2, sym("Cooking.Hob.Event.CarbonFilterMaxSaturationNearlyReached", "Present"))
	want(t, b2, "status/carbon_filter_replace", "true")
	b2.ApplyDescriptions([]hc.Description{{Name: "Cooking.Hob.Status.CarbonFilterSaturation", Value: &hc.Value{V: 80.0}}})
	want(t, b2, "status/carbon_filter_replace", "true")
	b2.ApplyDescriptions([]hc.Description{{Name: "Cooking.Hob.Status.CarbonFilterSaturation", Value: &hc.Value{V: 3.0}}})
	want(t, b2, "status/carbon_filter_replace", "false")
	// Event keys inside access changes are ignored.
	b2.ApplyDescriptions([]hc.Description{{Name: "Cooking.Hob.Event.CarbonFilterMaxSaturationReached", Value: &hc.Value{Symbol: "Present"}}})
	want(t, b2, "status/carbon_filter_replace", "false")
}

func TestPowerOffResets(t *testing.T) {
	b := newTestBridge()
	apply(b,
		sym("BSH.Common.Setting.PowerState", "On"),
		sym("Cooking.Hob.Option.ZoneSelector", "FrontLeft"), boolv("Cooking.Hob.Option.JoinZone", true),
		sym("Cooking.Hob.Status.Zone.100.State", "Active"), sym("Cooking.Hob.Status.Zone.100.PowerLevel", "50"),
		sym("Cooking.Hob.Status.Zone.200.State", "Active"), sym("Cooking.Hob.Status.Zone.200.PowerLevel", "50"),
		sym("Cooking.Hob.Status.Zone.400.State", "ResiduelHeat"),
		num("Cooking.Hob.Status.Zone.100.RemainingProgramTime", 300),
		num("BSH.Common.Option.RemainingProgramTime", 300),
		sym("Cooking.Hob.Setting.Ventilation", "Level05"),
	)
	want(t, b, "joined_left", "true")
	apply(b, sym("BSH.Common.Setting.PowerState", "Off"))
	want(t, b, "zone/front_left/power", "off")
	want(t, b, "zone/front_left/state", "off")
	want(t, b, "zone/front_right/state", "residual_heat") // reported by the hob, kept
	want(t, b, "zone/front_left/timer_deadline", "")
	want(t, b, "status/program_timer_deadline", "")
	want(t, b, "joined_left", "false")
	want(t, b, "status/fan_level", "3") // the fan is the hob's to report (after-run)
}

func TestThrottledTopics(t *testing.T) {
	b := newTestBridge()
	tick := func(s float64) { apply(b, num("BSH.Common.Option.ElapsedProgramTime", s)) }
	tick(0)
	tick(1) // from zero: at once
	want(t, b, "status/elapsed_program_time", "1")
	tick(2) // within the gap: held back
	want(t, b, "status/elapsed_program_time", "1")
	b.mu.Lock()
	b.lastAt["status/elapsed_program_time"] = time.Now().Add(-time.Minute)
	b.mu.Unlock()
	tick(3)
	want(t, b, "status/elapsed_program_time", "3")
	tick(0) // back to zero: at once
	want(t, b, "status/elapsed_program_time", "0")
}

func TestUnknownAndIgnoredKeys(t *testing.T) {
	b := newTestBridge()
	b.ApplyValues([]hc.Value{{UID: 99999, Raw: []byte("1"), V: 1.0}}, false)
	apply(b, sym("Cooking.Hob.Setting.BuzzerBeepLevel", "AllActive"), num("BSH.Common.Status.StopWatchTime", 3))
	if len(b.lastPub) != 0 {
		t.Fatalf("published %v", b.lastPub)
	}
	if !b.unmapped["uid 99999"] || !b.unmapped["Cooking.Hob.Setting.BuzzerBeepLevel"] {
		t.Fatalf("unmapped %v", b.unmapped)
	}
}

func TestPublishLink(t *testing.T) {
	b := newTestBridge()
	since := time.Date(2026, 11, 9, 16, 0, 0, 0, time.UTC)
	b.PublishLink(hc.LinkRefused, since, true)
	want(t, b, "status/link_state", "refused")
	want(t, b, "status/link_since", "2026-11-09T16:00:00Z")
	want(t, b, "status/power_cycle_needed", "true")
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

func TestLabels(t *testing.T) {
	power := map[string]string{"Off": "off", "10": "1", "15": "1.5", "45": "4.5", "90": "9", "KeepWarm": "KeepWarm", "Boost1": "Boost1"}
	for in, out := range power {
		if got := powerLevelAPIToLabel(in); got != out {
			t.Errorf("power %q = %q, want %q", in, got, out)
		}
	}
	fan := map[string]string{"Off": "off", "Automatic": "auto", "Level01": "1", "Level02": "1.5", "Level08": "4.5", "Level15": "8", "Level17": "9",
		"BoostLevel1": "boost1", "BoostLevel2": "boost2", "AfterRun": "afterrun", "Mystery": "mystery"}
	for in, out := range fan {
		if got := fanLabel(in); got != out {
			t.Errorf("fan %q = %q, want %q", in, got, out)
		}
	}
	if got := fanLabel(42.0); got != "42" {
		t.Errorf("unknown numeric fan value = %q", got)
	}
	states := []struct{ state, op, want string }{
		{"Active", "Run", "active"}, {"Off", "Ready", "off"}, {"ResiduelHeat", "", "residual_heat"},
		{"NotSelectable", "", "not_selectable"}, {"Active", "Pause", "paused"}, {"Off", "Error", "error"}, {"", "", "off"},
	}
	for _, s := range states {
		if got := zoneStateLabel(s.state, s.op); got != s.want {
			t.Errorf("zoneStateLabel(%q, %q) = %q, want %q", s.state, s.op, got, s.want)
		}
	}
}

// Seen on the real hob (2026-11-10 07:09): PowerState=Off arrives, and the
// zone's ResiduelHeat push follows 0.24 s later. The reset must not publish
// "off" for the zone in between.
func TestPowerOffDoesNotContradictResidualHeat(t *testing.T) {
	b := newTestBridge()
	b.powerOffDelay = 100 * time.Millisecond
	out := capture(b)
	apply(b, sym("BSH.Common.Setting.PowerState", "On"),
		sym("Cooking.Hob.Status.Zone.300.State", "Active"), sym("Cooking.Hob.Status.Zone.300.PowerLevel", "65"))
	apply(b, sym("BSH.Common.Setting.PowerState", "Off"))
	apply(b, sym("Cooking.Hob.Status.Zone.300.State", "ResiduelHeat"), sym("Cooking.Hob.Status.Zone.300.PowerLevel", "Off"))
	time.Sleep(250 * time.Millisecond) // past the grace
	want(t, b, "zone/rear_right/state", "residual_heat")
	for _, l := range strings.Split(out.String(), "\n") {
		if strings.Contains(l, "zone/rear_right/state") && strings.HasSuffix(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(l), "[retained]")), "= off") {
			t.Fatalf("rear_right state went off before residual heat:\n%s", out)
		}
	}
}

// A push the hob never sends is still cleaned up, after the grace.
func TestPowerOffResetsMissedPushesAfterGrace(t *testing.T) {
	b := newTestBridge()
	b.powerOffDelay = 50 * time.Millisecond
	apply(b, sym("BSH.Common.Setting.PowerState", "On"),
		sym("Cooking.Hob.Status.Zone.100.State", "Active"), sym("Cooking.Hob.Status.Zone.100.PowerLevel", "50"))
	apply(b, sym("BSH.Common.Setting.PowerState", "Off")) // and no zone push follows
	want(t, b, "zone/front_left/state", "active")         // not yet
	if !waitFor(t, 2*time.Second, func() bool { v, _ := b.get("zone/front_left/state"); return v == "off" }) {
		t.Fatal("stale active zone never reset")
	}
	want(t, b, "zone/front_left/power", "off")
}

// Switched on again inside the grace: nothing is reset.
func TestPowerOffResetCancelledByPowerOn(t *testing.T) {
	b := newTestBridge()
	b.powerOffDelay = 60 * time.Millisecond
	apply(b, sym("BSH.Common.Setting.PowerState", "On"),
		sym("Cooking.Hob.Status.Zone.100.State", "Active"), sym("Cooking.Hob.Status.Zone.100.PowerLevel", "50"))
	apply(b, sym("BSH.Common.Setting.PowerState", "Off"))
	apply(b, sym("BSH.Common.Setting.PowerState", "On"))
	time.Sleep(200 * time.Millisecond)
	want(t, b, "zone/front_left/state", "active")
	want(t, b, "zone/front_left/power", "5")
}
