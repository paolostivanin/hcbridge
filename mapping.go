package main

import (
	"fmt"
	"strconv"
	"strings"
)

// API ↔ MQTT topic suffix mapping for STATE keys (status/setting/option).
// State topics are retained. Keys not listed here are dropped; the first
// sighting of each is logged at INFO so new keys the hob sends get noticed.
var keyToSuffix = map[string]string{
	"BSH.Common.Status.OperationState":     "status/operation_state",
	"BSH.Common.Status.LocalControlActive": "status/local_control",
	"BSH.Common.Setting.PowerState":        "status/power_state",
	"BSH.Common.Setting.ChildLock":         "status/child_lock",
	"BSH.Common.Option.Duration":           "status/program_duration",
	"BSH.Common.Option.ElapsedProgramTime": "status/elapsed_program_time",
	"BSH.Common.Option.ProgramProgress":    "status/program_progress",
	"BSH.Common.Root.ActiveProgram":        "status/active_program",

	// Per-zone state comes from /programs/{active,selected}/options. The
	// hob exposes only one zone at a time — whichever the panel last
	// focused — and Bosch does not push it via SSE; it is refreshed on every
	// options poll (Bridge.applyOptions). Listed here in case an SSE event
	// ever does carry it.
	"Cooking.Hob.Option.ZoneSelector": "status/focused_zone",
}

// Keys deliberately dropped since 2.0.0 (their entities were removed as
// unused). Logged at DEBUG, not as "new" keys.
var ignoredKeys = map[string]bool{
	"BSH.Common.Setting.AlarmClock":               true,
	"BSH.Common.Status.RemoteControlActive":       true,
	"BSH.Common.Status.RemoteControlStartAllowed": true,
}

// Retained state topics published by versions before 2.0.0 and no longer
// used. They are cleared (empty retained payload) on every broker connect so
// nothing stale lingers on the broker.
var removedStateTopics = []string{
	"status/alarm_clock_seconds",
	"status/alarm_clock_minutes",
	"status/remaining_program_time",
	"status/focused_zone_power",
	"status/focused_zone_joined",
	"status/remote_start_allowed",
	"status/remote_control_active",
}

// Note: integrated venting is NOT exposed via the public Home Connect API for
// PXX801D67E (confirmed via /settings/Cooking.{Hob,Hood,Common}.Setting.* direct
// probes — all returned 409 UnsupportedSetting). --mode=dump also probes the
// documented hood program option path.

// EVENT-class keys are one-shot (AlarmClock rang, program finished, etc).
// These get published non-retained to event/* topics so HA mqtt event
// triggers can fire on them without re-firing on restart.
var eventKeyToSuffix = map[string]string{
	"BSH.Common.Event.AlarmClockElapsed": "event/alarm_clock_elapsed",
	"BSH.Common.Event.ProgramFinished":   "event/program_finished",
	"BSH.Common.Event.ProgramAborted":    "event/program_aborted",
}

// Strip enum prefixes for compact MQTT payloads.
//
//	"BSH.Common.EnumType.PowerState.On" -> "On"
//	"BSH.Common.EnumType.OperationState.Run" -> "Run"
func cleanEnum(v interface{}) string {
	s, ok := v.(string)
	if !ok {
		return fmt.Sprintf("%v", v)
	}
	if i := strings.LastIndex(s, "."); i >= 0 {
		return s[i+1:]
	}
	return s
}

// powerLevelAPIToLabel converts Cooking.Hob.EnumType.PowerLevel.NN to the
// displayed level. Bosch encodes the displayed power × 10 (so the panel-
// shown "1" is API value 10, "1.5" is 15, "9" is 90, etc). "Off" stays "off".
// Non-numeric levels (e.g. "Boost2") pass through unchanged; the dashboard
// renders them as-is. The encoding is empirical, not documented.
func powerLevelAPIToLabel(v interface{}) string {
	s, ok := v.(string)
	if !ok {
		return ""
	}
	suffix := s
	if i := strings.LastIndex(s, "."); i >= 0 {
		suffix = s[i+1:]
	}
	if strings.EqualFold(suffix, "Off") {
		return "off"
	}
	n, err := strconv.Atoi(suffix)
	if err != nil {
		return suffix
	}
	if n%10 == 0 {
		return strconv.Itoa(n / 10)
	}
	return strconv.FormatFloat(float64(n)/10, 'f', 1, 64)
}

// formatValue converts an API value into its MQTT payload string.
// Booleans become "true"/"false"; numbers plain decimals;
// PowerLevel enums are demangled (10 → "1", 15 → "1.5").
func formatValue(key string, v interface{}) string {
	if key == "Cooking.Hob.Option.PowerLevel" {
		if lbl := powerLevelAPIToLabel(v); lbl != "" {
			return lbl
		}
	}
	switch x := v.(type) {
	case nil:
		return ""
	case bool:
		if x {
			return "true"
		}
		return "false"
	case string:
		// Most enum-typed values carry a "BSH.Common.EnumType.X.Foo" string.
		if strings.Contains(x, ".EnumType.") {
			return cleanEnum(x)
		}
		return x
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
	return fmt.Sprintf("%v", v)
}
