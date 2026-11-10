package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Local feature name -> MQTT topic suffix for plain state values (retained).
// Feature names come from the hob's profile; values arrive already resolved
// to enum member names. Keys not mapped anywhere below are dropped, the
// first sighting of each logged at INFO so new keys get noticed.
var keyToSuffix = map[string]string{
	"BSH.Common.Status.OperationState":          "status/operation_state",
	"BSH.Common.Status.LocalControlActive":      "status/local_control",
	"BSH.Common.Setting.PowerState":             "status/power_state",
	"BSH.Common.Setting.ChildLock":              "status/child_lock",
	"BSH.Common.Option.Duration":                "status/program_duration",
	"BSH.Common.Option.ElapsedProgramTime":      "status/elapsed_program_time",
	"BSH.Common.Option.ProgramProgress":         "status/program_progress",
	"BSH.Common.Root.ActiveProgram":             "status/active_program",
	"Cooking.Hob.Option.ZoneSelector":           "status/focused_zone",
	"Cooking.Hob.Setting.Ventilation":           "status/fan_level",
	"Cooking.Hob.Status.CarbonFilterSaturation": "status/carbon_filter_saturation",
	"BSH.Common.Status.WiFiSignalStrength":      "status/wifi_signal",
}

// throttledSuffixes are published at most this often while they change.
// The hob pushes elapsed time and progress every second while a program
// runs; every tick as a retained publish only churns the broker and HA's
// recorder. A change to or from zero always goes out at once.
var throttledSuffixes = map[string]time.Duration{
	"status/elapsed_program_time": 15 * time.Second,
	"status/program_progress":     15 * time.Second,
	"status/wifi_signal":          5 * time.Minute,
}

// One-shot events, published non-retained to event/* so HA automations
// can trigger on them without re-firing on a restart. The payload is the
// event state (Present, Confirmed, Off).
var eventKeyToSuffix = map[string]string{
	"BSH.Common.Event.AlarmClockElapsed":                       "event/alarm_clock_elapsed",
	"BSH.Common.Event.ProgramFinished":                         "event/program_finished",
	"BSH.Common.Event.ProgramAborted":                          "event/program_aborted",
	"Cooking.Hob.Event.CarbonFilterMaxSaturationNearlyReached": "event/carbon_filter_nearly_saturated",
	"Cooking.Hob.Event.CarbonFilterMaxSaturationReached":       "event/carbon_filter_saturated",
}

// Keys the hob pushes that are understood but deliberately not published.
// Logged at DEBUG, not as new keys.
var ignoredKeys = map[string]bool{
	"BSH.Common.Root.SelectedProgram":           true,
	"BSH.Common.Setting.AlarmClock":             true,
	"BSH.Common.Setting.AllowBackendConnection": true,
	// The hob's own view of its Home Connect cloud link. Not published: it
	// pushes a change to false (seen at 17:04 on 2026-11-09) but never the
	// recovery, so it read false for hours while the cloud worked; only the
	// 6-hourly full read corrected it.
	"BSH.Common.Status.BackendConnected":                 true,
	"BSH.Common.Setting.AllowConsumerInsights":           true,
	"BSH.Common.Setting.TemperatureUnit":                 true,
	"BSH.Common.Status.RemoteControlActive":              true,
	"BSH.Common.Status.RemoteControlStartAllowed":        true,
	"BSH.Common.Status.CustomerServiceConnectionAllowed": true,
	"BSH.Common.Status.WipeProtectionActive":             true,
	"BSH.Common.Status.StopWatchState":                   true,
	"BSH.Common.Status.StopWatchTime":                    true,
	"BSH.Common.Status.SoftwareUpdateTransactionID":      true,
	"BSH.Common.Event.ConnectLocalWiFi":                  true,
	"BSH.Common.Event.CustomerServiceRequest":            true,
	"BSH.Common.Event.SoftwareDownloadAvailable":         true,
	"BSH.Common.Event.SoftwareUpdateAvailable":           true,
	"BSH.Common.Event.SoftwareUpdateSuccessful":          true,
	"Cooking.Hob.Option.PowerLevel":                      true, // the focused zone's level; zones report their own
	"Cooking.Hob.Option.CookingSensorLevel":              true,
	"Cooking.Hob.Option.FryingSensorLevel":               true,
}

// ignoredPrefixes covers whole families: the hob's configuration settings
// (buzzer, power management, ...), the cooking sensor, PowerMove.
var ignoredPrefixes = []string{
	"Cooking.Hob.Setting.",
	"Cooking.Hob.Option.PowerMoveMode",
	"Cooking.Hob.Event.CookingSensor",
}

func isIgnored(key string) bool {
	if ignoredKeys[key] {
		return true
	}
	for _, p := range ignoredPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// zoneKeyRe matches per-zone keys: Cooking.Hob.Status.Zone.<id>.<attr>.
var zoneKeyRe = regexp.MustCompile(`^Cooking\.Hob\.(?:Status|Setting|Option)\.Zone\.(\w+)\.(\w+)$`)

// zoneHalves maps the hob's zone IDs to the four cooking zones. On the
// PXX801D67E a flex-joined pair is reported as both base zones active at
// the same level; the combined zones 120/340 and the venting zone F55 in
// the profile never carry values (observed 2026-11-09).
var zoneHalves = map[string]string{
	"100": "FrontLeft",
	"200": "RearLeft",
	"300": "RearRight",
	"400": "FrontRight",
}

// Zones in topic order.
var allZones = []string{"FrontLeft", "FrontRight", "RearLeft", "RearRight"}

// Retained state topics published by earlier versions and no longer used.
// They are cleared (empty retained payload) on every broker connect.
var removedStateTopics = []string{
	// gone in the cloud 2.0 rewrite
	"status/alarm_clock_seconds",
	"status/alarm_clock_minutes",
	"status/remaining_program_time",
	"status/focused_zone_power",
	"status/focused_zone_joined",
	"status/remote_start_allowed",
	"status/remote_control_active",
	// cloud-only, gone with the local transport
	"status/rate_limited",
	"status/rate_limited_until",
	"status/api_calls_24h",
	"status/appliance_online",
	"cloud",
}

// cleanEnum strips an enum prefix ("BSH.Common.EnumType.PowerState.On" ->
// "On"). Local values arrive as bare member names already; this keeps
// fully qualified names readable too.
func cleanEnum(v interface{}) string {
	s, ok := v.(string)
	if !ok {
		return fmt.Sprintf("%v", v)
	}
	if strings.Contains(s, ".EnumType.") {
		if i := strings.LastIndex(s, "."); i >= 0 {
			return s[i+1:]
		}
	}
	return s
}

// powerLevelAPIToLabel converts a PowerLevel enum member to the displayed
// level. The hob encodes the displayed power x 10 ("10" is level 1, "15"
// is 1.5, "90" is 9). "Off" becomes "off"; KeepWarm and Boost1/Boost2 pass
// through.
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

// fanLabel converts a Ventilation enum member to a short label: off, auto,
// 1 .. 9 in half steps (Level01 is 1, Level02 1.5, Level17 9), boost1,
// boost2, afterrun. Unknown members are lower-cased.
func fanLabel(v interface{}) string {
	s, ok := v.(string)
	if !ok {
		if v == nil {
			return ""
		}
		return fmt.Sprintf("%v", v)
	}
	switch s = cleanEnum(s); s {
	case "Off":
		return "off"
	case "Automatic":
		return "auto"
	case "BoostLevel1":
		return "boost1"
	case "BoostLevel2":
		return "boost2"
	case "AfterRun":
		return "afterrun"
	}
	if n, err := strconv.Atoi(strings.TrimPrefix(s, "Level")); err == nil && strings.HasPrefix(s, "Level") {
		level := float64(n+1) / 2
		if level == float64(int(level)) {
			return strconv.Itoa(int(level))
		}
		return strconv.FormatFloat(level, 'f', 1, 64)
	}
	return strings.ToLower(s)
}

// zoneStateLabel combines a zone's State with its OperationState: a paused
// or failed zone says so, otherwise the heating state.
func zoneStateLabel(state, op string) string {
	switch op {
	case "Pause":
		return "paused"
	case "Error":
		return "error"
	case "ActionRequired":
		return "action_required"
	}
	switch state {
	case "Active":
		return "active"
	case "ResiduelHeat", "ResidualHeat":
		return "residual_heat"
	case "NotSelectable":
		return "not_selectable"
	case "Off", "":
		return "off"
	}
	return strings.ToLower(state)
}

// formatValue converts a resolved value into its MQTT payload string.
func formatValue(key string, v interface{}) string {
	switch {
	case strings.HasSuffix(key, ".PowerLevel"):
		if lbl := powerLevelAPIToLabel(v); lbl != "" {
			return lbl
		}
	case key == "Cooking.Hob.Setting.Ventilation":
		return fanLabel(v)
	}
	switch x := v.(type) {
	case nil:
		return ""
	case bool:
		return strconv.FormatBool(x)
	case string:
		return cleanEnum(x)
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
	return fmt.Sprintf("%v", v)
}
