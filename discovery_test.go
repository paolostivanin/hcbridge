package main

import (
	"fmt"
	"testing"
)

// liveEntityIDs maps every unique_id to the entity_id HA uses for it. The
// first block was read from the production registry before 2.0.0; the
// dashboard and the HA package depend on those IDs.
var liveEntityIDs = map[string]string{
	"hcbridge_cooktop_operation_state":        "sensor.cooktop_operation_state",
	"hcbridge_cooktop_power_state":            "sensor.cooktop_power_state",
	"hcbridge_cooktop_active_program":         "sensor.cooktop_active_program",
	"hcbridge_cooktop_program_duration":       "sensor.cooktop_program_duration",
	"hcbridge_cooktop_elapsed_program_time":   "sensor.cooktop_elapsed_program_time",
	"hcbridge_cooktop_program_progress":       "sensor.cooktop_program_progress",
	"hcbridge_cooktop_focused_zone":           "sensor.cooktop_focused_zone",
	"hcbridge_cooktop_zone_front_left_power":  "sensor.cooktop_front_left_zone",
	"hcbridge_cooktop_zone_front_right_power": "sensor.cooktop_front_right_zone",
	"hcbridge_cooktop_zone_rear_left_power":   "sensor.cooktop_rear_left_zone",
	"hcbridge_cooktop_zone_rear_right_power":  "sensor.cooktop_rear_right_zone",
	"hcbridge_cooktop_joined_left":            "binary_sensor.cooktop_left_zones_joined",
	"hcbridge_cooktop_joined_right":           "binary_sensor.cooktop_right_zones_joined",
	"hcbridge_cooktop_program_timer":          "sensor.cooktop_program_timer",
	"hcbridge_cooktop_child_lock":             "binary_sensor.cooktop_child_lock",
	"hcbridge_cooktop_local_control":          "binary_sensor.cooktop_local_control",
	"hcbridge_cooktop_heartbeat":              "sensor.cooktop_bridge_heartbeat",
	// new in 2.0.0 (local)
	"hcbridge_cooktop_zone_front_left_state":    "sensor.cooktop_front_left_zone_state",
	"hcbridge_cooktop_zone_front_right_state":   "sensor.cooktop_front_right_zone_state",
	"hcbridge_cooktop_zone_rear_left_state":     "sensor.cooktop_rear_left_zone_state",
	"hcbridge_cooktop_zone_rear_right_state":    "sensor.cooktop_rear_right_zone_state",
	"hcbridge_cooktop_zone_front_left_timer":    "sensor.cooktop_front_left_zone_timer",
	"hcbridge_cooktop_zone_front_right_timer":   "sensor.cooktop_front_right_zone_timer",
	"hcbridge_cooktop_zone_rear_left_timer":     "sensor.cooktop_rear_left_zone_timer",
	"hcbridge_cooktop_zone_rear_right_timer":    "sensor.cooktop_rear_right_zone_timer",
	"hcbridge_cooktop_fan_level":                "sensor.cooktop_fan_level",
	"hcbridge_cooktop_carbon_filter_saturation": "sensor.cooktop_carbon_filter_saturation",
	"hcbridge_cooktop_carbon_filter_replace":    "binary_sensor.cooktop_carbon_filter_replace",
	"hcbridge_cooktop_wifi_signal":              "sensor.cooktop_wifi_signal",
	"hcbridge_cooktop_backend_connected":        "binary_sensor.cooktop_home_connect_cloud",
	"hcbridge_cooktop_link_state":               "sensor.cooktop_local_link",
	"hcbridge_cooktop_link_since":               "sensor.cooktop_local_link_since",
	"hcbridge_cooktop_power_cycle_needed":       "binary_sensor.cooktop_power_cycle_needed",
}

// Entities that report on the bridge and the link: never gated by the link.
var ungated = map[string]bool{"heartbeat": true, "link_state": true, "link_since": true, "power_cycle_needed": true}

func TestDiscoveryEntityIDsAndAvailability(t *testing.T) {
	cfg := testConfig()
	seen := map[string]bool{}
	for _, e := range discoveryEntities(cfg) {
		p := discoveryPayload(cfg, deviceMeta{}, e)
		uid := p["unique_id"].(string)
		if seen[uid] {
			t.Errorf("duplicate entity %s", uid)
		}
		seen[uid] = true
		want, ok := liveEntityIDs[uid]
		if !ok {
			t.Errorf("unexpected entity %s", uid)
			continue
		}
		if p["default_entity_id"] != want {
			t.Errorf("%s: default_entity_id %v, want %s", uid, p["default_entity_id"], want)
		}
		if _, ok := p["object_id"]; ok {
			t.Errorf("%s: still sends object_id", uid)
		}
		avail := p["availability"].([]map[string]string)
		if ungated[e.objectID] {
			if len(avail) != 1 || avail[0]["topic"] != cfg.StateTopic("connected") || p["entity_category"] != "diagnostic" {
				t.Errorf("%s: link-health entity availability %v category %v", uid, avail, p["entity_category"])
			}
		} else if len(avail) != 2 || p["availability_mode"] != "all" || avail[1]["topic"] != cfg.StateTopic("link") {
			t.Errorf("%s: data entity availability %v mode %v", uid, avail, p["availability_mode"])
		}
	}
	for uid := range liveEntityIDs {
		if !seen[uid] {
			t.Errorf("entity %s missing", uid)
		}
	}
	for _, r := range removedDiscovery {
		if seen["hcbridge_cooktop_"+r.objectID] {
			t.Errorf("removed entity %s still published", r.objectID)
		}
	}
}

// Every entity 1.x or the cloud draft ever published is either still live
// or cleaned up, so nothing lingers in HA.
func TestNoOrphanedEntities(t *testing.T) {
	cfg := testConfig()
	live := map[string]bool{}
	for _, e := range discoveryEntities(cfg) {
		live[e.component+"/"+e.objectID] = true
	}
	removed := map[string]bool{}
	for _, r := range removedDiscovery {
		removed[r.component+"/"+r.objectID] = true
	}
	ever := []string{
		// 1.x (git show c694b68:discovery.go)
		"sensor/operation_state", "sensor/power_state", "sensor/active_program", "sensor/program_duration",
		"sensor/elapsed_program_time", "sensor/program_progress", "sensor/remaining_program_time",
		"sensor/focused_zone", "sensor/focused_zone_power", "binary_sensor/focused_zone_joined",
		"sensor/zone_front_left_power", "sensor/zone_front_right_power", "sensor/zone_rear_left_power", "sensor/zone_rear_right_power",
		"binary_sensor/joined_left", "binary_sensor/joined_right", "sensor/program_timer", "binary_sensor/child_lock",
		"binary_sensor/local_control", "binary_sensor/remote_start_allowed", "binary_sensor/rate_limited",
		"sensor/rate_limited_until", "sensor/alarm_clock_seconds", "number/alarm_clock_minutes",
		// cloud 2.0 draft
		"sensor/api_calls_24h", "binary_sensor/cloud_connection", "binary_sensor/appliance_connected", "sensor/heartbeat",
	}
	for _, k := range ever {
		if !live[k] && !removed[k] {
			t.Errorf("%s was published before but is neither live nor removed", k)
		}
	}
}

func TestDeviceBlock(t *testing.T) {
	cfg := testConfig()
	d := deviceBlock(cfg, deviceMeta{})
	if fmt.Sprint(d["identifiers"]) != "[hcbridge_cooktop]" || d["model"] != "Home Connect cooktop" {
		t.Fatalf("empty meta: %v", d)
	}
	if _, ok := d["sw_version"]; ok {
		t.Fatal("sw_version without known versions")
	}
	d = deviceBlock(cfg, deviceMeta{Brand: "BOSCH", VIB: "PXX801D67E", Serial: "123", HaVersion: "1.332", SwVersion: "2.17.1.13549", HwVersion: "1.0.4.4"})
	if d["manufacturer"] != "Bosch" || d["model"] != "PXX801D67E" || d["serial_number"] != "123" ||
		d["sw_version"] != "1.332-2.17.1.13549" || d["hw_version"] != "1.0.4.4" ||
		fmt.Sprint(d["identifiers"]) != "[hcbridge_cooktop]" {
		t.Fatalf("device block %v", d)
	}
	if _, ok := d["connections"]; ok {
		t.Fatal("connections would merge the device with other integrations' MAC entries")
	}
}

func TestChildLockHasNoLockDeviceClass(t *testing.T) {
	cfg := testConfig()
	for _, e := range discoveryEntities(cfg) {
		if e.objectID == "child_lock" {
			if _, ok := discoveryPayload(cfg, deviceMeta{}, e)["device_class"]; ok {
				t.Fatal("child_lock has a device_class; 'lock' inverts its meaning")
			}
			return
		}
	}
	t.Fatal("child_lock entity missing")
}

func TestLinkStateOptions(t *testing.T) {
	cfg := testConfig()
	for _, e := range discoveryEntities(cfg) {
		if e.objectID == "link_state" {
			got := fmt.Sprint(discoveryPayload(cfg, deviceMeta{}, e)["options"])
			if got != "[connecting connected refused unreachable key_error error]" {
				t.Fatalf("options %s", got)
			}
			return
		}
	}
	t.Fatal("link_state entity missing")
}
