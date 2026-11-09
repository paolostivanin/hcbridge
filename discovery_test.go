package main

import "testing"

// liveEntityIDs maps every kept unique_id to the entity_id HA registered for
// it (read from the production entity registry before 2.0.0), plus the four
// entities new in 2.0.0. The dashboard depends on these IDs.
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
	"hcbridge_cooktop_rate_limited":           "binary_sensor.cooktop_api_rate_limited",
	"hcbridge_cooktop_rate_limited_until":     "sensor.cooktop_api_rate_limit_clears",
	// new in 2.0.0
	"hcbridge_cooktop_api_calls_24h":       "sensor.cooktop_api_calls_24h",
	"hcbridge_cooktop_cloud_connection":    "binary_sensor.cooktop_cloud_connection",
	"hcbridge_cooktop_appliance_connected": "binary_sensor.cooktop_appliance_connected",
	"hcbridge_cooktop_heartbeat":           "sensor.cooktop_bridge_heartbeat",
}

func TestDiscoveryEntityIDsAndAvailability(t *testing.T) {
	cfg := testConfig()
	seen := map[string]bool{}
	for _, e := range discoveryEntities(cfg) {
		p := discoveryPayload(cfg, e)
		uid := p["unique_id"].(string)
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
		if _, ok := p["device"].(map[string]interface{})["via_device"]; ok {
			t.Errorf("%s: via_device points at a device that does not exist", uid)
		}
		avail := p["availability"].([]map[string]string)
		if e.diagnostic {
			if len(avail) != 1 || p["entity_category"] != "diagnostic" {
				t.Errorf("%s: diagnostic entity availability %v category %v", uid, avail, p["entity_category"])
			}
		} else if len(avail) != 2 || p["availability_mode"] != "all" || avail[1]["topic"] != cfg.StateTopic("cloud") {
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

func TestChildLockHasNoLockDeviceClass(t *testing.T) {
	cfg := testConfig()
	for _, e := range discoveryEntities(cfg) {
		if e.objectID == "child_lock" {
			if _, ok := discoveryPayload(cfg, e)["device_class"]; ok {
				t.Fatal("child_lock has a device_class; 'lock' inverts its meaning")
			}
			return
		}
	}
	t.Fatal("child_lock entity missing")
}

func TestRateLimitEntitiesStayVisibleWhenCloudIsDown(t *testing.T) {
	cfg := testConfig()
	for _, e := range discoveryEntities(cfg) {
		switch e.objectID {
		case "rate_limited", "rate_limited_until", "cloud_connection", "heartbeat":
			for _, a := range discoveryPayload(cfg, e)["availability"].([]map[string]string) {
				if a["topic"] == cfg.StateTopic("cloud") {
					t.Errorf("%s depends on the cloud topic", e.objectID)
				}
			}
		}
	}
}
