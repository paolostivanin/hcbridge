package main

import (
	"context"
	"encoding/json"
	"fmt"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// PublishDiscovery emits Home Assistant MQTT discovery configs for all entities.
// Topics: homeassistant/<component>/<node_id>/<object_id>/config (retained).
// All entities share the same `device` block so HA groups them.
func PublishDiscovery(ctx context.Context, cfg *Config, cli mqtt.Client, log *Logger) error {
	slug := cfg.Appliance.TopicSlug
	device := map[string]interface{}{
		"identifiers":  []string{"hcbridge_" + slug},
		"name":         cfg.Appliance.FriendlyID,
		"manufacturer": "Bosch",
		"model":        "Home Connect cooktop",
		"via_device":   "hcbridge",
	}
	availability := []map[string]string{{
		"topic":                cfg.StateTopic("connected"),
		"payload_available":    "online",
		"payload_not_available": "offline",
	}}

	type entry struct {
		component string // sensor, binary_sensor, button, switch, number, select
		objectID  string
		payload   map[string]interface{}
	}

	mk := func(component, objectID, name string, extra map[string]interface{}) entry {
		base := map[string]interface{}{
			"name":              name,
			"has_entity_name":   true,
			"unique_id":         "hcbridge_" + slug + "_" + objectID,
			"object_id":         slug + "_" + objectID,
			"device":            device,
			"availability":      availability,
			"availability_mode": "all",
		}
		for k, v := range extra {
			base[k] = v
		}
		return entry{component, objectID, base}
	}

	st := func(suffix string) string { return cfg.StateTopic(suffix) }
	cm := func(suffix string) string { return cfg.CmdTopic(suffix) }

	entries := []entry{
		mk("sensor", "operation_state", "Operation state", map[string]interface{}{
			"state_topic": st("status/operation_state"),
			"icon":        "mdi:pot-steam",
		}),
		mk("sensor", "power_state", "Power state", map[string]interface{}{
			"state_topic": st("status/power_state"),
			"icon":        "mdi:power",
		}),
		mk("sensor", "alarm_clock_seconds", "Alarm clock", map[string]interface{}{
			"state_topic":         st("status/alarm_clock_seconds"),
			"unit_of_measurement": "s",
			"device_class":        "duration",
			"icon":                "mdi:timer-outline",
		}),
		mk("sensor", "remaining_program_time", "Remaining program time", map[string]interface{}{
			"state_topic":         st("status/remaining_program_time"),
			"unit_of_measurement": "s",
			"device_class":        "duration",
			"icon":                "mdi:timer-sand",
		}),
		mk("sensor", "active_program", "Active program", map[string]interface{}{
			"state_topic": st("status/active_program"),
			"icon":        "mdi:pot",
		}),
		mk("sensor", "program_duration", "Program duration", map[string]interface{}{
			"state_topic":         st("status/program_duration"),
			"unit_of_measurement": "s",
			"device_class":        "duration",
			"icon":                "mdi:timer-cog",
		}),
		mk("sensor", "elapsed_program_time", "Elapsed program time", map[string]interface{}{
			"state_topic":         st("status/elapsed_program_time"),
			"unit_of_measurement": "s",
			"device_class":        "duration",
			"icon":                "mdi:clock-start",
		}),
		mk("sensor", "program_progress", "Program progress", map[string]interface{}{
			"state_topic":         st("status/program_progress"),
			"unit_of_measurement": "%",
			"icon":                "mdi:progress-clock",
		}),
		mk("sensor", "focused_zone", "Focused zone", map[string]interface{}{
			"state_topic": st("status/focused_zone"),
			"icon":        "mdi:circle-double",
		}),
		mk("sensor", "focused_zone_power", "Focused zone power", map[string]interface{}{
			"state_topic": st("status/focused_zone_power"),
			"icon":        "mdi:fire",
		}),
		mk("binary_sensor", "focused_zone_joined", "Zones joined", map[string]interface{}{
			"state_topic":  st("status/focused_zone_joined"),
			"payload_on":   "true",
			"payload_off":  "false",
			"icon":         "mdi:link-variant",
		}),

		// Per-zone "last observed" power level. The bridge maintains a state
		// map for all 4 zones and updates each one whenever the panel's focus
		// shifts to it. Resets to "off" when PowerState=Off.
		mk("sensor", "zone_front_left_power", "Front-left zone", map[string]interface{}{
			"state_topic": "homeconnect/" + slug + "/zone/front_left/power",
			"icon":        "mdi:circle-slice-1",
		}),
		mk("sensor", "zone_front_right_power", "Front-right zone", map[string]interface{}{
			"state_topic": "homeconnect/" + slug + "/zone/front_right/power",
			"icon":        "mdi:circle-slice-2",
		}),
		mk("sensor", "zone_rear_left_power", "Rear-left zone", map[string]interface{}{
			"state_topic": "homeconnect/" + slug + "/zone/rear_left/power",
			"icon":        "mdi:circle-slice-3",
		}),
		mk("sensor", "zone_rear_right_power", "Rear-right zone", map[string]interface{}{
			"state_topic": "homeconnect/" + slug + "/zone/rear_right/power",
			"icon":        "mdi:circle-slice-4",
		}),
		mk("binary_sensor", "joined_left", "Left zones joined", map[string]interface{}{
			"state_topic": "homeconnect/" + slug + "/joined_left",
			"payload_on":  "true",
			"payload_off": "false",
			"icon":        "mdi:link-variant",
		}),
		mk("binary_sensor", "joined_right", "Right zones joined", map[string]interface{}{
			"state_topic": "homeconnect/" + slug + "/joined_right",
			"payload_on":  "true",
			"payload_off": "false",
			"icon":        "mdi:link-variant",
		}),

		// Program-level timer deadline as a device_class=timestamp sensor.
		// Bosch's API exposes only one RemainingProgramTime field per active
		// program (not per zone — see README), so we publish one entity at
		// the program level. HA renders this as a live countdown ("in 3
		// minutes"). Empty string → value_template returns None → entity
		// shows "unknown" instead of a stale past timestamp.
		mk("sensor", "program_timer", "Program timer", map[string]interface{}{
			"state_topic":    st("status/program_timer_deadline"),
			"device_class":   "timestamp",
			"value_template": "{{ value if value else none }}",
			"icon":           "mdi:timer-sand",
		}),

		mk("binary_sensor", "child_lock", "Child lock", map[string]interface{}{
			"state_topic":   st("status/child_lock"),
			"payload_on":    "true",
			"payload_off":   "false",
			"device_class":  "lock",
			"icon":          "mdi:lock",
		}),
		mk("binary_sensor", "local_control", "Local control", map[string]interface{}{
			"state_topic":  st("status/local_control"),
			"payload_on":   "true",
			"payload_off":  "false",
			"icon":         "mdi:gesture-tap",
		}),
		mk("binary_sensor", "remote_start_allowed", "Remote start allowed", map[string]interface{}{
			"state_topic": st("status/remote_start_allowed"),
			"payload_on":  "true",
			"payload_off": "false",
			"icon":        "mdi:remote",
		}),

		// Bosch Home Connect 1000-call/day quota. When the bridge gets a 429
		// it sets a global block; per-zone polls and MQTT-driven writes are
		// suppressed until rate_limited_until passes. SSE keeps streaming.
		mk("binary_sensor", "rate_limited", "API rate limited", map[string]interface{}{
			"state_topic":  st("status/rate_limited"),
			"payload_on":   "true",
			"payload_off":  "false",
			"device_class": "problem",
			"icon":         "mdi:speedometer-slow",
		}),
		mk("sensor", "rate_limited_until", "API rate limit clears", map[string]interface{}{
			"state_topic":    st("status/rate_limited_until"),
			"device_class":   "timestamp",
			"value_template": "{{ value if value else none }}",
			"icon":           "mdi:clock-alert-outline",
		}),

		mk("number", "alarm_clock_minutes", "Alarm clock", map[string]interface{}{
			"command_topic":           cm("alarm_clock_minutes"),
			"state_topic":             st("status/alarm_clock_minutes"),
			"min":                     0,
			"max":                     639,
			"step":                    1,
			"mode":                    "box",
			"unit_of_measurement":     "min",
			"icon":                    "mdi:timer",
		}),

	}

	for _, e := range entries {
		topic := fmt.Sprintf("%s/%s/hcbridge_%s/%s/config",
			cfg.MQTT.DiscoveryTopic, e.component, slug, e.objectID)
		body, err := json.Marshal(e.payload)
		if err != nil {
			return fmt.Errorf("marshal discovery for %s: %w", e.objectID, err)
		}
		t := cli.Publish(topic, 1, true, body)
		t.Wait()
		if err := t.Error(); err != nil {
			return fmt.Errorf("publish discovery for %s: %w", e.objectID, err)
		}
	}
	log.Info("MQTT discovery published (%d entities)", len(entries))
	return nil
}
