package main

import (
	"encoding/json"
	"fmt"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// discoveryEntity is one HA MQTT discovery config.
type discoveryEntity struct {
	component string // sensor, binary_sensor
	objectID  string // part of unique_id and of the discovery topic; never change it
	// entityID is the entity_id HA should use. Since HA dropped the
	// discovery `object_id` (2026.4) IDs were generated from device name +
	// entity name; default_entity_id pins them, so a re-registration (e.g.
	// after the device is deleted in HA) recreates the IDs the dashboard
	// uses instead of deriving new ones. For an already registered entity
	// HA keeps the registry entry, keyed by unique_id.
	entityID   string
	name       string
	diagnostic bool // bridge health rather than appliance data
	extra      map[string]interface{}
}

// removedEntity is a discovery config published by versions before 2.0.0.
type removedEntity struct{ component, objectID string }

// Entities removed in 2.0.0 as unused: no dashboard, automation or template
// referenced them, and the alarm clock number was the bridge's only MQTT write
// path (0 commands in six months) - removing it also removed the way for
// anyone on the broker to burn the API quota with writes. clearRemoved()
// publishes an empty config for each, which deletes the entity from HA.
var removedDiscovery = []removedEntity{
	{"number", "alarm_clock_minutes"},
	{"sensor", "alarm_clock_seconds"},
	{"sensor", "remaining_program_time"},
	{"sensor", "focused_zone_power"},
	{"binary_sensor", "focused_zone_joined"},
	{"binary_sensor", "remote_start_allowed"},
}

func discoveryTopic(cfg *Config, component, objectID string) string {
	return fmt.Sprintf("%s/%s/hcbridge_%s/%s/config",
		cfg.MQTT.DiscoveryTopic, component, cfg.Appliance.TopicSlug, objectID)
}

func discoveryEntities(cfg *Config) []discoveryEntity {
	slug := cfg.Appliance.TopicSlug
	st := func(suffix string) string { return cfg.StateTopic(suffix) }
	id := func(component, name string) string { return component + "." + slug + "_" + name }
	boolPayloads := func(m map[string]interface{}) map[string]interface{} {
		m["payload_on"] = "true"
		m["payload_off"] = "false"
		return m
	}
	zone := func(objectID, slugName, name, icon string) discoveryEntity {
		return discoveryEntity{"sensor", objectID, id("sensor", slugName), name, false, map[string]interface{}{
			"state_topic": st("zone/" + slugName[:len(slugName)-len("_zone")] + "/power"),
			"icon":        icon,
		}}
	}

	return []discoveryEntity{
		{"sensor", "operation_state", id("sensor", "operation_state"), "Operation state", false, map[string]interface{}{
			"state_topic": st("status/operation_state"),
			"icon":        "mdi:pot-steam",
		}},
		{"sensor", "power_state", id("sensor", "power_state"), "Power state", false, map[string]interface{}{
			"state_topic": st("status/power_state"),
			"icon":        "mdi:power",
		}},
		{"sensor", "active_program", id("sensor", "active_program"), "Active program", false, map[string]interface{}{
			"state_topic": st("status/active_program"),
			"icon":        "mdi:pot",
		}},
		{"sensor", "program_duration", id("sensor", "program_duration"), "Program duration", false, map[string]interface{}{
			"state_topic":         st("status/program_duration"),
			"unit_of_measurement": "s",
			"device_class":        "duration",
			"icon":                "mdi:timer-cog",
		}},
		{"sensor", "elapsed_program_time", id("sensor", "elapsed_program_time"), "Elapsed program time", false, map[string]interface{}{
			"state_topic":         st("status/elapsed_program_time"),
			"unit_of_measurement": "s",
			"device_class":        "duration",
			"icon":                "mdi:clock-start",
		}},
		{"sensor", "program_progress", id("sensor", "program_progress"), "Program progress", false, map[string]interface{}{
			"state_topic":         st("status/program_progress"),
			"unit_of_measurement": "%",
			"icon":                "mdi:progress-clock",
		}},
		{"sensor", "focused_zone", id("sensor", "focused_zone"), "Focused zone", false, map[string]interface{}{
			"state_topic": st("status/focused_zone"),
			"icon":        "mdi:circle-double",
		}},

		// Per-zone "last observed" power level. The bridge maintains a state
		// map for all 4 zones and updates each one whenever the panel's focus
		// shifts to it. Resets to "off" when PowerState=Off.
		zone("zone_front_left_power", "front_left_zone", "Front-left zone", "mdi:circle-slice-1"),
		zone("zone_front_right_power", "front_right_zone", "Front-right zone", "mdi:circle-slice-2"),
		zone("zone_rear_left_power", "rear_left_zone", "Rear-left zone", "mdi:circle-slice-3"),
		zone("zone_rear_right_power", "rear_right_zone", "Rear-right zone", "mdi:circle-slice-4"),
		{"binary_sensor", "joined_left", id("binary_sensor", "left_zones_joined"), "Left zones joined", false, boolPayloads(map[string]interface{}{
			"state_topic": st("joined_left"),
			"icon":        "mdi:link-variant",
		})},
		{"binary_sensor", "joined_right", id("binary_sensor", "right_zones_joined"), "Right zones joined", false, boolPayloads(map[string]interface{}{
			"state_topic": st("joined_right"),
			"icon":        "mdi:link-variant",
		})},

		// Program-level timer deadline as a device_class=timestamp sensor.
		// Bosch's API exposes only one RemainingProgramTime per active
		// program (not per zone — see README). Empty string → value_template
		// returns None → "unknown" instead of a stale past timestamp.
		{"sensor", "program_timer", id("sensor", "program_timer"), "Program timer", false, map[string]interface{}{
			"state_topic":    st("status/program_timer_deadline"),
			"device_class":   "timestamp",
			"value_template": "{{ value if value else none }}",
			"icon":           "mdi:timer-sand",
		}},

		// No device_class: the "lock" class reads on=Unlocked, which showed an
		// engaged child lock as unlocked.
		{"binary_sensor", "child_lock", id("binary_sensor", "child_lock"), "Child lock", false, boolPayloads(map[string]interface{}{
			"state_topic": st("status/child_lock"),
			"icon":        "mdi:lock",
		})},

		// ---- diagnostics: bridge and link health -------------------------
		// These keep only the bridge's own availability topic: the rate-limit
		// card must stay visible exactly when the cloud link is down.
		{"binary_sensor", "local_control", id("binary_sensor", "local_control"), "Local control", true, boolPayloads(map[string]interface{}{
			"state_topic": st("status/local_control"),
			"icon":        "mdi:gesture-tap",
		})},
		{"binary_sensor", "rate_limited", id("binary_sensor", "api_rate_limited"), "API rate limited", true, boolPayloads(map[string]interface{}{
			"state_topic":  st("status/rate_limited"),
			"device_class": "problem",
			"icon":         "mdi:speedometer-slow",
		})},
		{"sensor", "rate_limited_until", id("sensor", "api_rate_limit_clears"), "API rate limit clears", true, map[string]interface{}{
			"state_topic":    st("status/rate_limited_until"),
			"device_class":   "timestamp",
			"value_template": "{{ value if value else none }}",
			"icon":           "mdi:clock-alert-outline",
		}},
		{"sensor", "api_calls_24h", id("sensor", "api_calls_24h"), "API calls 24h", true, map[string]interface{}{
			"state_topic": st("status/api_calls_24h"),
			"state_class": "measurement",
			"icon":        "mdi:counter",
		}},
		{"binary_sensor", "cloud_connection", id("binary_sensor", "cloud_connection"), "Cloud connection", true, map[string]interface{}{
			"state_topic":  st("cloud"),
			"payload_on":   "online",
			"payload_off":  "offline",
			"device_class": "connectivity",
		}},
		{"binary_sensor", "appliance_connected", id("binary_sensor", "appliance_connected"), "Appliance connected", true, boolPayloads(map[string]interface{}{
			"state_topic":  st("status/appliance_online"),
			"device_class": "connectivity",
		})},
		{"sensor", "heartbeat", id("sensor", "bridge_heartbeat"), "Bridge heartbeat", true, map[string]interface{}{
			"state_topic":    st("status/heartbeat"),
			"device_class":   "timestamp",
			"value_template": "{{ value if value else none }}",
			"icon":           "mdi:heart-pulse",
		}},
	}
}

// discoveryPayload builds the config message for one entity.
func discoveryPayload(cfg *Config, e discoveryEntity) map[string]interface{} {
	slug := cfg.Appliance.TopicSlug
	bridgeAvail := map[string]string{
		"topic":                 cfg.StateTopic("connected"),
		"payload_available":     "online",
		"payload_not_available": "offline",
	}
	cloudAvail := map[string]string{
		"topic":                 cfg.StateTopic("cloud"),
		"payload_available":     "online",
		"payload_not_available": "offline",
	}
	p := map[string]interface{}{
		"name":              e.name,
		"unique_id":         "hcbridge_" + slug + "_" + e.objectID,
		"default_entity_id": e.entityID,
		"device": map[string]interface{}{
			"identifiers":  []string{"hcbridge_" + slug},
			"name":         cfg.Appliance.FriendlyID,
			"manufacturer": "Bosch",
			"model":        "Home Connect cooktop",
		},
		"origin": map[string]interface{}{
			"name": "hcbridge",
			"sw":   version,
			"url":  "https://github.com/paolostivanin/hcbridge",
		},
	}
	if e.diagnostic {
		p["entity_category"] = "diagnostic"
		p["availability"] = []map[string]string{bridgeAvail}
	} else {
		// Appliance data is only trustworthy while the bridge is up AND has
		// a live event stream; otherwise HA shows it as unavailable instead
		// of frozen at the last value.
		p["availability"] = []map[string]string{bridgeAvail, cloudAvail}
		p["availability_mode"] = "all"
	}
	for k, v := range e.extra {
		p[k] = v
	}
	return p
}

// PublishDiscovery emits Home Assistant MQTT discovery configs for all entities.
// Topics: homeassistant/<component>/hcbridge_<slug>/<object_id>/config (retained).
// All entities share the same `device` block so HA groups them.
func PublishDiscovery(cfg *Config, cli mqtt.Client, log *Logger) error {
	entities := discoveryEntities(cfg)
	for _, e := range entities {
		body, err := json.Marshal(discoveryPayload(cfg, e))
		if err != nil {
			return fmt.Errorf("marshal discovery for %s: %w", e.objectID, err)
		}
		t := cli.Publish(discoveryTopic(cfg, e.component, e.objectID), 1, true, body)
		if !t.WaitTimeout(publishTimeout) {
			return fmt.Errorf("publish discovery for %s: no ack within %s", e.objectID, publishTimeout)
		}
		if err := t.Error(); err != nil {
			return fmt.Errorf("publish discovery for %s: %w", e.objectID, err)
		}
	}
	log.Info("MQTT discovery published (%d entities)", len(entities))
	return nil
}
