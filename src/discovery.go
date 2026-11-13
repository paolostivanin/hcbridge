package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	hc "hcbridge/src/internal/homeconnect"
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
	entityID string
	name     string
	// diagnostic puts the entity in HA's diagnostic category.
	diagnostic bool
	// gated: appliance data, only trustworthy while the bridge has a live
	// session with the hob. Availability is then the bridge's last will AND
	// the link topic, so HA shows the entity as unavailable instead of
	// frozen. Link health itself is never gated.
	gated bool
	extra map[string]interface{}
}

// removedEntity is a discovery config published by an earlier version.
type removedEntity struct{ component, objectID string }

// Entities earlier versions published. clearRemoved() publishes an empty
// config for each, which deletes the entity from HA.
var removedDiscovery = []removedEntity{
	// Removed as unused in the cloud 2.0 rewrite (the alarm clock number was
	// the bridge's only MQTT write path).
	{"number", "alarm_clock_minutes"},
	{"sensor", "alarm_clock_seconds"},
	{"sensor", "remaining_program_time"},
	{"sensor", "focused_zone_power"},
	{"binary_sensor", "focused_zone_joined"},
	{"binary_sensor", "remote_start_allowed"},
	// Cloud API only: gone with the local transport.
	{"binary_sensor", "rate_limited"},
	{"sensor", "rate_limited_until"},
	{"sensor", "api_calls_24h"},
	{"binary_sensor", "cloud_connection"},
	{"binary_sensor", "appliance_connected"},
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
	timestamp := func(topic, icon string) map[string]interface{} {
		return map[string]interface{}{
			"state_topic":    topic,
			"device_class":   "timestamp",
			"value_template": "{{ value if value else none }}",
			"icon":           icon,
		}
	}
	data := func(component, objectID, entity, name string, extra map[string]interface{}) discoveryEntity {
		return discoveryEntity{component, objectID, id(component, entity), name, false, true, extra}
	}

	zones := []struct{ zone, label, icon string }{
		{"FrontLeft", "Front-left", "mdi:circle-slice-1"},
		{"FrontRight", "Front-right", "mdi:circle-slice-2"},
		{"RearLeft", "Rear-left", "mdi:circle-slice-3"},
		{"RearRight", "Rear-right", "mdi:circle-slice-4"},
	}

	es := []discoveryEntity{
		data("sensor", "operation_state", "operation_state", "Operation state", map[string]interface{}{
			"state_topic": st("status/operation_state"),
			"icon":        "mdi:pot-steam",
		}),
		data("sensor", "power_state", "power_state", "Power state", map[string]interface{}{
			"state_topic": st("status/power_state"),
			"icon":        "mdi:power",
		}),
		data("sensor", "active_program", "active_program", "Active program", map[string]interface{}{
			"state_topic": st("status/active_program"),
			"icon":        "mdi:pot",
		}),
		data("sensor", "program_duration", "program_duration", "Program duration", map[string]interface{}{
			"state_topic":         st("status/program_duration"),
			"unit_of_measurement": "s",
			"device_class":        "duration",
			"icon":                "mdi:timer-cog",
		}),
		data("sensor", "elapsed_program_time", "elapsed_program_time", "Elapsed program time", map[string]interface{}{
			"state_topic":         st("status/elapsed_program_time"),
			"unit_of_measurement": "s",
			"device_class":        "duration",
			"icon":                "mdi:clock-start",
		}),
		data("sensor", "program_progress", "program_progress", "Program progress", map[string]interface{}{
			"state_topic":         st("status/program_progress"),
			"unit_of_measurement": "%",
			"icon":                "mdi:progress-clock",
		}),
		// The zone the panel has focus on (the hob pushes it on every touch).
		data("sensor", "focused_zone", "focused_zone", "Focused zone", map[string]interface{}{
			"state_topic": st("status/focused_zone"),
			"icon":        "mdi:circle-double",
		}),
	}
	for _, z := range zones {
		zs := zoneSlug(z.zone)
		es = append(es,
			data("sensor", "zone_"+zs+"_power", zs+"_zone", z.label+" zone", map[string]interface{}{
				"state_topic": st("zone/" + zs + "/power"),
				"icon":        z.icon,
			}),
			data("sensor", "zone_"+zs+"_state", zs+"_zone_state", z.label+" zone state", map[string]interface{}{
				"state_topic": st("zone/" + zs + "/state"),
				"icon":        "mdi:fire",
			}),
			data("sensor", "zone_"+zs+"_timer", zs+"_zone_timer", z.label+" zone timer",
				timestamp(st("zone/"+zs+"/timer_deadline"), "mdi:timer-sand")),
		)
	}
	es = append(es,
		data("binary_sensor", "joined_left", "left_zones_joined", "Left zones joined", boolPayloads(map[string]interface{}{
			"state_topic": st("joined_left"),
			"icon":        "mdi:link-variant",
		})),
		data("binary_sensor", "joined_right", "right_zones_joined", "Right zones joined", boolPayloads(map[string]interface{}{
			"state_topic": st("joined_right"),
			"icon":        "mdi:link-variant",
		})),
		// The program-level remaining time as a deadline. It is the timer of
		// the zone the panel has focus on, not the soonest one: for a single
		// countdown use the zone timers above.
		data("sensor", "program_timer", "program_timer", "Program timer",
			timestamp(st("status/program_timer_deadline"), "mdi:timer-sand")),
		// No device_class: the "lock" class reads on=Unlocked, which showed an
		// engaged child lock as unlocked.
		data("binary_sensor", "child_lock", "child_lock", "Child lock", boolPayloads(map[string]interface{}{
			"state_topic": st("status/child_lock"),
			"icon":        "mdi:lock",
		})),
		data("sensor", "fan_level", "fan_level", "Fan level", map[string]interface{}{
			"state_topic": st("status/fan_level"),
			"icon":        "mdi:fan",
		}),
		data("sensor", "carbon_filter_saturation", "carbon_filter_saturation", "Carbon filter saturation", map[string]interface{}{
			"state_topic":         st("status/carbon_filter_saturation"),
			"unit_of_measurement": "%",
			"state_class":         "measurement",
			"icon":                "mdi:air-filter",
		}),
		data("binary_sensor", "carbon_filter_replace", "carbon_filter_replace", "Carbon filter replace", boolPayloads(map[string]interface{}{
			"state_topic":  st("status/carbon_filter_replace"),
			"device_class": "problem",
			"icon":         "mdi:air-filter",
		})),
	)

	// ---- diagnostics -------------------------------------------------------
	// Appliance-side diagnostics are gated like the data; the link health
	// and the heartbeat keep only the bridge's own availability, so they stay
	// visible exactly when the hob cannot be reached.
	diag := func(e discoveryEntity) discoveryEntity { e.diagnostic = true; return e }
	es = append(es,
		diag(data("binary_sensor", "local_control", "local_control", "Local control", boolPayloads(map[string]interface{}{
			"state_topic": st("status/local_control"),
			"icon":        "mdi:gesture-tap",
		}))),
		diag(data("sensor", "wifi_signal", "wifi_signal", "Wi-Fi signal", map[string]interface{}{
			"state_topic":         st("status/wifi_signal"),
			"unit_of_measurement": "dBm",
			"device_class":        "signal_strength",
			"state_class":         "measurement",
		})),
		discoveryEntity{"sensor", "link_state", id("sensor", "local_link"), "Local link", true, false, map[string]interface{}{
			"state_topic":  st("status/link_state"),
			"device_class": "enum",
			"options":      linkStateOptions(),
			"icon":         "mdi:lan-connect",
		}},
		discoveryEntity{"sensor", "link_since", id("sensor", "local_link_since"), "Local link since", true, false,
			timestamp(st("status/link_since"), "mdi:clock-outline")},
		discoveryEntity{"binary_sensor", "power_cycle_needed", id("binary_sensor", "power_cycle_needed"), "Power cycle needed", true, false, boolPayloads(map[string]interface{}{
			"state_topic":  st("status/power_cycle_needed"),
			"device_class": "problem",
			"icon":         "mdi:power-cycle",
		})},
		discoveryEntity{"sensor", "heartbeat", id("sensor", "bridge_heartbeat"), "Bridge heartbeat", true, false,
			timestamp(st("status/heartbeat"), "mdi:heart-pulse")},
	)
	return es
}

func linkStateOptions() []string {
	out := make([]string, 0, len(hc.LinkStates))
	for _, s := range hc.LinkStates {
		out = append(out, string(s))
	}
	return out
}

// deviceBlock groups every entity under one HA device. The identifiers
// never change (they key the device registry); the rest describes the hob.
func deviceBlock(cfg *Config, meta deviceMeta) map[string]interface{} {
	d := map[string]interface{}{
		"identifiers":  []string{"hcbridge_" + cfg.Appliance.TopicSlug},
		"name":         cfg.Appliance.FriendlyID,
		"manufacturer": "Bosch",
		"model":        "Home Connect cooktop",
	}
	if meta.Brand != "" {
		d["manufacturer"] = titleCase(meta.Brand)
	}
	if meta.VIB != "" {
		d["model"] = meta.VIB
	}
	if meta.Serial != "" {
		d["serial_number"] = meta.Serial
	}
	// As the Home Connect app shows it: appliance-network module.
	switch {
	case meta.HaVersion != "" && meta.SwVersion != "":
		d["sw_version"] = meta.HaVersion + "-" + meta.SwVersion
	case meta.SwVersion != "":
		d["sw_version"] = meta.SwVersion
	}
	if meta.HwVersion != "" {
		d["hw_version"] = meta.HwVersion
	}
	return d
}

func titleCase(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + strings.ToLower(s[1:])
}

// discoveryPayload builds the config message for one entity.
func discoveryPayload(cfg *Config, meta deviceMeta, e discoveryEntity) map[string]interface{} {
	slug := cfg.Appliance.TopicSlug
	bridgeAvail := map[string]string{
		"topic":                 cfg.StateTopic("connected"),
		"payload_available":     "online",
		"payload_not_available": "offline",
	}
	linkAvail := map[string]string{
		"topic":                 cfg.StateTopic("link"),
		"payload_available":     "online",
		"payload_not_available": "offline",
	}
	p := map[string]interface{}{
		"name":              e.name,
		"unique_id":         "hcbridge_" + slug + "_" + e.objectID,
		"default_entity_id": e.entityID,
		"device":            deviceBlock(cfg, meta),
		"origin": map[string]interface{}{
			"name": "hcbridge",
			"sw":   version,
			"url":  "https://github.com/paolostivanin/hcbridge",
		},
	}
	if e.diagnostic {
		p["entity_category"] = "diagnostic"
	}
	if e.gated {
		p["availability"] = []map[string]string{bridgeAvail, linkAvail}
		p["availability_mode"] = "all"
	} else {
		p["availability"] = []map[string]string{bridgeAvail}
	}
	for k, v := range e.extra {
		p[k] = v
	}
	return p
}

// maxDiscoveryFailures: consecutive failed publishes after which a discovery
// pass gives up. Each publish may wait publishTimeout for an ack, so an
// unresponsive broker must not be ground through 32 times.
const maxDiscoveryFailures = 3

// PublishDiscovery emits Home Assistant MQTT discovery configs for all entities.
// Topics: homeassistant/<component>/hcbridge_<slug>/<object_id>/config (retained).
// All entities share the same `device` block so HA groups them.
//
// One failed entity does not stop the others: the pass carries on, and what
// failed is retried once. It stops early when the connection is gone or the
// broker keeps failing; whatever is missing is published again by the next
// connect.
func PublishDiscovery(cfg *Config, meta deviceMeta, cli mqtt.Client, log *Logger) error {
	if cli == nil {
		return nil
	}
	entities := discoveryEntities(cfg)
	published := 0
	publishOne := func(e discoveryEntity) error {
		body, err := json.Marshal(discoveryPayload(cfg, meta, e))
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
		published++
		return nil
	}
	// pass publishes list and returns what failed; aborted means it stopped
	// early (connection lost, or too many failures in a row).
	pass := func(list []discoveryEntity) (failed []discoveryEntity, errs []error, aborted bool) {
		consecutive := 0
		for _, e := range list {
			if !cli.IsConnectionOpen() {
				return failed, append(errs, errors.New("MQTT connection lost during discovery")), true
			}
			if err := publishOne(e); err != nil {
				failed, errs = append(failed, e), append(errs, err)
				if consecutive++; consecutive >= maxDiscoveryFailures {
					return failed, append(errs, fmt.Errorf("giving up after %d failures in a row", consecutive)), true
				}
				continue
			}
			consecutive = 0
		}
		return failed, errs, false
	}

	failed, errs, aborted := pass(entities)
	if len(failed) > 0 && !aborted {
		_, errs, _ = pass(failed) // one retry; only what still fails is reported
	}
	if len(errs) == 0 {
		log.Info("MQTT discovery published (%d entities)", published)
		return nil
	}
	log.Warn("MQTT discovery: %d of %d entities published (the rest follow on the next connect)", published, len(entities))
	return errors.Join(errs...)
}
