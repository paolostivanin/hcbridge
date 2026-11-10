//go:build e2e

// End-to-end test against a real MQTT broker and a fake hob speaking the
// local protocol:
//
//	docker run --rm -d --name hcb-mqtt -p 127.0.0.1:18831:1883 eclipse-mosquitto mosquitto -c /mosquitto-no-auth.conf
//	HCBRIDGE_E2E_BROKER=127.0.0.1:18831 go test -tags e2e -race -run E2E -v ./...
package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	hc "hcbridge/src/internal/homeconnect"
	"hcbridge/src/internal/homeconnect/hcfake"
)

type retained struct {
	mu sync.Mutex
	m  map[string]string
}

func (r *retained) get(topic string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.m[topic]
	return v, ok
}

func TestE2E(t *testing.T) {
	broker := os.Getenv("HCBRIDGE_E2E_BROKER")
	if broker == "" {
		t.Skip("HCBRIDGE_E2E_BROKER not set")
	}
	host, portStr, _ := net.SplitHostPort(broker)
	port, _ := strconv.Atoi(portStr)

	// --- fake hob: on, front-left at 5 with a timer, fan at 4.5 ---------------
	hob := hcfake.New()
	defer hob.Close()
	hob.SetValues(map[int]any{
		531:  2,  // PowerState On
		552:  3,  // OperationState Run
		8202: 2,  // Zone 100 State Active
		8204: 10, // Zone 100 PowerLevel "50"
		8209: 300,
		4409: 18, // Ventilation Level08
	})

	// --- broker state left by 1.x and the cloud draft -------------------------
	seed := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + broker).SetClientID("e2e-seed"))
	if tok := seed.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("seed connect: %v", tok.Error())
	}
	stale := map[string]string{
		"homeassistant/number/hcbridge_cooktop/alarm_clock_minutes/config":        `{"name":"old"}`,
		"homeassistant/binary_sensor/hcbridge_cooktop/rate_limited/config":        `{"name":"old"}`,
		"homeassistant/binary_sensor/hcbridge_cooktop/appliance_connected/config": `{"name":"old"}`,
		"homeconnect/cooktop/status/focused_zone_power":                           "5",
		"homeconnect/cooktop/status/rate_limited":                                 "false",
		"homeconnect/cooktop/status/appliance_online":                             "true",
		"homeconnect/cooktop/cloud":                                               "online",
	}
	for topic, payload := range stale {
		seed.Publish(topic, 1, true, payload).WaitTimeout(5 * time.Second)
	}
	seed.Disconnect(100)

	// --- bridge ---------------------------------------------------------------
	dir := t.TempDir()
	cfg, err := parseConfig([]byte("appliance:\n  host: " + hob.Host() + "\n  profile: /unused\nstate_dir: " + dir + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MQTT.Host, cfg.MQTT.Port = host, port
	cfg.MQTT.ClientID = "hcbridge-e2e"
	log := NewLogger("", "DEBUG")
	bridge := NewBridge(cfg, deviceMeta{}, log)
	if err := bridge.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	done := make(chan struct{})
	ctl := newController(cfg, hcfake.Profile(), "0a1b2c3d", bridge, log)
	ctl.offlineGrace = 200 * time.Millisecond
	ctl.evalEvery = 50 * time.Millisecond
	ctl.link.mon.threshold = 500 * time.Millisecond
	ctl.optsHook = func(o *hc.Options) {
		o.BackoffMin, o.BackoffMax, o.MinDialGap = 50*time.Millisecond, 200*time.Millisecond, 20*time.Millisecond
	}
	go func() { ctl.run(ctx, cancel, sigCh); bridge.Disconnect(); close(done) }()

	// --- observer -------------------------------------------------------------
	seen := &retained{m: map[string]string{}}
	obs := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + broker).SetClientID("e2e-obs"))
	if tok := obs.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("observer connect: %v", tok.Error())
	}
	defer obs.Disconnect(100)
	obs.SubscribeMultiple(map[string]byte{"homeconnect/#": 1, "homeassistant/+/hcbridge_cooktop/#": 1}, func(_ mqtt.Client, m mqtt.Message) {
		seen.mu.Lock()
		if len(m.Payload()) == 0 {
			delete(seen.m, m.Topic())
		} else {
			seen.m[m.Topic()] = string(m.Payload())
		}
		seen.mu.Unlock()
	}).WaitTimeout(5 * time.Second)

	want := func(topic, value string) {
		t.Helper()
		if !waitFor(t, 5*time.Second, func() bool { v, _ := seen.get(topic); return v == value }) {
			v, present := seen.get(topic)
			t.Errorf("%s = %q (present=%v), want %q", topic, v, present, value)
		}
	}

	want("homeconnect/cooktop/connected", "online")
	want("homeconnect/cooktop/link", "online")
	want("homeconnect/cooktop/status/link_state", "connected")
	want("homeconnect/cooktop/status/power_cycle_needed", "false")
	want("homeconnect/cooktop/status/power_state", "On")
	want("homeconnect/cooktop/status/operation_state", "Run")
	want("homeconnect/cooktop/zone/front_left/power", "5")
	want("homeconnect/cooktop/zone/front_left/state", "active")
	want("homeconnect/cooktop/status/fan_level", "4.5")
	if v, _ := seen.get("homeconnect/cooktop/zone/front_left/timer_deadline"); v == "" {
		t.Error("no zone timer deadline")
	}
	if v, _ := seen.get("homeconnect/cooktop/status/heartbeat"); v == "" {
		t.Error("no heartbeat")
	}

	// Pushes, including a value that arrives inside an access change.
	hob.Notify(map[int]any{4409: 95})
	want("homeconnect/cooktop/status/fan_level", "boost1")
	hob.NotifyDescription(4409, "READ", 101)
	want("homeconnect/cooktop/status/fan_level", "afterrun")

	// Stale topics and entities from earlier versions are gone.
	for topic := range stale {
		if !waitFor(t, 5*time.Second, func() bool { _, ok := seen.get(topic); return !ok }) {
			v, _ := seen.get(topic)
			t.Errorf("removed topic %s still retained: %q", topic, v)
		}
	}

	// Discovery: every entity, IDs pinned, firmware in the device block.
	if !waitFor(t, 5*time.Second, func() bool {
		v, _ := seen.get("homeassistant/sensor/hcbridge_cooktop/fan_level/config")
		return strings.Contains(v, hcfake.SwVersion)
	}) {
		t.Error("discovery not republished with the firmware versions")
	}
	n := 0
	seen.mu.Lock()
	for topic, payload := range seen.m {
		if !strings.HasPrefix(topic, "homeassistant/") {
			continue
		}
		var p map[string]interface{}
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			t.Errorf("%s: %v", topic, err)
			continue
		}
		uid, _ := p["unique_id"].(string)
		if want, ok := liveEntityIDs[uid]; !ok || p["default_entity_id"] != want {
			t.Errorf("%s: unique_id %v default_entity_id %v", topic, p["unique_id"], p["default_entity_id"])
		}
		n++
	}
	seen.mu.Unlock()
	if n != len(liveEntityIDs) {
		t.Errorf("%d discovery configs, want %d", n, len(liveEntityIDs))
	}

	// A broker-side wipe of a state topic is restored on reconnect: drop the
	// bridge's connection by kicking it with a duplicate client ID.
	kick := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + broker).SetClientID("hcbridge-e2e"))
	kick.Connect().WaitTimeout(5 * time.Second)
	kick.Publish("homeconnect/cooktop/zone/front_left/power", 1, true, "").WaitTimeout(5 * time.Second)
	kick.Disconnect(100)
	want("homeconnect/cooktop/zone/front_left/power", "5")

	// Power off: zones and timers reset.
	hob.Notify(map[int]any{531: 1})
	want("homeconnect/cooktop/status/power_state", "Off")
	want("homeconnect/cooktop/zone/front_left/power", "off")
	if !waitFor(t, 5*time.Second, func() bool {
		v, _ := seen.get("homeconnect/cooktop/zone/front_left/timer_deadline")
		return v == "None"
	}) {
		t.Error("zone timer not cleared at power-off")
	}

	// The hob starts refusing local sessions: refused, data unavailable, and
	// after the (shrunk) threshold the power-cycle flag. Then it recovers.
	hob.Refuse(503, nil, "")
	hob.Drop()
	want("homeconnect/cooktop/status/link_state", "refused")
	want("homeconnect/cooktop/link", "offline")
	want("homeconnect/cooktop/status/power_cycle_needed", "true")
	hob.Refuse(0, nil, "")
	want("homeconnect/cooktop/status/link_state", "connected")
	want("homeconnect/cooktop/status/power_cycle_needed", "false")
	want("homeconnect/cooktop/link", "online")

	// Shutdown: Close 1000 to the hob, last will offline, state persisted.
	sigCh <- syscall.SIGTERM
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("controller did not stop")
	}
	want("homeconnect/cooktop/connected", "offline")
	if codes := hob.CloseCodes(); len(codes) == 0 || codes[len(codes)-1] != 1000 {
		t.Errorf("close codes %v", codes)
	}
	for _, f := range []string{"alive", "device_id", "device.json", "link.json"} {
		if f == "device_id" {
			continue // the controller is handed its ID; main creates the file
		}
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("%s not written: %v", f, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "calls.json")); err == nil {
		t.Error("calls.json written: the cloud call log is gone in 2.0.0")
	}
}
