//go:build e2e

// End-to-end test against a real MQTT broker and a fake Home Connect API:
//
//	docker run --rm -d --name hcb-mqtt -p 127.0.0.1:18831:1883 eclipse-mosquitto mosquitto -c /mosquitto-no-auth.conf
//	HCBRIDGE_E2E_BROKER=127.0.0.1:18831 go test -tags e2e -run E2E -v ./...
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
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

	// --- fake Home Connect API ---------------------------------------------
	events := make(chan string, 16)
	var mu sync.Mutex
	powerOn := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		mu.Lock()
		on := powerOn
		mu.Unlock()
		switch {
		case strings.HasSuffix(p, "/events"):
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			for {
				select {
				case ev := <-events:
					fmt.Fprint(w, ev)
					w.(http.Flusher).Flush()
				case <-r.Context().Done():
					return
				}
			}
		case strings.HasSuffix(p, "/status"):
			fmt.Fprint(w, `{"data":{"status":[{"key":"BSH.Common.Status.OperationState","value":"BSH.Common.EnumType.OperationState.Run"},{"key":"BSH.Common.Status.LocalControlActive","value":false}]}}`)
		case strings.HasSuffix(p, "/settings"):
			ps := "On"
			if !on {
				ps = "Off"
			}
			fmt.Fprintf(w, `{"data":{"settings":[{"key":"BSH.Common.Setting.PowerState","value":"BSH.Common.EnumType.PowerState.%s"},{"key":"BSH.Common.Setting.ChildLock","value":false}]}}`, ps)
		case strings.HasSuffix(p, "/programs/active/options"):
			fmt.Fprint(w, `{"data":{"options":[
				{"key":"Cooking.Hob.Option.ZoneSelector","value":"Cooking.Hob.EnumType.ZoneSelector.FrontLeft"},
				{"key":"Cooking.Hob.Option.PowerLevel","value":"Cooking.Hob.EnumType.PowerLevel.50"},
				{"key":"Cooking.Hob.Option.JoinZone","value":false},
				{"key":"BSH.Common.Option.RemainingProgramTime","value":300},
				{"key":"BSH.Common.Option.Duration","value":600},
				{"key":"BSH.Common.Option.ElapsedProgramTime","value":300},
				{"key":"BSH.Common.Option.ProgramProgress","value":50}]}}`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	// --- broker state left by a pre-2.0.0 bridge ------------------------------
	seed := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + broker).SetClientID("e2e-seed"))
	if tok := seed.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("seed connect: %v", tok.Error())
	}
	for topic, payload := range map[string]string{
		"homeassistant/number/hcbridge_cooktop/alarm_clock_minutes/config": `{"name":"old"}`,
		"homeconnect/cooktop/status/focused_zone_power":                    "5",
		"homeconnect/cooktop/status/remote_control_active":                 "false",
	} {
		seed.Publish(topic, 1, true, payload).WaitTimeout(5 * time.Second)
	}
	seed.Disconnect(100)

	// --- bridge ---------------------------------------------------------------
	dir := t.TempDir()
	cfg := testConfig()
	cfg.apiBase = srv.URL
	cfg.OAuth.TokenFile = filepath.Join(dir, "token.json")
	cfg.MQTT.Host, cfg.MQTT.Port = host, port
	cfg.MQTT.ClientID = "hcbridge-e2e"
	log := NewLogger("", "DEBUG")
	auth := NewAuthenticator(cfg, log)
	auth.token = &Token{AccessToken: "x", RefreshToken: "y", ExpiresAt: time.Now().Add(time.Hour)}
	quota := NewQuota()
	api := NewAPIClient(cfg, auth, quota)
	bridge := NewBridge(cfg, api, log, cfg.Appliance.HaID)
	if err := bridge.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	done := make(chan struct{})
	ctl := newController(cfg, api, auth, quota, bridge, log, false)
	go func() { ctl.run(ctx, cancel, sigCh); bridge.Disconnect(); close(done) }()

	// --- observer -------------------------------------------------------------
	seen := &retained{m: map[string]string{}}
	observe := func() mqtt.Client {
		c := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + broker).SetClientID("e2e-obs"))
		if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("observer connect: %v", tok.Error())
		}
		c.SubscribeMultiple(map[string]byte{"homeconnect/#": 1, "homeassistant/+/hcbridge_cooktop/#": 1}, func(_ mqtt.Client, m mqtt.Message) {
			seen.mu.Lock()
			if len(m.Payload()) == 0 {
				delete(seen.m, m.Topic())
			} else {
				seen.m[m.Topic()] = string(m.Payload())
			}
			seen.mu.Unlock()
		}).WaitTimeout(5 * time.Second)
		return c
	}
	obs := observe()
	defer obs.Disconnect(100)

	want := func(topic, value string) {
		t.Helper()
		ok := waitFor(t, 5*time.Second, func() bool { v, _ := seen.get(topic); return v == value })
		if !ok {
			v, present := seen.get(topic)
			t.Errorf("%s = %q (present=%v), want %q", topic, v, present, value)
		}
	}

	want("homeconnect/cooktop/connected", "online")
	want("homeconnect/cooktop/cloud", "online")
	want("homeconnect/cooktop/status/power_state", "On")
	want("homeconnect/cooktop/status/focused_zone", "FrontLeft")
	want("homeconnect/cooktop/zone/front_left/power", "5")
	want("homeconnect/cooktop/status/program_progress", "50")
	want("homeconnect/cooktop/status/appliance_online", "true")
	if v, _ := seen.get("homeconnect/cooktop/status/program_timer_deadline"); v == "" {
		t.Error("no program timer deadline")
	}
	if v, _ := seen.get("homeconnect/cooktop/status/heartbeat"); v == "" {
		t.Error("no heartbeat")
	}

	// Removed entities and topics are gone from the broker.
	for _, topic := range []string{
		"homeassistant/number/hcbridge_cooktop/alarm_clock_minutes/config",
		"homeconnect/cooktop/status/focused_zone_power",
		"homeconnect/cooktop/status/remote_control_active",
	} {
		if !waitFor(t, 5*time.Second, func() bool { _, ok := seen.get(topic); return !ok }) {
			v, _ := seen.get(topic)
			t.Errorf("removed topic %s still retained: %q", topic, v)
		}
	}

	// Discovery: every entity, IDs pinned.
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

	// Power off over SSE: zones reset, timer cleared.
	mu.Lock()
	powerOn = false
	mu.Unlock()
	events <- "event: STATUS\ndata: {\"haId\":\"HAID\",\"items\":[{\"key\":\"BSH.Common.Setting.PowerState\",\"value\":\"BSH.Common.EnumType.PowerState.Off\"}]}\n\n"
	want("homeconnect/cooktop/status/power_state", "Off")
	want("homeconnect/cooktop/zone/front_left/power", "off")
	if !waitFor(t, 5*time.Second, func() bool { _, ok := seen.get("homeconnect/cooktop/status/program_timer_deadline"); return !ok }) {
		t.Error("program timer not cleared at power-off")
	}

	// Shutdown: last state offline, call log persisted.
	sigCh <- syscall.SIGTERM
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("controller did not stop")
	}
	want("homeconnect/cooktop/connected", "offline")
	if _, err := os.Stat(filepath.Join(dir, "calls.json")); err != nil {
		t.Errorf("call log not saved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "alive")); err != nil {
		t.Errorf("liveness file not written: %v", err)
	}
	t.Logf("API calls made: %d", quota.Count24h())
}
