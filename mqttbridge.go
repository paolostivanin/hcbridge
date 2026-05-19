package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Zones in API enum order. Bosch's API only ever returns one zone at a time
// (whichever the panel last focused), so we maintain a "last observed" map
// to give HA a per-zone view that updates as the user touches each zone.
var allZones = []string{"FrontLeft", "FrontRight", "RearLeft", "RearRight"}

type Bridge struct {
	cfg  *Config
	api  *APIClient
	log  *Logger
	cli  mqtt.Client
	haID string

	mu          sync.Mutex
	lastPub     map[string]string // topic suffix → last payload (for change detection / restart)
	zoneState   map[string]string // zone name → power label ("off", "1", "1.5", ...)
	joinedLeft  bool
	joinedRight bool
}

func NewBridge(cfg *Config, api *APIClient, log *Logger, haID string) *Bridge {
	b := &Bridge{
		cfg:       cfg,
		api:       api,
		log:       log,
		haID:      haID,
		lastPub:   map[string]string{},
		zoneState: map[string]string{},
	}
	for _, z := range allZones {
		b.zoneState[z] = "off"
	}
	return b
}

// IsHobActive returns true when the bridge believes the hob is powered on
// (PowerState != Off). Used by the smart poller to decide whether to poll
// /programs/active/options.
func (b *Bridge) IsHobActive() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if v, ok := b.lastPub["status/power_state"]; ok {
		return v == "On" || v == "Standby"
	}
	return false
}

func (b *Bridge) Connect(ctx context.Context) error {
	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:%d", b.cfg.MQTT.Host, b.cfg.MQTT.Port)).
		SetClientID(b.cfg.MQTT.ClientID).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetCleanSession(false).
		SetKeepAlive(30 * time.Second).
		SetPingTimeout(10 * time.Second).
		SetWill(b.cfg.StateTopic("connected"), "offline", 1, true).
		SetOnConnectHandler(b.onConnect).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			b.log.Warn("MQTT connection lost: %v", err)
		})
	if b.cfg.MQTT.Username != "" {
		opts.SetUsername(b.cfg.MQTT.Username)
		opts.SetPassword(b.cfg.MQTT.Password)
	}
	b.cli = mqtt.NewClient(opts)
	tok := b.cli.Connect()
	if !tok.WaitTimeout(15 * time.Second) {
		return errors.New("MQTT connect timeout")
	}
	if err := tok.Error(); err != nil {
		return fmt.Errorf("MQTT connect: %w", err)
	}
	return nil
}

func (b *Bridge) onConnect(_ mqtt.Client) {
	b.log.Info("MQTT connected to %s:%d", b.cfg.MQTT.Host, b.cfg.MQTT.Port)
	b.publish("connected", "online", true)

	if err := PublishDiscovery(context.Background(), b.cfg, b.cli, b.log); err != nil {
		b.log.Error("publish discovery: %v", err)
	}

	// Seed rate-limit state so HA doesn't show "unknown" during the first
	// tick window after reconnect. The 30s ticker in main.go takes over.
	b.PublishRateLimit(IsRateLimited())

	subs := map[string]byte{
		b.cfg.CmdTopic("alarm_clock"):         1,
		b.cfg.CmdTopic("alarm_clock_minutes"): 1,
	}
	tok := b.cli.SubscribeMultiple(subs, b.handleCommand)
	tok.Wait()
	if err := tok.Error(); err != nil {
		b.log.Error("MQTT subscribe: %v", err)
	}
}

func (b *Bridge) Disconnect() {
	if b.cli != nil && b.cli.IsConnected() {
		b.publish("connected", "offline", true)
		b.cli.Disconnect(500)
	}
}

func (b *Bridge) publish(suffix, payload string, retained bool) {
	b.mu.Lock()
	if prev, ok := b.lastPub[suffix]; ok && prev == payload {
		b.mu.Unlock()
		return
	}
	b.lastPub[suffix] = payload
	b.mu.Unlock()
	b.publishRaw(b.cfg.StateTopic(suffix), payload, retained)
}

// publishRaw bypasses the change-detection dedup. Used for one-shot events
// where back-to-back identical payloads (e.g. two alarms that both fire
// "Present") must both be delivered. Also handles dry-run (nil cli) by
// printing to stdout for --mode=watch.
func (b *Bridge) publishRaw(topic, payload string, retained bool) {
	if b.cli == nil {
		retainedTag := ""
		if retained {
			retainedTag = " [retained]"
		}
		fmt.Printf("[%s] [PUB]    %-50s = %s%s\n",
			time.Now().Format("15:04:05.000"), topic, payload, retainedTag)
		return
	}
	tok := b.cli.Publish(topic, 1, retained, payload)
	go func() {
		tok.Wait()
		if err := tok.Error(); err != nil {
			b.log.Warn("publish %s: %v", topic, err)
		}
	}()
}

// ApplyEvent translates an SSE event to MQTT publishes and (for key
// derived state) computes alarm_clock_minutes from alarm_clock_seconds.
func (b *Bridge) ApplyEvent(ev SSEEvent) {
	switch ev.Event {
	case "CONNECTED":
		b.publish("connected", "online", true)
		return
	case "DISCONNECTED":
		// Appliance went offline (not the bridge). Mirror to a sub-topic
		// so HA still shows the bridge as online but flags the appliance.
		b.publish("status/appliance_online", "false", true)
		return
	case "PAIRED", "DEPAIRED", "RECONNECTED":
		// no-op here; main triggers a full resync on RECONNECTED.
		return
	}
	for _, it := range ev.Items {
		b.applyKV(it.Key, it.Value)
	}
}

func (b *Bridge) ApplySnapshot(kv map[string]interface{}) {
	b.publish("status/appliance_online", "true", true)
	for k, v := range kv {
		b.applyKV(k, v)
	}
}

func (b *Bridge) applyKV(key string, value interface{}) {
	if suffix, ok := eventKeyToSuffix[key]; ok {
		// One-shot events: non-retained payload is the EventPresentState
		// ("Present" / "Confirmed" / "Off"). We publish the cleaned enum.
		// Bypass dedup so back-to-back identical events both fire.
		b.publishRaw(b.cfg.StateTopic(suffix), formatValue(key, value), false)
		return
	}

	suffix, ok := keyToSuffix[key]
	if !ok {
		b.log.Debug("unmapped key %s = %v", key, value)
		return
	}
	payload := formatValue(key, value)
	b.publish(suffix, payload, true)

	// Derived: alarm_clock seconds → minutes (for the HA number entity).
	if key == "BSH.Common.Setting.AlarmClock" {
		if secs, err := strconv.Atoi(payload); err == nil {
			minutes := secs / 60
			b.publish("status/alarm_clock_minutes", strconv.Itoa(minutes), true)
		}
	}

	// Bosch's API only ever exposes one RemainingProgramTime — the active
	// program's, which the physical panel may apply to one of several zones
	// but is reported as a single value at the program level. Republish it
	// as a wall-clock deadline so HA can render a live countdown.
	if key == "BSH.Common.Option.RemainingProgramTime" {
		if secs, err := strconv.Atoi(payload); err == nil {
			b.updateProgramTimer(secs)
		}
	}

	// Hob power down → all zones are off. Reset our tracking map so the HA
	// per-zone tiles don't keep showing stale "last observed" values forever.
	if key == "BSH.Common.Setting.PowerState" && payload == "Off" {
		b.resetZones()
	}
}

// PollActiveOptions fetches /programs/active/options, updates the per-zone
// state map for whichever zone is currently focused, and refreshes the
// program-level timer deadline. Safe to call frequently; publish() dedups.
func (b *Bridge) PollActiveOptions(ctx context.Context) {
	body, code, err := b.api.RawGET(ctx, "/api/homeappliances/"+b.haID+"/programs/active/options")
	if err != nil {
		b.log.Debug("PollActiveOptions: %v", err)
		return
	}
	if code == 404 && b.IsHobActive() {
		// /active 404 = no running program (e.g. a per-zone timer just
		// expired and OperationState went to Ready) but the hob is still
		// powered on. Bosch keeps the post-expiry zone state in
		// /programs/selected/options — same option shape, with PowerLevel
		// updated to Off — so falling back surfaces zone-off promptly
		// instead of waiting for the next user touch. Gated on IsHobActive
		// so we don't pull stale data after PowerState=Off (resetZones
		// already covers that path).
		body, code, err = b.api.RawGET(ctx, "/api/homeappliances/"+b.haID+"/programs/selected/options")
		if err != nil {
			b.log.Debug("PollActiveOptions selected fallback: %v", err)
			return
		}
	}
	if code != 200 {
		return
	}
	var resp optionsListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		b.log.Warn("PollActiveOptions parse: %v", err)
		return
	}
	var zone, power string
	var joined bool
	var sawJoin bool
	for _, kv := range resp.Data.Items {
		switch kv.Key {
		case "Cooking.Hob.Option.ZoneSelector":
			zone = cleanEnum(kv.Value)
		case "Cooking.Hob.Option.PowerLevel":
			power = formatValue(kv.Key, kv.Value)
		case "Cooking.Hob.Option.JoinZone":
			if v, ok := kv.Value.(bool); ok {
				joined, sawJoin = v, true
			}
		case "BSH.Common.Option.RemainingProgramTime":
			if v, ok := kv.Value.(float64); ok {
				b.updateProgramTimer(int(v))
			}
		}
	}
	if zone == "" {
		return
	}
	b.updateZone(zone, power, joined, sawJoin)
}

// updateZone applies one observation of a focused zone to the state map and
// publishes the per-zone power topic (plus join-side topic when seen).
func (b *Bridge) updateZone(zone, power string, joined, sawJoin bool) {
	if power == "" {
		power = "off"
	}
	b.mu.Lock()
	b.zoneState[zone] = power
	if sawJoin {
		switch zone {
		case "FrontLeft", "RearLeft":
			b.joinedLeft = joined
		case "FrontRight", "RearRight":
			b.joinedRight = joined
		}
	}
	jl, jr := b.joinedLeft, b.joinedRight
	b.mu.Unlock()

	b.publish("zone/"+zoneSlug(zone)+"/power", power, true)
	if sawJoin {
		switch zone {
		case "FrontLeft", "RearLeft":
			b.publish("joined_left", strconv.FormatBool(jl), true)
		case "FrontRight", "RearRight":
			b.publish("joined_right", strconv.FormatBool(jr), true)
		}
	}
}

// updateProgramTimer converts the API's RemainingProgramTime (seconds) into
// an RFC3339 wall-clock deadline and publishes it. Empty string when no timer
// is active (remaining <= 0) so HA renders the entity as "unknown" rather
// than a stale past timestamp.
func (b *Bridge) updateProgramTimer(remainingSecs int) {
	var deadline time.Time
	if remainingSecs > 0 {
		deadline = time.Now().Add(time.Duration(remainingSecs) * time.Second)
	}
	b.publish("status/program_timer_deadline", formatDeadline(deadline), true)
}

// resetZones marks all zones off, clears the program timer, and republishes.
// Triggered on PowerState=Off — when the hob loses power, no zone is active
// and no timer is running.
func (b *Bridge) resetZones() {
	b.mu.Lock()
	for _, z := range allZones {
		b.zoneState[z] = "off"
	}
	b.joinedLeft = false
	b.joinedRight = false
	b.mu.Unlock()
	for _, z := range allZones {
		b.publish("zone/"+zoneSlug(z)+"/power", "off", true)
	}
	b.publish("joined_left", "false", true)
	b.publish("joined_right", "false", true)
	b.publish("status/program_timer_deadline", "", true)
}

// PublishRateLimit publishes the current Bosch API rate-limit state to MQTT.
// Driven by a 30s ticker in main.go; publish() dedups so only transitions
// generate broker traffic. Empty until-timestamp when not blocked, so the
// HA timestamp sensor renders "unknown" rather than a stale past value.
func (b *Bridge) PublishRateLimit(blocked bool, until time.Time) {
	b.publish("status/rate_limited", strconv.FormatBool(blocked), true)
	if blocked {
		b.publish("status/rate_limited_until", formatDeadline(until), true)
	} else {
		b.publish("status/rate_limited_until", "", true)
	}
}

// formatDeadline returns RFC3339 if t is non-zero, "" otherwise.
// Empty string is interpreted as "no timer" by the HA value_template.
func formatDeadline(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// zoneSlug converts API zone name to topic slug: "FrontLeft" → "front_left".
func zoneSlug(zone string) string {
	var out strings.Builder
	for i, r := range zone {
		if i > 0 && r >= 'A' && r <= 'Z' {
			out.WriteByte('_')
		}
		if r >= 'A' && r <= 'Z' {
			r += 'a' - 'A'
		}
		out.WriteRune(r)
	}
	return out.String()
}

func (b *Bridge) handleCommand(_ mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	payload := strings.TrimSpace(string(msg.Payload()))
	b.log.Info("cmd received: %s = %q", topic, payload)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	prefix := b.cfg.CmdTopic("")
	if !strings.HasPrefix(topic, prefix) {
		return
	}
	cmd := strings.TrimPrefix(topic, prefix)

	var err error
	switch cmd {
	case "alarm_clock":
		secs, e := strconv.Atoi(payload)
		if e != nil {
			err = fmt.Errorf("alarm_clock: bad seconds %q", payload)
			break
		}
		err = b.api.PutSetting(ctx, b.haID, "BSH.Common.Setting.AlarmClock", secs)

	case "alarm_clock_minutes":
		mins, e := strconv.Atoi(payload)
		if e != nil {
			err = fmt.Errorf("alarm_clock_minutes: bad minutes %q", payload)
			break
		}
		err = b.api.PutSetting(ctx, b.haID, "BSH.Common.Setting.AlarmClock", mins*60)

	default:
		b.log.Warn("unknown command topic %s", topic)
		return
	}

	if err != nil {
		b.log.Error("cmd %s failed: %v", cmd, err)
	}
}
