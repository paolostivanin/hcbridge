package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Zones in API enum order. Bosch's API only ever returns one zone at a time
// (whichever the panel last focused), so we maintain a "last observed" map
// to give HA a per-zone view that updates as the user touches each zone.
var allZones = []string{"FrontLeft", "FrontRight", "RearLeft", "RearRight"}

// runningStates are the OperationStates in which /programs/active exists.
// In any other state /active only answers 404, so the poll goes straight to
// /programs/selected (one call instead of a 404 plus a fallback).
var runningStates = map[string]bool{"Run": true, "Pause": true, "ActionRequired": true, "DelayedStart": true}

const publishTimeout = 30 * time.Second

type Bridge struct {
	cfg  *Config
	api  *APIClient
	log  *Logger
	cli  mqtt.Client
	haID string
	out  io.Writer // dry-run sink when cli is nil (--mode=watch, tests)

	mu sync.Mutex
	// lastPub is the desired retained state: topic suffix → payload. It is
	// republished in full on every broker (re)connect, so state published
	// while the broker was away or lost by a broker restart is restored.
	lastPub map[string]string
	// failed marks suffixes whose last publish did not complete, so the
	// next identical publish is not swallowed by the dedup.
	failed       map[string]bool
	zoneState    map[string]string // zone name → power label ("off", "1", "1.5", ...)
	joinedLeft   bool
	joinedRight  bool
	eventAt      map[string]time.Time // key → when an SSE event last set it
	lastDeadline time.Time            // last published program timer deadline
	unmapped     map[string]bool      // unmapped keys already logged

	// applyMu serialises applyOptions against resetZones so their publishes
	// cannot interleave. zoneEpoch is bumped by every resetZones; a poll
	// captures it before its GET and drops the result if it moved, so a poll
	// that was in flight when the hob switched off cannot relight the zone
	// tiles.
	applyMu   sync.Mutex
	zoneEpoch atomic.Uint64

	applianceOnline atomic.Bool
}

func NewBridge(cfg *Config, api *APIClient, log *Logger, haID string) *Bridge {
	b := &Bridge{
		cfg:       cfg,
		api:       api,
		log:       log,
		haID:      haID,
		out:       os.Stdout,
		lastPub:   map[string]string{},
		failed:    map[string]bool{},
		zoneState: map[string]string{},
		eventAt:   map[string]time.Time{},
		unmapped:  map[string]bool{},
	}
	for _, z := range allZones {
		b.zoneState[z] = "off"
	}
	b.applianceOnline.Store(true) // until the API says otherwise
	return b
}

// IsHobActive returns true when the hob is powered on. Standby does not
// count: no zone can be cooking, so polling it would only spend quota.
func (b *Bridge) IsHobActive() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastPub["status/power_state"] == "On"
}

// CanPoll reports whether a per-zone options poll is worth its call: the hob
// is on and connected. While the appliance is disconnected every poll only
// earns a 409, and those count towards Bosch's successive-error limit.
func (b *Bridge) CanPoll() bool {
	return b.applianceOnline.Load() && b.IsHobActive()
}

func (b *Bridge) programRunning() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.lastPub["status/operation_state"]
	return !ok || s == "" || runningStates[s] // unknown: try /active first
}

func (b *Bridge) Connect(ctx context.Context) error {
	scheme := "tcp"
	if b.cfg.MQTT.TLS {
		scheme = "ssl"
	}
	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("%s://%s:%d", scheme, b.cfg.MQTT.Host, b.cfg.MQTT.Port)).
		SetClientID(b.cfg.MQTT.ClientID).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5*time.Second).
		SetCleanSession(false).
		SetKeepAlive(30*time.Second).
		SetPingTimeout(10*time.Second).
		SetWill(b.cfg.StateTopic("connected"), "offline", 1, true).
		SetOnConnectHandler(b.onConnect).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			b.log.Warn("MQTT connection lost: %v", err)
		})
	if b.cfg.MQTT.Username != "" {
		opts.SetUsername(b.cfg.MQTT.Username)
		opts.SetPassword(b.cfg.MQTT.Password)
	}
	if b.cfg.MQTT.TLS {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if b.cfg.MQTT.CAFile != "" {
			pem, err := os.ReadFile(b.cfg.MQTT.CAFile)
			if err != nil {
				return fmt.Errorf("read mqtt.ca_file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return fmt.Errorf("mqtt.ca_file %s: no PEM certificates found", b.cfg.MQTT.CAFile)
			}
			tc.RootCAs = pool
		}
		opts.SetTLSConfig(tc)
	}
	b.cli = mqtt.NewClient(opts)
	tok := b.cli.Connect()
	if !tok.WaitTimeout(15 * time.Second) {
		// Not fatal: with ConnectRetry paho keeps trying in the background
		// and onConnect publishes everything once it gets through. Exiting
		// here lost the boot race against the broker container.
		b.log.Warn("MQTT broker %s:%d not reachable yet; retrying in the background", b.cfg.MQTT.Host, b.cfg.MQTT.Port)
		return nil
	}
	if err := tok.Error(); err != nil {
		return fmt.Errorf("MQTT connect: %w", err)
	}
	return nil
}

func (b *Bridge) onConnect(_ mqtt.Client) {
	b.log.Info("MQTT connected to %s:%d", b.cfg.MQTT.Host, b.cfg.MQTT.Port)
	b.publish("connected", "online")

	if err := PublishDiscovery(b.cfg, b.cli, b.log); err != nil {
		b.log.Error("publish discovery: %v", err)
	}
	b.clearRemoved()

	// Republish the whole desired state: anything published while the
	// broker was unreachable, or lost by a broker restart without
	// persistence, is restored here.
	b.mu.Lock()
	state := make(map[string]string, len(b.lastPub))
	for k, v := range b.lastPub {
		state[k] = v
	}
	b.mu.Unlock()
	for suffix, payload := range state {
		b.send(b.cfg.StateTopic(suffix), payload, true, b.onFail(suffix, payload))
	}
}

// clearRemoved publishes empty retained payloads to the discovery and state
// topics of entities removed in 2.0.0, which deletes them from HA and from
// the broker. Idempotent, so it simply runs on every connect.
func (b *Bridge) clearRemoved() {
	for _, d := range removedDiscovery {
		b.send(discoveryTopic(b.cfg, d.component, d.objectID), "", true, nil)
	}
	for _, s := range removedStateTopics {
		b.send(b.cfg.StateTopic(s), "", true, nil)
	}
}

func (b *Bridge) Disconnect() {
	if b.cli != nil && b.cli.IsConnected() {
		tok := b.cli.Publish(b.cfg.StateTopic("connected"), 1, true, "offline")
		tok.WaitTimeout(2 * time.Second)
		b.cli.Disconnect(500)
	}
}

// publish sets a retained state topic, skipping unchanged values. Reports
// whether it published.
func (b *Bridge) publish(suffix, payload string) bool {
	b.mu.Lock()
	if prev, ok := b.lastPub[suffix]; ok && prev == payload && !b.failed[suffix] {
		b.mu.Unlock()
		return false
	}
	b.lastPub[suffix] = payload
	delete(b.failed, suffix)
	b.mu.Unlock()
	b.send(b.cfg.StateTopic(suffix), payload, true, b.onFail(suffix, payload))
	return true
}

func (b *Bridge) onFail(suffix, payload string) func() {
	return func() {
		b.mu.Lock()
		if b.lastPub[suffix] == payload {
			b.failed[suffix] = true
		}
		b.mu.Unlock()
	}
}

// send publishes without dedup (one-shot events, discovery cleanup, the
// reconnect republish). onFail, if set, runs when the publish fails or
// times out. With no MQTT client it prints to b.out instead (dry run).
func (b *Bridge) send(topic, payload string, retained bool, onFail func()) {
	if b.cli == nil {
		retainedTag := ""
		if retained {
			retainedTag = " [retained]"
		}
		fmt.Fprintf(b.out, "[%s] [PUB]    %-50s = %s%s\n",
			time.Now().Format("15:04:05.000"), topic, payload, retainedTag)
		return
	}
	tok := b.cli.Publish(topic, 1, retained, payload)
	go func() {
		if !tok.WaitTimeout(publishTimeout) {
			b.log.Warn("publish %s: no ack within %s", topic, publishTimeout)
			if onFail != nil {
				onFail()
			}
			return
		}
		if err := tok.Error(); err != nil {
			b.log.Warn("publish %s: %v", topic, err)
			if onFail != nil {
				onFail()
			}
		}
	}()
}

// ApplyEvent translates an SSE event to MQTT publishes.
func (b *Bridge) ApplyEvent(ev SSEEvent) {
	switch ev.Event {
	case "CONNECTED":
		// The appliance came back online (the counterpart of DISCONNECTED;
		// the bridge's own availability is the LWT-backed "connected" topic).
		b.SetApplianceOnline(true)
		return
	case "DISCONNECTED":
		b.SetApplianceOnline(false)
		return
	case "PAIRED", "DEPAIRED", "STREAM_UP", "STREAM_DOWN":
		return // the controller handles the stream events
	}
	now := time.Now()
	b.mu.Lock()
	for _, it := range ev.Items {
		b.eventAt[it.Key] = now
	}
	b.mu.Unlock()
	for _, it := range ev.Items {
		b.applyKV(it.Key, it.Value)
	}
}

func (b *Bridge) SetApplianceOnline(online bool) {
	b.applianceOnline.Store(online)
	b.publish("status/appliance_online", strconv.FormatBool(online))
}

// ApplySnapshot applies a /status + /settings snapshot taken at startedAt.
// Keys an SSE event set after startedAt are skipped: the snapshot is older
// than the event, and applying it would e.g. turn a fresh PowerState=On back
// into Off and stop polling for the whole session.
func (b *Bridge) ApplySnapshot(kv map[string]interface{}, online bool, startedAt time.Time) {
	b.SetApplianceOnline(online)
	for k, v := range kv {
		b.mu.Lock()
		newer := b.eventAt[k].After(startedAt)
		b.mu.Unlock()
		if newer {
			b.log.Debug("snapshot: keeping newer event value for %s", k)
			continue
		}
		b.applyKV(k, v)
	}
}

func (b *Bridge) applyKV(key string, value interface{}) {
	if suffix, ok := eventKeyToSuffix[key]; ok {
		// One-shot events: non-retained payload is the EventPresentState
		// ("Present" / "Confirmed" / "Off"). We publish the cleaned enum.
		// Bypass dedup so back-to-back identical events both fire.
		b.send(b.cfg.StateTopic(suffix), formatValue(key, value), false, nil)
		return
	}

	// Bosch's API only ever exposes one RemainingProgramTime — the active
	// program's, which the physical panel may apply to one of several zones
	// but is reported as a single value at the program level. It is
	// published only as a wall-clock deadline so HA can render a live
	// countdown.
	if key == "BSH.Common.Option.RemainingProgramTime" {
		if secs, err := strconv.Atoi(formatValue(key, value)); err == nil {
			b.updateProgramTimer(secs)
		}
		return
	}

	suffix, ok := keyToSuffix[key]
	if !ok {
		b.mu.Lock()
		first := !b.unmapped[key]
		b.unmapped[key] = true
		b.mu.Unlock()
		if first && !ignoredKeys[key] {
			b.log.Info("unmapped key %s = %v (logged once)", key, value)
		} else {
			b.log.Debug("unmapped key %s = %v", key, value)
		}
		return
	}
	payload := formatValue(key, value)
	b.publish(suffix, payload)

	// Hob power down → all zones are off. Reset our tracking map so the HA
	// per-zone tiles don't keep showing stale "last observed" values forever.
	if key == "BSH.Common.Setting.PowerState" && payload == "Off" {
		b.resetZones()
	}
}

// PollActiveOptions fetches the program options, updates the per-zone state
// map for whichever zone is currently focused, and refreshes the program
// timer. One API call: /programs/active/options while a program runs, else
// /programs/selected/options; two only when /active unexpectedly 404s.
// Reports whether any published value changed.
func (b *Bridge) PollActiveOptions(ctx context.Context) bool {
	if !b.CanPoll() {
		return false
	}
	epoch := b.zoneEpoch.Load()
	base := "/api/homeappliances/" + b.haID + "/programs/"
	fromActive := b.programRunning()
	var body []byte
	var code int
	var err error
	if fromActive {
		body, code, err = b.api.RawGET(ctx, base+"active/options")
		if err != nil {
			b.log.Debug("PollActiveOptions: %v", err)
			return false
		}
	}
	if !fromActive || code == 404 {
		// No running program (e.g. a per-zone timer just expired and
		// OperationState went to Ready) but the hob is still powered on.
		// Bosch keeps the post-expiry zone state in /programs/selected/options
		// — same option shape, with PowerLevel updated to Off — so it
		// surfaces zone-off promptly instead of waiting for the next touch.
		fromActive = false
		body, code, err = b.api.RawGET(ctx, base+"selected/options")
		if err != nil {
			b.log.Debug("PollActiveOptions selected: %v", err)
			return false
		}
	}
	if code != 200 {
		return false
	}
	var resp optionsListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		b.log.Warn("PollActiveOptions parse: %v", err)
		return false
	}
	return b.applyOptions(resp.Data.Items, fromActive, epoch)
}

// applyOptions publishes one /programs/{active,selected}/options response.
// The focused zone is published here as well as the per-zone map: SSE never
// carries ZoneSelector/PowerLevel/JoinZone. Program-level options (duration,
// elapsed, progress, remaining) are taken from /active only; /selected
// describes a program that is not running, so only its zone state and timer
// are used. epoch is zoneEpoch as read before the GET; the result is dropped
// if the zones were reset since, or if the hob is no longer on.
func (b *Bridge) applyOptions(items []apiKV, fromActive bool, epoch uint64) bool {
	b.applyMu.Lock()
	defer b.applyMu.Unlock()
	if b.zoneEpoch.Load() != epoch || !b.IsHobActive() {
		b.log.Debug("options result dropped: hob switched off while the poll was in flight")
		return false
	}
	changed := false
	var zone, power string
	var joined bool
	var sawJoin bool
	for _, kv := range items {
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
				changed = b.updateProgramTimer(int(v)) || changed
			}
		case "BSH.Common.Option.Duration",
			"BSH.Common.Option.ElapsedProgramTime",
			"BSH.Common.Option.ProgramProgress":
			if fromActive {
				changed = b.publish(keyToSuffix[kv.Key], formatValue(kv.Key, kv.Value)) || changed
			}
		}
	}
	if zone == "" {
		return changed
	}
	if partnerZone(zone) == "" {
		// Not one of the four known zones (e.g. a null ZoneSelector would
		// otherwise create a zone/<nil>/power topic).
		b.log.Debug("options: ignoring unknown zone %q", zone)
		return changed
	}
	if power == "" {
		power = "off"
	}
	changed = b.publish("status/focused_zone", zone) || changed
	changed = b.updateZone(zone, power, joined, sawJoin) || changed
	return changed
}

// updateZone applies one observation of a focused zone to the state map and
// publishes the per-zone power topic (plus join-side topic when seen). While a
// pair is flex-joined the focused zone's power is mirrored onto its partner —
// Home Connect only ever reports the focused half, so without this the other
// half's per-zone tile stays stale at "off". On the join→unjoin transition the
// partner is dropped back to off (it self-corrects on its next focus).
func (b *Bridge) updateZone(zone, power string, joined, sawJoin bool) bool {
	b.mu.Lock()
	b.zoneState[zone] = power
	var wasJoined bool
	if sawJoin {
		switch zone {
		case "FrontLeft", "RearLeft":
			wasJoined, b.joinedLeft = b.joinedLeft, joined
		case "FrontRight", "RearRight":
			wasJoined, b.joinedRight = b.joinedRight, joined
		}
	}
	// Mirror power across the flex-joined pair (clear it on un-join). The reset
	// keys off the *previous* join state, not merely joined==false, so an
	// independent (never-joined) zone is never zeroed during normal use.
	partner, partnerPower := "", ""
	if sawJoin {
		if p := partnerZone(zone); p != "" {
			if joined {
				partner, partnerPower = p, power
			} else if wasJoined {
				partner, partnerPower = p, "off"
			}
		}
	}
	if partner != "" {
		b.zoneState[partner] = partnerPower
	}
	jl, jr := b.joinedLeft, b.joinedRight
	b.mu.Unlock()

	changed := b.publish("zone/"+zoneSlug(zone)+"/power", power)
	if partner != "" {
		changed = b.publish("zone/"+zoneSlug(partner)+"/power", partnerPower) || changed
	}
	if sawJoin {
		switch zone {
		case "FrontLeft", "RearLeft":
			changed = b.publish("joined_left", strconv.FormatBool(jl)) || changed
		case "FrontRight", "RearRight":
			changed = b.publish("joined_right", strconv.FormatBool(jr)) || changed
		}
	}
	return changed
}

// timerJitter is how far the computed deadline may drift before it is
// republished. RemainingProgramTime is pushed several times a minute, and
// now+remaining wobbles by about a second each time; republishing every
// wobble only churned the recorder.
const timerJitter = 2 * time.Second

// updateProgramTimer converts the API's RemainingProgramTime (seconds) into
// an RFC3339 wall-clock deadline and publishes it. Empty string when no timer
// is active (remaining <= 0) so HA renders the entity as "unknown" rather
// than a stale past timestamp.
func (b *Bridge) updateProgramTimer(remainingSecs int) bool {
	var deadline time.Time
	if remainingSecs > 0 {
		deadline = time.Now().Add(time.Duration(remainingSecs) * time.Second)
	}
	b.mu.Lock()
	prev := b.lastDeadline
	if !deadline.IsZero() && !prev.IsZero() {
		d := deadline.Sub(prev)
		if d < timerJitter && d > -timerJitter {
			b.mu.Unlock()
			return false
		}
	}
	b.lastDeadline = deadline
	b.mu.Unlock()
	return b.publish("status/program_timer_deadline", formatDeadline(deadline))
}

// resetZones marks all zones off, clears the program timer, and republishes.
// Triggered on PowerState=Off — when the hob loses power, no zone is active
// and no timer is running. Callers must not hold applyMu (applyOptions never
// reaches here: it does not forward PowerState).
func (b *Bridge) resetZones() {
	b.applyMu.Lock()
	defer b.applyMu.Unlock()
	b.zoneEpoch.Add(1)
	b.mu.Lock()
	for _, z := range allZones {
		b.zoneState[z] = "off"
	}
	b.joinedLeft = false
	b.joinedRight = false
	b.lastDeadline = time.Time{}
	b.mu.Unlock()
	for _, z := range allZones {
		b.publish("zone/"+zoneSlug(z)+"/power", "off")
	}
	b.publish("joined_left", "false")
	b.publish("joined_right", "false")
	b.publish("status/program_timer_deadline", "")
}

// PublishRateLimit publishes the current Bosch API rate-limit state to MQTT.
// Driven by a ticker in the controller; publish() dedups so only transitions
// generate broker traffic. Empty until-timestamp when not blocked, so the
// HA timestamp sensor renders "unknown" rather than a stale past value.
func (b *Bridge) PublishRateLimit(blocked bool, until time.Time) {
	b.publish("status/rate_limited", strconv.FormatBool(blocked))
	if blocked {
		b.publish("status/rate_limited_until", formatDeadline(until))
	} else {
		b.publish("status/rate_limited_until", "")
	}
}

// PublishAPICalls publishes the bridge's own rolling 24 h API call count.
func (b *Bridge) PublishAPICalls(n int) {
	b.publish("status/api_calls_24h", strconv.Itoa(n))
}

// SetCloud publishes whether the bridge currently has a live event stream.
// Data entities use it as a second availability topic, so HA shows them as
// unavailable instead of silently frozen while the bridge is blind.
func (b *Bridge) SetCloud(online bool) {
	v := "offline"
	if online {
		v = "online"
	}
	b.publish("cloud", v)
}

// PublishHeartbeat publishes the main loop's liveness timestamp. The LWT only
// covers a dropped TCP connection; a stuck main loop keeps the MQTT client's
// pings going, so only a stale heartbeat reveals it.
func (b *Bridge) PublishHeartbeat(now time.Time) {
	b.publish("status/heartbeat", formatDeadline(now))
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

// partnerZone returns the other half of a zone's flex-induction pair, or ""
// if the zone has no partner (or is not a known zone).
func partnerZone(zone string) string {
	switch zone {
	case "FrontLeft":
		return "RearLeft"
	case "RearLeft":
		return "FrontLeft"
	case "FrontRight":
		return "RearRight"
	case "RearRight":
		return "FrontRight"
	}
	return ""
}
