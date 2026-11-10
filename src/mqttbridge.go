package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	hc "hcbridge/src/internal/homeconnect"
)

const publishTimeout = 30 * time.Second

// powerOffGrace is how long after PowerState=Off the hob gets to report the
// end state of every zone itself (it does so within about 0.3 s: levels to
// off, running zones to residual heat or off). Only what is still stale
// after that is reset, so the safety net never contradicts the hob: reset at
// once, a zone showed "off" for 0.24 s before the hob's "residual heat"
// arrived (seen 2026-11-10).
const powerOffGrace = 2 * time.Second

// carbonReplaceAt is the saturation (percent) from which the carbon filter
// is flagged for replacement. The hob's own "nearly saturated" event was
// seen at 96%; the event also latches the flag.
const carbonReplaceAt = 90

// zoneObs is the last reported state of one cooking zone.
type zoneObs struct {
	state string // State member: Off, Active, ResiduelHeat, NotSelectable
	op    string // OperationState member: Ready, Run, Pause, Finished, ...
	power string // power label: off, 1, 1.5, ..., Boost1
}

// Bridge turns resolved appliance values into MQTT publishes and owns the
// MQTT connection. Values arrive in wire order from one goroutine (the
// client's event dispatcher); the MQTT callbacks run on paho's goroutines.
type Bridge struct {
	cfg *Config
	log *Logger
	cli mqtt.Client
	out io.Writer // dry-run sink when cli is nil (local-watch, tests)

	mu sync.Mutex
	// lastPub is the desired retained state: topic suffix -> payload. It is
	// republished in full on every broker (re)connect, so state published
	// while the broker was away or lost by a broker restart is restored.
	lastPub map[string]string
	// failed marks suffixes whose last publish did not complete, so the
	// next identical publish is not swallowed by the dedup.
	failed        map[string]bool
	lastAt        map[string]time.Time // when each suffix was last published (throttling)
	deadlines     map[string]time.Time // last published timer deadline per suffix
	unmapped      map[string]bool      // unmapped keys already logged
	zones         map[string]*zoneObs  // by zone name (FrontLeft, ...)
	selector      string               // the zone the panel has focus on
	joinFlag      map[string]bool      // "left"/"right": last JoinZone reported for that side
	carbonSat     float64
	carbonKnown   bool
	carbonLatched bool
	meta          deviceMeta

	powerOffDelay time.Duration // powerOffGrace; zero resets synchronously (tests)

	discoveryMu sync.Mutex
}

func NewBridge(cfg *Config, meta deviceMeta, log *Logger) *Bridge {
	b := &Bridge{
		cfg:       cfg,
		log:       log,
		out:       os.Stdout,
		lastPub:   map[string]string{},
		failed:    map[string]bool{},
		lastAt:    map[string]time.Time{},
		deadlines: map[string]time.Time{},
		unmapped:  map[string]bool{},
		zones:     map[string]*zoneObs{},
		joinFlag:  map[string]bool{},
		meta:      meta,

		powerOffDelay: powerOffGrace,
	}
	for _, z := range allZones {
		b.zones[z] = &zoneObs{}
	}
	return b
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

	b.publishDiscovery()
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

func (b *Bridge) publishDiscovery() {
	b.discoveryMu.Lock()
	defer b.discoveryMu.Unlock()
	if err := PublishDiscovery(b.cfg, b.Meta(), b.cli, b.log); err != nil {
		b.log.Error("publish discovery: %v", err)
	}
}

// clearRemoved publishes empty retained payloads to the discovery and state
// topics of entities earlier versions published, which deletes them from HA
// and from the broker. Idempotent, so it simply runs on every connect.
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
	b.lastAt[suffix] = time.Now()
	delete(b.failed, suffix)
	b.mu.Unlock()
	b.send(b.cfg.StateTopic(suffix), payload, true, b.onFail(suffix, payload))
	return true
}

// publishThrottled is publish with the rate limit of throttledSuffixes.
func (b *Bridge) publishThrottled(suffix, payload string) bool {
	if gap, ok := throttledSuffixes[suffix]; ok {
		b.mu.Lock()
		prev, had := b.lastPub[suffix]
		last := b.lastAt[suffix]
		b.mu.Unlock()
		zeroEdge := prev == "0" || payload == "0" || prev == "" || payload == ""
		if had && prev != payload && !zeroEdge && time.Since(last) < gap {
			return false
		}
	}
	return b.publish(suffix, payload)
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

// ApplyValues publishes a batch of values pushed by the hob. snapshot marks
// the full /ro/allMandatoryValues read after each (re)connect: it carries
// every event's resting state, which must not fire event topics.
func (b *Bridge) ApplyValues(vals []hc.Value, snapshot bool) {
	for _, v := range vals {
		if !v.Known() {
			b.logUnmapped(fmt.Sprintf("uid %d", v.UID), string(v.Raw), false)
			continue
		}
		b.applyKV(v.Name, v.Display(), snapshot)
	}
}

// ApplyDescriptions applies the values that arrive inside access changes.
// Some values only ever come this way: the carbon filter saturation, for
// one, is not readable while the hob is off and shows up as the value of
// the access change at power-on.
func (b *Bridge) ApplyDescriptions(ds []hc.Description) {
	for _, d := range ds {
		if d.Value == nil || d.Name == "" {
			continue
		}
		if _, isEvent := eventKeyToSuffix[d.Name]; isEvent {
			continue
		}
		b.applyKV(d.Name, d.Value.Display(), false)
	}
}

func (b *Bridge) logUnmapped(key string, value interface{}, ignored bool) {
	b.mu.Lock()
	first := !b.unmapped[key]
	b.unmapped[key] = true
	b.mu.Unlock()
	if first && !ignored {
		b.log.Info("unmapped key %s = %v (logged once)", key, value)
	} else {
		b.log.Debug("unmapped key %s = %v", key, value)
	}
}

func (b *Bridge) applyKV(key string, value interface{}, snapshot bool) {
	if suffix, ok := eventKeyToSuffix[key]; ok {
		if snapshot {
			return
		}
		state := cleanEnum(value)
		b.send(b.cfg.StateTopic(suffix), state, false, nil)
		if strings.HasPrefix(key, "Cooking.Hob.Event.CarbonFilter") && state == "Present" {
			b.mu.Lock()
			b.carbonLatched = true
			b.mu.Unlock()
			b.updateCarbon()
		}
		return
	}
	if m := zoneKeyRe.FindStringSubmatch(key); m != nil {
		b.applyZone(m[1], m[2], value)
		return
	}

	switch key {
	case "BSH.Common.Option.RemainingProgramTime":
		// Published only as a wall-clock deadline so HA renders a live
		// countdown; the hob pushes the remaining seconds every second.
		if secs, ok := value.(float64); ok {
			b.updateDeadline("status/program_timer_deadline", int(secs))
		}
		return
	case "Cooking.Hob.Option.ZoneSelector":
		zone := cleanEnum(value)
		b.mu.Lock()
		b.selector = zone
		b.mu.Unlock()
	case "Cooking.Hob.Option.JoinZone":
		// The flag describes the focused zone's side; ZoneSelector arrives
		// before it whenever the focus moves.
		joined, ok := value.(bool)
		if !ok {
			return
		}
		b.mu.Lock()
		side := sideOf(b.selector)
		if side != "" {
			b.joinFlag[side] = joined
		}
		b.mu.Unlock()
		b.publishJoins()
		return
	case "Cooking.Hob.Status.CarbonFilterSaturation":
		if pct, ok := value.(float64); ok {
			b.mu.Lock()
			b.carbonSat, b.carbonKnown = pct, true
			b.mu.Unlock()
			b.updateCarbon()
		}
	}

	suffix, ok := keyToSuffix[key]
	if !ok {
		b.logUnmapped(key, value, isIgnored(key))
		return
	}
	payload := formatValue(key, value)
	b.publishThrottled(suffix, payload)

	if key == "BSH.Common.Setting.PowerState" && payload == "Off" {
		b.schedulePowerOffReset()
	}
}

// schedulePowerOffReset runs resetOnPowerOff after the grace, if the hob is
// still off by then (it may have been switched on again).
func (b *Bridge) schedulePowerOffReset() {
	if b.powerOffDelay <= 0 {
		b.resetOnPowerOff()
		return
	}
	time.AfterFunc(b.powerOffDelay, func() {
		b.mu.Lock()
		off := b.lastPub["status/power_state"] == "Off"
		b.mu.Unlock()
		if off {
			b.resetOnPowerOff()
		}
	})
}

// applyZone handles Cooking.Hob.Status.Zone.<id>.<attr>.
func (b *Bridge) applyZone(id, attr string, value interface{}) {
	zone, ok := zoneHalves[id]
	if !ok {
		b.logUnmapped("Cooking.Hob.Status.Zone."+id+"."+attr, value, true)
		return
	}
	slug := zoneSlug(zone)
	switch attr {
	case "PowerLevel":
		power := formatValue(".PowerLevel", value)
		if power == "" {
			power = "off"
		}
		b.mu.Lock()
		b.zones[zone].power = power
		b.mu.Unlock()
		b.publish("zone/"+slug+"/power", power)
	case "State", "OperationState":
		s := cleanEnum(value)
		b.mu.Lock()
		z := b.zones[zone]
		if attr == "State" {
			z.state = s
		} else {
			z.op = s
		}
		label := zoneStateLabel(z.state, z.op)
		b.mu.Unlock()
		b.publish("zone/"+slug+"/state", label)
		b.publishJoins()
	case "RemainingProgramTime":
		if secs, ok := value.(float64); ok {
			b.updateDeadline("zone/"+slug+"/timer_deadline", int(secs))
		}
	default:
		// Duration, elapsed time, progress, programs, sensor levels, shape:
		// known, not published.
	}
}

func sideOf(zone string) string {
	switch zone {
	case "FrontLeft", "RearLeft":
		return "left"
	case "FrontRight", "RearRight":
		return "right"
	}
	return ""
}

// publishJoins publishes the flex-join state of both sides. A side counts as
// joined while the hob's JoinZone flag for it is set and both of its zones
// are active, so a stale flag cannot outlive the joined cooking.
func (b *Bridge) publishJoins() {
	b.mu.Lock()
	left := b.joinFlag["left"] && b.zones["FrontLeft"].state == "Active" && b.zones["RearLeft"].state == "Active"
	right := b.joinFlag["right"] && b.zones["FrontRight"].state == "Active" && b.zones["RearRight"].state == "Active"
	b.mu.Unlock()
	b.publish("joined_left", strconv.FormatBool(left))
	b.publish("joined_right", strconv.FormatBool(right))
}

// updateCarbon publishes the filter-replacement flag: on from carbonReplaceAt
// percent or after the hob's own saturation event, off again once the
// saturation reads low (a fresh filter was fitted and the warning reset).
func (b *Bridge) updateCarbon() {
	b.mu.Lock()
	if b.carbonKnown && b.carbonSat < 50 {
		b.carbonLatched = false
	}
	known := b.carbonKnown || b.carbonLatched
	replace := b.carbonLatched || (b.carbonKnown && b.carbonSat >= carbonReplaceAt)
	b.mu.Unlock()
	if known {
		b.publish("status/carbon_filter_replace", strconv.FormatBool(replace))
	}
}

// timerJitter is how far a computed deadline may drift before it is
// republished. Remaining times are pushed every second, and
// now+remaining wobbles by about a second each time; republishing every
// wobble only churns the recorder.
const timerJitter = 2 * time.Second

// updateDeadline converts a remaining time (seconds) into an RFC3339
// wall-clock deadline and publishes it to suffix. Empty when no timer runs
// (remaining <= 0), so HA renders "unknown" instead of a stale timestamp.
func (b *Bridge) updateDeadline(suffix string, remainingSecs int) bool {
	var deadline time.Time
	if remainingSecs > 0 {
		deadline = time.Now().Add(time.Duration(remainingSecs) * time.Second)
	}
	b.mu.Lock()
	prev := b.deadlines[suffix]
	if !deadline.IsZero() && !prev.IsZero() {
		d := deadline.Sub(prev)
		if d < timerJitter && d > -timerJitter {
			b.mu.Unlock()
			return false
		}
	}
	b.deadlines[suffix] = deadline
	b.mu.Unlock()
	return b.publish(suffix, formatDeadline(deadline))
}

// resetOnPowerOff: once the hob is off no zone heats, no timer runs and no
// pair is joined. The hob pushes all of this itself right after power-off;
// resetting here (after powerOffGrace) covers a missed push. Residual heat is
// kept: the hob reports it after power-off.
func (b *Bridge) resetOnPowerOff() {
	b.mu.Lock()
	for _, z := range b.zones {
		z.power = "off"
		if z.state == "Active" {
			z.state = "Off"
		}
		z.op = ""
	}
	b.joinFlag = map[string]bool{}
	labels := map[string]string{}
	for name, z := range b.zones {
		labels[name] = zoneStateLabel(z.state, z.op)
	}
	b.mu.Unlock()
	for _, z := range allZones {
		slug := zoneSlug(z)
		b.publish("zone/"+slug+"/power", "off")
		b.publish("zone/"+slug+"/state", labels[z])
		b.updateDeadline("zone/"+slug+"/timer_deadline", 0)
	}
	b.updateDeadline("status/program_timer_deadline", 0)
	b.publishJoins()
}

// PublishLink publishes the local link's health. These topics are not gated
// by the link itself: they must stay visible exactly when it is down.
func (b *Bridge) PublishLink(state hc.LinkState, since time.Time, powerCycleNeeded bool) {
	b.publish("status/link_state", string(state))
	b.publish("status/link_since", formatDeadline(since))
	b.publish("status/power_cycle_needed", strconv.FormatBool(powerCycleNeeded))
}

// SetLink publishes whether the bridge has a live session with the hob.
// Data entities use it as a second availability topic, so HA shows them as
// unavailable instead of silently frozen while the bridge is blind.
func (b *Bridge) SetLink(online bool) {
	v := "offline"
	if online {
		v = "online"
	}
	b.publish("link", v)
}

// Meta returns the current device info.
func (b *Bridge) Meta() deviceMeta {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.meta
}

// SetDeviceInfo records what the handshake reported. When the firmware
// versions change the discovery configs are republished so HA's device page
// shows them; reports whether anything changed.
func (b *Bridge) SetDeviceInfo(m deviceMeta) bool {
	b.mu.Lock()
	changed := !b.meta.sameVersions(m) || b.meta.VIB != m.VIB || b.meta.Serial != m.Serial || b.meta.Brand != m.Brand
	b.meta = m
	b.mu.Unlock()
	if changed && b.cli != nil && b.cli.IsConnected() {
		go b.publishDiscovery()
	}
	return changed
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

// zoneSlug converts a zone name to its topic slug: "FrontLeft" -> "front_left".
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
