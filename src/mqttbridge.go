package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"maps"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	hc "hcbridge/src/internal/homeconnect"
)

const (
	// publishTimeout is how long a QoS 1 publish may wait for the broker's
	// ack before it counts as failed (and the value is sent again).
	publishTimeout = 30 * time.Second
	// mqttWriteTimeout bounds every socket write and paho's hand-off of a
	// publish. Without it a broker that stops reading (but keeps the TCP
	// connection) wedges paho's writer, its keepalive pings included.
	mqttWriteTimeout = 5 * time.Second
	// publishRetryDelay spaces out the resends of a failed publish.
	publishRetryDelay = time.Second
	// maxQueuedOneShots caps the events held while the broker is away.
	maxQueuedOneShots = 100
)

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

// outMsg is one queued publish. A state message names a retained state topic
// by suffix and goes out with whatever lastPub holds for it at send time, so
// a queued topic always carries its newest value. A one-shot message (events,
// deleting removed topics) carries its own topic and payload.
type outMsg struct {
	suffix   string // state topic suffix; empty for a one-shot
	topic    string
	payload  string
	retained bool
}

// Bridge turns resolved appliance values into MQTT publishes and owns the
// MQTT connection. Appliance values arrive in wire order from the client's
// event dispatcher; link and availability changes come from the controller's
// goroutines and timers; the MQTT callbacks run on paho's goroutines.
//
// All of them only change the desired state (lastPub) and queue the topic.
// One goroutine, the sender, hands the queue to paho, always with the
// newest value. So a value can never overtake a newer one on its way to the
// broker, and nobody but the sender ever waits on MQTT.
type Bridge struct {
	cfg *Config
	log *Logger
	cli mqtt.Client
	out io.Writer // dry-run sink when cli is nil (local-watch, tests)

	mu sync.Mutex
	// lastPub is the desired retained state: topic suffix -> payload. Every
	// change queues the suffix; on every broker (re)connect all of it is
	// queued again, so state published while the broker was away or lost by
	// a broker restart is restored.
	lastPub map[string]string
	// The send queue, in order. A state suffix is in it at most once: a
	// queued suffix that changes again keeps its place.
	queue     []outMsg
	queued    map[string]bool      // state suffixes in queue
	force     map[string]bool      // queued suffixes to send even if unchanged (reconnect)
	oneShots  int                  // one-shot messages in queue
	sentVal   map[string]string    // last payload handed to MQTT per suffix
	lastAt    map[string]time.Time // when each suffix was last handed to MQTT (throttling)
	retryAt   map[string]time.Time // a failed publish is resent from then on
	inFlight  int                  // messages taken off the queue, not yet handed to MQTT
	closing   bool
	dryTimer  *time.Timer              // dry run: drains again when a held-back value may go
	throttle  map[string]time.Duration // throttledSuffixes; a field so tests can shrink it
	retryWait time.Duration            // publishRetryDelay; a field so tests can shrink it

	kickCh   chan struct{} // wakes the sender
	stopCh   chan struct{} // closed by Disconnect
	loopDone chan struct{} // closed when the sender has stopped
	loopOnce sync.Once     // the sender starts with the first kick
	dryMu    sync.Mutex    // dry run: one drain at a time, so lines stay in order

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
	powerGen      uint64        // bumped on every PowerState value: identifies the Off a reset belongs to
	powerTimer    *time.Timer   // the pending power-off reset

	discoveryMu sync.Mutex
}

func NewBridge(cfg *Config, meta deviceMeta, log *Logger) *Bridge {
	b := &Bridge{
		cfg:       cfg,
		log:       log,
		out:       os.Stdout,
		lastPub:   map[string]string{},
		queued:    map[string]bool{},
		force:     map[string]bool{},
		sentVal:   map[string]string{},
		lastAt:    map[string]time.Time{},
		retryAt:   map[string]time.Time{},
		throttle:  maps.Clone(throttledSuffixes),
		retryWait: publishRetryDelay,
		kickCh:    make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		loopDone:  make(chan struct{}),
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

// maxReconnectInterval caps paho's reconnect backoff (1 s, 2 s, 4 s, ...).
// The library default is 10 minutes: after a broker outage of that length the
// bridge could sleep that long past the broker coming back, leaving HA stale.
const maxReconnectInterval = 30 * time.Second

// clientOptions builds the MQTT client configuration: one persistent session
// with a retained last will, auto-reconnect with a capped backoff.
func (b *Bridge) clientOptions() (*mqtt.ClientOptions, error) {
	scheme := "tcp"
	if b.cfg.MQTT.TLS {
		scheme = "ssl"
	}
	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("%s://%s:%d", scheme, b.cfg.MQTT.Host, b.cfg.MQTT.Port)).
		SetClientID(b.cfg.MQTT.ClientID).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(maxReconnectInterval).
		SetConnectRetry(true).
		SetConnectRetryInterval(5*time.Second).
		SetCleanSession(false).
		SetKeepAlive(30*time.Second).
		SetPingTimeout(10*time.Second).
		SetWriteTimeout(mqttWriteTimeout).
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
				return nil, fmt.Errorf("read mqtt.ca_file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("mqtt.ca_file %s: no PEM certificates found", b.cfg.MQTT.CAFile)
			}
			tc.RootCAs = pool
		}
		opts.SetTLSConfig(tc)
	}
	return opts, nil
}

func (b *Bridge) Connect(ctx context.Context) error {
	opts, err := b.clientOptions()
	if err != nil {
		return err
	}
	// Until the session with the hob is up, its data is not current. The
	// broker may still hold the previous run's retained link=online, and HA
	// would trust it the moment connected=online arrives, so link=offline is
	// the first thing onConnect sends. Queued only: there is no client yet.
	b.mu.Lock()
	b.publishLocked("link", "offline")
	b.mu.Unlock()

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
	// First, ahead of everything queued and of discovery: the hob link, then
	// online. HA keeps the entities unavailable (the broker may have fired
	// the last will) until online arrives, so it must not wait behind
	// discovery; and the link must be there before it, or HA trusts a stale
	// retained link for a moment.
	b.mu.Lock()
	if b.closing {
		b.mu.Unlock()
		return
	}
	b.lastPub["connected"] = "online"
	b.requeueFrontLocked("link", "connected")
	b.mu.Unlock()
	b.kick()
	b.waitHandedOver(mqttWriteTimeout, "link", "connected")

	b.publishDiscovery()
	b.clearRemoved()

	// Then the whole desired state, sent even where unchanged: anything lost
	// by a broker restart is restored. This comes after discovery on purpose:
	// paho resends the publishes that were in flight when the connection
	// dropped, concurrently with this callback, and those may be older. Going
	// last, the current values win.
	b.mu.Lock()
	if !b.closing {
		b.requeueAllLocked()
	}
	b.mu.Unlock()
	b.kick()
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
		b.sendOnce(discoveryTopic(b.cfg, d.component, d.objectID), "", true)
	}
	for _, s := range removedStateTopics {
		b.sendOnce(b.cfg.StateTopic(s), "", true)
	}
}

// Disconnect lets the queue drain briefly, stops the sender, then announces
// offline and closes the connection. Bounded: a broker that stopped reading
// costs at most a few seconds of the service manager's stop timeout.
func (b *Bridge) Disconnect() {
	if b.cli == nil {
		return
	}
	b.mu.Lock()
	if b.closing {
		b.mu.Unlock()
		return
	}
	b.closing = true // no more reconnect republishing
	b.mu.Unlock()
	if b.cli.IsConnectionOpen() {
		b.settle(time.Second)
	}
	close(b.stopCh)
	b.loopOnce.Do(func() { close(b.loopDone) }) // the sender never started
	select {
	case <-b.loopDone:
	case <-time.After(time.Second):
		b.log.Warn("MQTT sender did not stop; announcing offline anyway")
	}
	if b.cli.IsConnected() {
		tok := b.cli.Publish(b.cfg.StateTopic("connected"), 1, true, "offline")
		tok.WaitTimeout(2 * time.Second)
		b.cli.Disconnect(500)
	}
}

// publish sets a retained state topic, skipping unchanged values. Reports
// whether the desired state changed (the sender then delivers it).
func (b *Bridge) publish(suffix, payload string) bool {
	b.mu.Lock()
	changed := b.publishLocked(suffix, payload)
	b.mu.Unlock()
	if changed {
		b.kick()
	}
	return changed
}

// publishLocked is publish for callers that hold b.mu and need several
// changes to be atomic. It only queues: the caller kicks the sender after
// unlocking.
func (b *Bridge) publishLocked(suffix, payload string) bool {
	if payload == "" {
		payload = noValue // never an empty retained payload: see noValue
	}
	if prev, ok := b.lastPub[suffix]; ok && prev == payload {
		return false
	}
	b.lastPub[suffix] = payload
	b.enqueueLocked(suffix)
	return true
}

func (b *Bridge) enqueueLocked(suffix string) {
	if !b.queued[suffix] {
		b.queued[suffix] = true
		b.queue = append(b.queue, outMsg{suffix: suffix})
	}
}

// requeueFrontLocked moves the given state suffixes (those with a value) to
// the head of the queue, in that order, to be sent even if unchanged.
func (b *Bridge) requeueFrontLocked(suffixes ...string) {
	front := make([]outMsg, 0, len(suffixes)+len(b.queue))
	first := map[string]bool{}
	for _, s := range suffixes {
		if _, ok := b.lastPub[s]; ok {
			front = append(front, outMsg{suffix: s})
			first[s] = true
			b.queued[s], b.force[s] = true, true
			delete(b.retryAt, s)
		}
	}
	for _, m := range b.queue {
		if m.suffix == "" || !first[m.suffix] {
			front = append(front, m)
		}
	}
	b.queue = front
}

// requeueAllLocked queues the whole desired state, to be sent even where
// the broker should already have it.
func (b *Bridge) requeueAllLocked() {
	keys := make([]string, 0, len(b.lastPub))
	for s := range b.lastPub {
		keys = append(keys, s)
	}
	sort.Strings(keys)
	for _, s := range keys {
		b.enqueueLocked(s)
		b.force[s] = true
		delete(b.retryAt, s)
	}
}

// sendOnce queues a one-shot publish: no dedup, payload fixed.
func (b *Bridge) sendOnce(topic, payload string, retained bool) {
	b.mu.Lock()
	if b.oneShots >= maxQueuedOneShots {
		b.mu.Unlock()
		b.log.Warn("MQTT queue full; dropping %s = %s", topic, payload)
		return
	}
	b.oneShots++
	b.queue = append(b.queue, outMsg{topic: topic, payload: payload, retained: retained})
	b.mu.Unlock()
	b.kick()
}

// nextLocked takes the first queued message that may go out at now. wake is
// when the earliest message held back so far may go (zero: none).
//
// A state topic goes out with its current desired value, unless that is
// what was last sent (it changed and changed back) or it is held back: a
// failed publish waits retryWait, and a throttled topic waits out its gap
// unless the value moves to or from zero or "no value".
func (b *Bridge) nextLocked(now time.Time) (m outMsg, ok bool, wake time.Time) {
	hold := func(t time.Time) {
		if wake.IsZero() || t.Before(wake) {
			wake = t
		}
	}
	for i := 0; i < len(b.queue); {
		m := b.queue[i]
		if m.suffix == "" {
			b.queue = append(b.queue[:i], b.queue[i+1:]...)
			b.oneShots--
			return m, true, wake
		}
		s := m.suffix
		payload := b.lastPub[s]
		prev, had := b.sentVal[s]
		forced := b.force[s]
		if !forced && had && prev == payload {
			b.queue = append(b.queue[:i], b.queue[i+1:]...)
			delete(b.queued, s)
			continue
		}
		if at := b.retryAt[s]; now.Before(at) {
			hold(at)
			i++
			continue
		}
		if gap, throttled := b.throttle[s]; throttled && had && !forced {
			edge := prev == "0" || payload == "0" || prev == noValue || payload == noValue
			if until := b.lastAt[s].Add(gap); !edge && now.Before(until) {
				hold(until)
				i++
				continue
			}
		}
		b.queue = append(b.queue[:i], b.queue[i+1:]...)
		delete(b.queued, s)
		delete(b.force, s)
		delete(b.retryAt, s)
		b.sentVal[s], b.lastAt[s] = payload, now
		return outMsg{suffix: s, topic: b.cfg.StateTopic(s), payload: payload, retained: true}, true, wake
	}
	return outMsg{}, false, wake
}

// kick wakes the sender. Never call it with b.mu held: in a dry run it
// drains the queue inline.
func (b *Bridge) kick() {
	if b.cli == nil {
		b.drainDry()
		return
	}
	b.loopOnce.Do(func() { go b.sendLoop() })
	select {
	case b.kickCh <- struct{}{}:
	default:
	}
}

// sendLoop is the sender: the only goroutine that hands publishes to paho.
func (b *Bridge) sendLoop() {
	defer close(b.loopDone)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		wake, stopped := b.drainMQTT()
		if stopped {
			return
		}
		var timerC <-chan time.Time
		if !wake.IsZero() {
			timer.Reset(time.Until(wake))
			timerC = timer.C
		}
		select {
		case <-b.stopCh:
			return
		case <-b.kickCh:
		case <-timerC:
		}
		timer.Stop()
	}
}

// drainMQTT sends what may go out now, while the connection is open. While
// it is not, the queue simply waits: onConnect requeues everything and kicks.
func (b *Bridge) drainMQTT() (wake time.Time, stopped bool) {
	for {
		select {
		case <-b.stopCh:
			return time.Time{}, true
		default:
		}
		if !b.cli.IsConnectionOpen() {
			return time.Time{}, false
		}
		b.mu.Lock()
		m, ok, w := b.nextLocked(time.Now())
		if ok {
			b.inFlight++
		}
		b.mu.Unlock()
		if !ok {
			return w, false
		}
		b.publishNow(m)
		b.mu.Lock()
		b.inFlight--
		b.mu.Unlock()
	}
}

// publishNow hands one message to paho (blocking at most mqttWriteTimeout)
// and watches for its ack in the background. A state value that fails is
// queued again, unless it has been superseded meanwhile.
func (b *Bridge) publishNow(m outMsg) {
	tok := b.cli.Publish(m.topic, 1, m.retained, m.payload)
	go func() {
		var err error
		if !tok.WaitTimeout(publishTimeout) {
			err = fmt.Errorf("no ack within %s", publishTimeout)
		} else {
			err = tok.Error()
		}
		if err == nil {
			return
		}
		b.log.Warn("publish %s: %v", m.topic, err)
		if m.suffix != "" {
			b.retry(m.suffix, m.payload)
		}
	}()
}

func (b *Bridge) retry(suffix, payload string) {
	b.mu.Lock()
	if b.sentVal[suffix] == payload {
		delete(b.sentVal, suffix)
	}
	again := b.lastPub[suffix] == payload && !b.closing
	if again {
		b.retryAt[suffix] = time.Now().Add(b.retryWait)
		b.enqueueLocked(suffix)
	}
	b.mu.Unlock()
	if again {
		b.kick()
	}
}

// drainDry is the sender of a dry run (no MQTT client: local-watch, tests):
// it prints what would be published, inline and in order. A value held back
// by the throttle is printed when it may go.
func (b *Bridge) drainDry() {
	b.dryMu.Lock()
	defer b.dryMu.Unlock()
	for {
		b.mu.Lock()
		now := time.Now()
		m, ok, wake := b.nextLocked(now)
		if !ok {
			if !wake.IsZero() {
				if b.dryTimer == nil {
					b.dryTimer = time.AfterFunc(wake.Sub(now), b.drainDry)
				} else {
					b.dryTimer.Reset(wake.Sub(now))
				}
			}
			b.mu.Unlock()
			return
		}
		b.mu.Unlock()
		retainedTag := ""
		if m.retained {
			retainedTag = " [retained]"
		}
		fmt.Fprintf(b.out, "[%s] [PUB]    %-50s = %s%s\n",
			now.Format("15:04:05.000"), m.topic, m.payload, retainedTag)
	}
}

// waitHandedOver waits until none of the suffixes is queued or in flight,
// up to d.
func (b *Bridge) waitHandedOver(d time.Duration, suffixes ...string) {
	deadline := time.Now().Add(d)
	for {
		b.mu.Lock()
		done := b.inFlight == 0
		for _, s := range suffixes {
			done = done && !b.queued[s]
		}
		b.mu.Unlock()
		if done || time.Now().After(deadline) {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// settle waits until nothing is queued or in flight, up to d. Reports
// whether the queue drained.
func (b *Bridge) settle(d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		b.mu.Lock()
		idle := len(b.queue) == 0 && b.inFlight == 0
		b.mu.Unlock()
		if idle {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
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
		if d.Value.V == nil && d.Value.Symbol == "" {
			// Nothing to report (a JSON null): publishing it would send an
			// empty state and erase a retained value such as the filter's.
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
		b.sendOnce(b.cfg.StateTopic(suffix), state, false)
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
	b.publish(suffix, payload) // throttledSuffixes are rate-limited by the sender

	if key == "BSH.Common.Setting.PowerState" {
		b.powerStateChanged(payload == "Off")
	}
}

// powerStateChanged starts a new power generation: whatever reset an
// earlier Off scheduled is cancelled, and an Off schedules its own, which
// runs after the grace only if no PowerState value has arrived since. So in
// Off, On, Off the first Off's reset cannot fire inside the second's grace.
func (b *Bridge) powerStateChanged(off bool) {
	b.mu.Lock()
	b.powerGen++
	gen := b.powerGen
	if b.powerTimer != nil {
		b.powerTimer.Stop()
		b.powerTimer = nil
	}
	changed := false
	if off {
		if b.powerOffDelay <= 0 {
			changed = b.resetOnPowerOffLocked()
		} else {
			b.powerTimer = time.AfterFunc(b.powerOffDelay, func() { b.powerOffExpired(gen) })
		}
	}
	b.mu.Unlock()
	if changed {
		b.kick()
	}
}

// powerOffExpired runs the reset of power generation gen, unless a newer
// PowerState value superseded it. Check and reset are one critical section,
// so a zone update cannot slip in between and be overwritten.
func (b *Bridge) powerOffExpired(gen uint64) {
	b.mu.Lock()
	changed := false
	if gen == b.powerGen && b.lastPub["status/power_state"] == "Off" {
		b.powerTimer = nil
		changed = b.resetOnPowerOffLocked()
	}
	b.mu.Unlock()
	if changed {
		b.kick()
	}
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
	changed := b.publishJoinsLocked()
	b.mu.Unlock()
	if changed {
		b.kick()
	}
}

func (b *Bridge) publishJoinsLocked() bool {
	left := b.joinFlag["left"] && b.zones["FrontLeft"].state == "Active" && b.zones["RearLeft"].state == "Active"
	right := b.joinFlag["right"] && b.zones["FrontRight"].state == "Active" && b.zones["RearRight"].state == "Active"
	l := b.publishLocked("joined_left", strconv.FormatBool(left))
	r := b.publishLocked("joined_right", strconv.FormatBool(right))
	return l || r
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
	b.mu.Lock()
	changed := b.updateDeadlineLocked(suffix, remainingSecs)
	b.mu.Unlock()
	if changed {
		b.kick()
	}
	return changed
}

// updateDeadlineLocked keeps deadlines and lastPub in step: both change in
// the same critical section.
func (b *Bridge) updateDeadlineLocked(suffix string, remainingSecs int) bool {
	var deadline time.Time
	if remainingSecs > 0 {
		deadline = time.Now().Add(time.Duration(remainingSecs) * time.Second)
	}
	prev := b.deadlines[suffix]
	if !deadline.IsZero() && !prev.IsZero() {
		d := deadline.Sub(prev)
		if d < timerJitter && d > -timerJitter {
			return false
		}
	}
	b.deadlines[suffix] = deadline
	return b.publishLocked(suffix, formatDeadline(deadline))
}

// resetOnPowerOff: once the hob is off no zone heats, no timer runs and no
// pair is joined. The hob pushes all of this itself right after power-off;
// resetting here (after powerOffGrace) covers a missed push. Residual heat is
// kept: the hob reports it after power-off.
// Called with b.mu held; reports whether anything changed.
func (b *Bridge) resetOnPowerOffLocked() bool {
	changed := false
	for _, name := range allZones {
		z := b.zones[name]
		z.power = "off"
		if z.state == "Active" {
			z.state = "Off"
		}
		z.op = ""
		slug := zoneSlug(name)
		changed = b.publishLocked("zone/"+slug+"/power", "off") || changed
		changed = b.publishLocked("zone/"+slug+"/state", zoneStateLabel(z.state, z.op)) || changed
		changed = b.updateDeadlineLocked("zone/"+slug+"/timer_deadline", 0) || changed
	}
	b.joinFlag = map[string]bool{}
	changed = b.updateDeadlineLocked("status/program_timer_deadline", 0) || changed
	return b.publishJoinsLocked() || changed
}

// PublishLink publishes the local link's health. These topics are not gated
// by the link itself: they must stay visible exactly when it is down.
// The three topics change together, so they are queued together.
func (b *Bridge) PublishLink(state hc.LinkState, since time.Time, powerCycleNeeded bool) {
	b.mu.Lock()
	changed := b.publishLocked("status/link_state", string(state))
	changed = b.publishLocked("status/link_since", formatDeadline(since)) || changed
	changed = b.publishLocked("status/power_cycle_needed", strconv.FormatBool(powerCycleNeeded)) || changed
	b.mu.Unlock()
	if changed {
		b.kick()
	}
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
