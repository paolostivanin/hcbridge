package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

type doneToken struct{ err error }

// lostToken is a publish the broker never acks.
type lostToken struct{ doneToken }

func (lostToken) WaitTimeout(time.Duration) bool { return false }

func (t doneToken) Wait() bool                     { return true }
func (t doneToken) WaitTimeout(time.Duration) bool { return true }
func (t doneToken) Done() <-chan struct{}          { c := make(chan struct{}); close(c); return c }
func (t doneToken) Error() error                   { return t.err }

type pubRec struct {
	topic, payload string
	qos            byte
	retained       bool
}

// fakeMQTT records publishes. Failures can be injected per topic, and the
// connection can be reported closed after a number of publishes.
type fakeMQTT struct {
	mqtt.Client // nil: a method the tests do not expect panics

	mu         sync.Mutex
	pubs       []pubRec
	failures   map[string]int // topic -> failures still to inject
	noAck      map[string]int // topic -> publishes still to leave unacked
	closeAfter int            // >0: connection reads closed after this many publishes
	holds      map[string]*heldTopic
	stall      chan struct{} // non-nil: every Publish blocks until closed, at most stallFor
	stallFor   time.Duration
}

// heldTopic blocks the next Publish of a topic until released.
type heldTopic struct {
	entered, release chan struct{}
}

func newFakeMQTT() *fakeMQTT {
	return &fakeMQTT{failures: map[string]int{}, holds: map[string]*heldTopic{}}
}

// hold makes the next Publish of topic block until release is called;
// entered closes when that Publish has started.
func (f *fakeMQTT) hold(topic string) (entered <-chan struct{}, release func()) {
	h := &heldTopic{entered: make(chan struct{}), release: make(chan struct{})}
	f.mu.Lock()
	f.holds[topic] = h
	f.mu.Unlock()
	var once sync.Once
	return h.entered, func() { once.Do(func() { close(h.release) }) }
}

func (f *fakeMQTT) Disconnect(uint) {}

func (f *fakeMQTT) IsConnected() bool { return f.IsConnectionOpen() }
func (f *fakeMQTT) IsConnectionOpen() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeAfter == 0 || len(f.pubs) < f.closeAfter // closeAfter -1: never open
}

func (f *fakeMQTT) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	f.mu.Lock()
	h := f.holds[topic]
	delete(f.holds, topic)
	stall, stallFor := f.stall, f.stallFor
	f.mu.Unlock()
	if h != nil {
		close(h.entered)
		<-h.release
	}
	if stall != nil { // like paho with a write timeout: blocks, then fails
		select {
		case <-stall:
		case <-time.After(stallFor):
			return doneToken{err: errors.New("publish was broken by timeout")}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var p string
	switch v := payload.(type) {
	case []byte:
		p = string(v)
	case string:
		p = v
	default:
		p = fmt.Sprint(v)
	}
	f.pubs = append(f.pubs, pubRec{topic: topic, payload: p, qos: qos, retained: retained})
	if f.failures[topic] > 0 {
		f.failures[topic]--
		return doneToken{err: errors.New("injected failure")}
	}
	if f.noAck[topic] > 0 {
		f.noAck[topic]--
		return lostToken{}
	}
	return doneToken{}
}

func (f *fakeMQTT) published() []pubRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pubRec(nil), f.pubs...)
}

// last returns the last payload published to topic.
func (f *fakeMQTT) last(topic string) (string, bool) {
	pubs := f.published()
	for i := len(pubs) - 1; i >= 0; i-- {
		if pubs[i].topic == topic {
			return pubs[i].payload, true
		}
	}
	return "", false
}

func (f *fakeMQTT) count(topic string) int {
	n := 0
	for _, p := range f.published() {
		if p.topic == topic {
			n++
		}
	}
	return n
}

// ---- connection options (fix: the library's 10 minute reconnect cap) -------

func TestClientOptions(t *testing.T) {
	cfg := testConfig()
	cfg.MQTT.Host, cfg.MQTT.ClientID = "192.0.2.10", "hcbridge"
	b := NewBridge(cfg, deviceMeta{}, quietLogger())
	opts, err := b.clientOptions()
	if err != nil {
		t.Fatal(err)
	}
	if opts.MaxReconnectInterval != 30*time.Second {
		t.Errorf("MaxReconnectInterval = %s, want 30s (paho's default is 10 min)", opts.MaxReconnectInterval)
	}
	if !opts.AutoReconnect || !opts.ConnectRetry || opts.CleanSession || opts.KeepAlive != 30 {
		t.Errorf("auto-reconnect %v, connect-retry %v, clean session %v, keepalive %ds", opts.AutoReconnect, opts.ConnectRetry, opts.CleanSession, opts.KeepAlive)
	}
	if opts.WriteTimeout != mqttWriteTimeout {
		t.Errorf("WriteTimeout = %s, want %s (paho's default blocks a publish 30 s and never times out a socket write)", opts.WriteTimeout, mqttWriteTimeout)
	}
	if !opts.WillEnabled || opts.WillTopic != "homeconnect/cooktop/connected" || string(opts.WillPayload) != "offline" || !opts.WillRetained || opts.WillQos != 1 {
		t.Errorf("last will %v %q %q retained=%v qos=%d", opts.WillEnabled, opts.WillTopic, opts.WillPayload, opts.WillRetained, opts.WillQos)
	}
	if opts.ClientID != "hcbridge" || len(opts.Servers) != 1 || opts.Servers[0].String() != "tcp://192.0.2.10:1883" || opts.OnConnect == nil {
		t.Errorf("client %q servers %v", opts.ClientID, opts.Servers)
	}

	cfg.MQTT.TLS, cfg.MQTT.Port, cfg.MQTT.CAFile = true, 8883, "/nonexistent/ca.pem"
	if _, err := b.clientOptions(); err == nil || !strings.Contains(err.Error(), "ca_file") {
		t.Errorf("missing CA file: %v", err)
	}
}

// ---- "online" first (fix: the dedup swallowed it on reconnect) -------------

// The hob link goes first, then online: on a cold start the link is offline
// until the session is up, and HA must see that before connected=online, or
// it shows the previous run's retained values as current.
func TestOnlineAnnouncedFirstOnEveryConnect(t *testing.T) {
	b := newTestBridge()
	f := newFakeMQTT()
	b.cli = f
	b.SetLink(false) // what Connect queues before the first connect
	b.settle(time.Second)
	for i := 0; i < 3; i++ { // the first connect, then two reconnects with the dedup state kept
		before := len(f.published())
		b.onConnect(f)
		b.settle(time.Second)
		got := f.published()[before:]
		if len(got) < 2 {
			t.Fatalf("connect %d: published %+v", i, got)
		}
		if l := got[0]; l.topic != "homeconnect/cooktop/link" || l.payload != "offline" || !l.retained || l.qos != 1 {
			t.Fatalf("connect %d: first publish is %+v, want link=offline (retained, QoS 1)", i, l)
		}
		if c := got[1]; c.topic != "homeconnect/cooktop/connected" || c.payload != "online" || !c.retained || c.qos != 1 {
			t.Fatalf("connect %d: second publish is %+v, want connected=online (retained, QoS 1) before discovery", i, c)
		}
	}
}

// ---- one sender, newest value last (fix: the replay overwrote newer values) -

// The reconnect republish runs while the hob keeps pushing. A value that
// changes while the republish is on its way must end up on the broker, not
// be overwritten by the republished old one (which the dedup would then
// never correct).
func TestReplayCannotOverwriteANewerValue(t *testing.T) {
	b := newTestBridge()
	f := newFakeMQTT()
	b.cli = f
	apply(b, sym("BSH.Common.Setting.PowerState", "On"),
		sym("Cooking.Hob.Status.Zone.100.State", "Active"), sym("Cooking.Hob.Status.Zone.100.PowerLevel", "50"))
	b.SetLink(true)
	b.settle(time.Second)

	power := b.cfg.StateTopic("zone/front_left/power")
	entered, release := f.hold(power)
	defer release()
	done := make(chan struct{})
	go func() { b.onConnect(f); close(done) }() // a broker reconnect
	select {
	case <-entered: // the republished "5" is on its way
	case <-time.After(2 * time.Second):
		t.Fatal("republish never reached the zone power")
	}
	apply(b, sym("Cooking.Hob.Status.Zone.100.PowerLevel", "Off"), sym("Cooking.Hob.Status.Zone.100.State", "Off"))
	b.SetLink(false)
	release()
	<-done
	if !b.settle(2 * time.Second) {
		t.Fatal("queue never drained")
	}
	b.mu.Lock()
	desired := maps.Clone(b.lastPub)
	b.mu.Unlock()
	for suffix, v := range desired {
		if got, _ := f.last(b.cfg.StateTopic(suffix)); got != v {
			t.Errorf("broker has %s = %q, desired %q", suffix, got, v)
		}
	}
	if got, _ := f.last(power); got != "off" {
		t.Fatalf("zone power on the broker = %q, want off", got)
	}
}

// A broker that stops reading must not hold up the hob's values (a full
// event queue stops the session's read loop, and with it the pongs), and
// shutting down must stay bounded.
func TestStalledBrokerDoesNotBlockTheHob(t *testing.T) {
	b := newTestBridge()
	f := newFakeMQTT()
	f.stall, f.stallFor = make(chan struct{}), 300*time.Millisecond
	defer close(f.stall)
	b.cli = f
	done := make(chan struct{})
	go func() {
		for i := 0; i < 2000; i++ {
			apply(b, num("Cooking.Hob.Status.Zone.200.RemainingProgramTime", float64(100+10*i)),
				sym("BSH.Common.Event.ProgramFinished", "Present"))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("applying values waited on the stalled broker")
	}
	start := time.Now()
	b.Disconnect()
	if d := time.Since(start); d > 4*time.Second {
		t.Fatalf("Disconnect took %s against a stalled broker", d)
	}
}

// ---- discovery keeps going past a failed entity (fix: it aborted) ----------

func discoveryTopicOf(cfg *Config, i int) (string, string) {
	e := discoveryEntities(cfg)[i]
	return discoveryTopic(cfg, e.component, e.objectID), e.objectID
}

func TestDiscoveryContinuesPastAFailedEntityAndRetriesIt(t *testing.T) {
	cfg := testConfig()
	f := newFakeMQTT()
	bad, _ := discoveryTopicOf(cfg, 4)
	f.failures[bad] = 1 // fails once; the retry gets through
	if err := PublishDiscovery(cfg, deviceMeta{}, f, quietLogger()); err != nil {
		t.Fatalf("a transient failure must not surface: %v", err)
	}
	for i := range discoveryEntities(cfg) {
		topic, _ := discoveryTopicOf(cfg, i)
		want := 1
		if topic == bad {
			want = 2
		}
		if got := f.count(topic); got != want {
			t.Errorf("%s published %d times, want %d", topic, got, want)
		}
	}
}

func TestDiscoveryReportsAPersistentFailureAfterOneRetry(t *testing.T) {
	cfg := testConfig()
	f := newFakeMQTT()
	bad, id := discoveryTopicOf(cfg, 4)
	f.failures[bad] = 99
	err := PublishDiscovery(cfg, deviceMeta{}, f, quietLogger())
	if err == nil || !strings.Contains(err.Error(), id) {
		t.Fatalf("error %v should name %s", err, id)
	}
	if got := f.count(bad); got != 2 {
		t.Errorf("failed entity attempted %d times, want 2 (pass + one retry)", got)
	}
	if got := len(f.published()); got != len(discoveryEntities(cfg))+1 {
		t.Errorf("%d publishes, want every entity once plus one retry", got)
	}
}

func TestDiscoveryStopsWhenTheConnectionIsGone(t *testing.T) {
	cfg := testConfig()
	f := newFakeMQTT()
	f.closeAfter = 3
	err := PublishDiscovery(cfg, deviceMeta{}, f, quietLogger())
	if err == nil || !strings.Contains(err.Error(), "connection lost") {
		t.Fatalf("error %v", err)
	}
	if got := len(f.published()); got != 3 {
		t.Errorf("%d publishes after the connection closed, want it to stop at 3", got)
	}
}

func TestDiscoveryGivesUpAfterConsecutiveFailures(t *testing.T) {
	cfg := testConfig()
	f := newFakeMQTT()
	for i := range discoveryEntities(cfg) {
		topic, _ := discoveryTopicOf(cfg, i)
		f.failures[topic] = 99
	}
	if err := PublishDiscovery(cfg, deviceMeta{}, f, quietLogger()); err == nil || !strings.Contains(err.Error(), "giving up") {
		t.Fatalf("error %v", err)
	}
	if got := len(f.published()); got != maxDiscoveryFailures {
		t.Errorf("%d publishes against a failing broker, want %d (no grinding through every entity)", got, maxDiscoveryFailures)
	}
}

// ---- the real paho client against an in-process broker ----------------------

func brokerBridge(t *testing.T, fb *fakeBroker) *Bridge {
	t.Helper()
	cfg := testConfig()
	cfg.MQTT.Host, cfg.MQTT.Port = fb.hostPort()
	cfg.MQTT.ClientID = "hcbridge-test"
	cfg.MQTT.Username, cfg.MQTT.Password = "u", "p"
	b := NewBridge(cfg, deviceMeta{}, quietLogger())
	if err := b.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Disconnect)
	return b
}

func waitRetained(t *testing.T, fb *fakeBroker, topic, value string) {
	t.Helper()
	if !waitFor(t, 5*time.Second, func() bool { v, _ := fb.get(topic); return v == value }) {
		v, ok := fb.get(topic)
		t.Fatalf("broker has %s = %q (present %v), want %q", topic, v, ok, value)
	}
}

// Connect: the link goes out offline before online, then discovery and the
// state; a broker restart that lost the retained state gets all of it back;
// Disconnect announces offline last and says goodbye.
func TestBridgeAgainstABroker(t *testing.T) {
	fb := newFakeBroker(t)
	b := brokerBridge(t, fb)
	root := b.cfg.StateTopic("")
	waitRetained(t, fb, root+"connected", "online")
	waitRetained(t, fb, root+"link", "offline")
	// The link before online, both before discovery. (paho may send the
	// very first publish twice: one it hands over while resuming its store
	// at connect is resent as a duplicate. Same value, so harmless.)
	first := func(prefix string) int {
		for i, p := range fb.published() {
			if strings.HasPrefix(p.topic, prefix) {
				return i
			}
		}
		return 1 << 30
	}
	waitFor(t, 5*time.Second, func() bool { return first("homeassistant/") < 1<<30 })
	if l, c, d := first(root+"link"), first(root+"connected"), first("homeassistant/"); !(l < c && c < d) {
		t.Fatalf("first link at %d, connected at %d, discovery at %d: want that order", l, c, d)
	}

	apply(b, sym("BSH.Common.Setting.PowerState", "On"), sym("Cooking.Hob.Status.Zone.100.PowerLevel", "50"))
	b.SetLink(true)
	waitRetained(t, fb, root+"zone/front_left/power", "5")
	waitRetained(t, fb, root+"link", "online")

	fb.wipe(root + "zone/front_left/power") // the broker restarts without persistence
	fb.dropAll()
	waitRetained(t, fb, root+"zone/front_left/power", "5")
	if c, _ := fb.counts(); c < 2 {
		t.Fatalf("%d connects, want a reconnect", c)
	}

	b.Disconnect()
	waitRetained(t, fb, root+"connected", "offline")
	if !waitFor(t, 2*time.Second, func() bool { _, d := fb.counts(); return d == 1 }) {
		_, d := fb.counts()
		t.Fatalf("%d clean disconnects, want 1", d)
	}
	b.Disconnect() // a second call is a no-op
}

func TestClientOptionsTLS(t *testing.T) {
	cfg := testConfig()
	cfg.MQTT.Host, cfg.MQTT.TLS, cfg.MQTT.Port = "192.0.2.10", true, 8883
	cfg.MQTT.Username, cfg.MQTT.Password = "user", "secret"
	b := NewBridge(cfg, deviceMeta{}, quietLogger())

	cfg.MQTT.CAFile = filepath.Join(t.TempDir(), "ca.pem")
	os.WriteFile(cfg.MQTT.CAFile, selfSignedPEM(t), 0o600)
	opts, err := b.clientOptions()
	if err != nil {
		t.Fatal(err)
	}
	if opts.Servers[0].String() != "ssl://192.0.2.10:8883" || opts.TLSConfig == nil || opts.TLSConfig.RootCAs == nil || opts.Username != "user" {
		t.Fatalf("servers %v tls %+v user %q", opts.Servers, opts.TLSConfig, opts.Username)
	}

	os.WriteFile(cfg.MQTT.CAFile, []byte("not a certificate"), 0o600)
	if _, err := b.clientOptions(); err == nil || !strings.Contains(err.Error(), "no PEM certificates") {
		t.Fatalf("bad CA file: %v", err)
	}
	cfg.MQTT.CAFile = ""
	if opts, err := b.clientOptions(); err != nil || opts.TLSConfig.RootCAs != nil {
		t.Fatalf("system roots: %v", err)
	}
}

func selfSignedPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// While the connection is down the queue waits (coalescing each topic to
// its newest value; one that changed back is not sent at all), and the next
// connect sends link and online ahead of it.
func TestQueueWhileDisconnected(t *testing.T) {
	b := newTestBridge()
	f := newFakeMQTT()
	b.cli = f
	b.SetLink(true)
	b.publish("status/x", "1")
	b.settle(time.Second)
	f.mu.Lock()
	f.closeAfter = len(f.pubs) // the connection drops
	f.mu.Unlock()
	b.publish("status/x", "2")
	b.publish("status/x", "1") // changed back: the broker already has it
	b.publish("status/y", "a")
	b.SetLink(false)
	time.Sleep(20 * time.Millisecond)
	before := len(f.published())
	if b.settle(10 * time.Millisecond) {
		t.Fatal("queue drained with the connection down")
	}

	f.mu.Lock()
	f.closeAfter = 0 // back
	f.mu.Unlock()
	b.kick()
	b.settle(time.Second)
	got := f.published()[before:]
	if n := f.count(b.cfg.StateTopic("status/x")); n != 1 {
		t.Errorf("status/x published %d times, want once (it changed back)", n)
	}
	if len(got) != 2 || got[0].topic != b.cfg.StateTopic("status/y") || got[1].payload != "offline" {
		t.Errorf("after reconnecting: %+v", got)
	}

	// A connect with messages still queued puts link and online first.
	f.mu.Lock()
	f.closeAfter = len(f.pubs)
	f.mu.Unlock()
	b.publish("status/z", "q")
	b.sendOnce(b.cfg.StateTopic("event/e"), "Present", false)
	f.mu.Lock()
	f.closeAfter = 0
	f.mu.Unlock()
	before = len(f.published())
	b.onConnect(f)
	b.settle(time.Second)
	got = f.published()[before:]
	if len(got) < 4 || got[0].topic != b.cfg.StateTopic("link") || got[1].topic != b.cfg.StateTopic("connected") ||
		got[2].topic != b.cfg.StateTopic("status/z") || got[3].topic != b.cfg.StateTopic("event/e") {
		t.Errorf("connect with a backlog: %+v", got[:min(len(got), 4)])
	}
}

// A publish the broker never acks is sent again.
func TestUnackedPublishIsSentAgain(t *testing.T) {
	b := newTestBridge()
	b.retryWait = 5 * time.Millisecond
	f := newFakeMQTT()
	f.noAck = map[string]int{}
	topic := b.cfg.StateTopic("status/x")
	f.noAck[topic] = 1
	b.cli = f
	b.publish("status/x", "1")
	if !waitFor(t, 2*time.Second, func() bool { return f.count(topic) == 2 }) {
		t.Fatalf("published %d times", f.count(topic))
	}
}

// New firmware versions republish discovery while connected; the one-shot
// queue is capped while the broker is away.
func TestDeviceInfoAndOneShotCap(t *testing.T) {
	b := newTestBridge()
	f := newFakeMQTT()
	b.cli = f
	if !b.SetDeviceInfo(deviceMeta{SwVersion: "2.17.1"}) {
		t.Fatal("new firmware not noticed")
	}
	topic, _ := discoveryTopicOf(b.cfg, 0)
	if !waitFor(t, 2*time.Second, func() bool { return f.count(topic) == 1 }) {
		t.Fatal("discovery not republished")
	}
	if b.SetDeviceInfo(deviceMeta{SwVersion: "2.17.1"}) {
		t.Fatal("unchanged firmware reported as changed")
	}

	f.mu.Lock()
	f.closeAfter = len(f.pubs)
	f.mu.Unlock()
	for i := 0; i < maxQueuedOneShots+10; i++ {
		b.sendOnce(b.cfg.StateTopic("event/e"), "Present", false)
	}
	b.mu.Lock()
	n := b.oneShots
	b.mu.Unlock()
	if n != maxQueuedOneShots {
		t.Fatalf("%d one-shots queued, want the cap %d", n, maxQueuedOneShots)
	}
}

func TestShutdownEdges(t *testing.T) {
	// A connect callback that arrives during shutdown does nothing.
	b := newTestBridge()
	f := newFakeMQTT()
	b.cli = f
	b.Disconnect() // the sender never started
	b.onConnect(f)
	if n := len(f.published()); n != 1 || f.published()[0].payload != "offline" {
		t.Fatalf("published %+v; want only connected=offline", f.published())
	}
	newTestBridge().Disconnect() // no client: nothing to do

	// A broken TLS setup fails Connect before any network access.
	cfg := testConfig()
	cfg.MQTT.Host, cfg.MQTT.TLS, cfg.MQTT.CAFile = "192.0.2.10", true, "/nonexistent/ca.pem"
	if err := NewBridge(cfg, deviceMeta{}, quietLogger()).Connect(context.Background()); err == nil {
		t.Fatal("Connect with a missing CA file")
	}
}

// After a reconnect paho resends what was in flight, in random order; the
// republish of the current state waits for those to be acknowledged, so
// the current values land last.
func TestRepublishWaitsForPahosResend(t *testing.T) {
	b := newTestBridge()
	f := newFakeMQTT()
	b.cli = f
	b.publish("status/x", "new")
	b.settle(time.Second)
	b.store.Open()
	defer b.store.Close()
	stale := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	stale.MessageID = 7
	b.store.Put("o.7", stale) // in flight when the connection dropped

	done := make(chan struct{})
	go func() { b.onConnect(f); close(done) }()
	time.Sleep(100 * time.Millisecond)
	if n := f.count(b.cfg.StateTopic("status/x")); n != 1 {
		t.Fatalf("status/x republished %d times while paho's resend was pending", n-1)
	}
	b.store.Del("o.7") // the broker acked the resend
	<-done
	b.settle(time.Second)
	if n := f.count(b.cfg.StateTopic("status/x")); n != 2 {
		t.Fatalf("status/x published %d times, want the republish after the resend", n)
	}
}

// Connects that never drain the queue do not pile up clean-up messages and
// push events out.
func TestCleanUpsAreQueuedOnce(t *testing.T) {
	b := newTestBridge()
	f := newFakeMQTT()
	f.closeAfter = -1 // never open
	b.cli = f
	for i := 0; i < 5; i++ { // what each connect queues
		b.clearRemoved()
	}
	b.mu.Lock()
	n := b.oneShots
	b.mu.Unlock()
	if want := len(removedDiscovery) + len(removedStateTopics); n != want {
		t.Fatalf("%d one-shots queued after 5 connects, want %d", n, want)
	}
	b.sendOnce(b.cfg.StateTopic("event/program_finished"), "Present", false)
	b.sendOnce(b.cfg.StateTopic("event/program_finished"), "Present", false)
	b.mu.Lock()
	n2 := b.oneShots
	b.mu.Unlock()
	if n2 != n+2 {
		t.Fatalf("events are occurrences: both must be queued (%d -> %d)", n, n2)
	}
}

// A value held back by the throttle goes out at shutdown, at once.
func TestHeldValueGoesOutAtShutdown(t *testing.T) {
	b := newTestBridge()
	b.throttle["status/program_progress"] = time.Hour
	f := newFakeMQTT()
	b.cli = f
	apply(b, num("BSH.Common.Option.ProgramProgress", 98))
	b.settle(time.Second)
	apply(b, num("BSH.Common.Option.ProgramProgress", 99)) // held
	if b.settle(50 * time.Millisecond) {
		t.Fatal("99 was not held back")
	}
	began := time.Now()
	b.Disconnect()
	if d := time.Since(began); d > 900*time.Millisecond {
		t.Errorf("Disconnect took %s: it waited for the throttle", d)
	}
	pubs := f.published()
	if v, _ := f.last(b.cfg.StateTopic("status/program_progress")); v != "99" || pubs[len(pubs)-1].payload != "offline" {
		t.Fatalf("progress %q on the broker, last publish %+v", v, pubs[len(pubs)-1])
	}
}

// paho's resume() writes the DUP flag on stored publishes while the same
// packet may be in flight; the store must hand it a copy.
func TestStoreKeepsACopyOfPublishes(t *testing.T) {
	st := copyingStore{mqtt.NewMemoryStore()}
	st.Open()
	defer st.Close()
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload, p.MessageID, p.Qos = "t", []byte("v"), 9, 1
	st.Put("o.9", p)
	got, ok := st.Get("o.9").(*packets.PublishPacket)
	if !ok || got == p || got.TopicName != "t" || string(got.Payload) != "v" || got.MessageID != 9 {
		t.Fatalf("stored %p (%+v), sent %p", got, got, p)
	}
	got.Dup = true
	if p.Dup {
		t.Fatal("marking the stored packet changed the one in flight")
	}
	rel := packets.NewControlPacket(packets.Pubrel)
	st.Put("i.1", rel)
	if st.Get("i.1") != rel {
		t.Error("other packets are stored as they are")
	}
}
