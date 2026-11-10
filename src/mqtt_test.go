package main

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type doneToken struct{ err error }

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
	return f.closeAfter == 0 || len(f.pubs) < f.closeAfter
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
