package main

import (
	"errors"
	"fmt"
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
}

func newFakeMQTT() *fakeMQTT { return &fakeMQTT{failures: map[string]int{}} }

func (f *fakeMQTT) IsConnected() bool { return f.IsConnectionOpen() }
func (f *fakeMQTT) IsConnectionOpen() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeAfter == 0 || len(f.pubs) < f.closeAfter
}

func (f *fakeMQTT) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
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

func TestOnlineAnnouncedFirstOnEveryConnect(t *testing.T) {
	b := newTestBridge()
	f := newFakeMQTT()
	b.cli = f
	for i := 0; i < 3; i++ { // the first connect, then two reconnects with the dedup state kept
		before := len(f.published())
		b.onConnect(f)
		got := f.published()[before:]
		if len(got) == 0 {
			t.Fatalf("connect %d: nothing published", i)
		}
		if first := got[0]; first.topic != "homeconnect/cooktop/connected" || first.payload != "online" || !first.retained || first.qos != 1 {
			t.Fatalf("connect %d: first publish is %+v, want connected=online (retained, QoS 1) before anything else", i, first)
		}
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
