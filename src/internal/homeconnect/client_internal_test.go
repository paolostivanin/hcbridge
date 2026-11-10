package homeconnect

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestNextBackoff(t *testing.T) {
	d := 5 * time.Second
	var got []time.Duration
	for i := 0; i < 8; i++ {
		got = append(got, d)
		d = nextBackoff(d, 5*time.Minute)
	}
	want := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second, 160 * time.Second, 5 * time.Minute, 5 * time.Minute}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

func TestApplyJitterBounds(t *testing.T) {
	d := 10 * time.Second
	if got := applyJitter(d, 0); got != 8*time.Second {
		t.Errorf("r=0: %v", got)
	}
	if got := applyJitter(d, 0.5); got != 10*time.Second {
		t.Errorf("r=0.5: %v", got)
	}
	if got := applyJitter(d, 0.999999); got >= 12*time.Second || got < 11*time.Second {
		t.Errorf("r~1: %v", got)
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		err   error
		fails int
		want  LinkState
	}{
		{&UpgradeError{Status: 503}, 0, LinkRefused},
		{fmt.Errorf("wrapped: %w", &UpgradeError{Status: 503}), 0, LinkRefused},
		{&UpgradeError{Status: 403}, 0, LinkError},
		{&DialError{Err: errors.New("i/o timeout")}, 0, LinkUnreachable},
		{ErrFirstFrameMAC, 1, LinkError},
		{ErrFirstFrameMAC, 3, LinkKeyError},
		{&TimeoutError{Resource: "/ci/services"}, 0, LinkError},
		{errors.New("other"), 0, LinkError},
	}
	for _, c := range cases {
		if got := classify(c.err, c.fails, 3); got != c.want {
			t.Errorf("classify(%v, %d) = %s, want %s", c.err, c.fails, got, c.want)
		}
	}
}

func TestDescribeRefusal(t *testing.T) {
	err := &UpgradeError{Status: 503, Header: map[string][]string{"Server": {"WebSocket++/0.8.2"}}, Body: []byte("busy")}
	got := describe(err)
	want := `websocket upgrade refused: HTTP 503 Service Unavailable, Server: WebSocket++/0.8.2, body: "busy"`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestNewClientValidation(t *testing.T) {
	p := &Profile{}
	log := nopLogger{}
	if _, err := NewClient(Options{DeviceID: "x", Log: log}, p, nil); err == nil {
		t.Error("missing host accepted")
	}
	if _, err := NewClient(Options{Host: "h", Log: log}, p, nil); err == nil {
		t.Error("missing device ID accepted")
	}
	c, err := NewClient(Options{Host: "192.168.30.23", DeviceID: "x", Log: log}, p, nil)
	if err != nil || c.hostport != "192.168.30.23:80" {
		t.Errorf("default port: %v %q", err, c.hostport)
	}
	c, _ = NewClient(Options{Host: "10.0.0.1:8080", DeviceID: "x", Log: log}, p, nil)
	if c.hostport != "10.0.0.1:8080" {
		t.Errorf("explicit port: %q", c.hostport)
	}
}

type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}
