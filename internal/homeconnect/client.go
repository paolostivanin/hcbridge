package homeconnect

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Logger is the logging the client needs; *main.Logger satisfies it.
type Logger interface {
	Debug(format string, args ...any)
	Info(format string, args ...any)
	Warn(format string, args ...any)
	Error(format string, args ...any)
}

// Options configures a Client. Zero durations take the defaults below,
// which follow what keeps these appliances healthy: one connection held for
// days, generous liveness timeouts, and slow, jittered reconnects.
type Options struct {
	Host       string // "ip" or "ip:port"; port 80 if omitted
	DeviceID   string // stable per installation; announced in the handshake
	DeviceName string // shown in the appliance's registered clients
	Log        Logger

	ConnectTimeout time.Duration // TCP connect (10 s)
	UpgradeTimeout time.Duration // TCP connect + HTTP upgrade (15 s)
	InitTimeout    time.Duration // wait for the appliance's first message (20 s)
	RequestTimeout time.Duration // per request (20 s)
	PingEvery      time.Duration // WebSocket ping interval (60 s)
	PongTimeout    time.Duration // max silence after a ping (30 s)
	BackoffMin     time.Duration // first retry delay (5 s)
	BackoffMax     time.Duration // retry delay cap (5 min)
	StableAfter    time.Duration // a session this long resets the backoff (60 s)
	MinDialGap     time.Duration // never dial more often than this (5 s)
	KeyErrorAfter  int           // first-frame MAC failures in a row before key_error (3)
	ResyncEvery    time.Duration // re-read all values on the live session (6 h; <0 disables)

	// NotBefore delays the first dial (carried over from a previous run so
	// a restart loop cannot hammer the appliance).
	NotBefore time.Time
	// OnDial is called right before every dial attempt.
	OnDial func(time.Time)
	// Resume continues a failure episode from a previous run (refused,
	// unreachable, ...): the client starts in that state instead of
	// reporting "connecting", so retries in the same state stay silent and
	// Since keeps marking the start of the episode.
	Resume LinkStatus
	// Raw emits EvRaw events with every decrypted frame (local-watch).
	Raw bool

	netDial func(ctx context.Context, network, addr string) (net.Conn, error) // tests
	jitter  func() float64                                                    // tests; [0,1)
}

func (o *Options) setDefaults() {
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	def(&o.ConnectTimeout, 10*time.Second)
	def(&o.UpgradeTimeout, 15*time.Second)
	def(&o.InitTimeout, 20*time.Second)
	def(&o.RequestTimeout, 20*time.Second)
	def(&o.PingEvery, 60*time.Second)
	def(&o.PongTimeout, 30*time.Second)
	def(&o.BackoffMin, 5*time.Second)
	def(&o.BackoffMax, 5*time.Minute)
	def(&o.StableAfter, 60*time.Second)
	def(&o.MinDialGap, 5*time.Second)
	def(&o.ResyncEvery, 6*time.Hour)
	if o.KeyErrorAfter == 0 {
		o.KeyErrorAfter = 3
	}
	if o.DeviceName == "" {
		o.DeviceName = "hcbridge"
	}
	if o.jitter == nil {
		o.jitter = rand.Float64
	}
}

// LinkState is the health of the local connection.
type LinkState string

const (
	LinkConnecting  LinkState = "connecting"
	LinkConnected   LinkState = "connected"
	LinkRefused     LinkState = "refused"     // HTTP 503 on the upgrade: the firmware refuses local sessions
	LinkUnreachable LinkState = "unreachable" // no TCP connection: hob off the network or path blocked
	LinkKeyError    LinkState = "key_error"   // frames fail authentication: profile key wrong or stale
	LinkError       LinkState = "error"       // anything else
)

// LinkStates lists every state, for fixed-option HA enum sensors.
var LinkStates = []LinkState{LinkConnecting, LinkConnected, LinkRefused, LinkUnreachable, LinkKeyError, LinkError}

// LinkStatus is reported on every state change.
type LinkStatus struct {
	State    LinkState
	Since    time.Time // when this state began (unchanged across retries)
	Err      error     // last error, nil when connected
	Attempts int       // failed attempts in this state
}

// EventKind tells the Event fields apart.
type EventKind int

const (
	EvLink        EventKind = iota + 1 // Link
	EvInfo                             // Info, after every handshake
	EvValues                           // Values; Snapshot for /ro/allMandatoryValues
	EvDescription                      // Changes (access/availability)
	EvRaw                              // Raw (only with Options.Raw)
)

// Event is delivered to the handler, one at a time and in wire order.
type Event struct {
	Kind     EventKind
	Link     LinkStatus
	Info     *DeviceInfo
	Values   []Value
	Snapshot bool
	Changes  []Description
	Raw      *RawFrame
}

// Description is one access/availability change.
type Description struct {
	UID       int
	Name      string
	Access    string
	Available *bool
	Value     *Value
}

// RawFrame is a decrypted frame, for local-watch --raw.
type RawFrame struct {
	Dir   string // "rx" or "tx"
	Plain []byte
}

// Client keeps one local session to one appliance alive.
type Client struct {
	opts     Options
	prof     *Profile
	hostport string
	log      Logger
	handler  func(Event)

	events     chan Event
	link       LinkStatus
	lastLogged time.Time // last "still <state>" summary
	mu         sync.Mutex
}

// NewClient validates the options. Run does the work.
func NewClient(o Options, p *Profile, h func(Event)) (*Client, error) {
	o.setDefaults()
	if p == nil {
		return nil, errors.New("homeconnect: no profile")
	}
	if o.Log == nil {
		return nil, errors.New("homeconnect: no logger")
	}
	if o.DeviceID == "" {
		return nil, errors.New("homeconnect: no device ID")
	}
	host := strings.TrimSpace(o.Host)
	if host == "" {
		return nil, errors.New("homeconnect: no host")
	}
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(strings.Trim(host, "[]"), "80")
	}
	return &Client{opts: o, prof: p, hostport: host, log: o.Log, handler: h}, nil
}

func (c *Client) emit(ev Event) {
	select {
	case c.events <- ev:
	default:
		c.log.Error("event queue full; the handler is not keeping up")
		c.events <- ev
	}
}

// Run connects and stays connected until ctx is cancelled, then closes the
// session cleanly and returns once every event has been handled.
func (c *Client) Run(ctx context.Context) {
	c.events = make(chan Event, 1024)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for ev := range c.events {
			c.handler(ev)
		}
	}()
	defer func() {
		close(c.events)
		<-drained
	}()

	o := &c.opts
	if o.Resume.State != "" && o.Resume.State != LinkConnected && o.Resume.State != LinkConnecting {
		c.mu.Lock()
		c.link = o.Resume
		c.lastLogged = time.Now()
		c.mu.Unlock()
	} else {
		c.setLink(LinkConnecting, nil)
	}
	backoff := o.BackoffMin
	firstFrameFails := 0
	var lastDial time.Time
	for {
		next := lastDial.Add(o.MinDialGap)
		if o.NotBefore.After(next) {
			next = o.NotBefore
		}
		if !sleepCtx(ctx, time.Until(next)) {
			return
		}
		lastDial = time.Now()
		if o.OnDial != nil {
			o.OnDial(lastDial)
		}
		connectedAt, err := c.runSession(ctx)
		if ctx.Err() != nil {
			return
		}
		if !connectedAt.IsZero() {
			up := time.Since(connectedAt)
			firstFrameFails = 0
			if up >= o.StableAfter {
				backoff = o.BackoffMin
			}
			c.log.Warn("local session ended after %s: %v", up.Round(time.Second), err)
			c.setLink(LinkConnecting, err)
		} else {
			if errors.Is(err, ErrFirstFrameMAC) {
				firstFrameFails++
			} else {
				firstFrameFails = 0
			}
			c.setLink(classify(err, firstFrameFails, o.KeyErrorAfter), err)
		}
		if !sleepCtx(ctx, applyJitter(backoff, o.jitter())) {
			return
		}
		backoff = nextBackoff(backoff, o.BackoffMax)
	}
}

// runSession dials, handshakes and serves one connection. connectedAt is
// zero if the handshake never completed.
func (c *Client) runSession(ctx context.Context) (connectedAt time.Time, err error) {
	conn, err := c.dial(ctx)
	if err != nil {
		return time.Time{}, err
	}
	s, err := newSession(c, conn)
	if err != nil {
		conn.Close()
		return time.Time{}, err
	}
	go s.readLoop()

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var init *Message
	select {
	case init = <-s.initCh:
	case <-s.fail:
		s.end()
		return time.Time{}, s.closedErr()
	case <-time.After(c.opts.InitTimeout):
		s.closeGracefully(1000, "")
		return time.Time{}, &TimeoutError{Resource: "/ei/initialValues"}
	case <-ctx.Done():
		s.closeGracefully(1000, "")
		return time.Time{}, ctx.Err()
	}
	info, err := s.handshake(sctx, init)
	if err != nil {
		s.end()
		if errors.Is(err, ErrFirstFrameMAC) || errors.Is(err, ErrDesync) {
			return time.Time{}, err
		}
		return time.Time{}, fmt.Errorf("handshake: %w", err)
	}
	connectedAt = time.Now()
	c.emit(Event{Kind: EvInfo, Info: info})
	c.setLink(LinkConnected, nil)

	pingErr := make(chan error, 1)
	go s.pingLoop(sctx, pingErr)
	var resync <-chan time.Time
	if c.opts.ResyncEvery > 0 {
		t := time.NewTicker(c.opts.ResyncEvery)
		defer t.Stop()
		resync = t.C
	}
	for {
		select {
		case <-ctx.Done():
			s.closeGracefully(1000, "")
			return connectedAt, ctx.Err()
		case <-s.fail:
			s.end()
			return connectedAt, s.closedErr()
		case err := <-pingErr:
			s.closeGracefully(1000, "")
			return connectedAt, err
		case m := <-s.initCh:
			// The appliance restarted the handshake on the same socket.
			c.log.Warn("appliance re-sent /ei/initialValues; redoing the handshake")
			info, err := s.handshake(sctx, m)
			if err != nil {
				s.closeGracefully(1000, "")
				return connectedAt, fmt.Errorf("re-handshake: %w", err)
			}
			c.emit(Event{Kind: EvInfo, Info: info})
		case <-resync:
			if _, err := s.get(sctx, "/ro/allMandatoryValues", s.version("/ro"), nil); err != nil {
				var re *RequestError
				var te *TimeoutError
				if !errors.As(err, &re) && !errors.As(err, &te) {
					s.closeGracefully(1000, "")
					return connectedAt, fmt.Errorf("resync: %w", err)
				}
				c.log.Warn("periodic resync: %v", err)
			}
		}
	}
}

// setLink records and reports a link state. Retries in the same state do
// not re-emit, so Since marks the start of the episode.
func (c *Client) setLink(state LinkState, err error) {
	c.mu.Lock()
	prev := c.link
	now := time.Now()
	changed := prev.State != state
	if changed {
		c.link = LinkStatus{State: state, Since: now, Err: err}
		c.lastLogged = now
	} else {
		c.link.Err = err
	}
	if state != LinkConnected && state != LinkConnecting {
		c.link.Attempts++
	}
	st := c.link
	summary := !changed && now.Sub(c.lastLogged) >= time.Hour
	if summary {
		c.lastLogged = now
	}
	c.mu.Unlock()

	switch {
	case changed && state == LinkConnected:
		if prev.State != "" && prev.State != LinkConnecting {
			c.log.Info("local link connected after %s for %s (%d attempts)", prev.State, now.Sub(prev.Since).Round(time.Second), prev.Attempts)
		} else {
			c.log.Info("local link connected")
		}
	case changed && err != nil && state != LinkConnecting:
		c.log.Warn("local link %s: %s", state, describe(err))
	case summary:
		c.log.Info("local link still %s: %d attempts since %s", state, st.Attempts, st.Since.Format(time.RFC3339))
	case err != nil:
		c.log.Debug("local link %s: %v", state, err)
	}
	if changed {
		c.emit(Event{Kind: EvLink, Link: st})
	}
}

// describe adds the HTTP details of a refused upgrade to the error text.
func describe(err error) string {
	var ue *UpgradeError
	if !errors.As(err, &ue) {
		return err.Error()
	}
	var b strings.Builder
	b.WriteString(ue.Error())
	for _, h := range []string{"Server", "Content-Type", "Retry-After"} {
		if v := ue.Header.Get(h); v != "" {
			fmt.Fprintf(&b, ", %s: %s", h, v)
		}
	}
	if len(ue.Body) > 0 {
		b.WriteString(", body: ")
		b.WriteString(strconv.Quote(string(ue.Body)))
	}
	return b.String()
}

// classify maps a failed connection attempt to a link state.
func classify(err error, firstFrameFails, keyErrorAfter int) LinkState {
	var ue *UpgradeError
	var de *DialError
	switch {
	case errors.As(err, &ue) && ue.Status == 503:
		return LinkRefused
	case errors.As(err, &de):
		return LinkUnreachable
	case errors.Is(err, ErrFirstFrameMAC) && firstFrameFails >= keyErrorAfter:
		return LinkKeyError
	}
	return LinkError
}

func nextBackoff(cur, max time.Duration) time.Duration {
	if cur *= 2; cur > max {
		return max
	}
	return cur
}

// applyJitter spreads d over [0.8d, 1.2d).
func applyJitter(d time.Duration, r float64) time.Duration {
	return time.Duration(float64(d) * (0.8 + 0.4*r))
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
