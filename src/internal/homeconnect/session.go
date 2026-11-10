package homeconnect

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// DeviceInfo is what the appliance reports about itself during the
// handshake (/ci/info or /iz/info, and /ni/info).
type DeviceInfo struct {
	DeviceID, ENumber, Brand, VIB, MAC  string
	HaVersion, SwVersion, HwVersion     string // haVersion: appliance; sw/hwVersion: network module
	DeviceType, CustomerIndex, FdString string
	SerialNumber                        string
	Services                            map[string]int
	RSSI                                *int // from /ni/info
}

// session is one WebSocket connection: handshake, request/response
// matching, NOTIFY dispatch and liveness. A new session (with fresh crypto
// state and message IDs) is built for every connection.
type session struct {
	c     *Client
	conn  *websocket.Conn
	codec *Codec

	wmu sync.Mutex // Seal + WriteMessage as one step: tx chain order == wire order

	mu       sync.Mutex
	pending  map[int64]chan *Message
	nextID   int64
	sID      int64
	services map[string]int

	lastRx atomic.Int64 // UnixNano of the last frame of any kind
	rxOK   atomic.Int64 // frames that passed authentication

	initCh   chan *Message // POST /ei/initialValues from the appliance
	fail     chan struct{} // closed when the session became unusable; readErr says why
	failOnce sync.Once
	done     chan struct{} // closed when the read loop ends (the socket is gone)
	readErr  error         // valid after fail is closed
}

func newSession(c *Client, conn *websocket.Conn) (*session, error) {
	psk, iv := c.prof.Keys()
	codec, err := NewCodec(psk, iv, RoleClient)
	if err != nil {
		return nil, err
	}
	s := &session{
		c: c, conn: conn, codec: codec,
		pending:  map[int64]chan *Message{},
		services: map[string]int{},
		initCh:   make(chan *Message, 1),
		fail:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	s.lastRx.Store(time.Now().UnixNano())
	touch := func(string) error { s.lastRx.Store(time.Now().UnixNano()); return nil }
	conn.SetPongHandler(touch)
	conn.SetPingHandler(func(data string) error {
		touch(data)
		err := conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(10*time.Second))
		if errors.Is(err, websocket.ErrCloseSent) {
			return nil
		}
		return err
	})
	return s, nil
}

// dial opens the TCP connection and performs the WebSocket upgrade. The
// request carries no Origin header (the appliance's server ignores upgrades
// that have one) and no extensions.
func (c *Client) dial(ctx context.Context) (*websocket.Conn, error) {
	netDial := c.opts.netDial
	if netDial == nil {
		nd := &net.Dialer{
			Timeout: c.opts.ConnectTimeout,
			KeepAliveConfig: net.KeepAliveConfig{
				Enable: true, Idle: 45 * time.Second, Interval: 15 * time.Second, Count: 4,
			},
		}
		netDial = nd.DialContext
	}
	d := websocket.Dialer{
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := netDial(ctx, network, addr)
			if err != nil {
				return nil, &DialError{Err: err}
			}
			return conn, nil
		},
		HandshakeTimeout: c.opts.UpgradeTimeout,
	}
	conn, resp, err := d.DialContext(ctx, "ws://"+c.hostport+"/homeconnect", nil)
	if err != nil {
		var de *DialError
		if errors.As(err, &de) {
			return nil, de
		}
		if errors.Is(err, websocket.ErrBadHandshake) && resp != nil && resp.StatusCode != 101 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			return nil, &UpgradeError{Status: resp.StatusCode, Header: resp.Header, Body: body}
		}
		return nil, fmt.Errorf("websocket upgrade: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	return conn, nil
}

// setFailed records why the session is unusable, once.
func (s *session) setFailed(err error) {
	s.failOnce.Do(func() {
		s.readErr = err
		close(s.fail)
	})
}

// readLoop runs for the life of the connection. Everything it hands to the
// client's event queue is in wire order, which is what keeps the
// /ro/allMandatoryValues snapshot ahead of any later NOTIFY.
//
// After a framing error the loop keeps reading (and discarding) until the
// socket closes, so the session can still exchange Close frames with the
// appliance instead of dropping the connection on it.
func (s *session) readLoop() {
	defer close(s.done)
	broken := false
	for {
		mt, data, err := s.conn.ReadMessage()
		if err != nil {
			s.setFailed(err)
			return
		}
		s.lastRx.Store(time.Now().UnixNano())
		if broken {
			continue
		}
		if mt != websocket.BinaryMessage {
			s.c.log.Debug("ignoring non-binary frame (%d bytes)", len(data))
			continue
		}
		plain, err := s.codec.Open(data)
		if err != nil {
			if s.rxOK.Load() == 0 {
				s.setFailed(ErrFirstFrameMAC)
			} else {
				s.setFailed(fmt.Errorf("%w: %v", ErrDesync, err))
			}
			broken = true
			continue
		}
		s.rxOK.Add(1)
		if s.c.opts.Raw {
			s.c.emit(Event{Kind: EvRaw, Raw: &RawFrame{Dir: "rx", Plain: plain}})
		}
		msg, err := parseMessage(plain)
		if err != nil {
			// Appliances occasionally send malformed JSON. The crypto chain
			// is intact, so skip the message and keep the session.
			s.c.log.Warn("skipping undecodable message: %v: %s", err, quoted(plain))
			continue
		}
		s.dispatch(msg)
	}
}

func (s *session) dispatch(m *Message) {
	switch m.Action {
	case ActionResponse:
		if m.Code == nil {
			switch m.Resource {
			case "/ro/allMandatoryValues", "/ro/values":
				s.emitValues(m, true)
			case "/ro/allDescriptionChanges":
				s.emitDescriptions(m)
			}
		}
		s.mu.Lock()
		ch := s.pending[m.MsgID]
		delete(s.pending, m.MsgID)
		s.mu.Unlock()
		if ch != nil {
			ch <- m
		} else {
			s.c.log.Debug("unmatched response %s msgID %d", m.Resource, m.MsgID)
		}
	case ActionNotify:
		switch m.Resource {
		case "/ro/values":
			s.emitValues(m, false)
		case "/ro/descriptionChange":
			s.emitDescriptions(m)
		default:
			s.c.log.Debug("notify %s (%d items)", m.Resource, len(m.Data))
		}
	case ActionPost, ActionGet:
		if m.Resource == "/ei/initialValues" {
			select {
			case s.initCh <- m:
			default:
				s.c.log.Warn("dropping extra /ei/initialValues")
			}
			return
		}
		// Read-only: requests from the appliance are never answered.
		s.c.log.Info("appliance sent %s %s; not answering (read-only)", m.Action, m.Resource)
	default:
		s.c.log.Debug("unknown action %q on %s", m.Action, m.Resource)
	}
}

func (s *session) emitValues(m *Message, snapshot bool) {
	items, bad := decodeList[uidValue](m.Data)
	if bad > 0 {
		s.c.log.Warn("%s: skipped %d undecodable values", m.Resource, bad)
	}
	vals := make([]Value, 0, len(items))
	for _, it := range items {
		vals = append(vals, s.c.prof.Decode(int(it.UID), it.Value))
	}
	s.c.emit(Event{Kind: EvValues, Values: vals, Snapshot: snapshot})
}

func (s *session) emitDescriptions(m *Message) {
	items, _ := decodeList[wireDescription](m.Data)
	out := make([]Description, 0, len(items))
	for _, it := range items {
		d := Description{UID: int(it.UID), Access: lower(it.Access), Available: it.Available}
		if f, ok := s.c.prof.Feature(d.UID); ok {
			d.Name = f.Name
		}
		if len(it.Value) > 0 && !isJSONNull(it.Value) {
			v := s.c.prof.Decode(d.UID, it.Value)
			d.Value = &v
		}
		out = append(out, d)
	}
	s.c.emit(Event{Kind: EvDescription, Changes: out})
}

func (s *session) write(m outMessage) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	frame, err := s.codec.Seal(b)
	if err != nil {
		return err
	}
	if s.c.opts.Raw {
		s.c.emit(Event{Kind: EvRaw, Raw: &RawFrame{Dir: "tx", Plain: b}})
	}
	s.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return s.conn.WriteMessage(websocket.BinaryMessage, frame)
}

func (s *session) version(resource string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.services[serviceOf(resource)]; ok && v > 0 {
		return v
	}
	return 1
}

// get is the only request the bridge ever makes of the appliance.
func (s *session) get(ctx context.Context, resource string, version int, data any) (*Message, error) {
	ch := make(chan *Message, 1)
	s.mu.Lock()
	id := s.nextID
	s.nextID++
	s.pending[id] = ch
	sid := s.sID
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
	}()
	if err := s.write(outMessage{SID: sid, MsgID: id, Resource: resource, Version: version, Action: ActionGet, Data: data}); err != nil {
		return nil, err
	}
	t := time.NewTimer(s.c.opts.RequestTimeout)
	defer t.Stop()
	select {
	case m := <-ch:
		if m.Code != nil {
			return m, &RequestError{Resource: resource, Code: *m.Code}
		}
		return m, nil
	case <-t.C:
		return nil, &TimeoutError{Resource: resource}
	case <-s.fail:
		return nil, s.closedErr()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// closedErr is why the session failed; call only after fail is closed.
func (s *session) closedErr() error {
	if s.readErr != nil {
		return s.readErr
	}
	return ErrSessionClosed
}

// end closes the connection: with a Close handshake if the socket is still
// there, else just the file descriptor.
func (s *session) end() {
	select {
	case <-s.done:
		s.conn.Close()
	default:
		s.closeGracefully(1000, "")
	}
}

// notify sends a NOTIFY (only /ei/deviceReady is ever sent).
func (s *session) notify(resource string, version int) error {
	s.mu.Lock()
	id := s.nextID
	s.nextID++
	sid := s.sID
	s.mu.Unlock()
	return s.write(outMessage{SID: sid, MsgID: id, Resource: resource, Version: version, Action: ActionNotify})
}

// handshake answers the appliance's /ei/initialValues and walks the
// session setup: services, authentication, device info, readiness and the
// description changes. The full value snapshot follows (snapshot).
func (s *session) handshake(ctx context.Context, init *Message) (*DeviceInfo, error) {
	var ed struct {
		EdMsgID flexInt `json:"edMsgID"`
	}
	if len(init.Data) > 0 {
		_ = json.Unmarshal(init.Data[0], &ed)
	}
	s.mu.Lock()
	s.sID = init.SID
	s.nextID = int64(ed.EdMsgID)
	if s.nextID == 0 {
		s.nextID = init.MsgID + 1
	}
	s.services = map[string]int{}
	s.mu.Unlock()

	var deviceType any = "Application"
	if init.Version == 1 {
		deviceType = 2
	}
	reply := outMessage{
		SID: init.SID, MsgID: init.MsgID, Resource: init.Resource, Version: init.Version,
		Action: ActionResponse,
		Data:   []initValuesReply{{DeviceType: deviceType, DeviceName: s.c.opts.DeviceName, DeviceID: s.c.opts.DeviceID}},
	}
	if err := s.write(reply); err != nil {
		return nil, err
	}

	m, err := s.get(ctx, "/ci/services", 1, nil)
	if err != nil {
		return nil, err
	}
	svcs, _ := decodeList[struct {
		Service string  `json:"service"`
		Version flexInt `json:"version"`
	}](m.Data)
	s.mu.Lock()
	for _, sv := range svcs {
		s.services[sv.Service] = int(sv.Version)
	}
	services := make(map[string]int, len(s.services))
	for k, v := range s.services {
		services[k] = v
	}
	s.mu.Unlock()

	info := &DeviceInfo{Services: services}
	tolerate := func(err error) error {
		var re *RequestError
		if errors.As(err, &re) {
			s.c.log.Debug("%v (ignored)", err)
			return nil
		}
		return err
	}

	if v, ok := services["ci"]; ok && v < 3 {
		nonce := make([]byte, 32)
		if _, err := rand.Read(nonce); err != nil {
			return nil, err
		}
		if _, err := s.get(ctx, "/ci/authentication", v, []map[string]string{{"nonce": base64.RawURLEncoding.EncodeToString(nonce)}}); tolerate(err) != nil {
			return nil, err
		}
		m, err := s.get(ctx, "/ci/info", v, nil)
		if tolerate(err) != nil {
			return nil, err
		}
		if err == nil {
			fillInfo(info, m)
		}
	}
	if v, ok := services["iz"]; ok {
		m, err := s.get(ctx, "/iz/info", v, nil)
		if tolerate(err) != nil {
			return nil, err
		}
		if err == nil && info.SwVersion == "" {
			fillInfo(info, m)
		}
	}
	if services["ei"] == 2 {
		if err := s.notify("/ei/deviceReady", 2); err != nil {
			return nil, err
		}
	}
	if v, ok := services["ni"]; ok {
		m, err := s.get(ctx, "/ni/info", v, nil)
		if tolerate(err) != nil {
			return nil, err
		}
		if err == nil {
			fillNetwork(info, m)
		}
	}
	if _, err := s.get(ctx, "/ro/allDescriptionChanges", s.version("/ro"), nil); tolerate(err) != nil {
		return nil, err
	}
	return info, nil
}

// snapshot reads every value (/ro/allMandatoryValues; dispatch emits them
// ahead of anything pushed later). Some firmware answers 500 or is slow
// here: one retry on timeout, and an error answer leaves the session up
// (pushes still arrive) but comes back as failed, so the caller reads again
// soon. Any other failure (err) ends the session.
func (s *session) snapshot(ctx context.Context) (failed, err error) {
	for attempt := 0; ; attempt++ {
		_, err := s.get(ctx, "/ro/allMandatoryValues", s.version("/ro"), nil)
		var te *TimeoutError
		if errors.As(err, &te) && attempt == 0 {
			s.c.log.Warn("/ro/allMandatoryValues timed out; retrying once")
			continue
		}
		var re *RequestError
		if errors.As(err, &re) {
			return err, nil
		}
		return nil, err
	}
}

func str(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
	return fmt.Sprint(v)
}

func fillInfo(info *DeviceInfo, m *Message) {
	if len(m.Data) == 0 {
		return
	}
	var d map[string]any
	if json.Unmarshal(m.Data[0], &d) != nil {
		return
	}
	set := func(dst *string, key string) {
		if v := str(d[key]); v != "" {
			*dst = v
		}
	}
	set(&info.DeviceID, "deviceID")
	set(&info.ENumber, "eNumber")
	set(&info.Brand, "brand")
	set(&info.VIB, "vib")
	set(&info.MAC, "mac")
	set(&info.HaVersion, "haVersion")
	set(&info.SwVersion, "swVersion")
	set(&info.HwVersion, "hwVersion")
	set(&info.DeviceType, "deviceType")
	set(&info.CustomerIndex, "customerIndex")
	set(&info.FdString, "fdString")
	set(&info.SerialNumber, "serialNumber")
}

func fillNetwork(info *DeviceInfo, m *Message) {
	for _, raw := range m.Data {
		var d struct {
			RSSI    *int  `json:"rssi"`
			Primary *bool `json:"primary"`
		}
		if json.Unmarshal(raw, &d) != nil || d.RSSI == nil {
			continue
		}
		if info.RSSI == nil || (d.Primary != nil && *d.Primary) {
			info.RSSI = d.RSSI
		}
	}
}

// pingLoop sends a WebSocket ping every PingEvery and fails if no frame of
// any kind arrived within PongTimeout of it. Idle Bosch hobs are slow to
// pong, so the timeout is generous on purpose: an aborted session is far
// worse for the appliance than a late one.
func (s *session) pingLoop(ctx context.Context, errc chan<- error) {
	t := time.NewTicker(s.c.opts.PingEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		sent := time.Now()
		if err := s.conn.WriteControl(websocket.PingMessage, nil, sent.Add(10*time.Second)); err != nil {
			errc <- fmt.Errorf("ping: %w", err)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.c.opts.PongTimeout):
		}
		if s.lastRx.Load() < sent.UnixNano() {
			errc <- ErrPongTimeout
			return
		}
	}
}

// closeGracefully sends a Close frame, gives the appliance a moment to
// answer it (its server closes the TCP connection first), then closes the
// socket. Never just drop a connection the appliance still believes in.
func (s *session) closeGracefully(code int, reason string) {
	_ = s.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(closeWait))
	select {
	case <-s.done:
	case <-time.After(closeWait):
	}
	s.conn.Close()
	<-s.done
}

func quoted(b []byte) string {
	if len(b) > 300 {
		return strconv.Quote(string(b[:300])) + "..."
	}
	return strconv.Quote(string(b))
}

func lower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}
