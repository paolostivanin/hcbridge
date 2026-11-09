// Package hcfake is a fake Home Connect appliance for tests: an HTTP server
// that speaks the local WebSocket protocol (AES framing, handshake, value
// pushes) with knobs for the failure modes that matter (HTTP 503 on the
// upgrade, no pongs, a corrupted frame, a wrong key, dropped connections).
// It records what the client sent so tests can assert on it.
//
// Only tests import it; it never links into the bridge binary.
package hcfake

import (
	"bytes"
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	hc "hcbridge/internal/homeconnect"
)

//go:embed profile
var profileFS embed.FS

const (
	HaID      = "TEST-HOB-000000000001"
	HaVersion = "1.332"
	SwVersion = "2.17.1.13549"
	HwVersion = "1.0.3.3"
)

// Profile returns the synthetic test profile the fake speaks with.
func Profile() *hc.Profile {
	read := func(name string) []byte {
		b, err := profileFS.ReadFile("profile/" + name)
		if err != nil {
			panic(err)
		}
		return b
	}
	p, err := hc.ParseProfile(read(HaID+".json"),
		bytes.NewReader(read(HaID+"_DeviceDescription.xml")),
		bytes.NewReader(read(HaID+"_FeatureMapping.xml")))
	if err != nil {
		panic(err)
	}
	return p
}

// WriteProfileDir copies the synthetic profile files into dir.
func WriteProfileDir(dir string) string {
	err := fs.WalkDir(profileFS, "profile", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := profileFS.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dir, filepath.Base(p)), b, 0o600)
	})
	if err != nil {
		panic(err)
	}
	return dir
}

// Msg is one message the client sent, as decoded by the fake.
type Msg struct {
	SID      int64           `json:"sID"`
	MsgID    int64           `json:"msgID"`
	Resource string          `json:"resource"`
	Version  int             `json:"version"`
	Action   string          `json:"action"`
	Data     json.RawMessage `json:"data"`
	Raw      []byte          `json:"-"` // exact plaintext as sent
}

// Server is the fake appliance.
type Server struct {
	ts *httptest.Server

	mu            sync.Mutex
	psk, iv       []byte
	refuse        int
	refuseHeader  http.Header
	refuseBody    string
	pong          bool
	corruptNext   bool
	ci, ei        int
	iz, ni        bool
	initVersion   int
	values        map[int]any
	failMandatory int
	silentOnce    bool
	conns         map[*conn]bool
	nextSID       int64

	upgradeHeaders []http.Header
	received       []Msg
	closeCodes     []int
	dialTimes      []time.Time
	concurrent     int
	maxConcurrent  int
	pings          int
	sessions       int
}

type conn struct {
	ws    *websocket.Conn
	codec *hc.Codec
	sid   int64
	wmu   sync.Mutex
}

// New starts a fake appliance using the synthetic profile's key.
func New() *Server {
	psk, iv := Profile().Keys()
	s := &Server{
		psk: psk, iv: iv, pong: true, ci: 2, ei: 2, ni: true, initVersion: 2,
		values:  map[int]any{},
		conns:   map[*conn]bool{},
		nextSID: 1000,
	}
	s.ts = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

// Close stops the server and drops every connection.
func (s *Server) Close() {
	s.Drop()
	s.ts.Close()
}

// Host is the address to give the client ("127.0.0.1:port").
func (s *Server) Host() string { return s.ts.Listener.Addr().String() }

// Refuse makes every upgrade answer status (0: accept again).
func (s *Server) Refuse(status int, header http.Header, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refuse, s.refuseHeader, s.refuseBody = status, header, body
}

// SetPong turns answering pings on or off.
func (s *Server) SetPong(on bool) { s.mu.Lock(); s.pong = on; s.mu.Unlock() }

// CorruptNextMAC breaks the MAC of the next frame the fake sends.
func (s *Server) CorruptNextMAC() { s.mu.Lock(); s.corruptNext = true; s.mu.Unlock() }

// SetKey changes the key used for new connections (simulates a re-paired
// appliance whose profile is now stale).
func (s *Server) SetKey(psk []byte) { s.mu.Lock(); s.psk = psk; s.mu.Unlock() }

// SetServices sets the service versions /ci/services announces.
func (s *Server) SetServices(ci, ei int, iz, ni bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ci, s.ei, s.iz, s.ni = ci, ei, iz, ni
}

// SetInitVersion sets the version of the /ei/initialValues message.
func (s *Server) SetInitVersion(v int) { s.mu.Lock(); s.initVersion = v; s.mu.Unlock() }

// SetValues sets values the snapshot reports (merged into the current set).
func (s *Server) SetValues(v map[int]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for uid, x := range v {
		s.values[uid] = x
	}
}

// FailMandatory makes /ro/allMandatoryValues answer code (0: normally).
// silentOnce leaves the next one unanswered (a timeout).
func (s *Server) FailMandatory(code int, silentOnce bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failMandatory, s.silentOnce = code, silentOnce
}

// Notify pushes NOTIFY /ro/values to every connected client and records
// the values for later snapshots.
func (s *Server) Notify(v map[int]any) {
	s.SetValues(v)
	data := make([]map[string]any, 0, len(v))
	for uid, x := range v {
		data = append(data, map[string]any{"uid": uid, "value": x})
	}
	for _, c := range s.live() {
		s.send(c, map[string]any{"sID": c.sid, "msgID": 1, "resource": "/ro/values", "version": 1, "action": "NOTIFY", "data": data})
	}
}

// SendRaw seals and sends arbitrary plaintext to every client.
func (s *Server) SendRaw(plain []byte) {
	for _, c := range s.live() {
		s.sendBytes(c, plain)
	}
}

// Drop closes every connection at the TCP level, without a Close frame.
func (s *Server) Drop() {
	for _, c := range s.live() {
		c.ws.NetConn().Close()
	}
}

// Observations.

func (s *Server) UpgradeHeaders() []http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]http.Header(nil), s.upgradeHeaders...)
}

func (s *Server) Received() []Msg {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Msg(nil), s.received...)
}

func (s *Server) CloseCodes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.closeCodes...)
}

func (s *Server) DialTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.dialTimes...)
}

func (s *Server) MaxConcurrent() int { s.mu.Lock(); defer s.mu.Unlock(); return s.maxConcurrent }
func (s *Server) Concurrent() int    { s.mu.Lock(); defer s.mu.Unlock(); return s.concurrent }
func (s *Server) Pings() int         { s.mu.Lock(); defer s.mu.Unlock(); return s.pings }
func (s *Server) Sessions() int      { s.mu.Lock(); defer s.mu.Unlock(); return s.sessions }

func (s *Server) live() []*conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		out = append(out, c)
	}
	return out
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.upgradeHeaders = append(s.upgradeHeaders, r.Header.Clone())
	s.dialTimes = append(s.dialTimes, time.Now())
	refuse, hdr, body := s.refuse, s.refuseHeader, s.refuseBody
	psk, iv := s.psk, s.iv
	s.mu.Unlock()

	if r.URL.Path != "/homeconnect" {
		http.NotFound(w, r)
		return
	}
	if refuse != 0 {
		for k, vs := range hdr {
			w.Header()[k] = vs
		}
		w.WriteHeader(refuse)
		w.Write([]byte(body))
		return
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	codec, err := hc.NewCodec(psk, iv, hc.RoleDevice)
	if err != nil {
		ws.Close()
		return
	}
	s.mu.Lock()
	s.nextSID++
	c := &conn{ws: ws, codec: codec, sid: s.nextSID}
	s.conns[c] = true
	s.concurrent++
	s.sessions++
	if s.concurrent > s.maxConcurrent {
		s.maxConcurrent = s.concurrent
	}
	initVersion := s.initVersion
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.concurrent--
		s.mu.Unlock()
		ws.Close()
	}()

	ws.SetPingHandler(func(data string) error {
		s.mu.Lock()
		s.pings++
		pong := s.pong
		s.mu.Unlock()
		if !pong {
			return nil
		}
		return ws.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
	})
	ws.SetCloseHandler(func(code int, _ string) error {
		s.mu.Lock()
		s.closeCodes = append(s.closeCodes, code)
		s.mu.Unlock()
		return ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, ""), time.Now().Add(time.Second))
	})

	// The appliance speaks first.
	s.send(c, map[string]any{
		"sID": c.sid, "msgID": 500, "resource": "/ei/initialValues", "version": initVersion,
		"action": "POST", "data": []map[string]any{{"edMsgID": 7000}},
	})
	for {
		mt, frame, err := ws.ReadMessage()
		if err != nil {
			return
		}
		if mt != websocket.BinaryMessage {
			continue
		}
		plain, err := c.codec.Open(frame)
		if err != nil {
			return // a real appliance would drop the client too
		}
		var m Msg
		if json.Unmarshal(plain, &m) != nil {
			continue
		}
		m.Raw = plain
		s.mu.Lock()
		s.received = append(s.received, m)
		s.mu.Unlock()
		s.answer(c, m)
	}
}

func (s *Server) answer(c *conn, m Msg) {
	if m.Action != "GET" && m.Action != "POST" {
		return // RESPONSE to initialValues, NOTIFY deviceReady
	}
	reply := map[string]any{"sID": c.sid, "msgID": m.MsgID, "resource": m.Resource, "version": m.Version, "action": "RESPONSE"}
	s.mu.Lock()
	switch m.Resource {
	case "/ci/services":
		svcs := []map[string]any{{"service": "ci", "version": s.ci}, {"service": "ei", "version": s.ei}, {"service": "ro", "version": 1}}
		if s.iz {
			svcs = append(svcs, map[string]any{"service": "iz", "version": 1})
		}
		if s.ni {
			svcs = append(svcs, map[string]any{"service": "ni", "version": 1})
		}
		reply["data"] = svcs
	case "/ci/authentication":
		reply["data"] = []map[string]any{{"response": "ok"}}
	case "/ci/info", "/iz/info":
		reply["data"] = []map[string]any{{
			"deviceID": "fake", "eNumber": "TESTHOB01/01", "brand": "BOSCH", "vib": "TESTHOB01",
			"mac": "00-00-5E-00-53-01", "haVersion": HaVersion, "swVersion": SwVersion, "hwVersion": HwVersion,
			"deviceType": "Hob", "customerIndex": "01", "serialNumber": "000000000000000001", "fdString": "0001",
		}}
	case "/ni/info":
		reply["data"] = []map[string]any{{"interfaceID": 0, "type": "WiFi", "rssi": -61, "primary": true, "status": "CONNECTED"}}
	case "/ro/allDescriptionChanges":
		reply["data"] = []map[string]any{{"uid": 4409, "access": "READ", "available": true}}
	case "/ro/allMandatoryValues":
		if s.silentOnce {
			s.silentOnce = false
			s.mu.Unlock()
			return
		}
		if s.failMandatory != 0 {
			reply["code"] = s.failMandatory
			break
		}
		data := make([]map[string]any, 0, len(s.values))
		for uid, v := range s.values {
			data = append(data, map[string]any{"uid": uid, "value": v})
		}
		reply["data"] = data
	default:
		reply["code"] = 404
	}
	s.mu.Unlock()
	s.send(c, reply)
}

func (s *Server) send(c *conn, m map[string]any) {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	s.sendBytes(c, b)
}

func (s *Server) sendBytes(c *conn, plain []byte) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	frame, err := c.codec.Seal(plain)
	if err != nil {
		return
	}
	s.mu.Lock()
	if s.corruptNext {
		s.corruptNext = false
		frame[len(frame)-1] ^= 0xff
	}
	s.mu.Unlock()
	c.ws.WriteMessage(websocket.BinaryMessage, frame)
}
