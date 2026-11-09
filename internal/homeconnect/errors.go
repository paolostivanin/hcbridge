package homeconnect

import (
	"errors"
	"fmt"
	"net/http"
)

var (
	// ErrFirstFrameMAC: the very first frame of a connection failed the MAC
	// check. Repeated across connections it means the profile key is wrong
	// or stale (the appliance was re-paired).
	ErrFirstFrameMAC = errors.New("homeconnect: first frame failed authentication (wrong or stale profile key?)")
	// ErrDesync: a later frame failed; the connection must be rebuilt.
	ErrDesync = errors.New("homeconnect: frame chain desynchronised")
	// ErrPongTimeout: no frame of any kind arrived within the pong timeout
	// after a ping.
	ErrPongTimeout = errors.New("homeconnect: no answer to ping")
	// ErrSessionClosed: the connection went away while a request waited.
	ErrSessionClosed = errors.New("homeconnect: session closed")
)

// DialError is a failure to open the TCP connection (timeout, connection
// refused, host or network unreachable): the hob is off the network or the
// path to it is blocked.
type DialError struct{ Err error }

func (e *DialError) Error() string { return "connect: " + e.Err.Error() }
func (e *DialError) Unwrap() error { return e.Err }

// UpgradeError is a non-101 answer to the WebSocket upgrade. A 503 here is
// the firmware refusing local sessions altogether.
type UpgradeError struct {
	Status int
	Header http.Header
	Body   []byte // at most 1 KiB
}

func (e *UpgradeError) Error() string {
	return fmt.Sprintf("websocket upgrade refused: HTTP %d %s", e.Status, http.StatusText(e.Status))
}

// RequestError is an appliance error code in a RESPONSE.
type RequestError struct {
	Resource string
	Code     int
}

func (e *RequestError) Error() string {
	return fmt.Sprintf("%s: appliance error %d", e.Resource, e.Code)
}

// TimeoutError is a request the appliance did not answer in time.
type TimeoutError struct{ Resource string }

func (e *TimeoutError) Error() string { return e.Resource + ": no response" }
