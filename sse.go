package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// SSEEvent is one decoded message from the Home Connect event stream, or one
// of the client's synthetic events: STREAM_UP (a session was established) and
// STREAM_DOWN (an established session ended).
type SSEEvent struct {
	Event string // STATUS / NOTIFY / EVENT / CONNECTED / DISCONNECTED / PAIRED / DEPAIRED / STREAM_UP / STREAM_DOWN
	HaID  string
	Items []SSEItem
}

type SSEItem struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	Unit  string      `json:"unit,omitempty"`
}

type sseDataPayload struct {
	HaID  string    `json:"haId"`
	Items []SSEItem `json:"items"`
}

type EventHandler func(SSEEvent)

// maxSSELine caps one SSE line. Real events are a few hundred bytes; the cap
// only stops a broken or hostile peer from growing a line until the 256 MB
// container runs out of memory.
const maxSSELine = 1 << 20

type SSEClient struct {
	cfg   *Config
	auth  *Authenticator
	log   *Logger
	quota *Quota

	// One client for every connect. HTTP/2 is disabled: Bosch's SSE
	// endpoint has a known issue where HTTP/2 connections never deliver
	// response headers (Go reports "http2: timeout awaiting response
	// headers"); an empty, non-nil TLSNextProto map disables ALPN h2.
	// Keep-alives are off because a stream connection is never reused; a
	// fresh Transport per connect used to leave an idle connection and its
	// goroutines behind after every failed or EOF'd attempt.
	http *http.Client

	// Reconnect backoff bounds and the stall watchdog; fields rather than
	// constants so tests can shrink them.
	backoffInitial   time.Duration
	backoffMax       time.Duration
	backoffMaxStop   time.Duration // ceiling while in the budgetStop tier
	minStableSession time.Duration // session length that resets the backoff
	idleTimeout      time.Duration // no line for this long = stalled stream
}

func NewSSEClient(cfg *Config, auth *Authenticator, quota *Quota, log *Logger) *SSEClient {
	dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	return &SSEClient{
		cfg: cfg, auth: auth, log: log, quota: quota,
		http: &http.Client{Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext,
			TLSHandshakeTimeout:   15 * time.Second,
			ResponseHeaderTimeout: 60 * time.Second,
			TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
			DisableKeepAlives:     true,
		}},
		backoffInitial:   5 * time.Second,
		backoffMax:       60 * time.Second,
		backoffMaxStop:   10 * time.Minute,
		minStableSession: 60 * time.Second,
		// Bosch sends KEEP-ALIVE about every 55 s; two missed ones mean the
		// stream is dead even if the TCP connection is still up (e.g. held
		// open by a middlebox), which nothing else would ever notice.
		idleTimeout: 120 * time.Second,
	}
}

// Run blocks until ctx is cancelled. It connects to the per-appliance event stream
// and dispatches events to handler. On disconnect it backs off and reconnects.
// After every established connect (HTTP 200) it emits STREAM_UP so the caller
// can resync (the caller decides whether one is due), and when that session
// ends it emits STREAM_DOWN. Failed attempts emit nothing: a resync per failed
// retry used to cost a full resync every minute during an outage.
//
// Backoff runs backoffInitial -> backoffMax across consecutive failures and
// resets only after a session that stayed up for minStableSession, so Bosch's
// routine ~24 h stream close reconnects after 5 s, while a stream that
// connects and drops at once keeps backing off (each connect costs a call).
// In the budgetStop tier the ceiling rises to backoffMaxStop. While a rate
// limit block is in effect no connect is attempted at all: Bosch rejects it
// anyway, and blind retries are what kept the old bridge hammering the API.
func (s *SSEClient) Run(ctx context.Context, haID string, handler EventHandler) {
	backoff := s.backoffInitial

	for {
		if blocked, until := s.quota.Blocked(); blocked {
			wait := time.Until(until) + 5*time.Second
			s.log.Warn("SSE paused: API rate-limited until %s (retry in %s)",
				until.Format(time.RFC3339), wait.Round(time.Second))
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}

		established, err := s.runOnce(ctx, haID, handler)
		if !established.IsZero() && time.Since(established) >= s.minStableSession {
			backoff = s.backoffInitial
		}
		if ctx.Err() != nil {
			return
		}
		if blocked, _ := s.quota.Blocked(); blocked {
			// The loop head waits out the block; no extra backoff needed.
			s.log.Warn("SSE disconnected: %v", err)
			continue
		}
		s.log.Warn("SSE disconnected: %v (reconnect in %s)", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		limit := s.backoffMax
		if currentTier(s.quota) == tierStop && s.backoffMaxStop > limit {
			limit = s.backoffMaxStop
		}
		if backoff > limit {
			backoff = limit
		}
	}
}

// runOnce holds one event-stream session. established is when the stream
// was established (HTTP 200), zero if it never was, so Run can judge session
// length; err is the reason the session ended and is always non-nil.
func (s *SSEClient) runOnce(ctx context.Context, haID string, handler EventHandler) (established time.Time, err error) {
	var zero time.Time
	tok, err := s.auth.AccessToken(ctx)
	if err != nil {
		return zero, fmt.Errorf("token: %w", err)
	}
	// connCtx lets the stall watchdog end this session without touching
	// ctx, which means "shut down" to Run.
	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()

	url := s.cfg.APIBase() + "/api/homeappliances/" + haID + "/events"
	req, err := http.NewRequestWithContext(connCtx, "GET", url, nil)
	if err != nil {
		return zero, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Accept-Language", "en-US")
	req.Header.Set("Cache-Control", "no-cache")

	s.quota.RecordCall()
	resp, err := s.http.Do(req)
	if err != nil {
		return zero, err
	}
	defer resp.Body.Close()
	s.quota.RecordResult(resp.StatusCode)
	if resp.StatusCode == 401 {
		if _, rerr := s.auth.ForceRefresh(ctx, tok); rerr != nil {
			return zero, fmt.Errorf("auth refresh: %w", rerr)
		}
		return zero, errors.New("401 unauthorized; will reconnect after refresh")
	}
	if resp.StatusCode == 429 {
		// Share the block with the REST client: polls and resyncs stop too,
		// and Run waits it out instead of reconnecting every minute. If the
		// length can't be parsed, fall back to the normal backoff rather than
		// guessing an hour of blindness.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if secs := retryAfterSeconds(resp.Header, body); secs > 0 {
			s.quota.Block(time.Now().Add(time.Duration(secs) * time.Second))
		}
		return zero, fmt.Errorf("SSE 429: %s", snippet(body))
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return zero, fmt.Errorf("SSE %d: %s", resp.StatusCode, snippet(body))
	}
	established = time.Now()
	s.log.Info("SSE connected to %s", url)
	// Let the caller resync now that the stream is up. Runs synchronously on
	// this goroutine, so events arriving meanwhile queue in the socket and
	// are applied after the snapshot - newer values win.
	handler(SSEEvent{Event: "STREAM_UP", HaID: haID})
	defer handler(SSEEvent{Event: "STREAM_DOWN", HaID: haID})

	// The watchdog starts after the STREAM_UP handler, which may take up to
	// its resync timeout.
	idle := time.AfterFunc(s.idleTimeout, connCancel)
	defer idle.Stop()

	br := bufio.NewReaderSize(resp.Body, 8192)
	var (
		evType string
		dataB  strings.Builder
		idStr  string
	)
	flush := func() {
		if evType == "" && dataB.Len() == 0 {
			return
		}
		s.dispatch(handler, evType, idStr, dataB.String())
		evType, idStr = "", ""
		dataB.Reset()
	}
	for {
		line, err := readLine(br, maxSSELine)
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return established, ctx.Err()
			case connCtx.Err() != nil:
				return established, fmt.Errorf("stalled: no data for %s", s.idleTimeout)
			case errors.Is(err, io.EOF):
				return established, errors.New("EOF on event stream")
			}
			return established, fmt.Errorf("read SSE: %w", err)
		}
		idle.Reset(s.idleTimeout)
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			evType = value
		case "data":
			if dataB.Len() > 0 {
				dataB.WriteByte('\n')
			}
			dataB.WriteString(value)
		case "id":
			idStr = value
		}
	}
}

// readLine reads up to and including '\n', failing if the line exceeds max.
func readLine(br *bufio.Reader, max int) (string, error) {
	var buf []byte
	for {
		chunk, err := br.ReadSlice('\n')
		if len(buf)+len(chunk) > max {
			return "", fmt.Errorf("SSE line longer than %d bytes", max)
		}
		buf = append(buf, chunk...)
		if err == nil {
			return string(buf), nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return "", err
		}
	}
}

func (s *SSEClient) dispatch(handler EventHandler, evType, id, data string) {
	if evType == "KEEP-ALIVE" {
		return
	}
	out := SSEEvent{Event: evType, HaID: id}

	switch evType {
	case "CONNECTED", "DISCONNECTED", "PAIRED", "DEPAIRED":
		// no items
	default:
		if data == "" {
			return
		}
		var p sseDataPayload
		if err := json.Unmarshal([]byte(data), &p); err != nil {
			s.log.Warn("SSE %s: bad JSON: %v (raw=%s)", evType, err, snippet([]byte(data)))
			return
		}
		if p.HaID != "" {
			out.HaID = p.HaID
		}
		out.Items = p.Items
	}
	handler(out)
}
