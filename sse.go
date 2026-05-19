package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// SSEEvent is one decoded message from the Home Connect event stream.
type SSEEvent struct {
	Event string // STATUS / NOTIFY / EVENT / CONNECTED / DISCONNECTED / KEEP-ALIVE / PAIRED / DEPAIRED
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

type SSEClient struct {
	cfg  *Config
	auth *Authenticator
	log  *Logger
}

func NewSSEClient(cfg *Config, auth *Authenticator, log *Logger) *SSEClient {
	return &SSEClient{cfg: cfg, auth: auth, log: log}
}

// Run blocks until ctx is cancelled. It connects to the per-appliance event stream
// and dispatches events to handler. On disconnect it backs off and reconnects.
// Caller is responsible for triggering an initial state resync after reconnect
// (we signal this by emitting a synthetic Event="RECONNECTED" with no items).
func (s *SSEClient) Run(ctx context.Context, haID string, handler EventHandler) {
	backoff := 5 * time.Second
	const maxBackoff = 60 * time.Second

	for {
		if err := s.runOnce(ctx, haID, handler); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			s.log.Warn("SSE disconnected: %v (reconnect in %s)", err, backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		// Tell the consumer we just reconnected so they can re-fetch full state.
		handler(SSEEvent{Event: "RECONNECTED", HaID: haID})
	}
}

func (s *SSEClient) runOnce(ctx context.Context, haID string, handler EventHandler) error {
	tok, err := s.auth.AccessToken(ctx)
	if err != nil {
		return fmt.Errorf("token: %w", err)
	}
	url := s.cfg.APIBase() + "/api/homeappliances/" + haID + "/events"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Accept-Language", "en-US")
	req.Header.Set("Cache-Control", "no-cache")

	// SSE is a long-lived stream; the server emits KEEP-ALIVE every ~55s.
	// Force HTTP/1.1: Bosch's SSE endpoint has a known issue where
	// HTTP/2 connections never deliver response headers (Go reports
	// "http2: timeout awaiting response headers"). Setting TLSNextProto
	// to an empty (non-nil) map disables ALPN negotiation for h2.
	c := &http.Client{
		Transport: &http.Transport{
			ResponseHeaderTimeout: 60 * time.Second,
			TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
		},
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 401 {
		_, rerr := s.auth.ForceRefresh(ctx)
		if rerr != nil {
			return fmt.Errorf("auth refresh: %w", rerr)
		}
		return errors.New("401 unauthorized; will reconnect after refresh")
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("SSE %d: %s", resp.StatusCode, string(body))
	}
	s.log.Info("SSE connected to %s", url)

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
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line, err := br.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("EOF on event stream")
			}
			return fmt.Errorf("read SSE: %w", err)
		}
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
			s.log.Warn("SSE %s: bad JSON: %v (raw=%s)", evType, err, data)
			return
		}
		if p.HaID != "" {
			out.HaID = p.HaID
		}
		out.Items = p.Items
	}
	handler(out)
}
