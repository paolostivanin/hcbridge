package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const acceptJSON = "application/vnd.bsh.sdk.v1+json"

type APIClient struct {
	cfg   *Config
	auth  *Authenticator
	quota *Quota
	http  *http.Client
}

func NewAPIClient(cfg *Config, auth *Authenticator, quota *Quota) *APIClient {
	return &APIClient{
		cfg:   cfg,
		auth:  auth,
		quota: quota,
		http:  &http.Client{Timeout: 30 * time.Second},
	}
}

type apiKV struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	Unit  string      `json:"unit,omitempty"`
}

type listResponse struct {
	Data struct {
		Items []apiKV `json:"status"`
	} `json:"data"`
}

type settingsListResponse struct {
	Data struct {
		Items []apiKV `json:"settings"`
	} `json:"data"`
}

type optionsListResponse struct {
	Data struct {
		Items []apiKV `json:"options"`
	} `json:"data"`
}

type appliancesResponse struct {
	Data struct {
		Homeappliances []struct {
			HaID      string `json:"haId"`
			Name      string `json:"name"`
			Type      string `json:"type"`
			Brand     string `json:"brand"`
			VIB       string `json:"vib"`
			Connected bool   `json:"connected"`
			Enumber   string `json:"enumber"`
		} `json:"homeappliances"`
	} `json:"data"`
}

func (c *APIClient) do(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	if blocked, until := c.quota.Blocked(); blocked {
		return nil, 429, fmt.Errorf("rate-limited until %s (skipping %s %s)", until.Format(time.RFC3339), method, path)
	}

	url := c.cfg.APIBase() + path

	send := func(tok string) (*http.Response, error) {
		var rdr io.Reader
		if body != nil {
			rdr = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, rdr)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Accept", acceptJSON)
		if body != nil {
			req.Header.Set("Content-Type", acceptJSON)
		}
		c.quota.RecordCall()
		return c.http.Do(req)
	}

	tok, err := c.auth.AccessToken(ctx)
	if err != nil {
		return nil, 0, err
	}
	resp, err := send(tok)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode == 401 {
		resp.Body.Close()
		c.quota.RecordResult(401)
		tok, err = c.auth.ForceRefresh(ctx, tok)
		if err != nil {
			return nil, 401, err
		}
		resp, err = send(tok)
		if err != nil {
			return nil, 0, err
		}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	c.quota.RecordResult(resp.StatusCode)

	// On 429, honour the announced block. Body shape:
	// {"error":{"key":"429","description":"The rate limit \"1000 calls in 1
	// day\" was reached. Requests are blocked during the remaining period of
	// 54434 seconds."}}
	if resp.StatusCode == 429 {
		retrySecs := retryAfterSeconds(resp.Header, data)
		if retrySecs <= 0 {
			retrySecs = 3600 // sane default if we can't parse
		}
		c.quota.Block(time.Now().Add(time.Duration(retrySecs) * time.Second))
	}

	return data, resp.StatusCode, err
}

// retryAfterSeconds extracts the block length from a 429 response: the body's
// "remaining period of N seconds" first (the form Bosch is known to send), then
// a standard Retry-After header (delta-seconds). Returns 0 if neither parses.
func retryAfterSeconds(h http.Header, body []byte) int {
	if n := parse429RetrySeconds(body); n > 0 {
		return n
	}
	if v := strings.TrimSpace(h.Get("Retry-After")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

// retryPeriodRe matches Bosch's 429 description, e.g. "...blocked during the
// remaining period of 82537 seconds.". The original hand-rolled scanner
// compared an 8-byte slice against the 7-byte " second", so it never matched
// and every block fell back to the 3600 s default.
var retryPeriodRe = regexp.MustCompile(`of (\d+) seconds?`)

func parse429RetrySeconds(body []byte) int {
	var r struct {
		Error struct {
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return 0
	}
	m := retryPeriodRe.FindStringSubmatch(r.Error.Description)
	if m == nil {
		return 0
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0
	}
	return n
}

func (c *APIClient) ListAppliances(ctx context.Context) ([]string, error) {
	body, code, err := c.do(ctx, "GET", "/api/homeappliances", nil)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("list appliances %d: %s", code, snippet(body))
	}
	var r appliancesResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("parse appliances: %w", err)
	}
	out := make([]string, 0, len(r.Data.Homeappliances))
	for _, a := range r.Data.Homeappliances {
		out = append(out, fmt.Sprintf("%s\t%s\t%s\t%s\t%s\tconnected=%v",
			a.HaID, a.Type, a.Brand, a.VIB, a.Enumber, a.Connected))
	}
	return out, nil
}

// FetchAll returns the union of /status and /settings as a flat key→value map,
// and whether the appliance is online. Two API calls; one when /status answers
// 409, which is how the API reports an appliance that is not connected (the
// settings call would only fail the same way).
//
// Active program options (per-zone state) are deliberately not fetched here:
// Bridge.PollActiveOptions reads them with the right endpoint and the per-zone
// bookkeeping.
func (c *APIClient) FetchAll(ctx context.Context, haID string) (map[string]interface{}, bool, error) {
	out := make(map[string]interface{})

	statusBody, statusCode, err := c.do(ctx, "GET", "/api/homeappliances/"+haID+"/status", nil)
	if err != nil {
		return nil, false, fmt.Errorf("fetch status: %w", err)
	}
	switch statusCode {
	case 200:
		var r listResponse
		if err := json.Unmarshal(statusBody, &r); err != nil {
			return nil, false, fmt.Errorf("parse status: %w", err)
		}
		for _, kv := range r.Data.Items {
			out[kv.Key] = kv.Value
		}
	case 409:
		return out, false, nil
	case 404:
	default:
		return nil, false, fmt.Errorf("status %d: %s", statusCode, snippet(statusBody))
	}

	settingsBody, settingsCode, err := c.do(ctx, "GET", "/api/homeappliances/"+haID+"/settings", nil)
	if err != nil {
		return nil, false, fmt.Errorf("fetch settings: %w", err)
	}
	if settingsCode == 200 {
		var r settingsListResponse
		if err := json.Unmarshal(settingsBody, &r); err != nil {
			return nil, false, fmt.Errorf("parse settings: %w", err)
		}
		for _, kv := range r.Data.Items {
			out[kv.Key] = kv.Value
		}
	} else if settingsCode != 404 && settingsCode != 409 {
		return nil, false, fmt.Errorf("settings %d: %s", settingsCode, snippet(settingsBody))
	}

	return out, true, nil
}

// RawGET fetches an arbitrary API path and returns the response body + status.
// Used by --mode=dump to enumerate everything the API exposes for an appliance.
func (c *APIClient) RawGET(ctx context.Context, path string) ([]byte, int, error) {
	return c.do(ctx, "GET", path, nil)
}

// PutSetting sends PUT /api/homeappliances/{haId}/settings/{key} with a typed value.
func (c *APIClient) PutSetting(ctx context.Context, haID, key string, value interface{}) error {
	return c.putKV(ctx, "/api/homeappliances/"+haID+"/settings/"+key, key, value)
}

// PutActiveOption sends PUT /api/homeappliances/{haId}/programs/active/options/{key}.
// On a hob the change needs confirming on the panel, so it is CLI-only
// (--mode=put) and never driven from MQTT.
func (c *APIClient) PutActiveOption(ctx context.Context, haID, key string, value interface{}) error {
	return c.putKV(ctx, "/api/homeappliances/"+haID+"/programs/active/options/"+key, key, value)
}

// PutSelectedOption sends PUT /api/homeappliances/{haId}/programs/selected/options/{key}.
func (c *APIClient) PutSelectedOption(ctx context.Context, haID, key string, value interface{}) error {
	return c.putKV(ctx, "/api/homeappliances/"+haID+"/programs/selected/options/"+key, key, value)
}

func (c *APIClient) putKV(ctx context.Context, path, key string, value interface{}) error {
	payload := struct {
		Data apiKV `json:"data"`
	}{Data: apiKV{Key: key, Value: value}}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, code, err := c.do(ctx, "PUT", path, body)
	if err != nil {
		return err
	}
	if code != 204 && code != 200 {
		return fmt.Errorf("PUT %s -> %d: %s", key, code, snippet(resp))
	}
	return nil
}
