package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const acceptJSON = "application/vnd.bsh.sdk.v1+json"

type APIClient struct {
	cfg  *Config
	auth *Authenticator
	http *http.Client
}

func NewAPIClient(cfg *Config, auth *Authenticator) *APIClient {
	return &APIClient{
		cfg:  cfg,
		auth: auth,
		http: &http.Client{Timeout: 30 * time.Second},
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

// rateLimitMu protects the rateLimitedUntil timestamp shared across goroutines.
var rateLimitMu sync.Mutex
var rateLimitedUntil time.Time

// IsRateLimited returns true if a 429 response is still in effect.
func IsRateLimited() (bool, time.Time) {
	rateLimitMu.Lock()
	defer rateLimitMu.Unlock()
	return time.Now().Before(rateLimitedUntil), rateLimitedUntil
}

func setRateLimit(until time.Time) {
	rateLimitMu.Lock()
	rateLimitedUntil = until
	rateLimitMu.Unlock()
}

func (c *APIClient) do(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	if blocked, until := IsRateLimited(); blocked {
		return nil, 429, fmt.Errorf("rate-limited until %s (skipping %s %s)", until.Format(time.RFC3339), method, path)
	}

	url := c.cfg.APIBase() + path

	send := func() (*http.Response, error) {
		tok, err := c.auth.AccessToken(ctx)
		if err != nil {
			return nil, err
		}
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
		return c.http.Do(req)
	}

	resp, err := send()
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode == 401 {
		resp.Body.Close()
		if _, rerr := c.auth.ForceRefresh(ctx); rerr != nil {
			return nil, 401, rerr
		}
		resp, err = send()
		if err != nil {
			return nil, 0, err
		}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)

	// On 429, parse the suggested retry interval and set the global block.
	// Body shape: {"error":{"description":"... blocked during the remaining
	//              period of NNN seconds.", "key":"429"}}
	if resp.StatusCode == 429 {
		retrySecs := parse429RetrySeconds(data)
		if retrySecs <= 0 {
			retrySecs = 3600 // sane default if we can't parse
		}
		setRateLimit(time.Now().Add(time.Duration(retrySecs) * time.Second))
	}

	return data, resp.StatusCode, err
}

func parse429RetrySeconds(body []byte) int {
	var r struct {
		Error struct {
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return 0
	}
	// "...remaining period of 82537 seconds." — pull the integer.
	// Be lax: scan for "of <digits> seconds".
	desc := r.Error.Description
	idx := 0
	for {
		i := indexOf(desc[idx:], "of ")
		if i < 0 {
			return 0
		}
		idx += i + 3
		end := idx
		for end < len(desc) && desc[end] >= '0' && desc[end] <= '9' {
			end++
		}
		if end == idx {
			continue
		}
		// Verify this is followed by " seconds"
		if end < len(desc) && desc[end:min(end+8, len(desc))] != " second" {
			continue
		}
		n := 0
		for k := idx; k < end; k++ {
			n = n*10 + int(desc[k]-'0')
		}
		return n
	}
}

func indexOf(s, sub string) int {
	if len(sub) == 0 {
		return 0
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (c *APIClient) ListAppliances(ctx context.Context) ([]string, error) {
	body, code, err := c.do(ctx, "GET", "/api/homeappliances", nil)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("list appliances %d: %s", code, string(body))
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

// FetchAll returns the union of /status and /settings as a flat key→value map.
func (c *APIClient) FetchAll(ctx context.Context, haID string) (map[string]interface{}, error) {
	out := make(map[string]interface{})

	statusBody, statusCode, err := c.do(ctx, "GET", "/api/homeappliances/"+haID+"/status", nil)
	if err != nil {
		return nil, fmt.Errorf("fetch status: %w", err)
	}
	if statusCode == 200 {
		var r listResponse
		if err := json.Unmarshal(statusBody, &r); err != nil {
			return nil, fmt.Errorf("parse status: %w", err)
		}
		for _, kv := range r.Data.Items {
			out[kv.Key] = kv.Value
		}
	} else if statusCode != 404 && statusCode != 409 {
		return nil, fmt.Errorf("status %d: %s", statusCode, string(statusBody))
	}

	settingsBody, settingsCode, err := c.do(ctx, "GET", "/api/homeappliances/"+haID+"/settings", nil)
	if err != nil {
		return nil, fmt.Errorf("fetch settings: %w", err)
	}
	if settingsCode == 200 {
		var r settingsListResponse
		if err := json.Unmarshal(settingsBody, &r); err != nil {
			return nil, fmt.Errorf("parse settings: %w", err)
		}
		for _, kv := range r.Data.Items {
			out[kv.Key] = kv.Value
		}
	} else if settingsCode != 404 && settingsCode != 409 {
		return nil, fmt.Errorf("settings %d: %s", settingsCode, string(settingsBody))
	}

	// Active program options (per-zone state lives here for hobs, and Bosch
	// does not push these via SSE). 404 when nothing is cooking — fine.
	optsBody, optsCode, err := c.do(ctx, "GET", "/api/homeappliances/"+haID+"/programs/active/options", nil)
	if err != nil {
		return nil, fmt.Errorf("fetch program options: %w", err)
	}
	if optsCode == 200 {
		var r optionsListResponse
		if err := json.Unmarshal(optsBody, &r); err != nil {
			return nil, fmt.Errorf("parse program options: %w", err)
		}
		for _, kv := range r.Data.Items {
			out[kv.Key] = kv.Value
		}
	} else if optsCode != 404 && optsCode != 409 {
		return nil, fmt.Errorf("program options %d: %s", optsCode, string(optsBody))
	}

	return out, nil
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
// Used for per-zone state writes on hobs (Cooking.Hob.Option.ZoneSelector,
// Cooking.Hob.Option.PowerLevel, BSH.Common.Option.Duration, etc.). Requires
// Hob-Control scope.
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
		return fmt.Errorf("PUT %s -> %d: %s", key, code, string(resp))
	}
	return nil
}
