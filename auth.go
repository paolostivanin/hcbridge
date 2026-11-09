package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type Token struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	Scope        string    `json:"scope"`
}

type tokenResponse struct {
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	ExpiresIn        int    `json:"expires_in"`
	Scope            string `json:"scope"`
	TokenType        string `json:"token_type"`
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

type deviceCodeResponse struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	ExpiresIn               int    `json:"expires_in"`
	Interval                int    `json:"interval"`
}

// refreshEarly is how long before expiry the access token is refreshed.
const refreshEarly = 5 * time.Minute

// refreshRetryAfter is how long refreshes stay suppressed after the token
// endpoint rejected one. Bosch allows 100 refreshes a day; a revoked refresh
// token retried behind the 60 s SSE backoff would burn through that and
// flood the log, while a 15 min pause still recovers promptly from a
// transient server-side failure. Transport errors are not suppressed.
const refreshRetryAfter = 15 * time.Minute

type Authenticator struct {
	cfg    *Config
	log    *Logger
	client *http.Client

	mu    sync.Mutex
	token *Token

	// refreshMu makes refreshes single-flight: two callers inside the
	// pre-expiry window (or two simultaneous 401s) used to send two
	// refreshes with the same refresh token, which a rotating or
	// reuse-detecting server answers by killing the later one or the
	// whole token family.
	refreshMu   sync.Mutex
	lastFailAt  time.Time
	lastFailErr error
}

func NewAuthenticator(cfg *Config, log *Logger) *Authenticator {
	return &Authenticator{
		cfg:    cfg,
		log:    log,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (a *Authenticator) Load() error {
	path := a.cfg.OAuth.TokenFile
	if fi, err := os.Stat(path); err == nil && fi.Mode().Perm()&0o077 != 0 {
		a.log.Warn("token file %s is mode %o; it holds a refresh token and should be 0600", path, fi.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	t := &Token{}
	if err := json.Unmarshal(data, t); err != nil {
		return fmt.Errorf("parse token file: %w", err)
	}
	a.mu.Lock()
	a.token = t
	a.mu.Unlock()
	return nil
}

func (a *Authenticator) current() *Token {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.token
}

func (a *Authenticator) save(t *Token) error {
	data, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(a.cfg.OAuth.TokenFile, data, 0o600)
}

// AccessToken returns a valid access token, refreshing if it is close to
// expiry. If that refresh fails while the current token is still valid, the
// current token is returned so the call goes ahead.
func (a *Authenticator) AccessToken(ctx context.Context) (string, error) {
	t := a.current()
	if t == nil {
		return "", errors.New("no token loaded; run --mode=auth first")
	}
	if time.Until(t.ExpiresAt) > refreshEarly {
		return t.AccessToken, nil
	}
	tok, err := a.refresh(ctx, t.AccessToken)
	if err != nil {
		if time.Now().Before(t.ExpiresAt) {
			a.log.Warn("token refresh failed, using the current token until it expires at %s: %v",
				t.ExpiresAt.Format(time.RFC3339), err)
			return t.AccessToken, nil
		}
		return "", err
	}
	return tok, nil
}

// ForceRefresh refreshes after the server rejected `stale` with a 401. If
// another caller has refreshed in the meantime, its token is returned.
func (a *Authenticator) ForceRefresh(ctx context.Context, stale string) (string, error) {
	return a.refresh(ctx, stale)
}

func (a *Authenticator) refresh(ctx context.Context, stale string) (string, error) {
	a.refreshMu.Lock()
	defer a.refreshMu.Unlock()

	t := a.current()
	if t == nil || t.RefreshToken == "" {
		return "", errors.New("no refresh token; reauth required")
	}
	if t.AccessToken != stale && time.Until(t.ExpiresAt) > refreshEarly {
		return t.AccessToken, nil // refreshed by another caller while we waited
	}
	if a.lastFailErr != nil && time.Since(a.lastFailAt) < refreshRetryAfter {
		return "", fmt.Errorf("token refresh suppressed for %s after a rejected refresh: %w",
			(refreshRetryAfter - time.Since(a.lastFailAt)).Round(time.Second), a.lastFailErr)
	}

	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", t.RefreshToken)
	form.Set("client_id", a.cfg.OAuth.ClientID)

	req, err := http.NewRequestWithContext(ctx, "POST",
		a.cfg.APIBase()+"/security/oauth/token",
		strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("refresh request: %w", err) // transport: not suppressed
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	tr := &tokenResponse{}
	if err := json.Unmarshal(body, tr); err != nil {
		// Only quote the body of an error response: a 200 body that failed
		// to decode would still contain live tokens.
		detail := "(body withheld)"
		if resp.StatusCode != 200 {
			detail = snippet(body)
		}
		return "", a.rejected(fmt.Errorf("parse refresh response (%d): %s", resp.StatusCode, detail))
	}
	if tr.Error != "" || resp.StatusCode != 200 || tr.AccessToken == "" {
		return "", a.rejected(fmt.Errorf("refresh failed (%d): %s: %s", resp.StatusCode, tr.Error, tr.ErrorDescription))
	}
	a.lastFailErr = nil

	newTok := &Token{
		AccessToken:  tr.AccessToken,
		RefreshToken: tr.RefreshToken,
		ExpiresAt:    time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second),
		Scope:        tr.Scope,
	}
	if newTok.RefreshToken == "" {
		newTok.RefreshToken = t.RefreshToken
	}
	a.mu.Lock()
	a.token = newTok
	a.mu.Unlock()
	if err := a.save(newTok); err != nil {
		// The new token is valid and, if the server rotated the refresh
		// token, the only valid one, so keep using it; the next refresh
		// retries the save. A restart before then would load the stale
		// file and need a re-auth.
		a.log.Error("save refreshed token to %s: %v (continuing with the in-memory token)", a.cfg.OAuth.TokenFile, err)
	}
	return newTok.AccessToken, nil
}

func (a *Authenticator) rejected(err error) error {
	a.lastFailAt = time.Now()
	a.lastFailErr = err
	return err
}

// DeviceFlow runs the OAuth2 device authorization grant interactively.
// Prints the user code + verification URL and polls until approval.
func (a *Authenticator) DeviceFlow(ctx context.Context) error {
	form := url.Values{}
	form.Set("client_id", a.cfg.OAuth.ClientID)
	form.Set("scope", strings.Join(a.cfg.OAuth.Scopes, " "))

	req, err := http.NewRequestWithContext(ctx, "POST",
		a.cfg.APIBase()+"/security/oauth/device_authorization",
		strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("device_authorization request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != 200 {
		return fmt.Errorf("device_authorization failed (%d): %s", resp.StatusCode, snippet(body))
	}
	dc := &deviceCodeResponse{}
	if err := json.Unmarshal(body, dc); err != nil {
		return fmt.Errorf("parse device_authorization: %w", err)
	}

	fmt.Println()
	if dc.VerificationURIComplete != "" {
		fmt.Printf("  One-click URL (already contains the code):\n    %s\n\n", dc.VerificationURIComplete)
		fmt.Printf("  ...or open this URL and paste the code manually:\n    URL:  %s\n    code: %s\n\n",
			dc.VerificationURI, dc.UserCode)
	} else {
		fmt.Printf("  Open this URL in a browser:\n    %s\n\n", dc.VerificationURI)
		fmt.Printf("  Enter this code (no spaces):\n    %s\n\n", dc.UserCode)
	}
	fmt.Printf("  Waiting up to %d s for approval; polling every %d s.\n\n", dc.ExpiresIn, dc.Interval)

	interval := time.Duration(dc.Interval) * time.Second
	if interval == 0 {
		interval = 5 * time.Second
	}
	deadline := time.Now().Add(time.Duration(dc.ExpiresIn) * time.Second)

	for {
		if time.Now().After(deadline) {
			return errors.New("device flow timeout: user did not approve in time")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}

		pollForm := url.Values{}
		pollForm.Set("grant_type", "urn:ietf:params:oauth:grant-type:device_code")
		pollForm.Set("device_code", dc.DeviceCode)
		pollForm.Set("client_id", a.cfg.OAuth.ClientID)

		pollReq, err := http.NewRequestWithContext(ctx, "POST",
			a.cfg.APIBase()+"/security/oauth/token",
			strings.NewReader(pollForm.Encode()))
		if err != nil {
			return err
		}
		pollReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		pResp, err := a.client.Do(pollReq)
		if err != nil {
			fmt.Printf("  poll error (will retry): %v\n", err)
			continue
		}
		pBody, _ := io.ReadAll(io.LimitReader(pResp.Body, 1<<20))
		pResp.Body.Close()

		tr := &tokenResponse{}
		_ = json.Unmarshal(pBody, tr)

		switch tr.Error {
		case "":
			if tr.AccessToken == "" {
				return fmt.Errorf("token response (%d) missing access_token", pResp.StatusCode)
			}
			t := &Token{
				AccessToken:  tr.AccessToken,
				RefreshToken: tr.RefreshToken,
				ExpiresAt:    time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second),
				Scope:        tr.Scope,
			}
			a.mu.Lock()
			a.token = t
			a.mu.Unlock()
			if err := a.save(t); err != nil {
				return a.rescueToken(t, err)
			}
			fmt.Printf("\n  Authorized. Token saved to %s\n\n", a.cfg.OAuth.TokenFile)
			return nil
		case "authorization_pending":
			continue
		case "slow_down":
			interval += 5 * time.Second
			continue
		case "expired_token":
			return errors.New("device code expired before user approval")
		case "access_denied":
			return errors.New("user denied authorization")
		default:
			return fmt.Errorf("device flow error: %s: %s", tr.Error, tr.ErrorDescription)
		}
	}
}

// rescueToken handles a valid token that could not be saved to the configured
// path. It goes to a private temp file rather than stderr, so it does not end
// up in a terminal scrollback or a service's error log.
func (a *Authenticator) rescueToken(t *Token, saveErr error) error {
	dump, _ := json.MarshalIndent(t, "", "  ")
	f, err := os.CreateTemp("", "hcbridge-token-*.json") // 0600
	if err == nil {
		_, err = f.Write(dump)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		return fmt.Errorf("save token to %s: %v; rescue copy also failed: %v (re-run --mode=auth)",
			a.cfg.OAuth.TokenFile, saveErr, err)
	}
	fmt.Fprintf(os.Stderr,
		"\nFAILED to save token to %s: %v\n\n"+
			"The token IS valid and was written to %s (mode 0600).\n"+
			"Move it into place (owned by the service user, mode 0600), or point\n"+
			"`token_file` under `oauth:` at a writable path.\n\n",
		a.cfg.OAuth.TokenFile, saveErr, f.Name())
	return fmt.Errorf("save token: %w", saveErr)
}
