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
	"path/filepath"
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

type Authenticator struct {
	cfg    *Config
	client *http.Client

	mu    sync.Mutex
	token *Token
}

func NewAuthenticator(cfg *Config) *Authenticator {
	return &Authenticator{
		cfg:    cfg,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (a *Authenticator) Load() error {
	data, err := os.ReadFile(a.cfg.OAuth.TokenFile)
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

func (a *Authenticator) save(t *Token) error {
	if err := os.MkdirAll(filepath.Dir(a.cfg.OAuth.TokenFile), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}
	tmp := a.cfg.OAuth.TokenFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, a.cfg.OAuth.TokenFile)
}

// AccessToken returns a valid access token, refreshing if needed.
func (a *Authenticator) AccessToken(ctx context.Context) (string, error) {
	a.mu.Lock()
	t := a.token
	a.mu.Unlock()
	if t == nil {
		return "", errors.New("no token loaded; run with --auth first")
	}
	if time.Until(t.ExpiresAt) > 5*time.Minute {
		return t.AccessToken, nil
	}
	return a.refresh(ctx)
}

// ForceRefresh refreshes regardless of expiry; used after a 401.
func (a *Authenticator) ForceRefresh(ctx context.Context) (string, error) {
	return a.refresh(ctx)
}

func (a *Authenticator) refresh(ctx context.Context) (string, error) {
	a.mu.Lock()
	t := a.token
	a.mu.Unlock()
	if t == nil || t.RefreshToken == "" {
		return "", errors.New("no refresh token; reauth required")
	}

	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", t.RefreshToken)
	form.Set("client_id", a.cfg.OAuth.ClientID)

	req, _ := http.NewRequestWithContext(ctx, "POST",
		a.cfg.APIBase()+"/security/oauth/token",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("refresh request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	tr := &tokenResponse{}
	if err := json.Unmarshal(body, tr); err != nil {
		return "", fmt.Errorf("parse refresh response (%d): %s", resp.StatusCode, string(body))
	}
	if tr.Error != "" || resp.StatusCode != 200 {
		return "", fmt.Errorf("refresh failed: %s: %s", tr.Error, tr.ErrorDescription)
	}

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
		return "", fmt.Errorf("save refreshed token: %w", err)
	}
	return newTok.AccessToken, nil
}

// DeviceFlow runs the OAuth2 device authorization grant interactively.
// Prints the user code + verification URL and polls until approval.
func (a *Authenticator) DeviceFlow(ctx context.Context) error {
	form := url.Values{}
	form.Set("client_id", a.cfg.OAuth.ClientID)
	form.Set("scope", strings.Join(a.cfg.OAuth.Scopes, " "))

	req, _ := http.NewRequestWithContext(ctx, "POST",
		a.cfg.APIBase()+"/security/oauth/device_authorization",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("device_authorization request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("device_authorization failed (%d): %s", resp.StatusCode, string(body))
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

		pollReq, _ := http.NewRequestWithContext(ctx, "POST",
			a.cfg.APIBase()+"/security/oauth/token",
			strings.NewReader(pollForm.Encode()))
		pollReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		pResp, err := a.client.Do(pollReq)
		if err != nil {
			fmt.Printf("  poll error (will retry): %v\n", err)
			continue
		}
		pBody, _ := io.ReadAll(pResp.Body)
		pResp.Body.Close()

		tr := &tokenResponse{}
		_ = json.Unmarshal(pBody, tr)

		switch tr.Error {
		case "":
			if tr.AccessToken == "" {
				return fmt.Errorf("token response missing access_token: %s", string(pBody))
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
				// Emit the token to stderr so the user can recover without
				// having to redo the OAuth flow.
				dump, _ := json.MarshalIndent(t, "", "  ")
				fmt.Fprintf(os.Stderr,
					"\nFAILED to save token to %s: %v\n\n"+
						"The token IS valid. To recover: write the JSON below to a writable path,\n"+
						"then add `token_file: <that path>` under `oauth:` in your config.\n\n"+
						"--- token.json ---\n%s\n--- end ---\n\n",
					a.cfg.OAuth.TokenFile, err, string(dump))
				return fmt.Errorf("save token: %w", err)
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
