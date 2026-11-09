package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	OAuth struct {
		ClientID  string   `yaml:"client_id"`
		Scopes    []string `yaml:"scopes"`
		Region    string   `yaml:"region"`
		TokenFile string   `yaml:"token_file"`
	} `yaml:"oauth"`

	Appliance struct {
		HaID       string `yaml:"ha_id"`
		TopicSlug  string `yaml:"topic_slug"`
		FriendlyID string `yaml:"friendly_id"`
	} `yaml:"appliance"`

	MQTT struct {
		Host           string `yaml:"host"`
		Port           int    `yaml:"port"`
		Username       string `yaml:"username"`
		Password       string `yaml:"password"`
		ClientID       string `yaml:"client_id"`
		DiscoveryTopic string `yaml:"discovery_topic"`
		TLS            bool   `yaml:"tls"`
		CAFile         string `yaml:"ca_file"`
	} `yaml:"mqtt"`

	Logfile  string `yaml:"logfile"`
	LogLevel string `yaml:"log_level"`

	// apiBase overrides APIBase(); unexported and not read from YAML, so it
	// is only settable from tests (httptest servers).
	apiBase string
}

// topicSlugRe keeps MQTT wildcards and separators out of the topic root:
// a "+" slug would subscribe to every appliance's topics, a "#" or "/" one
// breaks the topic layout.
var topicSlugRe = regexp.MustCompile(`^[a-z0-9_-]+$`)

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return parseConfig(data)
}

func parseConfig(data []byte) (*Config, error) {
	c := &Config{}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // a typo'd key is an error, not a silent default
	if err := dec.Decode(c); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if c.OAuth.Region == "" {
		c.OAuth.Region = "global"
	}
	if c.OAuth.TokenFile == "" {
		c.OAuth.TokenFile = "/var/lib/hcbridge/token.json"
	}
	if len(c.OAuth.Scopes) == 0 {
		c.OAuth.Scopes = []string{"IdentifyAppliance", "Hob-Monitor", "Hob-Control", "Hob-Settings"}
	}
	if c.Appliance.TopicSlug == "" {
		c.Appliance.TopicSlug = "cooktop"
	}
	if c.Appliance.FriendlyID == "" {
		c.Appliance.FriendlyID = "Cooktop"
	}
	if c.MQTT.Port == 0 {
		if c.MQTT.TLS {
			c.MQTT.Port = 8883
		} else {
			c.MQTT.Port = 1883
		}
	}
	if c.MQTT.ClientID == "" {
		// Host-specific by default: the session is persistent, so two
		// bridges sharing one client ID (e.g. a dev run with a copied
		// config) would kick each other off the broker in a loop.
		host, _ := os.Hostname()
		if host == "" {
			host = "unknown"
		}
		c.MQTT.ClientID = "hcbridge-" + host
	}
	if c.MQTT.DiscoveryTopic == "" {
		c.MQTT.DiscoveryTopic = "homeassistant"
	}
	if c.LogLevel == "" {
		c.LogLevel = "INFO"
	}

	if err := c.validate(); err != nil {
		return nil, err
	}
	// mqtt.host is only required in --mode=run; watch/auth/list-appliances
	// don't need a broker. Validation happens in main.go for the run path.
	return c, nil
}

func (c *Config) validate() error {
	var errs []string
	if c.OAuth.ClientID == "" {
		errs = append(errs, "oauth.client_id is required")
	}
	if c.OAuth.Region != "global" && c.OAuth.Region != "cn" {
		errs = append(errs, fmt.Sprintf("oauth.region must be global or cn, got %q", c.OAuth.Region))
	}
	if !topicSlugRe.MatchString(c.Appliance.TopicSlug) {
		errs = append(errs, fmt.Sprintf("appliance.topic_slug must match %s, got %q", topicSlugRe, c.Appliance.TopicSlug))
	}
	if c.MQTT.Port < 1 || c.MQTT.Port > 65535 {
		errs = append(errs, fmt.Sprintf("mqtt.port out of range: %d", c.MQTT.Port))
	}
	if c.MQTT.CAFile != "" && !c.MQTT.TLS {
		errs = append(errs, "mqtt.ca_file is set but mqtt.tls is false")
	}
	if strings.ContainsAny(c.MQTT.DiscoveryTopic, "+#") {
		errs = append(errs, fmt.Sprintf("mqtt.discovery_topic must not contain wildcards, got %q", c.MQTT.DiscoveryTopic))
	}
	switch strings.ToUpper(c.LogLevel) {
	case "DEBUG", "INFO", "WARN", "WARNING", "ERROR":
	default:
		errs = append(errs, fmt.Sprintf("log_level must be DEBUG, INFO, WARN or ERROR, got %q", c.LogLevel))
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (c *Config) APIBase() string {
	if c.apiBase != "" {
		return c.apiBase
	}
	if c.OAuth.Region == "cn" {
		return "https://api.home-connect.cn"
	}
	return "https://api.home-connect.com"
}

// StateDir holds the bridge's runtime state: the token, the persisted API
// call log and the liveness file. It is the token file's directory.
func (c *Config) StateDir() string {
	return filepath.Dir(c.OAuth.TokenFile)
}

func (c *Config) StateTopic(suffix string) string {
	return fmt.Sprintf("homeconnect/%s/%s", c.Appliance.TopicSlug, suffix)
}
