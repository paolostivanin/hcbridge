package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Appliance struct {
		Host       string `yaml:"host"`    // the hob's IP or hostname, optional :port (default 80)
		Profile    string `yaml:"profile"` // profile ZIP (homeconnect-profile-downloader) or extracted directory
		HaID       string `yaml:"ha_id"`   // optional: must match the profile (selects one if several)
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

	Link struct {
		// PowerCycleAfter: how long the hob may refuse local connections
		// (HTTP 503) before power_cycle_needed turns on.
		PowerCycleAfter time.Duration `yaml:"power_cycle_after"`
	} `yaml:"link"`

	StateDirectory string `yaml:"state_dir"`

	Logfile  string `yaml:"logfile"`
	LogLevel string `yaml:"log_level"`
}

const defaultStateDir = "/var/lib/hcbridge"

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
		if strings.Contains(err.Error(), "field oauth not found") {
			return nil, fmt.Errorf("parse config: the oauth block is gone in 2.0.0 (the bridge talks to the hob locally); " +
				"remove it and set appliance.host and appliance.profile")
		}
		return nil, fmt.Errorf("parse config: %w", err)
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
	if c.Link.PowerCycleAfter == 0 {
		c.Link.PowerCycleAfter = 30 * time.Minute
	}
	if c.StateDirectory == "" {
		c.StateDirectory = defaultStateDir
	}
	if c.LogLevel == "" {
		c.LogLevel = "INFO"
	}

	if err := c.validate(); err != nil {
		return nil, err
	}
	// mqtt.host is only required in --mode=run (local-watch and
	// check-config need no broker); main checks it there.
	return c, nil
}

func (c *Config) validate() error {
	var errs []string
	switch h := c.Appliance.Host; {
	case h == "":
		errs = append(errs, "appliance.host is required (the hob's IP address)")
	case strings.Contains(h, "/") || strings.ContainsAny(h, " \t"):
		errs = append(errs, fmt.Sprintf("appliance.host must be an IP or hostname with an optional :port, got %q", h))
	}
	if c.Appliance.Profile == "" {
		errs = append(errs, "appliance.profile is required (the ZIP from homeconnect-profile-downloader)")
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
	if c.Link.PowerCycleAfter < time.Minute {
		errs = append(errs, fmt.Sprintf("link.power_cycle_after must be at least 1m, got %s", c.Link.PowerCycleAfter))
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

// StateDir holds the bridge's runtime state: the device ID, the lock, the
// last known device info and link state, and the liveness file.
func (c *Config) StateDir() string { return c.StateDirectory }

func (c *Config) StateTopic(suffix string) string {
	return fmt.Sprintf("homeconnect/%s/%s", c.Appliance.TopicSlug, suffix)
}
