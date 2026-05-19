package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	OAuth struct {
		ClientID    string   `yaml:"client_id"`
		Scopes      []string `yaml:"scopes"`
		Region      string   `yaml:"region"`
		TokenFile   string   `yaml:"token_file"`
		RedirectURI string   `yaml:"redirect_uri"`
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
	} `yaml:"mqtt"`

	Logfile  string `yaml:"logfile"`
	LogLevel string `yaml:"log_level"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	c := &Config{}
	if err := yaml.Unmarshal(data, c); err != nil {
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
		c.MQTT.Port = 1883
	}
	if c.MQTT.ClientID == "" {
		c.MQTT.ClientID = "hcbridge"
	}
	if c.MQTT.DiscoveryTopic == "" {
		c.MQTT.DiscoveryTopic = "homeassistant"
	}
	if c.LogLevel == "" {
		c.LogLevel = "INFO"
	}

	if c.OAuth.ClientID == "" {
		return nil, fmt.Errorf("oauth.client_id is required")
	}
	// mqtt.host is only required in --mode=run; watch/auth/list-appliances
	// don't need a broker. Validation happens in main.go for the run path.
	return c, nil
}

func (c *Config) APIBase() string {
	if c.OAuth.Region == "cn" {
		return "https://api.home-connect.cn"
	}
	return "https://api.home-connect.com"
}

func (c *Config) StateTopic(suffix string) string {
	return fmt.Sprintf("homeconnect/%s/%s", c.Appliance.TopicSlug, suffix)
}

func (c *Config) CmdTopic(suffix string) string {
	return fmt.Sprintf("homeconnect/%s/cmd/%s", c.Appliance.TopicSlug, suffix)
}
