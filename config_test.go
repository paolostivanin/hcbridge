package main

import (
	"strings"
	"testing"
)

func TestConfigDefaults(t *testing.T) {
	c, err := parseConfig([]byte("oauth:\n  client_id: x\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.MQTT.Port != 1883 || c.Appliance.TopicSlug != "cooktop" || c.OAuth.Region != "global" {
		t.Errorf("defaults: port=%d slug=%q region=%q", c.MQTT.Port, c.Appliance.TopicSlug, c.OAuth.Region)
	}
	if !strings.HasPrefix(c.MQTT.ClientID, "hcbridge-") {
		t.Errorf("client id %q should be host-specific", c.MQTT.ClientID)
	}
	if c.StateDir() != "/var/lib/hcbridge" {
		t.Errorf("state dir %q", c.StateDir())
	}
	tlsCfg, err := parseConfig([]byte("oauth:\n  client_id: x\nmqtt:\n  tls: true\n"))
	if err != nil || tlsCfg.MQTT.Port != 8883 {
		t.Errorf("tls default port: %v %d", err, tlsCfg.MQTT.Port)
	}
}

func TestConfigRejects(t *testing.T) {
	cases := map[string]string{
		"unknown key":       "oauth:\n  client_id: x\n  redirect_uri: y\n",
		"missing client id": "mqtt:\n  host: h\n",
		"wildcard slug":     "oauth:\n  client_id: x\nappliance:\n  topic_slug: '+'\n",
		"slash slug":        "oauth:\n  client_id: x\nappliance:\n  topic_slug: a/b\n",
		"bad port":          "oauth:\n  client_id: x\nmqtt:\n  port: 70000\n",
		"bad region":        "oauth:\n  client_id: x\n  region: eu\n",
		"ca without tls":    "oauth:\n  client_id: x\nmqtt:\n  ca_file: /x.pem\n",
		"bad log level":     "oauth:\n  client_id: x\nlog_level: LOUD\n",
	}
	for name, y := range cases {
		if _, err := parseConfig([]byte(y)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The production config shape must keep loading.
func TestConfigProductionShape(t *testing.T) {
	y := `oauth:
  client_id: "ABC"
  region: global
  token_file: /var/lib/hcbridge/token.json
appliance:
  ha_id: "BOSCH-X"
  topic_slug: "cooktop"
  friendly_id: "Cooktop"
mqtt:
  host: "192.168.10.20"
  port: 1883
  username: "u"
  password: "p"
  client_id: hcbridge
  discovery_topic: homeassistant
logfile: "/var/log/hcbridge/hcbridge.log"
log_level: "INFO"
`
	c, err := parseConfig([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	if c.MQTT.ClientID != "hcbridge" {
		t.Errorf("explicit client id overridden: %q", c.MQTT.ClientID)
	}
}
