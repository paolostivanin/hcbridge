package main

import (
	"strings"
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	c, err := parseConfig([]byte(testConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if c.MQTT.Port != 1883 || c.Appliance.TopicSlug != "cooktop" || c.StateDir() != "/var/lib/hcbridge" || c.Link.PowerCycleAfter != 30*time.Minute {
		t.Errorf("defaults: port=%d slug=%q state=%q power_cycle_after=%s", c.MQTT.Port, c.Appliance.TopicSlug, c.StateDir(), c.Link.PowerCycleAfter)
	}
	if !strings.HasPrefix(c.MQTT.ClientID, "hcbridge-") {
		t.Errorf("client id %q should be host-specific", c.MQTT.ClientID)
	}
	tlsCfg, err := parseConfig([]byte(testConfigBase + "mqtt:\n  tls: true\n"))
	if err != nil || tlsCfg.MQTT.Port != 8883 {
		t.Errorf("tls default port: %v %d", err, tlsCfg.MQTT.Port)
	}
}

func TestConfigRejects(t *testing.T) {
	cases := map[string]string{
		"unknown key":           testConfigBase + "redirect_uri: y\n",
		"missing host":          "appliance:\n  profile: /p.zip\n",
		"missing profile":       "appliance:\n  host: 192.0.2.1\n",
		"host with scheme":      "appliance:\n  host: ws://192.0.2.1\n  profile: /p.zip\n",
		"wildcard slug":         testConfigBase + "  topic_slug: '+'\n",
		"slash slug":            testConfigBase + "  topic_slug: a/b\n",
		"bad port":              testConfigBase + "mqtt:\n  port: 70000\n",
		"ca without tls":        testConfigBase + "mqtt:\n  ca_file: /x.pem\n",
		"bad log level":         testConfigBase + "log_level: LOUD\n",
		"power cycle too short": testConfigBase + "link:\n  power_cycle_after: 10s\n",
	}
	for name, y := range cases {
		if _, err := parseConfig([]byte(y)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A 1.x / cloud config gets a clear message, not just "unknown field".
func TestConfigRejectsOAuthBlock(t *testing.T) {
	_, err := parseConfig([]byte("oauth:\n  client_id: x\n" + testConfigBase))
	if err == nil || !strings.Contains(err.Error(), "appliance.host") {
		t.Fatalf("got %v", err)
	}
}

// The production config shape must load.
func TestConfigProductionShape(t *testing.T) {
	y := `appliance:
  host: "192.168.30.23"
  profile: "/etc/hcbridge/profile/BOSCH-PXX801D67E-68A40EB28949.zip"
  ha_id: "BOSCH-PXX801D67E-68A40EB28949"
  topic_slug: "cooktop"
  friendly_id: "Cooktop"
mqtt:
  host: "192.168.10.20"
  port: 1883
  username: "u"
  password: "p"
  client_id: hcbridge
  discovery_topic: homeassistant
link:
  power_cycle_after: 30m
state_dir: /var/lib/hcbridge
logfile: "/var/log/hcbridge/hcbridge.log"
log_level: "INFO"
`
	c, err := parseConfig([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	if c.MQTT.ClientID != "hcbridge" || c.Appliance.Host != "192.168.30.23" {
		t.Errorf("explicit values overridden: %q %q", c.MQTT.ClientID, c.Appliance.Host)
	}
}
