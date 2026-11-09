package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	configPath := flag.String("config", "/etc/hcbridge/config.yaml", "Path to config file")
	mode := flag.String("mode", "run", "Mode: run | check-config | local-watch")
	raw := flag.Bool("raw", false, "local-watch: also print every decrypted frame")
	record := flag.String("record", "", "local-watch: append the received values as JSON lines to this file")
	showVersion := flag.Bool("version", false, "Print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(buildString())
		return
	}

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(2)
	}

	switch *mode {
	case "check-config":
		prof, err := loadProfileFor(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "profile: %v\n", err)
			os.Exit(2)
		}
		fmt.Printf("%s: OK (hob %s, topic root homeconnect/%s, broker %s:%d, state dir %s, power cycle flag after %s)\n",
			*configPath, cfg.Appliance.Host, cfg.Appliance.TopicSlug, cfg.MQTT.Host, cfg.MQTT.Port, cfg.StateDir(), cfg.Link.PowerCycleAfter)
		printProfileSummary(os.Stdout, prof, cfg.Appliance.Profile)
		return
	case "local-watch":
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		if err := runLocalWatch(ctx, cfg, *raw, *record, os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "local-watch: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("\nbye")
		return
	case "run":
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(2)
	}

	log := NewLogger(cfg.Logfile, cfg.LogLevel)
	// fatal logs to the log file and to stderr (captured by the service
	// manager), then exits.
	fatal := func(code int, format string, args ...interface{}) {
		log.Error(format, args...)
		fmt.Fprintf(os.Stderr, format+"\n", args...)
		os.Exit(code)
	}

	if cfg.MQTT.Host == "" {
		fatal(2, "mqtt.host is required for --mode=run (use --mode=local-watch to test without MQTT)")
	}
	log.Info("%s starting", buildString())

	dir := cfg.StateDir()
	if err := checkStateOwner(dir); err != nil {
		fatal(2, "%v", err)
	}
	release, err := acquireLock(dir)
	if err != nil {
		fatal(1, "%v", err)
	}
	defer release()
	prof, err := loadProfileFor(cfg)
	if err != nil {
		fatal(2, "profile: %v", err)
	}
	deviceID, created, err := loadOrCreateDeviceID(dir)
	if err != nil {
		fatal(1, "device ID: %v", err)
	}
	if created {
		log.Info("created device ID %s", deviceID)
	}
	meta, err := loadDeviceMeta(dir)
	if err != nil {
		log.Warn("read device.json: %v", err)
	}
	if meta.VIB == "" {
		meta.Brand, meta.VIB, meta.Serial = prof.Brand, prof.VIB, prof.Serial
	}
	log.Info("%s at %s, device ID %s", prof, cfg.Appliance.Host, deviceID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bridge := NewBridge(cfg, meta, log)
	if err := bridge.Connect(ctx); err != nil {
		fatal(1, "MQTT: %v", err)
	}
	defer bridge.Disconnect()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	newController(cfg, prof, deviceID, bridge, log).run(ctx, cancel, sigCh)
}
