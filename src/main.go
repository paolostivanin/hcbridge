package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	os.Exit(realMain(os.Args[1:], os.Stdout, os.Stderr))
}

// realMain is the whole program; it returns the exit code. Deferred
// cleanups (the lock, the MQTT disconnect) run before main exits.
func realMain(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("hcbridge", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "/etc/hcbridge/config.yaml", "Path to config file")
	mode := fs.String("mode", "run", "Mode: run | check-config | local-watch")
	raw := fs.Bool("raw", false, "local-watch: also print every decrypted frame")
	record := fs.String("record", "", "local-watch: append the received values as JSON lines to this file")
	showVersion := fs.Bool("version", false, "Print the version and exit")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if *showVersion {
		fmt.Fprintln(stdout, buildString())
		return 0
	}

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "config: %v\n", err)
		return 2
	}

	switch *mode {
	case "check-config":
		prof, err := loadProfileFor(cfg)
		if err != nil {
			fmt.Fprintf(stderr, "profile: %v\n", err)
			return 2
		}
		fmt.Fprintf(stdout, "%s: OK (hob %s, topic root homeconnect/%s, broker %s:%d, state dir %s, power cycle flag after %s)\n",
			*configPath, cfg.Appliance.Host, cfg.Appliance.TopicSlug, cfg.MQTT.Host, cfg.MQTT.Port, cfg.StateDir(), cfg.Link.PowerCycleAfter)
		printProfileSummary(stdout, prof, cfg.Appliance.Profile)
		return 0
	case "local-watch":
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		if err := runLocalWatch(ctx, cfg, *raw, *record, stdout); err != nil {
			fmt.Fprintf(stderr, "local-watch: %v\n", err)
			return 1
		}
		fmt.Fprintln(stdout, "\nbye")
		return 0
	case "run":
	default:
		fmt.Fprintf(stderr, "unknown mode: %s\n", *mode)
		return 2
	}

	log := NewLogger(cfg.Logfile, cfg.LogLevel)
	// fatal logs to the log file and to stderr (captured by the service
	// manager) and returns the exit code.
	fatal := func(code int, format string, args ...interface{}) int {
		log.Error(format, args...)
		fmt.Fprintf(stderr, format+"\n", args...)
		return code
	}

	if cfg.MQTT.Host == "" {
		return fatal(2, "mqtt.host is required for --mode=run (use --mode=local-watch to test without MQTT)")
	}
	log.Info("%s starting", buildString())

	dir := cfg.StateDir()
	if err := checkStateOwner(dir); err != nil {
		return fatal(2, "%v", err)
	}
	release, err := acquireLock(dir)
	if err != nil {
		return fatal(1, "%v", err)
	}
	defer release()
	prof, err := loadProfileFor(cfg)
	if err != nil {
		return fatal(2, "profile: %v", err)
	}
	deviceID, created, err := loadOrCreateDeviceID(dir)
	if err != nil {
		return fatal(1, "device ID: %v", err)
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
		return fatal(1, "MQTT: %v", err)
	}
	defer bridge.Disconnect()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	newController(cfg, prof, deviceID, bridge, log).run(ctx, cancel, sigCh)
	return 0
}
