package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	configPath := flag.String("config", "/etc/hcbridge/config.yaml", "Path to config file")
	mode := flag.String("mode", "run", "Mode: run | check-config | local-watch | auth | list-appliances | watch | dump | put")
	raw := flag.Bool("raw", false, "local-watch: also print every decrypted frame")
	record := flag.String("record", "", "local-watch: append the received values as JSON lines to this file")
	putKey := flag.String("key", "", "Key for --mode=put (e.g. BSH.Common.Setting.ChildLock)")
	putValue := flag.String("value", "", "JSON value for --mode=put (e.g. true | 60 | '\"BSH.Common.EnumType.PowerState.Off\"')")
	putTarget := flag.String("target", "setting", "PUT target for --mode=put: setting | active-option | selected-option")
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
	if *mode == "check-config" {
		fmt.Printf("%s: OK (appliance %q, topic root homeconnect/%s, broker %s:%d, state dir %s)\n",
			*configPath, cfg.Appliance.HaID, cfg.Appliance.TopicSlug, cfg.MQTT.Host, cfg.MQTT.Port, cfg.StateDir())
		if cfg.Appliance.Profile != "" {
			prof, err := loadProfileFor(cfg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "profile: %v\n", err)
				os.Exit(2)
			}
			printProfileSummary(os.Stdout, prof, cfg.Appliance.Profile)
			fmt.Printf("hob address %s\n", cfg.Appliance.Host)
		}
		return
	}

	if *mode == "local-watch" {
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		if err := runLocalWatch(ctx, cfg, *raw, *record, os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "local-watch: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("\nbye")
		return
	}
	if cfg.OAuth.ClientID == "" {
		fmt.Fprintf(os.Stderr, "config: oauth.client_id is required for --mode=%s\n", *mode)
		os.Exit(2)
	}

	log := NewLogger(cfg.Logfile, cfg.LogLevel)
	auth := NewAuthenticator(cfg, log)
	quota := NewQuota()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// fatal logs to the log file and to stderr (captured by the service
	// manager), then exits.
	fatal := func(code int, format string, args ...interface{}) {
		log.Error(format, args...)
		fmt.Fprintf(os.Stderr, format+"\n", args...)
		os.Exit(code)
	}

	switch *mode {
	case "auth":
		if err := checkTokenOwner(cfg.OAuth.TokenFile); err != nil {
			fatal(2, "%v", err)
		}
		if err := auth.DeviceFlow(ctx); err != nil {
			fatal(1, "device flow: %v", err)
		}
		return
	case "list-appliances", "watch", "dump", "put", "run":
	default:
		fatal(2, "unknown mode: %s", *mode)
	}

	// Every remaining mode uses (and may refresh) the stored token.
	if err := checkTokenOwner(cfg.OAuth.TokenFile); err != nil {
		fatal(2, "%v", err)
	}
	if err := auth.Load(); err != nil {
		fatal(1, "load token: %v (run --mode=auth first)", err)
	}
	api := NewAPIClient(cfg, auth, quota)

	if *mode == "list-appliances" {
		list, err := api.ListAppliances(ctx)
		if err != nil {
			fatal(1, "list appliances: %v", err)
		}
		fmt.Println("haId\ttype\tbrand\tvib\tenumber\tconnected")
		for _, line := range list {
			fmt.Println(line)
		}
		return
	}

	if cfg.Appliance.HaID == "" {
		fatal(2, "appliance.ha_id is empty (run --mode=list-appliances to find it)")
	}

	switch *mode {
	case "dump":
		runDump(ctx, cfg, api)
		return
	case "put":
		runPut(ctx, cfg, api, log, *putKey, *putValue, *putTarget)
		return
	case "watch":
		fmt.Fprintln(os.Stderr, "note: Home Connect allows one event stream per appliance and user;"+
			" running watch next to the service can disconnect the service's stream.")
		// Respect the service's persisted budget, but never write it back.
		_ = quota.Load(filepath.Join(cfg.StateDir(), "calls.json"))
		bridge := NewBridge(cfg, api, log, cfg.Appliance.HaID) // no MQTT: prints [PUB] lines
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		fmt.Println("    [SSE]=raw event from Home Connect | [PUB]=what run mode would publish | [POLL]=options poll")
		newController(cfg, api, auth, quota, bridge, log, true).run(ctx, cancel, sigCh)
		fmt.Println("\nbye")
		return
	}

	// --mode=run
	if cfg.MQTT.Host == "" {
		fatal(2, "mqtt.host is required for --mode=run (use --mode=watch to test without MQTT)")
	}
	log.Info("%s starting", buildString())
	quotaPath := filepath.Join(cfg.StateDir(), "calls.json")
	if err := quota.Load(quotaPath); err != nil {
		log.Warn("load API call log %s: %v (counting from zero)", quotaPath, err)
	}
	if blocked, until := quota.Blocked(); blocked {
		log.Warn("persisted rate-limit block active until %s", until.Format(time.RFC3339))
	}
	log.Info("%d API calls in the last 24 h (persisted)", quota.Count24h())

	bridge := NewBridge(cfg, api, log, cfg.Appliance.HaID)
	if err := bridge.Connect(ctx); err != nil {
		fatal(1, "MQTT: %v", err)
	}
	defer bridge.Disconnect()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	newController(cfg, api, auth, quota, bridge, log, false).run(ctx, cancel, sigCh)
}

// checkTokenOwner refuses to run as a user other than the token file's
// owner. A refresh rewrites the file; run as root (e.g. a quick
// --mode=dump on the production box) it would leave a root-owned 0600 token
// the service can no longer read after its next restart.
func checkTokenOwner(path string) error {
	fi, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	if euid := os.Geteuid(); uint32(euid) != st.Uid {
		return fmt.Errorf("token file %s is owned by uid %d but this process runs as uid %d; "+
			"run as the owner (e.g. sudo -u hcbridge ...) so a token refresh cannot replace it "+
			"with a file the service cannot read", path, st.Uid, euid)
	}
	return nil
}

func runPut(ctx context.Context, cfg *Config, api *APIClient, log *Logger, key, value, target string) {
	if key == "" || value == "" {
		fmt.Fprintln(os.Stderr, "--mode=put requires --key and --value")
		os.Exit(2)
	}
	var typed interface{}
	if err := json.Unmarshal([]byte(value), &typed); err != nil {
		fmt.Fprintf(os.Stderr, "--value must be valid JSON (e.g. true, 60, \"BSH.Common.EnumType.PowerState.Off\"): %v\n", err)
		os.Exit(2)
	}
	c, ccancel := context.WithTimeout(ctx, 30*time.Second)
	defer ccancel()
	var perr error
	var pathLabel string
	switch target {
	case "setting":
		pathLabel = "/settings/"
		perr = api.PutSetting(c, cfg.Appliance.HaID, key, typed)
	case "active-option":
		pathLabel = "/programs/active/options/"
		perr = api.PutActiveOption(c, cfg.Appliance.HaID, key, typed)
	case "selected-option":
		pathLabel = "/programs/selected/options/"
		perr = api.PutSelectedOption(c, cfg.Appliance.HaID, key, typed)
	default:
		fmt.Fprintf(os.Stderr, "unknown --target %q (use setting | active-option | selected-option)\n", target)
		os.Exit(2)
	}
	if perr != nil {
		log.Error("PUT failed: %v", perr)
		fmt.Fprintf(os.Stderr, "PUT failed: %v\n", perr)
		os.Exit(1)
	}
	fmt.Printf("OK: PUT %s%s = %s\n", pathLabel, key, value)
}

// runDump fetches every interesting endpoint for the configured appliance and
// prints raw JSON. Useful when something visible in the official app isn't
// surfacing through SSE, to figure out which key it's hiding under. About 15
// API calls, which the service's call counter does not see.
func runDump(ctx context.Context, cfg *Config, api *APIClient) {
	base := "/api/homeappliances/" + cfg.Appliance.HaID
	paths := []string{
		base,
		base + "/status",
		base + "/settings",
		base + "/programs",
		base + "/programs/active",
		base + "/programs/active/options",
		base + "/programs/selected",
		base + "/programs/selected/options",
		base + "/programs/available",
		base + "/commands",

		// Direct probes for venting cooktop keys that may not appear in the
		// settings collection but might still be readable by direct ID.
		// 404/409 means "this hob doesn't expose the key"; 200 means we found it.
		base + "/settings/Cooking.Hob.Setting.Ventilation",
		base + "/settings/Cooking.Hood.Setting.VentingLevel",
		base + "/settings/Cooking.Common.Setting.Hood.VentingLevel",
		base + "/status/Cooking.Hob.Setting.Ventilation",
		base + "/status/Cooking.Hood.Setting.VentingLevel",
		// The documented model for venting is a hood program with a venting
		// level option, not a setting.
		base + "/programs/available/Cooking.Common.Program.Hood.Venting",
	}
	for _, p := range paths {
		fmt.Printf("\n========== GET %s ==========\n", p)
		c, ccancel := context.WithTimeout(ctx, 30*time.Second)
		body, code, err := api.RawGET(c, p)
		ccancel()
		fmt.Printf("status: %d\n", code)
		if err != nil {
			fmt.Printf("error:  %v\n", err)
			continue
		}
		// Pretty-print JSON if possible, otherwise raw.
		var pretty interface{}
		if json.Unmarshal(body, &pretty) == nil {
			out, _ := json.MarshalIndent(pretty, "", "  ")
			fmt.Println(string(out))
		} else {
			fmt.Println(string(body))
		}
	}
}
