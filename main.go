package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	configPath := flag.String("config", "/etc/hcbridge/config.yaml", "Path to config file")
	mode := flag.String("mode", "run", "Mode: run | auth | list-appliances | watch | dump | put")
	putKey := flag.String("key", "", "Key for --mode=put (e.g. BSH.Common.Setting.ChildLock)")
	putValue := flag.String("value", "", "JSON value for --mode=put (e.g. true | 60 | '\"BSH.Common.EnumType.PowerState.Off\"')")
	putTarget := flag.String("target", "setting", "PUT target for --mode=put: setting | active-option | selected-option")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(2)
	}

	log := NewLogger(cfg.Logfile, cfg.LogLevel)
	auth := NewAuthenticator(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch *mode {
	case "auth":
		if err := auth.DeviceFlow(ctx); err != nil {
			log.Error("device flow: %v", err)
			os.Exit(1)
		}
		return

	case "list-appliances":
		if err := auth.Load(); err != nil {
			log.Error("load token: %v (run --mode=auth first)", err)
			os.Exit(1)
		}
		api := NewAPIClient(cfg, auth)
		list, err := api.ListAppliances(ctx)
		if err != nil {
			log.Error("list appliances: %v", err)
			os.Exit(1)
		}
		fmt.Println("haId\ttype\tbrand\tvib\tenumber\tconnected")
		for _, line := range list {
			fmt.Println(line)
		}
		return

	case "watch":
		if err := auth.Load(); err != nil {
			log.Error("load token: %v (run --mode=auth first)", err)
			os.Exit(1)
		}
		if cfg.Appliance.HaID == "" {
			log.Error("appliance.ha_id is empty (run --mode=list-appliances to find it)")
			os.Exit(2)
		}
		runWatch(ctx, cfg, auth, log)
		return

	case "dump":
		if err := auth.Load(); err != nil {
			log.Error("load token: %v (run --mode=auth first)", err)
			os.Exit(1)
		}
		if cfg.Appliance.HaID == "" {
			log.Error("appliance.ha_id is empty (run --mode=list-appliances to find it)")
			os.Exit(2)
		}
		runDump(ctx, cfg, auth, log)
		return

	case "put":
		if err := auth.Load(); err != nil {
			log.Error("load token: %v (run --mode=auth first)", err)
			os.Exit(1)
		}
		if cfg.Appliance.HaID == "" || *putKey == "" || *putValue == "" {
			log.Error("--mode=put requires appliance.ha_id in config plus --key and --value")
			os.Exit(2)
		}
		var typed interface{}
		if err := json.Unmarshal([]byte(*putValue), &typed); err != nil {
			log.Error("--value must be valid JSON (e.g. true, 60, \"BSH.Common.EnumType.PowerState.Off\"): %v", err)
			os.Exit(2)
		}
		api := NewAPIClient(cfg, auth)
		c, ccancel := context.WithTimeout(ctx, 30*time.Second)
		defer ccancel()
		var perr error
		var pathLabel string
		switch *putTarget {
		case "setting":
			pathLabel = "/settings/"
			perr = api.PutSetting(c, cfg.Appliance.HaID, *putKey, typed)
		case "active-option":
			pathLabel = "/programs/active/options/"
			perr = api.PutActiveOption(c, cfg.Appliance.HaID, *putKey, typed)
		case "selected-option":
			pathLabel = "/programs/selected/options/"
			perr = api.PutSelectedOption(c, cfg.Appliance.HaID, *putKey, typed)
		default:
			log.Error("unknown --target %q (use setting | active-option | selected-option)", *putTarget)
			os.Exit(2)
		}
		if perr != nil {
			log.Error("PUT failed: %v", perr)
			os.Exit(1)
		}
		fmt.Printf("OK: PUT %s%s = %s\n", pathLabel, *putKey, *putValue)
		return

	case "run":
		// fall through
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(2)
	}

	if err := auth.Load(); err != nil {
		log.Error("load token: %v (run --mode=auth first)", err)
		os.Exit(1)
	}
	if cfg.Appliance.HaID == "" {
		log.Error("appliance.ha_id is empty (run --mode=list-appliances to find it)")
		os.Exit(2)
	}
	if cfg.MQTT.Host == "" {
		log.Error("mqtt.host is required for --mode=run (use --mode=watch to test without MQTT)")
		os.Exit(2)
	}

	api := NewAPIClient(cfg, auth)
	bridge := NewBridge(cfg, api, log, cfg.Appliance.HaID)
	if err := bridge.Connect(ctx); err != nil {
		log.Error("MQTT: %v", err)
		os.Exit(1)
	}
	defer bridge.Disconnect()

	resync := func(reason string) {
		log.Info("snapshot resync (%s)", reason)
		c, ccancel := context.WithTimeout(ctx, 30*time.Second)
		defer ccancel()
		snap, err := api.FetchAll(c, cfg.Appliance.HaID)
		if err != nil {
			log.Warn("snapshot resync failed: %v", err)
			return
		}
		bridge.ApplySnapshot(snap)
		// FetchAll doesn't update per-zone state (different parsing),
		// so trigger a focused poll right after.
		bridge.PollActiveOptions(c)
	}
	resync("startup")

	// Debounce successive polls: Home Connect's 1000-call/day quota is
	// strict, so we never poll more than once per 5 seconds even if many
	// LocalControlActive transitions arrive in a burst.
	var pollMu sync.Mutex
	var lastPollAt time.Time
	pollNow := func(reason string) {
		pollMu.Lock()
		if time.Since(lastPollAt) < 5*time.Second {
			pollMu.Unlock()
			return
		}
		lastPollAt = time.Now()
		pollMu.Unlock()
		log.Debug("PollActiveOptions (%s)", reason)
		pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
		bridge.PollActiveOptions(pctx)
		pcancel()
	}

	// Fast-poll loop, active only while LocalControlActive=true. Bosch's
	// /programs/active/options returns only the currently-focused zone, so
	// a user touching FL→FR→RR within a few seconds will only have the last
	// zone captured at the 30s periodic cadence. Polling every 2s during
	// the active interaction window catches each focused zone in turn.
	// Cost is bounded: LCA is true only while the panel is being touched
	// (~20-30s per cooking session), so this adds ~10-15 polls per session.
	var fastPollMu sync.Mutex
	var fastPollCancel context.CancelFunc
	startFastPoll := func() {
		fastPollMu.Lock()
		defer fastPollMu.Unlock()
		if fastPollCancel != nil {
			return
		}
		var fpCtx context.Context
		fpCtx, fastPollCancel = context.WithCancel(ctx)
		go func() {
			t := time.NewTicker(2 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-fpCtx.Done():
					return
				case <-t.C:
				}
				log.Debug("PollActiveOptions (fast: LCA=true)")
				pctx, pcancel := context.WithTimeout(fpCtx, 10*time.Second)
				bridge.PollActiveOptions(pctx)
				pcancel()
			}
		}()
	}
	stopFastPoll := func() {
		fastPollMu.Lock()
		defer fastPollMu.Unlock()
		if fastPollCancel != nil {
			fastPollCancel()
			fastPollCancel = nil
		}
	}
	isFastPolling := func() bool {
		fastPollMu.Lock()
		defer fastPollMu.Unlock()
		return fastPollCancel != nil
	}

	sse := NewSSEClient(cfg, auth, log)
	go sse.Run(ctx, cfg.Appliance.HaID, func(ev SSEEvent) {
		if ev.Event == "RECONNECTED" {
			resync("sse reconnect")
			return
		}
		bridge.ApplyEvent(ev)
		// Smart trigger: poll (debounced) on edges that imply the focused
		// zone may have changed or that cooking just started — both leave
		// the per-zone map stale until the next periodic POLL otherwise.
		// LCA edges also drive the fast-poll loop on/off.
		for _, it := range ev.Items {
			switch it.Key {
			case "BSH.Common.Status.LocalControlActive":
				if v, ok := it.Value.(bool); ok {
					if v {
						go pollNow("LocalControlActive=true")
						startFastPoll()
					} else {
						stopFastPoll()
					}
				}
			case "BSH.Common.Root.ActiveProgram":
				if it.Value != nil {
					go pollNow("ActiveProgram set")
				}
			case "BSH.Common.Setting.PowerState":
				// Hob off → no point fast-polling an inactive appliance,
				// even if LCA hasn't transitioned to false yet.
				if s, ok := it.Value.(string); ok && strings.HasSuffix(s, ".Off") {
					stopFastPoll()
				}
			case "BSH.Common.Event.ProgramFinished", "BSH.Common.Event.ProgramAborted":
				// A program ending means Bosch has turned a zone off
				// physically, but SSE doesn't carry that PowerLevel change.
				// Poll now to refresh per-zone state instead of waiting up
				// to 30s for the next periodic. Filter on .Present so we
				// fire on the event, not the 10s-later .Off clear.
				if s, ok := it.Value.(string); ok && strings.HasSuffix(s, ".Present") {
					go pollNow("ProgramFinished")
				}
			}
		}
	})

	// Per-zone polling: every 30s while the hob is on AND fast-poll is not
	// active. With the 5-second debounce above, an LCA-triggered poll won't
	// be quickly followed by a periodic one. Daily cost during a 2h cooking
	// session: ~240 polls + ~50 LCA-triggered = ~290 calls — well under
	// 1000/day. Fast-poll adds another ~10-15 per session.
	zoneTicker := time.NewTicker(30 * time.Second)
	defer zoneTicker.Stop()

	// Periodic full refresh as a safety net (drift correction + token
	// health). 30 min × 3 calls/resync = 144 calls/day.
	resyncTicker := time.NewTicker(30 * time.Minute)
	defer resyncTicker.Stop()

	// Surface 429 state to HA. publish() dedups, so this only generates
	// MQTT traffic on transitions (no/from blocked). Costs nothing API-side.
	rateLimitTicker := time.NewTicker(30 * time.Second)
	defer rateLimitTicker.Stop()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-sigCh:
			log.Info("got signal %s, shutting down", sig)
			cancel()
			time.Sleep(500 * time.Millisecond)
			return
		case <-zoneTicker.C:
			if bridge.IsHobActive() && !isFastPolling() {
				pollNow("periodic")
			}
		case <-resyncTicker.C:
			resync("periodic")
		case <-rateLimitTicker.C:
			bridge.PublishRateLimit(IsRateLimited())
		}
	}
}

// runDump fetches every interesting endpoint for the configured appliance and
// prints raw JSON. Useful when something visible in the official app isn't
// surfacing through SSE, to figure out which key it's hiding under.
func runDump(ctx context.Context, cfg *Config, auth *Authenticator, log *Logger) {
	api := NewAPIClient(cfg, auth)
	base := "/api/homeappliances/" + cfg.Appliance.HaID
	paths := []string{
		"/api/homeappliances/" + cfg.Appliance.HaID,
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
		// 404 means "this hob doesn't expose the key"; 200 means we found it.
		base + "/settings/Cooking.Hob.Setting.Ventilation",
		base + "/settings/Cooking.Hood.Setting.VentingLevel",
		base + "/settings/Cooking.Common.Setting.Hood.VentingLevel",
		base + "/status/Cooking.Hob.Setting.Ventilation",
		base + "/status/Cooking.Hood.Setting.VentingLevel",
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

// runWatch is a no-MQTT debug mode that exercises the SAME bridge code path
// production uses, but routes MQTT publishes to stdout as [PUB] lines. Lets
// you verify per-zone tracking and topic shape before standing up a broker.
//
// Output legend:
//   [SSE]  one item arrived in an SSE event from Home Connect
//   [PUB]  the bridge would publish this MQTT topic in run mode
//   [POLL] periodic /programs/active/options fetch (5s while hob is on)
func runWatch(ctx context.Context, cfg *Config, auth *Authenticator, log *Logger) {
	api := NewAPIClient(cfg, auth)
	// Bridge with no MQTT client → publish() prints to stdout.
	bridge := NewBridge(cfg, api, log, cfg.Appliance.HaID)

	fmt.Println("=== snapshot ===")
	c, ccancel := context.WithTimeout(ctx, 30*time.Second)
	snap, err := api.FetchAll(c, cfg.Appliance.HaID)
	ccancel()
	if err != nil {
		log.Error("snapshot failed: %v", err)
		os.Exit(1)
	}
	keys := make([]string, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := snap[k]
		suffix, mapped := keyToSuffix[k]
		marker := "  "
		if !mapped {
			marker = "* "
			suffix = "(unmapped)"
		}
		fmt.Printf("%s%-50s -> %-32s = %v\n", marker, k, suffix, v)
	}
	fmt.Println("\n--- applying snapshot through bridge (will print [PUB] for each topic) ---")
	bridge.ApplySnapshot(snap)
	pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
	bridge.PollActiveOptions(pctx)
	pcancel()

	fmt.Println("\n=== streaming events (Ctrl-C to stop) ===")
	fmt.Println("    [SSE]=raw event from Home Connect | [PUB]=what bridge would publish to MQTT | [POLL]=/programs/active/options (30s periodic + 2s while LCA=true + edge-triggered)")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()

	// Fast-poll controller (see run mode for rationale): while LCA=true,
	// poll every 2s to capture each focused zone the user touches in turn.
	var fastPollMu sync.Mutex
	var fastPollCancel context.CancelFunc
	startFastPoll := func() {
		fastPollMu.Lock()
		defer fastPollMu.Unlock()
		if fastPollCancel != nil {
			return
		}
		var fpCtx context.Context
		fpCtx, fastPollCancel = context.WithCancel(wctx)
		go func() {
			t := time.NewTicker(2 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-fpCtx.Done():
					return
				case <-t.C:
				}
				fmt.Printf("[%s] [POLL]   /programs/active/options (fast: LCA=true)\n",
					time.Now().Format("15:04:05.000"))
				pc, pcc := context.WithTimeout(fpCtx, 10*time.Second)
				bridge.PollActiveOptions(pc)
				pcc()
			}
		}()
	}
	stopFastPoll := func() {
		fastPollMu.Lock()
		defer fastPollMu.Unlock()
		if fastPollCancel != nil {
			fastPollCancel()
			fastPollCancel = nil
		}
	}
	isFastPolling := func() bool {
		fastPollMu.Lock()
		defer fastPollMu.Unlock()
		return fastPollCancel != nil
	}

	// 30s periodic poll while hob is active. Paused while fast-poll is on
	// (LCA=true), since fast-poll already covers the active interaction.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-wctx.Done():
				return
			case <-t.C:
			}
			if !bridge.IsHobActive() || isFastPolling() {
				continue
			}
			if blocked, until := IsRateLimited(); blocked {
				fmt.Printf("[%s] [POLL]   skipped (rate-limited until %s)\n",
					time.Now().Format("15:04:05.000"), until.Format(time.RFC3339))
				continue
			}
			fmt.Printf("[%s] [POLL]   /programs/active/options (periodic)\n", time.Now().Format("15:04:05.000"))
			pc, pcc := context.WithTimeout(wctx, 10*time.Second)
			bridge.PollActiveOptions(pc)
			pcc()
		}
	}()

	sse := NewSSEClient(cfg, auth, log)
	go sse.Run(wctx, cfg.Appliance.HaID, func(ev SSEEvent) {
		ts := time.Now().Format("15:04:05.000")
		switch ev.Event {
		case "RECONNECTED":
			fmt.Printf("[%s] (RECONNECTED — would resync state in run mode)\n", ts)
			return
		case "CONNECTED", "DISCONNECTED", "PAIRED", "DEPAIRED":
			fmt.Printf("[%s] %s\n", ts, ev.Event)
			return
		}
		// Print raw items first (so you can see what's mapped vs unmapped),
		// then let bridge.ApplyEvent do its thing — which will print [PUB]
		// lines for any resulting topic publishes.
		for _, it := range ev.Items {
			suffix, mapped := keyToSuffix[it.Key]
			if !mapped {
				if _, evMap := eventKeyToSuffix[it.Key]; evMap {
					mapped = true
					suffix = eventKeyToSuffix[it.Key]
				}
			}
			marker := "  "
			if !mapped {
				marker = "* "
				suffix = "(unmapped)"
			}
			vJSON, _ := json.Marshal(it.Value)
			fmt.Printf("[%s] [SSE]    %s%-50s -> %-32s = %s\n",
				ts, marker, it.Key, suffix, string(vJSON))
		}
		bridge.ApplyEvent(ev)
		// Smart trigger: poll on LocalControlActive=true or ActiveProgram
		// set (matches run mode). Logged so the diagnostic shows which
		// edge fired the poll vs. the periodic ticker.
		triggerPoll := func(reason string) {
			go func() {
				fmt.Printf("[%s] [POLL]   /programs/active/options (%s)\n",
					time.Now().Format("15:04:05.000"), reason)
				pc, pcc := context.WithTimeout(wctx, 10*time.Second)
				defer pcc()
				bridge.PollActiveOptions(pc)
			}()
		}
		for _, it := range ev.Items {
			switch it.Key {
			case "BSH.Common.Status.LocalControlActive":
				if v, ok := it.Value.(bool); ok {
					if v {
						triggerPoll("LocalControlActive=true")
						startFastPoll()
					} else {
						stopFastPoll()
					}
				}
			case "BSH.Common.Root.ActiveProgram":
				if it.Value != nil {
					triggerPoll("ActiveProgram set")
				}
			case "BSH.Common.Setting.PowerState":
				if s, ok := it.Value.(string); ok && strings.HasSuffix(s, ".Off") {
					stopFastPoll()
				}
			case "BSH.Common.Event.ProgramFinished", "BSH.Common.Event.ProgramAborted":
				if s, ok := it.Value.(string); ok && strings.HasSuffix(s, ".Present") {
					triggerPoll("ProgramFinished")
				}
			}
		}
	})

	<-sigCh
	fmt.Println("\nbye")
}
