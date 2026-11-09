# hcbridge

Tiny single-binary bridge from the Home Connect cloud API to MQTT, written in Go.
Version 2.0.0 (`hcbridge --version`).

Built specifically for a Bosch induction cooktop with integrated ventilation
(PXX801D67E). The Home Connect public API exposes very little for hobs - by
design, for safety - so the goal here is "just enough" rather than feature
parity with the official `home_connect` HA integration. The bridge is
read-only towards the appliance: there are no MQTT command topics.

## What you get in Home Assistant (via MQTT discovery)

Every entity pins its entity ID with `default_entity_id`, so the IDs below
survive a re-registration. Appliance data entities use two availability
topics (the bridge's last will and the cloud link): they show `unavailable`,
not frozen values, while the bridge cannot see the hob.

Appliance data:
- `sensor.cooktop_operation_state` - Inactive / Ready / Run / Pause / Finished / ...
- `sensor.cooktop_power_state` - On / Off / Standby
- `sensor.cooktop_active_program`
- `sensor.cooktop_program_duration`, `_elapsed_program_time`, `_program_progress`
- `sensor.cooktop_program_timer` - RemainingProgramTime as a wall-clock deadline
  (HA renders a live countdown)
- `binary_sensor.cooktop_child_lock`
- `sensor.cooktop_focused_zone` - `FrontLeft / FrontRight / RearLeft / RearRight`,
  the zone the panel last had focus on, refreshed on every options poll
- `sensor.cooktop_front_left_zone` / `_front_right_zone` / `_rear_left_zone`
  / `_rear_right_zone` - per-zone "last observed" power: `off`, `1`, `1.5`,
  `2`, ..., `9`, or a raw level name such as `Boost2`
- `binary_sensor.cooktop_left_zones_joined` / `_right_zones_joined` - flex
  pairs merged

Diagnostics (bridge and link health; only the bridge's last will gates them,
so they stay visible while the cloud link is down):
- `binary_sensor.cooktop_cloud_connection` - live event stream (goes off after
  90 s without one, which also makes the data entities unavailable)
- `binary_sensor.cooktop_appliance_connected` - the hob is connected to Home
  Connect
- `binary_sensor.cooktop_api_rate_limited` / `sensor.cooktop_api_rate_limit_clears`
  - Bosch's quota block and when it ends
- `sensor.cooktop_api_calls_24h` - the bridge's own rolling 24 h request count
- `sensor.cooktop_bridge_heartbeat` - refreshed every 5 min by the main loop;
  alert on it going stale (a wedged process keeps its MQTT connection, so the
  last will alone does not catch it)
- `binary_sensor.cooktop_local_control` - someone is touching the panel

Removed in 2.0.0 as unused (the bridge deletes their discovery configs and
retained topics on connect): the alarm clock number and sensor (the only MQTT
write path), remaining program time (the program timer replaces it), focused
zone power, focused "zones joined", remote start allowed.

Per-zone tracking: the bridge keeps a state map for all 4 zones, updated
whenever the panel's focus shifts (every panel touch), and resets it to `off`
when PowerState=Off. While a pair is flex-joined, the focused half's power is
mirrored onto its partner (Home Connect reports only the focused half); the
mirror clears on un-join.

## What's *not* possible

Confirmed by direct API probing on PXX801D67E (firmware May 2026):

- **Integrated venting / fan**: not exposed via the public Home Connect API.
  All five direct probes against `/settings/Cooking.{Hob,Hood,Common}.Setting.*`
  returned `409 SDK.Error.UnsupportedSetting`. The docs model venting as a hood
  program option; `--mode=dump` also probes that path.
- **Per-zone power writes / remote-start**: `/programs/active` is `access: read`.
  You cannot start a zone or change a running zone's power level from the API.
- **All four zones in a single API call**: only the most-recently-focused
  zone is visible (`Cooking.Hob.Option.ZoneSelector` returns one zone at a
  time). The bridge works around this with the "last observed" map above -
  accurate as long as the user touches each zone before changing it (which is
  normal panel use).
- **Per-zone push events**: Bosch sends program-level option changes
  (`Duration`, `RemainingProgramTime`, `ProgramProgress`) over SSE, but **not**
  `ZoneSelector` / `PowerLevel` / `JoinZone`, so the bridge polls the program
  options (see "Polling").
- **Per-zone *timers***: the API exposes only one program-level
  `RemainingProgramTime`. The bridge publishes a single deadline; per-zone
  deadlines would be fiction.
- **Modifying running zones**: PUT to `/programs/active/options/...` is
  technically allowed by the API, but the hob displays an "Accept change?"
  prompt requiring physical panel confirmation. `--mode=put` exists for
  experiments; nothing is exposed in HA.

## Polling

A zone only changes after something SSE does report, so polling follows those
edges:

- **Edge-triggered** on `LocalControlActive=true` (panel touched),
  `ActiveProgram` set, and `ProgramFinished`/`ProgramAborted` (a zone timer
  expiring), debounced to one poll per 5 s.
- **2 s fast** while `LocalControlActive=true`, to catch each zone the user
  taps in turn; capped at 90 s per activation so a lost "released" event cannot
  leave it polling indefinitely.
- **Adaptive periodic**: every 30 s for 3 minutes after any SSE edge, then
  every 3 minutes, only while PowerState is On and the hob is connected. A 6 h
  simmer costs ~120 periodic calls (it was ~720 at a fixed 30 s).
- **Endpoint**: `/programs/active/options` while a program runs
  (Run/Pause/ActionRequired/DelayedStart), otherwise `/programs/selected/options`,
  which keeps the zone state after a per-zone timer expires (`PowerLevel=Off`).
  One call per poll.
- A poll whose response lands after PowerState=Off is dropped, so it cannot
  relight the zone tiles of a switched-off hob.

At each power-off the log gets one INFO line with the polls per trigger and how
many changed a value, for tuning the schedule.

## API budget

Home Connect limits (https://api-docs.home-connect.com/general/#rate-limiting):
1000 calls per day over a rolling window, 50 per minute, and a 10-minute block
after 10 successive error responses; every request counts, successful or not.
When the daily quota is reached, everything (SSE reconnects included) is
rejected for up to a day: the 429 body says "blocked during the remaining
period of N seconds", with N seen as high as 81000. Rejected calls do not
appear to count.

The bridge counts its own requests (REST + SSE connects) and responses and
degrades before Bosch blocks it:

| State | Behaviour |
|---|---|
| < 700 calls in 24 h | normal polling (see "Polling"), resync every 6 h |
| 700-899 calls in 24 h | periodic poll at most every 2 min, no fast poll |
| >= 900 calls in 24 h, >= 40 calls in the last minute, or 8 errors in a row | no discretionary polls or periodic resyncs; the reserve is kept for SSE reconnects and their (throttled) resyncs |

The call log and any active block are saved to `/var/lib/hcbridge/calls.json`
every 5 minutes and on shutdown, so a restart neither forgets the count nor
retries through a known block. When a 429 does arrive (REST or SSE), the
announced period is honoured and no SSE reconnect is attempted until it ends.
`--mode=dump` (about 15 calls) and `--mode=watch` are not added to the
service's count.

## Resilience

- **SSE**: exponential backoff 5 s -> 60 s across consecutive failures (10 min
  ceiling at >= 900 calls), reset only after a session that stayed up for 60 s,
  so Bosch's routine ~24 h stream close reconnects after 5 s but a stream that
  connects and drops at once keeps backing off. A stream silent for 120 s (two
  missed KEEP-ALIVEs) is dropped and reconnected.
- **Resync**: after each established SSE connect, unless one succeeded in the
  last 5 minutes (this also retries a startup resync that failed); when the
  appliance reconnects to Home Connect; and every 6 hours. Snapshot keys that
  an SSE event updated while the snapshot was in flight keep the event value.
- **Appliance offline**: a 409 from `/status` or an SSE DISCONNECTED marks it
  offline; polling pauses until it reconnects.
- **Token**: refreshed 5 minutes before expiry, one refresh at a time; a 401
  forces a refresh. If a refresh fails while the current token is still valid,
  the current token is used. After the token endpoint rejects a refresh,
  refreshes pause for 15 minutes (Bosch allows 100 a day). The token file is
  written via a private temp file, fsynced, and renamed.
- **MQTT**: auto-reconnect with a last will (`connected = offline`); the full
  retained state is republished on every reconnect, and a publish that was not
  acknowledged is retried on the next update. The first broker connection may
  take as long as it needs (no exit on a boot race).
- **Process**: under OpenRC `supervise-daemon` (see `openrc/hcbridge`) the
  bridge is restarted if it exits, and the healthcheck restarts it if the
  main loop stops refreshing `/var/lib/hcbridge/alive`.
- **Logs**: HTTP bodies are logged as at most 300 quoted bytes, never the body
  of a successful token response. lumberjack rotation: 5 MB x 7 backups x 14
  days, gzip.

## Setup

1. **Register an app** at https://developer.home-connect.com/
   - OAuth flow: **Device Flow**
   - Client type: Single Key
   - Copy the Client ID

2. **Build** (Go toolchain pinned in `go.mod`):
   ```sh
   git clone <this repo>
   cd hcbridge
   make release      # tests, refuses a dirty tree, prints version + sha256
   ```
   The binary is static (`CGO_ENABLED=0`), so a build from any distro runs on
   Alpine (musl). Deploy only release builds: `hcbridge --version` must not
   say `-dirty`.

3. **Install** (LXC / Linux host). The config holds the MQTT password: make it
   readable by the service but owned by root, so the service cannot rewrite it.

   On **Alpine** (OpenRC):
   ```sh
   addgroup -S hcbridge
   adduser -S -D -H -s /sbin/nologin -G hcbridge hcbridge
   install -d -m 0755 /opt/hcbridge /etc/hcbridge
   install -d -m 0700 -o hcbridge -g hcbridge /var/lib/hcbridge /var/log/hcbridge
   install -m 0755 hcbridge /opt/hcbridge/
   install -m 0640 -o root -g hcbridge config.example.yaml /etc/hcbridge/config.yaml
   install -m 0755 openrc/hcbridge /etc/init.d/hcbridge
   ```
   `logfile: /var/log/hcbridge/hcbridge.log` in the config (there is no
   journald); stdout/stderr land in `/var/log/hcbridge/std{out,err}.log`.

   On **Debian** (systemd):
   ```sh
   sudo useradd --system --no-create-home --shell /usr/sbin/nologin hcbridge
   sudo install -d -m 0755 /opt/hcbridge /etc/hcbridge
   sudo install -m 0755 hcbridge /opt/hcbridge/
   sudo install -m 0640 -o root -g hcbridge config.example.yaml /etc/hcbridge/config.yaml
   sudo install -m 0644 hcbridge.service /etc/systemd/system/
   sudo systemctl daemon-reload
   ```
   The unit creates `/var/lib/hcbridge` and `/var/log/hcbridge` itself.

4. **Configure**: edit `/etc/hcbridge/config.yaml` - at minimum `oauth.client_id`
   and `mqtt.host` - then check it:
   ```sh
   /opt/hcbridge/hcbridge --mode=check-config --config=/etc/hcbridge/config.yaml
   ```
   Unknown keys are errors.

5. **Authorize** (one-time, interactive, as the service user - the CLI modes
   refuse to run as anyone but the token file's owner, so a refresh can never
   leave a token the service cannot read):
   ```sh
   sudo -u hcbridge /opt/hcbridge/hcbridge --mode=auth --config=/etc/hcbridge/config.yaml
   ```
   Open the printed URL, enter the code, approve. The token lands in
   `/var/lib/hcbridge/token.json` (0600).

6. **Find your appliance ID**:
   ```sh
   sudo -u hcbridge /opt/hcbridge/hcbridge --mode=list-appliances --config=/etc/hcbridge/config.yaml
   ```
   Copy the `haId` for your hob into `appliance.ha_id`.

7. **Start**: `rc-update add hcbridge default && rc-service hcbridge start`
   (Alpine) or `systemctl enable --now hcbridge` (Debian). The log's first line
   is the version.

## Upgrading a running install (Alpine)

1. Build with `make release` and note the sha256. Upgrade while the hob is off
   and the bridge is not rate-limited.
2. Copy the binary next to the old one (`/opt/hcbridge/hcbridge.new`, mode
   0755) and compare the sha256.
3. Validate the live config with the new binary:
   `/opt/hcbridge/hcbridge.new --mode=check-config --config=/etc/hcbridge/config.yaml`
4. Keep the old binary and swap atomically (never copy over a running binary):
   `ln -f /opt/hcbridge/hcbridge /opt/hcbridge/hcbridge.prev && mv -f /opt/hcbridge/hcbridge.new /opt/hcbridge/hcbridge`
5. `rc-service hcbridge restart`, then check the log: version line, MQTT
   connected, SSE connected, startup resync without WARN.
6. Rollback: `mv -f /opt/hcbridge/hcbridge.prev /opt/hcbridge/hcbridge && rc-service hcbridge restart`.
   Leave `token.json` alone (a refresh may have rotated it; the format is
   unchanged).

## Modes

- `run` (default) - the service.
- `check-config` - load and validate the config, print a summary, exit.
- `auth` - OAuth device flow.
- `list-appliances` - print the account's appliances.
- `watch` - the same code path as `run` without MQTT: prints `[SSE]` events,
  `[PUB]` publishes and `[POLL]` lines. Home Connect allows one event stream
  per appliance and user, so running it next to the service can disconnect the
  service's stream.
- `dump` - GET every interesting endpoint and print the raw JSON (~15 calls).
- `put` - one PUT (`--key`, `--value` as JSON, `--target setting|active-option|selected-option`).
- `--version` - print version, VCS revision and toolchain.

## Testing

`make test` runs vet plus the unit tests under the race detector; `make check`
adds staticcheck and govulncheck. The end-to-end test drives the real bridge
and controller against a fake Home Connect API and a real broker:

```sh
docker run --rm -d --name hcb-mqtt -p 127.0.0.1:18831:1883 eclipse-mosquitto mosquitto -c /mosquitto-no-auth.conf
HCBRIDGE_E2E_BROKER=127.0.0.1:18831 go test -tags e2e -race -run E2E -v ./...
docker stop hcb-mqtt
```

## Verify with MQTT

```sh
mosquitto_sub -h <broker> -t 'homeconnect/cooktop/#' -v
```
Toggle the hob locally; state topics update within 1-2 seconds.

## Topic conventions

State (retained, published by the bridge):
- `homeconnect/cooktop/connected` - `online` | `offline` (last will)
- `homeconnect/cooktop/cloud` - `online` | `offline` (live event stream)
- `homeconnect/cooktop/status/operation_state`
- `homeconnect/cooktop/status/power_state`
- `homeconnect/cooktop/status/active_program`
- `homeconnect/cooktop/status/child_lock` - `true` | `false`
- `homeconnect/cooktop/status/local_control` - `true` | `false`
- `homeconnect/cooktop/status/program_duration`
- `homeconnect/cooktop/status/elapsed_program_time`
- `homeconnect/cooktop/status/program_progress`
- `homeconnect/cooktop/status/program_timer_deadline` - RFC3339 or empty
- `homeconnect/cooktop/status/focused_zone`
- `homeconnect/cooktop/status/appliance_online` - `true` | `false`
- `homeconnect/cooktop/status/rate_limited` - `true` | `false`
- `homeconnect/cooktop/status/rate_limited_until` - RFC3339 or empty
- `homeconnect/cooktop/status/api_calls_24h` - integer
- `homeconnect/cooktop/status/heartbeat` - RFC3339
- `homeconnect/cooktop/zone/{front_left,front_right,rear_left,rear_right}/power`
- `homeconnect/cooktop/joined_left` / `joined_right` - `true` | `false`

Events (not retained):
- `homeconnect/cooktop/event/alarm_clock_elapsed`
- `homeconnect/cooktop/event/program_finished`
- `homeconnect/cooktop/event/program_aborted`

There are no command topics.

## Security notes

- MQTT supports TLS (`mqtt.tls: true`, optional `mqtt.ca_file`, TLS 1.2+).
  On a plain-TCP broker, restrict the bridge's user with an ACL to
  `homeconnect/<slug>/#` and `homeassistant/+/hcbridge_<slug>/#`.
- The MQTT client ID defaults to `hcbridge-<hostname>`; it must be unique per
  running bridge because the session is persistent.
