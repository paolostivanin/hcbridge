# hcbridge

Tiny single-binary bridge from the Home Connect cloud API to MQTT, written in Go.

Built specifically for a Bosch induction cooktop with integrated ventilation
(PXX801D67E). The Home Connect public API exposes very little for hobs — by
design, for safety — so the goal here is "just enough" rather than feature
parity with the official `home_connect` HA integration.

## What you get in Home Assistant (via MQTT discovery)

Read-only sensors (real-time via SSE):
- `sensor.cooktop_operation_state` — Inactive / Ready / Run / Pause / Finished / …
- `sensor.cooktop_power_state` — On / Off / Standby
- `sensor.cooktop_alarm_clock_seconds`
- `sensor.cooktop_remaining_program_time`
- `sensor.cooktop_program_duration`, `_elapsed_program_time`, `_program_progress`
- `sensor.cooktop_active_program`
- `binary_sensor.cooktop_child_lock`
- `binary_sensor.cooktop_local_control` — someone is touching the panel
- `binary_sensor.cooktop_remote_start_allowed`
- `binary_sensor.cooktop_api_rate_limited` — true when the bridge has hit
  Bosch's 1000-call/day quota; SSE keeps flowing but polls and MQTT writes
  are suppressed until the limit clears
- `sensor.cooktop_api_rate_limit_clears` — RFC3339 timestamp when the
  current rate-limit block ends (HA renders as a live countdown)

"Currently focused zone" sensors (whichever zone the panel last had focus on):
- `sensor.cooktop_focused_zone` — `FrontLeft / FrontRight / RearLeft / RearRight`
- `sensor.cooktop_focused_zone_power` — `off`, `1`, `1.5`, `2`, …, `9`
- `binary_sensor.cooktop_focused_zone_joined` — flex-induction merge state

Per-zone "last observed" sensors — bridge maintains a state map for all 4
zones, updated whenever the panel's focus shifts (every panel touch). Polled
every 5 s while the hob is on; resets to `off` when PowerState=Off. While a
pair is flex-joined, the focused half's power is mirrored onto its partner
(Home Connect reports only the focused half), so both halves read the same
live level; the mirror clears on un-join.
- `sensor.cooktop_zone_front_left_power` / `_front_right_power`
  / `_rear_left_power` / `_rear_right_power`
- `binary_sensor.cooktop_joined_left` — left zones flex-merged
- `binary_sensor.cooktop_joined_right` — right zones flex-merged

Controls:
- `button.cooktop_power_off` — cuts power to the hob (the only direction
  Bosch allows; remote power-on is blocked for safety)
- `switch.cooktop_child_lock_switch`
- `number.cooktop_alarm_clock_minutes` — 0 to 639 minutes (fires
  `BSH.Common.Setting.AlarmClock`; whether your hob honors it as an
  independent kitchen timer is firmware-dependent — verify by testing)

## What's *not* possible

Confirmed by direct API probing on PXX801D67E (firmware May 2026):

- **Integrated venting / fan**: not exposed via the public Home Connect API.
  All five direct probes against `/settings/Cooking.{Hob,Hood,Common}.Setting.*`
  returned `409 SDK.Error.UnsupportedSetting`. The official Bosch app uses an
  internal endpoint we can't reach. No bridge-side workaround exists.
- **Per-zone power writes / remote-start**: `/programs/active` is `access: read`.
  You cannot start a zone or change a running zone's power level from the API.
- **All four zones in a single API call**: only the most-recently-focused
  zone is visible (`Cooking.Hob.Option.ZoneSelector` returns one zone at a
  time). The bridge works around this by maintaining a "last observed" state
  map for each zone — accurate as long as the user touches each zone before
  changing it (which is normal panel use). Flex-joined pairs are the one case
  the user can't "touch each half" (they act as a single area), so the bridge
  mirrors the focused half's power onto its partner while joined.
- **Per-zone push events**: Bosch sends program-level option changes
  (`Duration`, `RemainingProgramTime`, `ProgramProgress`) over SSE, but **not**
  `ZoneSelector` / `PowerLevel` / `JoinZone`. The bridge polls
  `/programs/active/options` to fill the gap:
  - **30 s periodic** while the hob is on (safety net)
  - **2 s fast** while `LocalControlActive=true` (catches each zone the
    user focuses as they tap across the panel; suppresses the periodic)
  - **Edge-triggered** on `ActiveProgram` set, `LocalControlActive=true`,
    and `ProgramFinished`/`ProgramAborted` (the first two kill the ~17 s
    dead window between hob-on and first periodic poll; the last catches
    Bosch auto-shutting a zone off when its timer expires)
  - **Fallback to `/programs/selected/options`** when `/programs/active/options`
    returns 404 while the hob is still powered on. After a per-zone timer
    expires, Bosch transitions OperationState Run→Ready and `/active` 404s,
    but `/selected` keeps the post-expiry zone state (`PowerLevel=Off`) — so
    the fallback surfaces zone-off in <100 ms instead of waiting for the
    next user touch.
- **Per-zone *timers***: the API exposes only one program-level
  `RemainingProgramTime` — there is no per-zone timer field. The physical
  panel may track multiple zone timers internally, but only the active one
  surfaces in the API. The bridge publishes a single
  `status/program_timer_deadline` (RFC3339 wall-clock) for HA to render as a
  countdown. Per-zone deadlines would be fiction.
- **Modifying running zones**: PUT to `/programs/active/options/...` is
  technically allowed by the API, but the hob displays an "Accept change?"
  prompt requiring physical panel confirmation — defeating any automation
  use. So per-zone writes are *not* exposed in HA.

## Setup

1. **Register an app** at https://developer.home-connect.com/
   - OAuth flow: **Device Flow**
   - Client type: Single Key
   - Copy the Client ID

2. **Build**:
   ```sh
   git clone <this repo>
   cd hcbridge
   CGO_ENABLED=0 go build -o hcbridge .
   ```
   `CGO_ENABLED=0` produces a static binary with no libc dependency. This
   matters if you build on one distro and deploy on another — in particular,
   a glibc-linked binary built on Debian will not run on Alpine (musl). The
   bridge has no cgo deps, so disabling it costs nothing.

3. **Install** (LXC / Linux host):

   On **Debian** (systemd):
   ```sh
   sudo useradd --system --no-create-home --shell /usr/sbin/nologin hcbridge
   sudo mkdir -p /opt/hcbridge /etc/hcbridge /var/lib/hcbridge /var/log/hcbridge
   sudo install -m 0755 hcbridge /opt/hcbridge/
   sudo install -m 0640 -o hcbridge -g hcbridge config.example.yaml /etc/hcbridge/config.yaml
   sudo chown hcbridge:hcbridge /var/lib/hcbridge /var/log/hcbridge
   sudo install -m 0644 hcbridge.service /etc/systemd/system/
   sudo systemctl daemon-reload
   ```

   On **Alpine** (OpenRC — no systemd):
   ```sh
   addgroup -S hcbridge
   adduser -S -D -H -s /sbin/nologin -G hcbridge hcbridge
   mkdir -p /opt/hcbridge /etc/hcbridge /var/lib/hcbridge /var/log/hcbridge
   install -m 0755 hcbridge /opt/hcbridge/
   install -m 0640 -o hcbridge -g hcbridge config.example.yaml /etc/hcbridge/config.yaml
   chown hcbridge:hcbridge /var/lib/hcbridge /var/log/hcbridge
   ```
   Then create `/etc/init.d/hcbridge`:
   ```sh
   #!/sbin/openrc-run
   name="hcbridge"
   command="/opt/hcbridge/hcbridge"
   command_args="--mode=run --config=/etc/hcbridge/config.yaml"
   command_user="hcbridge:hcbridge"
   command_background="yes"
   pidfile="/run/hcbridge.pid"
   depend() { need net; after firewall; }
   ```
   `chmod +x /etc/init.d/hcbridge`. Because there's no journald, set
   `logfile: /var/log/hcbridge/hcbridge.log` in the config — lumberjack
   handles rotation in-process (1MB × 3 backups × 14 days, gzip).

4. **Configure**: edit `/etc/hcbridge/config.yaml` — at minimum, set
   `oauth.client_id` and `mqtt.host`.

5. **Authorize** (one-time, interactive):
   ```sh
   sudo -u hcbridge /opt/hcbridge/hcbridge --mode=auth --config=/etc/hcbridge/config.yaml
   ```
   It prints a code and a URL. Open the URL, enter the code, approve.
   The refresh token lands in `/var/lib/hcbridge/token.json`.

6. **Find your appliance ID**:
   ```sh
   sudo -u hcbridge /opt/hcbridge/hcbridge --mode=list-appliances --config=/etc/hcbridge/config.yaml
   ```
   Copy the `haId` for your hob into `appliance.ha_id` in the config.

7. **Start**:

   Debian:
   ```sh
   sudo systemctl enable --now hcbridge
   journalctl -u hcbridge -f
   ```

   Alpine:
   ```sh
   rc-update add hcbridge default
   rc-service hcbridge start
   tail -f /var/log/hcbridge/hcbridge.log
   ```

## Verify without MQTT (debug mode)

Before standing up the bridge against a broker, you can confirm OAuth and the
Home Connect cloud connection in isolation:

```sh
./hcbridge --mode=watch --config=/etc/hcbridge/config.yaml
```

This:
1. Loads the saved token (refreshing if needed)
2. Fetches `/status` and `/settings` for the configured appliance, prints
   them sorted, and marks each key as mapped (will reach MQTT) or `*`-prefixed
   (unmapped, would be dropped in `run` mode)
3. Connects to the SSE event stream and prints every event with timestamp
   until you Ctrl-C

`mqtt.host` does not need to be set in the config for `watch` mode.

## Verify with MQTT

In another terminal:
```sh
mosquitto_sub -h <broker> -t 'homeconnect/cooktop/#' -v
```
Toggle the hob locally; you should see state topics update within 1–2 seconds.

To trigger a write end-to-end:
```sh
mosquitto_pub -h <broker> -t 'homeconnect/cooktop/cmd/child_lock' -m on
```
The cooktop should lock; `homeconnect/cooktop/status/child_lock` flips to `true`.

## Topic conventions

State (retained, published by the bridge):
- `homeconnect/cooktop/connected` — `online` | `offline` (last-will)
- `homeconnect/cooktop/status/operation_state`
- `homeconnect/cooktop/status/power_state`
- `homeconnect/cooktop/status/child_lock` — `true` | `false`
- `homeconnect/cooktop/status/local_control`
- `homeconnect/cooktop/status/remote_start_allowed`
- `homeconnect/cooktop/status/alarm_clock_seconds`
- `homeconnect/cooktop/status/alarm_clock_minutes`
- `homeconnect/cooktop/status/remaining_program_time`
- `homeconnect/cooktop/status/program_duration`
- `homeconnect/cooktop/status/elapsed_program_time`
- `homeconnect/cooktop/status/program_progress`
- `homeconnect/cooktop/status/active_program`
- `homeconnect/cooktop/status/focused_zone`
- `homeconnect/cooktop/status/focused_zone_power`
- `homeconnect/cooktop/status/focused_zone_joined`
- `homeconnect/cooktop/status/appliance_online` — `true` | `false`
- `homeconnect/cooktop/status/rate_limited` — `true` | `false`
- `homeconnect/cooktop/status/rate_limited_until` — RFC3339 wall-clock or empty
- `homeconnect/cooktop/zone/{front_left,front_right,rear_left,rear_right}/power`
  — `off`, `1`, `1.5`, `2`, …, `9` per zone (last observed; a flex-joined
  pair mirrors the focused half onto its partner)
- `homeconnect/cooktop/joined_left` / `joined_right` — `true` | `false`
- `homeconnect/cooktop/event/alarm_clock_elapsed` (non-retained)
- `homeconnect/cooktop/event/program_finished` (non-retained)
- `homeconnect/cooktop/event/program_aborted` (non-retained)

Commands (subscribed by the bridge):
- `homeconnect/cooktop/cmd/power_off` — any payload triggers PowerState=Off
- `homeconnect/cooktop/cmd/child_lock` — `on` | `off` | `true` | `false`
- `homeconnect/cooktop/cmd/alarm_clock` — integer seconds (0 clears)
- `homeconnect/cooktop/cmd/alarm_clock_minutes` — integer minutes

## Resilience

- SSE disconnect: exponential backoff 5 s → 60 s, then re-fetch full state
- Token refresh: proactive at 5 minutes before expiry; force-refresh on 401
- Periodic snapshot resync every 15 minutes as a drift safety net
- MQTT auto-reconnect with last-will so HA always sees `connected = offline`
  while the bridge is down
