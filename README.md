# hcbridge

A small single-binary bridge from a Bosch/Siemens/Neff (BSH) hob to MQTT,
written in Go, speaking the appliance's **local** Home Connect protocol: no
cloud at runtime, no API quota, no OAuth. Version 2.0.0 (`hcbridge --version`).

Built for a Bosch induction cooktop with integrated ventilation (PXX801D67E).
The bridge is **read-only** towards the appliance: it only reads what the hob
pushes and never writes to it, and there are no MQTT command topics.

## What you get in Home Assistant (via MQTT discovery)

Every entity pins its entity ID with `default_entity_id`, so the IDs survive a
re-registration. Appliance data entities use two availability topics (the
bridge's last will and the local link): they show `unavailable`, not frozen
values, while the bridge cannot see the hob.

Appliance data:
- `sensor.cooktop_operation_state` - Inactive / Ready / Run / Pause / Finished / ...
- `sensor.cooktop_power_state` - On / Off
- `sensor.cooktop_active_program`
- `sensor.cooktop_program_duration`, `_elapsed_program_time`, `_program_progress`
- `sensor.cooktop_program_timer` - the program-level remaining time as a
  wall-clock deadline (HA renders a live countdown)
- `sensor.cooktop_front_left_zone` / `_front_right_zone` / `_rear_left_zone` /
  `_rear_right_zone` - each zone's power level: `off`, `1`, `1.5`, ..., `9`,
  `KeepWarm`, `Boost1`, `Boost2`
- `sensor.cooktop_<zone>_zone_state` - `off`, `active`, `residual_heat`,
  `paused`, `error`, `not_selectable`
- `sensor.cooktop_<zone>_zone_timer` - that zone's timer as a deadline
- `binary_sensor.cooktop_left_zones_joined` / `_right_zones_joined` - flex pairs merged
- `sensor.cooktop_focused_zone` - the zone the panel has focus on
- `sensor.cooktop_fan_level` - the integrated fan: `off`, `auto`, `1` ... `9`
  in half steps, `boost1`, `boost2`, `afterrun`
- `sensor.cooktop_carbon_filter_saturation` (%) and
  `binary_sensor.cooktop_carbon_filter_replace` (from 90 %, or when the hob
  itself reports the filter nearly saturated)
- `binary_sensor.cooktop_child_lock`

Diagnostics:
- `binary_sensor.cooktop_local_control` - someone is touching the panel
- `sensor.cooktop_wifi_signal` (dBm) - the hob's own Wi-Fi signal
- `sensor.cooktop_local_link` - `connecting`, `connected`, `refused` (the hob
  answers HTTP 503, see below), `unreachable` (no TCP connection), `key_error`
  (the profile key no longer matches), `error`
- `sensor.cooktop_local_link_since` - when the current link state began
- `binary_sensor.cooktop_power_cycle_needed` - on after the hob has refused
  local connections for 30 minutes; off again once a session connects
- `sensor.cooktop_bridge_heartbeat` - refreshed every 5 min by the main loop;
  alert on it going stale (a wedged process keeps its MQTT connection, so the
  last will alone does not catch it)

The link and heartbeat entities are gated only by the bridge's last will, so
they stay visible exactly when the hob cannot be reached. The HA device page
shows the hob's model, serial number and firmware (`<appliance>-<network
module>`, as in the Home Connect app).

Removed in 2.0.0 (the bridge deletes their discovery configs and retained
topics on connect): the cloud-only API rate limit, API call count, cloud
connection and appliance connected entities, plus the entities 1.x had
(alarm clock, remaining program time, focused zone power, focused "zones
joined", remote start allowed).

Not available over the local protocol either: the energy the hob shows on its
display (the profile only has the setting that turns that display on or off).

## How it works

BSH appliances run a WebSocket server on the LAN (`ws://<hob>/homeconnect`,
the same one the official app uses when it is on the same network). Frames
are AES-256-CBC with a chained HMAC-SHA256, keyed per appliance; the key and
the description of the hob's features (which numeric UID means what) come
from a one-time download from the Home Connect account (see Setup). After
that the bridge needs nothing from the cloud.

The bridge keeps **one** session open around the clock. The hob pushes every
change within milliseconds: each zone's level, state and timer, the fan, the
program state. A full snapshot is read on every connect and every 6 hours.

## Known issue: the hob refuses local connections (HTTP 503)

Hobs and hoods with the older Home Connect network module (firmware
2.17.1.13549 in every report so far) sometimes start answering every local
WebSocket upgrade with `HTTP 503`, for anything from about half an hour to
days, while Wi-Fi and ping keep working. The firmware does this itself; no
fix has been published. Power-cycling the hob (breaker off for ~30 s) clears
it; forcing the hob to re-join Wi-Fi from the access point may also work.

Abrupt disconnects seem to trigger it, so the bridge is careful with the
connection:
- exactly one connection, held for days; a persistent device ID, so a
  reconnect replaces the bridge's own stale session on the hob
- WebSocket pings every 60 s with a 30 s pong timeout (idle hobs answer
  slowly; a 10 s timeout dropped the connection dozens of times a day in
  other projects); any frame counts as a sign of life
- no `Origin` header (the hob ignores upgrades that carry one)
- a clean WebSocket Close before every deliberate disconnect and on shutdown
- reconnects back off from 5 s to 5 min with jitter; the last dial survives a
  restart, so a crash loop cannot hammer the hob

When the hob refuses anyway, `sensor.cooktop_local_link` turns `refused` and,
after `link.power_cycle_after` (30 min), `binary_sensor.cooktop_power_cycle_needed`
turns on (see the automation below). It clears when a session connects again.

Do not run another local client (hcpy, the Home Connect Local integration)
against the same hob, and do not probe its port 80 with nc or curl.

## Setup

1. **Network.** Give the hob a DHCP reservation. The bridge dials that IP on
   TCP port 80; it does not use mDNS, so the hob may be on another VLAN as long
   as the bridge's TCP connection and the hob's replies are allowed.

2. **Profile (one-time, needs the Home Connect account).** Run
   [homeconnect-profile-downloader](https://github.com/bruestel/homeconnect-profile-downloader)
   on a desktop, log in, and download the profile for the hob (target "Home
   Connect Local" or "homeconnect_direct"). It writes a ZIP with
   `<haId>.json` and the two XML files. Only `connectionType: AES` profiles
   (port 80) are supported; the bridge rejects TLS ones. Download it again
   after re-pairing the hob or resetting its Wi-Fi (the key changes; the bridge
   reports `key_error`), and after a firmware update if new keys show up as
   unmapped in the log.

3. **Build** (Go toolchain pinned in `go.mod`):
   ```sh
   git clone <this repo>
   cd hcbridge
   make release      # tests, refuses a dirty tree, prints version + sha256
   ```
   The binary is static (`CGO_ENABLED=0`), so a build from any distro runs on
   Alpine (musl). Deploy only release builds: `hcbridge --version` must not
   say `-dirty`.

4. **Install** (LXC / Linux host). The config holds the MQTT password and the
   profile holds the hob's key: both readable by the service, owned by root.

   On **Alpine** (OpenRC):
   ```sh
   addgroup -S hcbridge
   adduser -S -D -H -s /sbin/nologin -G hcbridge hcbridge
   install -d -m 0755 /opt/hcbridge /etc/hcbridge
   install -d -m 0750 -o root -g hcbridge /etc/hcbridge/profile
   install -d -m 0700 -o hcbridge -g hcbridge /var/lib/hcbridge /var/log/hcbridge
   install -m 0755 hcbridge /opt/hcbridge/
   install -m 0640 -o root -g hcbridge config.example.yaml /etc/hcbridge/config.yaml
   install -m 0640 -o root -g hcbridge <profile>.zip /etc/hcbridge/profile/
   install -m 0755 openrc/hcbridge /etc/init.d/hcbridge
   ```
   `logfile: /var/log/hcbridge/hcbridge.log` in the config (there is no
   journald); stdout/stderr land in `/var/log/hcbridge/std{out,err}.log`.

   On **Debian** (systemd):
   ```sh
   sudo useradd --system --no-create-home --shell /usr/sbin/nologin hcbridge
   sudo install -d -m 0755 /opt/hcbridge /etc/hcbridge
   sudo install -d -m 0750 -o root -g hcbridge /etc/hcbridge/profile
   sudo install -m 0755 hcbridge /opt/hcbridge/
   sudo install -m 0640 -o root -g hcbridge config.example.yaml /etc/hcbridge/config.yaml
   sudo install -m 0640 -o root -g hcbridge <profile>.zip /etc/hcbridge/profile/
   sudo install -m 0644 hcbridge.service /etc/systemd/system/
   sudo systemctl daemon-reload
   ```
   The unit creates `/var/lib/hcbridge` and `/var/log/hcbridge` itself.

5. **Configure**: edit `/etc/hcbridge/config.yaml` - at minimum
   `appliance.host`, `appliance.profile` and `mqtt.host` - then check it. This
   reads the profile and lists what the hob can report, without touching the
   network:
   ```sh
   su -s /bin/sh hcbridge -c '/opt/hcbridge/hcbridge --mode=check-config --config=/etc/hcbridge/config.yaml'
   ```
   Unknown keys are errors.

6. **Try it** without MQTT (as the service user, with the service stopped):
   ```sh
   su -s /bin/sh hcbridge -c '/opt/hcbridge/hcbridge --mode=local-watch --config=/etc/hcbridge/config.yaml'
   ```
   Switch the hob on and touch a zone: every value prints as it arrives, with
   the `[PUB]` lines run mode would publish. Stop it with Ctrl-C (it closes the
   session cleanly). The first run creates the device ID in the state directory.

7. **Start**: `rc-update add hcbridge default && rc-service hcbridge start`
   (Alpine) or `systemctl enable --now hcbridge` (Debian). The log's first line
   is the version, then "connected to ... appliance <version>, network module
   <version>".

## Upgrading from 1.x (cloud) on Alpine

2.0.0 does not read the 1.x config (the `oauth` block is gone).

1. Build with `make release` and note the sha256. Install the profile and
   check the hob connects with `--mode=local-watch` (step 6 above; the 1.x
   service may keep running meanwhile, it only uses the cloud).
2. Keep the old setup for a rollback: `cp /etc/hcbridge/config.yaml /etc/hcbridge/config.yaml.1x`,
   and stop 1.x with its own init script: `rc-service hcbridge stop`.
3. Write the new config: remove `oauth`, add `appliance.host` and
   `appliance.profile` (keep `ha_id`, `mqtt`, `logfile`). Validate it with the
   new binary (`/opt/hcbridge/hcbridge.new --mode=check-config ...`).
4. Swap the binary atomically, keeping the old one:
   `ln -f /opt/hcbridge/hcbridge /opt/hcbridge/hcbridge.prev && mv -f /opt/hcbridge/hcbridge.new /opt/hcbridge/hcbridge`,
   and install the new `openrc/hcbridge`.
5. `rc-service hcbridge start`, then check the log (version, MQTT connected,
   "connected to ...") and in HA `sensor.cooktop_local_link` = `connected`.
6. Rollback: restore `hcbridge.prev`, `config.yaml.1x` and the old init
   script. `token.json` is left alone; if its refresh token has expired, 1.x
   needs `--mode=auth` again.

Update anything in HA that used the removed entities
(`binary_sensor.cooktop_cloud_connection`, `binary_sensor.cooktop_api_rate_limited`, ...).

## Modes

- `run` (default) - the service.
- `check-config` - load and validate the config and the profile, print what
  the profile says the hob can report (fan key, zones), exit. No network.
- `local-watch` - connect to the hob like the service does, without MQTT:
  prints every pushed value (`[VAL]`), access change (`[DESC]`), link change
  (`[LINK]`) and what run mode would publish (`[PUB]`). `--raw` adds the
  decrypted frames; `--record=<file>` appends the values as JSON lines
  (never the key or the device info). It takes the same lock as the service,
  so the two cannot run at once.
- `--version` - print version, VCS revision and toolchain.

## Resilience

- **Local link**: see "Known issue" above for the connection rules. A
  corrupted frame or a missing pong ends the session with a Close frame and a
  reconnect; a stale key shows as `key_error` after three failed connects.
  The data entities go unavailable 30 s after the session drops (a routine
  reconnect takes a second).
- **State**: the state directory holds the device ID (never replaced on its
  own: a damaged file is an error), the lock, the last link episode
  (`link.json`: a restart keeps the power-cycle clock and waits before
  redialling) and the last device info (`device.json`: the firmware in HA
  before the first connect).
- **MQTT**: auto-reconnect with a last will (`connected = offline`); the full
  retained state is republished on every reconnect, and a publish that was not
  acknowledged is retried on the next update. The first broker connection may
  take as long as it needs (no exit on a boot race). Values the hob pushes
  every second (elapsed time, progress) are published at most every 15 s.
- **Process**: under OpenRC `supervise-daemon` (see `openrc/hcbridge`) the
  bridge is restarted if it exits, and the healthcheck restarts it if the main
  loop stops refreshing `<state_dir>/alive`. A hob refusing connections does
  not stop that file being refreshed: a restart would not help.
- **Logs**: lumberjack rotation, 5 MB x 7 backups x 14 days, gzip. The
  profile key never appears in logs or recordings.

## Home Assistant automations

Notify when the hob needs a power cycle, and when it is back:

```yaml
- id: cooktop_power_cycle_needed
  alias: "Cooktop: power cycle needed"
  triggers:
    - trigger: state
      entity_id: binary_sensor.cooktop_power_cycle_needed
      from: "off"
      to: "on"
  actions:
    - action: notify.mobile_app_PHONE
      data:
        title: "Cooktop: local link stuck"
        message: >-
          The hob has refused local connections since
          {{ as_timestamp(states('sensor.cooktop_local_link_since')) | timestamp_custom('%H:%M') }}.
          Switch its breaker off for about 30 seconds.
- id: cooktop_link_restored
  alias: "Cooktop: local link restored"
  triggers:
    - trigger: state
      entity_id: binary_sensor.cooktop_power_cycle_needed
      from: "on"
      to: "off"
  actions:
    - action: notify.mobile_app_PHONE
      data:
        title: "Cooktop: link restored"
        message: "The bridge is connected to the hob again."
- id: cooktop_profile_key_rejected
  alias: "Cooktop: profile key rejected"
  triggers:
    - trigger: state
      entity_id: sensor.cooktop_local_link
      to: key_error
      for: "00:10:00"
  actions:
    - action: notify.mobile_app_PHONE
      data:
        title: "Cooktop: profile stale"
        message: "Download the hob's profile again and restart hcbridge."
```

Also worth an alert: `sensor.cooktop_bridge_heartbeat` older than 15 minutes,
and `binary_sensor.cooktop_carbon_filter_replace` turning on.

## Testing

`make test` runs vet plus the unit tests under the race detector; `make check`
adds staticcheck and govulncheck. The tests drive the real client and bridge
against `internal/homeconnect/hcfake`, a fake hob that speaks the protocol and
can refuse with 503, stop answering pings, corrupt a frame, change its key or
drop the connection. `make e2e` runs the end-to-end test against a throwaway
Mosquitto container:

```sh
make e2e
```

GitHub Actions (`.github/workflows/ci.yml`) runs the same on every push and
pull request to `main`: gofmt, `go mod tidy`, vet, a static build, the race
tests, staticcheck, govulncheck and the end-to-end test.

## Verify with MQTT

```sh
mosquitto_sub -h <broker> -t 'homeconnect/cooktop/#' -v
```
Touch the hob; state topics update within a second.

## Topic conventions

State (retained, published by the bridge):
- `homeconnect/cooktop/connected` - `online` | `offline` (last will)
- `homeconnect/cooktop/link` - `online` | `offline` (live session with the hob)
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
- `homeconnect/cooktop/status/fan_level`
- `homeconnect/cooktop/status/carbon_filter_saturation`
- `homeconnect/cooktop/status/carbon_filter_replace` - `true` | `false`
- `homeconnect/cooktop/status/wifi_signal`
- `homeconnect/cooktop/status/link_state`
- `homeconnect/cooktop/status/link_since` - RFC3339
- `homeconnect/cooktop/status/power_cycle_needed` - `true` | `false`
- `homeconnect/cooktop/status/heartbeat` - RFC3339
- `homeconnect/cooktop/zone/{front_left,front_right,rear_left,rear_right}/power`
- `homeconnect/cooktop/zone/{...}/state`
- `homeconnect/cooktop/zone/{...}/timer_deadline` - RFC3339 or empty
- `homeconnect/cooktop/joined_left` / `joined_right` - `true` | `false`

Events (not retained; payload `Present`, `Confirmed` or `Off`):
- `homeconnect/cooktop/event/alarm_clock_elapsed`
- `homeconnect/cooktop/event/program_finished`
- `homeconnect/cooktop/event/program_aborted`
- `homeconnect/cooktop/event/carbon_filter_nearly_saturated`
- `homeconnect/cooktop/event/carbon_filter_saturated`

There are no command topics.

## Security notes

- The profile's key gives full local control of the hob to anyone on the
  LAN who has it: keep it `0640 root:hcbridge`, never commit it (the repo's
  `.gitignore` covers profile ZIPs and XMLs). `check-config` warns about a
  world-readable profile.
- The bridge only sends the protocol's read requests and its handshake
  answers; there is no code path that writes a setting or starts a program.
- MQTT supports TLS (`mqtt.tls: true`, optional `mqtt.ca_file`, TLS 1.2+).
  On a plain-TCP broker, restrict the bridge's user with an ACL to
  `homeconnect/<slug>/#` and `homeassistant/+/hcbridge_<slug>/#`.
- The MQTT client ID defaults to `hcbridge-<hostname>`; it must be unique per
  running bridge because the session is persistent.

## Credits

The local protocol was reverse-engineered by
[hcpy](https://github.com/osresearch/hcpy) and documented further by
[homeconnect_websocket](https://github.com/chris-mc1/homeconnect_websocket),
[homeconnect_direct](https://github.com/Asmir1975/homeconnect_direct),
[go-homeconnect2mqtt](https://github.com/SukramJ/go-homeconnect2mqtt) and
[Kessel](https://github.com/ediiiz/Kessel); the profile comes from
[homeconnect-profile-downloader](https://github.com/bruestel/homeconnect-profile-downloader).
hcbridge's implementation is its own (Go standard library crypto, gorilla/websocket).
