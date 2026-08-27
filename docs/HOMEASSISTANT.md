# Home Assistant integration

> **Native integration exists:** [ha-carlinko](https://github.com/jebentancour/ha-carlinko) by
> [@jebentancour](https://github.com/jebentancour) is a read-only Home Assistant integration built
> on the same telemetry (no dashboard needed in the middle). The MQTT path below is the
> zero-extra-component alternative that uses this dashboard (telemetry **and** remote control).

## Before you start
- Home Assistant and the dashboard must share an MQTT broker (same LAN, Tailscale, or HA’s
  built-in Mosquitto add-on).
- MQTT works with a password-gated dashboard: the bridge runs server-side and talks to the broker.
- **Do not** add REST sensors / YAML entities for these. MQTT discovery publishes the device and
  all entities automatically once the bridge is enabled.

## MQTT setup

Configure in **Settings → MQTT**, or in `creds.json`:

```json
"mqtt": {
  "enabled": true,
  "host": "homeassistant.local",
  "port": 1883,
  "username": "",
  "password": "",
  "tls": false,
  "base_topic": "j5",
  "discovery_prefix": "homeassistant"
}
```

Requires `paho-mqtt` (listed in `requirements.txt` — installed by `install.sh` / Docker).

Topics are **per-vehicle**: `{base_topic}/{VIN}/…` (falls back to plate, then CarLinko
`vehicle_id`). Two dashboard instances can share the same `base_topic` without colliding.

After connect, Home Assistant creates one device with the entities below. PHEV fuel, direct TPMS,
and capability-gated controls appear only when the car reports them; discovery is re-sent if that
changes.

**Entity IDs** below use discovery `object_id` → typically `{domain}.{object_id}`. HA may prefix
with a sanitized device name (e.g. `sensor.omoda_9_…_battery`). `unique_id` is
`carlinko_{vehicle_id}_{object_id}`.

Availability follows car freshness (~40 min): when the car is dark, entities go unavailable.

Computed money (`lifetime_cost`, `running_cost`, …) is a **sensor**. Writable
`number.tariff` / `number.petrol_price` / `number.petrol_kml` write back to `creds.json` (no
restart). Dashboard Settings petrol fields stay browser-local and are **not** synced.

---

## Entities (MQTT discovery)

### Sensors (always)

| Entity ID | Name | Type | Description |
| --- | --- | --- | --- |
| `sensor.battery` | Battery | sensor | Traction SoC % |
| `sensor.range` | Range | sensor | EV range km |
| `sensor.odometer` | Odometer | sensor | Odometer km |
| `sensor.volt12` | 12V Battery | sensor | Aux 12V |
| `sensor.charge_power` | Charge Power | sensor | Instant charge/regen kW |
| `sensor.consumption` | Consumption | sensor | kWh/100km |
| `sensor.charge_remaining` | Charge remaining | sensor | Minutes to done |
| `sensor.charge_mode` | Charge mode | sensor | `none` / `ac` / `dc` |
| `sensor.charge_state` | Charge state | sensor | `idle`…`stop` |
| `sensor.charge_session_kwh` | Charge session | sensor | Session kWh |
| `sensor.charge_session_soc` | Charge session SoC | sensor | Session SoC % |
| `sensor.updated` | Updated | sensor | Last frame timestamp |
| `sensor.tyre_status` | Tyre status | sensor | `Normal` / `Check tyres` |
| `sensor.hv_state` | HV state | sensor | `off` / `lv` / `ready` / `unknown` |
| `sensor.volt12_status` | 12V status | sensor | `ok` / `low` / `critical` |
| `sensor.volt12_min7d` | 12V 7-day min | sensor | 7-day min V |
| `sensor.wltc_range` | Rated range | sensor | Rated/WLTC km |
| `sensor.parked_drain` | Parked drain | sensor | %/day parked |
| `sensor.km_today` | Km today | sensor | Today km |
| `sensor.km_week` | Km week | sensor | Week km |
| `sensor.km_month` | Km month | sensor | Month km |
| `sensor.energy_today` | Energy today | sensor | Today kWh |
| `sensor.energy_left` | Energy left | sensor | Usable energy left kWh |
| `sensor.efficiency_rating` | Efficiency rating | sensor | `optimal` / `normal` / `boros` |
| `sensor.avg_speed` | Average speed | sensor | Avg km/h |
| `sensor.charges_week` | Charges this week | sensor | Charge count |
| `sensor.charges_month` | Charges this month | sensor | Charge count |
| `sensor.charge_month_kwh` | Charge kWh this month | sensor | Month kWh |
| `sensor.charge_month_cost` | Charge cost this month | sensor | Month cost (not writable) |
| `sensor.lifetime_km` | Lifetime km | sensor | Log-span km |
| `sensor.lifetime_kwh` | Lifetime kWh billed | sensor | Log-span kWh |
| `sensor.lifetime_cost` | Lifetime cost | sensor | Log-span cost |
| `sensor.lifetime_saved` | Lifetime saved | sensor | vs petrol |
| `sensor.liters_saved` | Litres saved | sensor | Petrol L saved |
| `sensor.co2_saved` | CO2 saved | sensor | kg CO₂ |
| `sensor.running_cost` | Running cost | sensor | Currency/km |
| `sensor.month_cost_est` | Month cost estimate | sensor | Est. month cost |
| `sensor.days_to_charge` | Days to charge | sensor | Forecast days |
| `sensor.real_range` | Real-world range | sensor | Derived real range |
| `sensor.rated_range` | Car-rated range | sensor | Brochure/rated |
| `sensor.battery_usable` | Usable battery | sensor | Pack usable kWh |
| `sensor.battery_cycles` | Battery cycles | sensor | Est. cycles |
| `sensor.days_since_full` | Days since full charge | sensor | Days since 100% |
| `sensor.battery_care` | Battery care | sensor | `ok` / `due` / `overdue` / `unknown` |

### Sensors (conditional)

| Entity ID | Name | Type | Description |
| --- | --- | --- | --- |
| `sensor.fuel` | Fuel | sensor | PHEV tank % |
| `sensor.fuel_range` | Fuel range | sensor | PHEV fuel km |
| `sensor.total_range` | Total range | sensor | PHEV EV+fuel km |
| `sensor.fuel_consumption` | Fuel consumption | sensor | PHEV L/100km |
| `sensor.tyre_fl` … `tyre_rr` | Front/Rear L/R | sensor | Direct TPMS pressure |
| `sensor.tyre_fl_temp` … `tyre_rr_temp` | … temp | sensor | Direct TPMS °C |

### Binary sensors (always)

| Entity ID | Name | Type | Description |
| --- | --- | --- | --- |
| `binary_sensor.charging` | Charging | binary_sensor | Plugged / charging |
| `binary_sensor.online` | Online | binary_sensor | Fresh telemetry |
| `binary_sensor.moving` | Moving | binary_sensor | Car moving |
| `binary_sensor.tyres_ok` | Tyre problem | binary_sensor | ON = check tyres |
| `binary_sensor.door` | Any door | binary_sensor | Any door open |
| `binary_sensor.door_driver` | Driver door | binary_sensor | Driver |
| `binary_sensor.door_passenger` | Passenger door | binary_sensor | Passenger |
| `binary_sensor.door_rear_left` | Rear left door | binary_sensor | RL |
| `binary_sensor.door_rear_right` | Rear right door | binary_sensor | RR |
| `binary_sensor.seat_heat_left` | Seat heat left | binary_sensor | Driver heat on |
| `binary_sensor.seat_heat_right` | Seat heat right | binary_sensor | Passenger heat on |
| `binary_sensor.seat_vent_left` | Seat vent left | binary_sensor | Driver vent on |
| `binary_sensor.seat_vent_right` | Seat vent right | binary_sensor | Passenger vent on |
| `binary_sensor.defrost` | Defrost | binary_sensor | Front defrost **state** |
| `binary_sensor.balance_due` | Balance due | binary_sensor | LFP balance due |

### Binary sensors (if Engine)

| Entity ID | Name | Type | Description |
| --- | --- | --- | --- |
| `binary_sensor.engine_on` | Engine on | binary_sensor | Byte 26 / power-on candidate |

### Numbers (always, writable → `creds.json`)

| Entity ID | Name | Type | Description |
| --- | --- | --- | --- |
| `number.tariff` | Charging tariff | number | ¤/kWh |
| `number.petrol_price` | Petrol price | number | ¤/L |
| `number.petrol_kml` | Petrol economy | number | km/L ICE baseline |

### Controls (capability-gated)

Published only when CarLinko `vehicleControlConfig` / caps say the car supports them.

| Entity ID | Name | Type | Description |
| --- | --- | --- | --- |
| `lock.lock` | Lock | lock | Lock / unlock |
| `climate.climate` | Climate | climate | A/C on/off; temp if supported |
| `cover.windows` | Windows | cover | Open / close / vent |
| `cover.sunroof` | Sunroof | cover | Open / close / tilt |
| `cover.liftgate` | Liftgate | cover | Open / close |
| `button.find` | Find car | button | Horn/lights |
| `button.charge_stop` | Stop charging | button | Stop charge |
| `switch.engine` | Engine | switch | Remote engine on/off |
| `select.gear` | Gear | select | `low` / `high` |
| `button.quick_cool` | Quick cool | button | Rapid cool |
| `button.quick_heat` | Quick heat | button | Rapid heat |
| `switch.defrost` | Defog | switch | Front defog **command** (distinct from `binary_sensor.defrost`) |
| `switch.purify` | Air purify | switch | Cabin purify |
| `select.seat_heatL` | Driver seat heat | select | `off` / `L1`–`L3` |
| `select.seat_ventL` | Driver seat vent | select | `off` / `L1`–`L3` |
| `select.seat_heatR` | Passenger seat heat | select | `off` / `L1`–`L3` |
| `select.seat_ventR` | Passenger seat vent | select | `off` / `L1`–`L3` |
| `select.seat_heatLR` | Rear L seat heat | select | `off` / `L1`–`L3` |
| `select.seat_ventLR` | Rear L seat vent | select | `off` / `L1`–`L3` |
| `select.seat_heatRR` | Rear R seat heat | select | `off` / `L1`–`L3` |
| `select.seat_ventRR` | Rear R seat vent | select | `off` / `L1`–`L3` |

**Not discovered as HA entities:** windshield / steering heat (Control tab only if flags true).

---

## Commands

Commands use **named actions**, not raw hex. Opcodes live in `tools/control_opcodes.json`
(Blutter jump-table map, `_version: 2`). **A/C on/off** and **stop charging** are live-confirmed;
other actions should still be verified on your car. Remap via the Control tab (long-press) or
`POST /api/opcodes` — that updates the shared file so MQTT and the UI stay in sync.

Number entities (`tariff`, `petrol_price`, `petrol_kml`) are not car opcodes: they PATCH
`creds.json` and reload in-memory rates so the next telemetry tick republishes derived cost
sensors.

Events (non-retained): `{base_topic}/{vin}/event/charge_complete`,
`{base_topic}/{vin}/event/battery_low`.

### Topic sketch
```
{base}/{vin}/sensor/battery
{base}/{vin}/sensor/charge_state
{base}/{vin}/binary_sensor/engine_on
{base}/{vin}/number/tariff
{base}/{vin}/control/tariff/set   ← float, writes creds.json
{base}/{vin}/lock/state          ← LOCKED / UNLOCKED
{base}/{vin}/control/lock/set    ← LOCK / UNLOCK
{base}/{vin}/control/climate/set ← off / cool
{base}/{vin}/control/climate_temp/set ← °C
{base}/{vin}/control/engine/set  ← ON / OFF
{base}/{vin}/control/windows/set ← OPEN / CLOSE / VENT
{base}/{vin}/control/liftgate/set ← OPEN / CLOSE
{base}/{vin}/control/result      ← last command ack JSON
{base}/{vin}/availability
```
(`{vin}` = VIN when known, else plate, else CarLinko vehicle_id.)

---

## Automations

Use the MQTT entities (adjust IDs if HA prefixed the device name). Prefer MQTT events for
charge-complete / battery-low instead of polling.

```yaml
automation:
  - alias: "EV battery low"
    trigger:
      - platform: numeric_state
        entity_id: sensor.battery
        below: 20
    action:
      - service: notify.mobile_app_YOURPHONE
        data:
          title: "EV"
          message: "Battery {{ states('sensor.battery') }}% — time to charge."

  - alias: "EV charge complete"
    trigger:
      - platform: state
        entity_id: binary_sensor.charging
        from: "on"
        to: "off"
    action:
      - service: notify.mobile_app_YOURPHONE
        data:
          title: "EV"
          message: "Charging done — {{ states('sensor.battery') }}%."
```

Or MQTT trigger on `{base_topic}/{vin}/event/charge_complete` /
`{base_topic}/{vin}/event/battery_low`.

---

## Optional: REST (no MQTT)

Only if you cannot use MQTT. Polling `/api/summary` does **not** create the discovery entities
above and does **not** give remote controls. A dashboard password returns `401` on that API —
run un-gated on a private network / Tailscale, or use MQTT instead. See `/api/summary` in the
dashboard for available JSON fields if you build custom templates yourself.
