# CarLinko remote-control opcodes — Blutter jump table

Recovered from Dart AOT (`send_vehicle_control_data_utils.dart::assembledSendData`) plus
`vcLoadingMessage` / `VehicleControlActionEnum` labels. These are the `data` values for
`POST /user/vehicle/remoteControl`.

**Live-confirmed on this project:** A/C on `741001` / off `741000` (2026-08). Stop charging
`742701` was previously confirmed from an app log string. Other rows are **Blutter-mapped** —
verify on an awake car before trusting them.

Shipped map: [`tools/control_opcodes.json`](../tools/control_opcodes.json) (`_version: 2`).
CLI: `python tools/send_control.py acOn` · `python tools/send_control.py acTemp 22`.

## Format

```
74 <CMD> <STATE>        (6 hex chars; temp set is 7411 + °C byte)
  74      = control-command prefix
  <CMD>   = command id
  <STATE> = 00 off/close · 01 on/open · 02/03 extra mode/level
```

Init handshake (no actuation): `2301`, `24`, `77` — send `77` before an actuation (Control tab /
MQTT / `send_control.py` do this).

## Jump-table index → action → opcode

| Index | Action | Opcode | Confidence |
|------:|--------|--------|------------|
| 0 | door lock | `740100` | blutter |
| 1 | door unlock | `740200` | blutter |
| 2 | windows close | `740500` | blutter |
| 3 | windows vent | `740E00` | blutter |
| 4 | windows open | `740600` | blutter |
| 5 | trunk/liftgate open | `740300` | blutter |
| 6 | trunk/liftgate close | `740A00` | blutter |
| 7 | find car | `740400` | blutter |
| 8 | sunroof close | `740F00` | blutter |
| 9 | sunroof raise/tilt | `740F02` | blutter |
| 10 | sunroof open | `740F01` | blutter |
| 11 | engine on | `740700` | blutter |
| 12 | engine off | `740800` | blutter |
| 13 | temp set | `7411` + °C byte | blutter |
| 14 | A/C on | `741001` | **live** |
| 15 | A/C off | `741000` | **live** |
| 16–17 | quick heat on/off | `741F01` / `741F00` | blutter |
| 18–19 | quick cool on/off | `742001` / `742000` | blutter |
| 20–21 | air purify on/off | `742501` / `742500` | blutter |
| 22–23 | front defog on/off | `741201` / `741200` | blutter |
| 24–27 | L windshield / steering heat family | `7423xx` / `7424xx` | blutter |
| 36–37 | steering heat on/off | `742401` / `742400` | blutter |
| 46–51 | L seat heat L1–L3 / off | `741501`–`741503` / `741500` | blutter |
| 52–57 | L seat vent | `741A01`–`741A03` / `741A00` | blutter |
| 58–63 | L rear heat | `741701`–`741703` / `741700` | blutter |
| 64–69 | L rear vent | `741C01`–`741C03` / `741C00` | blutter |
| 70–75 | R seat heat | `741601`–`741603` / `741600` | blutter |
| 76–81 | R seat vent | `741B01`–`741B03` / `741B00` | blutter |
| 82–87 | R rear heat | `741901`–`741903` / `741900` | blutter |
| 88–93 | R rear vent | `741E01`–`741E03` / `741E00` | blutter |
| 95 | stop charging | `742701` | **live** (app log) |
| 96–97 | gear high / low | `742602` / `742600` | blutter |
| 98–99 | BLE lock / unlock | same as 0/1 | alias |

Indices ~28–35 (generic seat On/Off) are **incomplete in the binary** (some Off cases return
`""` and send nothing). Use the leveled `*1`/`*2`/`*3` / `*Off` commands instead.

## Function-ID cheat sheet

| ID | Feature |
|----|---------|
| 01/02 | lock / unlock |
| 03/0A | trunk open / close |
| 04 | find car |
| 05/06/0E | windows close / open / vent |
| 07/08 | engine on / off |
| 0F | sunroof |
| 10 | A/C |
| 11 | temperature |
| 12 | front defog |
| 15/16 | L/R seat heat |
| 17/19 | L/R rear heat |
| 1A/1B | L/R seat vent |
| 1C/1E | L/R rear vent |
| 1F/20 | quick heat / cool |
| 23/24 | windshield / steering heat |
| 25 | air purify |
| 26/27 | gear / stop charge |

## Named keys (dashboard / MQTT)

See `tools/control_opcodes.json`. Examples: `lock`, `engineOn`, `acOn`, `liftClose`,
`seatHeatL1`, `gearHigh`. Temp uses builder `7411%02X` (°C), not a fixed JSON entry.

## Old guess map (removed in v2)

The previous best-effort wiring was **wrong** (e.g. A/C was `742401`, which is steering heat;
`liftOpen` was `741201`, which is front defog). Do not reuse those hex values for the old labels.

## How to re-verify

1. Car awake, cellular online.
2. `python tools/send_control.py <action>` or Control-tab tester.
3. Optional: compare `/api/summary` / DB blob bytes (`b23` A/C, `b26` engine_on candidate,
   `b5` HV) before and after the command.
