"""Home Assistant MQTT bridge: discovery, telemetry publish, named remote-control commands.

Runs inside the dashboard server process. Needs paho-mqtt (requirements.txt); server.py imports
this module lazily inside try/except, so the dashboard still serves if it is missing.
Topics are namespaced per car out of the box:
  {base_topic}/{VIN|plate|vehicle_id}/sensor/battery
so two Docker instances on one broker do not collide when base_topic is shared.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
import traceback
from datetime import datetime

import paho.mqtt.client as mqtt

HERE = os.path.dirname(os.path.abspath(__file__))
_DATA = os.environ.get("CARLINKO_DATA") or HERE

# Fallback if tools/control_opcodes.json is missing/corrupt. The shipped file is the source of truth.
DEFAULT_OPCODES = {
    "lock": "741000",
    "unlock": "741001",
    "acOn": "742401",
    "acOff": "742400",
    "winOpen": "741501",
    "winClose": "741500",
    "winVent": "741502",
    "roofOpen": "741A01",
    "roofClose": "741A00",
    "roofTilt": "741A02",
    "liftOpen": "741201",
    "find": "740100",
    "chgStop": "742701",
}

POLL_S = 2.5
HEARTBEAT_S = 30.0  # republish retained state at least this often; otherwise only on change
ONLINE_AGE_MIN = 40.0
BATTERY_LOW_PCT = 20
CHARGE_STATE = {0: "idle", 1: "charging", 2: "complete", 3: "canceled", 4: "hot", 5: "stop"}
HV_STATE = {0: "off", 1: "lv", 2: "ready"}
TYRE_POS = (("fl", "Front left"), ("fr", "Front right"), ("rl", "Rear left"), ("rr", "Rear right"))
DOOR_BITS = (
    ("door_driver", "Driver door", 1),
    ("door_passenger", "Passenger door", 2),
    ("door_rear_left", "Rear left door", 4),
    ("door_rear_right", "Rear right door", 8),
)


def opcodes_path():
    # Static protocol map lives next to the code (tools/), not in the mutable data volume.
    return os.path.join(HERE, "control_opcodes.json")


def load_opcodes():
    """Read tools/control_opcodes.json; fall back to DEFAULT_OPCODES if missing/corrupt."""
    out = dict(DEFAULT_OPCODES)
    path = opcodes_path()
    if not os.path.isfile(path):
        return out
    try:
        raw = json.load(open(path, encoding="utf-8"))
    except Exception:
        return out
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        if k.startswith("_"):
            continue
        if k not in DEFAULT_OPCODES:
            continue
        s = str(v or "").strip()
        if s and all(ch in "0123456789abcdefABCDEF" for ch in s) and len(s) <= 16:
            out[k] = s
    return out


def save_opcodes(mapping):
    """Merge validated remaps into tools/control_opcodes.json. Returns the effective map."""
    path = opcodes_path()
    existing = dict(DEFAULT_OPCODES)
    if os.path.isfile(path):
        try:
            raw = json.load(open(path, encoding="utf-8"))
            if isinstance(raw, dict):
                for k, v in raw.items():
                    if str(k).startswith("_") or k not in DEFAULT_OPCODES:
                        continue
                    s = str(v or "").strip()
                    if s:
                        existing[k] = s
        except Exception:
            pass
    clean = dict(existing)
    for k, v in (mapping or {}).items():
        if k.startswith("_") or k not in DEFAULT_OPCODES:
            continue
        s = str(v or "").strip()
        if not s:
            clean[k] = DEFAULT_OPCODES[k]
            continue
        if not all(ch in "0123456789abcdefABCDEF" for ch in s) or len(s) > 16:
            raise ValueError(f"invalid opcode for {k}")
        clean[k] = s
    with open(path, "w", encoding="utf-8") as f:
        json.dump(clean, f, indent=2, ensure_ascii=False)
        f.write("\n")
    return load_opcodes()


def _mqtt_cfg(raw=None):
    c = raw if isinstance(raw, dict) else {}
    return {
        "enabled": bool(c.get("enabled")),
        "host": (c.get("host") or "").strip(),
        "port": int(c.get("port") or 1883),
        "username": (c.get("username") or "").strip(),
        "password": c.get("password") if c.get("password") is not None else "",
        "tls": bool(c.get("tls")),
        # Prefix only — vehicle slug (VIN → plate → vehicle_id) is always appended.
        "base_topic": (c.get("base_topic") or "j5").strip().strip("/") or "j5",
        "discovery_prefix": (c.get("discovery_prefix") or "homeassistant").strip().strip("/")
                            or "homeassistant",
    }


def _topic_slug(value):
    """MQTT-safe segment from VIN / plate / id. Empty if missing or placeholder."""
    s = str(value or "").strip()
    if not s or s.lower() == "auto":
        return ""
    s = re.sub(r"[^A-Za-z0-9_-]+", "_", s).strip("_")
    return s[:64]


def _n(v):
    return "" if v is None else str(v)


def _on(flag):
    return "ON" if flag else "OFF"


def _iso(ts):
    try:
        return datetime.fromtimestamp(float(ts)).astimezone().isoformat()
    except Exception:
        return ""


def _money_step(code, sample):
    if str(code or "").upper() == "IDR":
        return 1
    try:
        if abs(float(sample or 0)) >= 100:
            return 1
    except Exception:
        pass
    return 0.01


def _pressure_unit(unit):
    u = (unit or "psi").lower()
    return {"psi": "psi", "bar": "bar", "kpa": "kPa"}.get(u, "psi")


class MqttBridge:
    def __init__(self):
        self._lock = threading.RLock()
        self._cfg = _mqtt_cfg()
        self._client = None
        self._thread = None
        self._stop = threading.Event()
        self._connected = False
        self._last_error = None
        self._last_publish_ts = None
        self._discovery_sent = False
        self._disc_fp = None
        self._last_pubs = {}  # topic -> last payload string (skip unchanged publishes)
        self._last_pub_mono = 0.0
        self._prev_charge_state = None
        self._prev_battery = None
        self._battery_low_latched = False
        # Injected by server.py after import (avoids circular imports at module load).
        self.get_db_path = lambda: None
        self.decode = None
        self.control_caps = lambda: {}
        self.send_control = None
        self.get_vehicle = lambda: {}
        self.get_vehicle_id = lambda: ""
        self.get_summary = None
        self.get_cost_config = lambda: {}
        self.set_cost_config = None

    def status(self):
        with self._lock:
            return {
                "enabled": self._cfg["enabled"],
                "connected": self._connected,
                "last_error": self._last_error,
                "last_publish_ts": self._last_publish_ts,
                "topic_root": self._topic_root(),
                "vehicle_slug": self._vehicle_slug(),
            }

    def _vehicle_slug(self):
        """Prefer VIN (stable, globally unique), then plate, then CarLinko vehicle_id."""
        v = self.get_vehicle() or {}
        for cand in (v.get("vin"), v.get("plate"), self.get_vehicle_id()):
            s = _topic_slug(cand)
            if s:
                return s
        return "car"

    def _topic_root(self):
        """Effective MQTT namespace: {base_topic}/{vehicle_slug}."""
        base = (self._cfg.get("base_topic") or "j5").strip().strip("/") or "j5"
        return f"{base}/{self._vehicle_slug()}"

    def start(self, mqtt_creds=None):
        self.reload(mqtt_creds)

    def stop(self):
        self._stop.set()
        with self._lock:
            self._teardown_client()
        t = self._thread
        if t and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=5)
        self._thread = None

    def reload(self, mqtt_creds=None):
        """Apply new config; restart client/loop if needed."""
        cfg = _mqtt_cfg(mqtt_creds)
        self._stop.set()
        with self._lock:
            self._teardown_client()
            self._cfg = cfg
            self._discovery_sent = False
            self._disc_fp = None
            self._last_pubs = {}
            self._last_pub_mono = 0.0
        t = self._thread
        if t and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=5)
        self._stop = threading.Event()
        self._thread = None
        if not cfg["enabled"]:
            self._connected = False
            self._last_error = None
            return
        if not cfg["host"]:
            self._last_error = "mqtt.host is empty"
            print("mqtt_bridge:", self._last_error, flush=True)
            return
        self._thread = threading.Thread(target=self._run, name="mqtt-bridge", daemon=True)
        self._thread.start()

    def _teardown_client(self):
        c = self._client
        self._client = None
        self._connected = False
        if not c:
            return
        try:
            root = self._topic_root()
            c.publish(f"{root}/availability", "offline", qos=0, retain=True)
        except Exception:
            pass
        try:
            c.loop_stop()
        except Exception:
            pass
        try:
            c.disconnect()
        except Exception:
            pass

    def _run(self):
        cfg = self._cfg
        try:
            client = self._make_client(cfg)
        except Exception as e:
            self._last_error = str(e)[:200]
            print("mqtt_bridge: connect setup failed:", self._last_error, flush=True)
            return
        with self._lock:
            self._client = client
        try:
            client.connect(cfg["host"], cfg["port"], keepalive=60)
            client.loop_start()
        except Exception as e:
            self._last_error = str(e)[:200]
            print("mqtt_bridge: connect failed:", self._last_error, flush=True)
            self._teardown_client()
            return

        while not self._stop.wait(POLL_S):
            try:
                self._tick()
            except Exception as e:
                self._last_error = str(e)[:200]
                print("mqtt_bridge: tick error:", self._last_error, flush=True)
                traceback.print_exc()
        self._teardown_client()

    def _make_client(self, cfg):
        root = self._topic_root()
        # Include vehicle slug so two cars on one broker don't share a client_id.
        cid = f"carlinko-{root}".replace("/", "-")
        # paho v1 vs v2 callback API
        try:
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION1,
                client_id=cid,
                clean_session=True,
            )
        except Exception:
            client = mqtt.Client(client_id=cid, clean_session=True)
        if cfg["username"]:
            client.username_pw_set(cfg["username"], cfg["password"] or None)
        if cfg["tls"]:
            client.tls_set()
        client.will_set(f"{root}/availability", "offline", qos=0, retain=True)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        return client

    def _on_connect(self, client, userdata, flags, rc, *args):
        ok = (rc == 0)
        self._connected = ok
        if not ok:
            self._last_error = f"MQTT connect rc={rc}"
            print("mqtt_bridge:", self._last_error, flush=True)
            return
        self._last_error = None
        root = self._topic_root()
        # Subscribe all command topics under root/control/# (lock, climate, covers, …).
        client.subscribe(f"{root}/control/#")
        client.publish(f"{root}/availability", "online", qos=0, retain=True)
        self._discovery_sent = False
        print("mqtt_bridge: connected to", self._cfg["host"],
              "topics under", root, flush=True)

    def _on_disconnect(self, client, userdata, rc, *args):
        self._connected = False
        if rc != 0:
            self._last_error = f"MQTT disconnected rc={rc}"

    def _on_message(self, client, userdata, msg):
        try:
            topic = msg.topic or ""
            payload = (msg.payload or b"").decode("utf-8", "replace").strip()
            self._handle_command(topic, payload)
        except Exception as e:
            self._last_error = str(e)[:200]
            print("mqtt_bridge: command error:", self._last_error, flush=True)

    def _pub(self, topic, payload, retain=True):
        c = self._client
        if not c or not self._connected:
            return
        if not isinstance(payload, str):
            payload = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        c.publish(topic, payload, qos=0, retain=retain)

    def _pub_if_changed(self, topic, payload, retain=True, force=False):
        """Publish retained state only when the payload changed (or force=heartbeat)."""
        if not isinstance(payload, str):
            payload = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        if not force and self._last_pubs.get(topic) == payload:
            return False
        self._pub(topic, payload, retain=retain)
        self._last_pubs[topic] = payload
        return True

    def _device_info(self):
        v = self.get_vehicle() or {}
        vid = str(self.get_vehicle_id() or v.get("plate") or "car")
        name = v.get("model") or v.get("plate") or "CarLinko EV"
        return {
            "identifiers": [f"carlinko_{vid}"],
            "name": name,
            "manufacturer": "CarLinko",
            "model": v.get("model") or "EV",
            "suggested_area": "Garage",
        }

    def _publish_discovery(self, caps, out=None, cost=None):
        cfg = self._cfg
        pref = cfg["discovery_prefix"]
        base = self._topic_root()
        avail = {"topic": f"{base}/availability"}
        device = self._device_info()
        uniq = device["identifiers"][0]
        out = out or {}
        cost = cost or {}
        cur = (out.get("currency") or cost.get("currency") or {})
        code = (cur.get("code") or "IDR").upper()
        phev = out.get("powertrain") == "phev"
        direct_tpms = out.get("tyre_indirect") is False
        pun = _pressure_unit(out.get("tyre_unit"))
        step = _money_step(code, cost.get("tariff") if cost.get("tariff") is not None else out.get("tariff"))

        def disc(component, object_id, body):
            body = dict(body)
            body["availability"] = [avail]
            body["device"] = device
            body.setdefault("unique_id", f"{uniq}_{object_id}")
            body.setdefault("object_id", object_id)
            self._pub(f"{pref}/{component}/{uniq}/{object_id}/config", body, retain=True)

        def sensor(oid, name, topic, extra):
            body = {"name": name, "state_topic": f"{base}/{topic}"}
            body.update(extra)
            disc("sensor", oid, body)

        def binary(oid, name, topic, extra=None):
            body = {
                "name": name, "state_topic": f"{base}/{topic}",
                "payload_on": "ON", "payload_off": "OFF",
            }
            if extra:
                body.update(extra)
            disc("binary_sensor", oid, body)

        sensor("battery", "Battery", "sensor/battery", {
            "unit_of_measurement": "%", "device_class": "battery", "state_class": "measurement",
        })
        sensor("range", "Range", "sensor/range", {
            "unit_of_measurement": "km", "icon": "mdi:map-marker-distance",
            "state_class": "measurement",
        })
        sensor("odometer", "Odometer", "sensor/odometer", {
            "unit_of_measurement": "km", "icon": "mdi:counter", "state_class": "total_increasing",
        })
        sensor("volt12", "12V Battery", "sensor/volt12", {
            "unit_of_measurement": "V", "device_class": "voltage", "state_class": "measurement",
        })
        sensor("charge_power", "Charge Power", "sensor/charge_power", {
            "unit_of_measurement": "kW", "device_class": "power", "state_class": "measurement",
        })
        sensor("consumption", "Consumption", "sensor/consumption", {
            "unit_of_measurement": "kWh/100km", "icon": "mdi:lightning-bolt",
            "state_class": "measurement",
        })
        binary("charging", "Charging", "binary_sensor/charging", {"device_class": "battery_charging"})
        binary("online", "Online", "binary_sensor/online", {"device_class": "connectivity"})

        sensor("charge_remaining", "Charge remaining", "sensor/charge_remaining", {
            "unit_of_measurement": "min", "device_class": "duration", "state_class": "measurement",
        })
        sensor("charge_mode", "Charge mode", "sensor/charge_mode", {
            "device_class": "enum", "options": ["none", "ac", "dc"], "icon": "mdi:ev-plug-type2",
        })
        sensor("charge_state", "Charge state", "sensor/charge_state", {
            "device_class": "enum",
            "options": ["idle", "charging", "complete", "canceled", "hot", "stop"],
        })
        sensor("charge_session_kwh", "Charge session", "sensor/charge_session_kwh", {
            "unit_of_measurement": "kWh", "device_class": "energy", "state_class": "measurement",
        })
        sensor("charge_session_soc", "Charge session SoC", "sensor/charge_session_soc", {
            "unit_of_measurement": "%", "device_class": "battery", "state_class": "measurement",
        })
        binary("moving", "Moving", "binary_sensor/moving", {"device_class": "moving"})
        sensor("updated", "Updated", "sensor/updated", {"device_class": "timestamp"})

        if phev:
            sensor("fuel", "Fuel", "sensor/fuel", {
                "unit_of_measurement": "%", "icon": "mdi:gas-station", "state_class": "measurement",
            })
            sensor("fuel_range", "Fuel range", "sensor/fuel_range", {
                "unit_of_measurement": "km", "icon": "mdi:map-marker-distance",
                "state_class": "measurement",
            })
            sensor("total_range", "Total range", "sensor/total_range", {
                "unit_of_measurement": "km", "icon": "mdi:map-marker-distance",
                "state_class": "measurement",
            })
            sensor("fuel_consumption", "Fuel consumption", "sensor/fuel_consumption", {
                "unit_of_measurement": "L/100km", "icon": "mdi:fuel", "state_class": "measurement",
            })

        sensor("tyre_status", "Tyre status", "sensor/tyre_status", {
            "device_class": "enum", "options": ["Normal", "Check tyres"], "icon": "mdi:car-tire-alert",
        })
        binary("tyres_ok", "Tyre problem", "binary_sensor/tyres_ok", {"device_class": "problem"})
        if direct_tpms:
            for oid, label in TYRE_POS:
                sensor(f"tyre_{oid}", label, f"sensor/tyre_{oid}", {
                    "unit_of_measurement": pun, "device_class": "pressure", "state_class": "measurement",
                })
                sensor(f"tyre_{oid}_temp", f"{label} temp", f"sensor/tyre_{oid}_temp", {
                    "unit_of_measurement": "°C", "device_class": "temperature",
                    "state_class": "measurement",
                })

        binary("door", "Any door", "binary_sensor/door", {"device_class": "door"})
        for oid, label, _bit in DOOR_BITS:
            binary(oid, label, f"binary_sensor/{oid}", {"device_class": "door"})
        binary("seat_heat_left", "Seat heat left", "binary_sensor/seat_heat_left",
               {"device_class": "heat"})
        binary("seat_heat_right", "Seat heat right", "binary_sensor/seat_heat_right",
               {"device_class": "heat"})
        binary("seat_vent_left", "Seat vent left", "binary_sensor/seat_vent_left",
               {"icon": "mdi:car-seat"})
        binary("seat_vent_right", "Seat vent right", "binary_sensor/seat_vent_right",
               {"icon": "mdi:car-seat"})
        binary("defrost", "Defrost", "binary_sensor/defrost", {"icon": "mdi:car-defrost-front"})
        sensor("hv_state", "HV state", "sensor/hv_state", {
            "device_class": "enum", "options": ["off", "lv", "ready", "unknown"],
            "icon": "mdi:car-electric",
        })
        sensor("volt12_status", "12V status", "sensor/volt12_status", {
            "device_class": "enum", "options": ["ok", "low", "critical"],
        })
        sensor("volt12_min7d", "12V 7-day min", "sensor/volt12_min7d", {
            "unit_of_measurement": "V", "device_class": "voltage", "state_class": "measurement",
        })
        sensor("wltc_range", "Rated range", "sensor/wltc_range", {
            "unit_of_measurement": "km", "icon": "mdi:map-marker-distance",
            "state_class": "measurement",
        })
        sensor("parked_drain", "Parked drain", "sensor/parked_drain", {
            "unit_of_measurement": "%/d", "icon": "mdi:battery-minus", "state_class": "measurement",
        })

        sensor("km_today", "Km today", "sensor/km_today", {
            "unit_of_measurement": "km", "state_class": "measurement", "icon": "mdi:road-variant",
        })
        sensor("km_week", "Km week", "sensor/km_week", {
            "unit_of_measurement": "km", "state_class": "measurement", "icon": "mdi:road-variant",
        })
        sensor("km_month", "Km month", "sensor/km_month", {
            "unit_of_measurement": "km", "state_class": "measurement", "icon": "mdi:road-variant",
        })
        sensor("energy_today", "Energy today", "sensor/energy_today", {
            "unit_of_measurement": "kWh", "device_class": "energy", "state_class": "measurement",
        })
        sensor("energy_left", "Energy left", "sensor/energy_left", {
            "unit_of_measurement": "kWh", "device_class": "energy", "state_class": "measurement",
        })
        sensor("efficiency_rating", "Efficiency rating", "sensor/efficiency_rating", {
            "device_class": "enum", "options": ["optimal", "normal", "boros"],
        })
        sensor("avg_speed", "Average speed", "sensor/avg_speed", {
            "unit_of_measurement": "km/h", "device_class": "speed", "state_class": "measurement",
        })

        sensor("charges_week", "Charges this week", "sensor/charges_week", {
            "state_class": "measurement", "icon": "mdi:ev-station",
        })
        sensor("charges_month", "Charges this month", "sensor/charges_month", {
            "state_class": "measurement", "icon": "mdi:ev-station",
        })
        sensor("charge_month_kwh", "Charge kWh this month", "sensor/charge_month_kwh", {
            "unit_of_measurement": "kWh", "device_class": "energy", "state_class": "measurement",
        })
        sensor("charge_month_cost", "Charge cost this month", "sensor/charge_month_cost", {
            "unit_of_measurement": code, "device_class": "monetary", "state_class": "total",
            "icon": "mdi:cash",
        })

        sensor("lifetime_km", "Lifetime km", "sensor/lifetime_km", {
            "unit_of_measurement": "km", "state_class": "total_increasing", "icon": "mdi:counter",
        })
        sensor("lifetime_kwh", "Lifetime kWh billed", "sensor/lifetime_kwh", {
            "unit_of_measurement": "kWh", "device_class": "energy", "state_class": "total_increasing",
        })
        sensor("lifetime_cost", "Lifetime cost", "sensor/lifetime_cost", {
            "unit_of_measurement": code, "device_class": "monetary", "state_class": "total",
        })
        sensor("lifetime_saved", "Lifetime saved", "sensor/lifetime_saved", {
            "unit_of_measurement": code, "device_class": "monetary", "state_class": "total",
        })
        sensor("liters_saved", "Litres saved", "sensor/liters_saved", {
            "unit_of_measurement": "L", "state_class": "total", "icon": "mdi:gas-station-off",
        })
        sensor("co2_saved", "CO2 saved", "sensor/co2_saved", {
            "unit_of_measurement": "kg", "device_class": "weight", "state_class": "total",
        })

        sensor("running_cost", "Running cost", "sensor/running_cost", {
            "unit_of_measurement": f"{code}/km", "icon": "mdi:cash", "state_class": "measurement",
        })
        sensor("month_cost_est", "Month cost estimate", "sensor/month_cost_est", {
            "unit_of_measurement": code, "device_class": "monetary", "state_class": "total",
        })
        sensor("days_to_charge", "Days to charge", "sensor/days_to_charge", {
            "unit_of_measurement": "d", "icon": "mdi:calendar-clock", "state_class": "measurement",
        })
        sensor("real_range", "Real-world range", "sensor/real_range", {
            "unit_of_measurement": "km", "icon": "mdi:map-marker-distance",
            "state_class": "measurement",
        })
        sensor("rated_range", "Car-rated range", "sensor/rated_range", {
            "unit_of_measurement": "km", "icon": "mdi:map-marker-distance",
            "state_class": "measurement",
        })
        sensor("battery_usable", "Usable battery", "sensor/battery_usable", {
            "unit_of_measurement": "kWh", "device_class": "energy", "state_class": "measurement",
        })
        sensor("battery_cycles", "Battery cycles", "sensor/battery_cycles", {
            "state_class": "measurement", "icon": "mdi:battery-sync",
        })
        sensor("days_since_full", "Days since full charge", "sensor/days_since_full", {
            "unit_of_measurement": "d", "icon": "mdi:battery-charging-100",
            "state_class": "measurement",
        })
        sensor("battery_care", "Battery care", "sensor/battery_care", {
            "device_class": "enum", "options": ["ok", "due", "overdue", "unknown"],
        })
        binary("balance_due", "Balance due", "binary_sensor/balance_due")

        disc("number", "tariff", {
            "name": "Charging tariff",
            "state_topic": f"{base}/number/tariff",
            "command_topic": f"{base}/control/tariff/set",
            "min": 0, "max": 10000000, "step": step, "mode": "box",
            "unit_of_measurement": f"{code}/kWh", "icon": "mdi:currency-usd",
        })
        disc("number", "petrol_price", {
            "name": "Petrol price",
            "state_topic": f"{base}/number/petrol_price",
            "command_topic": f"{base}/control/petrol_price/set",
            "min": 0, "max": 10000000, "step": step, "mode": "box",
            "unit_of_measurement": f"{code}/L", "icon": "mdi:gas-station",
        })
        disc("number", "petrol_kml", {
            "name": "Petrol economy",
            "state_topic": f"{base}/number/petrol_kml",
            "command_topic": f"{base}/control/petrol_kml/set",
            "min": 0.1, "max": 100, "step": 0.1, "mode": "box",
            "unit_of_measurement": "km/L", "icon": "mdi:car-speed-limiter",
        })

        caps = caps or {}
        if caps.get("lock"):
            disc("lock", "lock", {
                "name": "Lock",
                "state_topic": f"{base}/lock/state",
                "command_topic": f"{base}/control/lock/set",
                "payload_lock": "LOCK", "payload_unlock": "UNLOCK",
                "state_locked": "LOCKED", "state_unlocked": "UNLOCKED",
            })
        ac = caps.get("ac") or {}
        if ac.get("switch"):
            disc("climate", "climate", {
                "name": "Climate",
                "mode_command_topic": f"{base}/control/climate/set",
                "mode_state_topic": f"{base}/climate/mode",
                "modes": ["off", "cool"],
                "current_temperature_topic": f"{base}/climate/temperature",
                "temperature_unit": "C",
                "action_topic": f"{base}/climate/action",
            })
        win = caps.get("windows") or {}
        if win.get("open") or win.get("close") or win.get("vent"):
            disc("cover", "windows", {
                "name": "Windows",
                "state_topic": f"{base}/cover/windows/state",
                "command_topic": f"{base}/control/windows/set",
                "payload_open": "OPEN", "payload_close": "CLOSE", "payload_stop": "VENT",
                "state_open": "open", "state_closed": "closed",
                "device_class": "awning",
            })
        roof = caps.get("sunroof") or {}
        if roof.get("open") or roof.get("tilt"):
            disc("cover", "sunroof", {
                "name": "Sunroof",
                "state_topic": f"{base}/cover/sunroof/state",
                "command_topic": f"{base}/control/sunroof/set",
                "payload_open": "OPEN", "payload_close": "CLOSE", "payload_stop": "TILT",
                "state_open": "open", "state_closed": "closed",
                "device_class": "window",
            })
        if caps.get("liftgate") or caps.get("trunk"):
            disc("cover", "liftgate", {
                "name": "Liftgate",
                "state_topic": f"{base}/cover/liftgate/state",
                "command_topic": f"{base}/control/liftgate/set",
                "payload_open": "OPEN", "payload_close": "CLOSE",
                "state_open": "open", "state_closed": "closed",
                "device_class": "garage",
            })
        if caps.get("find"):
            disc("button", "find", {
                "name": "Find car",
                "command_topic": f"{base}/control/find/set",
                "payload_press": "PRESS",
            })
        if caps.get("charging"):
            disc("button", "charge_stop", {
                "name": "Stop charging",
                "command_topic": f"{base}/control/charge_stop/set",
                "payload_press": "PRESS",
            })
        self._discovery_sent = True
        self._disc_fp = (phev, direct_tpms, code, step)

    def _disc_fingerprint(self, out, cost):
        out = out or {}
        cost = cost or {}
        cur = (out.get("currency") or cost.get("currency") or {})
        code = (cur.get("code") or "IDR").upper()
        tariff = cost.get("tariff") if cost.get("tariff") is not None else out.get("tariff")
        return (out.get("powertrain") == "phev", out.get("tyre_indirect") is False, code,
                _money_step(code, tariff))

    def _state_pubs(self, base, out, cost):
        chg = out.get("charging") or {}
        energy = out.get("energy") or {}
        km = out.get("km") or {}
        lf = out.get("lifetime") or {}
        ins = out.get("insights") or {}
        health = out.get("health") or {}
        care = out.get("battery_care") or {}
        fuel = out.get("fuel") or {}
        drain = out.get("drain") or {}
        battery = out.get("battery")
        cap = out.get("battery_kwh")
        energy_left = None
        if battery is not None and cap:
            energy_left = round(battery / 100.0 * cap, 2)
        cstate = chg.get("state")
        cmode = chg.get("mode")
        if not cmode:
            cmode = "none"
        charging = bool(chg.get("active"))
        rate = chg.get("rate_kw") if charging else 0
        if rate is None:
            rate = 0
        cons = energy.get("consumption") or energy.get("week_consumption")
        ac_temp = out.get("ac_temp_c")
        temp_ok = isinstance(ac_temp, int) and 16 <= ac_temp <= 30
        online = bool(out.get("online"))
        tyres_check = (out.get("tyre_status") or "").lower().find("check") >= 0
        try:
            doors = int(out.get("doors") or 0)
        except (TypeError, ValueError):
            doors = 0
        sh = out.get("seat_heat") or [0, 0]
        sv = out.get("seat_vent") or [0, 0]
        hv = out.get("hv_state")
        hv_s = HV_STATE.get(hv, "unknown") if hv is not None else ""
        chg_s = CHARGE_STATE.get(cstate, "") if cstate is not None else ""
        tpms = out.get("tpms") or []

        pubs = {
            f"{base}/sensor/battery": _n(battery),
            f"{base}/sensor/range": _n(out.get("range_km")),
            f"{base}/sensor/odometer": _n(out.get("odometer")),
            f"{base}/sensor/volt12": _n(out.get("volt12")),
            f"{base}/sensor/charge_power": _n(rate),
            f"{base}/sensor/consumption": _n(cons) if cons else "",
            f"{base}/binary_sensor/charging": _on(charging),
            f"{base}/binary_sensor/online": _on(online),
            f"{base}/lock/state": "UNLOCKED" if out.get("unlocked") else "LOCKED",
            f"{base}/climate/mode": "cool" if out.get("ac_on") else "off",
            f"{base}/climate/action": "cooling" if out.get("ac_on") else "off",
            f"{base}/climate/temperature": _n(ac_temp) if temp_ok else "",
            f"{base}/cover/windows/state": "open" if out.get("windows") else "closed",
            f"{base}/cover/sunroof/state": "open" if out.get("sunroof_open") else "closed",
            f"{base}/cover/liftgate/state": "open" if out.get("trunk_open") else "closed",
            f"{base}/availability": "online" if online else "offline",
            f"{base}/sensor/charge_remaining": _n(chg.get("remaining_min")),
            f"{base}/sensor/charge_mode": cmode,
            f"{base}/sensor/charge_state": chg_s,
            f"{base}/sensor/charge_session_kwh": _n(chg.get("session_kwh")),
            f"{base}/sensor/charge_session_soc": _n(chg.get("soc")),
            f"{base}/binary_sensor/moving": _on(out.get("moving")),
            f"{base}/sensor/updated": _iso(out.get("updated_ts")),
            f"{base}/sensor/tyre_status": out.get("tyre_status") or "",
            f"{base}/binary_sensor/tyres_ok": _on(tyres_check),
            f"{base}/binary_sensor/door": _on(doors != 0),
            f"{base}/binary_sensor/seat_heat_left": _on(len(sh) > 0 and sh[0]),
            f"{base}/binary_sensor/seat_heat_right": _on(len(sh) > 1 and sh[1]),
            f"{base}/binary_sensor/seat_vent_left": _on(len(sv) > 0 and sv[0]),
            f"{base}/binary_sensor/seat_vent_right": _on(len(sv) > 1 and sv[1]),
            f"{base}/binary_sensor/defrost": _on(out.get("defrost_front")),
            f"{base}/sensor/hv_state": hv_s,
            f"{base}/sensor/volt12_status": out.get("volt12_status") or "",
            f"{base}/sensor/volt12_min7d": _n(out.get("volt12_min7d")),
            f"{base}/sensor/wltc_range": _n(out.get("wltc_range_km")),
            f"{base}/sensor/parked_drain": _n(drain.get("per_day")) if drain else "",
            f"{base}/sensor/km_today": _n(km.get("today")),
            f"{base}/sensor/km_week": _n(km.get("week")),
            f"{base}/sensor/km_month": _n(km.get("month")),
            f"{base}/sensor/energy_today": _n(energy.get("today_kwh")),
            f"{base}/sensor/energy_left": _n(energy_left),
            f"{base}/sensor/efficiency_rating": energy.get("rating") or "",
            f"{base}/sensor/avg_speed": _n(out.get("avg_speed")),
            f"{base}/sensor/charges_week": _n(chg.get("week")),
            f"{base}/sensor/charges_month": _n(chg.get("month")),
            f"{base}/sensor/charge_month_kwh": _n(chg.get("month_kwh")),
            f"{base}/sensor/charge_month_cost": _n(chg.get("month_cost")),
            f"{base}/sensor/lifetime_km": _n(lf.get("km")),
            f"{base}/sensor/lifetime_kwh": _n(lf.get("kwh_billed")),
            f"{base}/sensor/lifetime_cost": _n(lf.get("cost")),
            f"{base}/sensor/lifetime_saved": _n(lf.get("saved")),
            f"{base}/sensor/liters_saved": _n(lf.get("liters_saved")),
            f"{base}/sensor/co2_saved": _n(lf.get("co2_saved")),
            f"{base}/sensor/running_cost": _n(ins.get("rp_per_km")),
            f"{base}/sensor/month_cost_est": _n(ins.get("month_cost_est")),
            f"{base}/sensor/days_to_charge": _n(ins.get("days_to_charge")),
            f"{base}/sensor/real_range": _n(ins.get("real_range")),
            f"{base}/sensor/rated_range": _n(ins.get("rated_range")),
            f"{base}/sensor/battery_usable": _n(health.get("usable_kwh")),
            f"{base}/sensor/battery_cycles": _n(health.get("cycles")),
            f"{base}/sensor/days_since_full": _n(care.get("days_since_full")),
            f"{base}/sensor/battery_care": care.get("state") or "",
            f"{base}/binary_sensor/balance_due": _on(care.get("balance_due")),
            f"{base}/number/tariff": _n(cost.get("tariff")),
            f"{base}/number/petrol_price": _n(cost.get("petrol_price")),
            f"{base}/number/petrol_kml": _n(cost.get("petrol_kml")),
        }
        for oid, _label, bit in DOOR_BITS:
            pubs[f"{base}/binary_sensor/{oid}"] = _on(bool(doors & bit))
        if out.get("powertrain") == "phev":
            pubs[f"{base}/sensor/fuel"] = _n(fuel.get("pct"))
            pubs[f"{base}/sensor/fuel_range"] = _n(fuel.get("range_km"))
            pubs[f"{base}/sensor/total_range"] = _n(fuel.get("total_range_km"))
            pubs[f"{base}/sensor/fuel_consumption"] = _n(fuel.get("l_100"))
        if out.get("tyre_indirect") is False:
            for i, (oid, _label) in enumerate(TYRE_POS):
                wheel = tpms[i] if i < len(tpms) else {}
                pubs[f"{base}/sensor/tyre_{oid}"] = _n(wheel.get("psi"))
                pubs[f"{base}/sensor/tyre_{oid}_temp"] = _n(wheel.get("temp"))
        return pubs, battery, cstate

    def _tick(self):
        if not self._connected:
            return
        try:
            caps = self.control_caps() or {}
        except Exception:
            caps = {}
        out = {}
        if self.get_summary:
            try:
                out = self.get_summary() or {}
            except Exception as e:
                self._last_error = str(e)[:200]
                out = {}
        try:
            cost = self.get_cost_config() or {}
        except Exception:
            cost = {}

        fp = self._disc_fingerprint(out, cost)
        if not self._discovery_sent or fp != self._disc_fp:
            self._publish_discovery(caps, out, cost)

        base = self._topic_root()
        if not out.get("updated") and out.get("updated_ts") is None:
            self._pub_if_changed(f"{base}/binary_sensor/online", "OFF")
            self._pub_if_changed(f"{base}/availability", "offline", retain=True)
            for key in ("tariff", "petrol_price", "petrol_kml"):
                self._pub_if_changed(f"{base}/number/{key}", _n(cost.get(key)))
            return

        pubs, battery, cstate = self._state_pubs(base, out, cost)
        now = time.monotonic()
        due = (now - self._last_pub_mono) >= HEARTBEAT_S
        any_changed = any(self._last_pubs.get(t) != p for t, p in pubs.items())
        if any_changed or due:
            for topic, payload in pubs.items():
                self._pub_if_changed(topic, payload, retain=True, force=due)
            self._last_pub_mono = now
            self._last_publish_ts = int(time.time())

        if self._prev_charge_state == 1 and cstate == 2:
            self._pub(f"{base}/event/charge_complete",
                      {"battery": battery, "ts": int(time.time())}, retain=False)
        if battery is not None:
            try:
                batt = float(battery)
            except (TypeError, ValueError):
                batt = None
            if batt is not None:
                if batt < BATTERY_LOW_PCT and not self._battery_low_latched:
                    self._pub(f"{base}/event/battery_low",
                              {"battery": battery, "ts": int(time.time())}, retain=False)
                    self._battery_low_latched = True
                elif batt >= BATTERY_LOW_PCT + 5:
                    self._battery_low_latched = False

        self._prev_charge_state = cstate
        self._prev_battery = battery

    def _fire_action(self, action_key):
        """Init 77 then fire mapped opcode. Returns result dict."""
        if not self.send_control:
            return {"ok": False, "error": "send_control not wired"}
        ops = load_opcodes()
        code = ops.get(action_key)
        if not code:
            return {"ok": False, "error": f"no opcode for {action_key}"}
        init = self.send_control("77", 20)
        time.sleep(0.6)
        d = self.send_control(code, 20)
        ok = str(d.get("code")) == "0000"
        return {"ok": ok, "action": action_key, "opcode": code,
                "code": d.get("code"), "msg": d.get("msg"), "init": init.get("code"),
                "ts": int(time.time())}

    def _ack(self, result):
        base = self._topic_root()
        self._pub(f"{base}/control/result", result, retain=False)

    def _handle_command(self, topic, payload):
        base = self._topic_root()
        prefix = f"{base}/control/"
        if not topic.startswith(prefix) or not topic.endswith("/set"):
            return
        mid = topic[len(prefix):-len("/set")]  # e.g. lock, climate, windows, charge_stop
        if mid in ("tariff", "petrol_price", "petrol_kml"):
            if not self.set_cost_config:
                self._ack({"ok": False, "error": "set_cost_config not wired"})
                return
            result = self.set_cost_config(mid, payload)
            self._ack(result)
            if result.get("ok"):
                base_n = self._topic_root()
                self._pub_if_changed(f"{base_n}/number/{mid}", _n(result.get("value")),
                                     retain=True, force=True)
            return
        pl = payload.upper()
        action = None

        if mid == "lock":
            if pl == "LOCK":
                action = "lock"
            elif pl == "UNLOCK":
                action = "unlock"
        elif mid == "climate":
            # HA may send mode string or JSON {"mode":"cool"|"off",...}
            mode = pl
            if payload.startswith("{"):
                try:
                    mode = str(json.loads(payload).get("mode") or "").upper()
                except Exception:
                    mode = pl
            if mode in ("OFF", "0"):
                action = "acOff"
            elif mode in ("COOL", "HEAT", "AUTO", "ON"):
                action = "acOn"
        elif mid == "windows":
            if pl == "OPEN":
                action = "winOpen"
            elif pl == "CLOSE":
                action = "winClose"
            elif pl in ("VENT", "STOP"):
                action = "winVent"
        elif mid == "sunroof":
            if pl == "OPEN":
                action = "roofOpen"
            elif pl == "CLOSE":
                action = "roofClose"
            elif pl in ("TILT", "STOP"):
                action = "roofTilt"
        elif mid == "liftgate":
            if pl == "OPEN":
                action = "liftOpen"
            # CLOSE not mapped distinctly in CTRL_ACTIONS; ignore
        elif mid == "find":
            if pl in ("PRESS", "ON", "1"):
                action = "find"
        elif mid == "charge_stop":
            if pl in ("PRESS", "ON", "1"):
                action = "chgStop"

        if not action:
            self._ack({"ok": False, "error": "unknown command", "topic": topic, "payload": payload})
            return

        # Capability gate
        try:
            caps = self.control_caps() or {}
        except Exception:
            caps = {}
        if not self._action_allowed(action, caps):
            self._ack({"ok": False, "error": "not supported by this car", "action": action})
            return

        result = self._fire_action(action)
        self._ack(result)

    @staticmethod
    def _action_allowed(action, caps):
        if action in ("lock", "unlock"):
            return bool(caps.get("lock"))
        if action in ("acOn", "acOff"):
            return bool((caps.get("ac") or {}).get("switch"))
        if action == "winOpen":
            return bool((caps.get("windows") or {}).get("open"))
        if action == "winClose":
            return bool((caps.get("windows") or {}).get("close"))
        if action == "winVent":
            return bool((caps.get("windows") or {}).get("vent"))
        if action == "roofOpen":
            return bool((caps.get("sunroof") or {}).get("open"))
        if action == "roofClose":
            return bool((caps.get("sunroof") or {}).get("open"))
        if action == "roofTilt":
            return bool((caps.get("sunroof") or {}).get("tilt"))
        if action == "liftOpen":
            return bool(caps.get("liftgate") or caps.get("trunk"))
        if action == "find":
            return bool(caps.get("find"))
        if action == "chgStop":
            return bool(caps.get("charging"))
        return False


# Singleton used by server.py
bridge = MqttBridge()
