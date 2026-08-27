"""Fire a CarLinko remote-control opcode (init 77, then command).

Usage:
  python send_control.py acOn
  python send_control.py 740700
  python send_control.py acTemp 22
"""
import os, sys, json, time, hmac, hashlib, base64, argparse
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import requests
import auth
import mqtt_bridge as MB

HERE = os.path.dirname(os.path.abspath(__file__))


def post(opcode, timeout=20):
    c = auth.cfg()
    vid = str(c.get("vehicle_id") or auth.VEHICLE_ID)
    dsn = str(c.get("device_sn") or auth.DEVICE_SN or "")
    body = {"vehicleId": vid, "deviceSn": dsn, "data": str(opcode), "timeOut": int(timeout)}
    tok = open(auth.TOKEN_FILE, encoding="utf-8").read().strip() if os.path.exists(auth.TOKEN_FILE) else ""

    def _once(tok):
        ts = auth.now_ms()
        ordered = {k: v for k, v in sorted({**body, "timestamp": ts}.items())}
        msg = json.dumps(ordered, separators=(",", ":"), ensure_ascii=False).encode()
        sig = base64.b64encode(hmac.new(auth.SIGN_KEY, msg, hashlib.sha256).digest()).decode()
        h = {"timestamp": ts, "signature": sig, "user-agent": "Dart/3.10 (dart:io)",
             "content-type": "application/json", "language": "en", "token": tok}
        return requests.post(
            auth.api_base() + "/user/vehicle/remoteControl",
            data=json.dumps(body, separators=(",", ":"), ensure_ascii=False),
            headers=h, timeout=timeout + 8,
        ).json()

    d = _once(tok)
    if str(d.get("code")) in ("9997", "40001", "40003", "401", "1001", "1002"):
        print("token stale, re-login...")
        tok = auth.login()
        d = _once(tok)
    return d


def resolve(name_or_hex, temp=None):
    s = (name_or_hex or "").strip()
    if s.lower() in ("actemp", "temp"):
        op = MB.build_ac_temp_opcode(temp)
        if not op:
            raise SystemExit("acTemp needs a celsius value, e.g. send_control.py acTemp 22")
        return op, "acTemp"
    ops = MB.load_opcodes()
    if s in ops:
        return ops[s], s
    if all(ch in "0123456789abcdefABCDEF" for ch in s) and 4 <= len(s) <= 16:
        return s, s
    raise SystemExit(f"unknown action or opcode: {s!r}\nKnown: {', '.join(sorted(ops))}")


def main():
    ap = argparse.ArgumentParser(description="Send CarLinko remoteControl opcode")
    ap.add_argument("action", help="named action (acOn) or hex (741001) or acTemp")
    ap.add_argument("temp", nargs="?", type=float, help="celsius when action is acTemp")
    ap.add_argument("--no-init", action="store_true", help="skip init 77")
    ap.add_argument("--timeout", type=int, default=20)
    args = ap.parse_args()
    code, label = resolve(args.action, args.temp)
    print(f"action={label}  opcode={code}")
    if not args.no_init:
        print("init 77 ->", json.dumps(post("77", args.timeout), ensure_ascii=False))
        time.sleep(0.6)
    print(f"{label} ->", json.dumps(post(code, args.timeout), ensure_ascii=False))


if __name__ == "__main__":
    main()
