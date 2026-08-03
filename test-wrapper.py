#!/usr/bin/env python3
"""
test-wrapper — Simulated bridge adapter wrapper for testing.

Registers a test bridge via a manifest (registry/<name>.yaml), writes a
message into the inbox, reads the outbox, and reports status. Demonstrates
the full bridge lifecycle (T-050) without needing a real external service.

Usage:
  test-wrapper.py register <name>   # write manifest (register bridge)
  test-wrapper.py send <name> <target> <text>   # write inbox message
  test-wrapper.py drain <name>      # read outbox + display (instead of sending)
  test-wrapper.py status <name>     # write status file
  test-wrapper.py unregister <name> # delete manifest (unregister bridge)
  test-wrapper.py list              # show registered bridges
"""

import json
import sys
import time
import uuid
from pathlib import Path

BRIDGE_DIR = Path(__file__).resolve().parent
REGISTRY = BRIDGE_DIR / "registry"
INBOX = BRIDGE_DIR / "inbox"
OUTBOX = BRIDGE_DIR / "outbox"
STATUS = BRIDGE_DIR / "status"
MEDIA = BRIDGE_DIR / "media"


def _manifest(name: str) -> dict:
    return {
        "name": name,
        "service": f"test-{name}",
        "host": "localhost",
        "target_format": ["email", "chat_id"],
        "capabilities": ["text"],
    }


def register(name: str):
    REGISTRY.mkdir(parents=True, exist_ok=True)
    path = REGISTRY / f"{name}.yaml"
    path.write_text(json.dumps(_manifest(name), indent=2), "utf-8")
    print(f"✓ Manifest written: {path} — bridge '{name}' registered")
    print("  Adapter registers the bridge within ~5s (registry poll).")


def unregister(name: str):
    path = REGISTRY / f"{name}.yaml"
    if path.exists():
        path.unlink()
        print(f"✓ Manifest removed: {path} — bridge '{name}' unregistered")
        print("  Adapter cleans up status/ + media/ within ~5s.")
    else:
        print(f"! No manifest found for '{name}'")


def send(name: str, target: str, text: str):
    inbox = INBOX / name
    inbox.mkdir(parents=True, exist_ok=True)
    msg = {
        "bridge": name,
        "id": str(uuid.uuid4()),
        "sender": "test-sender",
        "sender_name": "Test-Sender",
        "text": text,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "attachments": [],
        "chat": {
            "id": f"{name}:{target}",
            "type": "direct",
            "name": "Test-Chat",
        },
    }
    path = inbox / f"{msg['id']}.json"
    path.write_text(json.dumps(msg, ensure_ascii=False, indent=2), "utf-8")
    print(f"✓ Inbox message written: {path}")
    print(f"  text='{text}' → adapter processes it as a MessageEvent.")


def drain(name: str):
    outbox = OUTBOX / name
    if not outbox.exists():
        print(f"! No outbox/{name}/ — adapter has sent nothing.")
        return
    files = sorted(outbox.glob("*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        print(f"• outbox/{name}/ is empty")
        return
    for f in files:
        data = json.loads(f.read_text("utf-8"))
        print(f"  [{f.name}] target={data.get('target')} "
              f"typing={data.get('typing')} text={data.get('text', '')[:60]!r}")
        f.unlink(missing_ok=True)
    print(f"✓ {len(files)} outbox message(s) read + deleted")


def status(name: str, connected: bool = True):
    sdir = STATUS / name
    sdir.mkdir(parents=True, exist_ok=True)
    data = {
        "bridge": name,
        "connected": connected,
        "last_seen": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "error": None,
    }
    (sdir / "status.json").write_text(json.dumps(data, indent=2), "utf-8")
    print(f"✓ Status written: connected={connected}")


def list_bridges():
    if not REGISTRY.exists():
        print("• registry/ does not exist — no bridges registered")
        return
    names = sorted(p.stem for p in REGISTRY.glob("*.yaml"))
    if not names:
        print("• No bridges registered (registry/ empty)")
        return
    print(f"Registered bridges ({len(names)}):")
    for n in names:
        st = STATUS / n / "status.json"
        conn = "?"
        if st.exists():
            try:
                conn = json.loads(st.read_text("utf-8")).get("connected")
            except Exception:
                conn = "?"
        print(f"  • {n}  connected={conn}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "register":
        register(sys.argv[2])
    elif cmd == "unregister":
        unregister(sys.argv[2])
    elif cmd == "send":
        send(sys.argv[2], sys.argv[3], sys.argv[4])
    elif cmd == "drain":
        drain(sys.argv[2])
    elif cmd == "status":
        status(sys.argv[2], len(sys.argv) > 3 and sys.argv[3] != "down")
    elif cmd == "list":
        list_bridges()
    else:
        print(f"! Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
