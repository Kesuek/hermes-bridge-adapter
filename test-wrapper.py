#!/usr/bin/env python3
"""
test-wrapper — Simulierter Bridge-Adapter Wrapper für Testzwecke.

Registriert eine Test-Bridge per Manifest (registry/<name>.yaml), schreibt
eine Nachricht in die Inbox, liest die Outbox und zeigt den Status an.
Demonstriert den kompletten Bridge-Lebenszyklus (T-050) ohne echten Dienst.

Nutzung:
  test-wrapper.py register <name>   # Manifest ablegen (Bridge anmelden)
  test-wrapper.py send <name> <ziel> <text>   # Nachricht in Inbox schreiben
  test-wrapper.py drain <name>      # Outbox lesen + anzeigen (statt Senden)
  test-wrapper.py status <name>     # Status-Datei schreiben
  test-wrapper.py unregister <name> # Manifest löschen (Bridge abmelden)
  test-wrapper.py list              # angemeldete Bridges anzeigen
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
    print(f"✓ Manifest angelegt: {path} — Bridge '{name}' angemeldet")
    print("  Adapter registriert die Bridge innerhalb ~5s (Registry-Poll).")


def unregister(name: str):
    path = REGISTRY / f"{name}.yaml"
    if path.exists():
        path.unlink()
        print(f"✓ Manifest entfernt: {path} — Bridge '{name}' abgemeldet")
        print("  Adapter räumt status/ + media/ auf innerhalb ~5s.")
    else:
        print(f"! Kein Manifest für '{name}' gefunden")


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
    print(f"✓ Inbox-Nachricht geschrieben: {path}")
    print(f"  text='{text}' → Adapter verarbeitet sie als MessageEvent.")


def drain(name: str):
    outbox = OUTBOX / name
    if not outbox.exists():
        print(f"! Kein outbox/{name}/ — Adapter hat nichts gesendet.")
        return
    files = sorted(outbox.glob("*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        print(f"• outbox/{name}/ ist leer")
        return
    for f in files:
        data = json.loads(f.read_text("utf-8"))
        print(f"  [{f.name}] target={data.get('target')} "
              f"typing={data.get('typing')} text={data.get('text', '')[:60]!r}")
        f.unlink(missing_ok=True)
    print(f"✓ {len(files)} Outbox-Nachricht(en) ausgelesen + gelöscht")


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
    print(f"✓ Status geschrieben: connected={connected}")


def list_bridges():
    if not REGISTRY.exists():
        print("• registry/ existiert nicht — keine Bridges angemeldet")
        return
    names = sorted(p.stem for p in REGISTRY.glob("*.yaml"))
    if not names:
        print("• Keine Bridges angemeldet (registry/ leer)")
        return
    print(f"Angemeldete Bridges ({len(names)}):")
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
        print(f"! Unbekannter Befehl: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
