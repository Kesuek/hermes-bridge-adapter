#!/usr/bin/env python3
"""
imsg-wrapper — Bridge-Adapter Wrapper für iMessage.

Steuert imsg per SSH auf einem entfernten macOS-Host.
Konfigurierbar via Umgebungsvariablen (siehe unten).

- Outbox-Poller: Liest outbox/imsg/*.json, sendet per `imessage` SSH
- Inbound-Stream: imsg watch --json --receptions → inbox/imsg/
- Attachments: media/imsg/incoming/ + outgoing/
- Status: status/imsg/status.json

Umgebungsvariablen:
  BRIDGE_DIR         Pfad zum Bridge-Verzeichnis (default: ~/.hermes/bridge)
  IMSG_SSH_HOST      SSH-Ziel für imsg (default: user@mac-host.local)
  BRIDGE_POLL_INTERVAL Poll-Intervall in Sekunden (default: 1.0)
"""

import json
import logging
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("imsg-wrapper")

BRIDGE_DIR = Path(os.environ.get("BRIDGE_DIR", str(Path.home() / ".hermes" / "bridge")))
BRIDGE = "imsg"
SSH_HOST = os.environ.get("IMSG_SSH_HOST", "user@mac-host.local")
POLL_INTERVAL = float(os.environ.get("BRIDGE_POLL_INTERVAL", "1.0"))


# ── SSH helpers ──────────────────────────────────────────────────────


def ssh_run(cmd: str, timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a command on the Mac Mini via SSH."""
    full_cmd = ["ssh", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=30",
                SSH_HOST, f"export PATH='/opt/homebrew/bin:/opt/homebrew/sbin:$PATH' && {cmd}"]
    return subprocess.run(full_cmd, capture_output=True, text=True, timeout=timeout)


def ssh_stream(cmd: str):
    """Stream stdout from a remote command line by line."""
    full_cmd = ["ssh", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=30",
                "-o", "ServerAliveCountMax=3", SSH_HOST,
                f"export PATH='/opt/homebrew/bin:/opt/homebrew/sbin:$PATH' && {cmd}"]
    proc = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            text=True, bufsize=1)
    return proc


# ── Outbox (ausgehende Nachrichten) ──────────────────────────────────


def send_imessage(target: str, text: str, attachments: list = None):
    """Send an iMessage via SSH using imsg CLI."""
    phone = target.split(":", 1)[1] if ":" in target else target

    if attachments:
        for att in attachments:
            att_path = BRIDGE_DIR / att.get("path", "")
            if att_path.exists():
                logger.info("Sending attachment to %s: %s", phone, att_path)
                result = ssh_run(
                    f"imsg send --chat-identifier {_shq(phone)} --file {_shq(str(att_path))}"
                )
                if result.returncode != 0:
                    logger.warning("Failed to send attachment: %s", result.stderr.strip())

    if text:
        logger.info("Sending message to %s: %.80s", phone, text)
        result = ssh_run(f"imsg send --chat-identifier {_shq(phone)} --text {_shq(text)}")
        if result.returncode != 0:
            logger.warning("Failed to send message: %s", result.stderr.strip())


def _shq(s: str) -> str:
    """Simple shell-quote for SSH commands."""
    return "'" + s.replace("'", "'\\''") + "'"


def poll_outbox():
    """Poll outbox/imsg/ and send pending messages."""
    outbox_dir = BRIDGE_DIR / "outbox" / BRIDGE
    while True:
        try:
            for f in sorted(outbox_dir.glob("*.json"), key=lambda p: p.stat().st_mtime):
                try:
                    data = json.loads(f.read_text("utf-8"))
                except (json.JSONDecodeError, OSError) as e:
                    logger.warning("Invalid outbox JSON %s: %s", f, e)
                    f.unlink(missing_ok=True)
                    continue

                if data.get("typing"):
                    f.unlink(missing_ok=True)
                    continue

                send_imessage(
                    data.get("target", ""),
                    data.get("text", ""),
                    data.get("attachments"),
                )
                f.unlink(missing_ok=True)
        except Exception as e:
            logger.error("Outbox poll error: %s", e)
        time.sleep(POLL_INTERVAL)


# ── Inbox (eingehende Nachrichten) ──────────────────────────────────


def write_inbox(data: dict):
    """Write an incoming message as JSON to inbox/imsg/."""
    inbox_dir = BRIDGE_DIR / "inbox" / BRIDGE
    inbox_dir.mkdir(parents=True, exist_ok=True)
    msg_id = data.get("id", str(uuid.uuid4()))
    path = inbox_dir / f"{msg_id}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
    logger.debug("Wrote inbox: %s", path)


def handle_incoming():
    """Start imsg watch and process incoming messages."""
    logger.info("Starting imsg watch stream...")
    proc = ssh_stream("imsg watch --json --reactions --debounce 500ms")

    try:
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue

            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Skip own messages
            if raw.get("is_from_me", False):
                continue

            # Process attachments
            attachments = []
            for att in raw.get("attachments", []):
                src = Path(att.get("file_path", ""))
                if src.exists():
                    target = BRIDGE_DIR / "media" / BRIDGE / "incoming" / src.name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        shutil.copy2(str(src), str(target))
                        attachments.append({
                            "type": "image" if att.get("mime", "").startswith("image/")
                                    else "document",
                            "url": str(target),
                            "mime": att.get("mime", ""),
                            "size": att.get("size", 0),
                        })
                    except OSError as e:
                        logger.warning("Failed to copy attachment: %s", e)

            # Build chat ID
            chat_id = f"imsg:{raw.get('chat_id', raw.get('sender', ''))}"

            inbox_msg = {
                "bridge": BRIDGE,
                "id": raw.get("id", str(uuid.uuid4())),
                "sender": raw.get("sender", ""),
                "sender_name": raw.get("sender_name", ""),
                "text": raw.get("text", ""),
                "timestamp": raw.get("timestamp", ""),
                "attachments": attachments,
                "chat": {
                    "id": chat_id,
                    "type": "group" if raw.get("is_group", False) else "direct",
                    "name": raw.get("group_name"),
                },
                "reply_to": raw.get("reply_to"),
            }
            write_inbox(inbox_msg)

    except Exception as e:
        logger.error("Inbound stream error: %s", e)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


# ── Status ──────────────────────────────────────────────────────────


def write_status(connected: bool, error: str = None):
    """Write bridge status file."""
    status_dir = BRIDGE_DIR / "status" / BRIDGE
    status_dir.mkdir(parents=True, exist_ok=True)
    status = {
        "bridge": BRIDGE,
        "connected": connected,
        "last_seen": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "error": error,
    }
    (status_dir / "status.json").write_text(
        json.dumps(status, indent=2), "utf-8"
    )


# ── Main ────────────────────────────────────────────────────────────


def main():
    logger.info("imsg-wrapper starting — BRIDGE_DIR=%s, SSH_HOST=%s", BRIDGE_DIR, SSH_HOST)
    write_status(connected=True)

    # Outbox poller in background thread
    t = threading.Thread(target=poll_outbox, daemon=True, name="outbox-poller")
    t.start()

    # Inbound stream (blocks)
    try:
        handle_incoming()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        write_status(connected=False, error="shutdown")
        logger.info("imsg-wrapper stopped")


if __name__ == "__main__":
    main()
