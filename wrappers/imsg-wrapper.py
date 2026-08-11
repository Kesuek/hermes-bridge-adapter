#!/usr/bin/env python3
"""
imsg-wrapper — Bridge Adapter wrapper for iMessage.

Drives imsg over SSH on a remote macOS host, feeding the Bridge Adapter
(registry-based, see adapter.py).

Design (T-052):
- **Watch stream as primary source** — `imsg watch --json --reactions`
  delivers messages in real time, including reactions (T-043).
- **Auto-reconnect with backoff** — if the SSH tunnel or the remote watch
  process dies, the stream is restarted immediately with exponential
  backoff (1s → 2s → 4s → ... max 30s). ServerAliveInterval keeps the
  tunnel from going stale.
- **Periodic restart** — the watch stream is restarted on a fixed interval
  (default 6h). This solves the "ran for ~3 days then silently died"
  failure mode without having to diagnose whether SSH or `imsg watch`
  itself stopped: a regular restart clears both.
- **History polling as safety net** — runs in parallel at low frequency
  (30s), catching any messages missed during a reconnect/restart window.
  The last_seen.json state file dedupes so nothing is delivered twice.

Environment variables:
  BRIDGE_DIR           bridge directory (default: ~/.hermes/bridge)
  IMSG_SSH_HOST        SSH target (required; e.g. user@host)
  IMSG_OWN_HANDLES     comma-separated own handles/IDs (own messages skipped)
  BRIDGE_POLL_INTERVAL outbox poll interval in seconds (default: 1.0)
  BRIDGE_HISTORY_POLL_INTERVAL history safety-net poll (default: 30.0)
  BRIDGE_HISTORY_LIMIT messages checked per chat per history poll (default: 10)
  WATCH_RESTART_INTERVAL periodic watch restart (default: 21600 = 6h)
"""

import json
import logging
import os
import re
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
SSH_HOST = os.environ.get("IMSG_SSH_HOST", "").strip()
POLL_INTERVAL = float(os.environ.get("BRIDGE_POLL_INTERVAL", "1.0"))
HISTORY_POLL_INTERVAL = float(os.environ.get("BRIDGE_HISTORY_POLL_INTERVAL", "30.0"))
HISTORY_LIMIT = int(os.environ.get("BRIDGE_HISTORY_LIMIT", "10"))
WATCH_RESTART_INTERVAL = float(os.environ.get("WATCH_RESTART_INTERVAL", "21600"))  # 6h

STATE_FILE = BRIDGE_DIR / "state" / BRIDGE / "last_seen.json"
STATUS_FILE = BRIDGE_DIR / "status" / BRIDGE / "status.json"
MANIFEST_FILE = BRIDGE_DIR / "registry" / "imsg.yaml"

# Handles considered "ours" (own messages) — never re-injected.
# Set IMSG_OWN_HANDLES to a comma-separated list of your own handles/IDs.
OWN_HANDLES = {
    h.strip() for h in os.environ.get("IMSG_OWN_HANDLES", "").split(",") if h.strip()
}

SSH_BASE = [
    "ssh",
    "-o", "ConnectTimeout=10",
    "-o", "ServerAliveInterval=30",
    "-o", "ServerAliveCountMax=3",
    SSH_HOST,
    "export PATH='/opt/homebrew/bin:/opt/homebrew/sbin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH' &&",
]


def _remote(cmd: str) -> list:
    return SSH_BASE + [cmd]


def _shq(s: str) -> str:
    """Simple shell-quote for SSH commands."""
    return "'" + s.replace("'", "'\\''") + "'"


def ssh_run(cmd: str, timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a one-shot command on the Mac Mini via SSH."""
    return subprocess.run(_remote(cmd), capture_output=True, text=True, timeout=timeout)


# ── Registry: self-registration (T-050) ─────────────────────────────


MANIFEST_CONTENT = """\
name: imsg
service: imessage
host: mac-mini-01
target_format: [email, phone, chat_id]
capabilities: [text, attachments, reactions]
"""


def register_manifest():
    """Write the registry manifest so the adapter registers this bridge."""
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_FILE.write_text(MANIFEST_CONTENT, "utf-8")
    logger.info("Wrote registry manifest: %s", MANIFEST_FILE)


def unregister_manifest():
    """Remove the manifest so the adapter unregisters this bridge."""
    try:
        MANIFEST_FILE.unlink(missing_ok=True)
        logger.info("Removed registry manifest: %s", MANIFEST_FILE)
    except OSError as e:
        logger.warning("Failed to remove manifest: %s", e)


# ── Status ──────────────────────────────────────────────────────────


def write_status(connected: bool, error: str = None):
    status_dir = STATUS_FILE.parent
    status_dir.mkdir(parents=True, exist_ok=True)
    status = {
        "bridge": BRIDGE,
        "connected": connected,
        "last_seen": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "error": error,
    }
    STATUS_FILE.write_text(json.dumps(status, indent=2), "utf-8")


# ── State (last_seen dedup) ─────────────────────────────────────────


def load_last_seen() -> dict:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text("utf-8"))
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def save_last_seen(state: dict):
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(state, indent=2), "utf-8")
    except OSError as e:
        logger.warning("Failed to save state: %s", e)


# ── Inbox helpers ───────────────────────────────────────────────────


def write_inbox(data: dict):
    inbox_dir = BRIDGE_DIR / "inbox" / BRIDGE
    inbox_dir.mkdir(parents=True, exist_ok=True)
    msg_id = data.get("id", str(uuid.uuid4()))
    path = inbox_dir / f"{msg_id}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
    logger.info("Wrote inbox: %s (from %s)", path, data.get("sender", "?"))


def build_inbox_msg(raw: dict) -> dict:
    """Build an inbox message dict from a raw imsg watch/history record."""
    attachments = []
    for att in raw.get("attachments", []):
        # imsg attachment schema uses original_path / filename / mime_type / total_bytes.
        # (Older wrapper code read file_path/mime/size — wrong field names, so src was
        # empty → src.name == '.' → "Is a directory" copy failure → images never arrived.)
        # The path is on the remote Mac; keep the leading "~" — scp expands it
        # server-side to the SSH user's home, so no remote username is hardcoded.
        remote_path = att.get("original_path") or att.get("filename") or ""
        src = Path(remote_path)
        target = BRIDGE_DIR / "media" / BRIDGE / "incoming" / src.name
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            if not src.exists():
                # Pull from the Mac via SCP; "~" expands to the remote home.
                scp = subprocess.run(
                    ["scp", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=no",
                     f"{SSH_HOST}:{remote_path}", str(target)],
                    capture_output=True, text=True, timeout=60,
                )
                if scp.returncode != 0:
                    logger.warning("SCP attachment failed: %s", scp.stderr[:200])
                    continue
            else:
                shutil.copy2(str(src), str(target))
            mime = att.get("mime_type") or att.get("mime") or ""
            attachments.append({
                "type": "image" if mime.startswith("image/")
                        else "document",
                "url": str(target),
                "mime": mime,
                "size": att.get("total_bytes") or att.get("size") or 0,
            })
        except OSError as e:
            logger.warning("Failed to copy attachment: %s", e)

    # Raw chat identity only — the adapter builds the full reply address
    # ``imsg~<ident>`` (T-056). Keeping the bridge prefix out of chat.id
    # means the wrapper stays agnostic of the addressing convention.
    ident = raw.get("chat_identifier", "") or raw.get("sender", "")
    chat_id = ident or raw.get("chat_id", "")

    return {
        "bridge": BRIDGE,
        "id": str(raw.get("id", uuid.uuid4())),
        "sender": raw.get("sender", ""),
        "sender_name": raw.get("sender_name", ""),
        "text": raw.get("text", ""),
        "timestamp": raw.get("created_at", ""),
        "attachments": attachments,
        "chat": {
            "id": chat_id,
            "type": "group" if raw.get("is_group", False) else "direct",
            "name": raw.get("group_name"),
        },
        "reply_to": raw.get("reply_to"),
    }


def is_own(raw: dict) -> bool:
    """True if the message is from one of our own handles."""
    if raw.get("is_from_me", False):
        return True
    return raw.get("sender", "") in OWN_HANDLES


# ── Watch stream (primary) ──────────────────────────────────────────


def watch_loop():
    """Run the imsg watch stream with auto-reconnect + periodic restart.

    The stream is restarted immediately on failure (exponential backoff)
    and on a fixed interval (WATCH_RESTART_INTERVAL) to clear the
    "ran for days then silently died" failure mode.
    """
    backoff = 1.0
    next_restart = time.time() + WATCH_RESTART_INTERVAL
    stop_heartbeat = threading.Event()

    def _heartbeat():
        """Refresh the status heartbeat so last_seen stays fresh even when
        no messages arrive for a while. Runs in its own thread because the
        watch stream's read loop blocks on stdin."""
        while not stop_heartbeat.is_set():
            write_status(connected=True)
            stop_heartbeat.wait(60)

    while True:
        try:
            proc = subprocess.Popen(
                _remote("imsg watch --json --reactions --debounce 500ms"),
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
            )
            logger.info("Watch stream started (pid %s)", proc.pid)
            write_status(connected=True)
            hb = threading.Thread(target=_heartbeat, daemon=True)
            hb.start()

            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if is_own(raw):
                    continue
                write_inbox(build_inbox_msg(raw))
                backoff = 1.0  # healthy — reset backoff

                # Periodic restart while the stream is healthy
                if time.time() >= next_restart:
                    logger.info("Watch restart interval reached, cycling stream")
                    proc.terminate()
                    next_restart = time.time() + WATCH_RESTART_INTERVAL
                    break

            # Stream ended (SSH dead or watch stopped)
            stop_heartbeat.set()
            if proc.poll() is None:
                proc.terminate()
                proc.wait()
            logger.warning("Watch stream ended — reconnecting in %.1fs", backoff)
        except Exception as e:
            logger.error("Watch loop error: %s — reconnect in %.1fs", e, backoff)
            stop_heartbeat.set()

        write_status(connected=False, error="watch stream down")
        time.sleep(backoff)
        backoff = min(backoff * 2, 30.0)  # cap at 30s
        next_restart = time.time() + WATCH_RESTART_INTERVAL


# ── History safety net ──────────────────────────────────────────────


def poll_history_once(last_seen: dict) -> dict:
    """Poll chats + history; write messages newer than last_seen.

    Catches anything missed while the watch stream was down (reconnect /
    restart window). Returns the updated last_seen map.
    """
    res = ssh_run("imsg chats --json", timeout=20)
    if res.returncode != 0:
        logger.warning("imsg chats failed: %s", res.stderr.strip())
        return last_seen

    chats = []
    for line in res.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            chats.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    for chat in chats:
        # `imsg chats --json` reports the rowid as field "id"
        chat_id = str(chat.get("id", "") or chat.get("chat_id", ""))
        if not chat_id:
            continue
        hist = ssh_run(
            f"imsg history --chat-id {_shq(chat_id)} --limit {HISTORY_LIMIT} --json",
            timeout=20,
        )
        if hist.returncode != 0:
            continue

        last_id = int(last_seen.get(chat_id, 0) or 0)
        max_id = last_id
        for line in hist.stdout.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                continue
            msg_id = raw.get("id", 0)
            if msg_id:
                max_id = max(max_id, int(msg_id))
            if int(msg_id or 0) <= last_id:
                continue
            if is_own(raw):
                continue
            if not raw.get("text", "").strip() and not raw.get("attachments"):
                continue
            try:
                write_inbox(build_inbox_msg(raw))
            except Exception as e:
                logger.error("Failed writing inbox for msg %s: %s", msg_id, e)

        if max_id > last_id:
            last_seen[chat_id] = max_id

    return last_seen


def history_loop():
    """Run the history safety net at low frequency."""
    last_seen = load_last_seen()
    logger.info("History safety net started (every %.1fs)", HISTORY_POLL_INTERVAL)
    while True:
        try:
            last_seen = poll_history_once(last_seen)
            save_last_seen(last_seen)
        except Exception as e:
            logger.error("History poll error: %s", e)
        time.sleep(HISTORY_POLL_INTERVAL)


# ── Outbox (outgoing) ───────────────────────────────────────────────


def resolve_chat_identifier(target: str) -> str:
    """Resolve a chat target to a usable imsg chat-identifier.

    Accepts 'imsg~<handle>' (T-056 separator), legacy 'imsg:<handle>', or
    a bare '<handle>'. Numeric rowids are looked up via `imsg chats` and
    mapped to the chat_identifier (sending to a numeric rowid is unreliable
    on macOS 26).
    """
    # Strip a leading bridge prefix (``imsg~`` current, ``imsg:`` legacy)
    # so the wrapper stays agnostic of the addressing convention.
    for sep in ("~", ":"):
        if sep in target:
            head, _, _ = target.partition(sep)
            if head == BRIDGE:
                target = target.split(sep, 1)[1].strip()
                break
    target = target.strip()
    if not target:
        return ""
    if re.fullmatch(r"\d+", target):
        res = ssh_run("imsg chats --json")
        if res.returncode == 0:
            try:
                for chat in res.stdout.strip().splitlines():
                    data = json.loads(chat)
                    if str(data.get("id", "")) == target:
                        ident = data.get("identifier", "") or data.get("chat_identifier", "")
                        if ident:
                            logger.info("Resolved chat rowid %s → %s", target, ident)
                            return ident
            except (json.JSONDecodeError, ValueError):
                pass
        logger.warning("Could not resolve chat rowid %s, using as-is", target)
        return target
    return target


def send_imessage(target: str, text: str, attachments: list = None):
    phone = resolve_chat_identifier(target)
    if not phone:
        logger.warning("Empty chat target, skipping send")
        return

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


def outbox_loop():
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


# ── Main ────────────────────────────────────────────────────────────


def main():
    logger.info("imsg-wrapper starting — BRIDGE_DIR=%s, SSH_HOST=%s", BRIDGE_DIR, SSH_HOST)
    register_manifest()
    write_status(connected=True)

    threads = [
        threading.Thread(target=watch_loop, daemon=True, name="watch"),
        threading.Thread(target=history_loop, daemon=True, name="history"),
        threading.Thread(target=outbox_loop, daemon=True, name="outbox"),
    ]
    for t in threads:
        t.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        write_status(connected=False, error="shutdown")
        unregister_manifest()
        logger.info("imsg-wrapper stopped")


if __name__ == "__main__":
    main()
