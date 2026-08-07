#!/usr/bin/env python3
"""
talk-wrapper — Bridge Adapter wrapper for Nextcloud Talk.

Feeds the Bridge Adapter (registry-based, see adapter.py) from Nextcloud
Talk via its REST API. Unlike imsg, Talk has NO watch stream — it is pure
HTTP polling, so the inbound loop IS the primary source, with last_seen
dedup so nothing is delivered twice.

Verified Talk API (Nextcloud 34.0.2 / Talk 24.0.3):
  - Room list : GET {HOST}/ocs/v2.php/apps/spreed/api/v4/room?format=json
                (REQUIRES format=json in query)
  - Chat read : GET {HOST}/ocs/v2.php/apps/spreed/api/v1/chat/{token}?lookIntoFuture=0&limit=N
                (must NOT have format=json in query — that breaks it with
                998 "Invalid query"; send Accept: application/json instead)
  - Send      : POST {HOST}/ocs/v2.php/apps/spreed/api/v1/chat/{token}
                with message=<text>  → 201 + message id

Environment variables:
  BRIDGE_DIR             bridge directory (default: ~/.hermes/bridge)
  NEXTCLOUD_HOST         e.g. https://your-nextcloud.example.com
  NEXTCLOUD_USERNAME     Talk user (own messages are skipped)
  NEXTCLOUD_PASSWORD     app password
  BRIDGE_POLL_INTERVAL   inbound/outbox poll interval (default: 5.0)
"""

import json
import logging
import os
import time
import uuid
from pathlib import Path
import threading
import urllib.parse
import urllib.request
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("talk-wrapper")

# ── Env loading ─────────────────────────────────────────────────────
# Load NEXTCLOUD_* from ~/.hermes/.env if not already in the environment
# (systemd services don't source .env; this makes the wrapper self-contained).
_ENV_FILE = Path.home() / ".hermes" / ".env"
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text("utf-8").splitlines():
        _line = _line.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _k, _v = _line.split("=", 1)
        _k = _k.strip()
        _v = _v.strip().strip('"').strip("'")
        if _k.startswith("NEXTCLOUD_") and _k not in os.environ:
            os.environ[_k] = _v

BRIDGE_DIR = Path(os.environ.get("BRIDGE_DIR", str(Path.home() / ".hermes" / "bridge")))
BRIDGE = "talk"
POLL_INTERVAL = float(os.environ.get("BRIDGE_POLL_INTERVAL", "5.0"))

NC_HOST = os.environ.get("NEXTCLOUD_HOST", "").rstrip("/")
NC_USER = os.environ.get("NEXTCLOUD_USERNAME", "")
NC_PASS = os.environ.get("NEXTCLOUD_PASSWORD", "")

STATE_FILE = BRIDGE_DIR / "state" / BRIDGE / "last_seen.json"
STATUS_FILE = BRIDGE_DIR / "status" / BRIDGE / "status.json"
MANIFEST_FILE = BRIDGE_DIR / "registry" / "talk.yaml"

# Rooms that are personal notes / self-chats — don't mirror them.
SKIP_ROOM_TYPES = {4, 6}  # 4=one-to-one self, 6=note-to-self


def _api(method: str, path: str, query: dict | None = None, data: dict | None = None) -> list:
    """Call the Nextcloud OCS API and return the ocs.data list (or [] on failure)."""
    url = f"{NC_HOST}/ocs/v2.php{path}"
    if query:
        url += "?" + urllib.parse.urlencode(query)
    req = urllib.request.Request(url, method=method)
    req.add_header("OCS-APIRequest", "true")
    req.add_header("Accept", "application/json")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    import base64

    auth = base64.b64encode(f"{NC_USER}:{NC_PASS}".encode()).decode()
    req.add_header("Authorization", f"Basic {auth}")
    body = None
    if data:
        body = urllib.parse.urlencode(data).encode()
    try:
        with urllib.request.urlopen(req, data=body, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.error("API %s %s failed: %s", method, path, e)
        return []
    ocs = payload.get("ocs", {}) or {}
    result = ocs.get("data", [])
    # send_message expects a dict/truthy; get_chat/get_rooms expect a list.
    return result if isinstance(result, list) else result or []


def get_rooms() -> list:
    """List all conversations via API v4 (needs format=json in query)."""
    return _api("GET", "/apps/spreed/api/v4/room", query={"format": "json"})


def get_chat(room_token: str, limit: int = 20) -> list:
    """Read past messages from a room via API v1 (NO format=json in query)."""
    return _api(
        "GET",
        f"/apps/spreed/api/v1/chat/{room_token}",
        query={"lookIntoFuture": "0", "limit": str(limit)},
    )


def get_participants(room_token: str) -> list:
    """List participants of a room via API v4 (needs format=json in query)."""
    return _api(
        "GET",
        f"/apps/spreed/api/v4/room/{room_token}/participants",
        query={"format": "json"},
    )


def is_direct_room(room: dict) -> bool:
    """True if the room is a 1:1 conversation (exactly 2 participants, not
    the self-chats). Talk reports such chats as type=2 (group) when they're
    not created through the one-to-one flow, so we resolve via participants.

    A room is a direct chat if it has exactly two participants — the own
    account and one other. Self/note rooms (type 4/6) are already excluded
    by the caller, so here we only need the participant count.
    """
    token = room.get("token", "")
    if not token:
        return False
    parts = get_participants(token)
    return len(parts) == 2


def send_message(room_token: str, text: str) -> bool:
    """POST a message to a room. Returns True on success (201)."""
    data = _api(
        "POST",
        f"/apps/spreed/api/v1/chat/{room_token}",
        data={"message": text},
    )
    return bool(data)


# ── Registry: self-registration (T-050) ─────────────────────────────


MANIFEST_CONTENT = """\
name: talk
service: nextcloud-talk
host: your-nextcloud.example.com
target_format: [chat_id]
capabilities: [text]
"""


def register_manifest():
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_FILE.write_text(MANIFEST_CONTENT, "utf-8")
    logger.info("Wrote registry manifest: %s", MANIFEST_FILE)


def unregister_manifest():
    try:
        MANIFEST_FILE.unlink(missing_ok=True)
        logger.info("Removed registry manifest: %s", MANIFEST_FILE)
    except OSError as e:
        logger.warning("Failed to remove manifest: %s", e)


# ── Status ──────────────────────────────────────────────────────────


def write_status(connected: bool, error: str = None):
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
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


def build_inbox_msg(raw: dict, room_name: str, room_token: str, chat_type: str) -> dict:
    return {
        "bridge": BRIDGE,
        "id": str(raw.get("id", uuid.uuid4())),
        "sender": raw.get("actorId", ""),
        "sender_name": raw.get("actorDisplayName", ""),
        "text": raw.get("message", ""),
        "timestamp": raw.get("timestamp", ""),
        "attachments": [],
        "chat": {
            "id": room_token,
            "type": chat_type,
            "name": room_name,
        },
        "reply_to": None,
    }


def is_own(raw: dict) -> bool:
    """True if the message is from our own Talk user (or a system message)."""
    if raw.get("systemMessage"):
        return True
    return raw.get("actorId", "") == NC_USER


# ── Inbound polling (primary, no watch stream) ──────────────────────


def poll_once(last_seen: dict) -> dict:
    """Poll all rooms once; write new messages. Returns updated last_seen."""
    rooms = get_rooms()
    if not rooms:
        return last_seen
    for room in rooms:
        token = room.get("token", "")
        if not token:
            continue
        if room.get("type") in SKIP_ROOM_TYPES:
            continue
        room_name = room.get("name", "") or room.get("displayName", "")
        chat_type = "direct" if is_direct_room(room) else "group"
        msgs = get_chat(token)
        if not msgs:
            continue
        # dedup: only messages newer than last seen id for this room
        last_id = int(last_seen.get(token, 0) or 0)
        max_id = last_id
        for raw in msgs:
            mid = int(raw.get("id", 0) or 0)
            if mid:
                max_id = max(max_id, mid)
            if mid <= last_id:
                continue
            if is_own(raw):
                continue
            if not str(raw.get("message", "")).strip():
                continue
            try:
                write_inbox(build_inbox_msg(raw, room_name, token, chat_type))
            except Exception as e:
                logger.error("Failed writing inbox for msg %s: %s", mid, e)
        if max_id > last_id:
            last_seen[token] = max_id
    return last_seen


def inbound_loop():
    last_seen = load_last_seen()
    logger.info("Inbound polling started (every %.1fs)", POLL_INTERVAL)
    while True:
        try:
            last_seen = poll_once(last_seen)
            save_last_seen(last_seen)
        except Exception as e:
            logger.error("Inbound poll error: %s", e)
        time.sleep(POLL_INTERVAL)


# ── Outbox (outgoing) ───────────────────────────────────────────────


def extract_token(target: str) -> str:
    """Target is 'talk~<room_token>' (T-056), legacy 'talk:<room_token>',
    or bare '<room_token>'. Return the token."""
    for sep in ("~", ":"):
        if sep in target:
            head, _, _ = target.partition(sep)
            if head == BRIDGE:
                return target.split(sep, 1)[1].strip()
    return target.strip()


def outbox_loop():
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
                token = extract_token(data.get("target", ""))
                text = data.get("text", "")
                if token and text:
                    ok = send_message(token, text)
                    if not ok:
                        logger.warning("Failed to send to %s", token)
                f.unlink(missing_ok=True)
        except Exception as e:
            logger.error("Outbox poll error: %s", e)
        time.sleep(POLL_INTERVAL)


# ── Main ────────────────────────────────────────────────────────────


def main():
    if not (NC_HOST and NC_USER and NC_PASS):
        logger.error(
            "Missing Nextcloud env vars (NEXTCLOUD_HOST/USERNAME/PASSWORD). Exiting."
        )
        return
    logger.info("talk-wrapper starting — BRIDGE_DIR=%s, HOST=%s", BRIDGE_DIR, NC_HOST)
    register_manifest()
    write_status(connected=True)

    threads = [
        threading.Thread(target=inbound_loop, daemon=True, name="inbound"),
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
        logger.info("talk-wrapper stopped")


if __name__ == "__main__":
    main()
