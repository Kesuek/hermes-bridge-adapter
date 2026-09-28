"""File-level helpers: private writes, manifest, status, dedup state, inbox.

Shared by all wrappers (extracted from imsg-wrapper / talk-wrapper, T-090).
Every write goes through ``write_private`` (0600, T-071); inbox messages are
kept 0600 for consistency.
"""

import json
import logging
import os
import time
import uuid
from pathlib import Path

logger = logging.getLogger("hermes_bridge_sdk")


def bridge_dir() -> Path:
    """Resolve the bridge directory (BRIDGE_DIR env or ~/.hermes/bridge)."""
    return Path(os.environ.get("BRIDGE_DIR", str(Path.home() / ".hermes" / "bridge")))


def write_private(path: Path, content: str) -> None:
    """Write a file atomically with 0600 perms (T-071 + T-094).

    Atomic via temp + ``os.replace`` (a concurrent reader — e.g. the
    adapter's inbox poller — sees either the old or the new file, never
    a torn write; T-094). The temp name carries a uuid component (the
    T-079 pattern from ``adapter._atomic_write_json``) so concurrent
    writers never collide and the ``*.json`` glob never matches the
    temp. 0600 chmod (T-071) enforces privacy regardless of umask:
    state/status/manifest files may carry tokens or routing secrets.
    """
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(content, "utf-8")
        os.replace(tmp, path)
    except OSError:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    try:
        os.chmod(path, 0o600)
    except OSError:
        logger.warning("chmod 0o600 failed for %s", path)


def write_manifest(
    bridge: str,
    service: str,
    host: str,
    target_format: list,
    capabilities: list,
    manifest_file: Path | None = None,
) -> Path:
    """Write the registry manifest so the adapter registers this bridge (T-050)."""
    manifest_file = manifest_file or bridge_dir() / "registry" / f"{bridge}.yaml"
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    content = (
        f"name: {bridge}\n"
        f"service: {service}\n"
        f"host: {host}\n"
        f"target_format: {json.dumps(target_format)}\n"
        f"capabilities: {json.dumps(capabilities)}\n"
    )
    write_private(manifest_file, content)
    logger.info("Wrote registry manifest: %s", manifest_file)
    return manifest_file


def unregister_manifest(bridge: str, manifest_file: Path | None = None) -> None:
    """Remove the manifest so the adapter unregisters this bridge."""
    manifest_file = manifest_file or bridge_dir() / "registry" / f"{bridge}.yaml"
    try:
        manifest_file.unlink(missing_ok=True)
        logger.info("Removed registry manifest: %s", manifest_file)
    except OSError as e:
        logger.warning("Failed to remove manifest: %s", e)


def write_status(bridge: str, connected: bool, error: "str | None" = None,
                 status_file: Path | None = None) -> None:
    """Write the bridge status file (heartbeat payload)."""
    status_file = status_file or bridge_dir() / "status" / bridge / "status.json"
    status_file.parent.mkdir(parents=True, exist_ok=True)
    status = {
        "bridge": bridge,
        "connected": connected,
        "last_seen": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "error": error,
    }
    write_private(status_file, json.dumps(status, indent=2))


def load_last_seen(state_file: Path) -> dict:
    try:
        if state_file.exists():
            return json.loads(state_file.read_text("utf-8"))
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def save_last_seen(state: dict, state_file: Path) -> None:
    try:
        state_file.parent.mkdir(parents=True, exist_ok=True)
        write_private(state_file, json.dumps(state, indent=2))
    except OSError as e:
        logger.warning("Failed to save state: %s", e)


def write_inbox_private(bridge: str, data: dict, inbox_dir: Path | None = None) -> Path:
    """Atomically write an inbox message as 0600 JSON. Returns the path.

    T-100: stamps ``data["id"]`` with the same id used for the filename
    when the payload carries none — the adapter's inbound dedup
    (``dedup_map.json``) keys on ``id``/``message_id``, so a wrapper
    without a platform-native id still gets a stable per-message id
    instead of delivering id-less (at-least-once) forever. Wrapper-provided
    ids (e.g. the platform rowid) are preserved as-is.
    """
    inbox_dir = inbox_dir or bridge_dir() / "inbox" / bridge
    inbox_dir.mkdir(parents=True, exist_ok=True)
    msg_id = str(data.get("id") or data.get("message_id") or uuid.uuid4())
    data["id"] = msg_id
    path = inbox_dir / f"{msg_id}.json"
    write_private(path, json.dumps(data, ensure_ascii=False, indent=2))
    logger.info("Wrote inbox: %s (from %s)", path, data.get("sender", "?"))
    return path


def write_inbox(bridge: str, data: dict, inbox_dir: Path | None = None) -> Path:
    """Backwards-compatible alias for write_inbox_private."""
    return write_inbox_private(bridge, data, inbox_dir)


def strip_bridge_prefix(target: str, bridge: str) -> str:
    """Strip a leading ``<bridge>~`` (T-056) or legacy ``<bridge>:`` prefix.

    The wrapper stays agnostic of the addressing convention: the adapter
    builds the full reply address; the wrapper only needs the raw target.
    """
    for sep in ("~", ":"):
        if sep in target:
            head, _, _ = target.partition(sep)
            if head == bridge:
                return target.split(sep, 1)[1].strip()
    return target.strip()