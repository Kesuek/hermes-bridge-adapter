"""
Bridge Adapter — Hermes Gateway Platform Plugin.

Polls JSON files from inbox/ directories (written by external services),
dispatches them as MessageEvents to the Gateway, and writes outgoing
messages as JSON files to outbox/ directories.

Attachments are stored in a shared media/ directory under the bridge dir,
so both adapter and wrapper use consistent relative paths.

Directory structure::

    <bridge_dir>/
    ├── inbox/<bridge>/       ← written by external service, read by adapter
    ├── outbox/<bridge>/      ← written by adapter, read by external service
    ├── status/<bridge>/      ← written by external service, read by adapter
    └── media/
        ├── <bridge>/incoming/  ← incoming attachments (wrapper → adapter)
        └── <bridge>/outgoing/  ← outgoing attachments (adapter → wrapper)
"""

import asyncio
import json
import logging
import os
import re
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.session import SessionSource

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 1.0  # seconds
MAX_MESSAGE_LENGTH = 4000
MEDIA_CLEANUP_MAX_AGE = 86400  # 24h
OUTBOX_CLEANUP_MAX_AGE = 3600  # 1h
CLEANUP_INTERVAL = 300  # 5min
STATUS_POLL_INTERVAL = 60  # 1min
REGISTRY_POLL_INTERVAL = 5.0  # seconds
REGISTRY_DIR_NAME = "registry"
MANIFEST_FIELDS = ["name", "service", "host", "target_format", "capabilities"]
DEFAULT_MENTION_PATTERNS = [
    r"(?<![\\w@])@?hermes\\s+agent\\b[,:\\-]?",
    r"(?<![\\w@])@?hermes\\b[,:\\-]?",
]


def _now_iso() -> str:
    """Current timestamp in local ISO 8601 (e.g. ``2026-08-10T10:00:00+02:00``)."""
    return datetime.now(timezone.utc).astimezone().isoformat()


# ── Registry: Manifest Schema + Loader ───────────────────────────────


class BridgeManifest:
    """Parsed bridge manifest (``registry/<name>.yaml``).

    A manifest is the single source of truth for a bridge's identity: its
    presence in ``registry/`` means the bridge is registered, removing the
    file deregisters it. ``target_format`` declares which target shapes the
    bridge accepts (used by :meth:`accepts_target` as the basis for the
    routing check prepared in T-050; full fallback logic lands in T-053).
    """

    def __init__(
        self,
        name: str,
        service: str = "",
        host: str = "",
        target_format: Optional[list] = None,
        capabilities: Optional[list] = None,
    ):
        self.name = name
        self.service = service or name
        self.host = host or ""
        self.target_format = list(target_format) if target_format else []
        self.capabilities = list(capabilities) if capabilities else []

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"BridgeManifest(name={self.name!r}, service={self.service!r}, "
            f"host={self.host!r}, target_format={self.target_format!r}, "
            f"capabilities={self.capabilities!r})"
        )

    def accepts_target(self, target: str) -> bool:
        """Check a target against the bridge's declared target formats.

        With no ``target_format`` declared the bridge is permissive and
        accepts anything. Otherwise the target must match at least one of
        the declared formats:

        - ``email``  → target contains ``@``
        - ``phone``  → target contains at least one digit
        - ``chat_id``→ accepts any non-empty target (opaque chat id)
        """
        if not target:
            return False
        if not self.target_format:
            return True  # no declared constraint → accept anything
        if "email" in self.target_format and "@" in target:
            return True
        if "phone" in self.target_format and any(c.isdigit() for c in target):
            return True
        if "chat_id" in self.target_format:
            return True
        return False


def load_manifest(data: dict) -> BridgeManifest:
    """Build a :class:`BridgeManifest` from a parsed YAML dict.

    Raises :class:`ValueError` if the manifest is missing a non-empty
    ``name`` (the only required field).
    """
    if not isinstance(data, dict):
        raise ValueError(f"Manifest must be a mapping, got {type(data).__name__}")
    name = str(data.get("name", "")).strip()
    if not name:
        raise ValueError("Manifest missing 'name'")

    def _as_list(key):
        val = data.get(key)
        if val is None:
            return []
        if isinstance(val, (list, tuple)):
            return [str(v) for v in val]
        # scalar → single-element list
        return [str(val)]

    return BridgeManifest(
        name=name,
        service=str(data.get("service", "")).strip(),
        host=str(data.get("host", "")).strip(),
        target_format=_as_list("target_format"),
        capabilities=_as_list("capabilities"),
    )


def read_manifest(path: Path) -> BridgeManifest:
    """Read and parse a manifest YAML file from ``path``."""
    with open(path, "r", encoding="utf-8") as f:
        return load_manifest(yaml.safe_load(f) or {})


def scan_registry(registry_dir: Path) -> dict:
    """Return ``{bridge_name: BridgeManifest}`` from ``*.yaml`` files in ``registry_dir``.

    Missing directory → empty dict. Bad manifests (unreadable, invalid) are
    skipped with a warning rather than aborting the whole scan, so one
    broken manifest cannot take down all bridges.
    """
    result: dict[str, BridgeManifest] = {}
    if not registry_dir.exists():
        return result
    for f in sorted(registry_dir.glob("*.yaml")):
        try:
            m = read_manifest(f)
        except (OSError, ValueError) as e:
            logger.warning("Bad manifest %s: %s", f, e)
            continue
        result[m.name] = m
    return result


class BridgeConfig:
    """Per-bridge configuration, with fallback to global defaults.

    ``global_extra`` carries env/config-driven options (mention patterns,
    allow-list, poll interval). The optional ``manifest`` carries the
    bridge's declared identity (service, target_format, capabilities) from
    its ``registry/<name>.yaml`` manifest.
    """

    def __init__(self, name: str, global_extra: dict, manifest: "BridgeManifest" = None):
        self.name = name
        self.manifest = manifest
        self.service = getattr(manifest, "service", None) if manifest else None
        self.target_format = list(getattr(manifest, "target_format", []) or []) if manifest else []
        bridge_key = f"bridge_{name}_"
        self.mention_patterns = self._get_opt(
            global_extra, bridge_key, "mention_patterns", "BRIDGE_MENTION_PATTERNS"
        )
        self.allowed_users = self._get_opt(
            global_extra, bridge_key, "allowed_users", "BRIDGE_ALLOWED_USERS"
        )
        self.allow_all = self._get_opt(
            global_extra, bridge_key, "allow_all", "BRIDGE_ALLOW_ALL_USERS"
        )
        self.poll_interval = self._get_opt(
            global_extra, bridge_key, "poll_interval", "BRIDGE_POLL_INTERVAL",
            fallback=str(DEFAULT_POLL_INTERVAL),
        )

    @staticmethod
    def _get_opt(extra: dict, prefix: str, key: str, env: str, fallback: str = "") -> str:
        """Check bridge-specific extra first, then global extra, then env, then fallback."""
        val = extra.get(f"{prefix}{key}")
        if val is not None:
            return str(val)
        val = extra.get(key)
        if val is not None:
            return str(val)
        return os.getenv(env, fallback).strip()

    def is_user_allowed(self, user_id: str) -> bool:
        """Check if a user is allowed on this bridge."""
        if self.allow_all.lower() in ("true", "1", "yes"):
            return True
        if self.allowed_users:
            allowed = [u.strip() for u in self.allowed_users.split(",")]
            return user_id in allowed
        return False


class BridgeAdapter(BasePlatformAdapter):
    """Gateway platform adapter that polls JSON files from a bridge directory.

    Directory structure::

        <bridge_dir>/
        ├── inbox/<bridge>/       ← written by external service, read by adapter
        ├── outbox/<bridge>/      ← written by adapter, read by external service
        ├── status/<bridge>/      ← written by external service, read by adapter
        └── media/
            ├── <bridge>/incoming/  ← incoming attachments
            └── <bridge>/outgoing/  ← outgoing attachments

    Attachment paths in JSON are **relative** to ``<bridge_dir>``. Both the
    adapter and the wrapper resolve them to absolute paths when reading.
    """

    SUPPORTS_MESSAGE_EDITING = False
    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    splits_long_messages = True

    def __init__(self, config: PlatformConfig):
        platform = Platform("bridge-adapter")
        super().__init__(config, platform)
        self.platform = platform
        extra = config.extra or {}
        self._extra = extra

        # Bridge directory
        bridge_dir = (
            extra.get("bridge_dir")
            or os.getenv("BRIDGE_DIR", "")
        ).strip()
        self._bridge_dir = Path(bridge_dir) if bridge_dir else None

        # Polling interval (global default)
        try:
            self._poll_interval = float(
                extra.get("poll_interval")
                or os.getenv("BRIDGE_POLL_INTERVAL", str(DEFAULT_POLL_INTERVAL))
            )
        except (ValueError, TypeError):
            self._poll_interval = DEFAULT_POLL_INTERVAL

        # Bridges to monitor (populated dynamically from registry/)
        self._bridges: list[str] = list(extra.get("bridges", []) or [])
        self._bridge_configs: dict[str, BridgeConfig] = {}
        self._manifests: dict[str, BridgeManifest] = {}

        # Registry directory (single source of truth for bridge identity)
        self._registry_dir = (
            self._bridge_dir / REGISTRY_DIR_NAME if self._bridge_dir else None
        )

        # Internal state
        self._poll_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._status_task: Optional[asyncio.Task] = None
        self._registry_task: Optional[asyncio.Task] = None
        self._running = False
        self._seen_files: set[str] = set()
        self._reaction_handler: Optional[callable] = None

        # Unified threads (T-058): {name → thread dict} loaded from disk.
        self._unified_threads: dict[str, dict] = {}

    # ── Lifecycle ────────────────────────────────────────────────────

    async def connect(self, **kwargs) -> bool:
        """Start polling inbox directories.

        Bridges are discovered from ``registry/<name>.yaml`` manifests —
        presence = registered, ``rm`` = unregistered. An initial
        :meth:`_reconcile_registry` runs synchronously so the first poll
        cycle sees all registered bridges, then a background
        :meth:`_registry_loop` picks up manifests added/removed at runtime.
        Returns ``False`` if no bridge directory exists or no bridges are
        registered yet (the latter is not fatal in itself, but there is
        nothing to poll).
        """
        if not self._bridge_dir or not self._bridge_dir.exists():
            logger.error("Bridge directory does not exist: %s", self._bridge_dir)
            return False

        self._ensure_media_dirs()

        # Registry-based discovery (replaces the old one-shot inbox scan).
        self._reconcile_registry_sync()

        # Load persisted unified threads (T-058) so the inbound mapper and
        # multicast sender see the same view across restarts.
        self._load_unified_threads()

        # Whether or not any bridges are registered yet, the adapter boots
        # and the registry loop keeps watching for manifests to appear at
        # runtime. Zero bridges now is NOT a failure — the new registry
        # design is "wait for manifests", not a one-shot scan.
        self._running = True
        self._registry_task = asyncio.create_task(self._registry_loop())
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._status_task = asyncio.create_task(self._status_loop())
        self._mark_connected()

        if not self._bridges:
            logger.warning(
                "No bridges registered in %s — adapter running, waiting for manifests",
                self._registry_dir,
            )
        else:
            logger.info(
                "Bridge adapter connected — %d bridge(s) from registry, polling %s every %.1fs",
                len(self._bridges),
                self._bridge_dir,
                self._poll_interval,
            )
        return True

    async def disconnect(self) -> None:
        """Stop polling and cleanup tasks."""
        self._running = False
        for task in (
            self._poll_task,
            self._cleanup_task,
            self._status_task,
            self._registry_task,
        ):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._poll_task = None
        self._cleanup_task = None
        self._status_task = None
        self._registry_task = None
        self._mark_disconnected()
        logger.info("Bridge adapter disconnected")

    # ── Registry: runtime discovery ────────────────────────────────────

    async def _registry_loop(self) -> None:
        """Periodically scan ``registry/`` and reconcile bridges.

        New manifests → register (create dirs, add to ``_bridges``).
        Removed manifests → unregister (cleanup dirs, drop from ``_bridges``).
        The poll/status/cleanup loops iterate over ``self._bridges`` directly,
        so changes here take effect on their next cycle without a restart.
        """
        while self._running:
            try:
                await self._reconcile_registry()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Registry reconcile error: %s", e)
            await asyncio.sleep(REGISTRY_POLL_INTERVAL)

    async def _reconcile_registry(self) -> None:
        """Async wrapper around :meth:`_reconcile_registry_sync`.

        Kept async so the registry loop can ``await`` it; the work itself is
        synchronous (file I/O + in-memory bookkeeping).
        """
        self._reconcile_registry_sync()

    def _reconcile_registry_sync(self) -> None:
        """Diff the current registry against ``self._bridges`` and apply changes."""
        if not self._registry_dir:
            return
        manifests = scan_registry(self._registry_dir)

        # New bridges: register any manifest not yet known.
        for name, m in manifests.items():
            if name not in self._bridges:
                self._register_bridge(name, m)
                logger.info("Bridge '%s' registered (registry)", name)

        # Removed bridges: drop any known bridge no longer in the registry.
        for name in list(self._bridges):
            if name not in manifests:
                self._unregister_bridge(name)
                logger.info("Bridge '%s' unregistered (registry rm)", name)

    def _register_bridge(self, name: str, manifest: BridgeManifest) -> None:
        """Register a new bridge: track it and create its directory tree."""
        self._bridges.append(name)
        self._manifests[name] = manifest
        self._bridge_configs[name] = BridgeConfig(name, self._extra, manifest=manifest)
        if not self._bridge_dir:
            return
        # Create the per-bridge directory tree so the next poll cycle can
        # use it immediately. inbox/outbox are created on demand by the
        # wrapper/adapter, but pre-creating avoids first-cycle races.
        (self._bridge_dir / "inbox" / name).mkdir(parents=True, exist_ok=True)
        (self._bridge_dir / "outbox" / name).mkdir(parents=True, exist_ok=True)
        (self._bridge_dir / "status" / name).mkdir(parents=True, exist_ok=True)
        (self._bridge_dir / "media" / name / "incoming").mkdir(parents=True, exist_ok=True)
        (self._bridge_dir / "media" / name / "outgoing").mkdir(parents=True, exist_ok=True)

    def _unregister_bridge(self, name: str) -> None:
        """Unregister a bridge: drop it and clean up its status/media dirs.

        inbox/outbox are left in place (they may still contain in-flight
        files the wrapper needs to drain); only status/ and media/ are
        removed, since a re-registration would recreate them anyway.
        """
        if name in self._bridges:
            self._bridges.remove(name)
        self._manifests.pop(name, None)
        self._bridge_configs.pop(name, None)
        if not self._bridge_dir:
            return
        for sub in ("status", "media"):
            d = self._bridge_dir / sub / name
            if d.exists():
                shutil.rmtree(d, ignore_errors=True)

    # ── Unified threads (T-058) ────────────────────────────────────────

    UNIFIED_MODES = ("participant", "reactive", "silent", "protokoll")

    def _unified_path(self) -> Path:
        """Path to the unified_threads.json persistence file."""
        return self._bridge_dir / "unified_threads.json" if self._bridge_dir else Path()

    def _load_unified_threads(self) -> None:
        """Load unified threads from ``unified_threads.json`` (best-effort).

        Missing or unreadable files reset ``_unified_threads`` to an empty
        dict so the adapter keeps running instead of crashing on a bad file.
        """
        self._unified_threads = {}
        p = self._unified_path()
        if not p or not p.exists():
            return
        try:
            data = json.loads(p.read_text("utf-8"))
            if isinstance(data, dict):
                self._unified_threads = data
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Bad unified_threads.json: %s", e)

    def _save_unified_threads(self) -> None:
        """Persist unified threads to ``unified_threads.json``."""
        p = self._unified_path()
        if not p:
            return
        try:
            p.write_text(
                json.dumps(self._unified_threads, ensure_ascii=False, indent=2),
                "utf-8",
            )
        except OSError as e:
            logger.error("Failed to save unified_threads.json: %s", e)

    def _find_unified_for_member(self, bridge: str, chat_id: str) -> Optional[str]:
        """Return the unified thread name ``{bridge}:{chat_id}`` belongs to.

        A member is identified by the key ``{bridge}:{chat_id}`` in a
        thread's ``members`` dict. Returns the first matching thread name
        (or ``None`` if not a member of any thread).
        """
        key = f"{bridge}:{chat_id}"
        for name, thread in self._unified_threads.items():
            if key in thread.get("members", {}):
                return name
        return None

    @staticmethod
    def _unified_member_key(bridge: str, data: dict) -> str:
        """Build the member key ``{bridge}:{chat_id}`` for a ``/unified`` cmd."""
        chat_id = (data.get("chat", {}) or {}).get("id", "") or data.get("sender", "")
        return f"{bridge}:{chat_id}"

    def _unified_member_record(self, bridge: str, data: dict) -> dict:
        """Build a member record (for storage in ``members``)."""
        chat_id = (data.get("chat", {}) or {}).get("id", "") or data.get("sender", "")
        return {
            "bridge": bridge,
            "chat_id": chat_id,
            "user_id": data.get("sender", ""),
            "user_name": data.get("sender_name") or data.get("sender", ""),
            "joined_at": _now_iso(),
        }

    async def _send_reply(self, bridge: str, data: dict, message: str) -> None:
        """Write an outbox reply to the sender of a ``/unified`` command.

        Uses the ``~``-addressed routable form so the wrapper on the other
        end can route it back to the originating chat.
        """
        chat_id = (data.get("chat", {}) or {}).get("id", "") or data.get("sender", "")
        target = f"{bridge}~{chat_id}"
        await self._write_outbox(bridge, target, text=message)

    # ── /unified command handlers ────────────────────────────────────

    async def _handle_unified_command(
        self, bridge: str, data: dict, filepath: Path
    ) -> None:
        """Parse and dispatch a ``/unified`` command from an inbox message."""
        parts = (data.get("text", "") or "").split()
        # parts[0] == "/unified"
        sub = parts[1] if len(parts) > 1 else "help"
        args = parts[2:]

        dispatch = {
            "help": None,
            "create": self._cmd_unified_create,
            "status": self._cmd_unified_status,
            "join": self._cmd_unified_join,
            "leave": self._cmd_unified_leave,
            "members": self._cmd_unified_members,
            "mode": self._cmd_unified_mode,
        }
        handler = dispatch.get(sub)
        if sub == "help":
            await self._send_reply(bridge, data, self._unified_help_text())
        elif handler is None:
            await self._send_reply(
                bridge, data,
                f"Unknown /unified command '{sub}'. Try /unified help.",
            )
        elif sub == "status":
            await self._send_reply(bridge, data, self._cmd_unified_status(bridge, data))
        elif sub == "create":
            name = args[0] if args else ""
            if not name:
                await self._send_reply(bridge, data, "Usage: /unified create <name>")
            else:
                await self._send_reply(bridge, data, self._cmd_unified_create(bridge, data, name))
        elif sub == "join":
            name = args[0] if args else ""
            if not name:
                await self._send_reply(bridge, data, "Usage: /unified join <name>")
            else:
                await self._send_reply(bridge, data, self._cmd_unified_join(bridge, data, name))
        elif sub == "leave":
            name = args[0] if args else ""
            if not name:
                await self._send_reply(bridge, data, "Usage: /unified leave <name>")
            else:
                await self._send_reply(bridge, data, self._cmd_unified_leave(bridge, data, name))
        elif sub == "members":
            name = args[0] if args else ""
            if not name:
                await self._send_reply(bridge, data, "Usage: /unified members <name>")
            else:
                await self._send_reply(bridge, data, self._cmd_unified_members(bridge, data, name))
        elif sub == "mode":
            name = args[0] if args else ""
            mode = args[1] if len(args) > 1 else ""
            if not name or not mode:
                await self._send_reply(
                    bridge, data,
                    "Usage: /unified mode <name> <participant|reactive|silent|protokoll>",
                )
            else:
                await self._send_reply(
                    bridge, data, self._cmd_unified_mode(bridge, data, name, mode)
                )

        # Remove the processed inbox file.
        try:
            filepath.unlink()
        except OSError:
            pass

    @staticmethod
    def _unified_help_text() -> str:
        return (
            "Unified Threads — commands:\n"
            "  /unified create <name>     — create a new unified thread\n"
            "  /unified status             — list all threads you're in\n"
            "  /unified join <name>        — join a unified thread\n"
            "  /unified leave <name>       — leave a unified thread\n"
            "  /unified members <name>     — list members of a thread\n"
            "  /unified mode <name> <mode> — set mode (participant | reactive | silent | protokoll)\n"
            "  /unified help                — this help\n\n"
            "Members share one agent session across bridges. Reply to a "
            "thread with the address unified~<name> (multicast to all members)."
        )

    def _cmd_unified_create(self, bridge: str, data: dict, name: str) -> str:
        """Create a unified thread with the sender as first member."""
        if not name:
            return "Usage: /unified create <name>"
        if name in self._unified_threads:
            return f"Unified thread '{name}' already exists."
        member = self._unified_member_record(bridge, data)
        self._unified_threads[name] = {
            "name": name,
            "created_at": _now_iso(),
            "created_by": data.get("sender", ""),
            "members": {self._unified_member_key(bridge, data): member},
            "aliases": [],
            "mode": "participant",
        }
        self._save_unified_threads()
        return f"Created unified thread '{name}'. You are the first member. Reply to unified~{name}."

    def _cmd_unified_status(self, bridge: str, data: dict) -> str:
        """List all unified threads and their members/mode."""
        if not self._unified_threads:
            return "No unified threads yet. Create one with /unified create <name>."
        lines = ["Unified threads:"]
        for name, thread in self._unified_threads.items():
            n = len(thread.get("members", {}))
            lines.append(f"  • {name} — {n} member(s), mode={thread.get('mode', 'participant')}")
        return "\n".join(lines)

    def _cmd_unified_members(self, bridge: str, data: dict, name: str) -> str:
        """List the members of a unified thread."""
        thread = self._unified_threads.get(name)
        if not thread:
            return f"Unified thread '{name}' not found."
        members = thread.get("members", {})
        if not members:
            return f"Unified thread '{name}' has no members."
        lines = [f"Members of '{name}':"]
        for m in members.values():
            lines.append(f"  • {m['user_name']} ({m['user_id']}) — {m['bridge']}~{m['chat_id']}")
        return "\n".join(lines)

    def _cmd_unified_join(self, bridge: str, data: dict, name: str) -> str:
        """Add the sender as a member of a unified thread."""
        thread = self._unified_threads.get(name)
        if not thread:
            return f"Unified thread '{name}' not found."
        key = self._unified_member_key(bridge, data)
        if key in thread["members"]:
            return f"You are already a member of '{name}'."
        thread["members"][key] = self._unified_member_record(bridge, data)
        self._save_unified_threads()
        return f"Joined unified thread '{name}'. Reply to unified~{name}."

    def _cmd_unified_leave(self, bridge: str, data: dict, name: str) -> str:
        """Remove the sender from a unified thread."""
        thread = self._unified_threads.get(name)
        if not thread:
            return f"Unified thread '{name}' not found."
        key = self._unified_member_key(bridge, data)
        if key not in thread.get("members", {}):
            return f"You are not a member of '{name}'."
        del thread["members"][key]
        self._save_unified_threads()
        return f"Left unified thread '{name}'."

    def _cmd_unified_mode(self, bridge: str, data: dict, name: str, mode: str) -> str:
        """Set the mode of a unified thread (T-059 implements the logic)."""
        thread = self._unified_threads.get(name)
        if not thread:
            return f"Unified thread '{name}' not found."
        if mode not in self.UNIFIED_MODES:
            return f"Invalid mode '{mode}'. Valid: {', '.join(self.UNIFIED_MODES)}."
        thread["mode"] = mode
        self._save_unified_threads()
        return f"Mode of '{name}' set to '{mode}'."

    # ── Polling ─────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Poll all bridge inbox directories continuously."""
        while self._running:
            try:
                await self._poll_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Poll error: %s", e)
            await asyncio.sleep(self._poll_interval)

    async def _poll_all(self) -> None:
        """Check each bridge's inbox for new files."""
        for bridge in self._bridges:
            inbox_dir = self._bridge_dir / "inbox" / bridge
            if not inbox_dir.exists():
                continue
            try:
                files = sorted(inbox_dir.iterdir(), key=lambda p: p.stat().st_mtime)
            except OSError:
                continue

            for filepath in files:
                if not filepath.suffix == ".json":
                    continue
                key = str(filepath.absolute())
                if key in self._seen_files:
                    continue
                self._seen_files.add(key)

                try:
                    data = json.loads(filepath.read_text("utf-8"))
                except (json.JSONDecodeError, OSError) as e:
                    logger.warning("Invalid JSON in %s: %s", filepath, e)
                    continue

                await self._process_incoming(bridge, data, filepath)

    async def _process_incoming(
        self, bridge: str, data: dict, filepath: Path
    ) -> None:
        """Parse an inbox JSON file and dispatch as MessageEvent or reaction."""
        # Check for reaction events
        if data.get("type") == "reaction":
            await self._process_reaction(bridge, data, filepath)
            return

        # Validate required fields
        sender = data.get("sender", "")
        text = data.get("text", "")
        chat = data.get("chat", {})
        chat_id = chat.get("id", "") or sender
        chat_type = chat.get("type", "direct")
        chat_name = chat.get("name")

        if not sender:
            logger.warning("Inbox message missing 'sender', skipping %s", filepath)
            return

        # /unified commands (T-058): treat as adapter command, not a normal
        # message. Parsed and dispatched here so they never reach the agent.
        if text.strip().startswith("/unified"):
            await self._handle_unified_command(bridge, data, filepath)
            return

        # User authorization is handled by the Gateway framework
        # (authz_mixin._is_user_authorized), which reads BRIDGE_ALLOWED_USERS
        # via the plugin registry and default-denies. The adapter must NOT
        # gate here: deleting the file before the gateway sees it would break
        # the DM pairing flow (a new user's first DM would be dropped before
        # the gateway can issue a pairing code). Let the framework decide.

        # Build session source. The session chat_id MUST be the routable
        # address (<bridge>~<target>, T-056) so the gateway can route a
        # session reply back to the bridge. The raw identity stays as the
        # chat_name for display.
        thread_id = data.get("thread_id") or data.get("thread_root")

        # Unified thread mapping (T-058): if {bridge}:{chat_id} is a member of
        # a unified thread, map the session onto the shared virtual thread so
        # all members share one agent session (chat_type="thread",
        # chat_id="unified", thread_id=<name>). Replies go to unified~<name>.
        unified_name = self._find_unified_for_member(bridge, chat_id)
        if unified_name:
            n_members = len(self._unified_threads[unified_name].get("members", {}))
            source = SessionSource(
                platform=Platform("bridge-adapter"),
                chat_id="unified",
                chat_name=unified_name,
                chat_type="thread",
                user_id=sender,
                user_name=data.get("sender_name") or sender,
                thread_id=unified_name,
            )
            routing_ctx = (
                f"Message from {sender}, bridge {bridge}, "
                f"unified thread '{unified_name}' ({n_members} members), "
                f"reply to unified~{unified_name}"
            )
        else:
            routable_chat_id = f"{bridge}~{chat_id}"
            source = SessionSource(
                platform=Platform("bridge-adapter"),
                chat_id=routable_chat_id,
                chat_name=chat_name or chat_id,
                chat_type=chat_type,
                user_id=sender,
                user_name=data.get("sender_name") or sender,
                thread_id=thread_id,
            )

            # Append a compact routing context line so the agent knows exactly
            # which bridge this message came from and where to reply. This is
            # stable for the duration of the message (no prompt-cache invalidation),
            # and lets the agent address replies as <bridge>~<target> without
            # digging into raw_message. (Bridge-target separator is ``~`` — T-056.)
            routing_ctx = f"Message from {sender}, bridge {bridge}, reply to {routable_chat_id}"
            if data.get("reply_to"):
                routing_ctx += f" (reply_to {data.get('reply_to')})"
        effective_text = f"{text}\n\n[{routing_ctx}]" if text else f"[{routing_ctx}]"

        # Determine message type
        attachments = data.get("attachments", [])
        if attachments:
            att_type = attachments[0].get("type", "document")
            if att_type == "image":
                msg_type = MessageType.PHOTO
            elif att_type == "video":
                msg_type = MessageType.VIDEO
            elif att_type == "audio":
                msg_type = MessageType.AUDIO
            elif att_type == "sticker":
                msg_type = MessageType.STICKER
            else:
                msg_type = MessageType.DOCUMENT
        else:
            msg_type = MessageType.TEXT

        # Resolve attachment paths
        media_urls = []
        media_types = []
        for att in attachments:
            raw_path = att.get("url") or att.get("path", "")
            if raw_path:
                abs_path = self._resolve_media_path(raw_path)
                if abs_path and abs_path.exists():
                    media_urls.append(str(abs_path))
                    media_types.append(att.get("type", "document"))
                else:
                    logger.warning(
                        "Attachment not found: %s (resolved: %s)", raw_path, abs_path
                    )

        event = MessageEvent(
            text=effective_text,
            message_type=msg_type,
            source=source,
            raw_message=data,
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=data.get("reply_to"),
        )

        # Mention gating for group chats
        if chat_type != "direct" and not self._is_mentioned(text, bridge):
            logger.debug("Not mentioned in group chat, skipping: %s", filepath)
            # Remove the file so it isn't re-seen forever. _seen_files already
            # marks it, so without the unlink a dropped group message stays in
            # the inbox AND in _seen_files → never processed again until the
            # gateway restarts (the "needs a restart" bug). Mirror the
            # is_user_allowed path which also unlinks.
            try:
                filepath.unlink()
            except OSError:
                pass
            return

        await self.handle_message(event)

        # Remove processed file
        try:
            filepath.unlink()
        except OSError:
            pass

    async def _process_reaction(
        self, bridge: str, data: dict, filepath: Path
    ) -> None:
        """Handle a reaction event from the inbox."""
        if not self._reaction_handler:
            logger.debug("No reaction handler set, skipping reaction")
            try:
                filepath.unlink()
            except OSError:
                pass
            return

        reaction_event = {
            "platform": "bridge-adapter",
            "event_name": data.get("event", "reaction:added"),
            "reaction": data.get("reaction", ""),
            "user_id": data.get("sender", ""),
            "item_user_id": data.get("item_user_id", ""),
            "channel_id": data.get("chat", {}).get("id", ""),
            "message_ts": data.get("message_id", ""),
            "event_ts": data.get("timestamp", ""),
            "raw_event": data,
        }
        try:
            await self._reaction_handler(reaction_event)
        except Exception as e:
            logger.warning("Reaction handler failed: %s", e)

        try:
            filepath.unlink()
        except OSError:
            pass

    # ── Sending ──────────────────────────────────────────────────────

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        """Write an outgoing text message to the outbox.

        Routing fallback (T-053): if the target isn't routable — the bridge
        prefix isn't registered, or the target doesn't match the bridge's
        ``target_format`` — no outbox file is written. Instead a
        ``SendResult(success=False, error=...)`` is returned with a clear
        message so the caller (agent) can correct the addressing instead of
        silently dropping or misrouting the message.

        Unified threads (T-058): a target of the form ``unified~<name>`` is
        special-cased *before* bridge resolution — the message is multicast
        to every member's own routable address (one outbox JSON per member
        bridge), so a single agent reply reaches all bridges sharing the
        thread.
        """
        if isinstance(content, list):
            text = " ".join(str(c) for c in content if c)
        else:
            text = str(content)

        # Unified multicast (T-058): must run before _resolve_bridge_or_none,
        # since "unified" is not a registered bridge prefix and would
        # otherwise trigger the T-053 routing fallback.
        if chat_id.startswith("unified~"):
            name = chat_id.split("~", 1)[1]
            thread = self._unified_threads.get(name)
            if not thread:
                return SendResult(
                    success=False,
                    message_id="",
                    error=f"Unified thread '{name}' not found",
                    error_kind="routing",
                )
            members = thread.get("members", {})
            if not members:
                return SendResult(
                    success=False,
                    message_id="",
                    error=f"Unified thread '{name}' has no members",
                    error_kind="routing",
                )
            results = []
            for member in members.values():
                mb = member.get("bridge")
                mc = member.get("chat_id")
                if not mb or not mc:
                    continue
                r = await self._write_outbox(
                    mb, f"{mb}~{mc}", text=text, reply_to=reply_to,
                )
                results.append(r)
            ok = bool(results) and all(r.success for r in results)
            return SendResult(
                success=ok,
                message_id=",".join(r.message_id for r in results if r.message_id),
                error="" if ok else "one or more member sends failed",
            )

        bridge = self._resolve_bridge_or_none(chat_id)
        if bridge is None:
            return SendResult(
                success=False,
                message_id="",
                error=(
                    f"Target '{chat_id}' is not routable: bridge prefix "
                    f"unknown. Registered bridges: "
                    f"{', '.join(self._bridges) or '(none)'}. "
                    f"Address as <bridge>~<target>, e.g. imsg~user@example.com."
                ),
                error_kind="routing",
            )

        # Extract the target portion (after "<bridge>~") for format validation.
        # The ~ separator (T-056) keeps the bridge prefix out of the target
        # so it can't mask an invalid target (e.g. a digit in the prefix
        # satisfying the ``phone`` digit check).
        target_part = chat_id.split("~", 1)[1] if "~" in chat_id else chat_id
        if not self._validate_target(bridge, target_part):
            manifest = self._manifests.get(bridge)
            formats = ", ".join(manifest.target_format) if manifest else "any"
            return SendResult(
                success=False,
                message_id="",
                error=(
                    f"Target '{chat_id}' is not routable: '{target_part}' does "
                    f"not match bridge '{bridge}' target_format ({formats}). "
                    f"Use a valid target for this bridge."
                ),
                error_kind="routing",
            )

        thread_id = (metadata or {}).get("thread_id")
        return await self._write_outbox(bridge, chat_id, text=text, reply_to=reply_to, thread_id=thread_id)

    async def send_typing(self, chat_id):
        """Write a typing indicator to the outbox."""
        bridge = self._resolve_bridge(chat_id)
        return await self._write_outbox(bridge, chat_id, typing=True)

    async def send_image(self, chat_id, image_url, caption=None, reply_to=None):
        """Write an image message to the outbox."""
        bridge = self._resolve_bridge(chat_id)
        out_id = str(uuid.uuid4())[:8]
        rel_path = self._copy_to_media_dir(bridge, image_url, out_id, "outgoing")
        if not rel_path:
            rel_path = image_url
        attachment = {
            "type": "image",
            "path": rel_path,
            "caption": caption or "",
        }
        return await self._write_outbox(
            bridge, chat_id, text=caption or "", attachments=[attachment],
            reply_to=reply_to,
        )

    async def send_document(self, chat_id, path, caption=None, reply_to=None):
        """Write a document message to the outbox."""
        bridge = self._resolve_bridge(chat_id)
        out_id = str(uuid.uuid4())[:8]
        rel_path = self._copy_to_media_dir(bridge, path, out_id, "outgoing")
        if not rel_path:
            rel_path = path
        attachment = {
            "type": "document",
            "path": rel_path,
            "caption": caption or "",
        }
        return await self._write_outbox(
            bridge, chat_id, text=caption or "", attachments=[attachment],
            reply_to=reply_to,
        )

    async def get_chat_info(self, chat_id):
        return {"name": chat_id, "type": "dm"}

    # ── Reaction handler registration ────────────────────────────────

    def set_reaction_handler(
        self, handler: Optional[callable]
    ) -> None:
        """Register a handler for reaction events from the inbox."""
        self._reaction_handler = handler

    # ── Status polling ──────────────────────────────────────────────

    async def _status_loop(self) -> None:
        """Periodically read status/ files and log bridge health."""
        while self._running:
            try:
                await self._poll_status()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("Status poll error: %s", e)
            await asyncio.sleep(STATUS_POLL_INTERVAL)

    async def _poll_status(self) -> None:
        """Read status/<bridge>/ files and log health info."""
        for bridge in self._bridges:
            status_dir = self._bridge_dir / "status" / bridge
            if not status_dir.exists():
                continue
            try:
                for f in sorted(status_dir.glob("*.json")):
                    try:
                        data = json.loads(f.read_text("utf-8"))
                        connected = data.get("connected", False)
                        error = data.get("error")
                        if error:
                            logger.warning(
                                "Bridge '%s' reports error: %s", bridge, error
                            )
                        elif not connected:
                            logger.info("Bridge '%s' is disconnected", bridge)
                        else:
                            logger.debug(
                                "Bridge '%s' healthy (last_seen: %s)",
                                bridge, data.get("last_seen", "?"),
                            )
                    except (json.JSONDecodeError, OSError):
                        continue
            except OSError:
                continue

    # ── Cleanup ─────────────────────────────────────────────────────

    async def _cleanup_loop(self) -> None:
        """Periodically clean up old media and stale outbox files."""
        while self._running:
            try:
                await self._run_cleanup()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("Cleanup error: %s", e)
            await asyncio.sleep(CLEANUP_INTERVAL)

    async def _run_cleanup(self) -> None:
        """Remove old media files and stale outbox JSON files."""
        now = time.time()
        if not self._bridge_dir:
            return

        # Cleanup media/ files older than MEDIA_CLEANUP_MAX_AGE
        media_root = self._bridge_dir / "media"
        if media_root.exists():
            for f in media_root.rglob("*"):
                if f.is_file():
                    age = now - f.stat().st_mtime
                    if age > MEDIA_CLEANUP_MAX_AGE:
                        try:
                            f.unlink()
                            logger.debug("Cleaned up old media: %s", f)
                        except OSError:
                            pass

        # Cleanup stale outbox JSON files (no wrapper reading them)
        outbox_root = self._bridge_dir / "outbox"
        if outbox_root.exists():
            for f in outbox_root.rglob("*.json"):
                age = now - f.stat().st_mtime
                if age > OUTBOX_CLEANUP_MAX_AGE:
                    try:
                        f.unlink()
                        logger.debug("Cleaned up stale outbox: %s", f)
                    except OSError:
                        pass

    # ── Media helpers ────────────────────────────────────────────────

    def _ensure_media_dirs(self) -> None:
        """Create media/ subdirectory structure for all bridges."""
        if not self._bridge_dir:
            return
        for bridge in self._bridges:
            (self._bridge_dir / "media" / bridge / "incoming").mkdir(
                parents=True, exist_ok=True
            )
            (self._bridge_dir / "media" / bridge / "outgoing").mkdir(
                parents=True, exist_ok=True
            )

    def _resolve_media_path(self, raw_path: str) -> Optional[Path]:
        """Resolve a (possibly relative) attachment path to an absolute path."""
        p = Path(raw_path)
        if p.is_absolute():
            return p
        if self._bridge_dir:
            return (self._bridge_dir / raw_path).resolve()
        return None

    def _copy_to_media_dir(
        self, bridge: str, source_path: str, msg_id: str, direction: str
    ) -> Optional[str]:
        """Copy a file to ``media/<bridge>/<direction>/<msg_id>-<filename>``.

        Returns the **relative** path (from bridge dir) on success, or None
        if the source file doesn't exist or can't be copied.
        """
        source = Path(source_path)
        if not source.exists():
            logger.warning("Cannot copy attachment: %s does not exist", source_path)
            return None

        target_dir = self._bridge_dir / "media" / bridge / direction
        target_dir.mkdir(parents=True, exist_ok=True)

        ext = source.suffix or ""
        target_name = f"{msg_id}-{source.stem}{ext}"
        target = target_dir / target_name

        try:
            shutil.copy2(str(source), str(target))
            rel = target.relative_to(self._bridge_dir)
            logger.debug("Copied attachment: %s → %s", source, target)
            return str(rel)
        except OSError as e:
            logger.error("Failed to copy attachment %s: %s", source, e)
            return None

    # ── Bridge helpers ───────────────────────────────────────────────

    def _resolve_bridge(self, chat_id: str) -> str:
        """Determine which bridge a chat_id belongs to (defaults to first).

        The bridge-target separator is ``~`` (T-056): a chat_id of the form
        ``<bridge>~<target>`` routes to that bridge if its prefix is
        registered. ``~`` was chosen so it does not collide with any target
        format (email, phone, chat_id) or the platform separator ``:`` used
        by the cron scheduler.
        """
        if "~" in chat_id:
            prefix, _, _ = chat_id.partition("~")
            if prefix in self._bridges:
                return prefix
        return self._bridges[0] if self._bridges else "default"

    def _resolve_bridge_or_none(self, chat_id: str) -> Optional[str]:
        """Resolve the bridge for a chat_id, or ``None`` if not routable.

        Unlike :meth:`_resolve_bridge`, this does NOT fall back to a default
        bridge. Returns the bridge only when the ``<bridge>~`` prefix is an
        actually registered bridge (routing fallback / T-053). The bridge-
        target separator is ``~`` (T-056).
        """
        if "~" in chat_id:
            prefix, _, _ = chat_id.partition("~")
            if prefix in self._bridges:
                return prefix
        # Bare chat_id without a registered bridge prefix is unroutable
        if not self._bridges:
            return None
        # A bare target with no '~' and exactly one bridge → route to it
        if "~" not in chat_id and len(self._bridges) == 1:
            return self._bridges[0]
        return None

    def _validate_target(self, bridge: str, target: str) -> bool:
        """Validate a target against a bridge's declared target_format.

        Permissive by design: if the bridge has no manifest or declares no
        ``target_format``, we accept anything (the full routing fallback —
        T-053 — decides what happens when validation fails).
        """
        manifest = self._manifests.get(bridge)
        if manifest is None:
            return True  # unknown bridge → permissive
        return manifest.accepts_target(target)

    async def _write_outbox(
        self, bridge: str, chat_id: str, text: str = "",
        attachments: list = None, typing: bool = False,
        reply_to: str = None, thread_id: str = None,
    ) -> SendResult:
        """Write a JSON file to the outbox directory."""
        outbox_dir = self._bridge_dir / "outbox" / bridge
        outbox_dir.mkdir(parents=True, exist_ok=True)

        msg_id = str(uuid.uuid4())
        outbox = {
            "bridge": bridge,
            "id": f"out_{msg_id[:8]}",
            "target": chat_id,
            "text": text,
            "attachments": attachments or [],
            "typing": typing,
            "reply_to": reply_to,
            "thread_id": thread_id,
            "metadata": {},
        }

        filepath = outbox_dir / f"{msg_id}.json"
        try:
            filepath.write_text(json.dumps(outbox, ensure_ascii=False, indent=2), "utf-8")
            logger.debug("Wrote outbox: %s", filepath)
            return SendResult(success=True, message_id=outbox["id"])
        except OSError as e:
            logger.error("Failed to write outbox %s: %s", filepath, e)
            return SendResult(success=False, message_id="", error=str(e))

    def _is_mentioned(self, text: str, bridge: str = "") -> bool:
        """Check if the agent is mentioned, using bridge-specific patterns if available."""
        if not text:
            return False
        bc = self._bridge_configs.get(bridge) if bridge else None
        if bc and bc.mention_patterns:
            patterns = self._compile_mention_patterns(bc.mention_patterns)
        else:
            patterns = self._compile_mention_patterns(None)
        return any(p.search(text) for p in patterns)

    @staticmethod
    def _compile_mention_patterns(raw) -> list[re.Pattern]:
        if raw is None:
            patterns = list(DEFAULT_MENTION_PATTERNS)
        elif isinstance(raw, str):
            try:
                loaded = json.loads(raw) if raw.strip() else []
            except Exception:
                loaded = raw.split(",") if raw else []
            patterns = loaded if isinstance(loaded, list) else [raw]
        elif isinstance(raw, list):
            patterns = raw
        else:
            patterns = list(DEFAULT_MENTION_PATTERNS)
        return [re.compile(p, re.IGNORECASE) for p in patterns]


# ── Plugin Registration ──────────────────────────────────────────────


def _build_adapter(config):
    return BridgeAdapter(config)


def check_requirements() -> bool:
    return bool(os.getenv("BRIDGE_DIR"))


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(extra.get("bridge_dir") or os.getenv("BRIDGE_DIR"))


def _resolve_bridge_dir(pconfig) -> Optional[Path]:
    """Resolve the bridge directory from a PlatformConfig or env."""
    extra = getattr(pconfig, "extra", {}) or {}
    bridge_dir = extra.get("bridge_dir") or os.getenv("BRIDGE_DIR", "").strip()
    return Path(bridge_dir) if bridge_dir else None


async def _standalone_send(pconfig, chat_id, message, thread_id=None,
                           media_files=None, force_document=False) -> dict:
    """Out-of-process cron delivery for the bridge adapter.

    Implements the ``standalone_sender_fn`` contract so ``deliver=bridge-adapter``
    cron jobs can write to the outbox even when the cron process runs separately
    from the gateway (no live adapter). Mirrors ``BridgeAdapter._write_outbox``:
    parses ``<bridge>~<target>``, validates the target against the bridge's
    ``target_format``, and writes a JSON file to ``outbox/<bridge>/``.

    Returns a dict with ``success``/``error`` per the framework contract.
    """
    if not chat_id:
        return {"error": "bridge standalone send: empty chat_id"}

    bridge_dir = _resolve_bridge_dir(pconfig)
    if bridge_dir is None:
        return {"error": "bridge standalone send: BRIDGE_DIR not configured"}

    # Parse <bridge>~<target> (T-056 separator).
    if "~" in chat_id:
        bridge, _, target = chat_id.partition("~")
    else:
        bridge, target = chat_id, chat_id

    # Validate the target against the bridge's declared target_format.
    manifest = None
    manifest_path = bridge_dir / "registry" / f"{bridge}.yaml"
    try:
        if manifest_path.exists():
            manifest = read_manifest(manifest_path)
    except Exception:
        manifest = None
    if manifest is not None and not manifest.accepts_target(target):
        formats = ", ".join(manifest.target_format) or "any"
        return {
            "error": (
                f"Target '{target}' does not match bridge '{bridge}' "
                f"target_format ({formats}). Address as <bridge>~<target>."
            )
        }

    # Write the outbox JSON (same shape as _write_outbox).
    outbox_dir = bridge_dir / "outbox" / bridge
    try:
        outbox_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        return {"error": f"bridge standalone send: cannot create outbox dir: {e}"}

    msg_id = str(uuid.uuid4())
    outbox = {
        "bridge": bridge,
        "id": f"out_{msg_id[:8]}",
        "target": target,
        "text": str(message),
        "attachments": [],
        "typing": False,
        "reply_to": None,
        "thread_id": thread_id,
        "metadata": {},
    }
    filepath = outbox_dir / f"{msg_id}.json"
    try:
        filepath.write_text(json.dumps(outbox, ensure_ascii=False, indent=2), "utf-8")
    except OSError as e:
        return {"error": f"bridge standalone send: write failed: {e}"}
    return {"success": True, "platform": "bridge-adapter", "chat_id": chat_id,
            "message_id": outbox["id"]}


def env_enablement_fn() -> Optional[dict]:
    """Seed PlatformConfig.extra from env vars before adapter construction."""
    bridge_dir = os.getenv("BRIDGE_DIR", "").strip()
    if not bridge_dir:
        return None
    extra = {"bridge_dir": bridge_dir}
    poll_interval = os.getenv("BRIDGE_POLL_INTERVAL", "").strip()
    if poll_interval:
        extra["poll_interval"] = poll_interval
    return extra


def register(ctx):
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="bridge-adapter",
        label="Bridge Adapter",
        adapter_factory=_build_adapter,
        check_fn=check_requirements,
        validate_config=validate_config,
        env_enablement_fn=env_enablement_fn,
        required_env=["BRIDGE_DIR"],
        install_hint="Set BRIDGE_DIR to a directory with inbox/ outbox/ status/ subdirs",
        allowed_users_env="BRIDGE_ALLOWED_USERS",
        allow_all_env="BRIDGE_ALLOW_ALL_USERS",
        cron_deliver_env_var="BRIDGE_HOME_CHANNEL",
        # Out-of-process cron delivery. Without this hook, deliver=bridge-adapter
        # cron jobs fail with "No live adapter" when cron runs separately from
        # the gateway (T-057).
        standalone_sender_fn=_standalone_send,
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="🔌",
        platform_hint=(
            "You are on a generic text-messaging platform via the Bridge Adapter. "
            "Bridges (imsg, Matrix, Telegram, Talk, ...) self-register as YAML "
            "manifests in <bridge_dir>/registry/. To see which bridges are "
            "registered and their accepted target formats, read those manifest "
            "files (e.g. registry/imsg.yaml has target_format). To send a "
            "message, address the target as <bridge>~<target>, e.g. "
            "imsg~user@example.com for iMessage. Match the target to "
            "the bridge's target_format (email / phone / chat_id)."
        ),
    )
