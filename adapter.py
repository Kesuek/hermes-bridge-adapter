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
from pathlib import Path
from typing import Optional

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
DEFAULT_MENTION_PATTERNS = [
    r"(?<![\\w@])@?hermes\\s+agent\\b[,:\\-]?",
    r"(?<![\\w@])@?hermes\\b[,:\\-]?",
]


class BridgeConfig:
    """Per-bridge configuration, with fallback to global defaults."""

    def __init__(self, name: str, global_extra: dict):
        self.name = name
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

        # Bridges to monitor
        self._bridges: list[str] = extra.get("bridges", []) or []
        self._bridge_configs: dict[str, BridgeConfig] = {}

        # Internal state
        self._poll_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._status_task: Optional[asyncio.Task] = None
        self._running = False
        self._seen_files: set[str] = set()
        self._reaction_handler: Optional[callable] = None

    # ── Lifecycle ────────────────────────────────────────────────────

    async def connect(self, **kwargs) -> bool:
        """Start polling inbox directories."""
        if not self._bridge_dir or not self._bridge_dir.exists():
            logger.error("Bridge directory does not exist: %s", self._bridge_dir)
            return False

        self._ensure_media_dirs()

        # Auto-discover bridges
        inbox_root = self._bridge_dir / "inbox"
        if not self._bridges and inbox_root.exists():
            self._bridges = sorted(
                d.name for d in inbox_root.iterdir() if d.is_dir()
            )
            logger.info("Auto-discovered bridges: %s", self._bridges)

        if not self._bridges:
            logger.warning("No bridges configured and none auto-discovered")
            return False

        # Build per-bridge configs
        extra = getattr(self.config, "extra", {}) or {}
        for name in self._bridges:
            self._bridge_configs[name] = BridgeConfig(name, extra)

        self._running = True
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._status_task = asyncio.create_task(self._status_loop())
        self._mark_connected()
        logger.info(
            "Bridge adapter connected — %d bridge(s), polling %s every %.1fs",
            len(self._bridges),
            self._bridge_dir,
            self._poll_interval,
        )
        return True

    async def disconnect(self) -> None:
        """Stop polling and cleanup tasks."""
        self._running = False
        for task in (self._poll_task, self._cleanup_task, self._status_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._poll_task = None
        self._cleanup_task = None
        self._status_task = None
        self._mark_disconnected()
        logger.info("Bridge adapter disconnected")

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

        # User authorization per bridge
        bc = self._bridge_configs.get(bridge)
        if bc and not bc.is_user_allowed(sender):
            logger.info("User '%s' not allowed on bridge '%s'", sender, bridge)
            try:
                filepath.unlink()
            except OSError:
                pass
            return

        # Build session source
        thread_id = data.get("thread_id") or data.get("thread_root")
        source = SessionSource(
            platform=Platform("bridge-adapter"),
            chat_id=chat_id,
            chat_name=chat_name or chat_id,
            chat_type=chat_type,
            user_id=sender,
            user_name=data.get("sender_name") or sender,
            thread_id=thread_id,
        )

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
            text=text,
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
        """Write an outgoing text message to the outbox."""
        if isinstance(content, list):
            text = " ".join(str(c) for c in content if c)
        else:
            text = str(content)

        bridge = self._resolve_bridge(chat_id)
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
        """Determine which bridge a chat_id belongs to."""
        if ":" in chat_id:
            prefix, _, rest = chat_id.partition(":")
            if prefix in self._bridges:
                return prefix
        return self._bridges[0] if self._bridges else "default"

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
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="🔌",
    )
