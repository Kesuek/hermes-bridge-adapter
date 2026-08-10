"""Tests for Unified Threads (T-058).

Virtual thread mapping, ``/unified`` commands, and multicast broadcast.
Runs against the ``adapter`` module directly, mirroring ``test_registry.py``.
"""
import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import adapter  # noqa: E402
from adapter import BridgeAdapter  # noqa: E402


@pytest.fixture(autouse=True)
def _register_platform():
    """Register ``bridge-adapter`` so Platform("bridge-adapter") resolves."""
    from gateway.platform_registry import platform_registry
    from adapter import register

    class _Ctx:
        def __init__(self):
            self.names = []

        def register_platform(self, *, name, **kwargs):
            from gateway.platform_registry import PlatformEntry

            entry = PlatformEntry(
                name=name,
                adapter_factory=kwargs.get("adapter_factory"),
                check_fn=kwargs.get("check_fn"),
                **{
                    k: v for k, v in kwargs.items()
                    if k not in ("adapter_factory", "check_fn")
                },
            )
            platform_registry.register(entry)
            self.names.append(name)

    if not platform_registry.is_registered("bridge-adapter"):
        register(_Ctx())
    yield


def _make_adapter(tmp_path: Path) -> BridgeAdapter:
    """Build a BridgeAdapter wired to a temporary bridge_dir."""
    from gateway.config import PlatformConfig

    bridge_dir = tmp_path / "bridge"
    bridge_dir.mkdir()
    cfg = PlatformConfig(enabled=True, extra={"bridge_dir": str(bridge_dir)})
    return BridgeAdapter(cfg)


# ── Task 1: persistence roundtrip ────────────────────────────────────


def test_unified_threads_persist_roundtrip(tmp_path):
    a = _make_adapter(tmp_path)
    a._unified_threads = {
        "projekt": {
            "name": "projekt",
            "created_at": "2026-08-10T10:00:00+02:00",
            "created_by": "ronny",
            "members": {},
            "aliases": [],
            "mode": "participant",
        }
    }
    a._save_unified_threads()
    a._unified_threads = {}
    a._load_unified_threads()
    assert "projekt" in a._unified_threads
    assert a._unified_threads["projekt"]["mode"] == "participant"


def test_unified_load_missing_file_is_empty(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    assert a._unified_threads == {}


def test_unified_load_bad_json_is_empty(tmp_path, caplog):
    a = _make_adapter(tmp_path)
    p = a._bridge_dir / "unified_threads.json"
    p.write_text("{not valid json", encoding="utf-8")
    a._load_unified_threads()
    assert a._unified_threads == {}


# ── Task 3: create / status / members / join / leave ─────────────────


def test_unified_create(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    assert "projekt" in a._unified_threads
    assert a._unified_threads["projekt"]["created_by"] == "ronny"
    assert a._unified_threads["projekt"]["mode"] == "participant"
    # creator is a member
    assert "imsg:u1" in a._unified_threads["projekt"]["members"]


def test_unified_create_already_exists(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    # second create should not overwrite
    a._cmd_unified_create("imsg", {"sender": "bob", "chat": {"id": "u2"}}, "projekt")
    assert a._unified_threads["projekt"]["created_by"] == "ronny"


def test_unified_join_adds_member(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, "projekt")
    members = a._unified_threads["projekt"]["members"]
    assert "talk:t1" in members
    assert members["talk:t1"]["user_id"] == "anja"


def test_unified_join_unknown_thread_is_noop(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, "nope")
    assert "nope" not in a._unified_threads


def test_unified_leave_removes_member(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, "projekt")
    a._cmd_unified_leave("talk", {"sender": "anja", "chat": {"id": "t1"}}, "projekt")
    assert "talk:t1" not in a._unified_threads["projekt"]["members"]


def test_unified_leave_unknown_thread_is_noop(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    # should not raise
    a._cmd_unified_leave("talk", {"sender": "anja", "chat": {"id": "t1"}}, "nope")


def test_unified_status_lists_threads(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    text = a._cmd_unified_status("imsg", {"sender": "ronny", "chat": {"id": "u1"}})
    assert "projekt" in text


def test_unified_members_lists_member_user_ids(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, "projekt")
    text = a._cmd_unified_members(
        "imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt"
    )
    assert "ronny" in text
    assert "anja" in text


# ── Task 4: inbound virtual thread mapping ──────────────────────────


def test_unified_inbound_maps_to_virtual_thread(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a.handle_message = AsyncMock()

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "text": "Hallo",
                "chat": {"id": "u1", "type": "direct"},
            },
            tmp_path / "x.json",
        )

    asyncio.run(run())
    event = a.handle_message.await_args[0][0]
    assert event.source.chat_type == "thread"
    assert event.source.chat_id == "unified"
    assert event.source.thread_id == "projekt"
    assert "unified thread 'projekt'" in event.text


def test_unified_inbound_non_member_not_mapped(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a.handle_message = AsyncMock()

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "carla",
                "text": "Hallo",
                "chat": {"id": "carla-id", "type": "direct"},
            },
            tmp_path / "x.json",
        )

    asyncio.run(run())
    event = a.handle_message.await_args[0][0]
    # carla is not a member of "projekt" → normal routing, not unified
    assert event.source.chat_type != "thread" or event.source.thread_id != "projekt"
    assert event.source.chat_id != "unified"


# ── Task 5: multicast broadcast in send() ────────────────────────────


def test_send_unified_multicasts_to_all_members(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, "projekt")
    result = asyncio.run(a.send("unified~projekt", "Hallo alle"))
    assert result.success
    # outbox files for both member bridges
    assert (a._bridge_dir / "outbox" / "imsg").exists()
    assert (a._bridge_dir / "outbox" / "talk").exists()


def test_send_unified_unknown_thread_fails(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    result = asyncio.run(a.send("unified~nope", "Hallo"))
    assert not result.success
    assert result.error_kind == "routing"


def test_send_unified_multicast_content(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, "projekt")
    asyncio.run(a.send("unified~projekt", "Hallo alle"))

    # inspect the outbox files
    imsg_files = list((a._bridge_dir / "outbox" / "imsg").glob("*.json"))
    talk_files = list((a._bridge_dir / "outbox" / "talk").glob("*.json"))
    assert len(imsg_files) == 1
    assert len(talk_files) == 1

    imsg_data = json.loads(imsg_files[0].read_text("utf-8"))
    talk_data = json.loads(talk_files[0].read_text("utf-8"))
    # Each outbox target is the member's own routable address.
    assert imsg_data["target"] == "imsg~u1"
    assert talk_data["target"] == "talk~t1"
    assert imsg_data["text"] == "Hallo alle"
    assert talk_data["text"] == "Hallo alle"


# ── Task 2: /unified command parser ──────────────────────────────────


def test_unified_command_help(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a.handle_message = AsyncMock()
    a._send_reply = AsyncMock()

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "text": "/unified help",
                "chat": {"id": "u1", "type": "direct"},
            },
            tmp_path / "x.json",
        )

    asyncio.run(run())
    a.handle_message.assert_not_awaited()  # command, not a normal message
    a._send_reply.assert_awaited_once()


def test_unified_command_create_via_process_incoming(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._send_reply = AsyncMock()
    a.handle_message = AsyncMock()

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "text": "/unified create projekt",
                "chat": {"id": "u1", "type": "direct"},
            },
            tmp_path / "x.json",
        )

    asyncio.run(run())
    a.handle_message.assert_not_awaited()
    a._load_unified_threads()
    assert "projekt" in a._unified_threads


def test_unified_command_not_triggered_for_normal_message(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a.handle_message = AsyncMock()
    a._send_reply = AsyncMock()

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "text": "please help me with /unified",  # not a prefix
                "chat": {"id": "u1", "type": "direct"},
            },
            tmp_path / "x.json",
        )

    asyncio.run(run())
    a._send_reply.assert_not_awaited()
    a.handle_message.assert_awaited_once()


# ── Task 6: mode field + help text ───────────────────────────────────


def test_unified_help_lists_commands(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    text = a._unified_help_text()
    assert "/unified create" in text
    assert "/unified join" in text
    assert "/unified mode" in text
    assert "participant" in text


def test_unified_mode_sets_field(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_mode("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt", "silent")
    assert a._unified_threads["projekt"]["mode"] == "silent"


def test_unified_mode_rejects_invalid_value(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_mode(
        "imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt", "bogus"
    )
    # invalid value → mode unchanged (stays default participant)
    assert a._unified_threads["projekt"]["mode"] == "participant"


# ── T-059 Task 1: leader marking in routing context ───────────────────


def test_unified_routing_marks_leader(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a.handle_message = AsyncMock()

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "text": "Hallo",
                "chat": {"id": "u1", "type": "direct"},
            },
            tmp_path / "x.json",
        )

    asyncio.run(run())
    event = a.handle_message.await_args[0][0]
    assert "[Ronny Leader]" in event.text


def test_unified_routing_no_leader_marker_for_non_leader(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, "projekt")
    a.handle_message = AsyncMock()

    async def run():
        await a._process_incoming(
            "talk",
            {
                "sender": "anja",
                "text": "Hi",
                "chat": {"id": "t1", "type": "direct"},
            },
            tmp_path / "x.json",
        )

    asyncio.run(run())
    event = a.handle_message.await_args[0][0]
    assert "Leader]" not in event.text


# ── T-059 Task 2: reactive mode (mention gating) ──────────────────────


def test_unified_reactive_drops_unmentioned(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_mode("imsg", {"sender": "ronny"}, "projekt", "reactive")
    a.handle_message = AsyncMock()
    inbox_file = tmp_path / "bridge" / "inbox" / "imsg" / "m.json"
    inbox_file.parent.mkdir(parents=True, exist_ok=True)
    inbox_file.write_text("{}", encoding="utf-8")

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "text": "kein mention",
                "chat": {"id": "u1", "type": "direct"},
            },
            inbox_file,
        )

    asyncio.run(run())
    a.handle_message.assert_not_awaited()
    assert not inbox_file.exists()  # dropped + file deleted


def test_unified_reactive_passes_mentioned(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_mode("imsg", {"sender": "ronny"}, "projekt", "reactive")
    a.handle_message = AsyncMock()

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "text": "@hermes bitte antworten",
                "chat": {"id": "u1", "type": "direct"},
            },
            tmp_path / "x.json",
        )

    asyncio.run(run())
    a.handle_message.assert_awaited_once()


# ── T-059 Task 3: silent mode (listener) ──────────────────────────────


def test_unified_silent_drops_all(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_mode("imsg", {"sender": "ronny"}, "projekt", "silent")
    a.handle_message = AsyncMock()
    inbox_file = tmp_path / "bridge" / "inbox" / "imsg" / "m.json"
    inbox_file.parent.mkdir(parents=True, exist_ok=True)
    inbox_file.write_text("{}", encoding="utf-8")

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "text": "egal was",
                "chat": {"id": "u1", "type": "direct"},
            },
            inbox_file,
        )

    asyncio.run(run())
    a.handle_message.assert_not_awaited()
    assert not inbox_file.exists()


def test_unified_silent_drops_even_when_mentioned(tmp_path):
    """silent drops messages regardless of mentions — listener never replies."""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_mode("imsg", {"sender": "ronny"}, "projekt", "silent")
    a.handle_message = AsyncMock()
    inbox_file = tmp_path / "bridge" / "inbox" / "imsg" / "m.json"
    inbox_file.parent.mkdir(parents=True, exist_ok=True)
    inbox_file.write_text("{}", encoding="utf-8")

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "text": "@hermes du musst antworten!",
                "chat": {"id": "u1", "type": "direct"},
            },
            inbox_file,
        )

    asyncio.run(run())
    a.handle_message.assert_not_awaited()
    assert not inbox_file.exists()