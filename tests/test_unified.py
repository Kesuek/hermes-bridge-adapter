"""Tests for Unified Threads (T-058).

Virtual thread mapping, ``/unified`` commands, and multicast broadcast.
Runs against the ``adapter`` module directly, mirroring ``test_registry.py``.
"""
import asyncio
import json
import sys
import time
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
    # The session chat_id MUST be the routable address "unified~<name>",
    # NOT the bare "unified" slot. When the agent replies through the
    # session, the gateway sends to this chat_id; "unified" has no "~"
    # prefix and fails routing, while "unified~projekt" triggers the
    # multicast branch in send(). (Regression: caught live 2026-08-10 —
    # agent reply to a unified-thread message failed with "bridge prefix
    # unknown" because the session chat_id was "unified".)
    assert event.source.chat_id == "unified~projekt"
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


# ── T-059 Task 3: off mode (drop) + silent mode (mute/digest) ─────────


def test_unified_off_drops_all(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_mode("imsg", {"sender": "ronny"}, "projekt", "off")
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


def test_unified_silent_buffers_then_dispatches_digest(tmp_path):
    """silent (mute) collects messages into the digest buffer and flushes
    them as one bundled turn so the agent reads along but never replies."""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_mode("imsg", {"sender": "ronny"}, "projekt", "silent")
    a.handle_message = AsyncMock()

    # First message: buffered (not dispatched), file deleted.
    inbox_file = tmp_path / "bridge" / "inbox" / "imsg" / "m1.json"
    inbox_file.parent.mkdir(parents=True, exist_ok=True)
    inbox_file.write_text("{}", encoding="utf-8")

    async def run1():
        await a._process_incoming(
            "imsg",
            {"sender": "ronny", "text": "nachricht 1",
             "chat": {"id": "u1", "type": "direct"}},
            inbox_file,
        )

    asyncio.run(run1())
    a.handle_message.assert_not_awaited()
    assert not inbox_file.exists()
    # Message is in the digest buffer.
    st = a._unified_threads["projekt"]["_adaptive"]
    assert any(m["text"] == "nachricht 1" for m in st["buffer"])

    # Force the digest window to elapse, then a new message triggers the flush.
    st["digest_until"] = time.time() - 1
    inbox_file2 = tmp_path / "bridge" / "inbox" / "imsg" / "m2.json"
    inbox_file2.write_text("{}", encoding="utf-8")

    async def run2():
        await a._process_incoming(
            "imsg",
            {"sender": "ronny", "text": "nachricht 2",
             "chat": {"id": "u1", "type": "direct"}},
            inbox_file2,
        )

    asyncio.run(run2())
    a.handle_message.assert_awaited_once()
    event = a.handle_message.await_args[0][0]
    assert "[System:" in event.text
    assert "nachricht 1" in event.text
    assert "nachricht 2" in event.text
    assert "do not reply" in event.text


def test_unified_silent_buffers_even_when_mentioned(tmp_path):
    """silent (mute) buffers messages regardless of mentions — the agent
    reads along via digest but never replies to a direct mention."""
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
    # Not dispatched immediately — buffered for the digest.
    a.handle_message.assert_not_awaited()
    assert not inbox_file.exists()
    # The mention message is in the digest buffer (agent reads along).
    st = a._unified_threads["projekt"]["_adaptive"]
    assert any("du musst antworten" in m["text"] for m in st["buffer"])


# ── T-059 Task 4: participant mode NO_REPLY teaching ─────────────────


def test_platform_hint_teaches_participant_no_reply():
    hints = {}

    class _Ctx:
        def register_platform(self, *, name, **kwargs):
            hints["hint"] = kwargs.get("platform_hint", "")

    from adapter import register

    register(_Ctx())
    assert "NO_REPLY" in hints["hint"]
    assert "participant" in hints["hint"]


# ── T-059 Task 5: protokoll mode lifecycle (open/close) ──────────────


def test_protokoll_open_leader_only(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, "projekt")
    # Nicht-Leader darf nicht öffnen
    r = a._cmd_unified_protokoll_open("talk", {"sender": "anja"}, "projekt")
    assert "only" in r.lower() or "leader" in r.lower()
    assert a._unified_threads["projekt"].get("protokoll") is None
    # Leader darf öffnen
    r2 = a._cmd_unified_protokoll_open("imsg", {"sender": "ronny"}, "projekt")
    assert "open" in r2.lower() or "started" in r2.lower()
    assert a._unified_threads["projekt"].get("protokoll") is not None


def test_protokoll_open_explicit_name(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    r = a._cmd_unified_protokoll_open(
        "imsg", {"sender": "ronny"}, "projekt", "sitzung-2026-08-10"
    )
    assert "sitzung-2026-08-10" in r
    prot = a._unified_threads["projekt"]["protokoll"]
    assert prot["name"] == "sitzung-2026-08-10"
    assert prot["messages"] == []


def test_protokoll_open_unknown_thread(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    r = a._cmd_unified_protokoll_open("imsg", {"sender": "ronny"}, "nope")
    assert "not found" in r.lower()


def test_protokoll_close_leader_only(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, "projekt")
    a._cmd_unified_protokoll_open("imsg", {"sender": "ronny"}, "projekt")
    # Nicht-Leader darf nicht schliessen
    r = a._cmd_unified_protokoll_close("talk", {"sender": "anja"}, "projekt")
    assert "only" in r.lower() or "leader" in r.lower()
    assert a._unified_threads["projekt"].get("protokoll") is not None


def test_protokoll_close_creates_artifact(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_protokoll_open("imsg", {"sender": "ronny"}, "projekt", "sitzung")
    r = a._cmd_unified_protokoll_close("imsg", {"sender": "ronny"}, "projekt")
    assert "closed" in r.lower() or "artifact" in r.lower()
    # Artefakt-Datei existiert
    assert (a._bridge_dir / "protokoll" / "projekt" / "sitzung.md").exists()
    # protokoll-State nach close gelöscht
    assert a._unified_threads["projekt"].get("protokoll") is None


def test_protokoll_close_without_open(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    r = a._cmd_unified_protokoll_close("imsg", {"sender": "ronny"}, "projekt")
    assert "no" in r.lower() or "not" in r.lower() or "open" in r.lower()


def test_protokoll_command_open_via_process_incoming(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._send_reply = AsyncMock()
    a.handle_message = AsyncMock()

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "text": "/unified protokoll open projekt sitzung",
                "chat": {"id": "u1", "type": "direct"},
            },
            tmp_path / "x.json",
        )

    asyncio.run(run())
    a.handle_message.assert_not_awaited()
    a._send_reply.assert_awaited_once()
    a._load_unified_threads()
    assert a._unified_threads["projekt"]["protokoll"]["name"] == "sitzung"


# ── T-059 Task 6: protokoll mode collects messages ───────────────────


def test_protokoll_collects_messages(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_protokoll_open("imsg", {"sender": "ronny"}, "projekt")
    a.handle_message = AsyncMock()

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "text": "Punkt 1 besprochen",
                "chat": {"id": "u1", "type": "direct"},
            },
            tmp_path / "x.json",
        )

    asyncio.run(run())
    # Nachricht wurde gesammelt, nicht dispatched
    a.handle_message.assert_not_awaited()
    msgs = a._unified_threads["projekt"]["protokoll"]["messages"]
    assert any("Punkt 1" in m.get("text", "") for m in msgs)


def test_protokoll_collects_multiple_and_persists(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_protokoll_open("imsg", {"sender": "ronny"}, "projekt", "sitzung")
    a.handle_message = AsyncMock()

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "sender_name": "Ronny",
                "text": "Punkt 1",
                "chat": {"id": "u1", "type": "direct"},
            },
            tmp_path / "a.json",
        )
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "sender_name": "Ronny",
                "text": "Punkt 2",
                "chat": {"id": "u1", "type": "direct"},
            },
            tmp_path / "b.json",
        )

    asyncio.run(run())
    a.handle_message.assert_not_awaited()
    msgs = a._unified_threads["projekt"]["protokoll"]["messages"]
    assert len(msgs) == 2
    assert msgs[0]["text"] == "Punkt 1"
    assert msgs[1]["text"] == "Punkt 2"
    assert msgs[0]["sender_name"] == "Ronny"
    # persisted to disk
    a._load_unified_threads()
    msgs2 = a._unified_threads["projekt"]["protokoll"]["messages"]
    assert len(msgs2) == 2


def test_protokoll_close_artifact_contains_collected(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_protokoll_open("imsg", {"sender": "ronny"}, "projekt", "sitzung")
    a.handle_message = AsyncMock()

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "ronny",
                "sender_name": "Ronny",
                "text": "Punkt 1 besprochen",
                "chat": {"id": "u1", "type": "direct"},
            },
            tmp_path / "a.json",
        )

    asyncio.run(run())
    a._cmd_unified_protokoll_close("imsg", {"sender": "ronny"}, "projekt")
    artifact = a._bridge_dir / "protokoll" / "projekt" / "sitzung.md"
    assert artifact.exists()
    content = artifact.read_text("utf-8")
    assert "Punkt 1 besprochen" in content
    assert "Ronny" in content


def test_protokoll_not_dispatched_without_open_session(tmp_path):
    """If mode=protokoll but no session is open, fall through to participant."""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    # set mode to protokoll manually without opening a session
    a._unified_threads["projekt"]["mode"] = "protokoll"
    a._save_unified_threads()
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
    # no session open → fall through to normal dispatch
    a.handle_message.assert_awaited_once()


def test_protokoll_command_help_lists_lifecycle(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    text = a._unified_help_text()
    assert "protokoll" in text.lower()
    assert "open" in text
    assert "close" in text


# ── T-060: Reply-To-Ketten über Bridges ────────────────────────────────


def test_reply_map_persist_roundtrip(tmp_path):
    a = _make_adapter(tmp_path)
    a._reply_map = {"gw_1": {"bridge": "imsg", "local_msg_id": "msg_abc"}}
    a._save_reply_map()
    a._reply_map = {}
    a._load_reply_map()
    assert a._reply_map["gw_1"]["local_msg_id"] == "msg_abc"


def test_reply_map_registers_inbound(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_reply_map()
    a.handle_message = AsyncMock()

    async def run():
        await a._process_incoming("imsg", {
            "sender": "ronny", "text": "Hallo", "id": "msg_abc",
            "chat": {"id": "u1", "type": "direct"},
        }, tmp_path / "x.json")

    asyncio.run(run())
    # handle_message was called with a MessageEvent whose message_id is the
    # gateway_msg_id; the map must contain the mapping.
    assert any(v.get("local_msg_id") == "msg_abc" for v in a._reply_map.values())


def test_reply_to_resolves_across_bridges(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_reply_map()
    a._reply_map["gw_1"] = {"bridge": "imsg", "local_msg_id": "msg_abc"}
    a._save_reply_map()
    # Register the talk bridge so send() can route to talk~room1
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "talk.yaml").write_text(
        "name: talk\ntarget_format: [chat_id]\n", encoding="utf-8"
    )
    a._reconcile_registry_sync()
    # send to a different bridge member with reply_to=gw_1
    result = asyncio.run(a.send("talk~room1", "Antwort", reply_to="gw_1"))
    assert result.success
    # the talk outbox must have reply_to=msg_abc (resolved)
    files = list((a._bridge_dir / "outbox" / "talk").glob("*.json"))
    assert files
    data = json.loads(files[0].read_text("utf-8"))
    assert data["reply_to"] == "msg_abc"


# ── T-061: Adaptive Zustandsmaschine ───────────────────────────────────


def test_adaptive_state_transitions(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    # idle → active on first message
    a._adaptive_note_message("projekt", "ronny", "hi")
    assert a._unified_threads["projekt"].get("_adaptive", {}).get("state") == "active"


def test_adaptive_buffers_in_digest_mode(tmp_path):
    from unittest.mock import AsyncMock
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a.handle_message = AsyncMock()
    # 5 fast messages → digesting
    for i in range(5):
        a._adaptive_note_message("projekt", "ronny", f"msg {i}")
    assert a._unified_threads["projekt"]["_adaptive"]["state"] == "digesting"
    # next message is buffered, not dispatched
    async def run():
        await a._process_incoming("imsg", {
            "sender": "ronny", "text": "msg 5",
            "chat": {"id": "u1", "type": "direct"},
        }, tmp_path / "x.json")
    asyncio.run(run())
    a.handle_message.assert_not_awaited()


def test_adaptive_flush_dispatches_bundle(tmp_path):
    from unittest.mock import AsyncMock
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a.handle_message = AsyncMock()
    for i in range(5):
        a._adaptive_note_message("projekt", "ronny", f"msg {i}")
    # set digest_until into the past → flush is due
    a._unified_threads["projekt"]["_adaptive"]["digest_until"] = time.time() - 1
    async def run():
        await a._process_incoming("imsg", {
            "sender": "ronny", "text": "msg 5",
            "chat": {"id": "u1", "type": "direct"},
        }, tmp_path / "x.json")
    asyncio.run(run())
    a.handle_message.assert_awaited_once()
    event = a.handle_message.await_args[0][0]
    assert "[System:" in event.text  # bundle header


# ── T-062: Member-Deduplizierung ──────────────────────────────────────


def test_identity_map_resolves_alias(tmp_path):
    a = _make_adapter(tmp_path)
    a._identity_map = {
        "ronny": ["ronny.pietschke@icloud.com", "+491714824968", "ronny"],
    }
    assert a._resolve_identity("ronny.pietschke@icloud.com") == "ronny"
    assert a._resolve_identity("ronny") == "ronny"
    assert a._resolve_identity("anja") == "anja"  # unknown → itself


def test_join_dedup_same_person_two_bridges(tmp_path):
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._identity_map = {"ronny": ["ronny.pietschke@icloud.com", "ronny"]}
    a._cmd_unified_create(
        "imsg",
        {"sender": "ronny.pietschke@icloud.com", "chat": {"id": "u1"}},
        "projekt",
    )
    # same person joins from talk
    a._cmd_unified_join("talk", {"sender": "ronny", "chat": {"id": "t1"}}, "projekt")
    members = a._unified_threads["projekt"]["members"]
    # both bridges under ONE member (canonical identity)
    assert len(members) == 1
    # but both bridge:chat_id addresses are present
    assert "imsg:u1" in members or "talk:t1" in members


# ── T-063: Message-Relay ──────────────────────────────────────────────


def test_identity_map_with_wrapper(tmp_path):
    """T-063 Task 1: (wrapper, user_id) mapping prevents cross-bridge merges."""
    a = _make_adapter(tmp_path)
    a._identity_map = {
        "ronny": {
            "aliases": ["ronny.pietschke@icloud.com", "+491714824968", "ronny"],
            "wrappers": {"imsg": "ronny.pietschke@icloud.com", "talk": "ronny"},
        }
    }
    # (wrapper, user_id) match
    assert a._resolve_identity("imsg", "ronny.pietschke@icloud.com") == "ronny"
    assert a._resolve_identity("talk", "ronny") == "ronny"
    # gleicher Alias auf anderem Wrapper → alias match (kein dedup mismatch)
    assert a._resolve_identity("matrix", "ronny") == "ronny"
    # unbekannt → selbst
    assert a._resolve_identity("imsg", "anja") == "anja"
    # legacy 1-arg form still works
    assert a._resolve_identity("ronny") == "ronny"


def test_relay_to_other_members(tmp_path):
    """T-063 Task 2: relay writes to the outbox of the other member bridges."""
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, "projekt")
    # Nachricht von imsg → relay an talk (nicht an imsg)
    asyncio.run(a._relay_to_other_members("projekt", "imsg", "Ronny", "Hallo alle"))
    # talk-Outbox hat die Relay-Kopie
    talk_files = list((a._bridge_dir / "outbox" / "talk").glob("*.json"))
    assert talk_files, "relay should write to talk outbox"
    data = json.loads(talk_files[0].read_text("utf-8"))
    assert "[Ronny]" in data["text"]
    assert "Hallo alle" in data["text"]
    # imsg-Outbox ist leer (nicht an Ursprungs-Bridge)
    imsg_files = list((a._bridge_dir / "outbox" / "imsg").glob("*.json"))
    assert not imsg_files, "relay must not write back to source bridge"


def test_inbound_relays_to_other_members(tmp_path):
    """T-063 Task 3: _process_incoming mirrors to other members AND dispatches."""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, "projekt")
    a.handle_message = AsyncMock()
    inbox = tmp_path / "x.json"

    async def run():
        await a._process_incoming("imsg", {
            "sender": "ronny", "sender_name": "Ronny", "text": "Hallo",
            "chat": {"id": "u1", "type": "direct"},
        }, inbox)

    asyncio.run(run())
    # Agent bekommt die Nachricht (bleibt im Kontext)
    a.handle_message.assert_awaited_once()
    # talk-Outbox hat die Relay-Kopie
    talk_files = list((a._bridge_dir / "outbox" / "talk").glob("*.json"))
    assert talk_files, "relay should write to talk outbox"
    data = json.loads(talk_files[0].read_text("utf-8"))
    assert "[Ronny]" in data["text"]


def test_relay_dedup_same_person_two_bridges(tmp_path):
    """T-063 Task 4: relay dedups per (bridge, chat_id), not per person.

    A person who joined from two bridges still receives the message on each
    of their addresses (multicast); the dedup guarantees the SAME address
    is never written twice.
    """
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._identity_map = {"ronny": ["ronny.pietschke@icloud.com", "ronny"]}
    a._cmd_unified_create(
        "imsg",
        {"sender": "ronny.pietschke@icloud.com", "chat": {"id": "u1"}},
        "projekt",
    )
    # same person joins from talk → merges into the one member (T-062)
    a._cmd_unified_join("talk", {"sender": "ronny", "chat": {"id": "t1"}}, "projekt")
    asyncio.run(a._relay_to_other_members("projekt", "imsg", "Ronny", "Hallo"))
    # ronny is on talk (1 address) → exactly 1 talk outbox file
    talk_files = list((a._bridge_dir / "outbox" / "talk").glob("*.json"))
    assert len(talk_files) == 1, "relay must dedup same address, not same person"


def test_silent_flush_timer_dispatches_due_digest(tmp_path):
    """The silent-mode digest must flush on a timer, not only on the next
    inbound message. A single message followed by silence would otherwise
    sit in the buffer forever. (Bug caught live 2026-08-10.)"""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_mode("imsg", {"sender": "ronny"}, "projekt", "silent")
    a.handle_message = AsyncMock()

    # Buffer one message in silent mode (no dispatch, no flush yet).
    inbox_file = tmp_path / "bridge" / "inbox" / "imsg" / "m.json"
    inbox_file.parent.mkdir(parents=True, exist_ok=True)
    inbox_file.write_text("{}", encoding="utf-8")

    async def run():
        await a._process_incoming(
            "imsg",
            {"sender": "ronny", "text": "wichtige info",
             "chat": {"id": "u1", "type": "direct"}},
            inbox_file,
        )
    asyncio.run(run())
    a.handle_message.assert_not_awaited()
    st = a._unified_threads["projekt"]["_adaptive"]
    assert st["state"] == "digesting"
    assert any(m["text"] == "wichtige info" for m in st["buffer"])

    # Force the digest window to elapse, then run the flush loop.
    st["digest_until"] = time.time() - 1
    asyncio.run(a._flush_due_silent_digests())
    a.handle_message.assert_awaited_once()
    event = a.handle_message.await_args[0][0]
    assert "[System:" in event.text
    assert "wichtige info" in event.text
    assert "do not reply" in event.text
    # Buffer is cleared.
    assert a._unified_threads["projekt"]["_adaptive"]["buffer"] == []


# ── T-064: active_thread persistence + switch + send ─────────────────


def test_active_thread_persist_roundtrip(tmp_path):
    """T-064 Task 1: active_thread map survives save→load roundtrip."""
    a = _make_adapter(tmp_path)
    a._active_threads = {"ronny": "projekt"}
    a._save_active_threads()
    a._active_threads = {}
    a._load_active_threads()
    assert a._active_threads["ronny"] == "projekt"


def test_unified_switch_sets_active_thread(tmp_path):
    """T-064 Task 2: /unified switch sets the user's active thread.

    Switch is only allowed on threads the user is already a member of —
    the creator is a member by default, so this switches without join.
    """
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._load_active_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_switch("imsg", {"sender": "ronny"}, "projekt")
    assert a._active_threads["ronny"] == "projekt"


def test_inbound_maps_to_active_thread(tmp_path):
    """T-064 Task 3: a user with an active thread routes to it even when
    ``_find_unified_for_member`` finds no membership match for the source
    chat_id (the user is a member via a different bridge/address)."""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._load_active_threads()
    # ronny creates + joins from imsg:u1 — member key is imsg:u1.
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    # ronny switches active thread to projekt (allowed — member via imsg:u1).
    a._cmd_unified_switch("imsg", {"sender": "ronny"}, "projekt")
    a.handle_message = AsyncMock()

    # Now ronny sends from talk:t1 — NOT a member of projekt via that
    # address, so _find_unified_for_member returns None. The active_thread
    # fallback (T-064) must still route the message to unified~projekt.
    async def run():
        await a._process_incoming("talk", {
            "sender": "ronny", "text": "Hallo",
            "chat": {"id": "t1", "type": "direct"},
        }, tmp_path / "x.json")

    asyncio.run(run())
    event = a.handle_message.await_args[0][0]
    assert event.source.chat_id == "unified~projekt"
    assert event.source.thread_id == "projekt"


def test_unified_send_to_thread(tmp_path):
    """T-064 Task 4: /unified send <name> <message> multicasts to every
    member's outbox without switching the active thread."""
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, "projekt")
    result = asyncio.run(a._cmd_unified_send("imsg", {"sender": "ronny"}, "projekt", "Hallo alle"))
    assert "sent" in result.lower() or "delivered" in result.lower()
    # beide Member-Outboxen haben die Nachricht
    assert (a._bridge_dir / "outbox" / "imsg").exists()
    assert (a._bridge_dir / "outbox" / "talk").exists()


# ── T-065: Identity-Claim (Challenge-Response) ───────────────────────


def test_pending_claims_persist_roundtrip(tmp_path):
    """T-065 Task 1: pending_claims survives save→load roundtrip."""
    a = _make_adapter(tmp_path)
    a._pending_claims = {"abc123": {"code": "123456", "source": "imsg:ronny.pietschke@icloud.com",
                                     "target": "talk:ronny", "expires": 1786368000.0}}
    a._save_pending_claims()
    a._pending_claims = {}
    a._load_pending_claims()
    assert a._pending_claims["abc123"]["code"] == "123456"


def test_identity_claim_sends_code(tmp_path):
    """T-065 Task 2: /unified identity claim sends a code to the target bridge."""
    a = _make_adapter(tmp_path)
    a._bridges = ["imsg", "talk"]
    a._load_unified_threads()
    a._load_pending_claims()
    result = asyncio.run(a._cmd_unified_identity_claim("imsg", {"sender": "ronny.pietschke@icloud.com"}, "talk~ronny"))
    assert "code" in result.lower() or "sent" in result.lower()
    # Code wurde an talk-Outbox gesendet
    talk_files = list((a._bridge_dir / "outbox" / "talk").glob("*.json"))
    assert talk_files, "claim should send a code to the target bridge"
    data = json.loads(talk_files[0].read_text("utf-8"))
    assert "code" in data["text"].lower()
    # pending claim existiert
    assert a._pending_claims, "claim should create a pending entry"


def test_identity_confirm_merges(tmp_path):
    """T-065 Task 3: /unified identity confirm merges the two identities.

    A claim from imsg:ronny.pietschke@icloud.com targets talk~ronny. The
    confirm must come from talk:ronny; afterwards both addresses resolve
    to the same canonical person and the pending claim is cleared.
    """
    a = _make_adapter(tmp_path)
    a._bridges = ["imsg", "talk"]
    a._load_unified_threads()
    a._load_pending_claims()
    a._load_identity_map()
    # claim von imsg auf talk~ronny
    asyncio.run(a._cmd_unified_identity_claim("imsg", {"sender": "ronny.pietschke@icloud.com"}, "talk~ronny"))
    claim_id = next(iter(a._pending_claims))
    code = a._pending_claims[claim_id]["code"]
    # confirm von talk
    result = a._cmd_unified_identity_confirm("talk", {"sender": "ronny"}, code)
    assert "confirmed" in result.lower() or "merged" in result.lower()
    # identity_map hat die Zuordnung
    assert a._resolve_identity("imsg", "ronny.pietschke@icloud.com") == a._resolve_identity("talk", "ronny")
    # pending claim ist weg
    assert not a._pending_claims


def test_set_username(tmp_path):
    """T-065 Task 4: /unified set username <name> sets the display name."""
    a = _make_adapter(tmp_path)
    a._load_unified_threads()
    a._load_identity_map()
    a._cmd_unified_set_username("imsg", {"sender": "ronny.pietschke@icloud.com"}, "Ronny P.")
    person = a._resolve_identity("imsg", "ronny.pietschke@icloud.com")
    assert a._usernames.get(person) == "Ronny P."


def test_username_persist_roundtrip(tmp_path):
    """T-065 Task 4: usernames survive save→load roundtrip."""
    a = _make_adapter(tmp_path)
    a._usernames = {"ronny": "Ronny P."}
    a._save_usernames()
    a._usernames = {}
    a._load_usernames()
    assert a._usernames["ronny"] == "Ronny P."


def test_status_lists_merged_addresses_and_username(tmp_path):
    """T-065 Task 4: /unified status shows merged addresses + username."""
    a = _make_adapter(tmp_path)
    a._bridges = ["imsg", "talk"]
    a._load_unified_threads()
    a._load_identity_map()
    a._load_usernames()
    asyncio.run(a._cmd_unified_identity_claim("imsg", {"sender": "ronny.pietschke@icloud.com"}, "talk~ronny"))
    claim_id = next(iter(a._pending_claims))
    code = a._pending_claims[claim_id]["code"]
    a._cmd_unified_identity_confirm("talk", {"sender": "ronny"}, code)
    a._cmd_unified_set_username("imsg", {"sender": "ronny.pietschke@icloud.com"}, "Ronny P.")
    status = a._cmd_unified_status("imsg", {"sender": "ronny.pietschke@icloud.com"})
    assert "Ronny P." in status
    assert "ronny.pietschke@icloud.com" in status
    assert "ronny" in status


# ── T-066: Unified Handles ───────────────────────────────────────────


def test_resolve_unified_handle(tmp_path):
    """T-066 Task 1: _resolve_unified_handle returns unified~<username>
    when a display name is set, else falls back to unified~<user_id>."""
    a = _make_adapter(tmp_path)
    a._identity_map = {"ronny": {"wrappers": {"imsg": "ronny.pietschke@icloud.com"},
                                 "aliases": ["ronny.pietschke@icloud.com"]}}
    a._usernames = {"ronny": "Kesuek"}
    # Username gesetzt → unified~Kesuek
    assert a._resolve_unified_handle("imsg", "ronny.pietschke@icloud.com") == "unified~Kesuek"
    # Kein Username → Fallback auf rohes Handle
    assert a._resolve_unified_handle("imsg", "anja@example.com") == "unified~anja@example.com"