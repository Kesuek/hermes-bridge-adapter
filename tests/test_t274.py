"""Tests for T-274: mirror dedupe_key + deliver receipts.

P2 — every unified-thread mirror copy carries a stable ``metadata.dedupe_key``
(hash of the dispatched gateway_msg_id, propagated verbatim on later hops);
inbound messages whose dedupe_key is already in the persisted dedup ledger
(``dk~`` namespace in ``dedup_map.json``) are dropped: no turn, no new mirror.

P5 — every inbound message path returns (and persists) a deliver receipt:
``{"delivered_to_agent": bool, "drop_reason": str | None}``. Non-turn paths
(mode gates, silence guard, dedupe drops, ...) carry ``delivered_to_agent:
false`` plus a ``drop_reason``; a regular turn is ``true`` with no reason.
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
    bridge_dir.mkdir(exist_ok=True)
    cfg = PlatformConfig(enabled=True, extra={"bridge_dir": str(bridge_dir)})
    return BridgeAdapter(cfg)


def _mk_thread(tmp_path: Path, a: BridgeAdapter, name: str = "team") -> str:
    """Create a unified thread with 3 members on imsg/talk/a2a; returns name."""
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, name)
    a._cmd_unified_join("talk", {"sender": "anja", "chat": {"id": "t1"}}, name)
    a._cmd_unified_join("a2a", {"sender": "n1", "chat": {"id": "n1"}}, name)
    return name


def _proc(a: BridgeAdapter, bridge: str, data: dict, filepath: Path):
    """Run _process_incoming and return its receipt."""

    async def run():
        return await a._process_incoming(bridge, dict(data), filepath)

    return asyncio.run(run())


def _outbox(a: BridgeAdapter, bridge: str) -> list[dict]:
    d = a._bridge_dir / "outbox" / bridge
    if not d.exists():
        return []
    return [
        json.loads(p.read_text(encoding="utf-8"))
        for p in sorted(
            d.glob("*.json"), key=lambda p: p.stat().st_mtime,
        )
    ]


# ── P2: mirror copies carry a stable dedupe_key ──────────────────────


def test_mirror_copies_stamp_shared_dedupe_key(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    _mk_thread(tmp_path, a)
    a.handle_message = AsyncMock()

    receipt = _proc(a, "imsg", {
        "sender": "ronny", "text": "Hallo alle", "id": "m1",
        "chat": {"id": "u1", "type": "direct"},
    }, tmp_path / "m1.json")

    assert a.handle_message.await_count == 1
    talk = _outbox(a, "talk")
    a2a = _outbox(a, "a2a")
    assert len(talk) == 1 and len(a2a) == 1
    # Every mirror copy carries the SAME stable dedupe_key.
    kt = talk[0]["metadata"]["dedupe_key"]
    ka = a2a[0]["metadata"]["dedupe_key"]
    assert kt == ka
    # Key shape: bdk1- + 16 hex chars (hash of the message's gateway id).
    assert kt.startswith("bdk1-")
    assert len(kt) == len("bdk1-") + 16
    assert all(c in "0123456789abcdef" for c in kt[len("bdk1-"):])
    # The source bridge itself gets no relay copy.
    assert _outbox(a, "imsg") == []
    # The receipt already records the key of the message it mirrors.
    assert receipt["dedupe_key"] == kt


def test_untagged_send_multicast_has_no_dedupe_key(tmp_path):
    """Only relay mirrors are stamped — plain multicast replies stay clean."""
    a = _make_adapter(tmp_path)
    _mk_thread(tmp_path, a)
    asyncio.run(a.send("unified~team", "Antwort"))
    for bridge in ("imsg", "talk", "a2a"):
        for row in _outbox(a, bridge):
            assert "dedupe_key" not in row["metadata"]
            assert row["metadata"] == {}


def test_write_outbox_metadata_merge(tmp_path):
    a = _make_adapter(tmp_path)
    asyncio.run(a._write_outbox("imsg", "imsg~u1", text="x"))
    assert _outbox(a, "imsg")[0]["metadata"] == {}
    asyncio.run(a._write_outbox(
        "imsg", "imsg~u1", text="x", metadata={"dedupe_key": "bdk1-abcd"}
    ))
    assert _outbox(a, "imsg")[1]["metadata"] == {"dedupe_key": "bdk1-abcd"}


def test_seen_dedupe_key_dropped_no_turn_no_mirror(tmp_path):
    """Re-ingesting a copy with a seen dedupe_key: no turn, no new mirror."""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    _mk_thread(tmp_path, a)
    a.handle_message = AsyncMock()

    # hop 1: original keyed copy arrives via the a2a bridge
    key = "bdk1-" + "a" * 16
    _proc(a, "a2a", {
        "sender": "n1", "text": "Hallo", "id": "hop1",
        "metadata": {"dedupe_key": key},
        "chat": {"id": "n1", "type": "direct"},
    }, tmp_path / "hop1.json")
    assert a.handle_message.await_count == 1
    n_imsg = len(_outbox(a, "imsg"))
    n_talk = len(_outbox(a, "talk"))

    # hop 2: the same logical message loops back — new bridge, new local
    # id (the live P2 finding), but the same stable dedupe_key.
    receipt = _proc(a, "talk", {
        "sender": "anja", "text": "Hallo", "id": "hop2_entirely_new_id",
        "metadata": {"dedupe_key": key},
        "chat": {"id": "t1", "type": "direct"},
    }, tmp_path / "hop2.json")

    assert a.handle_message.await_count == 1, "no second turn for a seen key"
    assert len(_outbox(a, "imsg")) == n_imsg, "no new mirror from the drop"
    assert len(_outbox(a, "talk")) == n_talk
    assert receipt["delivered_to_agent"] is False
    assert receipt["drop_reason"] == "dedupe_key_seen"


def test_fresh_dedupe_key_is_recorded_and_propagated(tmp_path):
    """A first-sighting keyed message turns AND its mirrors carry the SAME
    key onward — the logical identity survives hop re-id-ing."""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    _mk_thread(tmp_path, a)
    a.handle_message = AsyncMock()

    key = "bdk1-" + "b" * 16
    _proc(a, "a2a", {
        "sender": "n1", "text": "Von Peer", "id": "hop1",
        "metadata": {"dedupe_key": key},
        "chat": {"id": "n1", "type": "direct"},
    }, tmp_path / "hop1.json")

    assert a.handle_message.await_count == 1  # first sighting → turn
    for bridge in ("imsg", "talk"):
        rows = _outbox(a, bridge)
        assert len(rows) == 1
        assert rows[0]["metadata"]["dedupe_key"] == key, "key must propagate"
    # Recorded in the persisted dedup ledger under the dk~ namespace.
    ledger = json.loads(
        (a._bridge_dir / "dedup_map.json").read_text(encoding="utf-8")
    )
    assert f"dk~{key}" in ledger


def test_dedupe_key_survives_restart(tmp_path):
    """The dk~ ledger lives in dedup_map.json — a restarted adapter drops
    a mirrored copy that loops back after the restart."""
    a1 = _make_adapter(tmp_path)
    a1._extra["allow_all"] = "true"
    _mk_thread(tmp_path, a1)
    a1.handle_message = AsyncMock()
    key = "bdk1-" + "c" * 16
    _proc(a1, "a2a", {
        "sender": "n1", "text": "Von Peer", "id": "hop1",
        "metadata": {"dedupe_key": key},
        "chat": {"id": "n1", "type": "direct"},
    }, tmp_path / "hop1.json")
    assert a1.handle_message.await_count == 1

    # Restart: fresh adapter instance over the SAME bridge_dir.
    before = {b: len(_outbox(a1, b)) for b in ("imsg", "talk")}
    a2 = _make_adapter(tmp_path)
    a2._extra["allow_all"] = "true"
    a2.handle_message = AsyncMock()
    a2._load_dedup_map()  # connect() loads the ledger in production
    receipt = _proc(a2, "talk", {
        "sender": "anja", "text": "Von Peer", "id": "hop2_new_id",
        "metadata": {"dedupe_key": key},
        "chat": {"id": "t1", "type": "direct"},
    }, tmp_path / "hop2.json")

    assert a2.handle_message.await_count == 0, "seen key drops after restart"
    assert receipt["drop_reason"] == "dedupe_key_seen"
    # No post-restart mirror either — outbox counts unchanged.
    assert len(_outbox(a1, "imsg")) == before["imsg"]
    assert len(_outbox(a1, "talk")) == before["talk"]


def test_seen_key_suppresses_even_command_shaped_text(tmp_path):
    """The ledger check runs BEFORE /unified command parsing — a looping
    mirror copy can never trigger commands (join/create/mode side effects)."""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    _mk_thread(tmp_path, a)
    a.handle_message = AsyncMock()
    a._send_reply = AsyncMock()

    key = "bdk1-" + "d" * 16
    _proc(a, "a2a", {
        "sender": "n1", "text": "Hallo", "id": "h1",
        "metadata": {"dedupe_key": key},
        "chat": {"id": "n1", "type": "direct"},
    }, tmp_path / "h1.json")

    _proc(a, "talk", {
        "sender": "anja", "text": "/u m team off", "id": "h2",
        "metadata": {"dedupe_key": key},
        "chat": {"id": "t1", "type": "direct"},
    }, tmp_path / "h2.json")

    a._send_reply.assert_not_awaited()
    assert a._unified_threads["team"]["mode"] == "participant"


# ── P5: deliver receipts — the drop_reason matrix ─────────────────────


def _receipt_path(a: BridgeAdapter, bridge: str) -> Path:
    return a._bridge_dir / "delivery_receipts" / f"{bridge}.jsonl"


def _last_receipt(a: BridgeAdapter, bridge: str) -> dict:
    lines = _receipt_path(a, bridge).read_text(
        encoding="utf-8").strip().splitlines()
    return json.loads(lines[-1])


@pytest.mark.parametrize(
    "mode, reason",
    [
        ("reactive", "mode_reactive_no_mention"),
        ("off", "mode_off"),
        ("silent", "silent_buffered"),
    ],
)
def test_mode_gate_receipts(tmp_path, mode, reason):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    name = _mk_thread(tmp_path, a)
    a._cmd_unified_mode("imsg", {"sender": "ronny"}, name, mode)
    a.handle_message = AsyncMock()

    receipt = _proc(a, "imsg", {
        "sender": "ronny", "text": "kein mention", "id": "m1",
        "chat": {"id": "u1", "type": "direct"},
    }, tmp_path / "m1.json")

    assert a.handle_message.await_count == 0
    assert receipt["delivered_to_agent"] is False
    assert receipt["drop_reason"] == reason
    # The relay still fired BEFORE the gate (humans always see it) — receipts
    # only describe the agent side.
    last = _last_receipt(a, "imsg")
    assert last["drop_reason"] == reason
    assert last["delivered_to_agent"] is False
    assert last["message_id"] == "m1"


def test_regular_turn_receipt(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    _mk_thread(tmp_path, a)
    a.handle_message = AsyncMock()

    receipt = _proc(a, "imsg", {
        "sender": "ronny", "text": "@hermes bitte antworten", "id": "ok1",
        "chat": {"id": "u1", "type": "direct"},
    }, tmp_path / "ok1.json")

    assert a.handle_message.await_count == 1
    assert receipt == {
        "ts": receipt["ts"],
        "bridge": "imsg",
        "file": "ok1.json",
        "message_id": "ok1",
        "dedupe_key": receipt["dedupe_key"],
        "delivered_to_agent": True,
        "drop_reason": None,
    }
    assert receipt["dedupe_key"].startswith("bdk1-")
    last = _last_receipt(a, "imsg")
    assert last["delivered_to_agent"] is True
    assert last["drop_reason"] is None


def test_silent_flush_bundle_receipt(tmp_path):
    """A message that lands inside a flushed silent digest DID reach the
    agent (read-only turn) → delivered true."""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    name = _mk_thread(tmp_path, a)
    a._cmd_unified_mode("imsg", {"sender": "ronny"}, name, "silent")
    a.handle_message = AsyncMock()

    r1 = _proc(a, "imsg", {
        "sender": "ronny", "text": "eins", "id": "s1",
        "chat": {"id": "u1", "type": "direct"},
    }, tmp_path / "s1.json")
    assert r1["drop_reason"] == "silent_buffered"

    # Expire the digest window so the next message triggers the flush.
    st = a._adaptive_state(name)
    st["digest_until"] = time.time() - 1
    r2 = _proc(a, "imsg", {
        "sender": "ronny", "text": "zwei", "id": "s2",
        "chat": {"id": "u1", "type": "direct"},
    }, tmp_path / "s2.json")

    assert a.handle_message.await_count == 1  # the bundle turn
    assert r2["delivered_to_agent"] is True
    assert r2["drop_reason"] is None


def test_adaptive_digest_buffer_receipt(tmp_path):
    """participant mode + open digesting window → buffered, no turn."""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    name = _mk_thread(tmp_path, a)
    a.handle_message = AsyncMock()
    # Force an open digesting window.
    st = a._adaptive_state(name)
    st["state"] = "digesting"
    st["digest_until"] = time.time() + 60
    st["cooldown_until"] = 0.0

    receipt = _proc(a, "imsg", {
        "sender": "ronny", "text": "hi", "id": "a1",
        "chat": {"id": "u1", "type": "direct"},
    }, tmp_path / "a1.json")

    assert a.handle_message.await_count == 0
    assert receipt["delivered_to_agent"] is False
    assert receipt["drop_reason"] == "adaptive_digest_buffered"


def test_t100_duplicate_receipt(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a.handle_message = AsyncMock()
    msg = {
        "sender": "ronny", "text": "Hallo", "id": "dup1",
        "chat": {"id": "u1", "type": "direct"},
    }
    _proc(a, "imsg", msg, tmp_path / "d1.json")
    receipt = _proc(a, "imsg", msg, tmp_path / "d2.json")
    assert a.handle_message.await_count == 1
    assert receipt["delivered_to_agent"] is False
    assert receipt["drop_reason"] == "duplicate_id"


def test_group_no_mention_receipt(tmp_path):
    """Group chat WITHOUT a mention: adapter-level drop, no turn."""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a.handle_message = AsyncMock()

    receipt = _proc(a, "imsg", {
        "sender": "ronny", "text": "blabla", "id": "g1",
        "chat": {"id": "g-c", "type": "group", "name": "Leute"},
    }, tmp_path / "g1.json")

    assert a.handle_message.await_count == 0
    assert receipt["delivered_to_agent"] is False
    assert receipt["drop_reason"] == "group_no_mention"


def test_a2a_digest_buffer_and_bundle_receipts(tmp_path, monkeypatch):
    """a2a digest buffering (no turn) and the flush (one bundle turn)."""
    monkeypatch.setenv("A2A_DIGEST_BRIDGES", "a2a")
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a.handle_message = AsyncMock()

    # Frame the first message as mid-series → buffered, no turn.
    msg = {
        "sender": "T7B9P4FM", "text": "von peer", "id": "p1",
        "chat": {"id": "T7B9P4FM", "type": "direct"},
    }
    st = a._a2a_digest_state("T7B9P4FM")
    st["last_inbound_ts"] = time.time()
    r1 = _proc(a, "a2a", msg, tmp_path / "p1.json")
    assert r1["delivered_to_agent"] is False
    assert r1["drop_reason"] == "a2a_digest_buffered"

    # Expire the window; the next message triggers the bundle flush.
    st2 = a._a2a_digest_state("T7B9P4FM")
    st2["digest_until"] = time.time() - 1
    st2["cooldown_until"] = 0.0
    r2 = _proc(a, "a2a", {**msg, "id": "p2"}, tmp_path / "p2.json")
    assert a.handle_message.await_count == 1, "bundle is one read-only turn"
    assert r2["delivered_to_agent"] is True
    assert r2["drop_reason"] is None


def test_receipt_jsonl_shape(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    _mk_thread(tmp_path, a)
    a.handle_message = AsyncMock()

    _proc(a, "imsg", {
        "sender": "ronny", "text": "moin", "id": "shape1",
        "chat": {"id": "u1", "type": "direct"},
    }, tmp_path / "shape1.json")

    line = _last_receipt(a, "imsg")
    assert set(line) == {
        "ts", "bridge", "file", "message_id",
        "dedupe_key", "delivered_to_agent", "drop_reason",
    }
    assert line["bridge"] == "imsg"
    assert line["file"] == "shape1.json"
    assert isinstance(line["delivered_to_agent"], bool)
    assert line["dedupe_key"]


def test_receipt_cap_trims_history(tmp_path):
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a.handle_message = AsyncMock()
    for i in range(a.DELIVERY_RECEIPT_CAP + 10):
        _proc(a, "imsg", {
            "sender": "ronny", "text": f"m{i}", "id": f"c{i}",
            "chat": {"id": "u1", "type": "direct"},
        }, tmp_path / f"c{i}.json")
    lines = _receipt_path(a, "imsg").read_text(
        encoding="utf-8").strip().splitlines()
    assert len(lines) <= a.DELIVERY_RECEIPT_CAP
    assert json.loads(lines[-1])["message_id"] == f"c{a.DELIVERY_RECEIPT_CAP + 9}"
