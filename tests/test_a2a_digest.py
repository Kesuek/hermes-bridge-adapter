"""Tests for the a2a digest (T-061E): adaptive coalesce outside unified threads.

``mode=a2a`` — same digest machinery pattern as T-080/T-061, but with its
own state file (``a2a_digest_states.json``), its own flush loop, and no
unified-thread involvement. Runs against the ``adapter`` module directly,
mirroring ``test_unified.py``.
"""
import asyncio
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
    bridge_dir.mkdir()
    cfg = PlatformConfig(enabled=True, extra={"bridge_dir": str(bridge_dir)})
    return BridgeAdapter(cfg)


# ── Bridge selection ────────────────────────────────────────────────


def test_default_bridge_is_a2a(tmp_path):
    a = _make_adapter(tmp_path)
    assert a._a2a_digest_bridges() == frozenset({"a2a"})


def test_env_off_disables_digest(tmp_path, monkeypatch):
    monkeypatch.setenv("A2A_DIGEST_BRIDGES", "off")
    a = _make_adapter(tmp_path)
    assert a._a2a_digest_bridges() == frozenset()


# ── Decision machine ────────────────────────────────────────────────


def test_first_message_passes_quiet_channel(tmp_path):
    a = _make_adapter(tmp_path)
    assert a._a2a_note_digest("RYBFNMET", "RYBFNMET", "hello") == "pass"


def test_isolated_message_passes_again(tmp_path):
    a = _make_adapter(tmp_path)
    a._a2a_note_digest("P1", "P1", "m1")
    # backdate the previous inbound so the gap exceeds A2A_SERIE_GAP
    a._a2a_digest_state("P1")["last_inbound_ts"] = time.time() - 120
    assert a._a2a_note_digest("P1", "P1", "m2") == "pass"


def test_series_opens_window_and_buffers(tmp_path):
    a = _make_adapter(tmp_path)
    assert a._a2a_note_digest("P1", "P1", "m1") == "pass"
    assert a._a2a_note_digest("P1", "P1", "m2") == "buffer"
    st = a._a2a_digest_state("P1")
    assert st["state"] == "digesting"
    assert len(st["buffer"]) == 1
    assert st["buffer"][0]["text"] == "m2"


def test_window_elapsed_next_entry_flushes_bundle(tmp_path):
    a = _make_adapter(tmp_path)
    a._a2a_note_digest("P1", "P1", "m1")  # pass
    a._a2a_note_digest("P1", "P1", "m2")  # buffer, window opens
    st = a._a2a_digest_state("P1")
    st["digest_until"] = time.time() - 1  # window elapsed before m3 arrives
    out = a._a2a_note_digest("P1", "P1", "m3")
    assert out.startswith("[a2a digest: 2 messages")
    assert "m2" in out and "m3" in out


def test_burst_cap_flushes_regardless(tmp_path):
    a = _make_adapter(tmp_path)
    a._a2a_note_digest("P1", "P1", "m1")
    assert a._a2a_note_digest("P1", "P1", "m2") == "buffer"
    st = a._a2a_digest_state("P1")
    st["digest_until"] = time.time() + 9999  # window open forever → cap decides
    for i in range(6):
        assert a._a2a_note_digest("P1", "P1", f"b{i}") == "buffer"
    out = a._a2a_note_digest("P1", "P1", "b7")
    assert out.startswith("[a2a digest: 8 messages")


def test_state_survives_adapter_reload(tmp_path):
    a = _make_adapter(tmp_path)
    a._a2a_note_digest("P1", "P1", "m1")
    a._a2a_note_digest("P1", "P1", "m2")
    from gateway.config import PlatformConfig

    cfg = PlatformConfig(
        enabled=True, extra={"bridge_dir": str(tmp_path / "bridge")}
    )
    b = BridgeAdapter(cfg)
    b._load_a2a_digest_states()
    st = b._a2a_digest_state("P1")
    assert st["state"] == "digesting"
    assert st["buffer"][0]["text"] == "m2"


# ── Timer side ──────────────────────────────────────────────────────


def test_timer_flushes_idle_bundle(tmp_path):
    a = _make_adapter(tmp_path)
    a._a2a_note_digest("P1", "P1", "m1")
    a._a2a_note_digest("P1", "P1", "m2")
    st = a._a2a_digest_state("P1")
    st["digest_until"] = time.time() - 1
    a.handle_message = AsyncMock()
    asyncio.run(a._flush_a2a_digests())
    a.handle_message.assert_awaited_once()
    ev = a.handle_message.await_args.args[0]
    assert "m2" in ev.text
    assert ev.source.chat_id == "P1"
    assert ev.source.chat_type == "direct"
    assert a._a2a_digest_state("P1")["state"] == "active"


def test_timer_defers_during_cooldown(tmp_path):
    a = _make_adapter(tmp_path)
    a._a2a_note_digest("P1", "P1", "m1")
    a._a2a_note_digest("P1", "P1", "m2")
    st = a._a2a_digest_state("P1")
    st["cooldown_until"] = time.time() + 9999
    a.handle_message = AsyncMock()
    asyncio.run(a._flush_a2a_digests())
    a.handle_message.assert_not_awaited()
    assert a._a2a_digest_state("P1")["state"] == "digesting"