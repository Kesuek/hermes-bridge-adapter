"""T-094/T-095/T-096/T-097: delivery-layer atomicity + at-least-once.

Covers:
- T-094: write_private / write_inbox_private are atomic (tmp + os.replace),
  leave no temp files behind, and keep 0600 perms
- T-095: drain_outbox_once keeps the file on send error (at-least-once) and
  does NOT delete files it cannot parse (torn-read protection)
- T-096: adapter _write_outbox is atomic (no *.tmp leftovers)
- T-097: adapter _poll_all does NOT mark parse-failed files as seen, so a
  torn read is retried on the next poll
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
sys.path.insert(0, str(ROOT / "wrappers"))

from hermes_bridge_sdk import files as sdk_files  # noqa: E402
from hermes_bridge_sdk import loops as sdk_loops  # noqa: E402


# ── T-094: atomic write_private ────────────────────────────────────────

def test_write_private_is_complete_and_leaves_no_tmp(tmp_path):
    target = tmp_path / "state.json"
    sdk_files.write_private(target, json.dumps({"v": 1}))
    assert json.loads(target.read_text()) == {"v": 1}
    leftovers = [p for p in tmp_path.iterdir() if p.name != "state.json"]
    assert leftovers == []


def test_write_private_keeps_0600(tmp_path):
    target = tmp_path / "state.json"
    sdk_files.write_private(target, "{}")
    mode = target.stat().st_mode & 0o777
    assert mode == 0o600


def test_write_private_overwrites_atomically(tmp_path):
    target = tmp_path / "state.json"
    sdk_files.write_private(target, "v1")
    sdk_files.write_private(target, "v2")
    assert target.read_text() == "v2"
    assert list(tmp_path.iterdir()) == [target]


def test_write_inbox_private_leaves_no_tmp(tmp_path):
    inbox = tmp_path / "inbox" / "imsg"
    p = sdk_files.write_inbox_private(
        "imsg", {"id": "m1", "sender": "x", "text": "hi"}, inbox_dir=inbox)
    assert p.exists()
    assert json.loads(p.read_text())["id"] == "m1"
    assert list(inbox.glob("*.tmp*")) == []


def test_write_private_survives_unwritable_target(tmp_path, monkeypatch):
    """If the write fails, the temp file is cleaned up and the error raises."""
    target = tmp_path / "sub" / "state.json"  # parent missing -> write fails
    with pytest.raises(OSError):
        sdk_files.write_private(target, "{}")
    assert list(tmp_path.iterdir()) == []


# ── T-095: outbox drain at-least-once ──────────────────────────────────

def test_drain_keeps_file_on_send_error(tmp_path):
    outbox = tmp_path / "outbox" / "imsg"
    outbox.mkdir(parents=True)
    f = outbox / "m1.json"
    f.write_text(json.dumps({"target": "chat1", "text": "hi"}))

    calls = []

    def send(target, text, attachments):
        calls.append((target, text))
        raise RuntimeError("platform down")

    sdk_loops.drain_outbox_once("imsg", send, once=True, outbox_dir=outbox)
    assert calls == [("chat1", "hi")]
    assert f.exists()  # kept for retry


def test_drain_unlinks_after_success(tmp_path):
    outbox = tmp_path / "outbox" / "imsg"
    outbox.mkdir(parents=True)
    f = outbox / "m1.json"
    f.write_text(json.dumps({"target": "chat1", "text": "hi"}))
    sdk_loops.drain_outbox_once("imsg", lambda t, x, a: None,
                                once=True, outbox_dir=outbox)
    assert not f.exists()


def test_drain_keeps_invalid_json(tmp_path):
    outbox = tmp_path / "outbox" / "imsg"
    outbox.mkdir(parents=True)
    f = outbox / "m1.json"
    f.write_text('{"target": "cha')  # torn read
    sdk_loops.drain_outbox_once("imsg", lambda t, x, a: None,
                                once=True, outbox_dir=outbox)
    assert f.exists()


def test_drain_consumes_typing(tmp_path):
    outbox = tmp_path / "outbox" / "imsg"
    outbox.mkdir(parents=True)
    f = outbox / "m1.json"
    f.write_text(json.dumps({"target": "chat1", "typing": True}))
    sent = []
    sdk_loops.drain_outbox_once(
        "imsg", lambda t, x, a: sent.append(t), once=True, outbox_dir=outbox)
    assert sent == []
    assert not f.exists()


def test_drain_strips_bridge_prefix(tmp_path):
    outbox = tmp_path / "outbox" / "imsg"
    outbox.mkdir(parents=True)
    f = outbox / "m1.json"
    f.write_text(json.dumps({"target": "imsg~chat1", "text": "hi"}))
    sent = []
    sdk_loops.drain_outbox_once(
        "imsg", lambda t, x, a: sent.append(t), once=True, outbox_dir=outbox)
    assert sent == ["chat1"]


# ── T-096: adapter _write_outbox atomic ────────────────────────────────

# _make_adapter pattern from test_hygiene.py (avoids duplicating it here)
@pytest.fixture()
def _register_platform():
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


def _register_bridge(a, bridge="imsg"):
    """Register a bridge manifest so _poll_all / _write_outbox see it
    (same pattern as test_hygiene.py)."""
    reg = a._bridge_dir / "registry"
    reg.mkdir(parents=True, exist_ok=True)
    (reg / f"{bridge}.yaml").write_text(
        f"name: {bridge}\ntarget_format: [chat_id]\n", encoding="utf-8")
    a._reconcile_registry_sync()
    a._extra["allow_all"] = "true"


def _make_adapter(tmp_path: Path):
    from gateway.config import PlatformConfig
    from adapter import BridgeAdapter

    bridge_dir = tmp_path / "bridge"
    bridge_dir.mkdir()
    cfg = PlatformConfig(enabled=True, extra={"bridge_dir": str(bridge_dir)})
    return BridgeAdapter(cfg)


def test_adapter_write_outbox_leaves_no_tmp(tmp_path, _register_platform):
    a = _make_adapter(tmp_path)
    r = asyncio.run(a._write_outbox("imsg", "chat1", "hi"))
    assert r.success
    outbox = tmp_path / "bridge" / "outbox" / "imsg"
    files = list(outbox.glob("*.json"))
    assert len(files) == 1
    assert json.loads(files[0].read_text())["text"] == "hi"
    assert list(outbox.glob("*.tmp*")) == []


# ── T-097: poller retries parse failures ──────────────────────────────

def test_poller_retries_invalid_json(tmp_path, _register_platform):
    a = _make_adapter(tmp_path)
    _register_bridge(a)
    inbox = a._bridge_dir / "inbox" / "imsg"
    inbox.mkdir(parents=True, exist_ok=True)
    f = inbox / "m1.json"
    f.write_text('{"sender": "x", "text": "he')  # torn

    asyncio.run(a._poll_all())
    assert str(f.absolute()) not in a._seen_files  # not marked seen

    # writer finishes -> file complete -> next poll processes it
    # (T-089: after processing the file is unlinked and the marker discarded)
    f.write_text('{"sender": "x", "text": "hello", "chat": {"id": "c1"}}')
    a.handle_message = AsyncMock()
    asyncio.run(a._poll_all())
    assert a.handle_message.await_count == 1
    assert not f.exists()  # processed -> unlinked


def test_poller_symlink_still_marked_seen(tmp_path, _register_platform):
    """T-072 behaviour preserved: symlinks are rejected AND marked seen."""
    a = _make_adapter(tmp_path)
    _register_bridge(a)
    inbox = a._bridge_dir / "inbox" / "imsg"
    inbox.mkdir(parents=True, exist_ok=True)
    target = tmp_path / "evil.json"
    target.write_text('{"sender": "x", "text": "hi"}')
    f = inbox / "m1.json"
    f.symlink_to(target)

    asyncio.run(a._poll_all())
    assert str(f.absolute()) in a._seen_files