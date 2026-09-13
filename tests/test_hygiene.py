"""Hygiene & cleanup tests (T-071/072/078/079/080).

Covers:
- T-071: wrapper state/status/manifest/inbox files are written with 0600 perms
- T-078: reply_map prunes entries older than the TTL on save
- T-079: _atomic_write_json uses a unique (uuid) temp name per write
- T-080: cooldown_until is enforced (blocks a flush during cooldown)
- T-072: adversarial file tests (symlink in inbox, manifest path traversal)

These tests run against the ``adapter`` module directly (mirroring
test_unified.py / test_registry.py) and the wrapper scripts via
importlib (the wrapper files use a hyphen in their names).
"""
import asyncio
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "wrappers"))

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


def _load_wrapper_module(path: Path, name: str):
    """Load a wrapper script (hyphen in filename) as a module."""
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# ── T-071: wrapper state files are 0600 ──────────────────────────────


def _load_imsg_wrapper(tmp_path, monkeypatch):
    w = _load_wrapper_module(ROOT / "wrappers" / "imsg-wrapper.py", "imsg_wrapper_hygiene")
    bridge_dir = tmp_path / "bridge"
    bridge_dir.mkdir()
    monkeypatch.setattr(w, "BRIDGE_DIR", bridge_dir)
    return w


def _load_talk_wrapper(tmp_path, monkeypatch):
    w = _load_wrapper_module(ROOT / "wrappers" / "talk-wrapper.py", "talk_wrapper_hygiene")
    bridge_dir = tmp_path / "bridge"
    bridge_dir.mkdir()
    monkeypatch.setenv("BRIDGE_DIR", str(bridge_dir))
    # Re-resolve the SDK's env-derived bridge dir for this test's tmp path.
    import hermes_bridge_sdk.files as sdk_files
    monkeypatch.setattr(sdk_files, "bridge_dir", lambda: bridge_dir)
    return w


def test_imsg_wrapper_status_file_is_0600(tmp_path, monkeypatch):
    """T-071: imsg wrapper status heartbeat must produce a 0600 status.json."""
    w = _load_imsg_wrapper(tmp_path, monkeypatch)
    import hermes_bridge_sdk.files as sdk_files
    monkeypatch.setattr(sdk_files, "bridge_dir", lambda: tmp_path / "bridge")
    sdk_files.write_status("imsg", connected=True)
    mode = (tmp_path / "bridge" / "status" / "imsg" / "status.json").stat().st_mode & 0o777
    assert mode == 0o600, f"status.json must be 0600, got {oct(mode)}"


def test_imsg_wrapper_state_file_is_0600(tmp_path, monkeypatch):
    """T-071: imsg-wrapper.save_last_seen must produce a 0600 last_seen.json."""
    w = _load_imsg_wrapper(tmp_path, monkeypatch)
    w.save_last_seen({"chat1": 100}, w.STATE_FILE)
    mode = w.STATE_FILE.stat().st_mode & 0o777
    assert mode == 0o600, f"last_seen.json must be 0600, got {oct(mode)}"


def test_imsg_wrapper_manifest_file_is_0600(tmp_path, monkeypatch):
    """T-071: BridgeRunner.register must produce a 0600 manifest."""
    w = _load_imsg_wrapper(tmp_path, monkeypatch)
    import hermes_bridge_sdk.files as sdk_files
    monkeypatch.setattr(sdk_files, "bridge_dir", lambda: tmp_path / "bridge")
    sdk_files.write_manifest(
        "imsg", "imessage", "mac-mini-01",
        ["email", "phone", "chat_id"], ["text", "attachments", "reactions"],
        manifest_file=tmp_path / "bridge" / "registry" / "imsg.yaml",
    )
    mode = (tmp_path / "bridge" / "registry" / "imsg.yaml").stat().st_mode & 0o777
    assert mode == 0o600, f"manifest must be 0600, got {oct(mode)}"


def test_imsg_wrapper_inbox_file_is_0600(tmp_path, monkeypatch):
    """T-071: write_inbox_private must produce a 0600 inbox file."""
    _load_imsg_wrapper(tmp_path, monkeypatch)
    import hermes_bridge_sdk.files as sdk_files
    monkeypatch.setattr(sdk_files, "bridge_dir", lambda: tmp_path / "bridge")
    sdk_files.write_inbox_private(
        "imsg", {"id": "msg1", "sender": "x", "text": "hi"},
        inbox_dir=tmp_path / "bridge" / "inbox" / "imsg")
    p = tmp_path / "bridge" / "inbox" / "imsg" / "msg1.json"
    mode = p.stat().st_mode & 0o777
    assert mode == 0o600, f"inbox file must be 0600, got {oct(mode)}"


def test_talk_wrapper_status_file_is_0600(tmp_path, monkeypatch):
    """T-071: talk wrapper status heartbeat must produce a 0600 status.json."""
    _load_talk_wrapper(tmp_path, monkeypatch)
    import hermes_bridge_sdk.files as sdk_files
    monkeypatch.setattr(sdk_files, "bridge_dir", lambda: tmp_path / "bridge")
    sdk_files.write_status("talk", connected=True)
    mode = (tmp_path / "bridge" / "status" / "talk" / "status.json").stat().st_mode & 0o777
    assert mode == 0o600, f"status.json must be 0600, got {oct(mode)}"


def test_talk_wrapper_state_file_is_0600(tmp_path, monkeypatch):
    """T-071: save_last_seen must produce a 0600 last_seen.json."""
    _load_talk_wrapper(tmp_path, monkeypatch)
    import hermes_bridge_sdk.files as sdk_files
    monkeypatch.setattr(sdk_files, "bridge_dir", lambda: tmp_path / "bridge")
    sdk_files.save_last_seen({"room1": 100}, tmp_path / "bridge" / "state" / "talk" / "last_seen.json")
    mode = (tmp_path / "bridge" / "state" / "talk" / "last_seen.json").stat().st_mode & 0o777
    assert mode == 0o600, f"last_seen.json must be 0600, got {oct(mode)}"


def test_talk_wrapper_manifest_file_is_0600(tmp_path, monkeypatch):
    """T-071: write_manifest must produce a 0600 manifest."""
    _load_talk_wrapper(tmp_path, monkeypatch)
    import hermes_bridge_sdk.files as sdk_files
    monkeypatch.setattr(sdk_files, "bridge_dir", lambda: tmp_path / "bridge")
    sdk_files.write_manifest(
        "talk", "nextcloud-talk", "your-nextcloud.example.com",
        ["chat_id"], ["text"],
        manifest_file=tmp_path / "bridge" / "registry" / "talk.yaml",
    )
    mode = (tmp_path / "bridge" / "registry" / "talk.yaml").stat().st_mode & 0o777
    assert mode == 0o600, f"manifest must be 0600, got {oct(mode)}"


def test_talk_wrapper_inbox_file_is_0600(tmp_path, monkeypatch):
    """T-071: write_inbox_private must produce a 0600 inbox file."""
    _load_talk_wrapper(tmp_path, monkeypatch)
    import hermes_bridge_sdk.files as sdk_files
    monkeypatch.setattr(sdk_files, "bridge_dir", lambda: tmp_path / "bridge")
    sdk_files.write_inbox_private("talk", {"id": "msg1", "sender": "x", "text": "hi"},
                                  inbox_dir=tmp_path / "bridge" / "inbox" / "talk")
    p = tmp_path / "bridge" / "inbox" / "talk" / "msg1.json"
    mode = p.stat().st_mode & 0o777
    assert mode == 0o600, f"inbox file must be 0600, got {oct(mode)}"


# ── T-078: reply_map cap + prune ─────────────────────────────────────


def test_reply_map_prunes_old_entries(tmp_path):
    """T-078: _save_reply_map must drop entries older than REPLY_MAP_TTL."""
    a = _make_adapter(tmp_path)
    a._reply_map = {
        "old1": {"bridge": "imsg", "local_msg_id": "m1",
                 "ts": time.time() - 10 * 86400},  # 10 days old
        "new1": {"bridge": "imsg", "local_msg_id": "m2",
                 "ts": time.time()},
    }
    a._save_reply_map()
    assert "old1" not in a._reply_map, "old reply_map entry must be pruned"
    assert "new1" in a._reply_map, "recent reply_map entry must be kept"


def test_reply_map_entry_has_ts_when_registered(tmp_path):
    """T-078: registering an inbound adds a ts field for the prune logic."""
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
    assert any("ts" in v for v in a._reply_map.values()), \
        "registered reply_map entries must carry a ts field"


def test_reply_map_cap_drops_oldest(tmp_path):
    """T-078: a reply_map beyond REPLY_MAP_CAP drops the oldest entries."""
    a = _make_adapter(tmp_path)
    cap = getattr(a, "REPLY_MAP_CAP", 5000)
    base = time.time() - 2 * cap  # ensure distinct, ordered ts
    entries = {
        f"k{i}": {"bridge": "imsg", "local_msg_id": f"m{i}", "ts": base + i}
        for i in range(cap + 10)
    }
    a._reply_map = entries
    a._save_reply_map()
    assert len(a._reply_map) <= cap, \
        f"reply_map must be capped at {cap}, got {len(a._reply_map)}"


# ── T-079: _atomic_write_json unique temp name ───────────────────────


def test_atomic_write_uses_unique_tmp_name(tmp_path):
    """T-079: two atomic writes must not collide on a shared .tmp path."""
    a = _make_adapter(tmp_path)
    p = tmp_path / "data.json"
    a._atomic_write_json(p, {"a": 1})
    a._atomic_write_json(p, {"b": 2})
    leftovers = list(tmp_path.glob("*.tmp"))
    assert not leftovers, f"leftover .tmp files: {leftovers}"
    # Final file is the last write.
    assert json.loads(p.read_text("utf-8")) == {"b": 2}


def test_atomic_write_tmp_name_contains_uuid_component(tmp_path, monkeypatch):
    """T-079: the temp name must include a uuid component (not a fixed
    suffix), so two concurrent bridges can't collide on the same path."""
    a = _make_adapter(tmp_path)
    captured = []

    real_replace = os.replace

    def spy_replace(src, dst):
        captured.append(Path(src).name)
        return real_replace(src, dst)

    monkeypatch.setattr(adapter.os, "replace", spy_replace)
    p = tmp_path / "data.json"
    a._atomic_write_json(p, {"a": 1})
    assert captured, "os.replace was not called"
    tmp_name = captured[0]
    # The temp name must NOT be the predictable ``data.json.tmp`` form.
    assert tmp_name != "data.json.tmp", \
        f"temp name must not be predictable, got {tmp_name!r}"
    # It must contain a uuid hex component (32 hex chars) between the name
    # and the .tmp suffix.
    import re
    m = re.match(r"^data\.json\.([0-9a-f]{32})\.tmp$", tmp_name)
    assert m, f"temp name must contain a uuid hex: {tmp_name!r}"


# ── T-080: cooldown_until blocks flush ───────────────────────────────


def test_adaptive_cooldown_blocks_immediate_flush(tmp_path):
    """T-080: a digest flush must be blocked while cooldown_until is in
    the future.

    Pre-fix the adapter set ``cooldown_until`` after a flush but never
    read it, so a second burst right after a flush was dispatched
    immediately (defeating the cooldown's purpose). The flush decision in
    ``_process_incoming`` must defer while ``now < cooldown_until``.
    """
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a.handle_message = AsyncMock()

    # Force the thread into digesting with the digest window elapsed
    # (flush is due) BUT a cooldown still active.
    st = a._adaptive_state("projekt")
    st["state"] = "digesting"
    st["buffer"] = [{"ts": time.time() - 5, "sender": "ronny", "text": "msg 0"}]
    st["digest_until"] = time.time() - 1  # flush is due
    st["cooldown_until"] = time.time() + 100  # but in cooldown
    a._save_unified_threads()

    inbox_file = tmp_path / "bridge" / "inbox" / "imsg" / "m.json"
    inbox_file.parent.mkdir(parents=True, exist_ok=True)
    inbox_file.write_text("{}", encoding="utf-8")

    async def run():
        await a._process_incoming("imsg", {
            "sender": "ronny", "text": "msg 1",
            "chat": {"id": "u1", "type": "direct"},
        }, inbox_file)

    asyncio.run(run())
    # Flush must be deferred by the cooldown — the message is buffered,
    # NOT dispatched as a bundle.
    a.handle_message.assert_not_awaited()
    # The new message joined the buffer (still digesting), not a dispatch.
    st2 = a._adaptive_state("projekt")
    assert st2["state"] == "digesting"
    assert any("msg 1" in m.get("text", "") for m in st2["buffer"])


def test_adaptive_flushes_after_cooldown_elapsed(tmp_path):
    """T-080 regression: once the cooldown has elapsed, a due flush dispatches
    normally — the cooldown must only block while active, not permanently."""
    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    a._load_unified_threads()
    a._cmd_unified_create("imsg", {"sender": "ronny", "chat": {"id": "u1"}}, "projekt")
    a.handle_message = AsyncMock()

    st = a._adaptive_state("projekt")
    st["state"] = "digesting"
    st["buffer"] = [{"ts": time.time() - 5, "sender": "ronny", "text": "msg 0"}]
    st["digest_until"] = time.time() - 1  # flush is due
    st["cooldown_until"] = time.time() - 1  # cooldown elapsed
    a._save_unified_threads()

    inbox_file = tmp_path / "bridge" / "inbox" / "imsg" / "m.json"
    inbox_file.parent.mkdir(parents=True, exist_ok=True)
    inbox_file.write_text("{}", encoding="utf-8")

    async def run():
        await a._process_incoming("imsg", {
            "sender": "ronny", "text": "msg 1",
            "chat": {"id": "u1", "type": "direct"},
        }, inbox_file)

    asyncio.run(run())
    a.handle_message.assert_awaited_once()
    event = a.handle_message.await_args[0][0]
    assert "[System:" in event.text


# ── T-072: adversarial file tests ────────────────────────────────────


def test_symlink_in_inbox_rejected(tmp_path):
    """T-072a: a symlink in the inbox must not be read through — the adapter
    must reject (not process) a symlinked inbox file."""
    a = _make_adapter(tmp_path)
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text("name: imsg\ntarget_format: [chat_id]\n",
                                   encoding="utf-8")
    a._reconcile_registry_sync()
    a._extra["allow_all"] = "true"
    a.handle_message = AsyncMock()

    inbox = a._bridge_dir / "inbox" / "imsg"
    inbox.mkdir(parents=True, exist_ok=True)
    target = tmp_path / "evil.json"
    target.write_text('{"sender": "x", "text": "hi", "chat": {"id": "c1"}}',
                      encoding="utf-8")
    (inbox / "msg.json").symlink_to(target)

    # _poll_all must skip the symlink (not follow it).
    asyncio.run(a._poll_all())
    a.handle_message.assert_not_awaited()
    # The symlink itself must not be unlinked by the adapter (it rejected,
    # not processed — unlinking a symlink the wrapper created would be
    # surprising). The target file is untouched.
    assert target.exists(), "target of the symlink must not be deleted"


def test_manifest_path_traversal_rejected(tmp_path):
    """T-072b: a manifest with a ``service`` field that contains path
    traversal must be rejected at registry reconcile — the evil bridge must
    not be registered."""
    a = _make_adapter(tmp_path)
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "evil.yaml").write_text(
        "name: evil\ntarget_format: [chat_id]\nservice: ../../etc/passwd\n",
        encoding="utf-8",
    )
    a._reconcile_registry_sync()
    assert "evil" not in a._bridges, \
        "manifest with traversal in service must be rejected"


# ── T-089: seen-file discard + claim expiry sweep ────────────────────


def test_seen_files_discarded_on_unlink(tmp_path):
    """T-089a: a processed inbox file must be removed from _seen_files.

    Previously the adapter only ever added to _seen_files, so entries
    accumulated for the whole process lifetime even though the corresponding
    files were unlinked right after processing. With the discard-on-unlink
    helper the set stays proportional to the number of in-flight files.
    """
    a = _make_adapter(tmp_path)
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text("name: imsg\ntarget_format: [chat_id]\n",
                                   encoding="utf-8")
    a._reconcile_registry_sync()
    a._extra["allow_all"] = "true"
    a.handle_message = AsyncMock()

    inbox = a._bridge_dir / "inbox" / "imsg"
    inbox.mkdir(parents=True, exist_ok=True)
    msg = inbox / "msg.json"
    msg.write_text('{"sender": "x", "text": "hi", "chat": {"id": "c1"}}',
                   encoding="utf-8")

    asyncio.run(a._poll_all())

    a.handle_message.assert_awaited_once()
    assert not msg.exists(), "processed inbox file must be unlinked"
    assert str(msg.absolute()) not in a._seen_files, (
        "a processed (unlinked) inbox file must be discarded from _seen_files"
    )


def test_run_cleanup_purges_expired_claims(tmp_path):
    """T-089b: _run_cleanup proactively deletes expired pending claims from
    the in-memory dict AND the persisted pending_claims.json, instead of
    leaving them to linger until the next confirm attempt touches them.
    """
    a = _make_adapter(tmp_path)
    a._bridges = ["imsg", "talk"]
    a._load_unified_threads()
    a._load_pending_claims()
    a._load_identity_map()
    expired_id = "deadbeef"
    a._pending_claims[expired_id] = {
        "code": "123456",
        "source": "imsg:ronny",
        "target": "talk~ronny",
        "expires": time.time() - 10,  # already expired
        "attempts": 0,
    }
    a._save_pending_claims()
    assert (a._bridge_dir / "pending_claims.json").exists()

    asyncio.run(a._run_cleanup())

    assert expired_id not in a._pending_claims
    # Persisted file must be updated too, not just the in-memory dict.
    a._load_pending_claims()
    assert expired_id not in a._pending_claims


def test_run_cleanup_runs_via_to_thread(tmp_path, monkeypatch):
    """T-088: the blocking media/outbox sweep must run through
    asyncio.to_thread(self._run_cleanup_sync), not inline in the event loop.
    """
    a = _make_adapter(tmp_path)
    calls = []
    real_to_thread = asyncio.to_thread

    async def fake_to_thread(fn, *args, **kwargs):
        calls.append(fn.__name__)
        return await real_to_thread(fn, *args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", fake_to_thread)
    asyncio.run(a._run_cleanup())
    assert calls == ["_run_cleanup_sync"]