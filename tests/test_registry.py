"""Tests for the registry-based bridge adapter (T-050).

These tests cover the manifest loader, registry scanning, target
validation, and the runtime reconcile logic. They run against the
``adapter`` module directly; the Hermes gateway SDK imports are
available because the plugin is loaded from a Hermes installation.
"""
import asyncio
import os
import sys
from pathlib import Path

import pytest

# Make the repo root importable so ``import adapter`` works regardless of
# how pytest is invoked.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import adapter  # noqa: E402
from adapter import (
    BridgeAdapter,
    BridgeConfig,
    BridgeManifest,
    load_manifest,
    read_manifest,
    scan_registry,
)


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _register_platform():
    """Register ``bridge-adapter`` in the platform_registry so that
    ``Platform("bridge-adapter")`` resolves (the enum's ``_missing_`` only
    creates a pseudo-member for runtime-registered plugins)."""
    from gateway.platform_registry import platform_registry

    from adapter import register

    class _Ctx:
        def __init__(self):
            self.names = []

        def register_platform(self, *, name, **kwargs):
            from gateway.platform_registry import PlatformEntry

            entry = PlatformEntry(name=name, adapter_factory=kwargs.get("adapter_factory"),
                                  check_fn=kwargs.get("check_fn"), **{
                                      k: v for k, v in kwargs.items()
                                      if k not in ("adapter_factory", "check_fn")})
            platform_registry.register(entry)
            self.names.append(name)

    if not platform_registry.is_registered("bridge-adapter"):
        register(_Ctx())

    yield
    # Leave registry as found; Hermes manages it at runtime.


# ── Task 1: Manifest loader ─────────────────────────────────────────


def test_load_manifest_valid():
    manifest = {
        "name": "imsg",
        "service": "imessage",
        "host": "mac-mini-01",
        "target_format": ["email", "phone", "chat_id"],
        "capabilities": ["text"],
    }
    m = load_manifest(manifest)
    assert m.name == "imsg"
    assert m.service == "imessage"
    assert m.host == "mac-mini-01"
    assert m.target_format == ["email", "phone", "chat_id"]
    assert m.capabilities == ["text"]


def test_load_manifest_defaults():
    m = load_manifest({"name": "telegram"})
    assert m.name == "telegram"
    assert m.service == "telegram"  # defaults to name
    assert m.host == ""
    assert m.target_format == []
    assert m.capabilities == []


def test_load_manifest_invalid_missing_name():
    with pytest.raises(ValueError):
        load_manifest({"service": "imessage"})


def test_load_manifest_invalid_empty_name():
    with pytest.raises(ValueError):
        load_manifest({"name": "", "service": "imessage"})


def test_load_manifest_invalid_not_mapping():
    with pytest.raises(ValueError):
        load_manifest(["not", "a", "mapping"])  # type: ignore[arg-type]


def test_load_manifest_scalar_target_format_becomes_list():
    m = load_manifest({"name": "x", "target_format": "email"})
    assert m.target_format == ["email"]


def test_read_manifest_roundtrip(tmp_path):
    p = tmp_path / "imsg.yaml"
    p.write_text(
        "name: imsg\n"
        "service: imessage\n"
        "host: mac-mini-01\n"
        "target_format: [email, phone, chat_id]\n"
        "capabilities: [text]\n",
        encoding="utf-8",
    )
    m = read_manifest(p)
    assert m.name == "imsg"
    assert m.service == "imessage"
    assert m.target_format == ["email", "phone", "chat_id"]


def test_read_manifest_empty_file(tmp_path):
    p = tmp_path / "empty.yaml"
    p.write_text("", encoding="utf-8")
    with pytest.raises(ValueError):
        read_manifest(p)


# ── Task 1/3: accepts_target ─────────────────────────────────────────


def test_accepts_target_no_constraint_accepts_anything():
    m = BridgeManifest(name="x")  # no target_format
    assert m.accepts_target("ronny@example.com")
    assert m.accepts_target("alice")
    assert m.accepts_target("+15551234567")


def test_accepts_target_empty_rejected():
    m = BridgeManifest(name="x")
    assert not m.accepts_target("")


def test_accepts_target_email_ok():
    m = BridgeManifest(name="imsg", target_format=["email"])
    assert m.accepts_target("ronny@example.com")


def test_accepts_target_email_wrong():
    m = BridgeManifest(name="imsg", target_format=["email"])
    assert not m.accepts_target("alice")  # no @, no digit → not routable


def test_accepts_target_phone_ok():
    m = BridgeManifest(name="sms", target_format=["phone"])
    assert m.accepts_target("+15551234567")


def test_accepts_target_phone_wrong():
    m = BridgeManifest(name="sms", target_format=["phone"])
    assert not m.accepts_target("alice")  # no digit


def test_accepts_target_chat_id_ok():
    m = BridgeManifest(name="matrix", target_format=["chat_id"])
    assert m.accepts_target("!room:matrix.org")


def test_accepts_target_multiple_formats():
    m = BridgeManifest(name="imsg", target_format=["email", "phone", "chat_id"])
    assert m.accepts_target("ronny@example.com")
    assert m.accepts_target("+15551234567")
    assert m.accepts_target("alice")  # chat_id accepts anything non-empty


# ── Task 2: scan_registry ─────────────────────────────────────────────


def test_scan_registry_finds_new_bridge(tmp_path):
    reg = tmp_path / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text(
        "name: imsg\nservice: imessage\ntarget_format: [email, phone, chat_id]\n",
        encoding="utf-8",
    )
    found = scan_registry(reg)
    assert "imsg" in found
    assert isinstance(found["imsg"], BridgeManifest)
    assert found["imsg"].service == "imessage"
    assert found["imsg"].target_format == ["email", "phone", "chat_id"]


def test_scan_registry_missing_dir(tmp_path):
    reg = tmp_path / "does-not-exist"
    assert scan_registry(reg) == {}


def test_scan_registry_ignores_non_yaml(tmp_path):
    reg = tmp_path / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text("name: imsg\n", encoding="utf-8")
    (reg / "notes.txt").write_text("name: not-a-bridge\n", encoding="utf-8")
    found = scan_registry(reg)
    assert set(found) == {"imsg"}


def test_scan_registry_skips_bad_manifest(tmp_path, caplog):
    reg = tmp_path / "registry"
    reg.mkdir()
    (reg / "good.yaml").write_text("name: good\n", encoding="utf-8")
    (reg / "bad.yaml").write_text("service: no-name\n", encoding="utf-8")  # missing name
    found = scan_registry(reg)
    assert set(found) == {"good"}


def test_scan_registry_removed(tmp_path):
    reg = tmp_path / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text("name: imsg\n", encoding="utf-8")
    assert set(scan_registry(reg)) == {"imsg"}
    (reg / "imsg.yaml").unlink()
    assert scan_registry(reg) == {}


def test_scan_registry_name_from_manifest_not_filename(tmp_path):
    reg = tmp_path / "registry"
    reg.mkdir()
    (reg / "file-a.yaml").write_text("name: real-a\n", encoding="utf-8")
    found = scan_registry(reg)
    assert "real-a" in found
    assert "file-a" not in found


# ── Task 2/4: reconcile_registry via BridgeAdapter ────────────────────


def _make_adapter(tmp_path: Path) -> BridgeAdapter:
    """Build a BridgeAdapter wired to a temporary bridge_dir."""
    from gateway.config import PlatformConfig

    bridge_dir = tmp_path / "bridge"
    bridge_dir.mkdir()
    cfg = PlatformConfig(
        enabled=True,
        extra={"bridge_dir": str(bridge_dir)},
    )
    return BridgeAdapter(cfg)


def test_reconcile_registers_new_bridge(tmp_path):
    a = _make_adapter(tmp_path)
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text(
        "name: imsg\nservice: imessage\ntarget_format: [email]\n",
        encoding="utf-8",
    )
    a._reconcile_registry_sync()
    assert "imsg" in a._bridges
    assert "imsg" in a._manifests
    assert a._manifests["imsg"].service == "imessage"
    # Folders created
    assert (a._bridge_dir / "inbox" / "imsg").is_dir()
    assert (a._bridge_dir / "outbox" / "imsg").is_dir()
    assert (a._bridge_dir / "status" / "imsg").is_dir()
    assert (a._bridge_dir / "media" / "imsg" / "incoming").is_dir()
    assert (a._bridge_dir / "media" / "imsg" / "outgoing").is_dir()


def test_reconcile_unregisters_removed_bridge(tmp_path):
    a = _make_adapter(tmp_path)
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text("name: imsg\n", encoding="utf-8")
    a._reconcile_registry_sync()
    assert "imsg" in a._bridges
    # Simulate rm of the manifest
    (reg / "imsg.yaml").unlink()
    a._reconcile_registry_sync()
    assert "imsg" not in a._bridges
    assert "imsg" not in a._manifests
    # status + media cleaned up
    assert not (a._bridge_dir / "status" / "imsg").exists()
    assert not (a._bridge_dir / "media" / "imsg").exists()


def test_reconcile_idempotent(tmp_path):
    a = _make_adapter(tmp_path)
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text("name: imsg\n", encoding="utf-8")
    a._reconcile_registry_sync()
    before = list(a._bridges)
    a._reconcile_registry_sync()
    a._reconcile_registry_sync()
    assert a._bridges == before


def test_reconcile_no_registry_dir_is_noop(tmp_path):
    a = _make_adapter(tmp_path)
    a._reconcile_registry_sync()  # registry/ does not exist yet
    assert a._bridges == []


def test_poll_all_picks_up_newly_registered_bridge(tmp_path):
    """Task 4: a bridge added to the registry is polled on the next cycle."""
    a = _make_adapter(tmp_path)
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "a.yaml").write_text("name: a\n", encoding="utf-8")
    a._reconcile_registry_sync()
    # Drop a message into inbox/a/ and ensure _poll_all processes it.
    inbox_a = a._bridge_dir / "inbox" / "a"
    inbox_a.mkdir(parents=True, exist_ok=True)
    (inbox_a / "msg.json").write_text(
        '{"sender":"u1","text":"hi","chat":{"id":"a:c1","type":"direct"}}',
        encoding="utf-8",
    )
    asyncio.run(a._poll_all())
    assert not (inbox_a / "msg.json").exists()  # processed → unlinked


# ── Task 3: BridgeConfig from manifest + _validate_target ────────────


def test_bridge_config_from_manifest():
    m = BridgeManifest(
        name="imsg", service="imessage", target_format=["email"], capabilities=["text"]
    )
    bc = BridgeConfig("imsg", {}, manifest=m)
    assert bc.name == "imsg"
    assert bc.manifest is m
    assert bc.service == "imessage"
    assert bc.target_format == ["email"]


def test_bridge_config_without_manifest_keeps_defaults():
    bc = BridgeConfig("imsg", {})
    assert bc.manifest is None
    assert bc.service is None  # not declared anywhere
    assert bc.target_format == []


def test_validate_target_with_manifest(tmp_path):
    a = _make_adapter(tmp_path)
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text(
        "name: imsg\ntarget_format: [email]\n", encoding="utf-8"
    )
    a._reconcile_registry_sync()
    assert a._validate_target("imsg", "ronny@example.com") is True
    assert a._validate_target("imsg", "alice") is False


def test_validate_target_unknown_bridge_is_permissive(tmp_path):
    a = _make_adapter(tmp_path)
    # No manifest for "telegram" → permissive
    assert a._validate_target("telegram", "anything") is True


def test_validate_target_no_manifest_is_permissive(tmp_path):
    a = _make_adapter(tmp_path)
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text("name: imsg\n", encoding="utf-8")  # no target_format
    a._reconcile_registry_sync()
    assert a._validate_target("imsg", "anything") is True


# ── Task 4: connect() boots from registry, no inbox scan ─────────────


def test_connect_with_registry(tmp_path):
    a = _make_adapter(tmp_path)
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text("name: imsg\n", encoding="utf-8")
    ok = asyncio.run(a.connect())
    assert ok is True
    assert "imsg" in a._bridges
    asyncio.run(a.disconnect())


def test_connect_without_registry_starts_and_waits(tmp_path):
    a = _make_adapter(tmp_path)
    ok = asyncio.run(a.connect())
    # Registry design: zero bridges is not a failure — the adapter boots
    # and the registry loop waits for manifests to appear at runtime.
    assert ok is True
    assert a._bridges == []
    asyncio.run(a.disconnect())

# ── T-051: routing context injection ────────────────────────────────


def test_process_incoming_injects_routing_context(tmp_path):
    from unittest.mock import AsyncMock

    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"  # allow the test sender
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text("name: imsg\ntarget_format: [email]\n", encoding="utf-8")
    a._reconcile_registry_sync()

    a.handle_message = AsyncMock()

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "user@example.com",
                "text": "Hallo",
                "chat": {"id": "imsg:user@example.com", "type": "direct"},
                "reply_to": "abc123",
            },
            tmp_path / "x.json",
        )

    asyncio.run(run())

    a.handle_message.assert_awaited_once()
    event = a.handle_message.await_args[0][0]
    assert "[Message from user@example.com, bridge imsg," in event.text
    assert "reply to imsg:user@example.com" in event.text
    assert "reply_to abc123" in event.text
    # original text preserved
    assert event.text.startswith("Hallo")


def test_group_chat_without_mention_deletes_file(tmp_path):
    """Mention-gated group message must delete the inbox file.

    Regression for the "needs a gateway restart" bug: a group message that
    fails mention gating was marked in _seen_files but never unlinked, so it
    stayed in the inbox AND in _seen_files → never processed again until a
    restart cleared _seen_files. The file must be removed on the spot.
    """
    from unittest.mock import AsyncMock

    a = _make_adapter(tmp_path)
    a._extra["allow_all"] = "true"
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text("name: imsg\ntarget_format: [email]\n", encoding="utf-8")
    a._reconcile_registry_sync()
    a.handle_message = AsyncMock()

    inbox_file = tmp_path / "bridge" / "inbox" / "imsg" / "grp.json"
    inbox_file.parent.mkdir(parents=True, exist_ok=True)
    inbox_file.write_text("{}", encoding="utf-8")
    a._seen_files.add(str(inbox_file.absolute()))

    async def run():
        await a._process_incoming(
            "imsg",
            {
                "sender": "someone@example.com",
                "text": "no @hermes here",
                "chat": {"id": "imsg:grp", "type": "group"},
            },
            inbox_file,
        )

    asyncio.run(run())

    a.handle_message.assert_not_awaited()
    assert not inbox_file.exists(), "mention-gated file should be deleted"


# ── T-053: routing fallback — unroutable target → SendResult error ──


def test_send_routable_writes_outbox(tmp_path):
    a = _make_adapter(tmp_path)
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text("name: imsg\ntarget_format: [email]\n", encoding="utf-8")
    a._reconcile_registry_sync()

    res = asyncio.run(a.send("imsg:user@example.com", "Hallo"))
    assert res.success is True
    assert (a._bridge_dir / "outbox" / "imsg").exists()


def test_send_unknown_bridge_fails_with_routing_error(tmp_path):
    a = _make_adapter(tmp_path)
    # No manifest for "talk" → not registered → unroutable
    res = asyncio.run(a.send("talk:some-chat", "Hallo"))
    assert res.success is False
    assert "talk" in res.error
    assert "bridge" in res.error.lower()
    # No outbox written for an unknown bridge
    assert not (a._bridge_dir / "outbox" / "talk").exists()


def test_send_wrong_target_format_fails_with_routing_error(tmp_path):
    a = _make_adapter(tmp_path)
    reg = a._bridge_dir / "registry"
    reg.mkdir()
    (reg / "imsg.yaml").write_text("name: imsg\ntarget_format: [email]\n", encoding="utf-8")
    a._reconcile_registry_sync()

    # imsg accepts email only; a bare name is not a valid target
    res = asyncio.run(a.send("imsg:alice", "Hallo"))
    assert res.success is False
    assert "target" in res.error.lower()
    assert "imsg" in res.error


def test_send_without_any_bridges_fails(tmp_path):
    a = _make_adapter(tmp_path)
    res = asyncio.run(a.send("anything", "Hallo"))
    assert res.success is False
