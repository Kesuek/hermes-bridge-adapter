"""Tests for the imsg-wrapper (T-075: SCP StrictHostKeyChecking).

The wrapper file ``wrappers/imsg-wrapper.py`` uses a hyphen in its filename
(the repo convention for wrappers — they're scripts, not importable modules),
so we load it via ``importlib`` against the absolute file path.

These tests focus on ``build_inbox_msg``'s SCP attachment pull: the SCP command
must NOT disable SSH host-key checking (T-075). The bug pre-fix was
``StrictHostKeyChecking=no`` at the single SCP call site in the wrapper.
"""
import importlib.util
import subprocess
import sys
from pathlib import Path

import threading
import time as _time

import pytest

ROOT = Path(__file__).resolve().parent.parent
WRAPPER_PATH = ROOT / "wrappers" / "imsg-wrapper.py"


def _load_wrapper_module():
    """Load ``wrappers/imsg-wrapper.py`` as a module (hyphen in filename)."""
    spec = importlib.util.spec_from_file_location(
        "imsg_wrapper_under_test", str(WRAPPER_PATH)
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["imsg_wrapper_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


# ── T-075: SCP must enforce StrictHostKeyChecking=yes ────────────────


def test_scp_pull_uses_strict_host_key_checking(monkeypatch, tmp_path):
    """T-075 fail-beweis: the SCP attachment pull must NOT use
    ``StrictHostKeyChecking=no`` and MUST use ``StrictHostKeyChecking=yes``.

    Pre-fix the wrapper builds the scp command with ``-o StrictHostKeyChecking=no``
    which opens an SSH-MITM window on every media download. The remote host is
    already in ``~/.ssh/known_hosts`` (the main SSH path ``ssh_run`` uses the
    OpenSSH default), so enforcing ``yes`` is a drop-in and fails closed if the
    host is ever unknown.
    """
    w = _load_wrapper_module()

    # Point the wrapper's globals at a temp bridge dir + a fake SSH host so the
    # SCP path is taken for an attachment whose src does not exist locally.
    bridge_dir = tmp_path / "bridge"
    bridge_dir.mkdir()
    monkeypatch.setattr(w, "BRIDGE_DIR", bridge_dir)
    monkeypatch.setattr(w, "SSH_HOST", "user@host")

    # Capture every subprocess.run call so we can inspect the scp argv.
    calls = []

    class _FakeResult:
        returncode = 0
        stderr = ""
        stdout = ""

    def fake_run(cmd, *a, **k):
        calls.append(list(cmd))
        return _FakeResult()

    # Patch subprocess.run on the wrapper's own subprocess reference (the
    # wrapper does `import subprocess` then calls `subprocess.run`).
    monkeypatch.setattr(w.subprocess, "run", fake_run)

    # An attachment whose original_path does not exist locally → triggers the
    # SCP branch in build_inbox_msg.
    raw = {
        "attachments": [
            {"original_path": "~/missing.jpg", "mime_type": "image/jpeg"}
        ],
        "chat_identifier": "u1",
        "sender": "u1",
        "id": "m1",
    }
    w.build_inbox_msg(raw)

    scp_calls = [c for c in calls if c and c[0] == "scp"]
    assert scp_calls, "scp should be invoked for a missing local attachment"
    joined = " ".join(scp_calls[0])

    assert "StrictHostKeyChecking=no" not in joined, (
        "SCP attachment pull must not disable host key checking (T-075); "
        f"got: {joined}"
    )
    assert "StrictHostKeyChecking=yes" in joined, (
        "SCP attachment pull must enforce StrictHostKeyChecking=yes (T-075); "
        f"got: {joined}"
    )

# ── T-091: watch loop must bump last_seen (history race) ────────────


def _patch_no_subprocess(monkeypatch):
    """Neutralise any subprocess call a stray code path might make."""
    class _FakeResult:
        returncode = 0
        stdout = ""
        stderr = ""

    def fake(*a, **k):
        return _FakeResult()

    def no_popen(*a, **k):
        raise AssertionError("no Popen expected")

    monkeypatch.setattr(subprocess, "run", fake)
    monkeypatch.setattr(subprocess, "Popen", no_popen)


def _load_w():
    return _load_wrapper_module()


def test_bump_last_seen_updates_state(monkeypatch, tmp_path):
    """T-091: _bump_last_seen must write the message id into last_seen.json
    so the history safety net skips already-delivered messages."""
    w = _load_w()
    monkeypatch.setattr(w, "STATE_FILE", tmp_path / "state" / "last_seen.json")
    _patch_no_subprocess(monkeypatch)

    w._bump_last_seen({"id": 3477, "chat_id": "42"})

    state = w.load_last_seen(w.STATE_FILE)
    assert state.get("42") == 3477


def test_bump_last_seen_monotonic(monkeypatch, tmp_path):
    """T-091: an older message id must never move last_seen backwards."""
    w = _load_w()
    monkeypatch.setattr(w, "STATE_FILE", tmp_path / "state" / "last_seen.json")
    _patch_no_subprocess(monkeypatch)

    w.save_last_seen({"42": 4000}, w.STATE_FILE)
    w._bump_last_seen({"id": 3477, "chat_id": "42"})

    state = w.load_last_seen(w.STATE_FILE)
    assert state.get("42") == 4000


def test_history_poll_skips_watch_seen_messages(monkeypatch, tmp_path):
    """T-091 fail-beweis: after the watch loop bumps last_seen, the history
    safety net must NOT re-write the same message to the inbox (the race
    that produced duplicate dispatches + reply_map entries).

    Reproduces the live pattern: watch delivered msg 3477 → adapter
    unlinked the inbox file → history poll re-delivered it 13 s later.
    """
    w = _load_w()
    monkeypatch.setattr(w, "STATE_FILE", tmp_path / "state" / "last_seen.json")
    _patch_no_subprocess(monkeypatch)

    # The watch loop has delivered msg 3477 and bumped last_seen.
    w._bump_last_seen({"id": 3477, "chat_id": "42"})

    # History poll sees the same message via `imsg chats` + `imsg history`.
    def fake_ssh_run(cmd, timeout=30):
        res = type("R", (), {})()
        res.returncode = 0
        if "imsg chats" in cmd:
            res.stdout = '{"id": "42", "identifier": "u1"}\n'
            res.stderr = ""
        else:  # imsg history
            res.stdout = '{"id": 3477, "sender": "u1", "text": "hello"}\n'
            res.stderr = ""
        return res

    monkeypatch.setattr(w, "ssh_run", fake_ssh_run)

    written = []
    monkeypatch.setattr(
        w, "write_inbox_private", lambda *a, **k: written.append(a)
    )

    last_seen = w.load_last_seen(w.STATE_FILE)
    last_seen = w.poll_history_once(last_seen)
    w.save_last_seen(last_seen, w.STATE_FILE)

    assert not written, (
        "history poll must not re-deliver a message the watch loop "
        f"already delivered (T-091); wrote: {written}"
    )


def test_history_loop_rereads_state_each_poll(monkeypatch, tmp_path):
    """T-091 (follow-up): history_loop must reload the state file before
    every poll — the watch loop bumps last_seen concurrently, and saving a
    stale local dict rolls the bump back (read-modify-write race that
    re-delivered msg 3481 in production)."""
    w = _load_w()
    state_file = tmp_path / "state" / "last_seen.json"
    monkeypatch.setattr(w, "STATE_FILE", state_file)
    _patch_no_subprocess(monkeypatch)

    # Watch loop bumps 3481 into the state file before the first poll runs.
    w.save_last_seen({"4": 3481}, w.STATE_FILE)
    seen_states = []

    def fake_poll(ls):
        seen_states.append(dict(ls))
        return ls

    monkeypatch.setattr(w, "poll_history_once", fake_poll)

    # Run exactly one iteration by raising after the first save.
    import threading as _t

    done = _t.Event()
    orig_save = w.save_last_seen

    def save_then_stop(state, path):
        orig_save(state, path)
        done.set()
        raise KeyboardInterrupt  # exit history_loop

    monkeypatch.setattr(w, "save_last_seen", save_then_stop)
    th = _t.Thread(target=w.history_loop, daemon=True)
    th.start()
    th.join(timeout=2)

    assert seen_states and seen_states[0].get("4") == 3481, (
        f"history_loop must re-read the state file before polling, saw: {seen_states}"
    )
