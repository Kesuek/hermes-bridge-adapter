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