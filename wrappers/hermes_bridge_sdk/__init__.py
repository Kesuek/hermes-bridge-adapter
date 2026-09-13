"""hermes_bridge_sdk — shared infrastructure for Bridge Adapter wrappers.

Extracted from imsg-wrapper / talk-wrapper (T-090). Wrappers provide only
platform-specific logic (build_inbox_msg, is_own, send, transport); the SDK
owns the bridge file contract: registry manifest, status heartbeat, last_seen
dedup state, inbox writer (atomic 0600), outbox loop (separator strip,
typing skip).

Contract (unchanged from the wrappers — adapter.py sees no difference):
    <bridge_dir>/registry/<name>.yaml   presence = registered, rm = unregistered
    <bridge_dir>/inbox/<name>/*.json    wrapper -> adapter
    <bridge_dir>/outbox/<name>/*.json   adapter -> wrapper
    <bridge_dir>/status/<name>/status.json
    <bridge_dir>/state/<name>/last_seen.json

Bridge-target separator is ``~`` (T-056); the SDK strips a leading
``<bridge>~`` / legacy ``<bridge>:`` prefix so wrappers stay agnostic.
"""

from .bridge_runner import BridgeRunner
from .files import (
    load_last_seen,
    save_last_seen,
    strip_bridge_prefix,
    write_inbox,
    write_inbox_private,
    write_manifest,
    write_private,
    write_status,
)
from .loops import drain_outbox_once, heartbeat_loop, outbox_loop

__all__ = [
    "BridgeRunner",
    "load_last_seen",
    "save_last_seen",
    "strip_bridge_prefix",
    "write_inbox",
    "write_inbox_private",
    "write_manifest",
    "write_private",
    "write_status",
    "drain_outbox_once",
    "outbox_loop",
    "heartbeat_loop",
]