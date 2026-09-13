"""Loop-level helpers: outbox polling, status heartbeat.

Extracted from imsg-wrapper / talk-wrapper (T-090). Both wrappers ran the
same outbox loop (mtime-sorted glob, typing-skip, separator-strip, unlink
after send) and the same heartbeat thread pattern (T-052).
"""

import json
import logging
import time
from pathlib import Path

from .files import bridge_dir, strip_bridge_prefix

logger = logging.getLogger("hermes_bridge_sdk")


def drain_outbox_once(
    bridge: str,
    send,  # callable(target: str, text: str, attachments: list | None) -> None
    poll_interval: float = 1.0,
    once: bool = False,
    outbox_dir: Path | None = None,
) -> None:
    """Poll outbox/<bridge>/ and send pending messages via ``send``.

    Contract preserved from the original wrappers: invalid JSON is dropped,
    ``typing`` markers are consumed without sending, files are unlinked
    after send (at-least-once; the adapter tolerates re-delivery).
    ``once=True`` processes a single sweep and returns (used by tests and
    one-shot commands like test-wrapper drain).
    """
    outbox_dir = outbox_dir or bridge_dir() / "outbox" / bridge
    while True:
        try:
            for f in sorted(outbox_dir.glob("*.json"), key=lambda p: p.stat().st_mtime):
                try:
                    data = json.loads(f.read_text("utf-8"))
                except (json.JSONDecodeError, OSError) as e:
                    logger.warning("Invalid outbox JSON %s: %s", f, e)
                    f.unlink(missing_ok=True)
                    continue

                if data.get("typing"):
                    f.unlink(missing_ok=True)
                    continue

                target = strip_bridge_prefix(data.get("target", ""), bridge)
                text = data.get("text", "")
                try:
                    if target or text:
                        send(target, text, data.get("attachments"))
                    else:
                        logger.warning("Outbox %s: empty target+text, skipped", f.name)
                except Exception as e:
                    logger.error("Send failed for %s: %s", f.name, e)
                f.unlink(missing_ok=True)
        except Exception as e:
            logger.error("Outbox poll error: %s", e)
        if once:
            return
        time.sleep(poll_interval)


def outbox_loop(bridge: str, send, poll_interval: float = 1.0,
                outbox_dir: Path | None = None) -> None:
    """Backwards-compatible name for the infinite outbox loop."""
    drain_outbox_once(bridge, send, poll_interval, once=False, outbox_dir=outbox_dir)


def heartbeat_loop(bridge: str, interval: float = 60.0, stop_event=None) -> None:
    """Refresh status.json on a timer so last_seen stays fresh even when
    no messages arrive (T-052: a stream's read loop blocks on stdin, so the
    heartbeat needs its OWN thread)."""
    from .files import write_status

    while not (stop_event and stop_event.is_set()):
        write_status(bridge, connected=True)
        stop_event.wait(interval) if stop_event else time.sleep(interval)