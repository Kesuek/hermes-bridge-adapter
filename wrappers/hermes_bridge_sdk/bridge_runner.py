"""BridgeRunner — assembles manifest, status, inbox/outbox loops for a bridge.

Extracted from imsg-wrapper / talk-wrapper (T-090). A wrapper provides only
platform hooks; the runner owns the lifecycle:

    runner = BridgeRunner(
        bridge="talk",
        service="nextcloud-talk",
        host="your-nextcloud.example.com",
        target_format=["chat_id"],
        capabilities=["text"],
        send=lambda target, text, attachments: nc_send(target, text),
        inbound=lambda: threading.Thread(target=inbound_loop, daemon=True),
        poll_interval=5.0,
    )
    runner.main()   # registers manifest, starts threads, blocks, cleans up
"""

import logging
import signal
import threading
import time

from .files import (
    unregister_manifest,
    write_manifest,
    write_status,
)
from .loops import drain_outbox_once

logger = logging.getLogger("hermes_bridge_sdk")


class BridgeRunner:
    """Lifecycle owner for one bridge wrapper."""

    def __init__(
        self,
        bridge: str,
        service: str,
        host: str,
        target_format: list,
        capabilities: list,
        send,  # (target: str, text: str, attachments: list | None) -> None
        extra_threads: list | None = None,  # callables returning a Thread
        poll_interval: float = 1.0,
        heartbeat_interval: float = 60.0,
        logger_name: str | None = None,
    ):
        self.bridge = bridge
        self.service = service
        self.host = host
        self.target_format = target_format
        self.capabilities = capabilities
        self.send = send
        self.extra_threads = extra_threads or []
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self._stop = threading.Event()
        self.log = logging.getLogger(logger_name or f"{bridge}-wrapper")

    # ── Lifecycle ───────────────────────────────────────────────────

    def register(self) -> None:
        write_manifest(
            self.bridge, self.service, self.host,
            self.target_format, self.capabilities,
        )

    def unregister(self) -> None:
        unregister_manifest(self.bridge)

    def start(self) -> list:
        """Register manifest, write initial status, start threads. Returns threads."""
        self.register()
        write_status(self.bridge, connected=True)

        from .loops import heartbeat_loop

        threads = [t() for t in self.extra_threads]
        threads.append(threading.Thread(
            target=heartbeat_loop,
            args=(self.bridge,),
            kwargs={"interval": self.heartbeat_interval},
            daemon=True,
            name=f"{self.bridge}-heartbeat",
        ))
        outbox = threading.Thread(
            target=drain_outbox_once,
            args=(self.bridge, self.send, self.poll_interval),
            kwargs={"once": False},
            daemon=True,
            name=f"{self.bridge}-outbox",
        )
        threads.append(outbox)
        for t in threads:
            t.start()
            self.log.info("Started thread: %s", t.name)
        return threads

    def wait(self) -> None:
        """Block until SIGINT/SIGTERM, then stop cleanly."""
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_signal)
        while not self._stop.is_set():
            time.sleep(1.0)

    def _handle_signal(self, signum, frame) -> None:
        self.log.info("Signal %s — shutting down...", signum)
        self._stop.set()

    def main(self) -> None:
        self.log.info(
            "%s starting — poll_interval=%.1fs", self.bridge, self.poll_interval
        )
        self.start()
        self.wait()
        write_status(self.bridge, connected=False, error="shutdown")
        self.unregister()
        self.log.info("%s stopped", self.bridge)