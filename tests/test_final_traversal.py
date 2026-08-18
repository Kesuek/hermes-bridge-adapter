"""Final-review verification: thread-name path traversal on protokoll close.

T-076 hardened the *sitzung name* via ``_safe_protokoll_name``, but the
thread *name* flows into ``_protokoll_dir(name)`` un-sanitized. A crafted
thread name like ``../../evil`` escapes ``bridge_dir/protokoll/`` when a
leader closes a protokoll session.
"""
import time

import pytest

from test_unified import _make_adapter


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


def test_protokoll_thread_name_traversal_rejected(tmp_path):
    """A thread NAME with ../ must NOT write the artifact outside protokoll/."""
    a = _make_adapter(tmp_path)
    a._bridges = ["imsg"]
    a._extra["allow_all"] = "true"

    bridge_dir = tmp_path / "bridge"

    # Create a thread whose NAME contains traversal (current: accepted).
    result = a._cmd_unified_create(
        "imsg", {"sender": "ronny", "chat": {"id": "c1"}}, "../../evil"
    )
    assert "../../evil" in a._unified_threads, f"create should register traversal name: {result}"

    # Open a protokoll session on that thread (leader-only).
    a._cmd_unified_protokoll_open(
        "imsg", {"sender": "ronny"}, "../../evil", "sitzung1"
    )
    # Append a message so close has something to write.
    thread = a._unified_threads["../../evil"]
    thread["protokoll"]["messages"].append(
        {"ts": time.time(), "sender": "ronny", "text": "hello"}
    )

    # Close the session — writes the artifact. Must NOT escape bridge_dir/protokoll.
    result = a._cmd_unified_protokoll_close("imsg", {"sender": "ronny"}, "../../evil")
    print("close result:", result)

    protokoll_root = bridge_dir / "protokoll"
    # Any file written must stay inside protokoll_root.
    evil_outside = bridge_dir / ".." / "evil"
    assert not evil_outside.exists(), f"traversal wrote outside protokoll/: {evil_outside}"
    for p in protokoll_root.rglob("*"):
        if p.is_file():
            assert p.resolve().is_relative_to(protokoll_root.resolve()), (
                f"artifact escaped protokoll dir: {p}"
            )
