# Writing a Bridge Wrapper

A bridge wrapper is any script that connects a messaging platform (iMessage, Matrix, Telegram, WhatsApp, Nextcloud Talk, etc.) to the Hermes Bridge Adapter. It communicates **solely through JSON files** — no HTTP, no plugins.

Most of the plumbing (registry manifest, status heartbeat, last_seen dedup, atomic inbox write, outbox loop) is provided by the `hermes_bridge_sdk` package — **start with the SDK** (section 1). The raw file contract is documented in the appendix for debugging and for SDK-free wrappers.

## Quick Overview

```
┌─────────────────────────────────────────────────────┐
│                   Bridge Wrapper                     │
│                                                      │
│   ┌──────────────┐     ┌──────────────────┐          │
│   │  Read outbox  │────▶│  Send via platform │          │
│   │  <bridge>/    │     │  API              │          │
│   └──────────────┘     └──────────────────┘          │
│                                                      │
│   ┌──────────────┐     ┌──────────────────┐          │
│   │  Receive from │────▶│  Write inbox      │          │
│   │  platform API │     │  <bridge>/        │          │
│   └──────────────┘     └──────────────────┘          │
│                                                      │
│   (manifest/status/heartbeat: SDK handles this)      │
└─────────────────────────────────────────────────────┘
```

Each bridge gets its own namespace under the bridge directory:

```
<bridge_dir>/
├── registry/<bridge>.yaml  ← SDK writes this (manifest = registered)
├── inbox/<bridge>/         ← You write incoming messages here
├── outbox/<bridge>/        ← Adapter writes outgoing messages here (you read)
├── status/<bridge>/        ← SDK writes the health heartbeat here
├── state/<bridge>/         ← SDK stores last_seen dedup state here
└── media/
    ├── <bridge>/incoming/  ← Incoming attachments (you copy here)
    └── <bridge>/outgoing/  ← Outgoing attachments (adapter copies here)
```

## 1. Writing a Wrapper with the SDK

### Your four hooks

A wrapper implements only platform-specific logic and hands it to `BridgeRunner`:

| Hook | Signature | Purpose |
|---|---|---|
| `send` | `(target, text, attachments) -> None` | Deliver an outbox message via the platform API |
| `build_inbox_msg` | platform-native event → inbox dict | Map platform fields to the adapter's inbox JSON (see appendix for the schema) |
| `is_own` | platform-native event → bool | Detect self-echo / system messages that must not be mirrored |
| inbound loop | a `Thread` (via `extra_threads`) | Poll or stream the platform; call `write_inbox_private()` per message |

### Complete minimal wrapper

```python
#!/usr/bin/env python3
"""Minimal SDK-based bridge wrapper (~40 lines of real logic)."""
import json, logging, os, sys, threading, time, uuid
from pathlib import Path

BRIDGE = "mybridge"
sys.path.insert(0, str(Path(__file__).resolve().parent))  # SDK lives in wrappers/

from hermes_bridge_sdk import BridgeRunner, load_last_seen, save_last_seen, write_inbox_private

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("mybridge-wrapper")

STATE_FILE = Path(os.environ.get("BRIDGE_DIR", str(Path.home() / ".hermes" / "bridge"))) / "state" / BRIDGE / "last_seen.json"

def send(target: str, text: str, attachments) -> None:
    # TODO: deliver via the platform's API; attachments carry paths relative
    # to the bridge dir (see appendix). The SDK already stripped any
    # "mybridge~" prefix from target and consumed typing markers.
    log.info("Would send to %s: %s", target, text[:80])

def poll_platform(state: dict) -> dict:
    """Poll the platform once; write new messages; return updated last_seen."""
    last = state.get("cursor", 0)
    for raw in platform_fetch_since(last):          # TODO: platform API
        if is_own(raw):
            continue
        write_inbox_private(BRIDGE, {
            "id": str(raw["id"]),
            "type": "message",
            "sender": raw["sender"],
            "sender_name": raw.get("name", ""),
            "text": raw["text"],
            "chat": {"id": raw["chat_id"], "type": "direct", "name": raw.get("chat_name", "")},
            "attachments": [],
            "reply_to": None,
        })
        last = max(last, raw["id"])
    state["cursor"] = last
    return state

def is_own(raw: dict) -> bool:
    return raw["sender"] == PLATFORM_SELF_USER      # TODO

def inbound_loop():
    # T-091 lesson: reload state before EVERY poll — a stale in-memory dict
    # rolls back updates from other loops (watch/history races).
    while True:
        try:
            save_last_seen(poll_platform(load_last_seen(STATE_FILE)), STATE_FILE)
        except Exception as e:
            log.error("Inbound poll error: %s", e)
        time.sleep(float(os.environ.get("BRIDGE_POLL_INTERVAL", "1.0")))

if __name__ == "__main__":
    BridgeRunner(
        bridge=BRIDGE,
        service="mybridge-service",
        host="my-host.example.com",
        target_format=["chat_id"],
        capabilities=["text"],
        send=send,
        extra_threads=[lambda: threading.Thread(target=inbound_loop, daemon=True)],
        poll_interval=float(os.environ.get("BRIDGE_POLL_INTERVAL", "1.0")),
    ).main()   # registers manifest, starts outbox+heartbeat+your threads, blocks, cleans up on SIGINT/SIGTERM
```

That's the whole wrapper. The runner registers the manifest on startup (adapter creates the directory tree), writes the status heartbeat every 60 s on its own thread, drains the outbox (mtime-sorted, typing-skip, `~`-prefix strip, unlink after send), and unregisters cleanly on shutdown.

### SDK helper reference

```python
from hermes_bridge_sdk import (
    BridgeRunner, write_manifest, unregister_manifest, write_status,
    load_last_seen, save_last_seen, write_inbox, write_inbox_private,
    strip_bridge_prefix, drain_outbox_once, outbox_loop, heartbeat_loop,
)
```

| Helper | What it does |
|---|---|
| `BridgeRunner(...).main()` | Lifecycle owner: manifest register/unregister, status heartbeat thread, outbox poll thread, your `extra_threads`, signal handling |
| `write_manifest(bridge, service, host, target_format, capabilities)` | Registry self-registration → `registry/<bridge>.yaml` (0600) |
| `write_status(bridge, connected, error=None)` | Health heartbeat payload → `status/<bridge>/status.json` |
| `heartbeat_loop(bridge, interval=60.0)` | Status refresh on its own thread (T-052: a stream's read loop blocks stdin, so the heartbeat needs its own thread) |
| `load_last_seen(file)` / `save_last_seen(state, file)` | last_seen dedup state — **always reload before each poll** (T-091 RMW race: a stale dict rolls back other loops' bumps → duplicate deliveries) |
| `write_inbox_private(bridge, data)` / `write_inbox` | Atomic inbox write with `0600` perms (T-071); `write_inbox` is the backwards-compatible alias |
| `strip_bridge_prefix(target, bridge)` | Strips `mybridge~` (T-056) or legacy `mybridge:` so the wrapper stays agnostic of addressing |
| `drain_outbox_once(bridge, send, interval, once=)` / `outbox_loop` | Outbox polling: mtime-sorted glob, invalid-JSON drop, typing-skip, send via your `send`, unlink after (at-least-once) |

Configuration via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BRIDGE_DIR` | `~/.hermes/bridge` | Path to the bridge directory |
| `BRIDGE_POLL_INTERVAL` | `1.0` | Outbox/inbound polling interval in seconds |
| `<BRIDGE>_*` | — | Bridge-specific config (API keys, endpoints, etc.) |

### Real-world examples

- `wrappers/talk-wrapper.py` — cleanest SDK example: pure HTTP polling (no watch stream), platform logic is only `_api()` + field mapping + `is_own`; everything else is SDK.
- `wrappers/imsg-wrapper.py` — production wrapper: SSH transport to a remote macOS host, `imsg watch --json` stream, attachment handling, systemd service.

## 2. Reactions

To support emoji reactions, write a reaction event to the inbox (via `write_inbox_private`):

```python
write_inbox_private(BRIDGE, {
    "id": f"reac_{uuid.uuid4().hex[:8]}",
    "type": "reaction",
    "event": "reaction:added",       # or "reaction:removed"
    "reaction": "👍",
    "sender": user_id,
    "message_id": message_id,        # ID of the message being reacted to
    "chat": {"id": chat_id},
})
```

## 3. Unified Threads (T-058)

A **Unified Thread** lets members on different bridges share one agent session. The adapter maps every member onto the same virtual thread (`chat_id="unified"`, `thread_id=<name>`), and a reply to `unified~<name>` is multicast to each member's own `outbox/<bridge>/`.

### Sending `/unified` commands

A `/unified` command is just an inbox JSON message whose `text` starts with `/unified`. The adapter intercepts it (it never reaches the agent) and writes the reply back to the sender's `outbox/<bridge>/`:

```python
write_inbox_private(BRIDGE, {
    "id": str(uuid.uuid4()), "type": "message", "sender": sender,
    "text": command_text,            # e.g. "/unified create projekt"
    "chat": {"id": chat_id, "type": "direct", "name": sender},
})
```

Available commands: `create <name>`, `status`, `join <name>`, `leave <name>`, `members <name>`, `mode <name> <mode>`, `switch <name>`, `send <name> <message>`, `protokoll open <name> [sitzung]`, `protokoll close <name>`, `help`.

### Delivering multicast replies

When the agent replies to `unified~projekt`, the adapter writes one outbox JSON **per member** to that member's own `outbox/<bridge>/`. Each wrapper only sees its own copy, addressed to its own chat:

```json
// outbox/imsg/<uuid>.json
{
  "id": "out_abc123",
  "bridge": "imsg",
  "target": "imsg~u1",
  "text": "Hallo alle"
}
```

```json
// outbox/talk/<uuid>.json
{
  "id": "out_def456",
  "bridge": "talk",
  "target": "talk~t1",
  "text": "Hallo alle"
}
```

So a wrapper delivering multicast replies needs **no special logic** — it keeps polling its own `outbox/<bridge>/` and delivering whatever appears there; the adapter handles the fan-out.

### Teilnehmer-Modi (T-059)

Each unified thread has a `mode` that controls how the adapter dispatches incoming member messages. The wrapper doesn't need to know the mode — it keeps writing to `inbox/` and reading from `outbox/` as usual; the adapter applies the mode before the agent is called.

| Mode | What the wrapper sees | What the agent does |
|------|----------------------|---------------------|
| `participant` (default) | Normal: inbox message → agent reply in `outbox/`. The agent decides whether to reply; it emits the literal token `NO_REPLY` (suppressed by the gateway) when it has nothing to say. | Decides per message |
| `reactive` | Only messages that mention the agent (`@hermes` or a bridge `mention_patterns` match) reach the agent. Un-mentioned messages are dropped by the adapter (no `outbox/` reply). | Replies only when mentioned |
| `off` | No `outbox/` reply is ever produced — the adapter drops every message. The agent gets no context. | Never replies, no context |
| `silent` | Mute switch: every message is buffered and flushed periodically as one digest turn, so the agent reads along but never replies. The digest is marked `[Silent digest — read only, do not reply]`. | Reads along, never replies |
| `protokoll` | No `outbox/` reply while a session is open — messages are collected into the adapter's `protokoll` state. On `close`, the adapter writes a Markdown artifact to `<bridge_dir>/protokoll/<thread>/<sitzung>.md`. | Does not reply while the session is open |

The thread creator (`created_by`) is the thread's **leader**. The routing-context line the adapter appends to every unified-thread message marks the leader as `[<Name> Leader]`, so a wrapper that surfaces raw text to the user sees no extra difference — the marker is only visible to the agent.

#### `/unified protokoll` lifecycle

`protokoll` is a leader-only lifecycle for capturing a thread's conversation as an artifact (e.g. a meeting protocol). The wrapper writes the commands like any other `/unified` command; the adapter handles the rest:

```python
# Leader opens a session (mode switches to protokoll, messages start being collected)
write_inbox_private(BRIDGE, {"id": str(uuid.uuid4()), "type": "message", "sender": leader,
                             "text": "/unified protokoll open projekt sitzung-2026-08-10",
                             "chat": {"id": leader_chat, "type": "direct"}})

# ... members keep chatting normally; the adapter collects, the agent stays silent ...

# Leader closes the session (artifact written, mode reverts to participant)
write_inbox_private(BRIDGE, {"id": str(uuid.uuid4()), "type": "message", "sender": leader,
                             "text": "/unified protokoll close projekt",
                             "chat": {"id": leader_chat, "type": "direct"}})
```

The artifact lands at `<bridge_dir>/protokoll/projekt/sitzung-2026-08-10.md`. Only the leader may `open`/`close`; non-leader attempts get a rejection reply in their `outbox/`.

### Persistence

Threads are persisted in `<bridge_dir>/unified_threads.json` (loaded on adapter start, rewritten on every mutating command). Members are keyed by `{bridge}:{chat_id}` (the first address a person joined from); a merged member also carries an `addresses` array of its other bridge addresses (T-062). The wrapper does not need to read this file — it's purely adapter state. While a protokoll session is open the thread record also holds the live `protokoll` state; after `close` it reverts to `null`. A `_adaptive` block (T-061) tracks the per-thread state-machine state and message buffer.

### Reply-To Chains Across Bridges (T-060)

The adapter maintains a persisted `gateway_msg_id → {bridge, local_msg_id}` map in `<bridge_dir>/reply_map.json`. The wrapper's role is unchanged: it still writes `reply_to` on inbox messages (its own bridge-local message id) and reads `reply_to` on outbox messages the same way. The adapter handles the cross-bridge translation:

- **Inbox** — the wrapper writes `reply_to: <local_msg_id>` as before. The adapter records the gateway-assigned `message_id` → `local_msg_id` mapping so a reply from a *different* bridge can resolve back to the original bridge's local id.
- **Outbox** — when the agent replies with a `reply_to` that is a gateway msg id (from a cross-bridge reply chain), the adapter resolves it to the destination bridge's local id before writing the outbox JSON. The wrapper only ever sees bridge-local ids in `reply_to`.

So the wrapper needs **no special logic** — keep writing/reading `reply_to` as the bridge-local id. The adapter transparently bridges the id space across bridges.

### Adaptive State Machine (T-061)

In `participant` mode, the adapter watches message frequency per unified thread. When it exceeds a threshold (3 messages in 30s, or 5 in 60s), the thread flips to **digesting** and buffers incoming messages instead of dispatching them one-by-one. After a 60s `digest_interval`, the buffer is flushed as **one** `MessageEvent` whose text is:

```
[System: 7 messages from 2 users]
[10:42] [alice] foo
[10:42] [bob] bar
...
```

From the wrapper's perspective nothing changes: the inbox messages it writes are still consumed as normal; the bundled turn appears in the agent's reply (if any) in `outbox/`. State and buffer persist in `unified_threads.json`, so a gateway restart doesn't lose the in-flight digest window. The wrapper does not need to know whether a thread is digesting.

### Member Deduplication (T-062)

The adapter maintains a persisted identity map in `<bridge_dir>/identity_map.json`:

```json
{ "alice": ["alice@example.com", "+49 170 1234567", "alice"] }
```

When the same person joins a unified thread from a second bridge, the adapter merges the new `{bridge}:{chat_id}` address into the existing member's `addresses` array instead of creating a duplicate member. The wrapper's role is unchanged — it keeps writing `/unified join` from each bridge as normal; the adapter dedups transparently. Multicast replies (`unified~<name>`) are delivered to every member address, including merged ones, so a person on two bridges receives the reply on both.

The identity map is **opt-in**: without an entry, a sender's canonical `person` equals its raw `user_id`, so two unrelated people with the same id on different bridges would be merged. Add explicit entries to declare which aliases belong together.

## Appendix: Raw File Contract

For debugging, SDK-free wrappers, and anyone who wants to know what the SDK writes on their behalf.

### Self-Registration (Registry)

A bridge registers itself by dropping a manifest into `registry/`. The adapter polls `registry/` and reconciles at runtime:

- **Manifest present** → bridge registered; `inbox/`, `outbox/`, `status/`, `media/` directories are created automatically.
- **Manifest removed** (`rm registry/<bridge>.yaml`) → bridge deregistered; `status/`/`media/` are cleaned up.

```yaml
# registry/imsg.yaml
name: imsg
service: imessage
host: mac-mini-01
target_format: [email, phone, chat_id]   # which target shapes this bridge accepts
capabilities: [text, attachments, reactions]
```

### Outbox JSON format (adapter → wrapper)

```json
{
  "id": "out_abc123",
  "target": "user_or_chat_id",
  "text": "Hello from Hermes!",
  "attachments": [
    { "type": "image", "path": "media/mybridge/outgoing/photo.jpg", "caption": "Optional caption" }
  ],
  "typing": false,
  "reply_to": "msg_001",
  "thread_id": "thread_001",
  "metadata": {}
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique message ID |
| `target` | string | Chat ID or recipient; may carry a `mybridge~` prefix (use `strip_bridge_prefix`) |
| `text` | string | Message text (may be empty if only attachment) |
| `attachments` | array | List of attachment objects (see below) |
| `typing` | bool | If true, show typing indicator (no text/attachments) — consume without sending |
| `reply_to` | string? | ID of message being replied to (bridge-local) |
| `thread_id` | string? | Thread ID for threaded conversations |
| `metadata` | object | Platform-specific extras |

### Inbox JSON format (wrapper → adapter)

```json
{
  "id": "msg_abc123",
  "type": "message",
  "sender": "user_42",
  "sender_name": "Alice",
  "text": "Hello Hermes!",
  "chat": { "id": "chat_99", "type": "direct", "name": "Alice" },
  "attachments": [
    { "type": "image", "path": "media/mybridge/incoming/photo.jpg", "mime": "image/jpeg" }
  ],
  "reply_to": { "id": "msg_001", "text": "Previous message" },
  "thread_id": "thread_001",
  "thread_root": "msg_001"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | ✅ | Unique message ID |
| `type` | string | ✅ | `"message"` or `"reaction"` |
| `sender` | string | ✅ | User ID (used for routing) |
| `sender_name` | string | | Display name |
| `text` | string | | Message text |
| `chat.id` | string | ✅ | **Raw chat identity** (no bridge prefix), e.g. `"chat_99"` |
| `chat.type` | string | | `"direct"` (default) or `"group"` |
| `chat.name` | string | | Human-readable chat name |
| `attachments` | array | | List of attachment objects |
| `reply_to` | object | | `{ "id": "...", "text": "..." }` |
| `thread_id` | string | | Thread identifier |
| `thread_root` | string | | Root message ID of the thread |

**⚠️ Important: The `chat.id` must be the RAW chat identity** (e.g. `"chat_99"`), **without** a bridge prefix. The adapter builds the full routable reply address (`<bridge>~<target>`) itself. If you include a prefix, the adapter would double-prefix it and replies would fail to route. The wrapper stays agnostic of the addressing convention.

### Attachments

**Incoming** (platform → Hermes): copy the file to `media/<bridge>/incoming/` and reference it with a path relative to the bridge dir in the inbox JSON (`{"type": "image", "path": "media/mybridge/incoming/photo.jpg", "mime": "image/jpeg"}`).

**Outgoing** (Hermes → platform): the adapter copies files to `media/<bridge>/outgoing/`. Your wrapper reads the relative `path` from the outbox JSON, resolves it against the bridge dir, and sends the file via the platform API.

### Testing Your Wrapper

1. Register the bridge by writing its manifest (the adapter creates the directory structure automatically — the SDK's `BridgeRunner` does this for you):
   ```bash
   cat > <bridge_dir>/registry/mybridge.yaml <<'EOF'
   name: mybridge
   service: mybridge
   target_format: [chat_id]
   capabilities: [text]
   EOF
   ```

2. Start your wrapper:
   ```bash
   python3 mybridge-wrapper.py
   ```

3. Simulate an incoming message:
   ```bash
   echo '{"id":"test_1","type":"message","sender":"test_user","text":"Hello!","chat":{"id":"test_chat","type":"direct"}}' \
     > <bridge_dir>/inbox/mybridge/test_1.json
   ```

4. Check that the adapter picks it up (look for "bridge-adapter" in gateway logs).

5. Simulate an outgoing message:
   ```bash
   echo '{"id":"out_test","target":"test_chat","text":"Reply from Hermes"}' \
     > <bridge_dir>/outbox/mybridge/out_test.json
   ```

6. Check that your wrapper picks it up and sends it.