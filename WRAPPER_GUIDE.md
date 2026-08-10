# Writing a Bridge Wrapper

A bridge wrapper is any script that connects a messaging platform (iMessage, Matrix, Telegram, WhatsApp, Nextcloud Talk, etc.) to the Hermes Bridge Adapter. It communicates **solely through JSON files** — no HTTP, no plugins, no special SDK.

## How It Works

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
│   ┌──────────────────────────────────────┐           │
│   │  Write status/<bridge>/status.json   │           │
│   └──────────────────────────────────────┘           │
└─────────────────────────────────────────────────────┘
```

## Directory Structure

Each bridge gets its own namespace under the bridge directory:

```
<bridge_dir>/
├── inbox/<bridge>/       ← You write incoming messages here
├── outbox/<bridge>/      ← Adapter writes outgoing messages here (you read)
├── status/<bridge>/      ← You write health status here
└── media/
    ├── <bridge>/incoming/  ← Incoming attachments (you copy here)
    └── <bridge>/outgoing/  ← Outgoing attachments (adapter copies here)
```

## Self-Registration (Registry)

A bridge registers itself by dropping a manifest into `registry/`. The
adapter polls `registry/` and reconciles at runtime:

- **Manifest present** → bridge registered; `inbox/`, `outbox/`, `status/`,
  `media/` directories are created automatically.
- **Manifest removed** (`rm registry/<bridge>.yaml`) → bridge deregistered;
  `status/`/`media/` are cleaned up.

```yaml
# registry/imsg.yaml
name: imsg
service: imessage
host: mac-mini-01
target_format: [email, phone, chat_id]   # which target shapes this bridge accepts
capabilities: [text, attachments, reactions]
```

The wrapper should write its manifest on startup and remove it on shutdown,
so the adapter registers/deregisters the bridge automatically.

## Wrapper Responsibilities

A wrapper must do four things:

### 1. Poll Outbox (read messages from Hermes)

Watch `outbox/<bridge>/` for new JSON files. When one appears:

```python
import json
from pathlib import Path

BRIDGE_DIR = Path("/path/to/bridge")
BRIDGE = "mybridge"

def poll_outbox():
    outbox_dir = BRIDGE_DIR / "outbox" / BRIDGE
    for f in sorted(outbox_dir.glob("*.json"), key=lambda p: p.stat().st_mtime):
        data = json.loads(f.read_text("utf-8"))
        send_via_platform(data)
        f.unlink(missing_ok=True)  # Delete after sending
```

**Outbox JSON format:**

```json
{
  "id": "out_abc123",
  "target": "user_or_chat_id",
  "text": "Hello from Hermes!",
  "attachments": [
    {
      "type": "image",
      "path": "media/mybridge/outgoing/photo.jpg",
      "caption": "Optional caption"
    }
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
| `target` | string | Chat ID or recipient (platform-specific) |
| `text` | string | Message text (may be empty if only attachment) |
| `attachments` | array | List of attachment objects (see below) |
| `typing` | bool | If true, show typing indicator (no text/attachments) |
| `reply_to` | string? | ID of message being replied to |
| `thread_id` | string? | Thread ID for threaded conversations |
| `metadata` | object | Platform-specific extras |

**Attachment object:**

```json
{
  "type": "image|video|audio|document",
  "path": "relative/path/in/bridge/dir",
  "caption": "Optional description"
}
```

### 2. Write Inbox (send messages to Hermes)

When a message arrives from the platform, write it as JSON to `inbox/<bridge>/`:

```python
import json
import uuid
from pathlib import Path

def write_inbox(sender, text, chat_id, chat_name="", attachments=None, reply_to=None, thread_id=None, thread_root=None):
    inbox_dir = BRIDGE_DIR / "inbox" / BRIDGE
    inbox_dir.mkdir(parents=True, exist_ok=True)

    msg = {
        "id": str(uuid.uuid4()),
        "type": "message",
        "sender": sender,
        "sender_name": sender,
        "text": text,
        "chat": {
            "id": chat_id,
            "type": "direct",       # "direct" or "group"
            "name": chat_name,
        },
        "attachments": attachments or [],
        "reply_to": reply_to,
        "thread_id": thread_id,
        "thread_root": thread_root,
    }

    path = inbox_dir / f"{msg['id']}.json"
    path.write_text(json.dumps(msg, ensure_ascii=False, indent=2), "utf-8")
```

**Inbox JSON format:**

```json
{
  "id": "msg_abc123",
  "type": "message",
  "sender": "user_42",
  "sender_name": "Alice",
  "text": "Hello Hermes!",
  "chat": {
    "id": "chat_99",
    "type": "direct",
    "name": "Alice"
  },
  "attachments": [
    {
      "type": "image",
      "path": "media/mybridge/incoming/photo.jpg",
      "mime": "image/jpeg"
    }
  ],
  "reply_to": {
    "id": "msg_001",
    "text": "Previous message"
  },
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

### 3. Handle Attachments

**Incoming** (platform → Hermes): Copy the file to `media/<bridge>/incoming/` and reference it with a relative path in the inbox JSON:

```python
import shutil

def handle_incoming_attachment(file_path):
    target = BRIDGE_DIR / "media" / BRIDGE / "incoming" / Path(file_path).name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(file_path, target)
    return {
        "type": "image",
        "path": str(target.relative_to(BRIDGE_DIR)),
        "mime": "image/jpeg",
    }
```

**Outgoing** (Hermes → platform): The adapter copies files to `media/<bridge>/outgoing/`. Your wrapper reads the relative path from the outbox JSON, resolves it, and sends the file via the platform API:

```python
def send_attachment(att):
    att_path = BRIDGE_DIR / att["path"]
    if att_path.exists():
        platform_send_file(chat_id, att_path)
```

### 4. Report Status

Write a status file so the adapter can monitor bridge health:

```python
import time

def write_status(connected, error=None):
    status_dir = BRIDGE_DIR / "status" / BRIDGE
    status_dir.mkdir(parents=True, exist_ok=True)
    status = {
        "bridge": BRIDGE,
        "connected": connected,
        "last_seen": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "error": error,
    }
    (status_dir / "status.json").write_text(
        json.dumps(status, indent=2), "utf-8"
    )
```

## Complete Minimal Wrapper

Here's a complete working wrapper skeleton:

```python
#!/usr/bin/env python3
"""Minimal bridge wrapper template."""
import json
import logging
import os
import time
import uuid
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mybridge-wrapper")

BRIDGE_DIR = Path(os.environ.get("BRIDGE_DIR", str(Path.home() / ".hermes" / "bridge")))
BRIDGE = "mybridge"
POLL_INTERVAL = 1.0


def send_via_platform(data):
    """Send a message using the platform's API."""
    target = data.get("target", "")
    text = data.get("text", "")
    attachments = data.get("attachments", [])

    # TODO: Implement platform-specific sending
    logger.info("Would send to %s: %.80s", target, text)

    for att in attachments:
        att_path = BRIDGE_DIR / att.get("path", "")
        if att_path.exists():
            logger.info("Would send attachment: %s", att_path)


def poll_outbox():
    """Poll outbox/ and send pending messages."""
    outbox_dir = BRIDGE_DIR / "outbox" / BRIDGE
    while True:
        try:
            for f in sorted(outbox_dir.glob("*.json"), key=lambda p: p.stat().st_mtime):
                try:
                    data = json.loads(f.read_text("utf-8"))
                except (json.JSONDecodeError, OSError):
                    f.unlink(missing_ok=True)
                    continue

                if data.get("typing"):
                    f.unlink(missing_ok=True)
                    continue

                send_via_platform(data)
                f.unlink(missing_ok=True)
        except Exception as e:
            logger.error("Outbox poll error: %s", e)
        time.sleep(POLL_INTERVAL)


def listen_for_messages():
    """Listen for incoming messages from the platform."""
    # TODO: Implement platform-specific message listening
    # When a message arrives, call write_inbox()
    pass


def write_inbox(sender, text, chat_id, chat_name="", attachments=None,
                reply_to=None, thread_id=None, thread_root=None):
    """Write an incoming message to inbox/<bridge>/."""
    inbox_dir = BRIDGE_DIR / "inbox" / BRIDGE
    inbox_dir.mkdir(parents=True, exist_ok=True)

    msg_id = str(uuid.uuid4())
    msg = {
        "id": msg_id,
        "type": "message",
        "sender": sender,
        "sender_name": sender,
        "text": text,
        "chat": {
            "id": chat_id,
            "type": "direct",
            "name": chat_name or chat_id,
        },
        "attachments": attachments or [],
        "reply_to": reply_to,
        "thread_id": thread_id,
        "thread_root": thread_root,
    }

    path = inbox_dir / f"{msg_id}.json"
    path.write_text(json.dumps(msg, ensure_ascii=False, indent=2), "utf-8")
    logger.debug("Wrote inbox: %s", path)


def write_status(connected, error=None):
    """Write bridge health status."""
    status_dir = BRIDGE_DIR / "status" / BRIDGE
    status_dir.mkdir(parents=True, exist_ok=True)
    status = {
        "bridge": BRIDGE,
        "connected": connected,
        "last_seen": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "error": error,
    }
    (status_dir / "status.json").write_text(
        json.dumps(status, indent=2), "utf-8"
    )


def main():
    logger.info("Wrapper starting — BRIDGE_DIR=%s, BRIDGE=%s", BRIDGE_DIR, BRIDGE)
    write_status(connected=True)

    import threading
    t = threading.Thread(target=poll_outbox, daemon=True, name="outbox-poller")
    t.start()

    try:
        listen_for_messages()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        write_status(connected=False, error="shutdown")


if __name__ == "__main__":
    main()
```

## Reactions

To support emoji reactions, write a reaction event to the inbox:

```python
def write_reaction(message_id, user_id, reaction, chat_id):
    inbox_dir = BRIDGE_DIR / "inbox" / BRIDGE
    msg = {
        "id": f"reac_{uuid.uuid4().hex[:8]}",
        "type": "reaction",
        "event": "reaction:added",       # or "reaction:removed"
        "reaction": reaction,             # e.g. "👍"
        "sender": user_id,
        "message_id": message_id,         # ID of the message being reacted to
        "chat": {"id": chat_id},
    }
    path = inbox_dir / f"{msg['id']}.json"
    path.write_text(json.dumps(msg, ensure_ascii=False, indent=2), "utf-8")
```

## Unified Threads (T-058)

A **Unified Thread** lets members on different bridges share one agent session. The adapter maps every member onto the same virtual thread (`chat_id="unified"`, `thread_id=<name>`), and a reply to `unified~<name>` is multicast to each member's own `outbox/<bridge>/`.

### Sending `/unified` commands

A `/unified` command is just an inbox JSON message whose `text` starts with `/unified`. The adapter intercepts it (it never reaches the agent) and writes the reply back to the sender's `outbox/<bridge>/`:

```python
def write_unified_command(sender, chat_id, command_text):
    write_inbox(
        sender=sender,
        text=command_text,            # e.g. "/unified create projekt"
        chat_id=chat_id,
        chat_type="direct",
    )
```

Available commands: `create <name>`, `status`, `join <name>`, `leave <name>`, `members <name>`, `mode <name> <mode>`, `protokoll open <name> [sitzung]`, `protokoll close <name>`, `help`.

### Delivering multicast replies

When the agent replies to `unified~projekt`, the adapter writes one outbox JSON **per member** to that member's own `outbox/<bridge>/`. Each wrapper only sees its own copy, addressed to its own chat:

```json
// outbox/imsg/<uuid>.json
{
  "id": "out_abc123",
  "bridge": "imsg",
  "target": "imsg~u1",
  "text": "Hallo alle",
  ...
}
```

```json
// outbox/talk/<uuid>.json
{
  "id": "out_def456",
  "bridge": "talk",
  "target": "talk~t1",
  "text": "Hallo alle",
  ...
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
write_inbox(sender=leader, text="/unified protokoll open projekt sitzung-2026-08-10",
            chat_id=leader_chat, chat_type="direct")

# ... members keep chatting normally; the adapter collects, the agent stays silent ...

# Leader closes the session (artifact written, mode reverts to participant)
write_inbox(sender=leader, text="/unified protokoll close projekt",
            chat_id=leader_chat, chat_type="direct")
```

The artifact lands at `<bridge_dir>/protokoll/projekt/sitzung-2026-08-10.md`. Only the leader may `open`/`close`; non-leader attempts get a rejection reply in their `outbox/`.

### Persistence

Threads are persisted in `<bridge_dir>/unified_threads.json` (loaded on adapter start, rewritten on every mutating command). Members are keyed by `{bridge}:{chat_id}` (the first address a person joined from); a merged member also carries an `addresses` array of its other bridge addresses (T-062). The wrapper does not need to read this file — it's purely adapter state. While a protokoll session is open the thread record also holds the live `protokoll` state; after `close` it reverts to `null`. A `_adaptive` block (T-061) tracks the per-thread state-machine state and message buffer.

### Reply-To-Ketten über Bridges (T-060)

The adapter maintains a persisted `gateway_msg_id → {bridge, local_msg_id}` map in `<bridge_dir>/reply_map.json`. The wrapper's role is unchanged: it still writes `reply_to` on inbox messages (its own bridge-local message id) and reads `reply_to` on outbox messages the same way. The adapter handles the cross-bridge translation:

- **Inbox** — the wrapper writes `reply_to: <local_msg_id>` as before. The adapter records the gateway-assigned `message_id` → `local_msg_id` mapping so a reply from a *different* bridge can resolve back to the original bridge's local id.
- **Outbox** — when the agent replies with a `reply_to` that is a gateway msg id (from a cross-bridge reply chain), the adapter resolves it to the destination bridge's local id before writing the outbox JSON. The wrapper only ever sees bridge-local ids in `reply_to`.

So the wrapper needs **no special logic** — keep writing/reading `reply_to` as the bridge-local id. The adapter transparently bridges the id space across bridges.

### Adaptive Zustandsmaschine (T-061)

In `participant` mode, the adapter watches message frequency per unified thread. When it exceeds a threshold (3 messages in 30s, or 5 in 60s), the thread flips to **digesting** and buffers incoming messages instead of dispatching them one-by-one. After a 60s `digest_interval`, the buffer is flushed as **one** `MessageEvent` whose text is:

```
[System: 7 messages from 2 users]
[10:42] [ronny] foo
[10:42] [anja] bar
...
```

From the wrapper's perspective nothing changes: the inbox messages it writes are still consumed as normal; the bundled turn appears in the agent's reply (if any) in `outbox/`. State and buffer persist in `unified_threads.json`, so a gateway restart doesn't lose the in-flight digest window. The wrapper does not need to know whether a thread is digesting.

### Member-Deduplizierung (T-062)

The adapter maintains a persisted identity map in `<bridge_dir>/identity_map.json`:

```json
{ "ronny": ["ronny.pietschke@icloud.com", "+491714824968", "ronny"] }
```

When the same person joins a unified thread from a second bridge, the adapter merges the new `{bridge}:{chat_id}` address into the existing member's `addresses` array instead of creating a duplicate member. The wrapper's role is unchanged — it keeps writing `/unified join` from each bridge as normal; the adapter dedups transparently. Multicast replies (`unified~<name>`) are delivered to every member address, including merged ones, so a person on two bridges receives the reply on both.

The identity map is **opt-in**: without an entry, a sender's canonical `person` equals its raw `user_id`, so two unrelated people with the same id on different bridges would be merged. Add explicit entries to declare which aliases belong together.

## Configuration via Environment Variables

All configuration should be done through environment variables so the wrapper works without hardcoded values:

| Variable | Default | Description |
|----------|---------|-------------|
| `BRIDGE_DIR` | `~/.hermes/bridge` | Path to the bridge directory |
| `BRIDGE_POLL_INTERVAL` | `1.0` | Outbox polling interval in seconds |
| `<BRIDGE>_*` | — | Bridge-specific config (API keys, endpoints, etc.) |

## Testing Your Wrapper

1. Register the bridge by writing its manifest (the adapter creates the
   directory structure automatically):
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
     > <bridge_dir>/inbox/mybiridge/test_1.json
   ```

4. Check that the adapter picks it up (look for "bridge-adapter" in gateway logs).

5. Simulate an outgoing message:
   ```bash
   echo '{"id":"out_test","target":"test_chat","text":"Reply from Hermes"}' \
     > <bridge_dir>/outbox/mybiridge/out_test.json
   ```

6. Check that your wrapper picks it up and sends it.

## Real-World Example

See `wrappers/imsg-wrapper.py` in this repository for a complete, production-ready wrapper that:

- Polls outbox via SSH to a remote macOS host
- Streams incoming messages via `imsg watch --json`
- Handles file attachments (images, documents)
- Reports connection status
- Runs as a systemd service
