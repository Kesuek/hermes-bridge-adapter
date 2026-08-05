# Hermes Bridge Adapter

Generic JSON-file-based bridge adapter for the [Hermes Agent](https://hermes-agent.nousresearch.com) Gateway.

Instead of each messaging platform (iMessage, Matrix, Telegram, WhatsApp, Signal) connecting directly to the Gateway, the Bridge Adapter provides a **shared JSON-file interface** — any external service can communicate with Hermes by reading/writing JSON files in a well-defined directory structure.

## How It Works

```
┌─────────────────┐     JSON files      ┌──────────────────┐
│  External        │  ┌──────────────┐   │  Hermes Gateway   │
│  Service         │──▶│  inbox/      │──▶│  Bridge Adapter   │
│  (imsg, Matrix,  │   │  <bridge>/   │   │  polls inbox/     │
│   Telegram, …)   │◀──│  outbox/     │◀──│  writes outbox/   │
│                  │   │  <bridge>/   │   │  dispatches       │
│                  │   │  status/     │   │  MessageEvents    │
│                  │   │  <bridge>/   │   │                   │
│                  │   │  media/      │   │                   │
└─────────────────┘  └──────────────┘   └──────────────────┘
```

## Directory Structure

```
<bridge_dir>/
├── registry/<bridge>.yaml ← bridge manifest (presence = registered)
├── inbox/<bridge>/       ← written by external service, read by adapter
├── outbox/<bridge>/      ← written by adapter, read by external service
├── status/<bridge>/      ← written by external service, read by adapter
└── media/
    ├── <bridge>/incoming/  ← incoming attachments (wrapper → adapter)
    └── <bridge>/outgoing/  ← outgoing attachments (adapter → wrapper)
```

## Bridge Self-Registration (Registry)

Bridges register with the adapter by dropping a manifest into `registry/`:

```yaml
# registry/imsg.yaml
name: imsg
service: imessage
host: mac-mini-01
target_format: [email, phone, chat_id]
capabilities: [text]
```

- **Presence of the manifest = the bridge is registered.** The adapter
  polls `registry/` every few seconds and reconciles:
  - a **new** manifest → the bridge is registered and its
    `inbox/`, `outbox/`, `status/`, `media/` directories are created;
  - the manifest is **removed** (`rm registry/imsg.yaml`) → the bridge is
    deregistered and its `status/`/`media/` directories are cleaned up.
- **No config edit needed to add a bridge** — just drop in a manifest (or
  remove it to take the bridge down). The adapter picks up changes at
  runtime without a restart.
- **`target_format`** declares which target shapes the bridge accepts
  (`email`, `phone`, `chat_id`). It is the basis for the routing check
  that decides whether a target is routable on a bridge.
- A bridge that is *registered* (manifest present) may still be
  *disconnected* (`status/<bridge>/status.json` says `connected: false`);
  the two are independent.

## JSON Schema

### Inbox (incoming message — wrapper → adapter)

```json
{
  "id": "msg_abc123",
  "type": "message",
  "timestamp": "2026-07-26T12:00:00Z",
  "from": {
    "id": "user_42",
    "name": "Alice",
    "platform": "imsg"
  },
  "chat": {
    "id": "chat_99",
    "name": "Project Chat"
  },
  "text": "Hello! Can you check the deployment status?",
  "thread_id": "thread_001",
  "thread_root": "msg_001",
  "attachments": [
    {
      "type": "image",
      "path": "media/imsg/incoming/screenshot.jpg",
      "mime": "image/jpeg"
    }
  ],
  "reply_to": {
    "id": "msg_001",
    "text": "Previous message"
  }
}
```

### Outbox (outgoing message — adapter → wrapper)

```json
{
  "id": "out_xyz789",
  "type": "message",
  "target": {
    "chat_id": "chat_99",
    "bridge": "imsg"
  },
  "text": "Sure, let me check that.",
  "thread_id": "thread_001",
  "attachments": [
    {
      "type": "image",
      "path": "media/imsg/outgoing/result.png",
      "mime": "image/png"
    }
  ],
  "typing": true,
  "reply_to": {
    "id": "msg_abc123"
  }
}
```

### Status (bridge health — wrapper → adapter)

```json
{
  "connected": true,
  "last_seen": "2026-07-26T12:00:00Z",
  "error": null,
  "version": "1.0.0"
}
```

## Features

- **Platform-agnostic** — any service that reads/writes JSON can be a bridge
- **Attachment support** — images, files, documents via shared media directory
- **Reactions** — 👍 reactions on messages
- **Typing indicators** — show when Hermes is typing
- **Reply chains** — reply_to preserves conversation context
- **Per-bridge config** — mention patterns, user allowlists, poll intervals
- **Auto-cleanup** — old media files and stale outbox entries are purged
- **Health monitoring** — status files polled every 60s, logged on disconnect
- **Multiple gateways** — works with parallel Hermes Gateway instances
- **Registry self-registration (T-050)** — bridges register via `registry/` manifests, picked up at runtime without a restart
- **Agent awareness (T-051)** — a system-prompt platform hint teaches the agent to read `registry/` and address messages as `<bridge>:<target>`; every inbound message carries a compact routing line (`[Message from <sender>, bridge <bridge>, reply to <bridge>:<target>]`)
- **Routing fallback (T-053)** — `send()` validates the target; unroutable targets (unknown bridge / wrong format) return a clear `SendResult` error instead of silently misrouting

## Installation

1. Install the plugin:
   ```bash
   cp -r bridge-adapter ~/.hermes/plugins/
   ```

2. Enable in `~/.hermes/config.yaml`:
   ```yaml
   plugins:
     enabled:
       - bridge-adapter
   gateway:
     platforms:
       bridge-adapter:
         enabled: true
   ```

3. Set environment:
   ```bash
   export BRIDGE_DIR=/home/user/.hermes/bridge
   ```

4. Restart the Gateway.

## Adding a New Bridge

Bridges **self-register** by dropping a manifest into `registry/`. The adapter polls `registry/` every few seconds and picks up new/removed manifests at runtime — no config change, no restart.

To add a new bridge (e.g. Telegram):

```bash
# 1. Write a registry manifest (this is the registration)
cat > <bridge_dir>/registry/telegram.yaml <<'EOF'
name: telegram
service: telegram
host: my-host
target_format: [chat_id]
capabilities: [text]
EOF

# 2. The adapter creates inbox/outbox/status/media/telegram automatically

# 3. Write a wrapper script that:
#    - Reads outbox/telegram/*.json → sends via Telegram API
#    - Writes incoming messages to inbox/telegram/*.json
#    - Writes status to status/telegram/status.json

# 4. Start the wrapper
python3 telegram-wrapper.py
```

To take a bridge down, remove its manifest (`rm registry/telegram.yaml`) — the adapter deregisters it and cleans up. See `imsg-wrapper.py` for a complete wrapper example.

### Directory structure with multiple bridges

```
<bridge_dir>/
├── registry/
│   ├── imsg.yaml          ← iMessage bridge manifest
│   └── telegram.yaml      ← Telegram bridge manifest
├── inbox/
│   ├── imsg/              ← iMessage wrapper writes here
│   └── telegram/          ← Telegram wrapper writes here
├── outbox/
│   ├── imsg/              ← Adapter writes iMessage replies here
│   └── telegram/          ← Adapter writes Telegram replies here
├── status/
│   ├── imsg/
│   └── telegram/
└── media/
    ├── imsg/
    └── telegram/
```

Each bridge is fully isolated — its own inbox, outbox, status, and media directories. Wrappers only touch their own namespace.

## Writing a Bridge Wrapper

A bridge wrapper is any script that:

1. **Reads** JSON files from `outbox/<bridge>/` (messages from Hermes)
2. **Sends** them via the platform's API (iMessage, Matrix, Telegram, etc.)
3. **Writes** incoming messages as JSON to `inbox/<bridge>/`
4. **Writes** status to `status/<bridge>/`

See `imsg-wrapper.py` for a complete example (watch stream with auto-reconnect, history safety net, and registry self-registration).

## Requirements

- Hermes Agent (Gateway mode)
- Python 3.11+
- No additional dependencies (uses Hermes Gateway SDK)

## License

MIT
