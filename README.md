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
├── inbox/<bridge>/       ← written by external service, read by adapter
├── outbox/<bridge>/      ← written by adapter, read by external service
├── status/<bridge>/      ← written by external service, read by adapter
└── media/
    ├── <bridge>/incoming/  ← incoming attachments (wrapper → adapter)
    └── <bridge>/outgoing/  ← outgoing attachments (adapter → wrapper)
```

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

Bridges are **auto-discovered** — the adapter scans `inbox/` for subdirectories and starts polling them automatically. No config changes needed.

To add a new bridge (e.g. Telegram):

```bash
# 1. Create the directory structure
mkdir -p <bridge_dir>/{inbox,outbox,status,media}/telegram

# 2. Write a wrapper script that:
#    - Reads outbox/telegram/*.json → sends via Telegram API
#    - Writes incoming messages to inbox/telegram/*.json
#    - Writes status to status/telegram/status.json

# 3. Start the wrapper
python3 telegram-wrapper.py
```

The adapter will pick it up on the next poll cycle (default: every 1 second).

### Directory structure with multiple bridges

```
<bridge_dir>/
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

See `imsg-wrapper.py` for a complete example.

## Requirements

- Hermes Agent (Gateway mode)
- Python 3.11+
- No additional dependencies (uses Hermes Gateway SDK)

## License

Apache 2.0
