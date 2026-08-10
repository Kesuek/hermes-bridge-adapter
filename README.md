# Hermes Bridge Adapter

Generic JSON-file-based bridge adapter for the [Hermes Agent](https://hermes-agent.nousresearch.com) Gateway.

Instead of each messaging platform connecting directly to the Gateway, the Bridge Adapter provides a **shared JSON-file interface** — any external service can communicate with Hermes by reading/writing JSON files in a well-defined directory structure.

**Terminology:** an **External Service** is the messaging platform itself (your messaging service, a chatbot, …). An **External Service Wrapper** (or just *wrapper*) is the script that binds that platform to the JSON-file contract — it reads `outbox/`, writes `inbox/` and `status/`, and copies attachments. The adapter never talks to the platform directly; it only exchanges JSON files with the wrapper.

## How It Works

```
┌─────────────────────┐     JSON files      ┌──────────────────┐
│  External Service   │  ┌──────────────┐   │  Hermes Gateway   │
│  Wrapper            │──▶│  inbox/      │──▶│  Bridge Adapter   │
│  (your messaging    │   │  <bridge>/   │   │  polls inbox/     │
│   service, chatbot) │◀──│  outbox/     │◀──│  writes outbox/   │
│                     │   │  <bridge>/   │   │  dispatches       │
│                     │   │  status/     │   │  MessageEvents    │
│                     │   │  <bridge>/   │   │                   │
│                     │   │  media/      │   │                   │
└─────────────────────┘  └──────────────┘   └──────────────────┘
```

## Directory Structure

```
<bridge_dir>/
├── registry/<bridge>.yaml ← bridge manifest (presence = registered)
├── unified_threads.json   ← persisted unified threads (T-058)
├── reply_map.json        ← gateway→local message-id map for cross-bridge reply chains (T-060)
├── identity_map.json     ← alias→person map for member dedup (T-062)
├── active_threads.json   ← per-person active thread (T-064)
├── pending_claims.json   ← in-flight identity-claim challenges (T-065)
├── usernames.json        ← per-person display name (T-065)
├── protokoll/<thread>/   ← rendered protokoll artifacts (T-059)
├── inbox/<bridge>/       ← written by external service wrapper, read by adapter
├── outbox/<bridge>/      ← written by adapter, read by external service wrapper
├── status/<bridge>/      ← written by external service wrapper, read by adapter
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
- **Agent awareness (T-051)** — a system-prompt platform hint teaches the agent to read `registry/` and address messages as `<bridge>~<target>`; every inbound message carries a compact routing line (`[Message from <sender>, bridge <bridge>, reply to <bridge>~<target>]`)
- **Routing fallback (T-053)** — `send()` validates the target; unroutable targets (unknown bridge / wrong format) return a clear `SendResult` error instead of silently misrouting
- **Unified Threads (T-058–T-066) — the differentiator.** Because every bridge is wired to the adapter through the same JSON-file contract, multiple bridges can share a **single agent session** — one conversation across iMessage, Talk, and any other wrapper. Native gateway adapters are isolated from each other; the Bridge Adapter turns them into one thread. `/unified` commands, 5 participant modes, cross-bridge reply chains, adaptive bundling, member dedup, **message relay** that mirrors every inbound message to the other member bridges so all humans see the full conversation, plus **active-thread switching**, **one-shot send** (T-064), **identity-claim challenge-response** to authorize a user-mapping across bridges (T-065), and **unified handles** (T-066) that decouple the on-thread identity from the raw bridge identity (`unified~<username>`, agent handle from config). **→ [Full docs: `UNIFIED_THREADS.md`](UNIFIED_THREADS.md)**

## Unified Threads (T-058–T-066)

**The Bridge Adapter's differentiator.** Because every bridge is wired to the adapter through the same JSON-file contract, multiple bridges can share a **single agent session** — one conversation across iMessage, Talk, and any other wrapper. Native gateway adapters are isolated from each other (each platform has its own session); the Bridge Adapter turns them into **one thread across all your messaging worlds**.

This includes:
- **`/unified` commands** — `create`/`join`/`leave`/`members`/`status`/`mode`/`switch`/`send`/`identity`/`set username`/`protokoll`/`help`
- **5 participant modes** — `participant` / `reactive` / `off` / `silent` / `protokoll`
- **Active thread (T-064)** — `/unified switch <name>` sets your active thread; your messages route there even from a bridge you joined via another address
- **One-shot send (T-064)** — `/unified send <name> <message>` multicasts to a thread without switching
- **Identity-claim (T-065)** — `/unified identity claim <bridge>~<target>` + `/unified identity confirm <code>` authorize a user-mapping via challenge-response: a code is sent to the target bridge, and only someone who controls both accounts can confirm. Merges the identities in `identity_map.json`. Plus `/unified set username <name>` for a display name shown in `status`.
- **Unified handles (T-066)** — every participant is shown by a unified handle, not a raw bridge identity: users resolve to `unified~<username>` (fallback `unified~<user_id>`), and the agent resolves to a configurable handle (`extra["agent_handle"]` / `BRIDGE_AGENT_HANDLE`, default `hermes`). Relay messages strip the `unified~` prefix for display (`[Kesuek] text`).
- **Message relay (T-063)** — every inbound message is mirrored to the outbox of the other member bridges as `[Name] text`, so all humans see the full conversation across messengers. Loop-safe (outbox-only), runs in all modes, the agent still gets the original.
- **Cross-bridge reply chains** — `gateway_msg_id → {bridge, local_msg_id}` map
- **Adaptive bundling** — `idle → active → digesting` under high frequency
- **Member deduplication** — the same person on two bridges = one member; identity map records which wrapper each alias belongs to

**→ [Full docs: `UNIFIED_THREADS.md`](UNIFIED_THREADS.md)**

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

To add a new bridge (e.g. your messaging service):

```bash
# 1. Write a registry manifest (this is the registration)
cat > <bridge_dir>/registry/myservice.yaml <<'EOF'
name: myservice
service: my-messaging-service
host: my-host
target_format: [chat_id]
capabilities: [text]
EOF

# 2. The adapter creates inbox/outbox/status/media/myservice automatically

# 3. Write a wrapper script that:
#    - Reads outbox/myservice/*.json → sends via your service's API
#    - Writes incoming messages to inbox/myservice/*.json
#    - Writes status to status/myservice/status.json

# 4. Start the wrapper
python3 myservice-wrapper.py
```

To take a bridge down, remove its manifest (`rm registry/myservice.yaml`) — the adapter deregisters it and cleans up. See `wrappers/imsg-wrapper.py` for a complete wrapper example.

### Directory structure with multiple bridges

```
<bridge_dir>/
├── registry/
│   ├── imsg.yaml          ← iMessage bridge manifest
│   └── myservice.yaml     ← your messaging service bridge manifest
├── inbox/
│   ├── imsg/              ← iMessage wrapper writes here
│   └── myservice/         ← your messaging service wrapper writes here
├── outbox/
│   ├── imsg/              ← Adapter writes iMessage replies here
│   └── myservice/         ← Adapter writes your messaging service replies here
├── status/
│   ├── imsg/
│   └── myservice/
└── media/
    ├── imsg/
    └── myservice/
```

Each bridge is fully isolated — its own inbox, outbox, status, and media directories. Wrappers only touch their own namespace.

## Writing a Bridge Wrapper

A bridge wrapper is any script that:

1. **Reads** JSON files from `outbox/<bridge>/` (messages from Hermes)
2. **Sends** them via the platform's API (your messaging service, chatbot, etc.)
3. **Writes** incoming messages as JSON to `inbox/<bridge>/`
4. **Writes** status to `status/<bridge>/`

See `wrappers/imsg-wrapper.py` for a complete example (watch stream with auto-reconnect, history safety net, and registry self-registration). Ready-made wrappers live in [`wrappers/`](wrappers/README.md) — each with a short "use this when" note.

## Requirements

- Hermes Agent (Gateway mode)
- Python 3.11+
- No additional dependencies (uses Hermes Gateway SDK)

