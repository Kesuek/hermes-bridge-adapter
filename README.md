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
- **Unified threads (T-058)** — `/unified` commands create shared agent sessions across bridges; `unified~<name>` multicasts a reply to every member bridge
- **Unified thread modes (T-059)** — per-thread `mode` (`participant` / `reactive` / `silent` / `protokoll`) controls dispatch behaviour; leader (`created_by`) marked as `[<Name> Leader]` in the routing context; `protokoll` lifecycle (`open`/`close`) collects messages into a Markdown artifact

## Unified Threads (T-058)

A **Unified Thread** lets several bridges (imsg, Talk, …) share a single agent session. Every member of a unified thread is mapped onto the same virtual thread — `chat_type="thread"`, `chat_id="unified"`, `thread_id=<name>` — so the gateway builds one shared session key and all members talk to the same agent context. The agent's reply is **multicast** to every member bridge.

### `/unified` commands

A message starting with `/unified` is parsed by the adapter as a command (it never reaches the agent). Commands are sent from any member bridge's `inbox/` like a normal message:

| Command | Description |
|---------|-------------|
| `/unified create <name>` | Create a new unified thread; the sender becomes the first member (and the **leader**) |
| `/unified status` | List all unified threads + member count + mode |
| `/unified join <name>` | Join an existing unified thread |
| `/unified leave <name>` | Leave a unified thread |
| `/unified members <name>` | List the members of a thread |
| `/unified mode <name> <mode>` | Set the thread's mode — `participant` / `reactive` / `silent` / `protokoll` (see [Teilnehmer-Modi](#teilnehmer-modi-t-059) below) |
| `/unified protokoll open <name> [sitzung]` | Open a protokoll session (leader-only); starts collecting messages and sets mode to `protokoll` |
| `/unified protokoll close <name>` | Close the protokoll session (leader-only); writes the Markdown artifact to `<bridge_dir>/protokoll/<name>/<sitzung>.md` and reverts mode to `participant` |
| `/unified help` | Show the command list |

### Addressing a unified thread

A member of a unified thread gets mapped onto the virtual thread automatically — their normal messages route to the shared session. The agent (or any cron job) replies with the special target:

```
unified~<name>
```

`send()` special-cases the `unified~` prefix **before** bridge resolution (`unified` is not a registered bridge prefix) and writes one outbox JSON per member to that member's own `outbox/<bridge>/`:

```
outbox/imsg/<uuid>.json   → target = imsg~<chat_id>
outbox/talk/<uuid>.json   → target = talk~<chat_id>
```

Each wrapper then delivers its copy via its own platform API, so one agent reply reaches every bridge in the thread.

### Teilnehmer-Modi (T-059)

Every unified thread has a `mode` field (default `participant`) that controls how the adapter dispatches incoming member messages. The adapter enforces `reactive`/`silent`/`protokoll` **deterministically before the gateway sees the message** — only `participant` lets the agent decide.

| Mode | Behaviour | Set with |
|------|-----------|----------|
| `participant` (default) | The agent decides whether to reply. It is taught (via `platform_hint`) to emit the literal token `NO_REPLY` when it has nothing to contribute; the gateway suppresses that reply. | `/unified mode <name> participant` |
| `reactive` | Mention-gating like a group chat: only messages that mention the agent (`@hermes`, or a bridge-specific `mention_patterns` match) are dispatched. Un-mentioned messages are dropped + the inbox file is deleted. | `/unified mode <name> reactive` |
| `silent` | Listener: the agent never replies. All messages are dropped + the inbox file is deleted — no agent turn is triggered. The agent still keeps the thread history via session persistence, so it "reads along" when it later returns to the thread. | `/unified mode <name> silent` |
| `protokoll` | Protocol mode: incoming messages are collected into the live protokoll session instead of dispatched. The agent does not reply while a session is open. See [Protokoll-Lifecycle](#protokoll-lifecycle). | `/unified protokoll open <name>` |

> **Note:** `reactive`/`silent`/`protokoll` drop the message in the adapter — the agent never sees it on this turn. The agent's shared session still accumulates history from the turns it *does* see, so later context is preserved.

#### Leader-Markierung

The thread creator (`created_by`) is the thread's **leader**. The routing-context line that the adapter appends to every unified-thread message marks the leader explicitly so the agent can tell protocol leadership apart from regular members:

```
Message from ronny, bridge imsg, unified thread 'projekt' (2 members), reply to unified~projekt [Ronny Leader]
```

Non-leader messages carry the same routing line without the `[<Name> Leader]` suffix.

#### Protokoll-Lifecycle

`protokoll` is a leader-only lifecycle for capturing a thread's conversation as an artifact (e.g. a meeting protocol):

1. **Open** — the leader runs `/unified protokoll open <name> [sitzung]`. The adapter records a live `protokoll` state on the thread (`name`, `opened_at`, `messages: []`) and switches the thread's mode to `protokoll`. From this point incoming messages are **collected** into `protokoll.messages` instead of dispatched — the agent does not reply.
2. **Close** — the leader runs `/unified protokoll close <name>`. The adapter renders the collected messages as Markdown to `<bridge_dir>/protokoll/<name>/<sitzung>.md`, clears the live `protokoll` state, and reverts the mode to `participant`.
3. **Retroaktiv** — closing a session that collected no messages produces a placeholder artifact noting that the session was opened after the fact; the agent can be asked to summarize the existing thread history on demand.

Only the leader (`created_by`) may `open`/`close`. Non-leader attempts are rejected with a clear message. The session name defaults to the thread name when none is given.

### Persistence

Unified threads are persisted in `<bridge_dir>/unified_threads.json`:

```json
{
  "projekt": {
    "name": "projekt",
    "created_at": "2026-08-10T10:00:00+02:00",
    "created_by": "ronny",
    "members": {
      "imsg:u1": {"bridge": "imsg", "chat_id": "u1", "user_id": "ronny", "user_name": "ronny", "joined_at": "..."},
      "talk:t1": {"bridge": "talk", "chat_id": "t1", "user_id": "anja", "user_name": "anja", "joined_at": "..."}
    },
    "aliases": [],
    "mode": "participant",
    "protokoll": null
  }
}
```

Members are keyed by `{bridge}:{chat_id}`. The file is loaded on `connect()` and rewritten on every mutating command, so threads survive a gateway restart. While a protokoll session is open, `protokoll` holds `{name, opened_at, opened_by, messages: [...]}`; after `close` it reverts to `null`.

### Notes / limits

- **Auth stays framework-side.** A user not authorized on a bridge is dropped by the gateway's authz mixin before the adapter's mapping sees them — they cannot join a thread.
- **Member dedup (T-062)** is a separate task — T-058 uses raw `{bridge}:{chat_id}` keys.
- **Mention patterns** drive `reactive` mode (and group-chat gating). The default patterns match `@hermes` / `hermes agent`; a bridge can override via `mention_patterns` in its manifest / `BRIDGE_MENTION_PATTERNS`. A message is "mentioned" if any pattern matches it.
- **`NO_REPLY` marker** is only relevant in `participant` mode — the agent emits the literal token `NO_REPLY` (or `[SILENT]`) and the gateway suppresses delivery. The other three modes drop deterministically in the adapter before the agent is ever called.

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

