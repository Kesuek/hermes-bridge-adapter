# Unified Threads — one agent session across multiple bridges

> **The Bridge Adapter's differentiator.** Because every bridge is wired to the adapter through the same JSON-file contract, multiple bridges can share a **single agent session** — one conversation across iMessage, Talk, and any other wrapper. Native gateway adapters are isolated from each other (each platform has its own session); the Bridge Adapter turns them into **one thread across all your messaging worlds**.

## Concept

A **Unified Thread** maps every member of a group onto the same virtual thread:

```
chat_type = "thread"
chat_id   = "unified"
thread_id = <name>
```

The gateway builds the session key `agent:main:bridge_adapter:thread:unified:<name>` — **without user_id** (verified against `gateway/session.py`: for `thread` + `thread_sessions_per_user=False` no user_id is appended). All members therefore share **one** session, and the agent auto-prefixes each message with `[Name]`.

**No core change** — the session mechanism already exists in the gateway; the adapter just maps onto it.

## `/unified` commands

A message starting with `/unified` (or the shorthand `/u`) is parsed by the adapter as a command (it never reaches the agent). Commands are sent from any member bridge's `inbox/` like a normal message. The `/u` alias supports per-command shortcuts: `c`→create, `j`→join, `l`→leave, `x`→exit, `m`→mode, `s`→switch, `st`→status, `me`→members, `d`→send, `h`/`?`→help. E.g. `/u c Team1` == `/unified create Team1`.

| Command | Description |
|---------|-------------|
| `/unified create <name>` | Create a thread; the sender becomes the first member (and the **leader**) |
| `/unified status` | List all threads + member count + mode |
| `/unified join <name>` | Join an existing thread |
| `/unified leave <name>` | Leave a thread (remove membership) |
| `/unified exit [name]` | **Pause** routing out of the thread — your messages from its chats go back to the normal per-bridge chat (own agent session, not the shared one). You stay a member. `switch`/`join` re-enter. (T-068) |
| `/unified members <name>` | List the members of a thread |
| `/unified mode <name> <mode>` | Set the mode — `participant` / `reactive` / `off` / `silent` / `protokoll` |
| `/unified switch <name>` | Set this thread as your **active thread** — your messages route here (T-064) |
| `/unified send <name> <message>` | One-shot send to a thread (multicast to all members, no switch) (T-064) |
| `/unified identity claim <bridge>~<target>` | Claim that you are also `<target>` on another bridge — sends a code to the target bridge (T-065) |
| `/unified identity confirm <code>` | Confirm an identity claim from the **target** bridge (proves you control both accounts) (T-065) |
| `/unified set username <name>` | Set your display name (per canonical person, shown in `status`) (T-065) |
| `/unified protokoll open <name> [sitzung]` | Open a protokoll session (leader-only) |
| `/unified protokoll close <name>` | Close the protokoll session (leader-only) |
| `/unified help` | Show the command list |

## Addressing

A member is mapped onto the virtual thread automatically — their normal messages route to the shared session. The agent (or any cron job) replies with the special target:

```
unified~<name>
```

`send()` special-cases the `unified~` prefix **before** bridge resolution (`unified` is not a registered bridge prefix) and writes **one outbox JSON per member** to that member's own `outbox/<bridge>/`:

```
outbox/imsg/<uuid>.json   → target = imsg~<chat_id>
outbox/talk/<uuid>.json   → target = talk~<chat_id>
```

Each wrapper delivers its copy via its own platform API — **one agent reply reaches every bridge in the thread**.

> **⚠️ The session chat_id must be routable.** The session `chat_id` is `unified~<name>`, NOT the bare `unified`. When the agent replies *through the session* (not by explicitly addressing `unified~<name>`), the gateway sends to the session chat_id. If that is `unified`, `_resolve_bridge_or_none` finds no `~` prefix → `bridge prefix unknown`. `unified~<name>` triggers the multicast branch. (Regression: caught live 2026-08-10, commit `dcd959c`.)

## Active thread (`switch`, T-064)

A user with multiple unified threads picks one as their **active thread** via `/unified switch <name>`. From then on, their incoming messages route onto that thread — even when the membership lookup (`_find_unified_for_member`) finds no match for the source `{bridge}:{chat_id}` (e.g. the user is a member via a different bridge, or joined then switched). The active thread is stored **per canonical person** (identity map, T-062), so the same person on two bridges switches once.

```
<bridge_dir>/active_threads.json
{ "alice": "projekt", "bob": "team" }
```

- **Membership required** — `switch` is only allowed on threads the user is already a member of. It's "which of my threads is active", not "join a new one". Join first with `/unified join <name>`.
- **Inbound fallback** — in `_process_incoming`, after the membership scan misses, the adapter checks `_active_threads[person]`. If set and the thread still exists, the message maps onto it.
- **Persists across restarts** — `active_threads.json` is loaded on `connect()` and rewritten on every `switch`.

## One-shot send (`send`, T-064)

`/unified send <name> <message>` delivers a single message to a unified thread **without switching** the active thread. It uses the existing multicast path (`send("unified~<name>", message)`) — one outbox JSON per member bridge — so every member sees the message on their own platform.

- **No membership requirement on the sender** — framework auth gates who may address the bridge; `send()` itself rejects unknown threads.
- **No mode interaction** — the message is written directly to the outbox; it does not pass through the adaptive buffer or mode gating (those apply to *inbound* dispatch, this is *outbound*).
- **Use case** — reply to a thread from a context where you don't want to switch (e.g. a quick ping from another bridge), or send as a one-off without committing.

## Exit / paused routing (T-068)

`/unified exit [name]` **pauses** the sender's routing out of a unified thread. It is distinct from `leave`:

- **`leave`** removes the user from the thread's `members` — they no longer see it at all and must re-join.
- **`exit`** keeps the user a member but **stops routing their messages** from the thread's chats into the shared unified session. Their messages from a member chat go to the **normal per-bridge chat** — their own agent session, not the shared thread session.

This is what you want when a chat is physically mapped as a thread member (so `_find_unified_for_member` would otherwise catch every message) but you want to switch back to writing a direct DM with the agent without leaving the team.

```
<bridge_dir>/paused_threads.json
{ "alice": ["Team1"], "bob": [] }
```

- **Routing** — in `_process_incoming`, after the membership lookup finds the thread, the adapter checks the sender's paused set. If the thread is paused, the message is **not** routed into `unified~<name>` — it falls through to the normal per-bridge chat.
- **Re-enter** — `/unified switch <name>` and `/unified join <name>` both **unpause** the thread.
- **Scope** — `/unified exit` with no name pauses *all* threads the person is a member of; with a name it pauses just that thread.
- **Persists across restarts** — `paused_threads.json` is loaded on `connect()` and rewritten on every `exit`/`switch`/`join`.

## Message-Relay (T-063)

In a Unified Thread, an incoming message from one member is **mirrored to the outbox of every other member bridge** — so all human participants see the full conversation across messenger boundaries, like a team chat across iMessage, Talk, and any other wrapper. The agent still receives the original message (it stays in context); the relay is **additional**, for the humans.

```
imsg: "Hallo alle"  →  agent (dispatched normally)
                  →  talk outbox:  "[Alice] Hallo alle"
                  →  matrix outbox: "[Alice] Hallo alle"
                  →  imsg outbox:  (nothing — source is excluded)
```

- **Format:** `[<Name>] text` — the sender's display name is prefixed so recipients know who wrote. No timestamp (each platform shows its own), no bridge prefix (the name is enough).
- **Targets:** every member address whose bridge is NOT the source, deduped per `(bridge, chat_id)`. A person who joined from two bridges receives the message on both (multicast); the same address is never written twice.
- **Runs in ALL modes** — `participant` / `reactive` / `off` / `silent` / `protokoll`. The mode controls how the **agent** reacts; the humans always see the message. The relay fires **before** the mode checks, and before the adaptive buffer check, so it is never gated or delayed by the mode/state machine.
- **Loop-safe** — the relay copy goes only into the **outbox** (the wrapper sends it via its platform API). The wrappers never feed sent messages back into the adapter's `inbox`, so a relayed copy can't be re-dispatched as an inbound message. No loop is possible.
- **Source excluded** — the originator's own bridge gets no relay copy; they already see their message on their platform.

## Participant modes

Every thread has a `mode` field (default `participant`) that controls how the adapter dispatches incoming member messages. `reactive`/`off`/`silent`/`protokoll` are enforced **deterministically before the gateway** — only `participant` lets the agent decide.

| Mode | Behaviour | Set with |
|------|-----------|----------|
| `participant` (default) | The agent decides whether to reply. Taught (via `platform_hint`) to emit `NO_REPLY` when it has nothing to contribute; the gateway suppresses that reply. | `/unified mode <name> participant` |
| `reactive` | Mention-gating like a group chat: only messages that mention the agent (`@hermes` or a `mention_patterns` match) are dispatched. Un-mentioned messages are dropped + the inbox file is deleted. | `/unified mode <name> reactive` |
| `off` | The agent gets nothing — no context, no turn. Every message is dropped + the inbox file is deleted. | `/unified mode <name> off` |
| `silent` | Mute switch: the agent reads along but never replies. Every message is buffered and flushed periodically (`digest_interval`) as one bundled turn, marked `[Silent digest — read only, do not reply]`. | `/unified mode <name> silent` |
| `protokoll` | Protocol mode: incoming messages are collected into the live session instead of dispatched. The agent does not reply while a session is open. | `/unified protokoll open <name>` |

> **Note:** `reactive`/`off` drop the message in the adapter — the agent never sees it on this turn. `silent` buffers it into a digest (the agent reads along but never replies). `protokoll` collects it into the session. The shared session still accumulates history from the turns the agent *does* see.

### Leader marking

The thread creator (`created_by`) is the thread's **leader**. The `protokoll` lifecycle (below) is restricted to the leader; non-leader attempts are rejected. (Note: the routing-context line the adapter appends no longer marks the leader explicitly — since T-066 it uses the **unified handle** instead of the raw bridge identity and names the source bridge: `Message from alice over talk~, unified thread 'Team1', reply to unified~Team1`.)

### Protokoll lifecycle

`protokoll` is a leader-only lifecycle for capturing a thread's conversation as an artifact (e.g. a meeting protocol):

1. **Open** — the leader runs `/unified protokoll open <name> [sitzung]`. The adapter records a live `protokoll` state on the thread (`name`, `opened_at`, `messages: []`) and switches the mode to `protokoll`. From then on incoming messages are **collected** into `protokoll.messages` instead of dispatched — the agent does not reply.
2. **Close** — the leader runs `/unified protokoll close <name>`. The adapter renders the collected messages as Markdown to `<bridge_dir>/protokoll/<name>/<sitzung>.md`, clears the live `protokoll` state, and reverts the mode to `participant`.
3. **Retroactive** — closing a session that collected no messages produces a placeholder artifact; the agent can be asked to summarize the existing thread history on demand.

Only the leader (`created_by`) may `open`/`close`. Non-leader attempts are rejected with a clear message. The session name defaults to the thread name when none is given.

## Adaptive state machine

Every thread carries a state machine that adapts dispatch behaviour to message frequency:

```
idle → active → digesting
```

- **`idle`** — no messages yet (initial state).
- **`active`** — messages seen; each is dispatched as its own turn (normal `participant` behaviour).
- **`digesting`** — high frequency (3 messages in 30s, or 5 in 60s, sliding window). Incoming messages are **buffered** instead of dispatched. After `digest_interval` (60s) the buffer is **flushed as a single bundled turn**: one `MessageEvent` whose text is a `[System: N messages from M users]` header followed by one `[HH:MM] [sender] text` line per buffered message. After the flush the state returns to `active` with a short cooldown.

State + buffer persist in `unified_threads.json` (the `_adaptive` block), so a gateway restart doesn't lose the in-flight digest window. Adaptive only applies in **`participant`** mode. `silent` reuses the same buffer but always collects (mute switch), not just under high frequency.

The thresholds and intervals are class constants on `BridgeAdapter` (`ADAPTIVE_THRESHOLD_30`, `ADAPTIVE_THRESHOLD_60`, `ADAPTIVE_DIGEST_INTERVAL`, `ADAPTIVE_COOLDOWN`).

## Reply-to chains across bridges

A reply chain on a single bridge uses the bridge-local message id (`reply_to`). Across bridges that id is meaningless — the iMessage wrapper doesn't know the Talk message id. The adapter bridges this gap with a persisted map:

```
<bridge_dir>/reply_map.json
{ "<gateway_msg_id>": {"bridge": "imsg", "local_msg_id": "msg_abc"}, ... }
```

- **Inbound** — on dispatch, the adapter records `gateway_msg_id → {bridge, local_msg_id}`. The gateway id is the event's `message_id`; the local id is the inbox JSON's `id`/`message_id`. If no gateway id is available, it falls back to a UUID.
- **Outbound** — `send()`/`send_image()`/`send_document()` (and the `unified~` multicast path) resolve a `reply_to` that matches a gateway id in the map to the stored `local_msg_id`. A bridge-local `reply_to` passes through unchanged.

The file is loaded on `connect()` and rewritten on every inbound registration, so reply chains survive a restart.

## Member deduplication

The same person may appear on two bridges under different aliases — `alice@example.com` on iMessage and `alice` on Talk — but they are one person and should be one member of a unified thread. The adapter collapses aliases via a persisted identity map. As of T-063 the map records **which wrapper each alias belongs to**, so a `(wrapper, user_id)` pair resolves precisely (preventing two people who share a bare alias on different wrappers from being merged):

```
<bridge_dir>/identity_map.json
{
  "alice": {
    "aliases": ["alice@example.com", "+49 170 1234567", "alice"],
    "wrappers": {"imsg": "alice@example.com", "talk": "alice"}
  }
}
```

- **`_resolve_identity(wrapper, user_id)`** maps a `(wrapper, user_id)` pair to the canonical person: a wrapper-declared alias match wins; a bare-alias match is the fallback; an unknown pair returns the `user_id` itself. The legacy bare-list shape (`{"alice": ["..."]}`) and the 1-arg call form remain supported for backwards compatibility.
- **Member record** — every member gets a `person` field (the canonical identity) and an `addresses` array of `{bridge, chat_id, user_id}` entries.
- **Join dedup** — `_cmd_unified_join` checks whether the sender's canonical `person` is already a member. If so, the new `{bridge}:{chat_id}` address is merged into the existing member's `addresses` array instead of creating a duplicate member entry. The primary member key stays the first address the person joined from.
- **Inbound routing** — `_find_unified_for_member` scans both the top-level `{bridge}:{chat_id}` keys and the merged `addresses` arrays, so a message from any of a person's bridges still maps to the shared thread.
- **Multicast** — `send("unified~<name>")` multicasts to every member's primary address **and** every merged address (deduped), so a person on two bridges receives the reply on both. The message **relay** (T-063) mirrors inbound messages the same way.

The file is loaded on `connect()`. Unknown aliases pass through unchanged, so the identity map is purely opt-in.

## Identity-Claim (Challenge-Response, T-065)

The identity map is hand-editable, but it can also be **authorized programmatically** with a challenge-response flow: a user claims that they are also a second identity on another bridge, and must prove control of both accounts before the merge takes effect.

1. **Claim** — `/unified identity claim <bridge>~<target>` (sent from the source bridge). The adapter generates a 6-digit code, records a pending claim (`{code, source, target, expires}`, 5-minute TTL) in `pending_claims.json`, and writes one outbox JSON to the **target** bridge's `outbox/<target_bridge>/` carrying the code (only someone who reads that bridge sees it).
2. **Confirm** — `/unified identity confirm <code>` sent **from the target bridge** by the claimed target identity. The adapter checks the code, the TTL, and that the sender matches the claim's `target`. On success it merges the target alias into the source person's `identity_map.json` entry (`aliases` + `wrappers`), persists the map, and clears the pending claim.

This is the root-level spoofing guard: an attacker who compromises only one bridge cannot confirm a claim, because the code was sent to the *other* bridge. The confirm must come from the claimed target.

Pending claims and the identity map are both loaded on `connect()` and rewritten atomically, so an in-flight challenge survives a restart.

## Username (`set username`, T-065)

`/unified set username <name>` sets a display name for the sender's canonical person, persisted in `<bridge_dir>/usernames.json` (`{person → display_name}`, loaded on `connect()`). The `status` command shows it alongside each person's merged addresses.

## Unified Handles (T-066)

Unified threads decouple the on-thread identity from the raw bridge identity. Every participant — humans and the agent alike — is shown by a **unified handle** rather than a bridge-local `user_id`:

- **User handle** — `_resolve_unified_handle(bridge, user_id)` returns `unified~<username>` when the person has a display name set (T-065), else falls back to the raw `unified~<user_id>`. This is the handle shown in the relay copies (`[Name] text` mirrored to the other member bridges) so all participants see a consistent name across messenger boundaries.
- **Agent handle** — the agent's own handle comes from the config (`extra["agent_handle"]` / `BRIDGE_AGENT_HANDLE`), defaulting to `hermes`. It is static (no `/unified` command mutates it).
- **Display prefix** — the relay strips the `unified~` prefix for display, so a message from `alice` reads `[alice] text` rather than `[unified~alice] text`. In a unified thread everything is unified, so the prefix is noise on screen.

Backwards compatible: without `agent_handle` in the config it stays `hermes`; without a username the user handle falls back to the raw identity. No existing thread breaks.

## Persistence

Unified threads are persisted in `<bridge_dir>/unified_threads.json`:

```json
{
  "projekt": {
    "name": "projekt",
    "created_at": "2026-08-10T10:00:00+02:00",
    "created_by": "alice",
    "members": {
      "imsg:u1": {
        "bridge": "imsg", "chat_id": "u1", "user_id": "alice@example.com",
        "user_name": "alice@example.com", "person": "alice",
        "joined_at": "...",
        "addresses": [
          {"bridge": "imsg", "chat_id": "u1", "user_id": "alice@example.com"},
          {"bridge": "talk", "chat_id": "t1", "user_id": "alice"}
        ]
      }
    },
    "aliases": [],
    "mode": "participant",
    "_adaptive": {"state": "idle", "buffer": [], "last_msg_ts": 0.0, "digest_until": 0.0, "cooldown_until": 0.0},
    "protokoll": null
  }
}
```

Members are keyed by `{bridge}:{chat_id}` (the first address a person joined from). The file is loaded on `connect()` and rewritten on every mutating command, so threads survive a gateway restart. While a protokoll session is open, `protokoll` holds `{name, opened_at, opened_by, messages: [...]}`; after `close` it reverts to `null`. The `_adaptive` block (T-061) tracks the per-thread state-machine state and message buffer.

The **active thread** map (T-064) lives in a separate file, `<bridge_dir>/active_threads.json` (`{person → thread_name}`), also loaded on `connect()` and rewritten on every `/unified switch`.

The **paused threads** (T-068) live in `<bridge_dir>/paused_threads.json` (`{person → [thread_names]}`), loaded on `connect()` and rewritten on every `/unified exit` / `switch` / `join`.

The **pending identity claims** (T-065) live in `<bridge_dir>/pending_claims.json` (`{claim_id → {code, source, target, expires}}`), loaded on `connect()` and rewritten on every `claim`/`confirm`. The **usernames** (T-065) live in `<bridge_dir>/usernames.json` (`{person → display_name}`), loaded on `connect()` and rewritten on every `/unified set username`. The **identity map** is now also written programmatically (`_save_identity_map`, atomic write) when a claim is confirmed — previously it was hand-edit only.

## Notes / limits

- **Auth stays framework-side.** A user not authorized on a bridge is dropped by the gateway's authz mixin before the adapter's mapping sees them — they cannot join a thread.
- **Mention patterns** drive `reactive` mode (and group-chat gating). The default patterns match `@hermes` / `hermes agent`; a bridge can override via `mention_patterns` in its manifest / `BRIDGE_MENTION_PATTERNS`.
- **`NO_REPLY` marker** is only relevant in `participant` mode — the agent emits the literal token `NO_REPLY` (or `[SILENT]`) and the gateway suppresses delivery. The other modes drop or buffer deterministically in the adapter before the agent is ever called.
- **Adaptive + modes** — adaptive bundling only applies in `participant` mode; `reactive`/`off` drop un-mentioned messages anyway (no digest needed), `protokoll` collects into the session, `silent` always buffers (mute switch).
- **Identity map is opt-in** — without an `identity_map.json` entry, a sender's `person` equals its raw `user_id`, so two different people with the same id on different bridges would be merged. Add explicit entries to control which aliases belong together.
