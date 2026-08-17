# Changelog

All notable changes to the Bridge Adapter plugin are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security
- **T-071:** Wrapper state/status/manifest/inbox files are now written with
  `0600` permissions (central `_write_private` helper). State files may carry
  routing secrets/tokens and must not be world-readable on a shared host;
  `write_text` alone left them at the umask default (often `0644`).
- **T-072:** Added a `gitleaks` GitHub Actions workflow (`.github/workflows/secret-scan.yml`)
  that scans every push and pull request for leaked secrets.
- **T-072a:** The inbox poller now rejects symlinks in `inbox/<bridge>/` — a
  symlink could point at `/etc/passwd` or another bridge's state file, letting
  a compromised wrapper read arbitrary files through the adapter.
- **T-072b:** Manifest string fields (`service`, `host`) are now validated
  against path separators and `..` traversal, so a malicious manifest can't
  inject a traversal string that later flows into filesystem construction.
- **T-075:** SCP attachment pull enforces `StrictHostKeyChecking=yes` (fails
  closed if the remote host is ever unknown, closing the SSH-MITM window).
- **T-076:** Protokoll session names are sanitized against `../`-traversal so
  a crafted sitzung name can't escape the thread's protokoll directory.
- **T-077:** `/unified identity claim` no longer echoes the 6-digit code back
  to the claimer — the challenge secret reaches the target bridge only.
- **T-069/T-070:** Attachment media paths are canonicalized and confined to
  `bridge_dir`, rejecting absolute paths and `../`-traversal.

### Fixed
- **T-078:** `reply_map` now prunes entries older than 7 days (and caps at
  5000) on save. Previously every inbound appended an entry and every save
  rewrote the whole file — the map grew unbounded (O(n²) over time).
- **T-079:** `_atomic_write_json` now uses a uuid temp name per write, so two
  concurrent bridges writing the same persistence file can't collide on a
  shared `<path>.tmp` path (the old predictable suffix risked one writer
  unlinking the other's temp in the error path).
- **T-080:** `cooldown_until` is now enforced — the adaptive flush in
  `_process_incoming` and the silent-digest timer defer while a post-flush
  cooldown is active. Previously the value was set after a flush but never
  read, so a burst right after a flush was dispatched immediately (defeating
  the cooldown's purpose as specified in T-061).
- **T-073:** Stable gateway `message_id` for cross-bridge reply chains — the
  reply map is keyed by the exact id set on the event, so a later
  `reply_to=event.message_id` resolves to the bridge-local id.

### Documentation
- **T-082:** Translated the three German section headings in
  `WRAPPER_GUIDE.md` (`Reply-To-Ketten`, `Adaptive Zustandsmaschine`,
  `Member-Deduplizierung`) to English.
- **T-083:** Corrected the `README.md` JSON schema examples to match the
  actual implementation: inbox uses a flat `sender` (string) and `reply_to`
  (string), not `from`/`reply_to` objects; outbox uses `target` as a string
  (`<bridge>~<chat_id>`), not a `{chat_id, bridge}` object; attachment
  objects use `url`/`mime`/`size`, not `path`/`mime`.
- **T-084:** Added this `CHANGELOG.md`.

## [2026-08-18] — Unified Threads, bridge registry, security hardening

### Bridge Registry (T-050–T-057)
- **T-050:** Bridges self-register via `registry/*.yaml` manifests; the
  adapter polls `registry/` every few seconds and picks up new/removed
  manifests at runtime — no config change, no restart.
- **T-051:** System-prompt platform hint teaches the agent to address
  messages as `<bridge>~<target>`; every inbound carries a compact routing
  line (`[Message from <sender>, bridge <bridge>, reply to <bridge>~<target>]`).
- **T-052:** Wrapper self-registration: wrappers write their own manifest on
  start and remove it on shutdown.
- **T-053:** Routing fallback: `send()` validates the target; unroutable
  targets (unknown bridge / wrong format) return a clear `SendResult` error
  instead of silently misrouting.
- **T-055:** Reactions support (imsg `--reactions`).
- **T-056:** Bridge-target separator changed to `~` (resolves the legacy `:`
  ambiguity where `imsg:user@example.com` parsed wrong).
- **T-057:** `standalone_sender_fn` for out-of-process cron delivery.

### Unified Threads (T-058–T-068) — the differentiator
- **T-058:** Multiple bridges share a single agent session through the same
  JSON-file contract — one conversation across iMessage, Talk, and any
  wrapper. `/unified` commands with `/u` alias shortcuts.
- **T-059:** Five participant modes: `participant`, `reactive` (mention
  gating), `off` (drop), `silent` (mute/digest — agent reads along, never
  replies), `protokoll` (leader-only session logging with `open`/`close`
  lifecycle rendering a Markdown artifact).
- **T-060:** Cross-bridge reply chains via persisted `reply_map.json`.
- **T-061:** Adaptive state machine: high-frequency threads flip to
  digesting and buffer messages into a single bundled turn.
- **T-062:** Member deduplication via persisted `identity_map.json` — the
  same person joining from two bridges merges into one member with multiple
  addresses.
- **T-063:** Message relay mirrors every inbound message to the other
  member bridges so all humans see the full conversation; (bridge, user_id)
  mapping prevents cross-bridge merges.
- **T-064:** Active-thread switching and one-shot `/unified send` (T-064).
- **T-065:** Identity-claim challenge-response to authorize a user-mapping
  across bridges; per-person display name (`/unified set username`); brute-force
  safe confirm codes (wrong attempts invalidate the claim).
- **T-066:** Unified handles decouple the on-thread identity from the raw
  bridge identity (`unified~<username>`, agent handle from config).
- **T-067/T-068:** `/u x` exit pauses a member chat back into the normal
  per-bridge DM.