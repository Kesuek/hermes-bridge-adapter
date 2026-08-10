# Security Policy

The Hermes Bridge Adapter brokers messages between messaging platforms and the
Hermes Agent. Because it handles identity, routing, and cross-bridge relay, we
take security reports seriously.

## Reporting a vulnerability

**Do not open a public issue for security problems.** Please report
vulnerabilities privately by email to the repository maintainer, or use the
GitHub **Security → Report a vulnerability** flow (private disclosure).

Please include:

- The affected version / commit.
- A description of the vulnerability and its impact.
- Steps to reproduce (redact any real personal data, credentials, or private
  configuration).
- Whether it is already publicly known.

We aim to acknowledge reports within **3 business days** and will keep you
informed of progress toward a fix. Please do not disclose the issue publicly
until we have published a fix or agreed otherwise.

## Supported versions

| Version | Supported          |
|---------|--------------------|
| `main`  | ✅ actively developed |

This is a fast-moving personal project — we recommend tracking `main` rather
than pinning a release.

## Security-relevant areas

These are the parts of the codebase most likely to attract an attack, and the
ones we pay closest attention to:

- **Identity claim / confirm (T-065)** — the challenge-response flow that
  merges two bridge identities into one person. Confirm codes are generated
  with `secrets` and invalidated after `IDENTITY_CONFIRM_MAX_ATTEMPTS` wrong
  guesses, so they cannot be brute-forced by an already-authorized user.
- **Routing / relay (T-063, T-068)** — messages mirrored across member
  bridges and the `unified~` multicast path. Loops are prevented by keeping
  relay copies outbox-only.
- **Authz boundary** — message authorization stays on the framework side; a
  user not authorized on a bridge is dropped before the adapter sees them.

## Good security practice (for contributors)

- Never commit secrets, tokens, private config, or real personal data — use
  placeholders (e.g. `alice@example.com`).
- Never loosen the challenge-response gate on identity claims.
- If a persisted file shape changes, migrate old data instead of breaking it.

## Thanks

We appreciate researchers who report issues responsibly and give us time to
fix them before disclosure.
