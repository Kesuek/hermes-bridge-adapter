# Contributing

Thanks for your interest in the Hermes Bridge Adapter!

## Ground rules

- **Public repo, public docs:** everything here is English and MIT-licensed.
  Never commit personal data, secrets, or private configuration. If an example
  needs an identity, use a generic placeholder (e.g. `alice@example.com`), not
  a real person.
- **Backwards compatibility:** existing JSON contracts, persisted files, and
  commands must keep working. If you change a persisted shape, migrate old
  data instead of breaking it.

## Getting started

1. Fork and clone the repo.
2. Create a branch: `git checkout -b my-feature`.
3. Make your changes.

## Development workflow

- **Tests first (TDD):** write a failing test that captures the behaviour,
  then implement until it passes.
- Run the full suite before committing:
  ```bash
  python -m pytest tests/ -q
  ```
- **Board-driven:** larger changes follow the task board. Add an entry to
  `TASKS.md` / `DECISIONS.md` (in the `board/` symlink) describing the task
  and its decisions.

## Committing

- Keep commits focused and descriptive.
- Reference the task id in the message (e.g. `feat: ... (T-065)`).
- Public-facing docs (README, UNIFIED_THREADS.md) must stay in sync with
  behaviour changes.

## Submitting

Open a pull request against `main`. Explain what you changed and why, and
confirm the test suite is green.
