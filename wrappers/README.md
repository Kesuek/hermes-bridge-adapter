# Wrappers

A *wrapper* is the external side of the bridge: a script (in any language, on
any host) that binds a real messaging platform to the JSON-file contract the
Bridge Adapter expects. Each wrapper:

- reads the adapter's `outbox/` and sends messages over its platform,
- writes incoming messages to `inbox/`,
- self-registers via a manifest in `registry/` (presence = registered),
- reports health to `status/`.

This directory holds the wrappers maintained in this repository. To add a new
one, model it on `imsg-wrapper.py` (the most complete example) or the
`WRAPPER_GUIDE.md` template at the repo root.

## Available wrappers

| Wrapper | Binds to | Type | Use this when |
|---------|----------|------|---------------|
| [`imsg-wrapper.py`](imsg-wrapper.py) | **iMessage** (macOS, over SSH) | realtime watch + polling | You have a Mac with iMessage and want a self-hosted, fully-controlled alternative to BlueBubbles/Photon. Uses `imsg watch` as the primary realtime source with auto-reconnect, plus a history-polling safety net. |
| [`talk-wrapper.py`](talk-wrapper.py) | **Nextcloud Talk** (REST API) | HTTP polling | You run Nextcloud Talk and want it as a Hermes channel. Pure HTTP polling (Talk has no watch stream), own-message filtering to avoid echo loops. |
| [`test-wrapper.py`](test-wrapper.py) | **None — simulated** | manual test tool | Validating a new bridge end-to-end *before* writing a real wrapper: register/unregister a test bridge, push a message, drain the outbox. See T-054. |

## Choosing a wrapper

- **Real person-to-person chat on your own infrastructure** → `imsg-wrapper.py`
  (self-hosted iMessage).
- **A chat space on a self-hosted cloud** (files, talk, etc.) → `talk-wrapper.py`.
- **Writing a new bridge or debugging the adapter** → `test-wrapper.py` to prove
  the loop without a real service.

All three are independent scripts — pick one, run it, and the adapter picks up
the bridge from its manifest automatically (no adapter restart needed).
