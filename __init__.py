"""Bridge Adapter gateway plugin.

Works in two load contexts:

- **As a Hermes plugin package** (deployed under
  ``~/.hermes/plugins/bridge-adapter/``): loaded as a package, so the
  relative ``from .adapter import register`` resolves normally.
- **As a plain top-level module** (pytest tests import ``adapter``
  directly): the relative import fails, so we fall back to importing the
  flat ``adapter`` module.
"""

try:
    from .adapter import register  # Hermes plugin package load
except ImportError as exc:  # pragma: no cover - flat-module load during pytest
    # Fall back ONLY when the relative import itself is unresolvable
    # ("attempted relative import with no known parent package").
    # Errors from adapter's own dependencies (e.g. a missing module inside
    # ``.adapter``) must NOT trigger the fallback — they would surface as a
    # misleading "No module named 'adapter'" and mask the real cause.
    if "no known parent package" not in str(exc):
        raise
    import adapter as _adapter

    register = _adapter.register

__all__ = ["register"]