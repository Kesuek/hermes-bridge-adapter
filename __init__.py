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
except ImportError:  # pragma: no cover - flat-module load during pytest
    import adapter as _adapter

    register = _adapter.register

__all__ = ["register"]
