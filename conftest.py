"""Pytest config for the bridge-adapter plugin repo.

The plugin lives at the repo root (``adapter.py`` is a top-level module
imported by Hermes via ``~/.hermes/plugins/bridge-adapter/``), so we make
the repo root importable and keep ``adapter`` as a plain top-level module.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))