"""Packaged demo strategies for `almanak strat demo`.

This package contains the 78 demo strategies that ship with
the almanak package.  The `demo` CLI command copies a strategy directory
from here into the user's working directory so they can immediately run
and modify it.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_PACKAGE_DIR = Path(__file__).parent

# Strategy names are the subdirectory names that contain a config.json.
# Immutable to prevent mutation-based bypass of the path-traversal guard.
DEMO_STRATEGY_NAMES: tuple[str, ...] = tuple(
    sorted(p.parent.name for p in _PACKAGE_DIR.glob("*/config.json"))
)


def get_demo_strategy_path(name: str) -> Path:
    """Return the absolute path to a demo strategy directory.

    Raises:
        KeyError: If no demo strategy with the given name exists.
    """
    if name not in DEMO_STRATEGY_NAMES:
        raise KeyError(
            f"Unknown demo strategy '{name}'. "
            f"Available: {', '.join(DEMO_STRATEGY_NAMES)}"
        )
    return _PACKAGE_DIR / name


def _load_strategy_metadata(strategy_dir: Path) -> dict | None:
    """Load STRATEGY_METADATA from a strategy.py file, returning its to_dict() or None."""
    strategy_file = strategy_dir / "strategy.py"
    if not strategy_file.exists():
        return None
    module_name = f"_demo_meta_{strategy_dir.name}"
    try:
        spec = importlib.util.spec_from_file_location(module_name, strategy_file)
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        # Find class with STRATEGY_METADATA
        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if isinstance(obj, type):
                meta = getattr(obj, "STRATEGY_METADATA", None)
                if meta and hasattr(meta, "description"):
                    return {
                        "description": meta.description or "",
                        "chain": meta.default_chain or (meta.supported_chains[0] if meta.supported_chains else ""),
                    }
    except Exception as e:
        logger.debug(f"Failed to load decorator metadata from {strategy_file}: {e}")
    finally:
        # Clean up to avoid polluting sys.modules
        sys.modules.pop(module_name, None)
    return None


def get_demo_strategy_metadata(name: str) -> dict:
    """Return metadata (description, chain) for a demo strategy.

    Reads from decorator metadata (STRATEGY_METADATA), falling back to config.json.
    """
    path = get_demo_strategy_path(name)

    # Primary: decorator metadata
    meta = _load_strategy_metadata(path)
    if meta:
        return meta

    # Fallback: config.json (legacy, for backwards compatibility)
    config_file = path / "config.json"
    try:
        with open(config_file) as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Malformed JSON in {config_file}: {e}") from e

    description = config.get("description", "")
    chain = config.get("chain", "unknown")
    return {"description": description, "chain": chain}


def list_demo_strategies() -> list[dict]:
    """Return a sorted list of dicts with name, description, chain."""
    return [{"name": name, **get_demo_strategy_metadata(name)} for name in DEMO_STRATEGY_NAMES]
