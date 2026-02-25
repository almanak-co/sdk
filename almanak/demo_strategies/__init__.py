"""Packaged demo strategies for `almanak strat demo`.

This package contains the 13 implemented demo strategies that ship with
the almanak package.  The `demo` CLI command copies a strategy directory
from here into the user's working directory so they can immediately run
and modify it.
"""

from __future__ import annotations

import json
from pathlib import Path

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


def get_demo_strategy_metadata(name: str) -> dict:
    """Return metadata (description, chain) for a demo strategy.

    Reads config.json for description and chain fields.
    """
    path = get_demo_strategy_path(name)
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
