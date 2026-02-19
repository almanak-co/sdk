"""
Almanak SDK Strategies.

This package contains example and production strategies:
- demo/: Tutorial and reference implementations
- incubating/: Experimental strategies under development
"""

from typing import Any, Dict, Type

# Strategy registry
STRATEGY_REGISTRY: dict[str, type[Any]] = {}


def register_strategy(name: str, strategy_class: type[Any]) -> None:
    """Register a strategy class in the factory."""
    STRATEGY_REGISTRY[name] = strategy_class


def get_strategy(name: str) -> type[Any]:
    """Get a strategy class by name."""
    if name not in STRATEGY_REGISTRY:
        raise ValueError(f"Unknown strategy: {name}. Available: {list(STRATEGY_REGISTRY.keys())}")
    return STRATEGY_REGISTRY[name]


def list_strategies() -> list[str]:
    """List all registered strategy names."""
    return list(STRATEGY_REGISTRY.keys())
