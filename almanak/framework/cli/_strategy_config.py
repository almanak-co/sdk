"""Dict -> typed strategy-config coercion shared by ``strat run`` and ``strat backtest``.

``IntentStrategy`` subclasses are generically typed against a per-strategy
config dataclass (``IntentStrategy[ConfigT]``,
``almanak/framework/strategies/intent_strategy.py``); strategy code reads
``self.config.<field>`` and breaks if handed a raw ``config.json`` dict.
The runner resolves the dataclass from ``__orig_bases__`` and instantiates
it from the dict; the backtest CLI (blueprint 31) must construct strategies
through the exact same path. This module IS that single path -- extracted
from ``run_helpers._instantiate_strategy`` so the two surfaces cannot drift.

This module must stay lightweight (stdlib + click only): the backtest CLI
imports it at module load, and ``run.py`` / ``run_helpers.py`` depend on it
-- never the other way around. ``DictConfigWrapper`` lives here for that
reason; ``run.py`` re-exports it for existing importers.
"""

from __future__ import annotations

import logging
import types
from decimal import Decimal
from typing import Any, Union, get_args, get_origin, get_type_hints

import click

logger = logging.getLogger(__name__)

# Keys injected by the runtime, not consumed by strategy config classes.
_RUNTIME_FIELDS = {"deployment_id", "chain", "wallet_address"}
# Meta-keys consumed by the CLI/framework, not by strategy config classes.
_FRAMEWORK_META_KEYS = {"anvil_funding", "strategy_display_name"}


class DictConfigWrapper:
    """Wrapper for dict configs to provide required methods.

    StrategyBase expects config objects to have:
    - to_dict(): Serialize to dictionary
    - update(**kwargs): Update config values

    This wrapper makes plain dicts compatible.
    """

    def __init__(self, data: dict[str, Any]):
        """Initialize with dictionary data."""
        self._data = data
        # Copy all keys as attributes for getattr access
        for key, value in data.items():
            setattr(self, key, value)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return dict(self._data)

    def __getattr__(self, name: str) -> Any:
        """Provide a clearer missing-key error for strategy authors."""
        available_keys = ", ".join(sorted(self._data.keys())) if self._data else "(empty config)"
        raise AttributeError(f"Config key '{name}' not found in DictConfigWrapper. Available keys: {available_keys}")

    def update(self, **kwargs) -> Any:
        """Update config values.

        Returns a result object compatible with StrategyBase expectations.
        """
        from dataclasses import dataclass

        @dataclass
        class UpdateResult:
            success: bool = True
            error: str | None = None
            updated_fields: list[Any] | None = None
            previous_values: dict[Any, Any] | None = None

            def __post_init__(self):
                if self.updated_fields is None:
                    self.updated_fields = []
                if self.previous_values is None:
                    self.previous_values = {}

        previous = {}
        for key, value in kwargs.items():
            if key in self._data:
                previous[key] = self._data[key]
            self._data[key] = value
            setattr(self, key, value)

        return UpdateResult(
            success=True,
            updated_fields=list(kwargs.keys()),
            previous_values=previous,
        )

    def __getitem__(self, key: str) -> Any:
        """Support dict-like access."""
        return self._data[key]

    def get(self, key: str, default: Any = None) -> Any:
        """Support dict-like .get() access."""
        return self._data.get(key, default)


def _accepts_decimal(field_type: Any) -> bool:
    """True when a field annotation is ``Decimal`` or a union including it.

    Covers plain ``Decimal``, ``Optional[Decimal]`` / ``Union[Decimal, ...]``
    (``typing.Union``), and PEP 604 unions (``Decimal | None``).
    """
    if field_type == Decimal:
        return True
    if get_origin(field_type) in (Union, types.UnionType):
        return any(arg == Decimal for arg in get_args(field_type))
    return False


def coerce_strategy_config(strategy_class: type, strategy_config: dict[str, Any]) -> Any:
    """Coerce a raw config dict into the strategy's declared config type.

    Resolves a dataclass config type from the strategy's ``__orig_bases__``
    generic parameter, converts ``int``/``float``/``str`` values to
    ``Decimal`` for ``Decimal``-typed fields (including optional/union
    annotations), and drops keys the dataclass does not declare (runtime
    fields and framework meta-keys are expected and not reported; anything
    else is echoed as ignored). Strategies without a dataclass generic keep
    their dict config wrapped in ``DictConfigWrapper`` so attribute access
    still works.

    Any failure during inference or dataclass construction falls back to
    the wrapped dict -- same forgiving behavior the runner has always had.

    Args:
        strategy_class: The strategy class about to be instantiated.
        strategy_config: Parsed strategy config dict (e.g. ``config.json``).

    Returns:
        A config dataclass instance, or a ``DictConfigWrapper`` around the
        original dict when no dataclass type is resolvable.
    """
    config_instance: Any = strategy_config
    try:
        bases = getattr(strategy_class, "__orig_bases__", [])
        for base in bases:
            args = get_args(base)
            if args and hasattr(args[0], "__dataclass_fields__"):
                # Found dataclass config type - create instance with defaults
                config_class = args[0]

                # Convert numeric values to Decimal where needed
                type_hints = get_type_hints(config_class)
                converted_config: dict[str, Any] = {}
                unknown_fields = []
                for k, v in strategy_config.items():
                    if k in config_class.__dataclass_fields__:
                        # Convert int/float/str to Decimal for Decimal fields.
                        # bool is excluded: it subclasses int, but Decimal(str(True))
                        # raises InvalidOperation, so a bool would only ever take
                        # the exception path to land unchanged anyway.
                        if (
                            _accepts_decimal(type_hints.get(k))
                            and isinstance(v, int | float | str)
                            and not isinstance(v, bool)
                        ):
                            try:
                                converted_config[k] = Decimal(str(v))
                            except Exception:
                                converted_config[k] = v
                        else:
                            converted_config[k] = v
                    elif k not in _RUNTIME_FIELDS and k not in _FRAMEWORK_META_KEYS:
                        unknown_fields.append(k)

                # Use dataclass config, filtering out unknown fields
                # (runtime fields like deployment_id/chain are handled separately)
                if unknown_fields:
                    logger.debug(f"Config class {config_class.__name__} ignoring unknown fields: {unknown_fields}")
                    click.echo(f"  Config class: {config_class.__name__} (ignored: {unknown_fields})")
                else:
                    click.echo(f"  Config class: {config_class.__name__}")
                config_instance = config_class(**converted_config) if converted_config else config_class()
                break
    except Exception as e:
        logger.debug(f"Could not infer config class: {e}")
        # Fall back to using dict or default config

    # Wrap dict config in DictConfigWrapper for compatibility
    if isinstance(config_instance, dict):
        config_instance = DictConfigWrapper(config_instance)
        click.echo("  Config wrapped in DictConfigWrapper")

    return config_instance
