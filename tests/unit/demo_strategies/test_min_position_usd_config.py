"""min_position_usd must be a real dataclass config field (VIB-5013).

Regression guard: the LP demo strategies read
``self.get_config("min_position_usd", "100")`` in ``__init__``, but their
dataclass configs historically had no ``min_position_usd`` field. The CLI
config loader (``run_helpers``) only copies config.json keys that exist in
``config_class.__dataclass_fields__`` — so the user's value was silently
dropped and the $100 default always won (live repro: strategy held forever
with "$27.88 below min_position_usd $100.00" despite ``"min_position_usd": 5``
in config.json).

These tests assert, for every dataclass-config LP demo:

1. ``min_position_usd`` IS a declared dataclass field typed ``Decimal``
   (the exact contract the CLI loader keys on), and
2. constructing the strategy with a config carrying ``min_position_usd``
   (JSON number or JSON string, as the CLI loader would pass through)
   yields ``strategy.min_position_usd == Decimal("5")``, and
3. omitting the key keeps the documented $100 default.

Demos with plain dict configs (pancakeswap_lp, rsi_macd_lp, traderjoe_pnl_lp,
traderjoe_crisis_lp) are unaffected — ``get_config`` reads dict keys directly
— and are intentionally not covered here.
"""

from __future__ import annotations

import json
from dataclasses import fields
from decimal import Decimal
from typing import get_type_hints

import pytest

from almanak.demo_strategies.aerodrome_slipstream_lp.strategy import (
    AerodromeSlipstreamLPConfig,
    AerodromeSlipstreamLPStrategy,
)
from almanak.demo_strategies.sushiswap_lp.strategy import SushiSwapLPConfig, SushiSwapLPStrategy
from almanak.demo_strategies.traderjoe_lp.strategy import TraderJoeLPConfig, TraderJoeLPStrategy
from almanak.demo_strategies.uniswap_lp.strategy import UniswapLPConfig, UniswapLPStrategy
from almanak.demo_strategies.uniswap_v4_hooks.strategy import UniswapV4HooksConfig, UniswapV4HooksStrategy
from almanak.demo_strategies.uniswap_v4_lp.strategy import UniswapV4LPConfig, UniswapV4LPStrategy

_WALLET = "0x" + "11" * 20

# (strategy_class, config_class, chain)
DEMOS = [
    pytest.param(UniswapLPStrategy, UniswapLPConfig, "arbitrum", id="uniswap_lp"),
    pytest.param(SushiSwapLPStrategy, SushiSwapLPConfig, "arbitrum", id="sushiswap_lp"),
    pytest.param(TraderJoeLPStrategy, TraderJoeLPConfig, "avalanche", id="traderjoe_lp"),
    pytest.param(UniswapV4LPStrategy, UniswapV4LPConfig, "arbitrum", id="uniswap_v4_lp"),
    pytest.param(UniswapV4HooksStrategy, UniswapV4HooksConfig, "base", id="uniswap_v4_hooks"),
    pytest.param(
        AerodromeSlipstreamLPStrategy,
        AerodromeSlipstreamLPConfig,
        "base",
        id="aerodrome_slipstream_lp",
    ),
]


def _build_config(config_class: type, raw_config: dict[str, object]) -> object:
    """Mirror the CLI loader's dataclass conversion (run_helpers).

    The loader copies only keys present in ``__dataclass_fields__`` and
    coerces JSON int/float/str values to ``Decimal`` for ``Decimal``-typed
    fields. Replicated here so the tests exercise the exact contract that
    silently dropped the key before the fix.
    """
    type_hints = get_type_hints(config_class)
    converted: dict[str, object] = {}
    for key, value in raw_config.items():
        if key not in config_class.__dataclass_fields__:  # type: ignore[attr-defined]
            continue  # the CLI loader silently drops unknown keys — the bug
        if type_hints.get(key) is Decimal and isinstance(value, int | float | str):
            converted[key] = Decimal(str(value))
        else:
            converted[key] = value
    return config_class(**converted)


@pytest.mark.parametrize(("strategy_class", "config_class", "chain"), DEMOS)
def test_min_position_usd_is_a_declared_decimal_field(strategy_class, config_class, chain) -> None:
    """The CLI loader only forwards keys in __dataclass_fields__ — the field must exist."""
    field_names = {f.name for f in fields(config_class)}
    assert "min_position_usd" in field_names
    assert get_type_hints(config_class)["min_position_usd"] is Decimal


@pytest.mark.parametrize(("strategy_class", "config_class", "chain"), DEMOS)
@pytest.mark.parametrize("json_value", ["5", '"5"'], ids=["json-number", "json-string"])
def test_min_position_usd_from_config_json_reaches_strategy(strategy_class, config_class, chain, json_value) -> None:
    """config.json {"min_position_usd": 5} must override the $100 default."""
    raw_config = json.loads('{"min_position_usd": ' + json_value + "}")

    strategy = strategy_class(
        config=_build_config(config_class, raw_config),
        chain=chain,
        wallet_address=_WALLET,
    )

    assert strategy.min_position_usd == Decimal("5")


@pytest.mark.parametrize(("strategy_class", "config_class", "chain"), DEMOS)
def test_min_position_usd_defaults_to_100_when_omitted(strategy_class, config_class, chain) -> None:
    """Omitting min_position_usd from config.json keeps the documented default."""
    strategy = strategy_class(
        config=_build_config(config_class, {}),
        chain=chain,
        wallet_address=_WALLET,
    )

    assert strategy.min_position_usd == Decimal("100")
