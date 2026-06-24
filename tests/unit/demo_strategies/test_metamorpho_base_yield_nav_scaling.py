"""Regression tests for the metamorpho_base_yield demo NAV scaling (VIB-5392).

The MetaMorpho ERC-4626 ``Deposit``/``Withdraw`` events report ``assets`` in the
underlying token's RAW base units (6 decimals for USDC). The strategy reports a
``value_usd`` to ``portfolio_snapshots`` via ``get_open_positions``; for a
TOKEN-typed position the framework consumes that value verbatim (no on-chain
reprice). Before the fix the strategy surfaced the raw 6-decimal amount as USD,
inflating NAV by exactly 1e6× — a 4 USDC (4_000_000 base-unit) deposit read as
$4,000,000 for the entire hold.

These tests build the REAL strategy object (not a SimpleNamespace of a raw row)
and assert:

* a 4-USDC deposit values at ~$4, not $4M (6-decimal asset), and
* an 18-decimal asset SCALES correctly (1e18 raw → 1 human unit) — proving the
  decimals are SOURCED from config, not assumed. This is a scaling assertion,
  NOT a USD-equivalence claim: value_usd == human_assets is USD-accurate only
  for a USD-pegged deposit token, which __init__ pins/validates. True
  multi-asset USD valuation is the framework PositionType.VAULT path.
"""

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.demo_strategies.metamorpho_base_yield.strategy import MetaMorphoBaseYield

_DEMO_DIR = Path(__file__).resolve().parents[3] / "almanak" / "demo_strategies" / "metamorpho_base_yield"


def _make_strategy(config_overrides: dict | None = None, drop_keys: list | None = None):
    """Construct MetaMorphoBaseYield without the framework base __init__.

    Mirrors tests/unit/demo_strategies/test_pendle_basics.py: build the instance
    with ``__new__``, wire a ``get_config`` backed by the real config.json (plus
    any override), then run the demo's own ``__init__``. ``drop_keys`` removes
    keys so the demo's OWN code default is exercised.
    """
    cfg = json.loads((_DEMO_DIR / "config.json").read_text(encoding="utf-8"))
    if config_overrides:
        cfg.update(config_overrides)
    for key in drop_keys or []:
        cfg.pop(key, None)
    strat = MetaMorphoBaseYield.__new__(MetaMorphoBaseYield)
    strat._config = cfg
    strat.get_config = lambda k, d=None: cfg.get(k, d)
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        MetaMorphoBaseYield.__init__(strat)
    # get_open_positions reads self.chain / self.deployment_id (framework-injected).
    # ``chain`` is a read-only property backed by ``_chain``; ``deployment_id`` is
    # patched directly onto the instance for the teardown summary.
    strat._chain = cfg.get("chain", "base")
    strat._deployment_id = "deployment:test"
    return strat


def _deposit_result(assets_raw: int, shares_raw: int) -> SimpleNamespace:
    """A real-shaped execution result carrying a parsed Deposit event.

    ``assets`` is in RAW base units exactly as the morpho_vault receipt parser
    emits it (``HexDecoder.decode_uint256`` of the ERC-4626 Deposit log).
    """
    return SimpleNamespace(extracted_data={"deposit_data": {"assets": str(assets_raw), "shares": str(shares_raw)}})


def _deposit_intent() -> SimpleNamespace:
    return SimpleNamespace(intent_type=SimpleNamespace(value="VAULT_DEPOSIT"))


def _redeem_intent() -> SimpleNamespace:
    return SimpleNamespace(intent_type=SimpleNamespace(value="VAULT_REDEEM"))


class TestVaultNavScaling:
    def test_four_usdc_deposit_values_at_four_dollars_not_four_million(self):
        """VIB-5392: 4 USDC (4_000_000 base units) must value at ~$4, not $4M."""
        strat = _make_strategy()
        assert strat.deposit_token_decimals == 6

        # 4 USDC = 4_000_000 raw base units; MetaMorpho shares are 18-decimal.
        strat.on_intent_executed(
            _deposit_intent(),
            success=True,
            result=_deposit_result(assets_raw=4_000_000, shares_raw=3_337_120_859_609_341_904),
        )

        # Internal accounting is now in human units.
        assert strat._total_deposited == Decimal("4")

        # The snapshot-facing value_usd is ~$4, NOT $4,000,000.
        summary = strat.get_open_positions()
        assert len(summary.positions) == 1
        pos = summary.positions[0]
        assert pos.value_usd == Decimal("4")
        assert pos.value_usd < Decimal("100")  # decisively not the 1e6× inflation

    def test_eighteen_decimal_scaling_sources_decimals(self):
        """Decimals are SOURCED, not assumed: an 18-decimal asset scales by 1e18.

        This asserts the base-unit→human SCALING only, not USD equivalence. DAI
        is a USD-pegged 18-decimal stablecoin, so it is a real example that both
        exercises the 1e18 scaling AND satisfies the demo's USD-peg pin (without
        which value_usd == human_assets would not be USD-accurate). The point is
        that 1e18 raw base units → Decimal("1") human unit, i.e. decimals come
        from config, not a hardcoded 6.
        """
        strat = _make_strategy(config_overrides={"deposit_token": "DAI", "deposit_token_decimals": 18})
        assert strat.deposit_token_decimals == 18

        # 1 DAI = 1e18 raw base units.
        strat.on_intent_executed(
            _deposit_intent(),
            success=True,
            result=_deposit_result(assets_raw=1_000_000_000_000_000_000, shares_raw=900_000_000_000_000_000),
        )

        # Scaling correctness: 1e18 raw → 1 human unit (NOT a USD claim about a
        # non-pegged asset — see the framework PositionType.VAULT path for that).
        assert strat._total_deposited == Decimal("1")

    def test_redeem_yield_is_scaled_to_human_units(self):
        """Redeem assets are scaled too, so yield = redeemed − deposited is real."""
        strat = _make_strategy()
        # Deposit 4 USDC.
        strat.on_intent_executed(
            _deposit_intent(),
            success=True,
            result=_deposit_result(assets_raw=4_000_000, shares_raw=3_337_120_859_609_341_904),
        )

        # While held, the public surface values the position at ~$4 (not $4M).
        held = strat.get_open_positions()
        assert len(held.positions) == 1
        assert held.positions[0].value_usd == Decimal("4")

        # Redeem 4.10 USDC (4_100_000 base units) -> $0.10 yield.
        redeem_result = SimpleNamespace(extracted_data={"redeem_data": {"assets_received": "4100000"}})
        strat.on_intent_executed(_redeem_intent(), success=True, result=redeem_result)

        assert strat._redeem_assets == Decimal("4.1")
        assert strat._total_yield_earned == Decimal("0.1")

        # Consumer contract after redeem: the position is fully closed, so the
        # snapshot-facing surface carries NO open position — NAV no longer drags
        # a stale (and pre-fix 1e6×-inflated) vault value forward (VIB-5392).
        after = strat.get_open_positions()
        assert after.positions == []

    def test_code_default_decimals_is_six(self):
        """A user relying on the CODE default (no config key) gets USDC's 6."""
        strat = _make_strategy(drop_keys=["deposit_token_decimals"])
        assert strat.deposit_token_decimals == 6

    def test_negative_decimals_rejected(self):
        with pytest.raises(ValueError, match="deposit_token_decimals must be >= 0"):
            _make_strategy(config_overrides={"deposit_token_decimals": -1})

    def test_non_pegged_deposit_token_rejected(self):
        """value_usd == human_assets only holds for a USD-pegged token.

        Pointing the demo at a non-pegged token (WETH) must fail closed rather
        than silently report 1 WETH as $1 — true multi-asset USD valuation is
        the framework PositionType.VAULT path, out of scope for this demo.
        """
        with pytest.raises(ValueError, match="USD-pegged"):
            _make_strategy(config_overrides={"deposit_token": "WETH", "deposit_token_decimals": 18})


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
