"""Characterization test: snapshot liquidation/interest parameters at plan-022 baseline.

This test asserts exact Decimal equality for every (protocol, asset) pair and APY
entry that existed BEFORE the plan-022 rewire. It is the regression contract for the
relocation — if any value changes, the rewire introduced a simulation-behavior
regression.

The expected literals are pasted verbatim from the pre-rewire calculator state.
This test does NOT import ``liquidation_params._initialize_defaults`` or
``_initialize_asset_defaults`` — it is the safety net for those functions.
"""

from __future__ import annotations

from decimal import Decimal

import pytest


# ---------------------------------------------------------------------------
# Protocol-default snapshot
# ---------------------------------------------------------------------------

EXPECTED_PROTOCOL_DEFAULTS: dict[str, tuple[Decimal, Decimal, Decimal, str]] = {
    # key -> (liquidation_threshold, maintenance_margin, liquidation_penalty, protocol_field)
    "aave_v3": (Decimal("0.825"), Decimal("0"), Decimal("0.05"), "aave_v3"),
    "compound_v3": (Decimal("0.85"), Decimal("0"), Decimal("0.05"), "compound_v3"),
    "morpho": (Decimal("0.825"), Decimal("0"), Decimal("0.05"), "morpho"),
    "spark": (Decimal("0.80"), Decimal("0"), Decimal("0.08"), "spark"),
    "gmx": (Decimal("0"), Decimal("0.01"), Decimal("0.05"), "gmx"),
    "gmx_v2": (Decimal("0"), Decimal("0.01"), Decimal("0.05"), "gmx_v2"),
    "hyperliquid": (Decimal("0"), Decimal("0.02"), Decimal("0.05"), "hyperliquid"),
    "binance_perp": (Decimal("0"), Decimal("0.04"), Decimal("0.05"), "binance_perp"),
    "bybit": (Decimal("0"), Decimal("0.05"), Decimal("0.05"), "bybit"),
    "dydx": (Decimal("0"), Decimal("0.012"), Decimal("0.05"), "dydx"),
}


# ---------------------------------------------------------------------------
# Asset-specific snapshot: (protocol, ASSET) -> (threshold, maintenance_margin, penalty)
# All asset keys are .upper()-cased (as stored in the registry).
# ---------------------------------------------------------------------------

EXPECTED_ASSET_PARAMS: dict[tuple[str, str], tuple[Decimal, Decimal, Decimal]] = {
    # aave_v3 assets
    ("aave_v3", "ETH"): (Decimal("0.86"), Decimal("0"), Decimal("0.05")),
    ("aave_v3", "WETH"): (Decimal("0.86"), Decimal("0"), Decimal("0.05")),
    ("aave_v3", "WBTC"): (Decimal("0.80"), Decimal("0"), Decimal("0.065")),
    ("aave_v3", "USDC"): (Decimal("0.88"), Decimal("0"), Decimal("0.045")),
    ("aave_v3", "USDT"): (Decimal("0.80"), Decimal("0"), Decimal("0.05")),
    ("aave_v3", "DAI"): (Decimal("0.80"), Decimal("0"), Decimal("0.05")),
    ("aave_v3", "LINK"): (Decimal("0.75"), Decimal("0"), Decimal("0.075")),
    ("aave_v3", "AAVE"): (Decimal("0.73"), Decimal("0"), Decimal("0.075")),
    ("aave_v3", "UNI"): (Decimal("0.77"), Decimal("0"), Decimal("0.10")),
    ("aave_v3", "WSTETH"): (Decimal("0.84"), Decimal("0"), Decimal("0.05")),
    ("aave_v3", "CBETH"): (Decimal("0.80"), Decimal("0"), Decimal("0.075")),
    ("aave_v3", "RETH"): (Decimal("0.79"), Decimal("0"), Decimal("0.075")),
    # compound_v3 assets
    ("compound_v3", "ETH"): (Decimal("0.90"), Decimal("0"), Decimal("0.05")),
    ("compound_v3", "WETH"): (Decimal("0.90"), Decimal("0"), Decimal("0.05")),
    ("compound_v3", "WBTC"): (Decimal("0.80"), Decimal("0"), Decimal("0.05")),
    ("compound_v3", "WSTETH"): (Decimal("0.90"), Decimal("0"), Decimal("0.05")),
    ("compound_v3", "CBETH"): (Decimal("0.90"), Decimal("0"), Decimal("0.05")),
    # gmx_v2 assets
    ("gmx_v2", "ETH"): (Decimal("0"), Decimal("0.01"), Decimal("0.05")),
    ("gmx_v2", "BTC"): (Decimal("0"), Decimal("0.01"), Decimal("0.05")),
    ("gmx_v2", "LINK"): (Decimal("0"), Decimal("0.015"), Decimal("0.05")),
    ("gmx_v2", "ARB"): (Decimal("0"), Decimal("0.02"), Decimal("0.05")),
    ("gmx_v2", "UNI"): (Decimal("0"), Decimal("0.02"), Decimal("0.05")),
    ("gmx_v2", "SOL"): (Decimal("0"), Decimal("0.015"), Decimal("0.05")),
}


# ---------------------------------------------------------------------------
# APY snapshot (from interest.py __post_init__ defaults)
# Historical keys are "morpho" (not morpho_blue) and "spark"
# ---------------------------------------------------------------------------

EXPECTED_SUPPLY_APYS: dict[str, Decimal] = {
    "aave_v3": Decimal("0.03"),
    "compound_v3": Decimal("0.025"),
    "morpho": Decimal("0.035"),
    "spark": Decimal("0.05"),
}

EXPECTED_BORROW_APYS: dict[str, Decimal] = {
    "aave_v3": Decimal("0.05"),
    "compound_v3": Decimal("0.045"),
    "morpho": Decimal("0.04"),
    "spark": Decimal("0.055"),
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLiquidationProtocolDefaults:
    """Exact-value snapshot for every protocol-level default."""

    def test_all_protocol_defaults_present(self) -> None:
        """Registry must have exactly the expected protocol keys."""
        from almanak.framework.backtesting.pnl.calculators.liquidation_params import (
            LiquidationParamRegistry,
        )

        registry = LiquidationParamRegistry()
        assert set(registry.protocol_defaults.keys()) == set(EXPECTED_PROTOCOL_DEFAULTS.keys())

    @pytest.mark.parametrize("protocol,expected", list(EXPECTED_PROTOCOL_DEFAULTS.items()))
    def test_protocol_default_values(self, protocol: str, expected: tuple) -> None:
        """Each protocol default must match the pre-rewire literal exactly."""
        from almanak.framework.backtesting.pnl.calculators.liquidation_params import (
            LiquidationParamRegistry,
            LiquidationParamSource,
        )

        threshold, margin, penalty, protocol_field = expected
        registry = LiquidationParamRegistry()
        params = registry.get_params(protocol)

        assert params.liquidation_threshold == threshold, (
            f"{protocol}: threshold {params.liquidation_threshold!r} != {threshold!r}"
        )
        assert params.maintenance_margin == margin, (
            f"{protocol}: margin {params.maintenance_margin!r} != {margin!r}"
        )
        assert params.liquidation_penalty == penalty, (
            f"{protocol}: penalty {params.liquidation_penalty!r} != {penalty!r}"
        )
        assert params.protocol == protocol_field, (
            f"{protocol}: .protocol {params.protocol!r} != {protocol_field!r}"
        )
        assert params.source == LiquidationParamSource.PROTOCOL_DEFAULT, (
            f"{protocol}: source {params.source!r} != PROTOCOL_DEFAULT"
        )


class TestLiquidationAssetParams:
    """Exact-value snapshot for every asset-specific row."""

    def test_all_asset_keys_present(self) -> None:
        """Registry must have exactly the expected (protocol, asset) asset keys."""
        from almanak.framework.backtesting.pnl.calculators.liquidation_params import (
            LiquidationParamRegistry,
        )

        registry = LiquidationParamRegistry()
        assert set(registry.asset_params.keys()) == set(EXPECTED_ASSET_PARAMS.keys())

    @pytest.mark.parametrize("key,expected", list(EXPECTED_ASSET_PARAMS.items()))
    def test_asset_param_values(self, key: tuple, expected: tuple) -> None:
        """Each asset-specific row must match the pre-rewire literal exactly."""
        from almanak.framework.backtesting.pnl.calculators.liquidation_params import (
            LiquidationParamRegistry,
            LiquidationParamSource,
        )

        protocol, asset = key
        threshold, margin, penalty = expected
        registry = LiquidationParamRegistry()
        params = registry.get_params(protocol, asset)

        assert params.liquidation_threshold == threshold, (
            f"{protocol}/{asset}: threshold {params.liquidation_threshold!r} != {threshold!r}"
        )
        assert params.maintenance_margin == margin, (
            f"{protocol}/{asset}: margin {params.maintenance_margin!r} != {margin!r}"
        )
        assert params.liquidation_penalty == penalty, (
            f"{protocol}/{asset}: penalty {params.liquidation_penalty!r} != {penalty!r}"
        )
        assert params.protocol == protocol, (
            f"{protocol}/{asset}: .protocol {params.protocol!r} != {protocol!r}"
        )
        assert params.asset == asset, (
            f"{protocol}/{asset}: .asset {params.asset!r} != {asset!r}"
        )
        assert params.source == LiquidationParamSource.ASSET_SPECIFIC, (
            f"{protocol}/{asset}: source {params.source!r} != ASSET_SPECIFIC"
        )


class TestInterestCalculatorAPYDefaults:
    """Exact-value snapshot for InterestCalculator's default APY dicts."""

    def test_supply_apy_keys(self) -> None:
        """Default supply APY dict must have exactly the expected keys."""
        from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

        calc = InterestCalculator()
        assert set(calc.protocol_supply_apys.keys()) == set(EXPECTED_SUPPLY_APYS.keys())

    def test_borrow_apy_keys(self) -> None:
        """Default borrow APY dict must have exactly the expected keys."""
        from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

        calc = InterestCalculator()
        assert set(calc.protocol_borrow_apys.keys()) == set(EXPECTED_BORROW_APYS.keys())

    @pytest.mark.parametrize("protocol,expected", list(EXPECTED_SUPPLY_APYS.items()))
    def test_supply_apy_values(self, protocol: str, expected: Decimal) -> None:
        """Each supply APY must match the pre-rewire literal exactly."""
        from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

        calc = InterestCalculator()
        assert calc.protocol_supply_apys[protocol] == expected, (
            f"{protocol} supply APY {calc.protocol_supply_apys[protocol]!r} != {expected!r}"
        )

    @pytest.mark.parametrize("protocol,expected", list(EXPECTED_BORROW_APYS.items()))
    def test_borrow_apy_values(self, protocol: str, expected: Decimal) -> None:
        """Each borrow APY must match the pre-rewire literal exactly."""
        from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

        calc = InterestCalculator()
        assert calc.protocol_borrow_apys[protocol] == expected, (
            f"{protocol} borrow APY {calc.protocol_borrow_apys[protocol]!r} != {expected!r}"
        )

    def test_user_supplied_supply_dict_wins(self) -> None:
        """Custom supply APYs passed at construction must override defaults (test_interest.py:807)."""
        from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

        custom = {"my_protocol": Decimal("0.10")}
        calc = InterestCalculator(protocol_supply_apys=custom)
        assert calc.protocol_supply_apys == custom

    def test_user_supplied_borrow_dict_wins(self) -> None:
        """Custom borrow APYs passed at construction must override defaults (test_interest.py:810)."""
        from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

        custom = {"my_protocol": Decimal("0.15")}
        calc = InterestCalculator(protocol_borrow_apys=custom)
        assert calc.protocol_borrow_apys == custom

    def test_to_dict_aave_v3_supply(self) -> None:
        """to_dict() serializes aave_v3 supply APY byte-identically (test_interest.py:786)."""
        from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

        calc = InterestCalculator()
        data = calc.to_dict()
        assert data["protocol_supply_apys"]["aave_v3"] == "0.03"

    def test_to_dict_aave_v3_borrow(self) -> None:
        """to_dict() serializes aave_v3 borrow APY byte-identically (test_interest.py:788)."""
        from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

        calc = InterestCalculator()
        data = calc.to_dict()
        assert data["protocol_borrow_apys"]["aave_v3"] == "0.05"
