"""Tests for case-insensitive collateral key resolution in Compound V3 adapter.

The compiler uppercases token symbols (wstETH -> WSTETH) but COMPOUND_V3_MARKETS
uses mixed-case keys. The adapter must resolve collateral keys case-insensitively.

VIB-1786.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.compound_v3.adapter import (
    COMPOUND_V3_COMET_ADDRESSES,
    COMPOUND_V3_MARKETS,
    CompoundV3Adapter,
)


@pytest.fixture
def adapter():
    """Create a CompoundV3Adapter configured for the Arbitrum WETH market."""
    with patch.object(CompoundV3Adapter, "__init__", lambda self: None):
        a = CompoundV3Adapter.__new__(CompoundV3Adapter)
        a.chain = "arbitrum"
        a.market = "weth"
        a.comet_address = COMPOUND_V3_COMET_ADDRESSES["arbitrum"]["weth"]
        a.market_config = COMPOUND_V3_MARKETS["arbitrum"]["weth"]
        a.wallet_address = "0x" + "0" * 40
        a._token_resolver = None

        # Stub _get_decimals to return 18 for all
        a._get_decimals = MagicMock(return_value=18)
        a._price_oracle = MagicMock(return_value=Decimal("3000"))
        return a


class TestResolveCollateralKey:
    """Test _resolve_collateral_key() method."""

    def test_exact_match(self, adapter):
        assert adapter._resolve_collateral_key("wstETH") == "wstETH"
        assert adapter._resolve_collateral_key("rETH") == "rETH"

    def test_uppercased_match(self, adapter):
        """Compiler sends WSTETH but market has wstETH."""
        assert adapter._resolve_collateral_key("WSTETH") == "wstETH"
        assert adapter._resolve_collateral_key("RETH") == "rETH"

    def test_lowercased_match(self, adapter):
        assert adapter._resolve_collateral_key("wsteth") == "wstETH"
        assert adapter._resolve_collateral_key("reth") == "rETH"

    def test_unknown_collateral_returns_none(self, adapter):
        assert adapter._resolve_collateral_key("USDC") is None
        assert adapter._resolve_collateral_key("UNKNOWN") is None

    def test_uppercase_keys_still_work(self):
        """USDC market uses uppercase keys (WETH, WBTC) — verify no regression."""
        with patch.object(CompoundV3Adapter, "__init__", lambda self: None):
            a = CompoundV3Adapter.__new__(CompoundV3Adapter)
            a.market_config = COMPOUND_V3_MARKETS["arbitrum"]["usdc"]
            assert a._resolve_collateral_key("WETH") == "WETH"
            assert a._resolve_collateral_key("weth") == "WETH"


class TestSupplyCollateralCaseInsensitive:
    """Test that supply_collateral() works with mixed-case symbols."""

    def test_supply_wsteth_uppercase(self, adapter):
        """Compiler sends WSTETH — should resolve to wstETH and succeed."""
        result = adapter.supply_collateral("WSTETH", Decimal("0.05"))
        assert result.success is True
        assert result.tx_data is not None

    def test_supply_reth_uppercase(self, adapter):
        result = adapter.supply_collateral("RETH", Decimal("0.1"))
        assert result.success is True

    def test_supply_exact_case(self, adapter):
        result = adapter.supply_collateral("wstETH", Decimal("0.05"))
        assert result.success is True

    def test_supply_unknown_fails(self, adapter):
        result = adapter.supply_collateral("USDC", Decimal("100"))
        assert result.success is False
        assert "Unsupported collateral" in result.error


class TestWithdrawCollateralCaseInsensitive:
    """Test that withdraw_collateral() works with mixed-case symbols."""

    def test_withdraw_wsteth_uppercase(self, adapter):
        result = adapter.withdraw_collateral("WSTETH", Decimal("0.05"))
        assert result.success is True

    def test_withdraw_all_wsteth_uppercase(self, adapter):
        result = adapter.withdraw_collateral("WSTETH", Decimal("0"), withdraw_all=True)
        assert result.success is True

    def test_withdraw_unknown_fails(self, adapter):
        result = adapter.withdraw_collateral("USDC", Decimal("100"))
        assert result.success is False


class TestGetCollateralInfoCaseInsensitive:
    """Test that get_collateral_info() works with mixed-case symbols."""

    def test_info_uppercase(self, adapter):
        info = adapter.get_collateral_info("WSTETH")
        assert info is not None
        assert info["symbol"] == "wstETH"

    def test_info_exact_case(self, adapter):
        info = adapter.get_collateral_info("wstETH")
        assert info is not None
        assert info["symbol"] == "wstETH"

    def test_info_unknown(self, adapter):
        assert adapter.get_collateral_info("USDC") is None


class TestHealthFactorCaseInsensitive:
    """Test that calculate_health_factor() works with mixed-case collateral keys."""

    def test_health_factor_uppercase_collateral(self, adapter):
        """Health factor calc should work when collateral keys are uppercased."""
        hf = adapter.calculate_health_factor(
            collateral_balances={"WSTETH": Decimal("1.0")},
            borrow_balance=Decimal("0.5"),
        )
        # Should not skip the collateral due to case mismatch
        assert hf.collateral_value_usd > 0

    def test_health_factor_prices_with_resolved_key(self, adapter):
        """Oracle must be called with the resolved key (wstETH), not the input (WSTETH)."""
        adapter._price_oracle = MagicMock(return_value=Decimal("3000"))
        adapter.calculate_health_factor(
            collateral_balances={"WSTETH": Decimal("1.0")},
            borrow_balance=Decimal("0.5"),
        )
        # First call should be for the resolved collateral key, not the uppercased input
        first_call_arg = adapter._price_oracle.call_args_list[0][0][0]
        assert first_call_arg == "wstETH", (
            f"Oracle called with '{first_call_arg}' instead of resolved key 'wstETH'"
        )


class TestAllMarketsCollateralResolution:
    """Verify all collateral keys across all chains resolve case-insensitively."""

    def test_all_collaterals_resolve_uppercased(self):
        """Every collateral key in COMPOUND_V3_MARKETS should resolve when uppercased."""
        with patch.object(CompoundV3Adapter, "__init__", lambda self: None):
            for chain, markets in COMPOUND_V3_MARKETS.items():
                for market_id, market_config in markets.items():
                    a = CompoundV3Adapter.__new__(CompoundV3Adapter)
                    a.market_config = market_config
                    for key in market_config.get("collaterals", {}):
                        resolved = a._resolve_collateral_key(key.upper())
                        assert resolved == key, (
                            f"Failed: {chain}/{market_id} collateral '{key}' not resolved "
                            f"from '{key.upper()}' (got {resolved})"
                        )
