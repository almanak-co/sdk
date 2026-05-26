"""Tests for Compound V3 WETH market on Arbitrum.

Validates WETH Comet market configuration and the fork_manager infrastructure
fix found in Kitchen Loop iteration 147 (first WETH market Anvil run).

Coverage:
- WETH market config exists and is complete for Arbitrum
- wstETH collateral is registered with correct address and BCF
- WETH market only accepts LST collateral (not USDC/WBTC)
- CompoundV3Config validates weth as a valid market
- fork_manager has wstETH slot 1 for Arbitrum (discovered via brute-force iter 147)
- TOKEN_ADDRESSES uses consistent mixed-case 'wstETH' for Arbitrum
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors.compound_v3.adapter import (
    COMPOUND_V3_COMET_ADDRESSES,
    COMPOUND_V3_MARKETS,
    CompoundV3Config,
)


TEST_WALLET = "0x1234567890123456789012345678901234567890"
ARBITRUM_WETH_COMET = "0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486"
WSTETH_ARBITRUM = "0x5979D7b546E38E414F7E9822514be443A4800529"


# =============================================================================
# Market Configuration Tests
# =============================================================================


class TestWETHMarketConfig:
    """Validate WETH market is correctly configured for Arbitrum."""

    def test_weth_comet_address_exists(self):
        assert "weth" in COMPOUND_V3_COMET_ADDRESSES.get("arbitrum", {})

    def test_weth_comet_address_is_correct(self):
        assert COMPOUND_V3_COMET_ADDRESSES["arbitrum"]["weth"] == ARBITRUM_WETH_COMET

    def test_weth_market_config_exists(self):
        assert "weth" in COMPOUND_V3_MARKETS.get("arbitrum", {})

    def test_weth_market_base_token_is_weth(self):
        market = COMPOUND_V3_MARKETS["arbitrum"]["weth"]
        assert market["base_token"] == "WETH"

    def test_weth_market_base_token_address(self):
        """WETH on Arbitrum: 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1."""
        market = COMPOUND_V3_MARKETS["arbitrum"]["weth"]
        assert market["base_token_address"] == "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

    def test_wsteth_is_valid_collateral(self):
        market = COMPOUND_V3_MARKETS["arbitrum"]["weth"]
        assert "wstETH" in market["collaterals"]

    def test_reth_is_valid_collateral(self):
        market = COMPOUND_V3_MARKETS["arbitrum"]["weth"]
        assert "rETH" in market["collaterals"]

    def test_wsteth_collateral_address(self):
        """wstETH on Arbitrum: 0x5979D7b546E38E414F7E9822514be443A4800529."""
        market = COMPOUND_V3_MARKETS["arbitrum"]["weth"]
        assert market["collaterals"]["wstETH"]["address"] == WSTETH_ARBITRUM

    def test_weth_market_does_not_accept_usdc_collateral(self):
        """WETH market only accepts LST collateral, not USDC/WBTC."""
        market = COMPOUND_V3_MARKETS["arbitrum"]["weth"]
        assert "USDC" not in market["collaterals"]
        assert "WBTC" not in market["collaterals"]

    def test_wsteth_borrow_collateral_factor(self):
        """wstETH BCF = 90% (high LTV for LST/WETH strong correlation)."""
        market = COMPOUND_V3_MARKETS["arbitrum"]["weth"]
        assert market["collaterals"]["wstETH"]["borrow_collateral_factor"] == Decimal("0.90")

    def test_reth_borrow_collateral_factor(self):
        """rETH BCF = 90% (same as wstETH for LST category)."""
        market = COMPOUND_V3_MARKETS["arbitrum"]["weth"]
        assert market["collaterals"]["rETH"]["borrow_collateral_factor"] == Decimal("0.90")


# =============================================================================
# CompoundV3Config Validation
# =============================================================================


class TestWETHMarketConfigValidation:
    """Validate CompoundV3Config accepts and rejects markets correctly."""

    def test_config_accepts_weth_market(self):
        config = CompoundV3Config(chain="arbitrum", wallet_address=TEST_WALLET, market="weth")
        assert config.market == "weth"
        assert config.chain == "arbitrum"

    def test_config_rejects_cbeth_market(self):
        """cbETH is not a configured market on Arbitrum."""
        with pytest.raises(ValueError, match="Invalid market"):
            CompoundV3Config(chain="arbitrum", wallet_address=TEST_WALLET, market="cbeth")

    def test_weth_market_distinct_from_usdc(self):
        """Ensure weth and usdc markets have different Comet addresses."""
        weth_addr = COMPOUND_V3_COMET_ADDRESSES["arbitrum"]["weth"]
        usdc_addr = COMPOUND_V3_COMET_ADDRESSES["arbitrum"]["usdc"]
        assert weth_addr != usdc_addr


# =============================================================================
# fork_manager wstETH Infrastructure
# =============================================================================


class TestForkManagerWstETHSlot:
    """Validate wstETH storage slot is registered for Arbitrum.

    Slot 1 was discovered via brute-force probing during iter 147 Anvil run.
    This prevents slow brute-force probing on every run.
    """

    def test_wsteth_slot_registered_for_arbitrum(self):
        from almanak.framework.anvil.fork_manager import KNOWN_BALANCE_SLOTS

        assert "wstETH" in KNOWN_BALANCE_SLOTS.get("arbitrum", {})

    def test_wsteth_slot_is_1(self):
        """Slot 1 confirmed via brute-force in iter 147."""
        from almanak.framework.anvil.fork_manager import KNOWN_BALANCE_SLOTS

        assert KNOWN_BALANCE_SLOTS["arbitrum"]["wstETH"] == 1

    def test_wsteth_token_address_exists_for_arbitrum(self):
        from almanak.framework.anvil.fork_manager import TOKEN_ADDRESSES

        arb_tokens = TOKEN_ADDRESSES.get("arbitrum", {})
        assert "wstETH" in arb_tokens

    def test_wsteth_address_is_correct(self):
        from almanak.framework.anvil.fork_manager import TOKEN_ADDRESSES

        assert TOKEN_ADDRESSES["arbitrum"]["wstETH"] == WSTETH_ARBITRUM

    def test_wsteth_decimals_registered(self):
        """wstETH has 18 decimals."""
        from almanak.framework.anvil.fork_manager import TOKEN_DECIMALS

        # Can be looked up either as 'wstETH' or via case-insensitive fallback
        decimals = TOKEN_DECIMALS.get("wstETH") or TOKEN_DECIMALS.get("WSTETH")
        assert decimals == 18
