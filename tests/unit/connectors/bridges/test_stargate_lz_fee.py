"""Tests for Stargate bridge LayerZero fee estimation."""

from decimal import Decimal

import pytest

from almanak.framework.connectors.bridges.stargate.adapter import StargateBridgeAdapter


@pytest.fixture
def adapter():
    """Create a StargateBridgeAdapter instance for testing."""
    return StargateBridgeAdapter()


class TestLayerZeroFeeEstimation:
    """Verify _estimate_layerzero_fee returns safe overestimates."""

    def test_avalanche_to_base_fee_sufficient(self, adapter):
        """Avalanche->Base fee must exceed observed 0.00627 AVAX on-chain fee."""
        fee = adapter._estimate_layerzero_fee("avalanche", "base", "USDC", 10**6)
        # Observed on-chain: ~0.00627 AVAX. Must be above that.
        assert fee > Decimal("0.00627"), f"Fee {fee} too low for Avalanche->Base"

    def test_avalanche_to_ethereum_fee_sufficient(self, adapter):
        """Avalanche->Ethereum should be higher than Avalanche->L2."""
        fee = adapter._estimate_layerzero_fee("avalanche", "ethereum", "USDC", 10**6)
        assert fee > Decimal("0.01"), f"Fee {fee} too low for Avalanche->Ethereum"

    def test_eth_l2_to_l2_fee_reasonable(self, adapter):
        """ETH L2->L2 routes should be moderate (paid in ETH)."""
        fee = adapter._estimate_layerzero_fee("base", "arbitrum", "USDC", 10**6)
        assert fee > Decimal("0.0005"), f"Fee {fee} too low for Base->Arbitrum"
        # Should not be absurdly high either
        assert fee < Decimal("1.0"), f"Fee {fee} unreasonably high"

    def test_polygon_fee_in_matic(self, adapter):
        """Polygon routes should reflect MATIC-denominated fees (higher numeric)."""
        fee = adapter._estimate_layerzero_fee("polygon", "base", "USDC", 10**6)
        # MATIC fees are higher in nominal terms than ETH fees
        assert fee > Decimal("0.1"), f"Fee {fee} too low for Polygon (should be in MATIC)"

    def test_bsc_fee_in_bnb(self, adapter):
        """BSC routes should reflect BNB-denominated fees."""
        fee = adapter._estimate_layerzero_fee("bsc", "base", "USDC", 10**6)
        assert fee > Decimal("0.001"), f"Fee {fee} too low for BSC"

    def test_large_transfer_surcharge(self, adapter):
        """Large transfers (>10^24 wei) get a 1.2x surcharge."""
        small_fee = adapter._estimate_layerzero_fee("base", "arbitrum", "USDC", 10**6)
        large_fee = adapter._estimate_layerzero_fee("base", "arbitrum", "USDC", 10**25)
        assert large_fee > small_fee, "Large transfer should cost more"
        # Surcharge is 1.2x
        ratio = large_fee / small_fee
        assert abs(ratio - Decimal("1.2")) < Decimal("0.01"), f"Surcharge ratio {ratio} != 1.2x"

    def test_unknown_route_uses_safe_default(self, adapter):
        """Unknown routes should still return a non-zero fee."""
        fee = adapter._estimate_layerzero_fee("sonic", "base", "USDC", 10**6)
        assert fee > Decimal("0"), "Unknown route must have non-zero fee"
        assert fee >= Decimal("0.003"), "Default should be conservative"

    def test_fee_always_positive(self, adapter):
        """Fee must be positive for all known chains."""
        chains = ["ethereum", "arbitrum", "optimism", "base", "avalanche", "polygon", "bsc"]
        for src in chains:
            for dst in chains:
                if src == dst:
                    continue
                fee = adapter._estimate_layerzero_fee(src, dst, "USDC", 10**6)
                assert fee > Decimal("0"), f"Fee for {src}->{dst} must be positive"

    def test_safety_multiplier_applied(self, adapter):
        """The 3x safety multiplier must be present in the output."""
        # Avalanche->Base route fee is 0.007 * 3 = 0.021
        fee = adapter._estimate_layerzero_fee("avalanche", "base", "USDC", 10**6)
        assert fee == Decimal("0.021"), f"Expected 0.007 * 3 = 0.021, got {fee}"
