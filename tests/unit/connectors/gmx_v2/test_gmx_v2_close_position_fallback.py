"""Tests for GMX V2 close_position full-close fallback (VIB-1886).

When size_delta_usd=None (close full position) and no cached position
exists, the adapter should use a large sentinel value instead of failing.
GMX V2 contracts clamp sizeDeltaUsd to position.sizeInUsd on-chain.
"""

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.connectors.gmx_v2.adapter import GMX_V2_MAX_CLOSE_SIZE_USD, GMXv2Adapter, GMXv2Config


@pytest.fixture
def adapter():
    """Create a GMXv2Adapter for Arbitrum with empty position cache."""
    config = GMXv2Config(
        chain="arbitrum",
        wallet_address="0x" + "ab" * 20,
        default_slippage_bps=30,
    )
    return GMXv2Adapter(config)


class TestClosePositionFullCloseFallback:
    """Verify close_position handles size_delta_usd=None without cached positions."""

    def test_close_full_position_without_cached_position_succeeds(self, adapter):
        """close_position(size_delta_usd=None) should NOT fail when _positions is empty.

        Previously returned: OrderResult(success=False, error="No size specified and no existing position found")
        Now should: use a sentinel large value and succeed.
        """
        assert adapter._positions == {}, "Precondition: positions cache must be empty"

        result = adapter.close_position(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
            size_delta_usd=None,  # "close full position"
        )

        assert result.success is True, (
            f"close_position(size_delta_usd=None) should succeed with sentinel value, "
            f"but got error: {result.error}"
        )
        assert result.order_key is not None

    def test_close_explicit_size_still_works(self, adapter):
        """Explicit size_delta_usd should work as before, no fallback needed."""
        result = adapter.close_position(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
            size_delta_usd=Decimal("1000"),
        )

        assert result.success is True
        assert result.order is not None
        assert result.order.size_delta_usd == Decimal("1000")

    def test_close_full_uses_sentinel_value_larger_than_any_position(self, adapter):
        """The sentinel value should be large enough that GMX clamps it to position size."""
        result = adapter.close_position(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
            size_delta_usd=None,
        )

        assert result.success is True
        # The order should have the sentinel value ($1 trillion)
        assert result.order.size_delta_usd == GMX_V2_MAX_CLOSE_SIZE_USD

    def test_close_with_cached_position_uses_position_size(self, adapter):
        """When a cached position exists, close_position should use its size."""
        from almanak.framework.connectors.gmx_v2.adapter import GMXv2Position

        # Simulate a cached position
        market_address = adapter._resolve_market("ETH/USD")
        collateral_address = adapter._resolve_token("USDC")
        position_key = adapter._get_position_key(market_address, collateral_address, True)
        adapter._positions[position_key] = GMXv2Position(
            position_key=position_key,
            market=market_address,
            collateral_token=collateral_address,
            is_long=True,
            size_in_usd=Decimal("5000"),
            size_in_tokens=Decimal("2.5"),
            collateral_amount=Decimal("5000"),
            entry_price=Decimal("2000"),
        )

        result = adapter.close_position(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
            size_delta_usd=None,
        )

        assert result.success is True
        assert result.order.size_delta_usd == Decimal("5000")


class TestKeeperFeeWarning:
    """Verify close_position warns about keeper execution fees."""

    def test_close_position_logs_keeper_fee_warning(self, adapter, caplog):
        """close_position should log a warning about keeper fee costs."""
        import logging

        with caplog.at_level(logging.WARNING):
            result = adapter.close_position(
                market="ETH/USD",
                collateral_token="USDC",
                is_long=True,
                size_delta_usd=Decimal("1000"),
            )

        assert result.success is True
        keeper_warnings = [r for r in caplog.records if "keeper execution fee" in r.message]
        assert len(keeper_warnings) > 0, "Expected a warning about keeper execution fee"
