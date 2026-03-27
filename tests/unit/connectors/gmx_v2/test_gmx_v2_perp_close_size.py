"""Tests for GMX V2 PERP_CLOSE on-chain size query (VIB-1946).

Verifies that the compiler reads exact position size from on-chain instead
of using max-uint sentinel which burns keeper execution fees.
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig


@pytest.fixture
def compiler():
    """Create a minimal IntentCompiler for GMX V2 tests."""
    return IntentCompiler(
        chain="arbitrum",
        wallet_address="0x" + "ab" * 20,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


class TestGetGmxPositionSizeOnchain:
    """Test _get_gmx_position_size_onchain helper."""

    def test_returns_exact_size_for_matching_position(self, compiler):
        """Should return the on-chain size_in_usd for the matching position."""
        mock_sdk = MagicMock()
        mock_sdk.get_account_positions.return_value = [
            {
                "market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
                "collateral_token": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "is_long": True,
                "size_in_usd": 4332882579080000000000000000000,  # ~$4.33 in 30 decimals
            }
        ]

        result = compiler._get_gmx_position_size_onchain(
            sdk=mock_sdk,
            market_address="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            is_long=True,
        )

        assert result == 4332882579080000000000000000000

    def test_returns_none_when_no_positions(self, compiler):
        """Should return None when wallet has no GMX V2 positions."""
        mock_sdk = MagicMock()
        mock_sdk.get_account_positions.return_value = []

        result = compiler._get_gmx_position_size_onchain(
            sdk=mock_sdk,
            market_address="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            is_long=True,
        )

        assert result is None

    def test_returns_none_when_position_query_fails(self, compiler):
        """Should return None (not raise) when position query fails."""
        from almanak.framework.connectors.gmx_v2.sdk import PositionQueryError

        mock_sdk = MagicMock()
        mock_sdk.get_account_positions.side_effect = PositionQueryError("Reader reverted")

        result = compiler._get_gmx_position_size_onchain(
            sdk=mock_sdk,
            market_address="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            is_long=True,
        )

        assert result is None

    def test_no_match_when_direction_differs(self, compiler):
        """Should not match a SHORT position when looking for LONG."""
        mock_sdk = MagicMock()
        mock_sdk.get_account_positions.return_value = [
            {
                "market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
                "collateral_token": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "is_long": False,  # SHORT, not LONG
                "size_in_usd": 4332882579080000000000000000000,
            }
        ]

        result = compiler._get_gmx_position_size_onchain(
            sdk=mock_sdk,
            market_address="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            is_long=True,
        )

        assert result is None

    def test_case_insensitive_address_matching(self, compiler):
        """Address matching should be case-insensitive."""
        mock_sdk = MagicMock()
        mock_sdk.get_account_positions.return_value = [
            {
                "market": "0x70D95587D40A2CAF56BD97485AB3EEC10BEE6336",  # uppercase
                "collateral_token": "0xAF88D065E77C8CC2239327C5EDB3A432268E5831",
                "is_long": True,
                "size_in_usd": 5000000000000000000000000000000000,  # $5000
            }
        ]

        result = compiler._get_gmx_position_size_onchain(
            sdk=mock_sdk,
            market_address="0x70d95587d40a2caf56bd97485ab3eec10bee6336",  # lowercase
            collateral_address="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            is_long=True,
        )

        assert result == 5000000000000000000000000000000000

    def test_skips_zero_size_positions(self, compiler):
        """Should skip positions with zero size_in_usd."""
        mock_sdk = MagicMock()
        mock_sdk.get_account_positions.return_value = [
            {
                "market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
                "collateral_token": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "is_long": True,
                "size_in_usd": 0,  # Closed position
            }
        ]

        result = compiler._get_gmx_position_size_onchain(
            sdk=mock_sdk,
            market_address="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            is_long=True,
        )

        assert result is None
