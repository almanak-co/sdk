"""Tests for Velodrome V2 (Optimism) address configuration (VIB-326).

Verifies that the Aerodrome connector works with Optimism chain configuration
for Velodrome V2, which shares the same Solidly-fork interface.
"""

from unittest.mock import MagicMock

import pytest

from almanak.core.contracts import AERODROME
from almanak.framework.connectors.aerodrome.adapter import (
    AerodromeAdapter,
    AerodromeConfig,
)
from almanak.framework.connectors.aerodrome.sdk import AerodromeSDK
from almanak.framework.intents.compiler import LP_POSITION_MANAGERS


# =============================================================================
# Contract Address Tests
# =============================================================================


class TestVelodromeAddressesConfigured:
    """Verify Velodrome V2 addresses are in the AERODROME registry."""

    def test_optimism_key_exists(self):
        """AERODROME dict has an 'optimism' key."""
        assert "optimism" in AERODROME

    def test_optimism_router_address(self):
        """Optimism router is the verified Velodrome V2 Router."""
        assert AERODROME["optimism"]["router"] == "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858"

    def test_optimism_factory_address(self):
        """Optimism factory is the verified Velodrome V2 PoolFactory."""
        assert AERODROME["optimism"]["factory"] == "0xF1046053aa5682b4F9a81b5481394DA16BE5FF5a"

    def test_optimism_voter_address(self):
        """Optimism voter is the verified Velodrome V2 Voter."""
        assert AERODROME["optimism"]["voter"] == "0x41C914ee0c7E1A5edCD0295623e6dC557B5aBf3C"

    def test_base_addresses_unchanged(self):
        """Base (Aerodrome) addresses are not affected."""
        assert AERODROME["base"]["router"] == "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43"
        assert AERODROME["base"]["factory"] == "0x420DD381b31aEf6683db6B902084cB0FFECe40Da"


# =============================================================================
# LP Position Manager Tests
# =============================================================================


class TestLPPositionManagerOptimism:
    """Verify LP_POSITION_MANAGERS includes Aerodrome for Optimism."""

    def test_aerodrome_in_optimism_managers(self):
        """LP_POSITION_MANAGERS['optimism'] includes 'aerodrome'."""
        assert "aerodrome" in LP_POSITION_MANAGERS["optimism"]

    def test_aerodrome_optimism_points_to_router(self):
        """Optimism aerodrome LP manager points to the Velodrome V2 Router."""
        assert LP_POSITION_MANAGERS["optimism"]["aerodrome"] == "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858"

    def test_base_aerodrome_unchanged(self):
        """Base aerodrome LP manager is not affected."""
        assert LP_POSITION_MANAGERS["base"]["aerodrome"] == "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43"

    def test_optimism_uniswap_unchanged(self):
        """Optimism Uniswap V3 LP manager is not affected."""
        assert LP_POSITION_MANAGERS["optimism"]["uniswap_v3"] == "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"


# =============================================================================
# SDK Initialization Tests
# =============================================================================


class TestAerodromeSDKOptimism:
    """Verify AerodromeSDK initializes for Optimism chain."""

    def test_sdk_init_optimism(self):
        """AerodromeSDK accepts chain='optimism'."""
        mock_resolver = MagicMock()
        sdk = AerodromeSDK(chain="optimism", token_resolver=mock_resolver)
        assert sdk.chain == "optimism"
        assert sdk.addresses == AERODROME["optimism"]

    def test_sdk_init_base_still_works(self):
        """AerodromeSDK still works for chain='base'."""
        mock_resolver = MagicMock()
        sdk = AerodromeSDK(chain="base", token_resolver=mock_resolver)
        assert sdk.chain == "base"
        assert sdk.addresses == AERODROME["base"]

    def test_sdk_init_unsupported_chain_raises(self):
        """AerodromeSDK raises ValueError for unsupported chains."""
        mock_resolver = MagicMock()
        with pytest.raises(ValueError, match="Unsupported chain"):
            AerodromeSDK(chain="arbitrum", token_resolver=mock_resolver)

    def test_sdk_router_address_optimism(self):
        """SDK uses Velodrome Router on Optimism."""
        mock_resolver = MagicMock()
        sdk = AerodromeSDK(chain="optimism", token_resolver=mock_resolver)
        assert sdk.addresses["router"] == "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858"

    def test_sdk_factory_address_optimism(self):
        """SDK uses Velodrome Factory on Optimism."""
        mock_resolver = MagicMock()
        sdk = AerodromeSDK(chain="optimism", token_resolver=mock_resolver)
        assert sdk.addresses["factory"] == "0xF1046053aa5682b4F9a81b5481394DA16BE5FF5a"


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestAerodromeAdapterOptimism:
    """Verify AerodromeAdapter initializes for Optimism chain."""

    def test_adapter_config_optimism(self):
        """AerodromeConfig accepts chain='optimism'."""
        config = AerodromeConfig(
            chain="optimism",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,
        )
        assert config.chain == "optimism"

    def test_adapter_init_optimism(self):
        """AerodromeAdapter initializes for Optimism."""
        config = AerodromeConfig(
            chain="optimism",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,
        )
        mock_resolver = MagicMock()
        adapter = AerodromeAdapter(config, token_resolver=mock_resolver)
        assert adapter.chain == "optimism"

    def test_adapter_config_unsupported_chain_raises(self):
        """AerodromeConfig raises ValueError for unsupported chains."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            AerodromeConfig(
                chain="arbitrum",
                wallet_address="0x1234567890123456789012345678901234567890",
                allow_placeholder_prices=True,
            )
