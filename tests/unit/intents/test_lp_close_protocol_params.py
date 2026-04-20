"""Unit tests for LPCloseIntent and CollectFeesIntent protocol_params field.

Validates that protocol_params can be passed to these intents for
protocols like Uniswap V4 that require additional on-chain data
(liquidity, currency addresses, position_id) for compilation.
"""

from decimal import Decimal

import pytest

from almanak.framework.intents.vocabulary import CollectFeesIntent, LPCloseIntent


class TestLPCloseIntentProtocolParams:
    """Test LPCloseIntent accepts protocol_params for V4 LP close."""

    def test_lp_close_with_protocol_params(self):
        """LPCloseIntent should accept protocol_params dict."""
        intent = LPCloseIntent(
            position_id="12345",
            pool="WETH/USDC/3000",
            protocol="uniswap_v4",
            chain="ethereum",
            protocol_params={
                "liquidity": 1000000,
                "currency0": "0x0000000000000000000000000000000000000000",
                "currency1": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            },
        )

        assert intent.protocol_params is not None
        assert intent.protocol_params["liquidity"] == 1000000
        assert intent.protocol_params["currency0"] == "0x0000000000000000000000000000000000000000"
        assert intent.protocol_params["currency1"] == "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

    def test_lp_close_without_protocol_params(self):
        """LPCloseIntent should work without protocol_params (backward compat)."""
        intent = LPCloseIntent(
            position_id="12345",
            pool="WETH/USDC/3000",
            protocol="uniswap_v3",
        )

        assert intent.protocol_params is None

    def test_lp_close_immutable(self):
        """LPCloseIntent with protocol_params should be immutable."""
        intent = LPCloseIntent(
            position_id="12345",
            protocol_params={"liquidity": 100},
        )

        with pytest.raises(Exception):
            intent.protocol_params = {"liquidity": 200}

    def test_lp_close_serialization(self):
        """LPCloseIntent with protocol_params should serialize correctly."""
        intent = LPCloseIntent(
            position_id="12345",
            pool="WETH/USDC/3000",
            protocol="uniswap_v4",
            protocol_params={"liquidity": 5000, "currency0": "0xabc", "currency1": "0xdef"},
        )

        serialized = intent.serialize()
        assert serialized["protocol_params"]["liquidity"] == 5000
        assert serialized["type"] == "LP_CLOSE"

    def test_lp_close_deserialization(self):
        """LPCloseIntent should deserialize protocol_params from dict."""
        data = {
            "type": "LP_CLOSE",
            "position_id": "12345",
            "pool": "WETH/USDC/3000",
            "protocol": "uniswap_v4",
            "protocol_params": {"liquidity": 5000},
        }

        intent = LPCloseIntent.deserialize(data)
        assert intent.protocol_params is not None
        assert intent.protocol_params["liquidity"] == 5000


class TestCollectFeesIntentProtocolParams:
    """Test CollectFeesIntent accepts protocol_params for V4 fee collection."""

    def test_collect_fees_with_protocol_params(self):
        """CollectFeesIntent should accept protocol_params dict."""
        intent = CollectFeesIntent(
            pool="WETH/USDC/3000",
            protocol="uniswap_v4",
            chain="ethereum",
            protocol_params={
                "position_id": 12345,
                "currency0": "0x0000000000000000000000000000000000000000",
                "currency1": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            },
        )

        assert intent.protocol_params is not None
        assert intent.protocol_params["position_id"] == 12345

    def test_collect_fees_without_protocol_params(self):
        """CollectFeesIntent should work without protocol_params (backward compat)."""
        intent = CollectFeesIntent(
            pool="WAVAX/USDC/20",
            protocol="traderjoe_v2",
        )

        assert intent.protocol_params is None

    def test_collect_fees_immutable(self):
        """CollectFeesIntent with protocol_params should be immutable."""
        intent = CollectFeesIntent(
            pool="WETH/USDC/3000",
            protocol_params={"position_id": 100},
        )

        with pytest.raises(Exception):
            intent.protocol_params = {"position_id": 200}
