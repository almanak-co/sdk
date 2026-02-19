"""Unit tests for deferred transaction refresh logic.

Tests the refresh_deferred_bundle() function which re-fetches fresh calldata
from aggregator protocols (LiFi, Enso) immediately before execution.
"""

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.execution.deferred_refresh import refresh_deferred_bundle
from almanak.framework.models.reproduction_bundle import ActionBundle

WALLET = "0x1234567890abcdef1234567890abcdef12345678"


def _make_approve_tx() -> dict:
    """Create a standard ERC-20 approve transaction dict."""
    return {
        "to": "0xTokenAddress",
        "value": "0",
        "data": "0x095ea7b3_approve_calldata",
        "gas_estimate": 50000,
        "description": "Approve USDC",
        "tx_type": "approve",
    }


def _make_lifi_bundle(deferred: bool = True) -> ActionBundle:
    """Create a LiFi ActionBundle with optional deferred swap."""
    approve_tx = _make_approve_tx()
    swap_tx = {
        "to": "0xLiFiDiamond",
        "value": "0",
        "data": "0xstale_lifi_calldata",
        "gas_estimate": 200000,
        "description": "Swap via LiFi",
        "tx_type": "swap_deferred" if deferred else "swap",
    }
    metadata = {
        "from_token": {"symbol": "USDC", "address": "0xUSDC", "chain": "arbitrum"},
        "to_token": {"symbol": "WETH", "address": "0xWETH", "chain": "arbitrum"},
        "amount_in": "100000000",
        "protocol": "lifi",
        "deferred_swap": deferred,
        "route_params": {
            "from_chain_id": 42161,
            "to_chain_id": 42161,
            "from_token": "0xUSDC",
            "to_token": "0xWETH",
            "from_amount": "100000000",
            "from_address": WALLET,
            "slippage": 0.05,
        },
    }
    return ActionBundle(
        intent_type="SWAP",
        transactions=[approve_tx, swap_tx],
        metadata=metadata,
    )


def _make_enso_bundle() -> ActionBundle:
    """Create an Enso ActionBundle with deferred swap."""
    approve_tx = _make_approve_tx()
    swap_tx = {
        "to": "0xEnsoRouter",
        "value": "0",
        "data": "0xstale_enso_calldata",
        "gas_estimate": 200000,
        "description": "Swap via Enso",
        "tx_type": "swap_deferred",
    }
    metadata = {
        "from_token": {"symbol": "USDC", "address": "0xUSDC", "chain": "arbitrum"},
        "to_token": {"symbol": "WETH", "address": "0xWETH", "chain": "arbitrum"},
        "amount_in": "100000000",
        "protocol": "enso",
        "chain": "arbitrum",
        "deferred_swap": True,
        "route_params": {
            "token_in": "0xUSDC",
            "token_out": "0xWETH",
            "amount_in": 100000000,
            "slippage_bps": 500,
        },
    }
    return ActionBundle(
        intent_type="SWAP",
        transactions=[approve_tx, swap_tx],
        metadata=metadata,
    )


class TestDeferredRefresh:
    """Tests for refresh_deferred_bundle()."""

    def test_non_deferred_bundle_passes_through(self):
        """Non-deferred bundles are returned unchanged (zero overhead path)."""
        bundle = _make_lifi_bundle(deferred=False)
        # Remove deferred_swap from metadata
        bundle.metadata["deferred_swap"] = False

        result = refresh_deferred_bundle(bundle, WALLET)

        # Should be the exact same object (not a copy)
        assert result is bundle
        assert result.transactions[1]["data"] == "0xstale_lifi_calldata"

    @patch("almanak.framework.execution.deferred_refresh._refresh_lifi")
    def test_lifi_deferred_bundle_gets_refreshed(self, mock_refresh_lifi):
        """LiFi deferred bundle gets fresh transaction data."""
        mock_refresh_lifi.return_value = {
            "to": "0xNewLiFiTarget",
            "value": 0,
            "data": "0xfresh_lifi_calldata",
            "gas_estimate": 250000,
            "description": "Swap USDC -> WETH via LiFi",
            "tx_type": "swap",
        }

        bundle = _make_lifi_bundle(deferred=True)
        result = refresh_deferred_bundle(bundle, WALLET)

        # Should be a different object
        assert result is not bundle

        # Approve TX should be untouched
        assert result.transactions[0]["data"] == "0x095ea7b3_approve_calldata"
        assert result.transactions[0]["tx_type"] == "approve"

        # Swap TX should be updated with fresh data
        swap_tx = result.transactions[1]
        assert swap_tx["to"] == "0xNewLiFiTarget"
        assert swap_tx["data"] == "0xfresh_lifi_calldata"
        assert swap_tx["gas_estimate"] == 250000
        assert swap_tx["tx_type"] == "swap"  # _deferred suffix stripped
        assert swap_tx["description"] == "Swap USDC -> WETH via LiFi"

        # Original bundle should be unchanged
        assert bundle.transactions[1]["data"] == "0xstale_lifi_calldata"
        assert bundle.transactions[1]["tx_type"] == "swap_deferred"

    @patch("almanak.framework.execution.deferred_refresh._refresh_enso")
    def test_enso_deferred_bundle_gets_refreshed(self, mock_refresh_enso):
        """Enso deferred bundle gets fresh transaction data."""
        mock_refresh_enso.return_value = {
            "to": "0xNewEnsoRouter",
            "value": 0,
            "data": "0xfresh_enso_calldata",
            "gas_estimate": 180000,
            "tx_type": "swap",
        }

        bundle = _make_enso_bundle()
        result = refresh_deferred_bundle(bundle, WALLET)

        assert result is not bundle

        swap_tx = result.transactions[1]
        assert swap_tx["to"] == "0xNewEnsoRouter"
        assert swap_tx["data"] == "0xfresh_enso_calldata"
        assert swap_tx["tx_type"] == "swap"  # _deferred suffix stripped

    def test_unknown_protocol_passes_through_with_warning(self):
        """Unknown protocol with deferred_swap=True passes through unchanged."""
        bundle = _make_lifi_bundle(deferred=True)
        bundle.metadata["protocol"] = "unknown_dex"

        result = refresh_deferred_bundle(bundle, WALLET)

        # Should be the original bundle (no copy needed since no refresh happened)
        assert result is bundle
        assert result.transactions[1]["data"] == "0xstale_lifi_calldata"
        assert result.transactions[1]["tx_type"] == "swap_deferred"

    @patch("almanak.framework.execution.deferred_refresh._refresh_lifi")
    def test_refresh_failure_falls_back_to_stale_data(self, mock_refresh_lifi):
        """When refresh fails, the original bundle is returned (no crash)."""
        mock_refresh_lifi.side_effect = Exception("API timeout")

        bundle = _make_lifi_bundle(deferred=True)
        result = refresh_deferred_bundle(bundle, WALLET)

        # Should return the original bundle unchanged
        assert result is bundle
        assert result.transactions[1]["data"] == "0xstale_lifi_calldata"

    def test_missing_route_params_passes_through(self):
        """Bundle with deferred_swap but no route_params passes through."""
        bundle = _make_lifi_bundle(deferred=True)
        del bundle.metadata["route_params"]

        result = refresh_deferred_bundle(bundle, WALLET)

        assert result is bundle
        assert result.transactions[1]["data"] == "0xstale_lifi_calldata"

    @patch("almanak.framework.execution.deferred_refresh._refresh_lifi")
    def test_bridge_deferred_tx_type_is_handled(self, mock_refresh_lifi):
        """Bridge transactions with _deferred suffix are also refreshed."""
        mock_refresh_lifi.return_value = {
            "to": "0xBridgeTarget",
            "value": 0,
            "data": "0xfresh_bridge_calldata",
            "gas_estimate": 300000,
            "tx_type": "bridge",
        }

        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[1]["tx_type"] = "bridge_deferred"

        result = refresh_deferred_bundle(bundle, WALLET)

        swap_tx = result.transactions[1]
        assert swap_tx["tx_type"] == "bridge"  # _deferred suffix stripped
        assert swap_tx["data"] == "0xfresh_bridge_calldata"
