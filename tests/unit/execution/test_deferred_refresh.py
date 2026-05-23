"""Unit tests for deferred transaction refresh logic.

Tests the refresh_deferred_bundle() function which re-fetches fresh calldata
from aggregator protocols (LiFi, Enso) immediately before execution.
"""

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.execution import deferred_refresh
from almanak.framework.execution.deferred_refresh import (
    _ANVIL_MIN_SLIPPAGE_BPS,
    refresh_deferred_bundle,
)
from almanak.framework.execution.simulator.config import is_local_rpc
from almanak.framework.models.reproduction_bundle import ActionBundle


def _patch_refresher(protocol: str, mock):
    """Patch the dispatch entry for a protocol in _DEFERRED_REFRESHERS."""
    return patch.dict(deferred_refresh._DEFERRED_REFRESHERS, {protocol: mock})

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
            "amount_in": "100000000",
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

    def test_lifi_deferred_bundle_gets_refreshed(self):
        """LiFi deferred bundle gets fresh transaction data."""
        mock_refresh_lifi = MagicMock(return_value={
            "to": "0xNewLiFiTarget",
            "value": 0,
            "data": "0xfresh_lifi_calldata",
            "gas_estimate": 250000,
            "description": "Swap USDC -> WETH via LiFi",
            "tx_type": "swap",
        })

        bundle = _make_lifi_bundle(deferred=True)
        with _patch_refresher("lifi", mock_refresh_lifi):
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

    def test_enso_deferred_bundle_gets_refreshed(self):
        """Enso deferred bundle gets fresh transaction data."""
        mock_refresh_enso = MagicMock(return_value={
            "to": "0xNewEnsoRouter",
            "value": 0,
            "data": "0xfresh_enso_calldata",
            "gas_estimate": 180000,
            "tx_type": "swap",
        })

        bundle = _make_enso_bundle()
        with _patch_refresher("enso", mock_refresh_enso):
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

    def test_refresh_failure_falls_back_to_stale_data(self):
        """When refresh fails, the original bundle is returned (no crash)."""
        mock_refresh_lifi = MagicMock(side_effect=Exception("API timeout"))

        bundle = _make_lifi_bundle(deferred=True)
        with _patch_refresher("lifi", mock_refresh_lifi):
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

    def test_anvil_widens_enso_slippage(self):
        """On Anvil forks, slippage_bps is widened to _ANVIL_MIN_SLIPPAGE_BPS."""
        mock_refresh_enso = MagicMock(return_value={
            "to": "0xEnsoRouter",
            "value": 0,
            "data": "0xfresh_calldata",
            "gas_estimate": 180000,
            "tx_type": "swap",
        })

        bundle = _make_enso_bundle()
        # Set tight slippage (50 bps = 0.5%)
        bundle.metadata["route_params"]["slippage_bps"] = 50

        with _patch_refresher("enso", mock_refresh_enso):
            result = refresh_deferred_bundle(
                bundle, WALLET, rpc_url="http://localhost:8545"
            )

        # Slippage should have been widened in the result
        assert result.metadata["route_params"]["slippage_bps"] == _ANVIL_MIN_SLIPPAGE_BPS
        # Original bundle must NOT be mutated
        assert bundle.metadata["route_params"]["slippage_bps"] == 50
        mock_refresh_enso.assert_called_once()
        # Verify widened slippage was passed to the API call (not applied after)
        called_metadata = mock_refresh_enso.call_args[0][0]
        assert called_metadata["route_params"]["slippage_bps"] == _ANVIL_MIN_SLIPPAGE_BPS

    def test_anvil_keeps_wide_slippage_unchanged(self):
        """If slippage is already >= _ANVIL_MIN_SLIPPAGE_BPS, don't change it."""
        mock_refresh_enso = MagicMock(return_value={
            "to": "0xEnsoRouter",
            "value": 0,
            "data": "0xfresh_calldata",
            "gas_estimate": 180000,
            "tx_type": "swap",
        })

        bundle = _make_enso_bundle()
        # Already wide slippage (1000 bps = 10%)
        bundle.metadata["route_params"]["slippage_bps"] = 1000

        with _patch_refresher("enso", mock_refresh_enso):
            result = refresh_deferred_bundle(
                bundle, WALLET, rpc_url="http://127.0.0.1:8545"
            )

        # Should not widen further
        assert result.metadata["route_params"]["slippage_bps"] == 1000
        # Verify original wide slippage was passed to API call unchanged
        called_metadata = mock_refresh_enso.call_args[0][0]
        assert called_metadata["route_params"]["slippage_bps"] == 1000

    def test_mainnet_rpc_does_not_widen_slippage(self):
        """Mainnet RPC URLs should NOT trigger slippage widening."""
        mock_refresh_enso = MagicMock(return_value={
            "to": "0xEnsoRouter",
            "value": 0,
            "data": "0xfresh_calldata",
            "gas_estimate": 180000,
            "tx_type": "swap",
        })

        bundle = _make_enso_bundle()
        bundle.metadata["route_params"]["slippage_bps"] = 50

        with _patch_refresher("enso", mock_refresh_enso):
            result = refresh_deferred_bundle(
                bundle, WALLET, rpc_url="https://arb-mainnet.g.alchemy.com/v2/key"
            )

        # Slippage should NOT have been widened
        assert result.metadata["route_params"]["slippage_bps"] == 50
        # Verify original tight slippage was passed to API call unchanged
        called_metadata = mock_refresh_enso.call_args[0][0]
        assert called_metadata["route_params"]["slippage_bps"] == 50

    def test_lifi_on_anvil_does_not_widen_slippage(self):
        """LiFi bundles on Anvil should NOT trigger Enso slippage widening."""
        mock_refresh_lifi = MagicMock(return_value={
            "to": "0xLiFiRouter",
            "value": 0,
            "data": "0xfresh_lifi_calldata",
            "gas_estimate": 200000,
            "tx_type": "swap",
        })

        bundle = _make_lifi_bundle(deferred=True)
        # LiFi uses "slippage" not "slippage_bps"
        bundle.metadata["route_params"]["slippage"] = 0.005

        with _patch_refresher("lifi", mock_refresh_lifi):
            result = refresh_deferred_bundle(
                bundle, WALLET, rpc_url="http://localhost:8545"
            )

        # Should succeed without crashing; slippage unchanged
        assert result.metadata["route_params"]["slippage"] == 0.005
        assert "slippage_bps" not in result.metadata["route_params"]

    def test_bridge_deferred_tx_type_is_handled(self):
        """Bridge transactions with _deferred suffix are also refreshed."""
        mock_refresh_lifi = MagicMock(return_value={
            "to": "0xBridgeTarget",
            "value": 0,
            "data": "0xfresh_bridge_calldata",
            "gas_estimate": 300000,
            "tx_type": "bridge",
        })

        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[1]["tx_type"] = "bridge_deferred"

        with _patch_refresher("lifi", mock_refresh_lifi):
            result = refresh_deferred_bundle(bundle, WALLET)

        swap_tx = result.transactions[1]
        assert swap_tx["tx_type"] == "bridge"  # _deferred suffix stripped
        assert swap_tx["data"] == "0xfresh_bridge_calldata"


class TestIsLocalRpc:
    """Tests for is_local_rpc() — canonical local-RPC detector used by deferred refresh."""

    def test_localhost(self):
        assert is_local_rpc("http://localhost:8545") is True

    def test_localhost_with_path(self):
        assert is_local_rpc("http://localhost:8545/some/path") is True

    def test_127_0_0_1(self):
        assert is_local_rpc("http://127.0.0.1:8545") is True

    def test_0_0_0_0(self):
        assert is_local_rpc("http://0.0.0.0:8545") is True

    def test_alchemy_url(self):
        assert is_local_rpc("https://arb-mainnet.g.alchemy.com/v2/key") is False

    def test_infura_url(self):
        assert is_local_rpc("https://mainnet.infura.io/v3/key") is False

    def test_none(self):
        assert is_local_rpc(None) is False

    def test_empty(self):
        assert is_local_rpc("") is False
