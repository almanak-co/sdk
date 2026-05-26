"""Tests for UniswapV4SDK gateway transport (VIB-2989, Phase 5.2).

The V4 SDK used to reach out via urllib directly for on-chain
``getPositionLiquidity`` and ``getSlot0`` queries. When ``gateway_client`` is
provided those queries must route through ``gateway_client.rpc.Call`` and
never touch urllib.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.uniswap_v4.sdk import UniswapV4SDK


def _make_gateway(hex_result: str, success: bool = True, error: str = "") -> MagicMock:
    client = MagicMock()
    rpc = MagicMock()
    response = MagicMock()
    response.success = success
    response.result = json.dumps(hex_result) if success else ""
    response.error = error
    rpc.Call = MagicMock(return_value=response)
    client.rpc = rpc
    return client


class TestGetPositionLiquidityGateway:
    def test_prefers_gateway_when_provided(self):
        liquidity_wei = 12345678
        hex_result = "0x" + format(liquidity_wei, "064x")
        gateway = _make_gateway(hex_result)
        sdk = UniswapV4SDK(chain="arbitrum", gateway_client=gateway)

        with patch("urllib.request.urlopen") as mock_urlopen:
            result = sdk.get_position_liquidity(token_id=42)

        assert result == liquidity_wei
        gateway.rpc.Call.assert_called_once()
        mock_urlopen.assert_not_called()

    def test_gateway_failure_raises(self):
        gateway = _make_gateway("", success=False, error="node down")
        sdk = UniswapV4SDK(chain="arbitrum", gateway_client=gateway)

        with patch("urllib.request.urlopen") as mock_urlopen:
            with pytest.raises(ValueError, match="Gateway eth_call"):
                sdk.get_position_liquidity(token_id=1)
        # Gateway path must fail fast, never fall back to the legacy urllib route.
        mock_urlopen.assert_not_called()

    def test_gateway_malformed_hex_raises(self):
        gateway = _make_gateway("not-hex")
        sdk = UniswapV4SDK(chain="arbitrum", gateway_client=gateway)

        with patch("urllib.request.urlopen") as mock_urlopen:
            with pytest.raises(ValueError, match="Malformed liquidity hex"):
                sdk.get_position_liquidity(token_id=1)
        mock_urlopen.assert_not_called()

    def test_rpc_request_payload_targets_position_manager(self):
        liquidity_wei = 777
        hex_result = "0x" + format(liquidity_wei, "064x")
        gateway = _make_gateway(hex_result)
        sdk = UniswapV4SDK(chain="arbitrum", gateway_client=gateway)

        sdk.get_position_liquidity(token_id=555)

        call_args = gateway.rpc.Call.call_args
        rpc_request = call_args.args[0]
        assert rpc_request.chain == "arbitrum"
        assert rpc_request.method == "eth_call"
        params = json.loads(rpc_request.params)
        assert params[0]["to"] == sdk.position_manager
        # selector 1efeed33 + zero-padded token_id 555
        expected_calldata = "0x1efeed33" + format(555, "064x")
        assert params[0]["data"] == expected_calldata
        assert params[1] == "latest"

    def test_without_gateway_still_requires_rpc_url(self):
        sdk = UniswapV4SDK(chain="arbitrum")
        with pytest.raises(ValueError, match="RPC URL required"):
            sdk.get_position_liquidity(token_id=1)


class TestGetPoolSqrtPriceGateway:
    def test_gateway_failure_returns_none(self):
        """Preserve existing 'fall back to estimated price' semantics on gateway error."""
        gateway = _make_gateway("", success=False, error="upstream 503")
        sdk = UniswapV4SDK(chain="arbitrum", gateway_client=gateway)

        # A minimally-valid PoolKey - we don't care about pool semantics, the
        # test asserts the failure path short-circuits to None.
        from almanak.connectors.uniswap_v4.sdk import NATIVE_CURRENCY, PoolKey

        key = PoolKey(
            currency0=NATIVE_CURRENCY,
            currency1="0x" + "11" * 20,
            fee=3000,
            tick_spacing=60,
            hooks=NATIVE_CURRENCY,
        )

        with patch("urllib.request.urlopen") as mock_urlopen:
            result = sdk.get_pool_sqrt_price(pool_key=key)

        assert result is None
        mock_urlopen.assert_not_called()


class TestInitializer:
    def test_gateway_client_kwarg_is_optional(self):
        """Backward compatibility: constructing without gateway_client still works."""
        sdk = UniswapV4SDK(chain="arbitrum")
        assert sdk._gateway_client is None

    def test_gateway_client_kwarg_is_stored(self):
        gateway = MagicMock()
        sdk = UniswapV4SDK(chain="arbitrum", gateway_client=gateway)
        assert sdk._gateway_client is gateway
