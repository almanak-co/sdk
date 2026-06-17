"""Unit tests for FluidSwapQuoteConnector (registry-facing quote surface).

Pins every branch of ``quote_swap``: protocol mismatch, SDK construction
failure, pool-enumeration failure, no pool for the pair, limit-gated quote
(retryable), hard quote failure, and the success shape (amount + metadata).
The SDK boundary is mocked; the real on-chain behaviour is covered by
``tests/intents/*/test_fluid_swap.py``.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.swap_quote_registry import (
    SwapQuoteRequest,
    SwapQuoteUnavailable,
)
from almanak.connectors.fluid.sdk import FluidMinAmountError, FluidSDKError
from almanak.connectors.fluid.swap_quote_provider import FluidSwapQuoteConnector

POOL = "0x3C0441B42195F4aD6aa9a0978E06096ea616CDa7"
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDT = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"


def _request(**overrides) -> SwapQuoteRequest:
    defaults = {
        "chain": "arbitrum",
        "protocol": "fluid",
        "token_in": USDC,
        "token_out": USDT,
        "amount_in": 50_000_000,
    }
    defaults.update(overrides)
    return SwapQuoteRequest(**defaults)


def _ctx() -> SimpleNamespace:
    return SimpleNamespace(rpc_url="http://localhost:8545", gateway_client=None)


def _quote(request, sdk=None, sdk_cls_side_effect=None):
    """Run quote_swap with the SDK class patched at its import source."""
    with patch("almanak.connectors._fluid_core.sdk.FluidSDK") as mock_cls:
        if sdk_cls_side_effect is not None:
            mock_cls.side_effect = sdk_cls_side_effect
        else:
            mock_cls.return_value = sdk if sdk is not None else MagicMock()
        return FluidSwapQuoteConnector().quote_swap(_ctx(), request)


class TestQuoteSwapFailures:
    def test_wrong_protocol_unavailable(self):
        with pytest.raises(SwapQuoteUnavailable, match="cannot quote"):
            FluidSwapQuoteConnector().quote_swap(_ctx(), _request(protocol="camelot"))

    def test_sdk_construction_failure_unavailable(self):
        with pytest.raises(SwapQuoteUnavailable, match="quote unavailable"):
            _quote(_request(), sdk_cls_side_effect=FluidSDKError("no transport"))

    def test_pool_enumeration_failure_unavailable(self):
        sdk = MagicMock()
        sdk.find_pool_for_pair.side_effect = FluidSDKError("rpc down")
        with pytest.raises(SwapQuoteUnavailable, match="enumeration failed"):
            _quote(_request(), sdk=sdk)

    def test_no_pool_unavailable(self):
        sdk = MagicMock()
        sdk.find_pool_for_pair.return_value = None
        with pytest.raises(SwapQuoteUnavailable, match="No Fluid pool"):
            _quote(_request(), sdk=sdk)

    def test_limit_gated_quote_is_retryable_unavailable(self):
        sdk = MagicMock()
        sdk.find_pool_for_pair.return_value = (POOL, True)
        sdk.get_swap_quote.side_effect = FluidMinAmountError("limits")
        with pytest.raises(SwapQuoteUnavailable, match="limit-gated \\(retryable\\)"):
            _quote(_request(), sdk=sdk)

    def test_hard_quote_failure_unavailable(self):
        sdk = MagicMock()
        sdk.find_pool_for_pair.return_value = (POOL, True)
        sdk.get_swap_quote.side_effect = FluidSDKError("boom")
        with pytest.raises(SwapQuoteUnavailable, match="quote failed"):
            _quote(_request(), sdk=sdk)


class TestQuoteSwapSuccess:
    def test_success_shape_and_metadata(self):
        sdk = MagicMock()
        sdk.find_pool_for_pair.return_value = (POOL, False)
        sdk.get_swap_quote.return_value = 49_975_000
        result = _quote(_request(token_in=USDT, token_out=USDC), sdk=sdk)
        assert result.amount_out == 49_975_000
        assert result.source == "fluid_dex_reserves_resolver"
        assert result.metadata["pool"] == POOL
        assert result.metadata["swap0to1"] is False
        sdk.get_swap_quote.assert_called_once_with(POOL, False, 50_000_000)

    def test_sdk_constructed_with_ctx_transport(self):
        sdk = MagicMock()
        sdk.find_pool_for_pair.return_value = (POOL, True)
        sdk.get_swap_quote.return_value = 1
        with patch("almanak.connectors._fluid_core.sdk.FluidSDK") as mock_cls:
            mock_cls.return_value = sdk
            FluidSwapQuoteConnector().quote_swap(_ctx(), _request())
            kwargs = mock_cls.call_args.kwargs
            assert kwargs["chain"] == "arbitrum"
            assert kwargs["rpc_url"] == "http://localhost:8545"
            assert kwargs["gateway_client"] is None
