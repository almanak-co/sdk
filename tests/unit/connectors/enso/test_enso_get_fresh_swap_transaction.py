"""Tests for EnsoAdapter.get_fresh_swap_transaction coercion behavior.

route_params["amount_in"] is serialized as a string by the intent compiler
(for JSON-boundary safety on large wei values). The deferred-refresh path
must coerce it back to an int before handing it to EnsoClient.get_route,
which is typed as `amount_in: int`.

These tests exercise the real coercion rather than mocking _refresh_enso.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.enso.adapter import EnsoAdapter
from almanak.connectors.enso.client import EnsoConfig


@pytest.fixture
def enso_config() -> EnsoConfig:
    return EnsoConfig(
        chain="arbitrum",
        wallet_address="0x1111111111111111111111111111111111111111",
        api_key="test-api-key",  # noqa: S106 - test fixture
    )


def _make_adapter(config: EnsoConfig) -> EnsoAdapter:
    """Build an EnsoAdapter skipping heavy __init__ side effects."""
    with patch.object(EnsoAdapter, "__init__", lambda self, *a, **kw: None):
        adapter = EnsoAdapter.__new__(EnsoAdapter)
        adapter.config = config
        adapter.chain = config.chain
        adapter.wallet_address = config.wallet_address
        adapter.tokens = {}
        adapter.use_safe_route_single = False
        adapter._token_resolver = None
        adapter._using_placeholders = True
        adapter._price_provider = {"USDC": Decimal("1"), "WETH": Decimal("2000")}
        adapter.client = MagicMock()
    return adapter


def _make_route_tx(
    to: str = "0xEnsoRouter0000000000000000000000000000000",
    data: str = "0xabc123",
    value: int = 0,
    gas: int = 180000,
    amount_out_wei: int = 500_000_000_000_000_000,
    price_impact: int = 5,
) -> MagicMock:
    """Build a RouteTransaction-like mock with the fields the adapter reads."""
    route_tx = MagicMock()
    route_tx.tx.to = to
    route_tx.tx.data = data
    route_tx.tx.value = value
    route_tx.gas = gas
    route_tx.get_amount_out_wei.return_value = amount_out_wei
    route_tx.price_impact = price_impact
    return route_tx


class TestGetFreshSwapTransactionAmountCoercion:
    def test_string_amount_in_is_coerced_to_int(self, enso_config):
        adapter = _make_adapter(enso_config)
        adapter.client.get_route.return_value = _make_route_tx()

        metadata = {
            "from_token": "USDC",
            "to_token": "WETH",
            "route_params": {
                "token_in": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                "token_out": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                "amount_in": "1000000000",  # str, as emitted by the compiler
                "slippage_bps": 50,
            },
        }

        fresh = adapter.get_fresh_swap_transaction(metadata)

        # client.get_route must receive an int, not a string
        kwargs = adapter.client.get_route.call_args.kwargs
        assert kwargs["amount_in"] == 1_000_000_000
        assert isinstance(kwargs["amount_in"], int)

        assert fresh["tx_type"] == "swap"
        assert fresh["to"] == "0xEnsoRouter0000000000000000000000000000000"
        assert fresh["data"] == "0xabc123"

    def test_large_wei_amount_round_trips_exactly(self, enso_config):
        """A value above 2**53 must be preserved exactly through string -> int."""
        adapter = _make_adapter(enso_config)
        adapter.client.get_route.return_value = _make_route_tx()

        big_amount = 12_345_678_901_234_567_890  # 20 digits, well over 2**53

        metadata = {
            "from_token": "USDC",
            "to_token": "WETH",
            "route_params": {
                "token_in": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                "token_out": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                "amount_in": str(big_amount),
                "slippage_bps": 50,
            },
        }

        adapter.get_fresh_swap_transaction(metadata)

        kwargs = adapter.client.get_route.call_args.kwargs
        assert kwargs["amount_in"] == big_amount

    def test_int_amount_in_still_accepted(self, enso_config):
        """Backwards-compat: if callers pass an int, coercion must not break."""
        adapter = _make_adapter(enso_config)
        adapter.client.get_route.return_value = _make_route_tx()

        metadata = {
            "from_token": "USDC",
            "to_token": "WETH",
            "route_params": {
                "token_in": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                "token_out": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                "amount_in": 2_500_000_000,
                "slippage_bps": 50,
            },
        }

        adapter.get_fresh_swap_transaction(metadata)

        kwargs = adapter.client.get_route.call_args.kwargs
        assert kwargs["amount_in"] == 2_500_000_000

    def test_missing_route_params_raises(self, enso_config):
        adapter = _make_adapter(enso_config)
        with pytest.raises(ValueError, match="route_params"):
            adapter.get_fresh_swap_transaction({})
