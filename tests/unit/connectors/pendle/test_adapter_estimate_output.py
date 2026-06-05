"""Tests for PendleAdapter quote fallback cascade."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.connectors.pendle.adapter import PendleAdapter
from almanak.connectors.pendle.models import PendleSwapQuote


def _adapter(
    *,
    api_client: MagicMock | None,
    on_chain_reader: MagicMock | None = None,
    gateway_client: MagicMock | None = None,
    rpc_url: str | None = "http://localhost:8545",
) -> PendleAdapter:
    adapter = object.__new__(PendleAdapter)
    adapter.chain = "ethereum"
    adapter._api_client = api_client
    adapter._on_chain_reader = on_chain_reader
    adapter._gateway_client = gateway_client
    adapter._rpc_url = rpc_url
    return adapter


def test_estimate_output_uses_lazy_api_client_quote() -> None:
    api_client = MagicMock()
    api_client.get_swap_quote.return_value = PendleSwapQuote(
        market_address="0xmarket",
        token_in="0xasset",
        token_out="0xpt",
        amount_in=1_000,
        amount_out=1_125,
        price_impact_bps=12,
    )
    adapter = _adapter(api_client=None)

    with patch("almanak.connectors.pendle.api_client.PendleAPIClient", return_value=api_client) as api_cls:
        estimate = adapter.estimate_output(
            market="0xmarket",
            token_in="0xasset",
            amount_in=1_000,
            swap_type="token_to_pt",
            slippage_bps=25,
        )

    assert estimate == 1_125
    api_cls.assert_called_once_with(chain="ethereum")
    api_client.get_swap_quote.assert_called_once_with(
        market="0xmarket",
        token_in="0xasset",
        amount_in=1_000,
        swap_type="token_to_pt",
        slippage_bps=25,
    )


def test_estimate_output_falls_back_to_lazy_gateway_reader_for_token_to_pt() -> None:
    api_client = MagicMock()
    api_client.get_swap_quote.side_effect = RuntimeError("api unavailable")
    gateway_client = MagicMock()
    reader = MagicMock()
    reader.estimate_pt_output.return_value = 1_052
    adapter = _adapter(api_client=api_client, on_chain_reader=None, gateway_client=gateway_client, rpc_url=None)

    with patch("almanak.connectors.pendle.on_chain_reader.PendleOnChainReader", return_value=reader) as reader_cls:
        estimate = adapter.estimate_output(
            market="0xmarket",
            token_in="0xasset",
            amount_in=1_000,
            swap_type="token_to_pt",
        )

    assert estimate == 1_052
    reader_cls.assert_called_once_with(gateway_client=gateway_client, chain="ethereum")
    reader.estimate_pt_output.assert_called_once_with("0xmarket", 1_000)


def test_estimate_output_falls_back_to_existing_reader_for_pt_to_token() -> None:
    api_client = MagicMock()
    api_client.get_swap_quote.side_effect = RuntimeError("api unavailable")
    reader = MagicMock()
    reader.get_pt_to_asset_rate.return_value = Decimal("0.95")
    adapter = _adapter(api_client=api_client, on_chain_reader=reader)

    estimate = adapter.estimate_output(
        market="0xmarket",
        token_in="0xpt",
        amount_in=1_000,
        swap_type="pt_to_token",
    )

    assert estimate == 950
    reader.get_pt_to_asset_rate.assert_called_once_with("0xmarket")


def test_estimate_output_uses_conservative_fallback_when_quotes_are_unavailable() -> None:
    api_client = MagicMock()
    api_client.get_swap_quote.side_effect = RuntimeError("api unavailable")
    reader = MagicMock()
    adapter = _adapter(api_client=api_client, on_chain_reader=reader)

    estimate = adapter.estimate_output(
        market="0xmarket",
        token_in="0xyt",
        amount_in=1_000,
        swap_type="yt_to_token",
    )

    assert estimate == 990
    reader.estimate_pt_output.assert_not_called()
    reader.get_pt_to_asset_rate.assert_not_called()
