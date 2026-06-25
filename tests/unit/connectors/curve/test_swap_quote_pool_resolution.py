"""ALM-2896: Curve swap-quote provider resolves pool_address when omitted.

The framework estimate_slippage AMM fallback calls the connector quoter with a
token pair but no pool_address. The connector must resolve the pool from its own
registry over the public adapter surface.
"""

from __future__ import annotations

import pytest

from almanak.connectors._strategy_base.swap_quote_registry import (
    SwapQuoteRequest,
    SwapQuoteUnavailable,
)
from almanak.connectors.curve.swap_quote_provider import CurveSwapQuoteConnector

ARB_2POOL = "0x7f90122BF0700F9E7e1F688fe926940E8839F353"
USDC_E = "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8"
USDT = "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"


class _Ctx:
    wallet_address = "0x0000000000000000000000000000000000000001"
    rpc_url = None
    gateway_client = object()  # presence only; quote_swap_output is patched
    token_resolver = None


def _request(pool_address=None, token_in=USDC_E, token_out=USDT):
    return SwapQuoteRequest(
        chain="arbitrum",
        protocol="curve",
        token_in=token_in,
        token_out=token_out,
        amount_in=5_000_000,
        pool_address=pool_address,
    )


def test_resolves_pool_when_address_omitted(monkeypatch):
    """pool_address=None for USDC.e/USDT on arbitrum resolves the 2pool."""
    captured: dict = {}

    def _fake_quote(self, *, pool_address, token_in, token_out, amount_in_wei):  # noqa: ANN001
        captured["pool_address"] = pool_address
        return 4_995_000

    monkeypatch.setattr(
        "almanak.connectors.curve.adapter.CurveAdapter.quote_swap_output",
        _fake_quote,
    )

    result = CurveSwapQuoteConnector().quote_swap(_Ctx(), _request(pool_address=None))

    assert captured["pool_address"].lower() == ARB_2POOL.lower()
    assert result.amount_out == 4_995_000
    assert result.metadata["pool_address"].lower() == ARB_2POOL.lower()


def test_unknown_pair_raises_unavailable(monkeypatch):
    """A pair with no Curve pool raises SwapQuoteUnavailable, not a generic error."""

    def _never(self, **kwargs):  # noqa: ANN001, ARG001
        raise AssertionError("should not quote an unresolved pool")

    monkeypatch.setattr(
        "almanak.connectors.curve.adapter.CurveAdapter.quote_swap_output",
        _never,
    )

    bogus = "0x000000000000000000000000000000000000dEaD"
    with pytest.raises(SwapQuoteUnavailable):
        CurveSwapQuoteConnector().quote_swap(_Ctx(), _request(token_out=bogus))


def test_explicit_pool_address_passes_through(monkeypatch):
    """An explicit pool_address is honoured as-is (resolution skipped)."""
    captured: dict = {}

    def _fake_quote(self, *, pool_address, token_in, token_out, amount_in_wei):  # noqa: ANN001
        captured["pool_address"] = pool_address
        return 4_995_000

    monkeypatch.setattr(
        "almanak.connectors.curve.adapter.CurveAdapter.quote_swap_output",
        _fake_quote,
    )

    CurveSwapQuoteConnector().quote_swap(_Ctx(), _request(pool_address=ARB_2POOL))
    assert captured["pool_address"] == ARB_2POOL
