"""Unit tests for ``read_lending_market_health`` — the framework lending-health seam.

The connector reader (``read_compound_v3_market_health``) is covered in
``tests/unit/connectors/compound_v3/test_market_health.py``; this exercises the
framework entrypoint itself: the gateway connectivity guard, the registry
fail-closed branches, and the ``(to, data) -> _gateway_eth_call(...)`` closure
binding + happy-path propagation (VIB-4851 PR-2 / CodeRabbit #2599).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.connectors._strategy_base.lending_read_base import LendingAccountState
from almanak.framework.accounting import lending_reads
from almanak.framework.accounting.lending_reads import read_lending_market_health

_REG = "almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry"
_INPUTS = {
    "comet_address": "0xcomet",
    "base_token": "USDC",
    "base_token_address": "0xusdc",
    "collaterals": {"WETH": {"address": "0xweth"}},
}


def _price(symbol: str) -> Decimal:
    return Decimal("1")


def _decimals(symbol: str, address: str) -> int:
    return 6


def _connected_gateway() -> MagicMock:
    gw = MagicMock()
    gw.is_connected = True
    return gw


def _invoke(gateway_client: object) -> LendingAccountState | None:
    return read_lending_market_health(
        protocol="compound_v3",
        chain="ethereum",
        wallet_address="0xWALLET",
        market_id="usdc",
        gateway_client=gateway_client,
        resolve_base_price=_price,
        resolve_base_decimals=_decimals,
    )


def test_none_gateway_returns_none():
    assert _invoke(None) is None


def test_disconnected_gateway_returns_none():
    gw = MagicMock()
    gw.is_connected = False
    assert _invoke(gw) is None


def test_missing_market_inputs_returns_none():
    with patch(f"{_REG}.market_health_inputs", return_value=None):
        assert _invoke(_connected_gateway()) is None


def test_missing_reader_returns_none():
    with (
        patch(f"{_REG}.market_health_inputs", return_value=_INPUTS),
        patch(f"{_REG}.market_health_reader", return_value=None),
    ):
        assert _invoke(_connected_gateway()) is None


def test_happy_path_binds_eth_call_and_propagates():
    sentinel = LendingAccountState(
        collateral_usd=Decimal("100"),
        debt_usd=Decimal("50"),
        health_factor=Decimal("1.8"),
        liquidation_threshold_bps=None,
        e_mode_category=None,
        lltv=Decimal("0.9"),
    )
    captured: dict = {}

    def fake_reader(*, eth_call, chain, comet_address, user_address, collaterals, base_token, base_token_address, resolve_base_price, resolve_base_decimals):
        captured.update(
            eth_call=eth_call,
            chain=chain,
            comet_address=comet_address,
            user_address=user_address,
            collaterals=collaterals,
            base_token=base_token,
            base_token_address=base_token_address,
            price=resolve_base_price,
            decimals=resolve_base_decimals,
        )
        return sentinel

    gw = _connected_gateway()
    with (
        patch(f"{_REG}.market_health_inputs", return_value=_INPUTS),
        patch(f"{_REG}.market_health_reader", return_value=fake_reader),
        patch.object(lending_reads, "_gateway_eth_call", return_value="0xdeadbeef") as mock_gec,
    ):
        result = _invoke(gw)
        # The bound closure delegates to _gateway_eth_call(gateway_client, chain, to,
        # data). Invoked inside the patch context so the mock is still active.
        assert captured["eth_call"]("0xto", "0xdata") == "0xdeadbeef"
        mock_gec.assert_called_once_with(gw, "ethereum", "0xto", "0xdata")

    # Return value propagated unchanged.
    assert result is sentinel
    # Inputs threaded verbatim from the registry; injected base callbacks passed through.
    assert captured["chain"] == "ethereum"
    assert captured["comet_address"] == "0xcomet"
    assert captured["user_address"] == "0xWALLET"
    assert captured["collaterals"] == {"WETH": {"address": "0xweth"}}
    assert captured["base_token"] == "USDC"
    assert captured["base_token_address"] == "0xusdc"
    assert captured["price"] is _price and captured["decimals"] is _decimals
