"""Aster Pro: market mapping, order sizing, request signing and compilation."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from eth_account import Account
from eth_account.messages import encode_typed_data

from almanak.connectors._strategy_base.base.compiler import PerpCompilerContext
from almanak.connectors.aster_perps.compiler import AsterPerpsCompiler
from almanak.connectors.aster_perps.gateway.api_client import (
    SymbolRules,
    _MicrosecondNonce,
    _typed_message,
    format_quantity,
    quantity_for_notional,
    sign_message,
)
from almanak.connectors.aster_perps.markets import client_order_id, to_symbol
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import PerpCloseIntent, PerpOpenIntent

ETH_RULES = SymbolRules(
    symbol="ETHUSDT",
    status="TRADING",
    step_size=Decimal("0.001"),
    min_qty=Decimal("0.001"),
    max_market_qty=Decimal("2000"),
    min_notional=Decimal("5"),
    quantity_precision=3,
    tick_size=Decimal("0.01"),
)


@pytest.mark.parametrize(
    ("market", "symbol"),
    [("ETH/USD", "ETHUSDT"), ("eth-usdt", "ETHUSDT"), ("ETHUSDT", "ETHUSDT"), ("ETHUSD", "ETHUSDT"),
     ("1000PEPE/USD", "1000PEPEUSDT")],
)
def test_market_maps_to_usdt_symbol(market: str, symbol: str) -> None:
    assert to_symbol(market) == symbol


@pytest.mark.parametrize("market", ["ETH/EUR", "ETH/USD/X", "", "E"])
def test_unrecognised_market_is_rejected(market: str) -> None:
    with pytest.raises(ValueError):
        to_symbol(market)


def test_client_order_id_is_deterministic_and_within_venue_limit() -> None:
    intent_id = "3f2b8c1e-0d4a-4c6b-9e7f-1a2b3c4d5e6f"
    first = client_order_id(intent_id, leg="open")
    assert first == client_order_id(intent_id, leg="open")
    assert first != client_order_id(intent_id, leg="close")
    assert len(first) <= 36


def test_quantity_rounds_down_to_step_and_never_exceeds_requested_notional() -> None:
    quantity = quantity_for_notional(Decimal("6"), Decimal("2727.56"), ETH_RULES)
    assert quantity == Decimal("0.002")
    assert quantity * Decimal("2727.56") <= Decimal("6")
    assert format_quantity(quantity, ETH_RULES) == "0.002"


def test_quantity_below_min_notional_is_refused_not_upsized() -> None:
    with pytest.raises(ValueError, match="below the venue minimum"):
        quantity_for_notional(Decimal("5.4"), Decimal("2727.56"), ETH_RULES)


def test_quantity_below_min_qty_is_refused() -> None:
    btc = SymbolRules(
        "BTCUSDT", "TRADING", Decimal("0.001"), Decimal("0.001"), Decimal("120"), Decimal("5"), 3, Decimal("0.1")
    )
    with pytest.raises(ValueError, match="minimum quantity"):
        quantity_for_notional(Decimal("50"), Decimal("86000"), btc)


def test_signature_recovers_to_the_signing_key_over_the_exact_query() -> None:
    account = Account.create()
    msg = "symbol=ETHUSDT&side=BUY&nonce=1&user=0xabc&signer=0xdef"
    signature = sign_message(account, 1666, msg)
    recovered = Account.recover_message(encode_typed_data(full_message=_typed_message(1666, msg)), signature=signature)
    assert recovered == account.address
    other = Account.recover_message(
        encode_typed_data(full_message=_typed_message(56, msg)), signature=signature
    )
    assert other != account.address


def test_nonce_is_strictly_increasing() -> None:
    nonce = _MicrosecondNonce()
    values = [nonce.next() for _ in range(1000)]
    assert values == sorted(set(values))


def _ctx(chain: str = "bsc") -> PerpCompilerContext:
    return PerpCompilerContext(
        chain=chain,
        wallet_address="0x" + "ab" * 20,
        rpc_url=None,
        rpc_timeout=10.0,
        permission_discovery=False,
        allow_placeholder_prices=False,
        token_resolver=None,
        gateway_client=None,
        price_oracle=None,
        cache={},
        services=MagicMock(),
        default_protocol="aster_perps",
        protocol="aster_perps",
    )


def _open(**overrides: object) -> PerpOpenIntent:
    fields: dict[str, object] = {
        "market": "ETH/USD",
        "collateral_token": "USDT",
        "collateral_amount": Decimal("1.2"),
        "size_usd": Decimal("6"),
        "is_long": True,
        "leverage": Decimal("5"),
        "protocol": "aster_perps",
    }
    fields.update(overrides)
    return PerpOpenIntent(**fields)


def test_open_compiles_to_an_offchain_order_request() -> None:
    intent = _open(max_slippage=Decimal("0.02"))
    result = AsterPerpsCompiler().compile(_ctx(), intent)
    assert result.status == CompilationStatus.SUCCESS
    bundle = result.action_bundle
    assert bundle.transactions == []
    assert bundle.metadata["protocol"] == "aster_perps"
    assert bundle.metadata["order_request"] == {
        "symbol": "ETHUSDT",
        "is_long": True,
        "notional_usd": "6",
        "close_position": False,
        "leverage": 5,
        "client_order_id": client_order_id(intent.intent_id, leg="open"),
        "max_slippage": "0.02",
    }


def test_close_carries_the_intents_slippage_bound() -> None:
    intent = PerpCloseIntent(
        market="ETH/USD", collateral_token="USDT", is_long=True, protocol="aster_perps", max_slippage=Decimal("0.03")
    )
    result = AsterPerpsCompiler().compile(_ctx(), intent)
    assert result.status == CompilationStatus.SUCCESS
    assert result.action_bundle.metadata["order_request"]["max_slippage"] == "0.03"


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"collateral_token": "BNB"}, "margin is USDT"),
        ({"leverage": Decimal("2.5")}, "whole number"),
        ({"leverage": Decimal("200")}, "whole number"),
        ({"market": "ETH/EUR"}, "Unrecognised Aster market"),
    ],
)
def test_open_rejects_what_the_venue_cannot_honour(overrides: dict[str, object], error: str) -> None:
    result = AsterPerpsCompiler().compile(_ctx(), _open(**overrides))
    assert result.status == CompilationStatus.FAILED
    assert error in (result.error or "")


def test_open_accepts_the_bsc_usdt_address_as_margin() -> None:
    usdt = "0x55d398326f99059fF775485246999027B3197955"
    result = AsterPerpsCompiler().compile(_ctx(), _open(collateral_token=usdt))
    assert result.status == CompilationStatus.SUCCESS


def test_open_rejects_non_bsc_chain() -> None:
    result = AsterPerpsCompiler().compile(_ctx(chain="arbitrum"), _open())
    assert result.status == CompilationStatus.FAILED


def test_close_compiles_to_a_full_reduce_only_close() -> None:
    intent = PerpCloseIntent(market="ETH/USD", collateral_token="USDT", is_long=True, protocol="aster_perps")
    result = AsterPerpsCompiler().compile(_ctx(), intent)
    assert result.status == CompilationStatus.SUCCESS
    order = result.action_bundle.metadata["order_request"]
    assert order["close_position"] is True
    assert order["symbol"] == "ETHUSDT"
    assert order["client_order_id"] == client_order_id(intent.intent_id, leg="close")


@pytest.mark.parametrize(
    "overrides",
    [{"size_usd": Decimal("3")}, {"position_id": "0x" + "ab" * 32}],
)
def test_close_rejects_partial_or_hash_keyed_closes(overrides: dict[str, object]) -> None:
    intent = PerpCloseIntent(
        market="ETH/USD", collateral_token="USDT", is_long=True, protocol="aster_perps", **overrides
    )
    result = AsterPerpsCompiler().compile(_ctx(), intent)
    assert result.status == CompilationStatus.FAILED


@pytest.mark.asyncio
async def test_a_symbol_without_a_tick_size_is_unknown_not_fatal_for_every_market() -> None:
    from almanak.connectors.aster_perps.gateway.api_client import AsterApiError, AsterProApiClient

    def _symbol(name: str, filters: list) -> dict:
        lot = {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"}
        return {"symbol": name, "status": "TRADING", "filters": [lot, {"filterType": "MIN_NOTIONAL", "notional": "5"}, *filters]}

    client = AsterProApiClient(Account.create())

    async def exchange_info(path: str, params: dict | None = None) -> dict:
        return {"symbols": [_symbol("ETHUSDT", [{"filterType": "PRICE_FILTER", "tickSize": "0.01"}]), _symbol("BADUSDT", [])]}

    client._public = exchange_info  # type: ignore[method-assign]
    assert (await client.symbol_rules("ETHUSDT")).tick_size == Decimal("0.01")
    with pytest.raises(AsterApiError, match="Unknown Aster symbol"):
        await client.symbol_rules("BADUSDT")
