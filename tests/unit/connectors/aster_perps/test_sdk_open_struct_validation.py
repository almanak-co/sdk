from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

import pytest
from eth_abi import decode as abi_decode

from almanak.connectors.aster_perps.sdk import (
    NATIVE_BNB_ADDRESS,
    SELECTOR_OPEN_MARKET_TRADE,
    SELECTOR_OPEN_MARKET_TRADE_BNB,
    OpenTradeStruct,
    encode_open_market_trade_calldata,
)

_OPEN_TRADE_ABI = "(address,bool,address,uint96,uint80,uint64,uint64,uint64,uint24)"
_PAIR_BASE = "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c"
_TOKEN_IN = "0x55d398326f99059fF775485246999027B3197955"
_VALID_TRADE = OpenTradeStruct(
    pair_base=_PAIR_BASE,
    is_long=True,
    token_in=_TOKEN_IN,
    amount_in=1,
    qty=1,
    price=1,
    stop_loss=0,
    take_profit=0,
    broker=0,
)


def _trade(**changes: Any) -> OpenTradeStruct:
    return replace(_VALID_TRADE, **changes)


def _decode(calldata: bytes) -> tuple[Any, ...]:
    return abi_decode([_OPEN_TRADE_ABI], calldata[4:])[0]


@pytest.mark.parametrize("field", ["pair_base", "token_in"])
@pytest.mark.parametrize(
    "address",
    [
        None,
        1,
        "",
        "1" * 40,
        "0x" + "1" * 39,
        "0x" + "1" * 41,
        "0X" + "1" * 40,
        "0x" + "g" * 40,
    ],
)
def test_malformed_addresses_raise_exact_value_error(field: str, address: object) -> None:
    with pytest.raises(ValueError) as exc_info:
        encode_open_market_trade_calldata(_trade(**{field: address}))

    assert str(exc_info.value) == f"Invalid EVM address: {address!r}"


@pytest.mark.parametrize("separator", ["+", "-", "_"])
def test_python_integer_syntax_is_not_accepted_as_an_address(separator: str) -> None:
    address = "0x1" + separator + "1" * 38

    with pytest.raises(ValueError) as exc_info:
        encode_open_market_trade_calldata(_trade(pair_base=address))

    assert str(exc_info.value) == f"Invalid EVM address: {address!r}"


def test_pair_address_is_validated_before_token_address() -> None:
    with pytest.raises(ValueError) as exc_info:
        encode_open_market_trade_calldata(_trade(pair_base="bad pair", token_in="bad token"))

    assert str(exc_info.value) == "Invalid EVM address: 'bad pair'"


def test_token_address_is_validated_before_native_compatibility() -> None:
    with pytest.raises(ValueError) as exc_info:
        encode_open_market_trade_calldata(_trade(token_in="bad token"), native=True)

    assert str(exc_info.value) == "Invalid EVM address: 'bad token'"


@pytest.mark.parametrize(
    ("native", "token_in", "message"),
    [
        (
            True,
            _TOKEN_IN,
            f"openMarketTradeBNB requires tokenIn=address(0) sentinel; got {_TOKEN_IN}",
        ),
        (
            False,
            NATIVE_BNB_ADDRESS,
            "openMarketTrade requires a non-zero ERC20 tokenIn; for native BNB margin use openMarketTradeBNB",
        ),
    ],
)
def test_native_token_mismatch_raises_exact_error_before_numeric_validation(
    native: bool, token_in: str, message: str
) -> None:
    with pytest.raises(ValueError) as exc_info:
        encode_open_market_trade_calldata(_trade(token_in=token_in, amount_in=0), native=native)

    assert str(exc_info.value) == message


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("amount_in", -1, "amountIn -1 out of uint96 range"),
        ("amount_in", 0, "amountIn 0 out of uint96 range"),
        ("amount_in", 2**96, f"amountIn {2**96} out of uint96 range"),
        ("qty", -1, "qty -1 out of uint80 range"),
        ("qty", 0, "qty 0 out of uint80 range"),
        ("qty", 2**80, f"qty {2**80} out of uint80 range"),
        ("price", -1, "price -1 out of uint64 range"),
        ("price", 0, "price 0 out of uint64 range"),
        ("price", 2**64, f"price {2**64} out of uint64 range"),
        ("stop_loss", -1, "stopLoss -1 out of uint64 range"),
        ("stop_loss", 2**64, f"stopLoss {2**64} out of uint64 range"),
        ("take_profit", -1, "takeProfit -1 out of uint64 range"),
        ("take_profit", 2**64, f"takeProfit {2**64} out of uint64 range"),
        ("broker", -1, "broker -1 out of uint24 range"),
        ("broker", 2**24, f"broker {2**24} out of uint24 range"),
    ],
)
def test_uint_boundaries_raise_exact_errors(field: str, value: int, message: str) -> None:
    with pytest.raises(ValueError) as exc_info:
        encode_open_market_trade_calldata(_trade(**{field: value}))

    assert str(exc_info.value) == message


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        (
            {"amount_in": 0, "qty": 0, "price": 0, "stop_loss": -1, "take_profit": -1, "broker": -1},
            "amountIn 0 out of uint96 range",
        ),
        (
            {"qty": 0, "price": 0, "stop_loss": -1, "take_profit": -1, "broker": -1},
            "qty 0 out of uint80 range",
        ),
        (
            {"price": 0, "stop_loss": -1, "take_profit": -1, "broker": -1},
            "price 0 out of uint64 range",
        ),
        (
            {"stop_loss": -1, "take_profit": -1, "broker": -1},
            "stopLoss -1 out of uint64 range",
        ),
        ({"take_profit": -1, "broker": -1}, "takeProfit -1 out of uint64 range"),
        ({"broker": -1}, "broker -1 out of uint24 range"),
    ],
)
def test_uint_fields_are_validated_in_wire_order(changes: dict[str, int], message: str) -> None:
    with pytest.raises(ValueError) as exc_info:
        encode_open_market_trade_calldata(_trade(**changes))

    assert str(exc_info.value) == message


@pytest.mark.parametrize(
    ("field", "operator"),
    [
        ("amount_in", "<="),
        ("qty", "<="),
        ("price", "<="),
        ("stop_loss", "<"),
        ("take_profit", "<"),
        ("broker", "<"),
    ],
)
def test_non_numeric_uint_fields_preserve_type_error(field: str, operator: str) -> None:
    with pytest.raises(TypeError) as exc_info:
        encode_open_market_trade_calldata(_trade(**{field: "1"}))

    assert str(exc_info.value) == f"'{operator}' not supported between instances of 'str' and 'int'"


@pytest.mark.parametrize(
    "changes",
    [
        {"amount_in": 1, "qty": 1, "price": 1, "stop_loss": 0, "take_profit": 0, "broker": 0},
        {
            "amount_in": 2**96 - 1,
            "qty": 2**80 - 1,
            "price": 2**64 - 1,
            "stop_loss": 2**64 - 1,
            "take_profit": 2**64 - 1,
            "broker": 2**24 - 1,
        },
    ],
)
def test_valid_uint_boundaries_encode_without_changes(changes: dict[str, int]) -> None:
    calldata = encode_open_market_trade_calldata(_trade(**changes))
    decoded = _decode(calldata)

    assert calldata[:4] == SELECTOR_OPEN_MARKET_TRADE
    assert decoded[3:] == tuple(changes[field] for field in changes)


@pytest.mark.parametrize(
    ("native", "token_in", "selector"),
    [
        (False, _TOKEN_IN.lower(), SELECTOR_OPEN_MARKET_TRADE),
        (False, _TOKEN_IN, SELECTOR_OPEN_MARKET_TRADE),
        (True, NATIVE_BNB_ADDRESS, SELECTOR_OPEN_MARKET_TRADE_BNB),
    ],
)
def test_valid_address_forms_and_native_modes_encode(native: bool, token_in: str, selector: bytes) -> None:
    calldata = encode_open_market_trade_calldata(_trade(token_in=token_in), native=native)
    decoded = _decode(calldata)

    assert calldata[:4] == selector
    assert decoded[0] == _PAIR_BASE.lower()
    assert decoded[2] == token_in.lower()


def test_existing_runtime_coercions_remain_accepted() -> None:
    trade = _trade(
        is_long="false",
        amount_in=Decimal("1.9"),
        qty=2.9,
        price=Decimal("3.9"),
        stop_loss=4.9,
        take_profit=Decimal("5.9"),
        broker=True,
    )

    decoded = _decode(encode_open_market_trade_calldata(trade))

    assert decoded[1] is True
    assert decoded[3:] == (1, 2, 3, 4, 5, 1)
