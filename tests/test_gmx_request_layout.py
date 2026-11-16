"""Pending request checks follow the deployed struct layout, including the UI fee snapshot."""

from types import SimpleNamespace

import pytest
from eth_abi import decode, encode
from eth_abi.exceptions import EncodingError

from almanak.connectors.gmx_v2.liquidity_reader import (
    DEPOSIT_ABI,
    WITHDRAWAL_ABI,
    validate_absent,
    validate_local,
)
from almanak.connectors.gmx_v2.order_reader import (
    ORDER_ABI,
    validate_absent_order,
    validate_exchange_order,
    validate_local_order,
)
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS
from tests.test_gmx_exchange_domain import exchange_target
from tests.test_gmx_positions import target

# Order.Numbers as deployed: orderType, decreasePositionSwapType, sizeDeltaUsd,
# initialCollateralDeltaAmount, triggerPrice, acceptablePrice, executionFee,
# callbackGasLimit, minOutputAmount, uiFeeFactor, updatedAtTime, validFromTime, srcChainId.
ORDER_FIELDS = 13
UI_FEE_FACTOR = 9
DEPOSIT = ("initialLongTokenAmount", "initialShortTokenAmount", "minMarketTokens")
WITHDRAWAL = ("marketTokenAmount", "minLongTokenAmount", "minShortTokenAmount")
SHARED = ("uiFeeFactor", "updatedAtTime", "executionFee", "callbackGasLimit", "srcChainId")


def changed(values, index, value=1):
    return (*values[:index], value, *values[index + 1 :])


def position_order():
    position = target()
    addresses = (
        (position.fund.address,) * 3
        + (ZERO_ADDRESS,) * 2
        + (position.market.share_asset.reference, position.collateral_asset.reference, ())
    )
    numbers = (2, 0, 10**33, 10**17, 0, 10**18, 10**15, 0, 0, 0, 1_788_827_800, 0, 0)
    return position, addresses, numbers, (position.side == "long", False, False, False)


def swap_order():
    exchange = exchange_target()
    addresses = (
        (exchange.fund.address,) * 3
        + (ZERO_ADDRESS,) * 3
        + (exchange.market.long_asset.reference, (exchange.market.share_asset.reference,))
    )
    numbers = (0, 0, 0, 10**17, 0, 0, 10**15, 0, 10**6, 0, 1_788_827_800, 0, 0)
    return exchange, addresses, numbers, (False, False, False, False)


@pytest.mark.parametrize("abi,width", [(ORDER_ABI, 13), (DEPOSIT_ABI, 8), (WITHDRAWAL_ABI, 8)])
def test_reader_structs_carry_the_ui_fee_snapshot(abi, width):
    addresses = abi[2 : abi.index(")")].split(",")
    flags = abi.count("bool")
    value = (
        tuple(() if kind.endswith("[]") else ZERO_ADDRESS for kind in addresses),
        tuple(range(width)),
        (False,) * flags,
        (),
    )
    assert decode((abi,), encode((abi,), (value,)))[0][1] == tuple(range(width))
    with pytest.raises(EncodingError, match=f"value has {width - 1} items"):
        encode((abi,), ((value[0], tuple(range(width - 1)), value[2], ()),))


def test_local_position_order_admits_only_its_request_fields():
    position, addresses, numbers, flags = position_order()
    assert len(numbers) == ORDER_FIELDS
    validate_local_order(position, addresses, numbers, flags, ())
    validate_local_order(position, addresses, changed(numbers, 0, 4), flags, ())
    for index in (1, 4, 7, UI_FEE_FACTOR, 11, 12):
        with pytest.raises(DataError, match="local market position order") as error:
            validate_local_order(position, addresses, changed(numbers, index), flags, ())
        assert error.value.kind == "unsupported"
    with pytest.raises(DataError, match="local market position order"):
        validate_local_order(position, addresses, changed(numbers, 0, 3), flags, ())
    # The update time sits after the UI fee snapshot and stays free.
    validate_local_order(position, addresses, changed(numbers, 10, 5), flags, ())


def test_local_position_order_refuses_changed_identity_and_extra_behavior():
    position, addresses, numbers, flags = position_order()
    for index in (0, 1, 2, 5, 6):
        with pytest.raises(DataError, match="owner, recipients, market or side") as error:
            validate_local_order(
                position, changed(addresses, index, "0x" + "ab" * 20), numbers, flags, ()
            )
        assert error.value.kind == "integrity"
    with pytest.raises(DataError, match="owner, recipients, market or side"):
        validate_local_order(position, addresses, numbers, changed(flags, 0, not flags[0]), ())
    for index in (1, 3):
        with pytest.raises(DataError, match="callback or extra behavior"):
            validate_local_order(position, addresses, numbers, changed(flags, index, True), ())
    with pytest.raises(DataError, match="callback or extra behavior"):
        validate_local_order(position, addresses, numbers, flags, (bytes(32),))


def test_local_swap_order_admits_only_its_request_fields():
    exchange, addresses, numbers, flags = swap_order()
    validate_local_order(exchange, addresses, numbers, flags, ())
    for index in (0, 1, 2, 4, 5, 7, UI_FEE_FACTOR, 11, 12):
        with pytest.raises(DataError, match="local exact-input market swap") as error:
            validate_exchange_order(exchange, addresses, changed(numbers, index), flags, ())
        assert error.value.kind == "unsupported"
    for index in (0, 1, 3):
        with pytest.raises(DataError, match="local exact-input market swap"):
            validate_exchange_order(exchange, addresses, numbers, changed(flags, index, True), ())
    with pytest.raises(DataError, match="local exact-input market swap"):
        validate_exchange_order(exchange, addresses, numbers, flags, (bytes(32),))
    short = changed(addresses, 6, exchange.market.short_asset.reference)
    validate_exchange_order(exchange, short, numbers, flags, ())


def test_local_swap_order_refuses_changed_owner_input_and_route():
    exchange, addresses, numbers, flags = swap_order()
    foreign = "0x" + "ab" * 20
    for index, value in ((0, foreign), (3, foreign), (5, foreign), (6, foreign), (7, ())):
        with pytest.raises(DataError, match="owner, recipients or market route") as error:
            validate_exchange_order(exchange, changed(addresses, index, value), numbers, flags, ())
        assert error.value.kind == "integrity"


def test_absent_order_has_no_retained_fields():
    empty = (ZERO_ADDRESS,) * 7 + ((),)
    validate_absent_order(empty, (0,) * ORDER_FIELDS, (False,) * 4, ())
    for numbers, flags, data in (
        (changed((0,) * ORDER_FIELDS, UI_FEE_FACTOR), (False,) * 4, ()),
        ((0,) * ORDER_FIELDS, (False, True, False, False), ()),
        ((0,) * ORDER_FIELDS, (False,) * 4, (bytes(32),)),
    ):
        with pytest.raises(DataError, match="inconsistent retained fields"):
            validate_absent_order(empty, numbers, flags, data)


def liquidity_request(kind, fields):
    names = (*(DEPOSIT if kind == "deposit" else WITHDRAWAL), *SHARED)
    values = dict(zip(names, fields, strict=True))
    event = SimpleNamespace(value=lambda _, name: values[name])
    return SimpleNamespace(kind=kind, events=(SimpleNamespace(event=lambda: event),))


def liquidity_addresses(exchange, kind):
    common = (
        exchange.fund.address,
        exchange.fund.address,
        ZERO_ADDRESS,
        ZERO_ADDRESS,
        exchange.market.share_asset.reference,
    )
    if kind == "withdrawal":
        return (*common, (), ())
    market = exchange.market
    return (*common, market.long_asset.reference, market.short_asset.reference, (), ())


@pytest.mark.parametrize("kind", ["deposit", "withdrawal"])
def test_pending_liquidity_equals_its_accepted_event_in_deployed_order(kind):
    exchange = exchange_target()
    addresses = liquidity_addresses(exchange, kind)
    # Three amounts, then uiFeeFactor, updatedAtTime, executionFee, callbackGasLimit, srcChainId.
    numbers = (10**17, 2 * 10**8, 3 * 10**18, 0, 1_788_827_800, 10**15, 0, 0)
    request = liquidity_request(kind, numbers)
    validate_local(exchange, request, addresses, numbers, (False,), ())
    for index in range(len(numbers)):
        with pytest.raises(DataError, match="accepted amounts or timing") as error:
            validate_local(exchange, request, addresses, changed(numbers, index, 7), (False,), ())
        assert error.value.kind == "integrity"
    # The retired layout held updatedAtTime where the UI fee snapshot now sits.
    retired = (*numbers[:3], *numbers[4:], 0)
    with pytest.raises(DataError, match="accepted amounts or timing"):
        validate_local(exchange, request, addresses, retired, (False,), ())


@pytest.mark.parametrize("kind", ["deposit", "withdrawal"])
def test_pending_liquidity_refuses_changed_recipients_and_callbacks(kind):
    exchange = exchange_target()
    addresses = liquidity_addresses(exchange, kind)
    numbers = (10**17, 2 * 10**8, 3 * 10**18, 0, 1_788_827_800, 10**15, 0, 0)
    request = liquidity_request(kind, numbers)
    for index in (0, 1, 2, 3, 4):
        with pytest.raises(DataError, match="recipients, tokens, routes or callbacks"):
            validate_local(
                exchange,
                request,
                changed(addresses, index, "0x" + "ab" * 20),
                numbers,
                (False,),
                (),
            )
    with pytest.raises(DataError, match="recipients, tokens, routes or callbacks"):
        validate_local(exchange, request, addresses, numbers, (True,), ())
    with pytest.raises(DataError, match="recipients, tokens, routes or callbacks"):
        validate_local(exchange, request, addresses, numbers, (False,), (bytes(32),))


@pytest.mark.parametrize("kind,count", [("deposit", 7), ("withdrawal", 5)])
def test_absent_liquidity_request_has_no_retained_fields(kind, count):
    empty = (ZERO_ADDRESS,) * count + ((), ())
    validate_absent(kind, empty, (0,) * 8, (False,), ())
    with pytest.raises(DataError, match="inconsistent fields"):
        validate_absent(kind, empty, changed((0,) * 8, 3), (False,), ())
    with pytest.raises(DataError, match="inconsistent fields"):
        validate_absent(kind, changed(empty, 0, "0x" + "ab" * 20), (0,) * 8, (False,), ())
