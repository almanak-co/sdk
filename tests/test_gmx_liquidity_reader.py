"""Retained native creation events constrain canonical pending GM data and cancellation timing."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import liquidity_reader
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS
from tests.gmx_native_runtime import current_gm_completed as current_gm_completed


@pytest.fixture(params=["deposit", "withdrawal"])
def created(completed, request):
    return next(
        o
        for o in completed.outcomes
        if o.request and o.request.kind == request.param and o.request.status == "accepted"
    )


def retained_properties(outcome):
    event, kind = outcome.request.events[0].event(), outcome.request.kind
    address_names = ("account", "receiver", "callbackContract", "uiFeeReceiver", "market")
    if kind == "deposit":
        address_names += ("initialLongToken", "initialShortToken")
    addresses = tuple(event.value("address", name) for name in address_names) + (
        event.array("address", "longTokenSwapPath"),
        event.array("address", "shortTokenSwapPath"),
    )
    amount_names = {
        "deposit": ("initialLongTokenAmount", "initialShortTokenAmount", "minMarketTokens"),
        "withdrawal": ("marketTokenAmount", "minLongTokenAmount", "minShortTokenAmount"),
    }[kind]
    numbers = tuple(
        event.value("uint256", name)
        for name in (
            *amount_names,
            "uiFeeFactor",
            "updatedAtTime",
            "executionFee",
            "callbackGasLimit",
            "srcChainId",
        )
    )
    return (
        addresses,
        numbers,
        (event.value("bool", "shouldUnwrapNativeToken"),),
        event.array("bytes32", "dataList"),
    )


class Reader:
    def __init__(self, outcome, values):
        self.outcome, self.values = outcome, values

    def call(self, address, signature, block, **kwargs):
        target, request = self.outcome.operation.target, self.outcome.request
        assert address == target.market.contracts.reader
        assert signature == "get" + request.kind.title() + "(address,bytes32)"
        assert block == self.outcome.after.block
        assert kwargs["args"] == (
            target.market.contracts.data_store,
            bytes.fromhex(request.key[2:]),
        )
        return (self.values,)


def read(outcome, values):
    return liquidity_reader.read_liquidity(
        Reader(outcome, values), outcome.operation.target, outcome.request, outcome.after.block
    )


def test_native_gm_pending_properties_retain_identity_and_explicit_cancellation_delay(
    created, monkeypatch
):
    def stored(reader, block, store, kind, key, *args):
        assert store == created.operation.target.market.contracts.data_store
        return {"Bool": False, "Uint": 300}[kind]

    monkeypatch.setattr(liquidity_reader, "stored", stored)
    pending = read(created, retained_properties(created))
    assert pending.key == created.request.key and not pending.cancellable
    mature = pending.model_copy(
        update={"block": pending.block.model_copy(update={"timestamp": pending.updated_at + 300})}
    )
    assert mature.cancellable
    assert not mature.model_copy(update={"cancellation_disabled": True}).cancellable
    assert (
        liquidity_reader.pending(SimpleNamespace(pending_order=None, pending_liquidity=pending))
        == pending
    )


@pytest.mark.parametrize("group", ["recipients", "amounts", "unwrap", "data"])
def test_native_gm_pending_reader_rejects_changed_accepted_terms(created, group):
    addresses, numbers, flags, data = retained_properties(created)
    altered = {
        "recipients": ((addresses[0], ZERO_ADDRESS, *addresses[2:]), numbers, flags, data),
        "amounts": (addresses, (numbers[0] + 1, *numbers[1:]), flags, data),
        "unwrap": (addresses, numbers, (True,), data),
        "data": (addresses, numbers, flags, ("0x" + "01" * 32,)),
    }[group]
    with pytest.raises(DataError, match="changed"):
        read(created, altered)


def test_native_gm_pending_reader_distinguishes_absent_from_corrupt_data(created):
    count = 7 if created.request.kind == "deposit" else 5
    addresses = (ZERO_ADDRESS,) * count + ((), ())
    values = (addresses, (0,) * 8, (False,), ())
    assert read(created, values) is None
    with pytest.raises(DataError, match="inconsistent"):
        read(created, (addresses, (1, *values[1][1:]), values[2], ()))


def test_native_gm_pending_reader_rejects_foreign_market_identity(created):
    wrong = created.request.model_copy(update={"market": created.operation.target.assets[0]})
    with pytest.raises(ValueError, match="exact retained request target"):
        liquidity_reader.read_liquidity(
            Reader(created, retained_properties(created)),
            created.operation.target,
            wrong,
            created.after.block,
        )
