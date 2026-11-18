"""Canonical pending GM requests retain original escrow and bounded cancellation semantics."""

from typing import Literal

from pydantic import model_validator

from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.oracle_reader import stored
from almanak.connectors.gmx_v2.requests import validate_cancellation
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS, Block, Hash32, Model, UInt

DEPOSIT_ABI = (
    "((address,address,address,address,address,address,address,address[],address[]),"
    "(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),(bool),bytes32[])"
)
WITHDRAWAL_ABI = (
    "((address,address,address,address,address,address[],address[]),"
    "(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),(bool),bytes32[])"
)


def handler(target, kind):
    contracts = target.market.contracts
    return {"deposit": contracts.deposit_handler, "withdrawal": contracts.withdrawal_handler}[kind]


class PendingLiquidity(Model):
    target: ExchangeTarget
    key: Hash32
    request_kind: Literal["deposit", "withdrawal"]
    block: Block
    updated_at: UInt
    cancellation_delay: UInt
    cancellation_disabled: bool

    @model_validator(mode="after")
    def scope(self):
        if self.block.chain != self.target.fund.chain:
            raise ValueError("pending GM liquidity request must retain its exact execution chain")
        return self

    @property
    def cancellable(self):
        return (
            not self.cancellation_disabled
            and self.block.timestamp >= self.updated_at + self.cancellation_delay
        )


def read_liquidity(reader, target, request, block):
    validate_cancellation(request, target.fund, request.kind, request.key)
    if request.kind not in ("deposit", "withdrawal") or (
        request.market,
        request.allocation,
        block.chain,
    ) != (target.market.share_asset, target.allocation, target.fund.chain):
        raise ValueError("GM liquidity reader requires its exact retained request target")
    contracts = target.market.contracts
    abi = DEPOSIT_ABI if request.kind == "deposit" else WITHDRAWAL_ABI
    prefix = request.kind.title()
    addresses, numbers, flags, data = reader.call(
        contracts.reader,
        f"get{prefix}(address,bytes32)",
        block,
        inputs=("address", "bytes32"),
        args=(contracts.data_store, bytes.fromhex(request.key[2:])),
        outputs=(abi,),
    )[0]
    if addresses[0] == ZERO_ADDRESS:
        validate_absent(request.kind, addresses, numbers, flags, data)
        return None
    validate_local(target, request, addresses, numbers, flags, data)
    disabled = stored(
        reader,
        block,
        contracts.data_store,
        "Bool",
        f"CANCEL_{request.kind.upper()}_FEATURE_DISABLED",
        ("address",),
        (handler(target, request.kind),),
    )
    return PendingLiquidity(
        target=target,
        key=request.key,
        request_kind=request.kind,
        block=block,
        updated_at=numbers[4],
        cancellation_delay=stored(
            reader, block, contracts.data_store, "Uint", "REQUEST_EXPIRATION_TIME"
        ),
        cancellation_disabled=disabled,
    )


def validate_absent(kind, addresses, numbers, flags, data):
    count = 7 if kind == "deposit" else 5
    if addresses != (ZERO_ADDRESS,) * count + ((), ()) or any((*numbers, *flags, *data)):
        raise DataError("integrity", "absent GM liquidity request contains inconsistent fields")


def validate_local(target, request, addresses, numbers, flags, data):
    common = (
        target.fund.address,
        target.fund.address,
        ZERO_ADDRESS,
        ZERO_ADDRESS,
        target.market.share_asset.reference,
    )
    expected = (
        (*common, target.market.long_asset.reference, target.market.short_asset.reference, (), ())
        if request.kind == "deposit"
        else (*common, (), ())
    )
    if addresses != expected or flags != (False,) or data:
        raise DataError(
            "integrity", "GM liquidity request changed recipients, tokens, routes or callbacks"
        )
    fields = (
        ("initialLongTokenAmount", "initialShortTokenAmount", "minMarketTokens")
        if request.kind == "deposit"
        else ("marketTokenAmount", "minLongTokenAmount", "minShortTokenAmount")
    )
    event = request.events[0].event()
    expected_numbers = tuple(
        event.value("uint256", name)
        for name in (
            *fields,
            "uiFeeFactor",
            "updatedAtTime",
            "executionFee",
            "callbackGasLimit",
            "srcChainId",
        )
    )
    if numbers != expected_numbers:
        raise DataError(
            "integrity", "pending GM liquidity request changed its accepted amounts or timing"
        )


def read_pending(reader, target, request, block):
    if request.kind == "order":
        from almanak.connectors.gmx_v2.order_reader import read_order

        return read_order(reader, target, request, block)
    return read_liquidity(reader, target, request, block)


def pending(snapshot):
    return snapshot.pending_order or getattr(snapshot, "pending_liquidity", None)
