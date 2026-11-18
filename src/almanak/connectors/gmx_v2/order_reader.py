"""Pinned pending order state for exact retained-request cancellation admission."""

from pydantic import model_validator

from almanak.connectors.gmx_v2.domain import Target
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.oracle_reader import stored
from almanak.connectors.gmx_v2.requests import validate_cancellation
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS, Block, Hash32, Model, UInt

ORDER_ABI = (
    "((address,address,address,address,address,address,address,address[]),"
    "(uint8,uint8,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,"
    "uint256),"
    "(bool,bool,bool,bool),bytes32[])"
)


class PendingOrder(Model):
    target: Target | ExchangeTarget
    key: Hash32
    block: Block
    order_type: UInt
    size_delta_usd_e30: UInt
    collateral_delta: UInt
    acceptable_price: UInt
    execution_fee: UInt
    minimum_output: UInt
    updated_at: UInt
    frozen: bool
    cancellation_delay: UInt
    cancellation_disabled: bool

    @model_validator(mode="after")
    def scope(self):
        supported = (0,) if isinstance(self.target, ExchangeTarget) else (2, 4)
        if self.block.chain != self.target.fund.chain or self.order_type not in supported:
            raise ValueError("pending GMX position order differs from its supported target scope")
        return self

    @property
    def cancellable(self):
        return (
            not self.cancellation_disabled
            and self.block.timestamp >= self.updated_at + self.cancellation_delay
        )


def read_order(reader, target, request, block):
    validate_cancellation(request, target.fund, "order", request.key)
    if (request.market, request.allocation, block.chain) != (
        target.market.share_asset,
        target.allocation,
        target.fund.chain,
    ):
        raise ValueError("GMX retained request differs from its configured cancellation target")
    contracts = target.market.contracts
    addresses, numbers, flags, data = reader.call(
        contracts.reader,
        "getOrder(address,bytes32)",
        block,
        inputs=("address", "bytes32"),
        args=(contracts.data_store, bytes.fromhex(request.key[2:])),
        outputs=(ORDER_ABI,),
    )[0]
    if addresses[0] == ZERO_ADDRESS:
        validate_absent_order(addresses, numbers, flags, data)
        return None
    validate_local_order(target, addresses, numbers, flags, data)
    return PendingOrder(
        target=target,
        key=request.key,
        block=block,
        order_type=numbers[0],
        size_delta_usd_e30=numbers[2],
        collateral_delta=numbers[3],
        acceptable_price=numbers[5],
        execution_fee=numbers[6],
        minimum_output=numbers[8],
        updated_at=numbers[10],
        frozen=flags[2],
        cancellation_delay=stored(
            reader, block, contracts.data_store, "Uint", "REQUEST_EXPIRATION_TIME"
        ),
        cancellation_disabled=stored(
            reader,
            block,
            contracts.data_store,
            "Bool",
            "CANCEL_ORDER_FEATURE_DISABLED",
            ("address", "uint256"),
            (contracts.order_handler, numbers[0]),
        ),
    )


def validate_local_order(target, addresses, numbers, flags, data):
    if isinstance(target, ExchangeTarget):
        validate_exchange_order(target, addresses, numbers, flags, data)
        return
    expected = (
        target.fund.address,
        target.fund.address,
        target.fund.address,
        ZERO_ADDRESS,
        ZERO_ADDRESS,
        target.market.share_asset.reference,
        target.collateral_asset.reference,
        (),
    )
    if addresses != expected or flags[0] != (target.side == "long"):
        raise DataError(
            "integrity", "GMX retained order changed its owner, recipients, market or side"
        )
    if numbers[0] not in (2, 4) or any(numbers[i] for i in (1, 4, 7, 9, 11, 12)):
        raise DataError(
            "unsupported", "GMX cancellation target is not a local market position order"
        )
    if flags[1] or flags[3] or data:
        raise DataError(
            "unsupported", "GMX retained order contains unsupported callback or extra behavior"
        )


def validate_exchange_order(target, addresses, numbers, flags, data):
    expected = (
        target.fund.address,
        target.fund.address,
        target.fund.address,
        ZERO_ADDRESS,
        ZERO_ADDRESS,
        ZERO_ADDRESS,
    )
    if (
        addresses[:6] != expected
        or addresses[6] not in tuple(asset.reference for asset in target.assets)
        or addresses[7] != (target.market.share_asset.reference,)
    ):
        raise DataError(
            "integrity", "GMX retained swap changed its owner, recipients or market route"
        )
    if (
        any(numbers[i] for i in (0, 1, 2, 4, 5, 7, 9, 11, 12))
        or any(flags[i] for i in (0, 1, 3))
        or data
    ):
        raise DataError(
            "unsupported", "GMX cancellation target is not a local exact-input market swap"
        )


def validate_absent_order(addresses, numbers, flags, data):
    if addresses != (ZERO_ADDRESS,) * 7 + ((),) or any((*numbers, *flags, *data)):
        raise DataError("integrity", "absent GMX order contains inconsistent retained fields")
