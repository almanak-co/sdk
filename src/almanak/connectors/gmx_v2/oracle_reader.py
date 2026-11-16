"""Pinned GMX stream configuration reads; no alternative oracle or provider selection."""

from typing import Annotated, Self

from eth_abi import encode
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.connectors.gmx_v2.oracle import StreamReport
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import erc20_address
from almanak.models import Address, Block, Hash32, Model, UInt

FLOAT = 10**30
FLOAT_DECIMALS = 30
# Chainlink data stream reports carry 18-decimal prices; GMX quotes one token
# unit at 10**(FLOAT_DECIMALS - decimals) USD units and scales a report into
# that unit with the token's stored multiplier, which is therefore a power of
# ten whose exponent encodes the token precision: 10**(42 - decimals).
STREAM_REPORT_DECIMALS = 18
STREAM_SCALE_DECIMALS = 2 * FLOAT_DECIMALS - STREAM_REPORT_DECIMALS


class StreamConfiguration(Model):
    block: Block
    asset: AssetType
    data_store: Address
    oracle: Address
    provider: Address
    provider_code_hash: Hash32
    verifier: Address
    verifier_code_hash: Hash32
    feed_id: Hash32
    multiplier: Annotated[UInt, Field(gt=0)]
    spread_reduction: Annotated[UInt, Field(le=FLOAT)]
    timestamp_adjustment: UInt

    @model_validator(mode="after")
    def scope(self) -> Self:
        erc20_address(self.asset, self.block.chain.chain_id)
        return self


def key(name, types=(), values=()):
    root = keccak(encode(("string",), (name,)))
    return keccak(encode(("bytes32", *types), (root, *values))) if types else root


def stored(reader, block, data_store, kind, name, types=(), values=()):
    return reader.call(
        data_store,
        f"get{kind}(bytes32)",
        block,
        inputs=("bytes32",),
        args=(key(name, types, values),),
        outputs=(
            {
                "Address": "address",
                "Uint": "uint256",
                "Int": "int256",
                "Bool": "bool",
                "Bytes32": "bytes32",
            }[kind],
        ),
    )[0]


def deployed_code(reader, block, contract) -> str:
    return reader._rpc(
        "eth_getCode", [contract, {"blockHash": block.hash, "requireCanonical": True}]
    )


def is_deployed(reader, block, contract) -> bool:
    return deployed_code(reader, block, contract) != "0x"


def code_hash(reader, block, contract):
    code = deployed_code(reader, block, contract)
    if code == "0x":
        raise DataError("unavailable", "GMX oracle dependency has no deployed code")
    return "0x" + keccak(bytes.fromhex(code[2:])).hex()


def stream_decimals(reader, block, data_store, token) -> int:
    """The precision GMX prices a token at, from its data stream multiplier.

    A synthetic index token has no contract to answer `decimals()`; the venue's
    own stream configuration is the only on-chain statement of its unit.
    """
    multiplier = stored(
        reader, block, data_store, "Uint", "DATA_STREAM_MULTIPLIER", ("address",), (token,)
    )
    exponent = len(str(multiplier)) - 1
    if multiplier <= 0 or multiplier != 10**exponent or exponent > STREAM_SCALE_DECIMALS:
        raise DataError(
            "unavailable", "GMX index token has no data stream precision at the pinned block"
        )
    return STREAM_SCALE_DECIMALS - exponent


def stream_configuration(reader, block, asset, data_store, oracle):
    token = erc20_address(asset, block.chain.chain_id)
    provider = stored(
        reader,
        block,
        data_store,
        "Address",
        "ORACLE_PROVIDER_FOR_TOKEN",
        ("address", "address"),
        (oracle, token),
    )
    if not stored(
        reader, block, data_store, "Bool", "IS_ORACLE_PROVIDER_ENABLED", ("address",), (provider,)
    ):
        raise DataError("unavailable", "configured GMX oracle provider is disabled")
    verifier = validate_provider(reader, block, provider, data_store, oracle)
    return StreamConfiguration(
        block=block,
        asset=asset,
        data_store=data_store,
        oracle=oracle,
        provider=provider,
        provider_code_hash=code_hash(reader, block, provider),
        verifier=verifier,
        verifier_code_hash=code_hash(reader, block, verifier),
        feed_id="0x"
        + stored(
            reader, block, data_store, "Bytes32", "DATA_STREAM_ID", ("address",), (token,)
        ).hex(),
        multiplier=stored(
            reader, block, data_store, "Uint", "DATA_STREAM_MULTIPLIER", ("address",), (token,)
        ),
        spread_reduction=stored(
            reader,
            block,
            data_store,
            "Uint",
            "DATA_STREAM_SPREAD_REDUCTION_FACTOR",
            ("address",),
            (token,),
        ),
        timestamp_adjustment=stored(
            reader,
            block,
            data_store,
            "Uint",
            "ORACLE_TIMESTAMP_ADJUSTMENT",
            ("address", "address"),
            (provider, token),
        ),
    )


def validate_provider(reader, block, provider, data_store, oracle):
    actual_store = reader.call(provider, "dataStore()", block, outputs=("address",))[0]
    actual_oracle = reader.call(provider, "oracle()", block, outputs=("address",))[0]
    adjusts = reader.call(provider, "shouldAdjustTimestamp()", block, outputs=("bool",))[0]
    onchain = reader.call(provider, "isChainlinkOnChainProvider()", block, outputs=("bool",))[0]
    if (actual_store, actual_oracle, adjusts, onchain) != (
        data_store.lower(),
        oracle.lower(),
        True,
        False,
    ):
        raise DataError(
            "unsupported", "GMX stream provider binding differs from the requested oracle"
        )
    return reader.call(provider, "verifier()", block, outputs=("address",))[0]


def raw_bounds(config: StreamConfiguration, report: StreamReport) -> tuple[int, int]:
    """The provider's multiplier-scaled bid and ask before any spread reduction."""
    if (report.asset, report.provider, report.feed_id) != (
        config.asset,
        config.provider,
        config.feed_id,
    ):
        raise ValueError("stream report differs from the explicitly configured asset and provider")
    return report.bid * config.multiplier // FLOAT, report.ask * config.multiplier // FLOAT


def normalized_bounds(config: StreamConfiguration, report: StreamReport) -> tuple[int, int]:
    """Exact provider arithmetic only; signature, reference-price and timing checks are separate."""
    minimum, maximum = raw_bounds(config, report)
    if config.spread_reduction == FLOAT:
        middle = (minimum + maximum) // 2
        return middle, middle
    adjustment = ((maximum - minimum) // 2) * config.spread_reduction // FLOAT
    return minimum + adjustment, maximum - adjustment
