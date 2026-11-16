"""Published signed reports retain an explicit source handler, Oracle and calldata shape."""

from typing import Annotated, Literal

from eth_abi import decode, encode
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.connectors.gmx_v2.candidate_permissions import ORDER_PARAMS
from almanak.connectors.gmx_v2.events import decode_event
from almanak.connectors.gmx_v2.oracle import decode_report
from almanak.connectors.gmx_v2.oracle_reader import normalized_bounds
from almanak.gateway.accounts import HexData
from almanak.gateway.ethereum import DataError
from almanak.models import Address, Model

PARAMETERS = ("bytes32", "(address[],address[],bytes[])")
RELAY_PARAMS = (
    "((address[],address[],bytes[]),"
    "(address[],uint256[],address[],bytes[],address[],address[]),"
    "(address,address,uint256,uint256,uint8,bytes32,bytes32,address)[],"
    "(address,uint256,address[]),uint256,uint256,bytes,uint256)"
)
RELAY_BATCH_PARAMETERS = (
    RELAY_PARAMS,
    "address",
    "("
    + ORDER_PARAMS
    + "[],(bytes32,uint256,uint256,uint256,uint256,uint256,bool,uint256)[],bytes32[])",
)


class CalldataSuffix(Model):
    prefix: HexData
    length: Annotated[int, Field(ge=0, le=64)]

    @model_validator(mode="after")
    def bounded(self):
        if len(bytes.fromhex(self.prefix[2:])) > self.length:
            raise ValueError("published report suffix prefix exceeds its declared length")
        return self

    def matches(self, suffix):
        return len(suffix) == self.length and suffix.startswith(bytes.fromhex(self.prefix[2:]))


class PublishedSource(Model):
    handler: Address
    oracle: Address
    request_kind: Literal["order", "deposit", "withdrawal", "relay_batch"]
    calldata_suffix: CalldataSuffix

    @property
    def selector(self):
        if self.request_kind == "relay_batch":
            return "0x" + keccak(text="batch(" + ",".join(RELAY_BATCH_PARAMETERS) + ")")[:4].hex()
        signature = "execute" + self.request_kind.title() + "(" + ",".join(PARAMETERS) + ")"
        return "0x" + keccak(text=signature)[:4].hex()


def report_vectors(transaction, source):
    if transaction.get("to") != source.handler:
        return None
    calldata = transaction.get("input")
    if not isinstance(calldata, str) or not calldata.startswith(source.selector):
        return None
    raw = bytes.fromhex(calldata[10:])
    parameters = RELAY_BATCH_PARAMETERS if source.request_kind == "relay_batch" else PARAMETERS
    decoded = decode(parameters, raw)
    canonical = encode(parameters, decoded)
    if not raw.startswith(canonical) or not source.calldata_suffix.matches(raw[len(canonical) :]):
        raise DataError("malformed", "published report calldata differs from its canonical shape")
    tokens, providers, payloads = (
        decoded[0][0] if source.request_kind == "relay_batch" else decoded[1]
    )
    if len(set(tokens)) != len(tokens) or not len(tokens) == len(providers) == len(payloads):
        raise DataError("malformed", "published report vectors are duplicate or incomplete")
    return dict(zip(tokens, zip(providers, payloads, strict=True), strict=True))


def selected_reports(vectors, destination, original):
    selected, originals = [], []
    source_by_asset = {binding.asset: binding for binding in original}
    if (
        len(source_by_asset) != len(original)
        or len({binding.asset for binding in destination}) != len(destination)
        or source_by_asset.keys() != {binding.asset for binding in destination}
    ):
        raise ValueError(
            "published source and target bindings must identify the same distinct assets"
        )
    for target in destination:
        source = source_by_asset[target.asset]
        entry = vectors.get(target.asset.reference)
        if entry is None:
            return None
        provider, payload = entry
        if provider != source.provider:
            raise DataError(
                "identity", "published report differs from its selected source provider"
            )
        old = decode_report(source.asset, source.provider, payload)
        new = decode_report(target.asset, target.provider, payload)
        normalized_bounds(source, old)
        normalized_bounds(target, new)
        originals.append(old)
        selected.append(new)
    return tuple(selected), tuple(originals)


def oracle_events(receipt, market, source):
    expected = (receipt["transactionHash"], receipt["blockHash"], int(receipt["blockNumber"], 16))
    if receipt.get("status") != "0x1":
        raise DataError("unavailable", "published report transaction did not succeed")
    events = tuple(
        event
        for log in receipt["logs"]
        if (event := decode_event(log, emitter=market.contracts.event_emitter)) is not None
        and event.name == "OraclePriceUpdate"
        and event.sender == source.oracle
    )
    if any(
        (event.transaction_hash, event.block_hash, event.block_number) != expected
        for event in events
    ):
        raise DataError("identity", "published Oracle event differs from its canonical receipt")
    return events


def verify_event(events, binding, report):
    matches = [
        event for event in events if event.value("address", "token") == report.asset.reference
    ]
    if len(matches) != 1:
        raise DataError("identity", "published receipt requires one selected-asset Oracle event")
    event = matches[0]
    actual = (
        event.value("address", "provider"),
        event.value("uint256", "minPrice"),
        event.value("uint256", "maxPrice"),
        event.value("uint256", "timestamp"),
    )
    expected = (
        binding.provider,
        *normalized_bounds(binding, report),
        report.observed_at - binding.timestamp_adjustment,
    )
    if actual != expected:
        raise DataError("identity", "published signed report differs from its Oracle event")
