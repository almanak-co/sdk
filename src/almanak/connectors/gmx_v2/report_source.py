"""Explicit canonical report source identity is separate from its identified-fork oracle target.

A source is a chain's keeper handler whose mined executions carry authentic signed
Chainlink Data Streams reports. Fork settlement replays those unchanged payloads; it
never synthesizes a price, a signature or a provider.
"""

import json
from functools import cache
from pathlib import Path
from typing import Annotated, Literal

from eth_abi import decode, encode
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.connectors.gmx_v2.deployments import deployed
from almanak.connectors.gmx_v2.events import decode_event
from almanak.connectors.gmx_v2.oracle import decode_report
from almanak.connectors.gmx_v2.oracle_reader import (
    code_hash,
    normalized_bounds,
    stream_configuration,
)
from almanak.gateway.accounts import HexData
from almanak.gateway.ethereum import DataError
from almanak.models import Address, Model

PARAMETERS = ("bytes32", "(address[],address[],bytes[])")
RequestKind = Literal["order", "deposit", "withdrawal"]


class CalldataSuffix(Model):
    prefix: HexData
    length: Annotated[int, Field(ge=0, le=64)]

    @model_validator(mode="after")
    def bounded_prefix(self):
        if len(bytes.fromhex(self.prefix[2:])) > self.length:
            raise ValueError("keeper calldata suffix prefix exceeds its declared length")
        return self

    def matches(self, suffix):
        return len(suffix) == self.length and suffix.startswith(bytes.fromhex(self.prefix[2:]))


class SourceSelection(Model):
    handler: Address
    oracle: Address
    kind: RequestKind
    calldata_suffix: CalldataSuffix

    @property
    def selector(self):
        signature = "execute" + self.kind.title() + "(" + ",".join(PARAMETERS) + ")"
        return "0x" + keccak(text=signature)[:4].hex()


KEEPER_HANDLERS: dict[RequestKind, str] = {
    "order": "OrderHandler",
    "deposit": "DepositHandler",
    "withdrawal": "WithdrawalHandler",
}


@cache
def calldata_suffixes() -> dict[str, "CalldataSuffix"]:
    """The bytes each chain's keepers append after the execute arguments: keeper behaviour,
    not a deployment fact, so it is the only per-chain input besides the deployment record."""
    raw = json.loads((Path(__file__).with_name("artifacts") / "keeper-calldata.json").read_text())
    return {chain: CalldataSuffix.model_validate(value) for chain, value in raw.items()}


def selections_for(market) -> tuple["SourceSelection", ...]:
    """The reviewed keeper handlers whose executions publish the market's signed reports.

    Order, deposit and withdrawal keepers submit the same SetPricesParams to the market's
    Oracle, so each execution carries authentic reports for its tokens. Every address comes
    from the reviewed deployment record, whose code hashes and roles are verified on chain.
    """
    suffix = calldata_suffixes().get(str(market.chain.chain_id))
    if suffix is None:
        raise DataError(
            "unsupported", f"no reviewed GMX keeper calldata on {market.chain.chain_id}"
        )
    return tuple(
        SourceSelection(
            handler=deployed(market, contract),
            oracle=market.contracts.oracle,
            kind=kind,
            calldata_suffix=suffix,
        )
        for kind, contract in KEEPER_HANDLERS.items()
    )


def transaction_vectors(transaction, selections):
    """The report vectors of a keeper execution through any of `selections`; None otherwise."""
    for selection in selections:
        vectors = source_transaction(transaction, selection)
        if vectors is not None:
            return vectors
    return None


def oracle_parameters(selected):
    """The keeper's SetPricesParams: tokens, their providers and unchanged signed payloads."""
    return (
        tuple(str(r.asset).split(":")[-1] for r in selected),
        tuple(r.provider for r in selected),
        tuple(bytes.fromhex(r.signed_payload[2:]) for r in selected),
    )


def configured_source(market):
    return SourceSelection(
        handler=market.contracts.order_handler,
        oracle=market.contracts.oracle,
        kind="order",
        calldata_suffix=CalldataSuffix(prefix="0x", length=0),
    )


def bindings(reader, market, origin, selection):
    source_store = reader.call(selection.handler, "dataStore()", origin, outputs=("address",))[0]
    source_oracle = reader.call(selection.handler, "oracle()", origin, outputs=("address",))[0]
    if (source_store, source_oracle) != (market.contracts.data_store, selection.oracle):
        raise DataError("identity", "selected report handler differs from its DataStore or Oracle")
    assets = tuple(dict.fromkeys((market.index_asset, market.long_asset, market.short_asset)))
    destination = tuple(
        stream_configuration(reader, origin, asset, source_store, market.contracts.oracle)
        for asset in assets
    )
    original = tuple(
        stream_configuration(reader, origin, asset, source_store, selection.oracle)
        for asset in assets
    )
    return destination, original


def identities(reader, market, block, selection, source_bindings):
    require_block(reader, block)
    addresses = tuple(
        dict.fromkeys(
            (
                selection.handler,
                selection.oracle,
                market.contracts.event_emitter,
                *(b.provider for b in source_bindings),
                *(b.verifier for b in source_bindings),
            )
        )
    )
    return {address: code_hash(reader, block, address) for address in addresses}


def source_transaction(transaction, selection):
    if transaction["to"] != selection.handler:
        return None
    if not transaction["input"].startswith(selection.selector):
        return None
    raw = bytes.fromhex(transaction["input"][10:])
    decoded = decode(PARAMETERS, raw)
    canonical = encode(PARAMETERS, decoded)
    if not raw.startswith(canonical) or not selection.calldata_suffix.matches(
        raw[len(canonical) :]
    ):
        raise DataError("malformed", "source keeper calldata does not retain canonical encoding")
    tokens, providers, payloads = decoded[1]
    if len(set(tokens)) != len(tokens) or not len(tokens) == len(providers) == len(payloads):
        raise DataError("malformed", "source keeper report vectors are duplicate or incomplete")
    return dict(zip(tokens, zip(providers, payloads, strict=True), strict=True))


def reports_for(vectors, destination, original):
    selected, source_reports = [], []
    source_by_asset = {binding.asset: binding for binding in original}
    for target in destination:
        source = source_by_asset[target.asset]
        entry = vectors.get(target.asset.reference)
        if entry is None:
            return None
        provider, payload = entry
        if provider != source.provider:
            raise DataError("identity", "source calldata differs from its selected stream provider")
        original_report = decode_report(source.asset, source.provider, payload)
        report = decode_report(target.asset, target.provider, payload)
        normalized_bounds(source, original_report)
        normalized_bounds(target, report)
        source_reports.append(original_report)
        selected.append(report)
    return tuple(selected), tuple(source_reports)


def canonical_receipt(reader, transaction, origin):
    if transaction.get("blockNumber") is None:
        raise DataError("unavailable", "source keeper transaction has no mined publication")
    block = reader.pin_block(int(transaction["blockNumber"], 16))
    receipt = reader._rpc("eth_getTransactionReceipt", [transaction["hash"]])
    expected = (transaction["hash"], block.hash, hex(block.number))
    actual = (receipt["transactionHash"], receipt["blockHash"], receipt["blockNumber"])
    if actual != expected or transaction["blockHash"] != block.hash:
        raise DataError("reorganization", "source keeper publication is no longer canonical")
    if block.number <= origin.number or receipt["status"] != "0x1":
        raise DataError("unavailable", "source keeper must succeed strictly after the fresh origin")
    return block, receipt


def source_events(receipt, market, selection):
    events = tuple(
        event
        for log in receipt["logs"]
        if (event := decode_event(log, emitter=market.contracts.event_emitter)) is not None
        and event.name == "OraclePriceUpdate"
        and event.sender == selection.oracle
    )

    expected = (receipt["transactionHash"], receipt["blockHash"], int(receipt["blockNumber"], 16))
    if any(
        (event.transaction_hash, event.block_hash, event.block_number) != expected
        for event in events
    ):
        raise DataError("identity", "source oracle event differs from its canonical receipt")
    return events


def verify_report_event(events, binding, report):
    matches = [
        event for event in events if event.value("address", "token") == report.asset.reference
    ]
    if len(matches) != 1:
        raise DataError("identity", "source receipt lacks one exact selected-asset oracle event")
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
        raise DataError("identity", "source oracle evidence differs from its signed report")


def keeper_authorized(reader, market, origin, sender):
    require_block(reader, origin)
    role_store = reader.call(
        market.contracts.order_handler, "roleStore()", origin, outputs=("address",)
    )[0]
    role = keccak(encode(("string",), ("ORDER_KEEPER",)))
    authorized = reader.call(
        role_store,
        "hasRole(address,bytes32)",
        origin,
        inputs=("address", "bytes32"),
        args=(sender, role),
        outputs=("bool",),
    )[0]
    if not authorized:
        raise DataError("authority", "source keeper lacks the target fork's existing keeper role")
    return role_store


def eligible(reports, destination, timestamp, expiration):
    adjustments = {binding.asset: binding.timestamp_adjustment for binding in destination}
    times = tuple(report.observed_at - adjustments[report.asset] for report in reports)
    return min(times) > timestamp and max(times) <= timestamp + expiration


def require_block(reader, expected):
    if reader.pin_block(expected.number) != expected:
        raise DataError("reorganization", "report source observation block is no longer canonical")
