"""Historical target Oracle calls verify published payloads before pricing model inputs."""

from typing import Annotated

from eth_abi import decode, encode
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.connectors.gmx_v2.economic_spot_types import Price
from almanak.connectors.gmx_v2.oracle import StreamReport, decode_report
from almanak.connectors.gmx_v2.oracle_reader import (
    StreamConfiguration,
    normalized_bounds,
    raw_bounds,
    stream_configuration,
)
from almanak.connectors.gmx_v2.published_source import (
    PublishedSource,
    oracle_events,
    report_vectors,
    selected_reports,
    verify_event,
)
from almanak.gateway.ethereum import DataError
from almanak.models import Block, Hash32, Model

# OracleUtils.ValidatedPrice of the pinned GMX revision (gmx-synthetics bf30ebb4): token,
# spread-reduced min and max, the provider's unreduced rawMin and rawMax, timestamp, provider.
PRICE_RESULT = "(address,uint256,uint256,uint256,uint256,uint256,address)[]"


def accepted_price(report, binding):
    """The ValidatedPrice the target Oracle must return for one published signed report."""
    return (
        report.asset.reference,
        *normalized_bounds(binding, report),
        *raw_bounds(binding, report),
        report.observed_at - binding.timestamp_adjustment,
        binding.provider,
    )


class PublishedPrices(Model):
    block: Block
    source: PublishedSource
    transaction: Hash32
    target_bindings: Annotated[tuple[StreamConfiguration, ...], Field(min_length=1, max_length=3)]
    source_bindings: Annotated[tuple[StreamConfiguration, ...], Field(min_length=1, max_length=3)]
    reports: Annotated[tuple[StreamReport, ...], Field(min_length=1, max_length=3)]

    @model_validator(mode="after")
    def scope(self):
        if not len(self.reports) == len(self.target_bindings) == len(self.source_bindings):
            raise ValueError("published prices require both bindings for every signed report")
        assets = tuple(report.asset for report in self.reports)
        if len(set(assets)) != len(assets):
            raise ValueError("published prices require distinct asset identities")
        for report, target, source in zip(
            self.reports, self.target_bindings, self.source_bindings, strict=True
        ):
            if target.block != self.block or source.block != self.block:
                raise ValueError("published price bindings differ from their historical block")
            if report.asset != source.asset or source.oracle != self.source.oracle:
                raise ValueError("published price differs from its explicit source Oracle")
            if source.data_store != target.data_store:
                raise ValueError("published price bindings select different DataStores")
            original = decode_report(
                source.asset, source.provider, bytes.fromhex(report.signed_payload[2:])
            )
            normalized_bounds(source, original)
            normalized_bounds(target, report)
        if self.minimum_timestamp < 0 or self.maximum_timestamp > self.block.timestamp:
            raise ValueError("published prices cannot use future or invalid report timestamps")
        return self

    @property
    def minimum_timestamp(self):
        return min(
            report.observed_at - binding.timestamp_adjustment
            for report, binding in zip(self.reports, self.target_bindings, strict=True)
        )

    @property
    def maximum_timestamp(self):
        return max(
            report.observed_at - binding.timestamp_adjustment
            for report, binding in zip(self.reports, self.target_bindings, strict=True)
        )

    @property
    def prices(self):
        return tuple(
            Price(asset=report.asset, minimum=bounds[0], maximum=bounds[1])
            for report, binding in zip(self.reports, self.target_bindings, strict=True)
            for bounds in (normalized_bounds(binding, report),)
        )


def read_bindings(reader, market, block, source):
    store = reader.call(source.handler, "dataStore()", block, outputs=("address",))[0]
    oracle = reader.call(source.handler, "oracle()", block, outputs=("address",))[0]
    if (store, oracle) != (market.contracts.data_store, source.oracle):
        raise DataError("identity", "published report handler selects another DataStore or Oracle")
    assets = tuple(dict.fromkeys((market.index_asset, market.long_asset, market.short_asset)))
    target = tuple(
        stream_configuration(reader, block, asset, store, market.contracts.oracle)
        for asset in assets
    )
    original = tuple(
        stream_configuration(reader, block, asset, store, source.oracle) for asset in assets
    )
    return target, original


def caller_read(reader, block, sender, target, signature, types, args, outputs):
    data = "0x" + (keccak(text=signature)[:4] + encode(types, args)).hex()
    value = reader._rpc(
        "eth_call",
        [
            {"from": sender, "to": target, "data": data},
            {"blockHash": block.hash, "requireCanonical": True},
        ],
    )
    raw = bytes.fromhex(value[2:])
    result = decode(outputs, raw)
    if encode(outputs, result) != raw:
        raise DataError("malformed", "target Oracle returned noncanonical result encoding")
    return result


def target_acceptance(reader, market, prices):
    assets = tuple(dict.fromkeys((market.index_asset, market.long_asset, market.short_asset)))
    if (
        prices.block.chain != market.chain
        or tuple(binding.asset for binding in prices.target_bindings) != assets
        or any(binding.oracle != market.contracts.oracle for binding in prices.target_bindings)
    ):
        raise ValueError("published price target differs from its configured market Oracle")
    reports = prices.reports
    params = (
        tuple(report.asset.reference for report in reports),
        tuple(report.provider for report in reports),
        tuple(bytes.fromhex(report.signed_payload[2:]) for report in reports),
    )
    actual = caller_read(
        reader,
        prices.block,
        market.contracts.order_handler,
        market.contracts.oracle,
        "validatePrices((address[],address[],bytes[]),bool)",
        ("(address[],address[],bytes[])", "bool"),
        (params, False),
        (PRICE_RESULT,),
    )[0]
    expected = tuple(
        accepted_price(report, binding)
        for report, binding in zip(reports, prices.target_bindings, strict=True)
    )
    if actual != expected:
        raise DataError("identity", "target Oracle bounds differ from the published signed reports")
    caller_read(
        reader,
        prices.block,
        market.contracts.order_handler,
        market.contracts.oracle,
        "setPrices((address[],address[],bytes[]))",
        ("(address[],address[],bytes[])",),
        (params,),
        (),
    )


def read_prices(reader, market, block, source, transaction, receipt):
    expected = (transaction["hash"], block.hash, hex(block.number))
    if (
        (receipt["transactionHash"], receipt["blockHash"], receipt["blockNumber"]) != expected
        or transaction["blockHash"] != block.hash
        or transaction["blockNumber"] != hex(block.number)
        or reader.chain != block.chain
        or market.chain != block.chain
    ):
        raise DataError(
            "identity", "published price source differs from its historical chain/block"
        )
    vectors = report_vectors(transaction, source)
    if vectors is None:
        return None
    destination, original = read_bindings(reader, market, block, source)
    selected = selected_reports(vectors, destination, original)
    if selected is None:
        return None
    reports, originals = selected
    events = oracle_events(receipt, market, source)
    for binding, report in zip(original, originals, strict=True):
        verify_event(events, binding, report)
    prices = PublishedPrices(
        block=block,
        source=source,
        transaction=transaction["hash"],
        target_bindings=destination,
        source_bindings=original,
        reports=reports,
    )
    target_acceptance(reader, market, prices)
    return prices
