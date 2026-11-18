"""Bounded Comet discovery and whole-market sampling through the historical gateway."""

from pydantic import model_validator

from almanak.backtest import progress
from almanak.backtest.dataset import RawEvidence
from almanak.backtest.gateway import ContractBinding
from almanak.backtest.prepared import AcquiredRange
from almanak.backtest.types import UnixTime
from almanak.caip import AssetType
from almanak.configuration.metadata import verify
from almanak.connectors.compound_v3.deployment import (
    REVISIONS,
    configured_market,
    immutable_address,
    implementation_address,
)
from almanak.connectors.compound_v3.domain import Market
from almanak.connectors.compound_v3.economic_data import RateRecord, RiskRecord, RiskSample, key
from almanak.connectors.compound_v3.economic_parameters import markets
from almanak.connectors.compound_v3.reader import ASSET_INFO, read_market
from almanak.connectors.evm_revision import read_matching_code
from almanak.models import ZERO_ADDRESS, Address, Block, Chain, Model


class Discovery(Model):
    source: Market
    block: Block
    implementation: Address
    extension: Address
    asset_list: Address
    collateral: tuple[AssetType, ...]
    price_feeds: tuple[Address, ...]

    @model_validator(mode="after")
    def scope(self):
        roles = (self.source.comet, self.implementation, self.extension, self.asset_list)
        if self.source.chain != self.block.chain or ZERO_ADDRESS in roles or len(set(roles)) != 4:
            raise ValueError("discovered Comet roles are absent, overlapping or on another chain")
        if len(self.collateral) > 24 or len(self.collateral) != len(self.price_feeds):
            raise ValueError("discovered Comet collateral and price feeds must remain correlated")
        if (
            len(set(self.collateral)) != len(self.collateral)
            or self.source.base_asset in self.collateral
        ):
            raise ValueError("discovered Comet collateral aliases another asset")
        for asset, feed in zip(self.collateral, self.price_feeds, strict=True):
            if (
                asset.chain != self.source.chain.chain_id
                or asset.namespace != "erc20"
                or ZERO_ADDRESS in (asset.reference, feed)
            ):
                raise ValueError(
                    "discovered Comet collateral requires chain-scoped ERC-20 assets and feeds"
                )
        return self


def contracts(config, discoveries=()):
    bound: dict[Chain, set[Address]] = {}
    for source in markets(config):
        bound.setdefault(source.chain, set()).update(
            (source.comet, source.base_asset.reference, source.base_price_feed)
        )
    for discovered in discoveries:
        bound[discovered.source.chain].update(
            (
                discovered.implementation,
                discovered.extension,
                discovered.asset_list,
                *(a.reference for a in discovered.collateral),
                *discovered.price_feeds,
            )
        )
    return tuple(
        ContractBinding(chain=chain, contracts=tuple(sorted(addresses)))
        for chain, addresses in bound.items()
    )


def discover(history, source, at):
    block = history.pin(history.floor(UnixTime(at)))
    reader = history.reader
    read_matching_code(
        reader, source.comet, block, tuple((name, row["proxy"]) for name, row in REVISIONS.items())
    )
    implementation = implementation_address(reader, source, block)
    _, values, _ = read_matching_code(
        reader,
        implementation,
        block,
        tuple((name, row["implementation"]) for name, row in REVISIONS.items()),
    )
    configured_market(source, values)
    collateral, feeds = [], []
    for index in range(values["numAssets"]):
        info = reader.call(
            source.comet,
            "getAssetInfo(uint8)",
            block,
            inputs=("uint8",),
            args=(index,),
            outputs=(ASSET_INFO,),
        )[0]
        if info[0] != index:
            raise ValueError("discovered Comet asset list has another collateral offset")
        collateral.append(AssetType(f"{source.chain.chain_id}/erc20:{info[1]}"))
        feeds.append(info[2])
    return Discovery(
        source=source,
        block=block,
        implementation=implementation,
        extension=immutable_address(values["extensionDelegate"]),
        asset_list=immutable_address(values["assetList"]),
        collateral=tuple(collateral),
        price_feeds=tuple(feeds),
    )


def envelope(reader, source, product, block, at):
    raw = tuple(RawEvidence(body=reader.cache[k].model_dump_json()) for k in sorted(reader.used))
    return {
        "key": key(source, product),
        "unit": "compound_v3_market_accounting"
        if product == "accounting"
        else "compound_v3_market_risk",
        "reference": block,
        "source_time": UnixTime(block.timestamp),
        "available_at": UnixTime(at),
        "retrieved_at": max(reader.cache[k].retrieved_at for k in reader.used),
        "evidence": tuple(e.digest for e in raw),
    }, raw


def validate_market(config, market):
    verify(config, market.base_asset)
    assets = {c.asset_type: c for c in market.collaterals}
    for allocation in config.positions:
        reserve = allocation.reserve
        if reserve.market != market.source or reserve.is_base:
            continue
        collateral = assets.get(reserve.asset)
        if collateral is None or collateral.price_feed != reserve.price_feed:
            raise ValueError("historical Comet market differs from its configured asset/feed")
        verify(config, collateral.asset)


def bounds(histories, config, settings):
    ranges = {}
    for chain, history in histories.items():
        first, last = (
            history.floor(settings.schedule.start),
            history.floor(settings.schedule.deadline),
        )
        if last.number >= history.upper.number:
            raise ValueError("Comet risk history requires a finalized following timestamp boundary")
        ranges[chain] = first, last
    count = sum(last.number - first.number + 1 for first, last in ranges.values())
    samples = sum(
        ranges[m.chain][1].number - ranges[m.chain][0].number + 1 for m in markets(config)
    )
    if count > settings.data.max_risk_blocks or samples > settings.data.max_risk_samples:
        raise ValueError(
            "Comet per-block risk acquisition exceeds its explicit block or sample limit"
        )
    return ranges


def group_at(history, at, start, max_blocks):
    block = history.floor(UnixTime(at))
    group = [block]
    if at != start:
        number = block.number - 1
        while number >= 0:
            previous = history.header(number)
            if previous.timestamp != block.timestamp:
                break
            if len(group) >= max_blocks:
                raise ValueError("Comet risk timestamp group exceeds the explicit block limit")
            group.insert(0, previous)
            number -= 1
    following = history.header(block.number + 1)
    if following.timestamp <= block.timestamp:
        raise ValueError("Comet risk timestamp floor is not the latest eligible block")
    return tuple(group), following


def risk_sample(history, config, source, at, settings):
    group, following = group_at(history, at, settings.schedule.start, settings.data.max_risk_blocks)
    reader = history.reader
    reader.used.clear()
    for header in (*group, following):
        history.pin(header)
    block = history.pin(group[-1])
    market = read_market(reader, source, block)
    validate_market(config, market)
    payload = RiskSample(market=market, block_group=group, following_block=following)
    fields, raw = envelope(reader, source, "risk", block, at)
    return RiskRecord(payload=payload, **fields), raw


def retained_risk_blocks(writer, source):
    # Cached headers count as completed work, not throughput in this attempt.
    return sum(
        len(RiskRecord.model_validate_json(record.model_dump_json()).payload.block_group)
        for span in writer.ranges(key(source, "risk"))
        for record in writer.range_records(span)
    )


def collect_risk(writer, history, config, source, settings):
    for left, right in writer.missing(key(source, "risk")):
        at = left
        while at <= right:
            record, raw = risk_sample(history, config, source, at, settings)
            end = min(right, record.payload.following_block.timestamp - 1)
            writer.commit_range(
                AcquiredRange(
                    key=record.key, start=UnixTime(at), end=UnixTime(end), evidence=record.evidence
                ),
                (record,),
                raw,
            )
            at = end + 1
            progress.advance(len(record.payload.block_group))


def accounting_sample(history, config, source, at):
    block = history.floor(UnixTime(at))
    reader = history.reader
    reader.used.clear()
    history.pin(block)
    market = read_market(reader, source, block)
    validate_market(config, market)
    fields, raw = envelope(reader, source, "accounting", block, at)
    return RateRecord(payload=market, **fields), raw


def collect_accounting(writer, history, config, source, settings):
    interval = settings.data.sample_seconds
    for left, right in writer.missing(key(source, "accounting")):
        for at in range(left, right + 1, interval):
            record, raw = accounting_sample(history, config, source, at)
            writer.commit_range(
                AcquiredRange(
                    key=record.key,
                    start=UnixTime(at),
                    end=UnixTime(min(right, at + interval - 1)),
                    evidence=record.evidence,
                ),
                (record,),
                raw,
            )
            progress.advance()
