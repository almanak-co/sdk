"""Bounded Morpho samples through the dedicated read-only historical gateway."""

from pydantic import model_validator

from almanak.backtest import progress
from almanak.backtest.dataset import RawEvidence
from almanak.backtest.gateway import ContractBinding
from almanak.backtest.prepared import AcquiredRange
from almanak.backtest.types import UnixTime
from almanak.configuration.metadata import verify
from almanak.connectors.morpho_blue.deployment import oracle_dependencies, read_deployment
from almanak.connectors.morpho_blue.domain import Market
from almanak.connectors.morpho_blue.economic_data import RateRecord, RiskRecord, RiskSample, key
from almanak.connectors.morpho_blue.economic_parameters import markets
from almanak.connectors.morpho_blue.reader import read_market
from almanak.models import ZERO_ADDRESS, Address, Block, Chain, Model


class Discovery(Model):
    source: Market
    block: Block
    feeds: tuple[Address, ...]

    @model_validator(mode="after")
    def scope(self):
        if self.source.chain != self.block.chain or ZERO_ADDRESS in self.feeds:
            raise ValueError("Morpho discovered feeds must have the configured chain scope")
        if len(set(self.feeds)) != len(self.feeds) or len(self.feeds) > 4:
            raise ValueError("Morpho discovered feeds require distinct supported oracle roles")
        return self


def contracts(config, discoveries=()):
    bound: dict[Chain, set[Address]] = {}
    for source in markets(config):
        bound.setdefault(source.chain, set()).update(
            (
                source.core,
                source.irm,
                source.oracle,
                source.loan_asset.reference,
                source.collateral_asset.reference,
            )
        )
    for discovered in discoveries:
        bound[discovered.source.chain].update(discovered.feeds)
    return tuple(
        ContractBinding(chain=chain, contracts=tuple(sorted(addresses)))
        for chain, addresses in bound.items()
    )


def discover(history, source, at):
    block = history.pin(history.floor(UnixTime(at)))
    deployment = read_deployment(history.reader, source, block)
    feeds = oracle_dependencies(deployment)
    return Discovery(source=source, block=block, feeds=feeds)


def envelope(reader, source, product, block, at):
    raw = tuple(RawEvidence(body=reader.cache[k].model_dump_json()) for k in sorted(reader.used))
    return {
        "key": key(source, product),
        "unit": "morpho_blue_market_accounting"
        if product == "accounting"
        else "morpho_blue_market_risk",
        "reference": block,
        "source_time": UnixTime(block.timestamp),
        "available_at": UnixTime(at),
        "retrieved_at": max(reader.cache[k].retrieved_at for k in reader.used),
        "evidence": tuple(e.digest for e in raw),
    }, raw


def validate_market(config, market):
    verify(config, market.loan_asset)
    verify(config, market.collateral_asset)


def bounds(histories, config, settings):
    ranges = {}
    for chain, history in histories.items():
        first, last = (
            history.floor(settings.schedule.start),
            history.floor(settings.schedule.deadline),
        )
        if last.number >= history.upper.number:
            raise ValueError(
                "Morpho risk history requires a finalized following timestamp boundary"
            )
        ranges[chain] = first, last
    count = sum(last.number - first.number + 1 for first, last in ranges.values())
    samples = sum(
        ranges[m.chain][1].number - ranges[m.chain][0].number + 1 for m in markets(config)
    )
    if count > settings.data.max_risk_blocks or samples > settings.data.max_risk_samples:
        raise ValueError(
            "Morpho per-block risk acquisition exceeds its explicit block or sample limit"
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
                raise ValueError("Morpho risk timestamp group exceeds the explicit block limit")
            group.insert(0, previous)
            number -= 1
    following = history.header(block.number + 1)
    if following.timestamp <= block.timestamp:
        raise ValueError("Morpho risk timestamp floor is not the latest eligible block")
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
