"""Bounded V3 discovery and hash-pinned sampling through the historical gateway."""

from pydantic import model_validator

from almanak.backtest import progress
from almanak.backtest.dataset import RawEvidence
from almanak.backtest.gateway import ContractBinding
from almanak.backtest.prepared import AcquiredRange
from almanak.backtest.types import UnixTime
from almanak.caip import AssetType
from almanak.configuration.metadata import verify
from almanak.connectors.aave_v3.domain import Reserve
from almanak.connectors.aave_v3.economic_data import RateRecord, RiskRecord, RiskSample, key
from almanak.connectors.aave_v3.economic_parameters import allocations
from almanak.connectors.aave_v3.reader import LEGACY_RESERVE, read_reserve, verify_revision
from almanak.models import ZERO_ADDRESS, Address, Block, Chain, Model


class Discovery(Model):
    source: Reserve
    block: Block
    provider: Address
    a_token: AssetType
    variable_debt_token: AssetType

    @model_validator(mode="after")
    def scope(self):
        if self.source.chain != self.block.chain or self.provider == ZERO_ADDRESS:
            raise ValueError("discovered Pool metadata has a missing provider or different chain")
        for token in (self.a_token, self.variable_debt_token):
            if (
                token.chain != self.source.chain.chain_id
                or token.namespace != "erc20"
                or token.reference == ZERO_ADDRESS
            ):
                raise ValueError("discovered reserve tokens must retain their ERC-20 chain scope")
        if len({self.a_token, self.variable_debt_token, self.source.asset}) != 3:
            raise ValueError("reserve debt, supply and underlying tokens must be distinct")
        return self


def contracts(config, discoveries=()):
    bound: dict[Chain, set[Address]] = {}
    for allocation in allocations(config):
        source = allocation.reserve
        bound.setdefault(source.chain, set()).update(
            (source.pool, source.asset.reference, allocation.oracle, allocation.price_source)
        )
    for discovered in discoveries:
        bound[discovered.source.chain].update(
            (
                discovered.provider,
                discovered.a_token.reference,
                discovered.variable_debt_token.reference,
            )
        )
    return tuple(
        ContractBinding(chain=chain, contracts=tuple(sorted(addresses)))
        for chain, addresses in bound.items()
    )


def discover(history, source, at):
    block = history.pin(history.floor(UnixTime(at)))
    reader = history.reader
    verify_revision(reader, source.pool, block, "pool")
    if reader.call(source.pool, "POOL_REVISION()", block)[0] != 11:
        raise ValueError("V3 historical discovery requires the verified Pool revision")
    provider = reader.call(source.pool, "ADDRESSES_PROVIDER()", block, outputs=("address",))[0]
    data = reader.call(
        source.pool,
        "getReserveData(address)",
        block,
        inputs=("address",),
        args=(source.asset.reference,),
        outputs=(LEGACY_RESERVE,),
    )[0]
    if ZERO_ADDRESS in (provider, data[8], data[10]):
        raise ValueError("configured historical Pool reserve is missing its provider or tokens")
    return Discovery(
        source=source,
        block=block,
        provider=provider,
        a_token=AssetType(f"{source.chain.chain_id}/erc20:{data[8]}"),
        variable_debt_token=AssetType(f"{source.chain.chain_id}/erc20:{data[10]}"),
    )


def evidence(reader):
    return tuple(RawEvidence(body=reader.cache[k].model_dump_json()) for k in sorted(reader.used))


def envelope(reader, source, product, block, at):
    raw = evidence(reader)
    return {
        "key": key(source, product),
        "unit": "aave_v3_reserve_accounting" if product == "accounting" else "aave_v3_pool_risk",
        "reference": block,
        "source_time": UnixTime(block.timestamp),
        "available_at": UnixTime(at),
        "retrieved_at": max(reader.cache[k].retrieved_at for k in reader.used),
        "evidence": tuple(e.digest for e in raw),
    }, raw


def validate_quote(config, allocation, reserve):
    verify(config, reserve.asset)
    quote = reserve.oracle
    if quote is None or (quote.oracle, quote.feed) != (allocation.oracle, allocation.price_source):
        raise ValueError("historical V3 reserve lacks its configured oracle and price source")


def bounds(histories, config, settings):
    ranges = {}
    for chain, history in histories.items():
        first = history.floor(settings.schedule.start)
        last = history.floor(settings.schedule.deadline)
        if last.number >= history.upper.number:
            raise ValueError("risk history requires a finalized following timestamp boundary")
        ranges[chain] = first, last
    count = sum(last.number - first.number + 1 for first, last in ranges.values())
    samples = sum(
        ranges[a.chain][1].number - ranges[a.chain][0].number + 1 for a in allocations(config)
    )
    if count > settings.data.max_risk_blocks or samples > settings.data.max_risk_samples:
        raise ValueError("per-block V3 risk acquisition exceeds its explicit block or sample limit")
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
                raise ValueError("risk timestamp group exceeds the explicit block limit")
            group.insert(0, previous)
            number -= 1
    following = history.header(block.number + 1)
    if following.timestamp <= block.timestamp:
        raise ValueError("risk timestamp floor is not the latest eligible block")
    return tuple(group), following


def risk_sample(history, config, allocation, at, settings):
    group, following = group_at(history, at, settings.schedule.start, settings.data.max_risk_blocks)
    reader = history.reader
    reader.used.clear()
    for header in (*group, following):
        history.pin(header)
    block = history.pin(group[-1])
    reserve = read_reserve(reader, allocation.reserve, block)
    validate_quote(config, allocation, reserve)
    grace = reader.call(
        allocation.reserve.pool,
        "getLiquidationGracePeriod(address)",
        block,
        inputs=("address",),
        args=(allocation.reserve.asset.reference,),
        outputs=("uint40",),
    )[0]
    payload = RiskSample(
        reserve=reserve, liquidation_grace_until=grace, block_group=group, following_block=following
    )
    fields, raw = envelope(reader, allocation.reserve, "risk", block, at)
    return RiskRecord(payload=payload, **fields), raw


def retained_risk_blocks(writer, source):
    # Cached headers count as completed work, not throughput in this attempt.
    return sum(
        len(RiskRecord.model_validate_json(record.model_dump_json()).payload.block_group)
        for span in writer.ranges(key(source, "risk"))
        for record in writer.range_records(span)
    )


def collect_risk(writer, history, config, allocation, settings):
    missing = writer.missing(key(allocation.reserve, "risk"))
    for left, right in missing:
        at = left
        while at <= right:
            record, raw = risk_sample(history, config, allocation, at, settings)
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


def accounting_sample(history, config, allocation, at):
    block = history.floor(UnixTime(at))
    reader = history.reader
    reader.used.clear()
    history.pin(block)
    reserve = read_reserve(reader, allocation.reserve, block)
    validate_quote(config, allocation, reserve)
    fields, raw = envelope(reader, allocation.reserve, "accounting", block, at)
    return RateRecord(payload=reserve, **fields), raw


def collect_accounting(writer, history, config, allocation, settings):
    interval = settings.data.sample_seconds
    for left, right in writer.missing(key(allocation.reserve, "accounting")):
        for at in range(left, right + 1, interval):
            record, raw = accounting_sample(history, config, allocation, at)
            span = AcquiredRange(
                key=record.key,
                start=UnixTime(at),
                end=UnixTime(min(right, at + interval - 1)),
                evidence=record.evidence,
            )
            writer.commit_range(span, (record,), raw)
            progress.advance()
