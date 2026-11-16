"""Capture consecutive risk blocks and exact published GMX opportunities within bounds."""

from collections.abc import Callable
from itertools import groupby, pairwise

from almanak.backtest import progress
from almanak.backtest.gateway import ContractBinding
from almanak.backtest.history_logs import LogScope
from almanak.backtest.prepared import AcquiredRange
from almanak.backtest.types import UnixTime
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.gmx_v2 import economic_data as data
from almanak.connectors.gmx_v2.deployments import records as deployment_records
from almanak.connectors.gmx_v2.domain import Target
from almanak.connectors.gmx_v2.economic_funding_reader import funding_indexes, prepared_funding
from almanak.connectors.gmx_v2.economic_liquidity_risk import LiquidityRiskState
from almanak.connectors.gmx_v2.economic_liquidity_risk_reader import liquidity_risk
from almanak.connectors.gmx_v2.economic_perpetual_risk import PerpetualRiskState
from almanak.connectors.gmx_v2.economic_pnl_reader import prepared_pnl
from almanak.connectors.gmx_v2.economic_position_fee_reader import (
    position_fee_policies,
    referral_storage,
)
from almanak.connectors.gmx_v2.economic_position_reader import (
    perpetual_risk,
    prepared_impact_settlement,
    prepared_position_pricing,
)
from almanak.connectors.gmx_v2.economic_prefetch import RiskReads
from almanak.connectors.gmx_v2.economic_solvency_reader import market_solvency
from almanak.connectors.gmx_v2.economic_spot_reader import prepared_spot
from almanak.connectors.gmx_v2.economic_swap_risk import SwapRiskState
from almanak.connectors.gmx_v2.economic_swap_risk_reader import swap_risk
from almanak.connectors.gmx_v2.economic_valuation_reader import prepared_accounting
from almanak.connectors.gmx_v2.published_collection import (
    publication_prices,
    publication_scope,
    scan_publications,
)
from almanak.connectors.v3_prepared_acquisition import evidence
from almanak.models import ZERO_ADDRESS, Address, Chain

RISK_READERS: dict[type, Callable] = {
    SwapRiskState: swap_risk,
    LiquidityRiskState: liquidity_risk,
    PerpetualRiskState: perpetual_risk,
}


def risk_reader(settings):
    return RISK_READERS[data.risk_family(settings)]


def resolve_boundaries(histories, config, settings):
    result = {}
    for chain in dict.fromkeys(a.chain for a in config.allocations):
        history = histories[chain]
        first = history.floor(settings.schedule.start)
        last = history.floor(settings.schedule.deadline)
        if last.number - first.number + 1 > settings.data.maximum_risk_blocks:
            raise ValueError("GMX historical risk range exceeds its explicit block limit")
        result[chain] = first, last
    return result


def contract_bindings(settings, config, boundaries=None):
    if {a.chain for a in config.allocations} != {h.chain for h in settings.history}:
        raise ValueError("GMX historical providers must cover exactly the configured chains")
    result = []
    for binding in settings.history:
        selected = tuple(t for t in targets(config) if t.fund.chain == binding.chain)
        addresses, sources = set(), []
        for target in selected:
            market = target.market
            source = settings.data.publication(target.allocation)
            addresses.update(market.contracts.model_dump().values())
            addresses.update(deployment_records(data.accounting_market(market)))
            addresses.update(
                asset.reference
                for asset in (
                    market.long_asset,
                    market.short_asset,
                    market.share_asset,
                    market.index_asset,
                    market.wrapped_native,
                )
            )
            addresses.update((source.handler, source.oracle))
            if boundaries is not None:
                sources.append(publication_scope(market, *boundaries[binding.chain]))
        scopes = tuple(dict.fromkeys(sources))
        result.append(
            ContractBinding(
                chain=binding.chain,
                contracts=tuple(sorted(addresses)),
                logs=tuple(LogScope(**scope.model_dump()) for scope in scopes),
                transaction_sources=scopes,
            )
        )
    return tuple(result)


def dependency_bindings(histories, config, boundaries, contracts):
    """Expand perpetual read authority with the handler's selected ReferralStorage.

    Position fee policy reads trader referral state from the contract the verified
    OrderHandler selects. It is outside the compiled deployment closure, so it is
    discovered at both canonical range boundaries and must not change between them.
    The result carries no caller scopes; apply it before the final Oracle grants.
    """
    from almanak.connectors.gmx_v2.economic_authority import expanded

    addresses: dict[Chain, set[Address]] = {}
    for target in targets(config):
        if not isinstance(target, Target):
            continue
        history = histories[target.fund.chain]
        found = {
            referral_storage(history.reader, target.market, history.pin(boundary))
            for boundary in boundaries[target.fund.chain]
        }
        if len(found) != 1:
            raise ValueError("GMX referral storage changed inside the prepared block range")
        addresses.setdefault(target.fund.chain, set()).update(found - {ZERO_ADDRESS})
    return expanded(contracts, addresses)


def publication_candidates(history, target, settings, first, last):
    return scan_publications(
        history.reader,
        data.accounting_market(target.market),
        settings.data.publication(target.allocation),
        first,
        last,
        settings.data.publication_limits,
    )


def read_perpetual_opportunity(reader, target, published, transaction_index, risk):
    if not isinstance(target, Target) or data.accounting_market(target.market) != risk.market:
        raise ValueError("GMX perpetual opportunities require their configured position target")
    market, block, prices = risk.market, risk.block, published.prices
    index = next(price for price in prices if price.asset == market.index_asset)
    spot = prepared_spot(reader, market, block, prices, pricing_type="SWAP", ui_factor=0)
    pricing = prepared_position_pricing(reader, market, block, index)
    return data.PerpetualOpportunity(
        publication=published,
        transaction_index=transaction_index,
        accounting=prepared_accounting(reader, spot, index, pnl_type="TRADERS"),
        impact=prepared_impact_settlement(reader, pricing, prices),
        funding=prepared_funding(reader, market, block, prices),
        funding_indexes=funding_indexes(reader, market, block),
        pnl=prepared_pnl(reader, market, block, prices),
        fees=position_fee_policies(reader, target, block),
        risk=risk,
        solvency=market_solvency(reader, market, block),
    )


def read_opportunities(reader, market, source, candidates, risk, target=None):
    from almanak.connectors.gmx_v2.economic_liquidity_collection import opportunity

    results = []
    for transaction, identity in sorted(candidates.items(), key=lambda item: item[1][2]):
        published = publication_prices(reader, market, source, transaction, identity)
        if published is None:
            continue
        if isinstance(risk, PerpetualRiskState):
            results.append(read_perpetual_opportunity(reader, target, published, identity[2], risk))
            continue
        if isinstance(risk, LiquidityRiskState):
            results.append(opportunity(reader, market, published, identity[2], risk))
            continue
        spot = prepared_spot(
            reader, market, risk.block, published.prices, pricing_type="SWAP", ui_factor=0
        )
        results.append(
            data.SwapOpportunity(
                publication=published,
                transaction_index=identity[2],
                spot=spot,
                risk=risk,
                solvency=market_solvency(reader, market, risk.block),
            )
        )
    return tuple(results)


def read_frames(history, target, settings, first, last, candidates):
    reader = history.reader
    family = data.risk_family(settings)
    market = data.frame_market(target, family)
    source = settings.data.publication(target.allocation)
    headers = tuple(history.header(number) for number in range(first.number, last.number + 1))
    if any(a.timestamp > b.timestamp for a, b in pairwise(headers)):
        raise ValueError("GMX canonical historical timestamps moved backwards")
    frames = []
    risk_reads = RiskReads(RISK_READERS[family])
    with progress.phase(
        f"preparation.risk.{target.allocation}",
        "Capturing bounded GMX risk and publications",
        len(headers),
        "blocks",
    ):
        for _, group in groupby(headers, key=lambda block: block.timestamp):
            reader.used.clear()
            risks, opportunities = [], []
            for anchor in group:
                block = history.pin(anchor)
                risk = risk_reads.read(reader, market, block)
                risks.append(risk)
                selected = {
                    tx: identity
                    for tx, identity in candidates.items()
                    if identity[0] == block.number
                }
                opportunities.extend(
                    read_opportunities(reader, market, source, selected, risk, target)
                )
                progress.advance()
            frame = data.MarketFrame(
                target=target, risk=tuple(risks), opportunities=tuple(opportunities)
            )
            frames.append((frame, evidence(reader)))
    return tuple(frames)


def collect(writer, history, target, settings):
    if not writer.missing(data.key(target)):
        return
    reader = history.reader
    reader.used.clear()
    reader._rpc("eth_chainId", [])
    first = history.floor(settings.schedule.start)
    last = history.floor(settings.schedule.deadline)
    if reader.pin_block("finalized") != history.upper:
        raise ValueError("GMX preparation changed its retained finalized history anchor")
    for header in tuple(history.headers.values()):
        history.pin(header)
    if last.number - first.number + 1 > settings.data.maximum_risk_blocks:
        raise ValueError("GMX historical risk range exceeds its explicit block limit")
    if last.number == history.upper.number:
        raise ValueError("GMX preparation requires a later finalized boundary header")
    history.pin(history.header(first.number + 1))
    history.pin(history.header(last.number + 1))
    candidates = publication_candidates(history, target, settings, first, last)
    boundary_raw = evidence(reader)
    frames = read_frames(history, target, settings, first, last, candidates)
    history.pin(first)
    history.pin(last)
    raw = tuple(
        {
            value.digest: value
            for value in (
                *boundary_raw,
                *(value for _, values in frames for value in values),
                *evidence(reader),
            )
        }.values()
    )
    digests = tuple(value.digest for value in raw)
    retrieved = max(reader.cache[key].retrieved_at for key in reader.used)
    records = tuple(
        data.Record(
            key=data.key(target),
            unit=data.UNIT,
            payload=frame,
            reference=frame.block,
            source_time=UnixTime(frame.block.timestamp),
            available_at=UnixTime(max(settings.schedule.start, frame.block.timestamp)),
            retrieved_at=retrieved,
            evidence=tuple(dict.fromkeys(value.digest for value in (*boundary_raw, *values))),
        )
        for frame, values in frames
    )
    writer.commit_range(
        AcquiredRange(
            key=data.key(target),
            start=settings.schedule.start,
            end=settings.schedule.deadline,
            evidence=digests,
        ),
        records,
        raw,
    )
