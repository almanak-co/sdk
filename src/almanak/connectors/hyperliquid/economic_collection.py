"""Acquire HyperCore candles and funding rates for a prepared dataset (D4, section 12).

Candle pages are fixed by the window alone: at most `CANDLE_PAGE` candles each, every
one of which the answer must hold, so a candle the venue no longer serves (it keeps the
latest 5000 of an interval) stops preparation with that reason. Funding pages follow
the venue's own paging: an answer of `FUNDING_PAGE` rates continues from the last one.
Each answer is retained whole as the evidence its records name.
"""

from almanak.backtest.dataset import RawEvidence
from almanak.backtest.hypercore_history import HistoryScope
from almanak.backtest.prepared import AcquiredRange
from almanak.connectors.hyperliquid.economic_data import (
    HOUR,
    CandleRecord,
    FundingRecord,
    candle_record,
    funding_record,
    requirements,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import (
    MAX_CANDLES,
    CandleRange,
    CandleSnapshot,
    FundingHistory,
    InfoRequest,
    typed,
)

CANDLE_PAGE = MAX_CANDLES
FUNDING_PAGE = 500


def contract_bindings(settings, config, boundaries=None):
    """HyperCore history needs no HyperEVM contract reads."""
    return ()


def candle_opens(requirement) -> tuple[int, ...]:
    """Open times, in seconds, of the candles whose closes the requirement samples."""
    seconds = requirement.sample_seconds
    first = int(requirement.start) - seconds
    return tuple(range(first, int(requirement.end) - seconds + 1, seconds))


def candle_queries(coin, interval, requirement) -> tuple[CandleSnapshot, ...]:
    """Pages of at most `CANDLE_PAGE` candles, fixed by the requirement alone."""
    opens = candle_opens(requirement)
    return tuple(
        CandleSnapshot(
            req=CandleRange(
                coin=coin, interval=interval, start_time=page[0] * 1000, end_time=page[-1] * 1000
            )
        )
        for page in (
            opens[index : index + CANDLE_PAGE] for index in range(0, len(opens), CANDLE_PAGE)
        )
    )


def candle_pages(coin, interval, requirement) -> tuple[InfoRequest, ...]:
    return tuple(InfoRequest(info=query) for query in candle_queries(coin, interval, requirement))


def scopes(config, settings) -> tuple[HistoryScope, ...]:
    """The gateway's authority: exactly the reads the requirements need."""
    result = []
    for allocation in config.allocations:
        target = config.target(allocation.name)
        candles, funding = requirements(target, settings)
        opens = candle_opens(candles)
        coin = target.market.coin
        result.append(
            HistoryScope(
                product="candles",
                coin=coin,
                interval=settings.data.candles,
                start_ms=opens[0] * 1000,
                end_ms=opens[-1] * 1000,
            )
        )
        result.append(
            HistoryScope(
                product="funding",
                coin=coin,
                start_ms=(int(funding.start) - 1) * 1000,
                end_ms=int(funding.end) * 1000,
            )
        )
    return tuple(result)


def collect(writer, client, target, settings):
    """Commit each of the target's requirements the dataset does not hold yet."""
    candles, funding = requirements(target, settings)
    if writer.missing(candles.key):
        commit(writer, candles, read_candles(client, target, settings, candles))
    if writer.missing(funding.key):
        commit(writer, funding, read_funding(client, target, funding))


def commit(writer, requirement, read):
    """Commit one requirement whole, naming every answer its records came from."""
    records, evidence = read
    writer.commit_range(
        AcquiredRange(
            key=requirement.key,
            start=requirement.start,
            end=requirement.end,
            evidence=tuple(sorted({item.digest for item in evidence})),
        ),
        records,
        evidence,
    )


def read_candles(client, target, settings, requirement):
    """Every candle the requirement samples, refusing a page that lacks one."""
    seconds = settings.data.candle_seconds
    records: list[CandleRecord] = []
    evidence: list[RawEvidence] = []
    for query in candle_queries(target.market.coin, settings.data.candles, requirement):
        result = client.fetch(InfoRequest(info=query))
        body = RawEvidence(body=result.model_dump_json())
        asked = query.req
        expected = tuple(range(asked.start_time, asked.end_time + 1, seconds * 1000))
        answer = typed(result)
        if tuple(candle.t for candle in answer) != expected:
            raise DataError(
                "unavailable",
                f"HyperCore serves only its latest 5000 {asked.interval} candles; "
                f"{asked.coin} candles from {asked.start_time} to {asked.end_time} are "
                "missing or out of order",
            )
        received = result.received_at / 1000
        records.extend(
            candle_record(requirement.key, candle, seconds, body.digest, received)
            for candle in answer
        )
        evidence.append(body)
    return tuple(records), tuple(evidence)


def read_funding(client, target, requirement):
    """Every hourly rate of the requirement, following the venue's 500-rate pages."""
    coin, end_ms = target.market.coin, int(requirement.end) * 1000
    start_ms = (int(requirement.start) - 1) * 1000
    records: list[FundingRecord] = []
    evidence: list[RawEvidence] = []
    while True:
        request = InfoRequest(info=FundingHistory(coin=coin, start_time=start_ms, end_time=end_ms))
        result = client.fetch(request)
        body = RawEvidence(body=result.model_dump_json())
        answer = typed(result)
        received = result.received_at / 1000
        records.extend(
            funding_record(requirement.key, rate, body.digest, received)
            for rate in answer
            if requirement.start <= rate.time // 1000 // HOUR * HOUR + 1 <= requirement.end
        )
        evidence.append(body)
        if len(answer) < FUNDING_PAGE:
            return tuple(records), tuple(evidence)
        start_ms = answer[-1].time + 1
