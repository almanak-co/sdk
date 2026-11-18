"""Prepared HyperCore inputs: trade candles and hourly funding rates (D4, section 12).

A candle covers `[t, t + interval)` and is available when it closes; its close is the
last trade at or before that time. A funding rate is HyperCore's for its hour, written a
fraction of a second after the hour, and available from the next whole second. Both
are sampled products: every interval and every hour of the window must have its record,
so a missing candle or a missing funding hour stops preparation instead of being
carried over. Each record names the `/info` answer it came from.
"""

from decimal import Decimal

from almanak.backtest.dataset import DataKey, DataRequirement, InputRecord
from almanak.backtest.types import UnixTime
from almanak.connectors.identifiers import ConnectorId
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_shapes import Candle, DecimalText, FundingRate
from almanak.models import Chain, Model, UInt

CONNECTOR = ConnectorId("hyperliquid")
MODEL = "hypercore-trade-candle-perpetual-v1"
REVISION = "hypercore-info-history-v1"
HOUR = 3600
# Funding before this hour was paid every eight hours; those rates are refused (D4).
HOURLY_FUNDING_FROM = 1686186000  # 2023-06-08T01:00:00Z


class CandlePayload(Model):
    open: DecimalText
    high: DecimalText
    low: DecimalText
    close: DecimalText
    volume: DecimalText
    trades: UInt


class CandleReference(Model):
    coin: str
    interval: str
    open_time: UInt


class FundingPayload(Model):
    rate: DecimalText
    premium: DecimalText


class FundingReference(Model):
    coin: str
    # The venue's own millisecond time of the rate.
    time_ms: UInt


CandleRecord = InputRecord[CandlePayload, CandleReference]
FundingRecord = InputRecord[FundingPayload, FundingReference]


def candle_key(chain: Chain, coin: str, interval: str) -> DataKey:
    return DataKey(
        connector=CONNECTOR, chain=chain, source=f"perp:{coin}", product=f"candles_{interval}"
    )


def funding_key(chain: Chain, coin: str) -> DataKey:
    return DataKey(
        connector=CONNECTOR, chain=chain, source=f"perp:{coin}", product="funding_hourly"
    )


def candle_start(start: int, seconds: int) -> int:
    """The first candle availability the window needs: the close at or before its start."""
    return start // seconds * seconds


def funding_start(start: int) -> int:
    return start // HOUR * HOUR + 1


def candle_requirement(chain, coin, policy, start, end) -> DataRequirement:
    return DataRequirement(
        key=candle_key(chain, coin, policy.candles),
        start=UnixTime(candle_start(start, policy.candle_seconds)),
        end=UnixTime(end),
        semantics="sampled",
        sample_seconds=policy.candle_seconds,
        max_age_seconds=policy.max_age_seconds,
        unit="USDC_per_coin_trades",
        visibility="published",
        payload_revision="hypercore-trade-candle-v1",
    )


def funding_requirement(chain, coin, start, end) -> DataRequirement:
    """Hourly rates from the window's first hour, refused before hourly funding."""
    if start < HOURLY_FUNDING_FROM:
        raise ValueError("HyperCore funding was hourly only from 2023-06-08T01:00:00Z")
    return DataRequirement(
        key=funding_key(chain, coin),
        start=UnixTime(funding_start(start)),
        end=UnixTime(end),
        semantics="sampled",
        sample_seconds=HOUR,
        max_age_seconds=HOUR,
        unit="hourly_rate_fraction",
        visibility="published",
        payload_revision="hypercore-funding-rate-v1",
    )


def requirements(target, settings) -> tuple[DataRequirement, DataRequirement]:
    start, end = int(settings.schedule.start), int(settings.schedule.deadline)
    chain, coin = target.fund.chain, target.market.coin
    return (
        candle_requirement(chain, coin, settings.data, start, end),
        funding_requirement(chain, coin, start, end),
    )


def candle_record(key: DataKey, candle: Candle, seconds: int, digest: str, retrieved_at: float):
    """One candle as a record available at its close, refused off its grid."""
    opened = candle.t // 1000
    if candle.t % (seconds * 1000) or candle.T != candle.t + seconds * 1000 - 1:
        raise DataError("malformed", "HyperCore candle is off its interval grid")
    if not Decimal(candle.l) <= min(Decimal(candle.o), Decimal(candle.c)) or Decimal(
        candle.h
    ) < max(Decimal(candle.o), Decimal(candle.c)):
        raise DataError("malformed", "HyperCore candle range excludes its open or close")
    return CandleRecord(
        key=key,
        unit="USDC_per_coin_trades",
        source_time=UnixTime(opened),
        available_at=UnixTime(opened + seconds),
        period_end=UnixTime(opened + seconds),
        retrieved_at=retrieved_at,
        evidence=(digest,),
        reference=CandleReference(coin=candle.s, interval=candle.i, open_time=opened),
        payload=CandlePayload(
            open=candle.o,
            high=candle.h,
            low=candle.l,
            close=candle.c,
            volume=candle.v,
            trades=candle.n,
        ),
    )


def funding_record(key: DataKey, rate: FundingRate, digest: str, retrieved_at: float):
    """One rate as a record of its hour, available one second after it."""
    hour = rate.time // 1000 // HOUR * HOUR
    if rate.time - hour * 1000 >= 1000:
        raise DataError("malformed", "HyperCore funding rate is not written on its hour")
    return FundingRecord(
        key=key,
        unit="hourly_rate_fraction",
        source_time=UnixTime(hour),
        available_at=UnixTime(hour + 1),
        retrieved_at=retrieved_at,
        evidence=(digest,),
        reference=FundingReference(coin=rate.coin, time_ms=rate.time),
        payload=FundingPayload(rate=rate.fundingRate, premium=rate.premium),
    )
