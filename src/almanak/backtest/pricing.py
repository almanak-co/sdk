"""Explicit price identities shared by strategy, valuation and connector models."""

import hashlib
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.collection import CollectionLimits
from almanak.backtest.dataset import DataKey, InputRecord
from almanak.backtest.types import ConnectorId, Name
from almanak.backtest.vendor_discovery import CoinId, CoinMapping, require_contract
from almanak.caip import AssetType
from almanak.gateway.providers import EnvName
from almanak.models import Address, Block, Chain, Hash32, Model, UInt
from almanak.triggers import PositiveSeconds


class VendorCollectionLimits(CollectionLimits):
    batch_size: Literal[1] = 1


class CoinGeckoProvider(Model):
    kind: Literal["coingecko"] = "coingecko"
    api: Literal["demo", "pro"]
    api_key_env: EnvName
    collection: VendorCollectionLimits = VendorCollectionLimits()


class CoinGeckoPrice(Model):
    kind: Literal["coingecko"]
    provider: Name
    coin_id: CoinId | None = None
    quote: Literal["USD"]
    interval: Literal["hourly", "5m", "daily"]
    max_age_seconds: PositiveSeconds
    availability: Literal["retrospective_timestamp"]
    availability_lag_seconds: Annotated[int, Field(ge=0)]

    @property
    def seconds(self):
        return {"hourly": 3600, "5m": 300, "daily": 86400}[self.interval]

    @model_validator(mode="after")
    def age(self):
        if self.max_age_seconds < self.seconds + self.availability_lag_seconds:
            raise ValueError("vendor maximum age must cover resolution and availability lag")
        return self


# The candle lengths a pool price series takes, in seconds.
TIMEFRAMES = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "12h": 43200, "1d": 86400}
Timeframe = Literal["1m", "5m", "15m", "1h", "4h", "12h", "1d"]


class PoolSource(Model):
    """One pool's on-chain price, read through the operator's RPC provider.

    `pool` is the pool contract (Uniswap V3, PancakeSwap V3, Aerodrome Slipstream,
    Aerodrome classic, Curve) or the pool id of a Uniswap V4 pool, on the binding's
    chain. The binding's asset must be one of the pool's coins and is priced in
    `quote`, another of its coins, decimal-adjusted. `quote` defaults to the other coin
    of a two-coin pool and must be declared for a Curve pool of more coins. Each point
    is dated by its candle boundary and becomes visible `availability_lag_seconds`
    after it.
    """

    kind: Literal["pool_state", "pool_swaps"]
    connector: Literal[
        "uniswap_v3", "pancakeswap_v3", "aerodrome_slipstream", "uniswap_v4", "aerodrome", "curve"
    ]
    pool: Address | Hash32
    timeframe: Timeframe
    max_age_seconds: PositiveSeconds
    availability_lag_seconds: Annotated[int, Field(ge=0)]
    # Omitted when absent, so a series priced in a two-coin pool's other coin keeps
    # its identity.
    quote: AssetType | None = Field(default=None, exclude_if=lambda value: value is None)

    @property
    def seconds(self) -> int:
        return TIMEFRAMES[self.timeframe]

    @model_validator(mode="after")
    def pool_identity(self):
        if (self.connector == "uniswap_v4") != (len(self.pool) == 66):
            raise ValueError(
                "a Uniswap V4 price source names its pool id; other pools name their contract"
            )
        if self.max_age_seconds < self.seconds + self.availability_lag_seconds:
            raise ValueError("pool price maximum age must cover the timeframe and availability lag")
        return self


class PoolPrice(PoolSource):
    """The pool's price at the last block at or before each candle boundary: closes only."""

    kind: Literal["pool_state"]


class PoolSwapCandles(PoolSource):
    """OHLCV candles of the pool's trades between consecutive candle boundaries.

    A candle opens at the previous boundary's pool price and closes at this one's; its
    high and low also cover every trade's price, and its volume sums the absolute
    quote-token amounts of those trades. A trade's price is the pool's resulting spot
    price (V3-style and V4 Swap events, Aerodrome Sync events) or, for Curve, its
    execution price including fees (TokenExchange events between the asset and quote).
    A candle without trades is flat with zero volume.
    """

    kind: Literal["pool_swaps"]


PoolSeriesSource = PoolPrice | PoolSwapCandles


class ProtocolPrice(Model):
    kind: Literal["protocol_oracle"]
    key: DataKey


# A strategy series covers at most a year of hourly points; a decision request is bounded.
MAX_WINDOW = 8760


class PriceBinding(Model):
    purpose: Literal["risk", "valuation", "strategy"]
    chain: Chain
    asset: AssetType
    market: DataKey | None = None
    source: Annotated[
        CoinGeckoPrice | PoolPrice | PoolSwapCandles | ProtocolPrice, Field(discriminator="kind")
    ]
    # A strategy series: how many of the latest available points each decision sees.
    # Omitted at its default so existing bindings keep their identity.
    window: Annotated[int, Field(ge=1, le=MAX_WINDOW)] = Field(
        default=1, exclude_if=lambda value: value == 1
    )

    @model_validator(mode="after")
    def series(self):
        if self.window > 1 and self.purpose != "strategy":
            raise ValueError("only a strategy price declares a series window")
        if self.window > 1 and not isinstance(self.source, PoolSeriesSource):
            raise ValueError("a strategy price series reads a pool (PoolPrice or PoolSwapCandles)")
        if isinstance(self.source, PoolSeriesSource) and self.purpose != "strategy":
            raise ValueError("pool prices are only a strategy price source")
        return self

    @property
    def lookback_seconds(self) -> int:
        """History before a decision that its window needs, beyond the latest point."""
        if not isinstance(self.source, PoolSeriesSource):
            return 0
        return (self.window - 1) * self.source.seconds

    @model_validator(mode="after")
    def scope(self):
        if self.chain.chain_id != self.asset.chain:
            raise ValueError("price asset belongs to another chain")
        if self.market is not None and self.market.chain != self.chain:
            raise ValueError("price market belongs to another chain")
        if isinstance(self.source, ProtocolPrice) and self.source.key.chain != self.chain:
            raise ValueError("oracle source belongs to another chain")
        if isinstance(self.source, CoinGeckoPrice) and self.source.coin_id is None:
            require_contract(self.asset)
        quote = self.source.quote if isinstance(self.source, PoolSeriesSource) else None
        if quote is not None and (quote.chain != self.chain.chain_id or quote == self.asset):
            raise ValueError("a price quote is another asset on the binding's chain")
        return self

    @property
    def key(self):
        if isinstance(self.source, ProtocolPrice):
            return self.source.key
        identity = str(self.asset) + ":" + self.source.model_dump_json()
        source = self.source
        connector = source.connector if isinstance(source, PoolSeriesSource) else "vendor"
        return DataKey(
            connector=ConnectorId(connector),
            chain=self.chain,
            source=hashlib.sha256(identity.encode()).hexdigest(),
            product="price",
        )


class VendorBinding(Model):
    name: Name
    provider: CoinGeckoProvider


class VendorReference(Model):
    vendor: Literal["coingecko"] = "coingecko"
    coin_id: CoinId
    discovery: CoinMapping | None = None
    endpoint: str
    timestamp_ms: Annotated[int, Field(ge=0)]


class AssetPrice(Model):
    asset: AssetType
    source: CoinGeckoPrice
    quote: Literal["USD"]
    value: Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]


VendorPriceRecord = InputRecord[AssetPrice, VendorReference]


Price = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]


class PoolPriceReference(Model):
    """Where a pool price point was read: its boundary block and the raw pool state.

    `sqrt_price_x96` for V3-style and V4 pools; `state` for others: Aerodrome reserves,
    Curve StableSwap balances then A, or Curve crypto last prices.
    """

    # The last block at or before the boundary; its end-of-block state is the close.
    block: Block
    sqrt_price_x96: Annotated[int, Field(strict=True, gt=0)] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    state: tuple[UInt, ...] | None = Field(default=None, exclude_if=lambda v: v is None)
    # Swap candles: the previous boundary's block and how many trades of the pair
    # followed it.
    opened: Block | None = Field(default=None, exclude_if=lambda v: v is None)
    swaps: UInt | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def raw(self):
        if (self.sqrt_price_x96 is None) == (self.state is None):
            raise ValueError("a pool price reference holds one raw pool state")
        return self


class PoolPricePoint(Model):
    """The declared asset priced in the pool's other token (`quote`), decimal-adjusted.

    `volume` is in whole quote-token units. Open, high, low and volume are present only
    for swap candles.
    """

    asset: AssetType
    quote: AssetType
    source: PoolPrice | PoolSwapCandles
    close: Price
    open: Price | None = Field(default=None, exclude_if=lambda v: v is None)
    high: Price | None = Field(default=None, exclude_if=lambda v: v is None)
    low: Price | None = Field(default=None, exclude_if=lambda v: v is None)
    volume: Annotated[Decimal, Field(ge=0, allow_inf_nan=False)] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    @property
    def value(self) -> Decimal:
        return self.close


PoolPriceRecord = InputRecord[PoolPricePoint, PoolPriceReference]


def price_binding(bindings, purpose, chain, asset, market=None):
    matches = [
        b
        for b in bindings
        if (b.purpose, b.chain, b.asset) == (purpose, chain, asset) and b.market in (None, market)
    ]
    exact = [b for b in matches if b.market is not None]
    selected = exact or matches
    if len(selected) != 1:
        raise ValueError(f"{purpose} pricing requires one explicit source for {asset}")
    return selected[0]
