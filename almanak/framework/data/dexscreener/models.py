"""Data models for DexScreener API responses.

All models are dataclasses for lightweight internal use. Fields match
the DexScreener REST API response schema.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DexTxnCounts:
    """Buy/sell transaction counts for a time window."""

    buys: int = 0
    sells: int = 0

    @property
    def total(self) -> int:
        return self.buys + self.sells

    @property
    def buy_ratio(self) -> float:
        """Fraction of transactions that are buys (0.0-1.0)."""
        if self.total == 0:
            return 0.5
        return self.buys / self.total


@dataclass(frozen=True)
class DexTxns:
    """Transaction counts across multiple time windows."""

    m5: DexTxnCounts = field(default_factory=DexTxnCounts)
    h1: DexTxnCounts = field(default_factory=DexTxnCounts)
    h6: DexTxnCounts = field(default_factory=DexTxnCounts)
    h24: DexTxnCounts = field(default_factory=DexTxnCounts)


@dataclass(frozen=True)
class DexVolume:
    """Trading volume in USD across time windows."""

    m5: float = 0.0
    h1: float = 0.0
    h6: float = 0.0
    h24: float = 0.0


@dataclass(frozen=True)
class DexPriceChange:
    """Price change percentage across time windows."""

    m5: float = 0.0
    h1: float = 0.0
    h6: float = 0.0
    h24: float = 0.0


@dataclass(frozen=True)
class DexLiquidity:
    """Pool liquidity in various denominations."""

    usd: float = 0.0
    base: float = 0.0
    quote: float = 0.0


@dataclass(frozen=True)
class DexToken:
    """Token info within a pair."""

    address: str = ""
    name: str = ""
    symbol: str = ""


@dataclass(frozen=True)
class DexPair:
    """A trading pair from DexScreener.

    Contains price, volume, liquidity, transaction counts, and metadata
    for a specific token pair on a specific DEX.
    """

    chain_id: str = ""
    dex_id: str = ""
    pair_address: str = ""
    url: str = ""

    base_token: DexToken = field(default_factory=DexToken)
    quote_token: DexToken = field(default_factory=DexToken)

    price_native: str = ""
    price_usd: str = ""

    txns: DexTxns = field(default_factory=DexTxns)
    volume: DexVolume = field(default_factory=DexVolume)
    price_change: DexPriceChange = field(default_factory=DexPriceChange)
    liquidity: DexLiquidity = field(default_factory=DexLiquidity)

    fdv: float | None = None
    market_cap: float | None = None
    pair_created_at: int | None = None  # Unix timestamp in milliseconds

    labels: list[str] = field(default_factory=list)
    boost_active: int = 0

    @property
    def price_usd_float(self) -> float:
        """Parse price_usd string to float, 0.0 on failure."""
        try:
            return float(self.price_usd) if self.price_usd else 0.0
        except (ValueError, TypeError):
            return 0.0

    @property
    def age_hours(self) -> float | None:
        """Age of the pair in hours since creation, or None if unknown."""
        if self.pair_created_at is None:
            return None
        import time

        age_seconds = time.time() - (self.pair_created_at / 1000)
        return max(0.0, age_seconds / 3600)


@dataclass(frozen=True)
class BoostedToken:
    """A boosted/promoted token on DexScreener."""

    chain_id: str = ""
    token_address: str = ""
    url: str = ""
    icon: str = ""
    description: str = ""
    amount: int = 0
    total_amount: int = 0


def parse_pair(raw: dict) -> DexPair:
    """Parse a raw DexScreener pair JSON dict into a DexPair."""
    txns_raw = raw.get("txns", {})
    txns = DexTxns(
        m5=_parse_txn_counts(txns_raw.get("m5", {})),
        h1=_parse_txn_counts(txns_raw.get("h1", {})),
        h6=_parse_txn_counts(txns_raw.get("h6", {})),
        h24=_parse_txn_counts(txns_raw.get("h24", {})),
    )

    vol_raw = raw.get("volume", {})
    volume = DexVolume(
        m5=_safe_float(vol_raw.get("m5")),
        h1=_safe_float(vol_raw.get("h1")),
        h6=_safe_float(vol_raw.get("h6")),
        h24=_safe_float(vol_raw.get("h24")),
    )

    pc_raw = raw.get("priceChange", {})
    price_change = DexPriceChange(
        m5=_safe_float(pc_raw.get("m5")),
        h1=_safe_float(pc_raw.get("h1")),
        h6=_safe_float(pc_raw.get("h6")),
        h24=_safe_float(pc_raw.get("h24")),
    )

    liq_raw = raw.get("liquidity", {}) or {}
    liquidity = DexLiquidity(
        usd=_safe_float(liq_raw.get("usd")),
        base=_safe_float(liq_raw.get("base")),
        quote=_safe_float(liq_raw.get("quote")),
    )

    base_raw = raw.get("baseToken", {}) or {}
    quote_raw = raw.get("quoteToken", {}) or {}

    boosts_raw = raw.get("boosts", {}) or {}

    return DexPair(
        chain_id=raw.get("chainId", ""),
        dex_id=raw.get("dexId", ""),
        pair_address=raw.get("pairAddress", ""),
        url=raw.get("url", ""),
        base_token=DexToken(
            address=base_raw.get("address", ""),
            name=base_raw.get("name", ""),
            symbol=base_raw.get("symbol", ""),
        ),
        quote_token=DexToken(
            address=quote_raw.get("address", ""),
            name=quote_raw.get("name", ""),
            symbol=quote_raw.get("symbol", ""),
        ),
        price_native=str(raw.get("priceNative", "")),
        price_usd=str(raw.get("priceUsd", "")),
        txns=txns,
        volume=volume,
        price_change=price_change,
        liquidity=liquidity,
        fdv=_safe_float_or_none(raw.get("fdv")),
        market_cap=_safe_float_or_none(raw.get("marketCap")),
        pair_created_at=raw.get("pairCreatedAt"),
        labels=raw.get("labels", []) or [],
        boost_active=int(boosts_raw.get("active", 0)),
    )


def parse_boosted_token(raw: dict) -> BoostedToken:
    """Parse a raw boosted token JSON dict."""
    return BoostedToken(
        chain_id=raw.get("chainId", ""),
        token_address=raw.get("tokenAddress", ""),
        url=raw.get("url", ""),
        icon=raw.get("icon", ""),
        description=raw.get("description", ""),
        amount=int(raw.get("amount", 0)),
        total_amount=int(raw.get("totalAmount", 0)),
    )


def _parse_txn_counts(raw: dict) -> DexTxnCounts:
    return DexTxnCounts(buys=int(raw.get("buys", 0)), sells=int(raw.get("sells", 0)))


def _safe_float(val) -> float:
    try:
        return float(val) if val is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def _safe_float_or_none(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
