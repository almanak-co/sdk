"""Core data provenance models for the Quant Data Layer.

Provides standardized metadata (DataMeta) and a generic envelope (DataEnvelope)
that wraps any data value with source, staleness, confidence, and finality info.

Also provides Instrument, a canonical identifier for trading pairs that avoids
CEX/DEX symbol confusion across all data methods.

Example:
    from almanak.framework.data.models import DataMeta, DataEnvelope, DataClassification

    meta = DataMeta(
        source="alchemy_rpc",
        observed_at=datetime.now(UTC),
        block_number=19_000_000,
        finality="finalized",
        staleness_ms=120,
        latency_ms=45,
        confidence=0.99,
        cache_hit=False,
    )

    envelope = DataEnvelope(value=my_pool_price, meta=meta)
    # Transparent delegation:
    envelope.price  # delegates to envelope.value.price

    # Instrument for canonical pair identification:
    from almanak.framework.data.models import Instrument, resolve_instrument
    inst = Instrument(base="WETH", quote="USDC", chain="arbitrum")
    inst = resolve_instrument("WETH/USDC", "arbitrum")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum

logger = logging.getLogger(__name__)


class DataClassification(Enum):
    """Classification controlling fail-closed vs graceful-fallback semantics.

    EXECUTION_GRADE: data used for trade decisions. Fails closed on error --
        no fallback to off-chain or degraded sources.
    INFORMATIONAL: data used for analytics, display, or non-critical decisions.
        Falls back through provider chain with degraded confidence.
    """

    EXECUTION_GRADE = "execution_grade"
    INFORMATIONAL = "informational"


@dataclass(frozen=True)
class DataMeta:
    """Provenance metadata attached to every data value.

    Attributes:
        source: Provider name (e.g. 'alchemy_rpc', 'geckoterminal', 'binance').
        observed_at: UTC timestamp when the value was observed at the source.
        block_number: On-chain block number (None for off-chain data).
        finality: Block finality level or 'off_chain' for API data.
        staleness_ms: Milliseconds since the value was observed.
        latency_ms: Milliseconds between request and response.
        confidence: 0.0 (unreliable) to 1.0 (fully confident).
        cache_hit: Whether this value was served from cache.
    """

    source: str
    observed_at: datetime
    block_number: int | None = None
    finality: str = "off_chain"
    staleness_ms: int = 0
    latency_ms: int = 0
    confidence: float = 1.0
    cache_hit: bool = False
    proxy_source: str | None = None

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"confidence must be between 0.0 and 1.0, got {self.confidence}")
        valid_finalities = {"finalized", "safe", "latest", "off_chain"}
        if self.finality not in valid_finalities:
            raise ValueError(f"finality must be one of {valid_finalities}, got '{self.finality}'")

    @property
    def is_on_chain(self) -> bool:
        """Whether this data originated from on-chain reads."""
        return self.block_number is not None

    @property
    def is_finalized(self) -> bool:
        """Whether the underlying block is finalized."""
        return self.finality == "finalized"


@dataclass(frozen=True)
class DataEnvelope[T]:
    """Generic envelope wrapping a value with provenance metadata.

    Supports transparent attribute delegation: accessing an attribute that
    doesn't exist on DataEnvelope itself is forwarded to ``self.value``.

    Example:
        @dataclass
        class PoolPrice:
            price: Decimal
            tick: int

        envelope = DataEnvelope(value=PoolPrice(price=Decimal("1800"), tick=200), meta=meta)
        envelope.price  # -> Decimal("1800")  (delegated to value)
        envelope.meta   # -> DataMeta(...)     (own attribute)
    """

    value: T
    meta: DataMeta
    classification: DataClassification = field(default=DataClassification.INFORMATIONAL)

    def __getattr__(self, name: str) -> object:
        # Only called when normal attribute lookup fails.
        # Delegate to the wrapped value.
        try:
            return getattr(self.value, name)
        except AttributeError:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'") from None

    @property
    def is_fresh(self) -> bool:
        """Convenience: data observed within the last 60 seconds."""
        age = (datetime.now(UTC) - self.meta.observed_at).total_seconds()
        return age < 60

    @property
    def is_execution_grade(self) -> bool:
        """Whether this envelope carries execution-grade data."""
        return self.classification == DataClassification.EXECUTION_GRADE


# ---------------------------------------------------------------------------
# Native token -> wrapped token mapping for canonicalization
# ---------------------------------------------------------------------------

# Maps uppercase native symbol -> wrapped symbol for canonical resolution.
# ETH -> WETH, MATIC -> WMATIC, AVAX -> WAVAX, BNB -> WBNB, etc.
_NATIVE_TO_WRAPPED: dict[str, str] = {
    "ETH": "WETH",
    "MATIC": "WMATIC",
    "AVAX": "WAVAX",
    "BNB": "WBNB",
    "MNT": "WMNT",
    "S": "WS",
    "XPL": "WXPL",
}


# ---------------------------------------------------------------------------
# OHLCV Proxy Map — wrapped token -> unwrapped token for data fallback
# ---------------------------------------------------------------------------

# Curated allowlist of wrapped tokens whose OHLCV data can be proxied from
# the unwrapped (native) token. ONLY includes 1:1 pegged canonical wrapped
# natives where the smart contract guarantees equal value.
#
# This map is intentionally flat (not chain-keyed) because wrapping semantics
# are a property of the token contract, not the chain.
#
# Do NOT add rebasing tokens, fee-on-wrap tokens, or liquid staking
# derivatives (stETH, wstETH) to this map.
OHLCV_PROXY_MAP: dict[str, str] = {
    "WMNT": "MNT",
    "WS": "S",
    "WXPL": "XPL",
    "WETH": "ETH",
    "WAVAX": "AVAX",
    "WMATIC": "MATIC",
    "WBNB": "BNB",
}


# ---------------------------------------------------------------------------
# CEX Symbol Map
# ---------------------------------------------------------------------------

# Maps (exchange, base_symbol, quote_symbol) -> exchange-specific trading symbol.
# Base/quote are canonical (wrapped) forms.
CEX_SYMBOL_MAP: dict[tuple[str, str, str], str] = {
    # Binance - uses unwrapped names, concatenated pairs
    ("binance", "WETH", "USDC"): "ETHUSDC",
    ("binance", "WETH", "USDT"): "ETHUSDT",
    ("binance", "WBTC", "USDT"): "BTCUSDT",
    ("binance", "WBTC", "USDC"): "BTCUSDC",
    ("binance", "LINK", "USDT"): "LINKUSDT",
    ("binance", "UNI", "USDT"): "UNIUSDT",
    ("binance", "AAVE", "USDT"): "AAVEUSDT",
    ("binance", "ARB", "USDT"): "ARBUSDT",
    ("binance", "OP", "USDT"): "OPUSDT",
    ("binance", "WMATIC", "USDT"): "MATICUSDT",
    ("binance", "WAVAX", "USDT"): "AVAXUSDT",
    ("binance", "WBNB", "USDT"): "BNBUSDT",
    ("binance", "GMX", "USDT"): "GMXUSDT",
    ("binance", "CRV", "USDT"): "CRVUSDT",
    ("binance", "PENDLE", "USDT"): "PENDLEUSDT",
    ("binance", "DAI", "USDT"): "DAIUSDT",
    # Coinbase
    ("coinbase", "WETH", "USDC"): "ETH-USD",
    ("coinbase", "WBTC", "USDC"): "BTC-USD",
    ("coinbase", "LINK", "USDC"): "LINK-USD",
    ("coinbase", "UNI", "USDC"): "UNI-USD",
    ("coinbase", "AAVE", "USDC"): "AAVE-USD",
    ("coinbase", "ARB", "USDC"): "ARB-USD",
    ("coinbase", "OP", "USDC"): "OP-USD",
}


@dataclass(frozen=True)
class Instrument:
    """Canonical trading pair identifier for CEX/DEX symbol disambiguation.

    Native tokens are canonicalized to their wrapped form (ETH -> WETH,
    MATIC -> WMATIC, etc.) to avoid confusion. Bridged variants remain
    explicit (USDC vs USDC.e).

    Attributes:
        base: Base token symbol in canonical (wrapped) form (e.g. "WETH", "WBTC").
        quote: Quote token symbol (e.g. "USDC", "USDT").
        chain: Chain name (e.g. "arbitrum", "base", "ethereum").
        venue: Optional venue/protocol name (e.g. "uniswap_v3", "binance").
        address: Optional pool or market contract address.
    """

    base: str
    quote: str
    chain: str
    venue: str | None = None
    address: str | None = None

    def __post_init__(self) -> None:
        if not self.base:
            raise ValueError("base symbol cannot be empty")
        if not self.quote:
            raise ValueError("quote symbol cannot be empty")
        if not self.chain:
            raise ValueError("chain cannot be empty")
        # Normalize to uppercase symbols and lowercase chain
        object.__setattr__(self, "base", self.base.upper())
        object.__setattr__(self, "quote", self.quote.upper())
        object.__setattr__(self, "chain", self.chain.lower())
        if self.venue is not None:
            object.__setattr__(self, "venue", self.venue.lower())

    @property
    def pair(self) -> str:
        """Return 'BASE/QUOTE' string."""
        return f"{self.base}/{self.quote}"

    def cex_symbol(self, exchange: str) -> str | None:
        """Look up the exchange-specific symbol for this instrument.

        Args:
            exchange: Exchange name (e.g. "binance", "coinbase").

        Returns:
            Exchange-specific symbol string, or None if not mapped.
        """
        return CEX_SYMBOL_MAP.get((exchange.lower(), self.base, self.quote))


def _canonicalize_symbol(symbol: str) -> str:
    """Canonicalize a token symbol by mapping native tokens to wrapped form.

    ETH -> WETH, MATIC -> WMATIC, AVAX -> WAVAX, BNB -> WBNB, etc.
    All other symbols are returned as-is (uppercased).
    """
    upper = symbol.upper()
    return _NATIVE_TO_WRAPPED.get(upper, upper)


def resolve_instrument(
    token_or_instrument: str | Instrument,
    chain: str,
    venue: str | None = None,
    *,
    quote: str | None = None,
) -> Instrument:
    """Resolve a plain string or Instrument into a canonical Instrument.

    Accepts:
      - An existing Instrument (returned as-is if chain matches, else new with updated chain)
      - A "BASE/QUOTE" string (e.g. "ETH/USDC", "WETH/USDC", "USDC.e/WETH")
      - A single token symbol (e.g. "WETH") -- quote defaults to "USDC"

    Native tokens are canonicalized to wrapped form: ETH -> WETH, MATIC -> WMATIC, etc.
    Bridged variants (USDC.e, USDT.e) are kept explicit.

    Args:
        token_or_instrument: String pair like "ETH/USDC" or single symbol, or Instrument.
        chain: Chain name (e.g. "arbitrum", "ethereum").
        venue: Optional venue/protocol name.

    Returns:
        Canonical Instrument with normalized symbols.
    """
    if isinstance(token_or_instrument, Instrument):
        if token_or_instrument.chain == chain.lower():
            return token_or_instrument
        return Instrument(
            base=token_or_instrument.base,
            quote=token_or_instrument.quote,
            chain=chain,
            venue=venue or token_or_instrument.venue,
            address=token_or_instrument.address,
        )

    raw = token_or_instrument.strip()
    if "/" in raw:
        parts = raw.split("/", 1)
        base_raw = parts[0].strip()
        quote_raw = parts[1].strip()
    else:
        base_raw = raw
        quote_raw = quote or "USDC"

    base = _canonicalize_symbol(base_raw)
    quote = _canonicalize_symbol(quote_raw)

    return Instrument(base=base, quote=quote, chain=chain, venue=venue)
