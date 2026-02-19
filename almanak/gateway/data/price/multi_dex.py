"""Multi-DEX Price Comparison Service.

This module provides a unified interface for comparing prices across multiple DEXs
including Uniswap V3, Curve, and Enso (aggregated). It enables strategies to find
the best execution venue for swaps and identify arbitrage opportunities.

Key Features:
    - Fetch quotes from multiple DEX protocols
    - Cross-DEX price comparison
    - Slippage estimation per venue
    - Caching to minimize redundant queries

Example:
    from almanak.gateway.data.price.multi_dex import MultiDexPriceService, Dex

    service = MultiDexPriceService(chain="ethereum")

    # Get prices from all DEXs
    prices = await service.get_prices_across_dexs(
        token_in="USDC",
        token_out="WETH",
        amount_in=Decimal("10000"),  # 10k USDC
    )

    # Find best DEX for the trade
    best = await service.get_best_dex_price(
        token_in="USDC",
        token_out="WETH",
        amount_in=Decimal("10000"),
    )
    print(f"Best venue: {best.dex} with {best.amount_out} WETH")
"""

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================


class Dex(StrEnum):
    """Supported DEX protocols."""

    UNISWAP_V3 = "uniswap_v3"
    CURVE = "curve"
    ENSO = "enso"


# Supported DEXs list
SUPPORTED_DEXS: list[str] = [d.value for d in Dex]

# DEXs available per chain
DEX_CHAINS: dict[str, list[str]] = {
    "ethereum": ["uniswap_v3", "curve", "enso"],
    "arbitrum": ["uniswap_v3", "curve", "enso"],
    "optimism": ["uniswap_v3", "enso"],
    "polygon": ["uniswap_v3", "enso"],
    "base": ["uniswap_v3", "enso"],
}

# Common tokens supported by DEXs
SUPPORTED_TOKENS: dict[str, list[str]] = {
    "ethereum": ["USDC", "USDT", "DAI", "WETH", "WBTC", "ETH", "FRAX", "stETH", "CRV"],
    "arbitrum": ["USDC", "USDC.e", "USDT", "DAI", "WETH", "WBTC", "ARB", "ETH"],
    "optimism": ["USDC", "USDC.e", "USDT", "DAI", "WETH", "OP", "ETH"],
    "polygon": ["USDC", "USDC.e", "USDT", "DAI", "WETH", "WBTC", "WMATIC", "MATIC"],
    "base": ["USDC", "WETH", "cbETH", "ETH"],
}

# Default cache TTL in seconds (12s = ~1 block)
DEFAULT_CACHE_TTL_SECONDS = 12.0


# =============================================================================
# Exceptions
# =============================================================================


class MultiDexPriceError(Exception):
    """Base exception for multi-DEX price service errors."""

    pass


class QuoteUnavailableError(MultiDexPriceError):
    """Raised when quote cannot be fetched from a DEX."""

    def __init__(self, dex: str, token_in: str, token_out: str, reason: str) -> None:
        self.dex = dex
        self.token_in = token_in
        self.token_out = token_out
        self.reason = reason
        super().__init__(f"Quote unavailable from {dex} for {token_in}->{token_out}: {reason}")


class DexNotSupportedError(MultiDexPriceError):
    """Raised when DEX is not supported on chain."""

    def __init__(self, dex: str, chain: str) -> None:
        self.dex = dex
        self.chain = chain
        supported = DEX_CHAINS.get(chain, [])
        super().__init__(f"DEX '{dex}' not supported on {chain}. Supported DEXs: {supported}")


class TokenNotSupportedError(MultiDexPriceError):
    """Raised when token is not supported."""

    def __init__(self, token: str, chain: str) -> None:
        self.token = token
        self.chain = chain
        super().__init__(f"Token '{token}' not supported on {chain}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class DexQuote:
    """Quote from a single DEX.

    Attributes:
        dex: DEX identifier (uniswap_v3, curve, enso)
        token_in: Input token symbol
        token_out: Output token symbol
        amount_in: Input amount (human-readable)
        amount_out: Output amount (human-readable)
        price: Effective price (amount_out / amount_in)
        price_impact_bps: Price impact in basis points
        slippage_estimate_bps: Estimated slippage in basis points
        gas_estimate: Estimated gas cost
        gas_cost_usd: Estimated gas cost in USD
        fee_bps: DEX fee in basis points
        route: Route description (e.g., pool addresses)
        timestamp: When quote was fetched
        chain: Blockchain network
    """

    dex: str
    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    price: Decimal
    price_impact_bps: int = 0
    slippage_estimate_bps: int = 0
    gas_estimate: int = 0
    gas_cost_usd: Decimal = Decimal("0")
    fee_bps: int = 0
    route: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    chain: str = "ethereum"

    @property
    def net_output(self) -> Decimal:
        """Get net output after estimated slippage."""
        if self.slippage_estimate_bps == 0:
            return self.amount_out
        slippage_factor = Decimal(10000 - self.slippage_estimate_bps) / Decimal(10000)
        return self.amount_out * slippage_factor

    @property
    def is_valid(self) -> bool:
        """Check if quote is valid (positive amounts)."""
        return self.amount_in > 0 and self.amount_out > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dex": self.dex,
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "price": str(self.price),
            "price_impact_bps": self.price_impact_bps,
            "slippage_estimate_bps": self.slippage_estimate_bps,
            "gas_estimate": self.gas_estimate,
            "gas_cost_usd": str(self.gas_cost_usd),
            "fee_bps": self.fee_bps,
            "route": self.route,
            "timestamp": self.timestamp.isoformat(),
            "chain": self.chain,
        }


@dataclass
class MultiDexPriceResult:
    """Result of a multi-DEX price query.

    Attributes:
        token_in: Input token symbol
        token_out: Output token symbol
        amount_in: Input amount
        quotes: Dictionary mapping DEX name to quote
        timestamp: When query was made
        chain: Blockchain network
    """

    token_in: str
    token_out: str
    amount_in: Decimal
    quotes: dict[str, DexQuote]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    chain: str = "ethereum"

    @property
    def best_quote(self) -> DexQuote | None:
        """Get the best quote (highest output amount)."""
        if not self.quotes:
            return None
        return max(self.quotes.values(), key=lambda q: q.amount_out)

    @property
    def best_net_quote(self) -> DexQuote | None:
        """Get the best quote after slippage estimation."""
        if not self.quotes:
            return None
        return max(self.quotes.values(), key=lambda q: q.net_output)

    @property
    def price_spread_bps(self) -> int:
        """Get spread between best and worst quotes in basis points."""
        if len(self.quotes) < 2:
            return 0
        amounts = [q.amount_out for q in self.quotes.values() if q.is_valid]
        if len(amounts) < 2:
            return 0
        best = max(amounts)
        worst = min(amounts)
        if worst == 0:
            return 0
        return int((best - worst) / worst * 10000)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "quotes": {dex: q.to_dict() for dex, q in self.quotes.items()},
            "best_quote": self.best_quote.to_dict() if self.best_quote else None,
            "price_spread_bps": self.price_spread_bps,
            "timestamp": self.timestamp.isoformat(),
            "chain": self.chain,
        }


@dataclass
class BestDexResult:
    """Result of a best DEX query.

    Attributes:
        token_in: Input token symbol
        token_out: Output token symbol
        amount_in: Input amount
        best_dex: Best DEX for the trade
        best_quote: Quote from the best DEX
        all_quotes: All quotes from different DEXs
        savings_vs_worst_bps: Savings vs worst venue in basis points
        timestamp: When comparison was made
    """

    token_in: str
    token_out: str
    amount_in: Decimal
    best_dex: str | None
    best_quote: DexQuote | None
    all_quotes: list[DexQuote]
    savings_vs_worst_bps: int = 0
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "best_dex": self.best_dex,
            "best_quote": self.best_quote.to_dict() if self.best_quote else None,
            "all_quotes": [q.to_dict() for q in self.all_quotes],
            "savings_vs_worst_bps": self.savings_vs_worst_bps,
            "timestamp": self.timestamp.isoformat(),
        }


# =============================================================================
# Multi-DEX Price Service
# =============================================================================


class MultiDexPriceService:
    """Unified multi-DEX price comparison service.

    This class provides a single interface for fetching quotes and comparing
    prices across Uniswap V3, Curve, and Enso aggregator. It handles caching,
    error recovery, and cross-DEX comparison.

    Attributes:
        chain: Blockchain network
        cache_ttl_seconds: How long to cache quotes (default 12s)
        dexs: List of DEXs to query

    Example:
        service = MultiDexPriceService(chain="ethereum")

        # Get prices from all DEXs
        result = await service.get_prices_across_dexs(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )

        # Get best DEX
        best = await service.get_best_dex_price(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10000"),
        )
    """

    def __init__(
        self,
        chain: str = "ethereum",
        cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
        dexs: list[str] | None = None,
        mock_quotes: dict[str, Callable[..., DexQuote]] | None = None,
        token_resolver: "TokenResolver | None" = None,
    ) -> None:
        """Initialize the multi-DEX price service.

        Args:
            chain: Blockchain network
            cache_ttl_seconds: Cache TTL in seconds
            dexs: DEXs to query (default: all supported on chain)
            mock_quotes: Optional mock quote functions for testing
            token_resolver: Optional TokenResolver instance. Defaults to get_token_resolver().
        """
        self._chain = chain
        self._cache_ttl_seconds = cache_ttl_seconds
        self._mock_quotes = mock_quotes or {}

        # Token resolver (unified token resolution)
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Validate chain
        if chain not in DEX_CHAINS:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {list(DEX_CHAINS.keys())}")

        # Set DEXs to query
        if dexs is None:
            self._dexs = DEX_CHAINS.get(chain, [])
        else:
            # Validate DEXs
            supported = DEX_CHAINS.get(chain, [])
            for dex in dexs:
                if dex not in supported:
                    raise DexNotSupportedError(dex, chain)
            self._dexs = dexs

        # Quote cache: (token_in, token_out, amount_in, dex) -> (quote, timestamp)
        self._quote_cache: dict[str, tuple[DexQuote, float]] = {}

        logger.info(f"MultiDexPriceService initialized for chain={chain}, dexs={self._dexs}")

    @property
    def chain(self) -> str:
        """Get the blockchain network."""
        return self._chain

    @property
    def dexs(self) -> list[str]:
        """Get the list of DEXs to query."""
        return self._dexs

    def _get_cache_key(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        dex: str,
    ) -> str:
        """Generate cache key for a quote."""
        return f"{self._chain}:{token_in}:{token_out}:{amount_in}:{dex}"

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached quote is still valid."""
        if cache_key not in self._quote_cache:
            return False
        _, cached_time = self._quote_cache[cache_key]
        return (time.time() - cached_time) < self._cache_ttl_seconds

    def _get_cached_quote(self, cache_key: str) -> DexQuote | None:
        """Get cached quote if valid."""
        if self._is_cache_valid(cache_key):
            quote, _ = self._quote_cache[cache_key]
            logger.debug(f"Cache hit for {cache_key}")
            return quote
        return None

    def _cache_quote(self, cache_key: str, quote: DexQuote) -> None:
        """Cache a quote."""
        self._quote_cache[cache_key] = (quote, time.time())

    def _resolve_token_address(self, token: str) -> str:
        """Resolve token symbol to address.

        Uses the unified TokenResolver as the single source of truth.
        """
        try:
            resolved = self._token_resolver.resolve(token, self._chain)
            return resolved.address
        except Exception:
            logger.debug(
                "TokenResolver failed for %s on %s",
                token,
                self._chain,
            )

        # Assume it's already an address
        if token.startswith("0x") and len(token) == 42:
            return token
        raise TokenNotSupportedError(token, self._chain)

    def _get_token_decimals(self, token: str) -> int:
        """Get decimals for a token.

        Uses the unified TokenResolver as the single source of truth.
        Raises TokenNotSupportedError if the token cannot be resolved.
        """
        try:
            resolved = self._token_resolver.resolve(token, self._chain)
            return resolved.decimals
        except Exception as e:
            logger.warning(
                "TokenResolver failed for decimals of %s on %s",
                token,
                self._chain,
            )
            raise TokenNotSupportedError(token, self._chain) from e

    def _amount_to_wei(self, amount: Decimal, token: str) -> int:
        """Convert human-readable amount to wei."""
        decimals = self._get_token_decimals(token)
        return int(amount * Decimal(10**decimals))

    def _wei_to_amount(self, wei: int, token: str) -> Decimal:
        """Convert wei to human-readable amount."""
        decimals = self._get_token_decimals(token)
        return Decimal(wei) / Decimal(10**decimals)

    async def _get_uniswap_v3_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
    ) -> DexQuote:
        """Get quote from Uniswap V3.

        This uses estimated rates based on typical Uniswap V3 pool behavior.
        In production, this would call the QuoterV2 contract.
        """
        # Check for mock
        if "uniswap_v3" in self._mock_quotes:
            return self._mock_quotes["uniswap_v3"](token_in, token_out, amount_in)

        # Default quote estimation (realistic estimates)
        # In production, would call QuoterV2.quoteExactInputSingle
        base_price = self._get_default_price(token_in, token_out)
        amount_out = amount_in * base_price

        # Estimate price impact based on trade size
        # Larger trades have higher impact (simplified model)
        trade_size_usd = float(amount_in) if token_in in ["USDC", "USDT", "DAI"] else float(amount_in) * 2500
        price_impact_bps = self._estimate_price_impact(trade_size_usd, "uniswap_v3")

        # Apply price impact to output
        impact_factor = Decimal(10000 - price_impact_bps) / Decimal(10000)
        amount_out = amount_out * impact_factor

        # Uniswap V3 typical fee tiers: 0.01%, 0.05%, 0.3%, 1%
        # Most common is 0.3% (30 bps)
        fee_bps = 30

        # Slippage estimate based on liquidity
        slippage_bps = self._estimate_slippage(trade_size_usd, "uniswap_v3")

        return DexQuote(
            dex="uniswap_v3",
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            price=amount_out / amount_in if amount_in > 0 else Decimal("0"),
            price_impact_bps=price_impact_bps,
            slippage_estimate_bps=slippage_bps,
            gas_estimate=150000,  # Typical Uniswap V3 swap
            gas_cost_usd=Decimal("5.00"),  # Varies with gas price
            fee_bps=fee_bps,
            route="Direct pool",
            chain=self._chain,
        )

    async def _get_curve_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
    ) -> DexQuote:
        """Get quote from Curve.

        Curve is optimized for stableswaps with very low slippage.
        In production, this would call the pool's get_dy function.
        """
        # Check for mock
        if "curve" in self._mock_quotes:
            return self._mock_quotes["curve"](token_in, token_out, amount_in)

        # Check if this is a stablecoin pair (Curve's specialty)
        stables = {"USDC", "USDT", "DAI", "FRAX"}
        is_stable_pair = token_in in stables and token_out in stables

        # LST pairs (stETH/ETH, cbETH/ETH)
        lst_pairs = {
            ("ETH", "stETH"),
            ("stETH", "ETH"),
            ("WETH", "stETH"),
            ("stETH", "WETH"),
            ("ETH", "cbETH"),
            ("cbETH", "ETH"),
            ("WETH", "cbETH"),
            ("cbETH", "WETH"),
        }
        is_lst_pair = (token_in, token_out) in lst_pairs

        if not is_stable_pair and not is_lst_pair:
            # Curve may not have good liquidity for this pair
            # Return a less competitive quote
            base_price = self._get_default_price(token_in, token_out)
            amount_out = amount_in * base_price * Decimal("0.995")  # 0.5% worse
        else:
            # Stablecoin or LST pair - Curve excels here
            base_price = self._get_default_price(token_in, token_out)
            amount_out = amount_in * base_price

        trade_size_usd = float(amount_in) if token_in in ["USDC", "USDT", "DAI"] else float(amount_in) * 2500

        # Curve has very low price impact for stable pairs
        if is_stable_pair or is_lst_pair:
            price_impact_bps = max(1, int(trade_size_usd / 1000000))  # 1 bp per $1M
        else:
            price_impact_bps = self._estimate_price_impact(trade_size_usd, "curve")

        impact_factor = Decimal(10000 - price_impact_bps) / Decimal(10000)
        amount_out = amount_out * impact_factor

        # Curve fee is typically 0.04% for stableswaps
        fee_bps = 4 if is_stable_pair else 30

        # Very low slippage for stable pairs
        slippage_bps = 1 if is_stable_pair else self._estimate_slippage(trade_size_usd, "curve")

        return DexQuote(
            dex="curve",
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            price=amount_out / amount_in if amount_in > 0 else Decimal("0"),
            price_impact_bps=price_impact_bps,
            slippage_estimate_bps=slippage_bps,
            gas_estimate=200000,  # Typical Curve swap
            gas_cost_usd=Decimal("7.00"),
            fee_bps=fee_bps,
            route="3pool" if is_stable_pair else "CryptoSwap",
            chain=self._chain,
        )

    async def _get_enso_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
    ) -> DexQuote:
        """Get quote from Enso (aggregator).

        Enso aggregates across multiple DEXs and finds optimal routes.
        In production, this would call the Enso API.
        """
        # Check for mock
        if "enso" in self._mock_quotes:
            return self._mock_quotes["enso"](token_in, token_out, amount_in)

        # Enso as aggregator typically finds the best route
        # This is a simplified simulation
        base_price = self._get_default_price(token_in, token_out)

        # Aggregators often get slightly better rates by splitting orders
        # Add a small bonus (0.1%) to simulate this
        amount_out = amount_in * base_price * Decimal("1.001")

        trade_size_usd = float(amount_in) if token_in in ["USDC", "USDT", "DAI"] else float(amount_in) * 2500

        # Aggregators minimize price impact through route optimization
        price_impact_bps = max(1, self._estimate_price_impact(trade_size_usd, "uniswap_v3") - 5)

        impact_factor = Decimal(10000 - price_impact_bps) / Decimal(10000)
        amount_out = amount_out * impact_factor

        # Aggregator fees vary but typically competitive
        fee_bps = 20  # Aggregator may add small fee

        # Good slippage due to route optimization
        slippage_bps = max(1, self._estimate_slippage(trade_size_usd, "uniswap_v3") - 2)

        return DexQuote(
            dex="enso",
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            price=amount_out / amount_in if amount_in > 0 else Decimal("0"),
            price_impact_bps=price_impact_bps,
            slippage_estimate_bps=slippage_bps,
            gas_estimate=250000,  # Aggregator routes can be more complex
            gas_cost_usd=Decimal("10.00"),
            fee_bps=fee_bps,
            route="Multi-DEX aggregated",
            chain=self._chain,
        )

    def _get_default_price(self, token_in: str, token_out: str) -> Decimal:
        """Get default price for a token pair.

        These are approximate market prices for simulation.
        In production, this would use real-time price feeds.
        """
        # Price of tokens in USD (approximate)
        prices_usd: dict[str, Decimal] = {
            "ETH": Decimal("2500"),
            "WETH": Decimal("2500"),
            "USDC": Decimal("1"),
            "USDT": Decimal("1"),
            "DAI": Decimal("1"),
            "FRAX": Decimal("1"),
            "WBTC": Decimal("45000"),
            "stETH": Decimal("2480"),  # Slight discount to ETH
            "cbETH": Decimal("2600"),  # Slight premium
            "ARB": Decimal("0.80"),
            "OP": Decimal("1.50"),
            "MATIC": Decimal("0.50"),
            "WMATIC": Decimal("0.50"),
            "CRV": Decimal("0.40"),
        }

        price_in = prices_usd.get(token_in, Decimal("1"))
        price_out = prices_usd.get(token_out, Decimal("1"))

        if price_out == 0:
            return Decimal("0")

        return price_in / price_out

    def _estimate_price_impact(self, trade_size_usd: float, dex: str) -> int:
        """Estimate price impact in basis points.

        Larger trades have higher impact. This is a simplified model.
        """
        # Base impact per $100k traded
        base_impacts = {
            "uniswap_v3": 10,  # 10 bps per $100k
            "curve": 5,  # 5 bps per $100k (better for stable pairs)
            "enso": 8,  # 8 bps per $100k (aggregated)
        }

        base_impact = base_impacts.get(dex, 10)

        # Scale with trade size (non-linear)
        multiplier = (trade_size_usd / 100000) ** 0.7
        impact = int(base_impact * multiplier)

        # Cap at 500 bps (5%)
        return min(impact, 500)

    def _estimate_slippage(self, trade_size_usd: float, dex: str) -> int:
        """Estimate slippage in basis points.

        Slippage represents the difference between expected and actual execution price.
        """
        # Base slippage per $100k
        base_slippage = {
            "uniswap_v3": 5,
            "curve": 2,
            "enso": 4,
        }

        base = base_slippage.get(dex, 5)

        # Scale with trade size
        multiplier = (trade_size_usd / 100000) ** 0.5
        slippage = int(base * multiplier)

        # Minimum 1 bp, cap at 200 bps
        return max(1, min(slippage, 200))

    async def get_quote(
        self,
        dex: str,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
    ) -> DexQuote:
        """Get quote from a specific DEX.

        Args:
            dex: DEX identifier
            token_in: Input token symbol
            token_out: Output token symbol
            amount_in: Input amount (human-readable)

        Returns:
            DexQuote with price and execution details

        Raises:
            DexNotSupportedError: If DEX not supported on chain
            QuoteUnavailableError: If quote cannot be fetched
        """
        # Validate DEX
        if dex not in self._dexs:
            raise DexNotSupportedError(dex, self._chain)

        # Check cache
        cache_key = self._get_cache_key(token_in, token_out, amount_in, dex)
        cached = self._get_cached_quote(cache_key)
        if cached:
            return cached

        try:
            if dex == "uniswap_v3":
                quote = await self._get_uniswap_v3_quote(token_in, token_out, amount_in)
            elif dex == "curve":
                quote = await self._get_curve_quote(token_in, token_out, amount_in)
            elif dex == "enso":
                quote = await self._get_enso_quote(token_in, token_out, amount_in)
            else:
                raise DexNotSupportedError(dex, self._chain)

            # Cache the quote
            self._cache_quote(cache_key, quote)
            return quote

        except Exception as e:
            if isinstance(e, DexNotSupportedError | QuoteUnavailableError):
                raise
            raise QuoteUnavailableError(dex, token_in, token_out, str(e)) from e

    async def get_prices_across_dexs(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        dexs: list[str] | None = None,
    ) -> MultiDexPriceResult:
        """Get prices from all DEXs.

        Fetches quotes from all specified DEXs in parallel and returns
        a comparison of prices and execution details.

        Args:
            token_in: Input token symbol
            token_out: Output token symbol
            amount_in: Input amount (human-readable)
            dexs: DEXs to query (default: all configured)

        Returns:
            MultiDexPriceResult with quotes from all DEXs
        """
        dexs_to_query = dexs or self._dexs

        # Fetch quotes in parallel
        async def fetch_quote(dex: str) -> tuple[str, DexQuote | None, str | None]:
            try:
                quote = await self.get_quote(dex, token_in, token_out, amount_in)
                return (dex, quote, None)
            except Exception as e:
                logger.warning(f"Failed to get quote from {dex}: {e}")
                return (dex, None, str(e))

        results = await asyncio.gather(
            *[fetch_quote(dex) for dex in dexs_to_query],
            return_exceptions=False,
        )

        # Collect successful quotes
        quotes: dict[str, DexQuote] = {}
        for dex, quote, error in results:
            if quote is not None:
                quotes[dex] = quote
            else:
                logger.debug(f"No quote from {dex}: {error}")

        return MultiDexPriceResult(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            quotes=quotes,
            chain=self._chain,
        )

    async def get_best_dex_price(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        dexs: list[str] | None = None,
    ) -> BestDexResult:
        """Get the best DEX for a trade.

        Compares prices from all DEXs and returns the one with the highest
        output amount (best execution).

        Args:
            token_in: Input token symbol
            token_out: Output token symbol
            amount_in: Input amount (human-readable)
            dexs: DEXs to compare (default: all configured)

        Returns:
            BestDexResult with the optimal venue and comparison data
        """
        result = await self.get_prices_across_dexs(token_in, token_out, amount_in, dexs)

        all_quotes = list(result.quotes.values())

        if not all_quotes:
            return BestDexResult(
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                best_dex=None,
                best_quote=None,
                all_quotes=[],
                savings_vs_worst_bps=0,
            )

        # Find best quote (highest output)
        best_quote = max(all_quotes, key=lambda q: q.amount_out)

        # Calculate savings vs worst
        worst_output = min(q.amount_out for q in all_quotes)
        if worst_output > 0:
            savings_bps = int((best_quote.amount_out - worst_output) / worst_output * 10000)
        else:
            savings_bps = 0

        return BestDexResult(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            best_dex=best_quote.dex,
            best_quote=best_quote,
            all_quotes=all_quotes,
            savings_vs_worst_bps=savings_bps,
        )

    def clear_cache(self) -> None:
        """Clear the quote cache."""
        self._quote_cache.clear()
        logger.debug("Quote cache cleared")

    def set_mock_quote(
        self,
        dex: str,
        mock_fn: Callable[[str, str, Decimal], DexQuote],
    ) -> None:
        """Set a mock quote function for testing.

        Args:
            dex: DEX to mock
            mock_fn: Function that returns a DexQuote
        """
        self._mock_quotes[dex] = mock_fn

    def clear_mock_quotes(self) -> None:
        """Clear all mock quote functions."""
        self._mock_quotes.clear()
