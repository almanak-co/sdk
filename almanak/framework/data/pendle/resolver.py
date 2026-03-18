"""Pendle market resolver: dynamic market discovery replacing hardcoded dicts.

Wraps PendleAPIClient with a clean query interface for agents and strategies.
Falls back to static registry when API is unavailable.

Usage:
    resolver = PendleMarketResolver("ethereum")
    markets = resolver.find_markets(underlying="sUSDe", active_only=True)
    best = resolver.get_best_market(underlying="sUSDe")
    market = resolver.resolve_by_pt_symbol("PT-sUSDe-7MAY2026")
"""

import logging
import time

from .api_client import CHAIN_ID_MAP, PendleAPIClient, PendleAPIError
from .models import PendleMarketData

logger = logging.getLogger(__name__)


class PendleMarketResolver:
    """Dynamic Pendle market discovery.

    Replaces hardcoded dicts (MARKET_BY_PT_TOKEN, PT_TOKEN_INFO, etc.)
    with live API lookups. Falls back to static dicts when API is unavailable.

    The resolver is designed so that any agent (Claude Code, LangGraph, or a
    strategy's decide()) can discover valid markets without hardcoded addresses.
    """

    def __init__(
        self,
        chain: str,
        api_client: PendleAPIClient | None = None,
        cache_ttl: float = 300.0,
    ):
        """Initialize the resolver.

        Args:
            chain: Target chain name (ethereum, arbitrum, etc.)
            api_client: Optional pre-configured API client. Created automatically if None.
            cache_ttl: How long to cache the market list (seconds). Default 5 min.
        """
        if chain not in CHAIN_ID_MAP:
            raise ValueError(f"Unsupported chain for Pendle: {chain}. Supported: {list(CHAIN_ID_MAP.keys())}")

        self._chain = chain

        if api_client is not None and api_client.chain != chain:
            raise ValueError(
                f"api_client chain mismatch: api_client.chain={api_client.chain!r} != chain={chain!r}. "
                f"The injected PendleAPIClient must target the same chain as the resolver."
            )

        self._api_client = api_client or PendleAPIClient(chain, cache_ttl_seconds=60.0)
        self._cache_ttl = cache_ttl
        self._market_cache: list[PendleMarketData] | None = None
        self._markets_by_pt_address: dict[str, PendleMarketData] = {}
        self._cache_expiry: float = 0.0

        # Pre-process static dicts for O(1) lookups
        self._preprocessed_pt_info = self._preprocess_static_pt_info()
        self._preprocessed_yt_info = self._preprocess_static_yt_info()
        self._preprocessed_market_by_pt = self._preprocess_static_market_by_pt()
        self._preprocessed_mint_sy = self._preprocess_static_mint_sy()

    @property
    def chain(self) -> str:
        return self._chain

    # =========================================================================
    # Public API
    # =========================================================================

    def find_markets(
        self,
        underlying: str | None = None,
        active_only: bool = True,
    ) -> list[PendleMarketData]:
        """Find Pendle markets, optionally filtered by underlying asset.

        Args:
            underlying: Filter by underlying asset symbol or address.
                        Supports partial match (e.g., "sUSDe", "wstETH", "USDT").
            active_only: If True, exclude expired markets.

        Returns:
            List of matching PendleMarketData, sorted by liquidity (desc).
        """
        markets = self._get_markets()

        if active_only:
            now = int(time.time())
            markets = [m for m in markets if not m.is_expired and (m.expiry == 0 or m.expiry > now)]

        if underlying:
            query = underlying.lower()
            markets = [m for m in markets if self._matches_underlying(m, query)]

        markets.sort(key=lambda m: m.liquidity_usd, reverse=True)
        return markets

    def get_best_market(self, underlying: str) -> PendleMarketData | None:
        """Get the best active market for an underlying asset.

        "Best" = highest liquidity among non-expired markets matching the underlying.

        Args:
            underlying: Underlying asset symbol or address (e.g., "sUSDe", "wstETH").

        Returns:
            Best PendleMarketData or None if no matches.
        """
        markets = self.find_markets(underlying=underlying, active_only=True)
        return markets[0] if markets else None

    def resolve_by_pt_symbol(self, pt_symbol: str) -> PendleMarketData | None:
        """Resolve a PT symbol to its market data.

        Tries API markets first (matching by pt_symbol field), then falls back
        to static MARKET_BY_PT_TOKEN dict.

        Args:
            pt_symbol: PT token symbol (e.g., "PT-sUSDe-7MAY2026", "PT-wstETH")

        Returns:
            PendleMarketData or None if not found.
        """
        pt_lower = pt_symbol.lower()

        # Try API markets -- match by symbol if available
        for market in self._get_markets():
            if market.pt_symbol and market.pt_symbol.lower() == pt_lower:
                return market

        # Try matching via static PT_TOKEN_INFO (address cross-reference)
        pt_info = self._static_pt_lookup(pt_symbol)
        if pt_info:
            pt_addr = pt_info[0].lower()
            # Use O(1) index if available, otherwise scan
            self._get_markets()  # ensure cache is populated
            if pt_addr in self._markets_by_pt_address:
                return self._markets_by_pt_address[pt_addr]

        # Fall back to static dict -> API single-market fetch
        return self._fallback_resolve_pt(pt_symbol)

    def resolve_by_market_address(self, market_address: str) -> PendleMarketData | None:
        """Resolve a market address to its data.

        Args:
            market_address: Market contract address.

        Returns:
            PendleMarketData or None.
        """
        addr_lower = market_address.lower()
        for market in self._get_markets():
            if market.market_address and market.market_address.lower() == addr_lower:
                return market

        # Try direct API fetch for addresses not in the top-100 list
        try:
            return self._api_client.get_market_data(market_address)
        except PendleAPIError:
            return None

    def resolve_pt_token_info(self, pt_symbol: str) -> tuple[str, int] | None:
        """Resolve PT token symbol to (address, decimals).

        Tries API first, falls back to static PT_TOKEN_INFO.

        Args:
            pt_symbol: PT token symbol.

        Returns:
            (address, decimals) or None.
        """
        # Try API
        market = self.resolve_by_pt_symbol(pt_symbol)
        if market and market.pt_address:
            return (market.pt_address, market.pt_decimals)

        # Fall back to static
        return self._static_pt_lookup(pt_symbol)

    def resolve_yt_token_info(self, yt_symbol: str) -> tuple[str, int] | None:
        """Resolve YT token symbol to (address, decimals).

        Tries static dict, then attempts PT-equivalent cross-reference.

        Args:
            yt_symbol: YT token symbol.

        Returns:
            (address, decimals) or None.
        """
        # Try static dict first
        static = self._static_yt_lookup(yt_symbol)
        if static:
            return static

        # Cross-reference: YT-X -> PT-X -> market -> yt_address
        if yt_symbol.upper().startswith("YT-"):
            pt_equivalent = "PT-" + yt_symbol[3:]
            market = self.resolve_by_pt_symbol(pt_equivalent)
            if market and market.yt_address:
                return (market.yt_address, market.yt_decimals)

        return None

    def resolve_market_address_from_pt_symbol(self, pt_symbol: str) -> str | None:
        """Resolve a PT symbol to its market address.

        Args:
            pt_symbol: PT token symbol.

        Returns:
            Market address or None.
        """
        market = self.resolve_by_pt_symbol(pt_symbol)
        if market:
            return market.market_address

        # Fall back to static
        return self._static_market_lookup(pt_symbol)

    def resolve_mint_sy_token(self, market_address: str) -> str | None:
        """Resolve the token that mints SY for a market.

        For most markets, this is the underlying asset. For yield-bearing
        token markets (like fUSDT0), it must be the yield-bearing token itself.

        Falls back to static MARKET_TOKEN_MINT_SY dict.

        Args:
            market_address: Market contract address.

        Returns:
            Token address that mints SY, or None.
        """
        # Static dict takes priority -- it captures yield-bearing token quirks
        # that the API's underlying_address field may not reflect correctly
        static = self._static_mint_sy_lookup(market_address)
        if static:
            return static

        # Fall back to API underlying_address
        market = self.resolve_by_market_address(market_address)
        if market and market.underlying_address:
            return market.underlying_address

        return None

    def clear_cache(self) -> None:
        """Clear the resolver's market cache, forcing a fresh API fetch."""
        self._market_cache = None
        self._cache_expiry = 0.0
        self._markets_by_pt_address = {}
        self._api_client.clear_cache()

    # =========================================================================
    # Internal Methods
    # =========================================================================

    def _get_markets(self) -> list[PendleMarketData]:
        """Fetch markets with caching."""
        now = time.monotonic()
        if self._market_cache is not None and now < self._cache_expiry:
            return self._market_cache

        try:
            markets = self._api_client.get_market_list()
            self._market_cache = markets
            self._markets_by_pt_address = {m.pt_address.lower(): m for m in markets if m.pt_address}
            self._cache_expiry = now + self._cache_ttl
            logger.debug(f"PendleMarketResolver: fetched {len(markets)} markets for {self._chain}")
            return markets
        except PendleAPIError as e:
            logger.warning(f"PendleMarketResolver: API unavailable ({e}), using cached/static data")
            if self._market_cache is not None:
                return self._market_cache
            return []

    def _matches_underlying(self, market: PendleMarketData, query: str) -> bool:
        """Check if a market's underlying matches a query string."""
        # Match by address (case-insensitive — addresses may be EIP-55 or lowercase)
        if query.startswith("0x") and market.underlying_address and market.underlying_address.lower() == query.lower():
            return True
        # Match by underlying symbol (if available from API)
        if market.underlying_symbol and query in market.underlying_symbol.lower():
            return True
        # Match by PT symbol (contains underlying name, e.g., PT-sUSDe-7MAY2026)
        if market.pt_symbol and query in market.pt_symbol.lower():
            return True
        return False

    def _fallback_resolve_pt(self, pt_symbol: str) -> PendleMarketData | None:
        """Fall back to static dicts for PT resolution."""
        market_addr = self._static_market_lookup(pt_symbol)
        if not market_addr:
            return None

        # Try fetching from API with the known market address
        try:
            market = self._api_client.get_market_data(market_addr)
            logger.info(f"PendleMarketResolver: resolved {pt_symbol} via static dict + API fetch")
            return market
        except PendleAPIError:
            # Build minimal PendleMarketData from static dicts
            pt_info = self._static_pt_lookup(pt_symbol)
            logger.warning(f"PendleMarketResolver: falling back to static data for {pt_symbol} on {self._chain}")
            return PendleMarketData(
                market_address=market_addr,
                chain_id=self._api_client.chain_id,
                pt_address=pt_info[0].lower() if pt_info else "",
            )

    def _static_pt_lookup(self, pt_symbol: str) -> tuple[str, int] | None:
        """Look up PT token in pre-processed PT_TOKEN_INFO dict (O(1))."""
        return self._preprocessed_pt_info.get(pt_symbol.lower())

    def _static_yt_lookup(self, yt_symbol: str) -> tuple[str, int] | None:
        """Look up YT token in pre-processed YT_TOKEN_INFO dict (O(1))."""
        return self._preprocessed_yt_info.get(yt_symbol.lower())

    def _static_market_lookup(self, pt_symbol: str) -> str | None:
        """Look up market address in pre-processed MARKET_BY_PT_TOKEN dict (O(1))."""
        return self._preprocessed_market_by_pt.get(pt_symbol.lower())

    def _static_mint_sy_lookup(self, market_address: str) -> str | None:
        """Look up SY minting token in pre-processed MARKET_TOKEN_MINT_SY dict (O(1))."""
        return self._preprocessed_mint_sy.get(market_address.lower())

    # =========================================================================
    # Static dict pre-processing (called once in __init__)
    # =========================================================================

    def _preprocess_static_pt_info(self) -> dict[str, tuple[str, int]]:
        from almanak.framework.connectors.pendle.sdk import PT_TOKEN_INFO

        chain_info = PT_TOKEN_INFO.get(self._chain, {})
        return {name.lower(): info for name, info in chain_info.items()}

    def _preprocess_static_yt_info(self) -> dict[str, tuple[str, int]]:
        from almanak.framework.connectors.pendle.sdk import YT_TOKEN_INFO

        chain_info = YT_TOKEN_INFO.get(self._chain, {})
        return {name.lower(): info for name, info in chain_info.items()}

    def _preprocess_static_market_by_pt(self) -> dict[str, str]:
        from almanak.framework.connectors.pendle.sdk import MARKET_BY_PT_TOKEN

        chain_markets = MARKET_BY_PT_TOKEN.get(self._chain, {})
        return {name.lower(): addr for name, addr in chain_markets.items()}

    def _preprocess_static_mint_sy(self) -> dict[str, str]:
        from almanak.framework.connectors.pendle.sdk import MARKET_TOKEN_MINT_SY

        chain_mints = MARKET_TOKEN_MINT_SY.get(self._chain, {})
        return {addr.lower(): mint_token for addr, mint_token in chain_mints.items()}
