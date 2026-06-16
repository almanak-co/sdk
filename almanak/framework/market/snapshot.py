"""Canonical MarketSnapshot class — VIB-4062.

The single concrete ``MarketSnapshot`` runtime class. All runtime surfaces
(local SDK, hosted, paper-fork, PnL backtest, HTTP backtest, unit-test)
produce instances of this class via ``MarketSnapshotBuilder``.

PRD §4.1 — this file is a *thin facade* over injected services and a
per-iteration cache. Broad analytics live in ``almanak.framework.market.analytics``
to keep this file from becoming a 4k-line god object.

In commits 2–5 of the VIB-4062 migration the class hosts both the legacy
``set_*`` test injectors (kept for backward compat) AND the new public
``seed_*`` API (PRD §4.6). Commit 6 removes the legacy aliases.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import logging
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from .models import (
    ADXData,
    ATRData,
    BalanceProvider,
    BollingerBandsData,
    CCIData,
    IchimokuData,
    IndicatorProvider,
    MACDData,
    MAData,
    OBVData,
    PriceData,
    PriceOracle,
    RSIData,
    RSIProvider,
    StochasticData,
    TokenBalance,
)

if TYPE_CHECKING:
    import pandas as pd

    from ..data.defi.pools import PoolReserves
    from ..data.funding import FundingRate, FundingRateSpread
    from ..data.health import HealthReport
    from ..data.lp import ILExposure, ProjectedILResult
    from ..data.models import DataEnvelope, Instrument
    from ..data.ohlcv.module import GapStrategy
    from ..data.pools.aggregation import AggregatedPrice
    from ..data.pools.analytics import PoolAnalytics, PoolAnalyticsResult
    from ..data.pools.history import PoolSnapshot
    from ..data.pools.liquidity import LiquidityDepth, SlippageEstimate
    from ..data.pools.reader import PoolPrice
    from ..data.position_health import PTPositionHealth
    from ..data.prediction_provider import (
        PredictionMarket,
        PredictionOrder,
        PredictionPosition,
    )
    from ..data.rates.history import FundingRateSnapshot, LendingRateSnapshot
    from ..data.risk.metrics import PortfolioRisk, RollingSharpeResult
    from ..data.staking.solana_lst_provider import LSTExchangeRate
    from ..data.volatility.realized import VolatilityResult, VolConeResult
    from ..data.wallet_activity import WalletActivityProvider
    from ..data.yields.aggregator import YieldOpportunity

logger = logging.getLogger(__name__)


# Default OHLCV timeframe used by indicator methods when neither an explicit
# timeframe argument nor a strategy-level default_timeframe is provided.
DEFAULT_TIMEFRAME = "4h"


# Strips "Data source '<name>' unavailable: " boilerplate from str(DataSourceUnavailable)
# before hint-matching so the word "unavailable" in the wrapper doesn't trigger a
# false transient hit on a purely permanent failure (e.g. "Unknown token for Binance").
_DSU_BOILERPLATE_RE = re.compile(r"data source '[^']*' unavailable:\s*")

# Matches the DEX quiet-pool staleness miss emitted by the OHLCV router
# (``ohlcv_router._build_stale_response_miss``). This phrase is unique to that
# path: it means the provider *returned* data that is merely old (no recent
# swaps) — a quiet-but-live pool — as opposed to a hard outage (timeout /
# unreachable), which never emits it. Captures the base token + chain so the
# liveness probe can confirm the pool is still priceable from the 24/7 oracle.
_QUIET_POOL_STALE_RE = re.compile(
    # ``[\w-]+`` for the chain so hyphenated names (arbitrum-one, polygon-zkevm,
    # zksync-era) are captured whole for the liveness probe, not truncated.
    r"returned stale OHLCV for (?P<base>[^/\s]+)/\S* on (?P<chain>[\w-]+)",
    re.IGNORECASE,
)

# Hard cap on the implicit OHLCV fetch size used by the volatility helpers
# (``realized_vol`` / ``vol_cone``) when ``ohlcv_limit`` is not supplied
# explicitly. Without this cap, sub-hourly timeframes balloon the request
# size at multi-hundred-thousand candles per call (``vol_cone(timeframe="1m")``
# with the default 90-day window asks for ~388,800 candles), which is
# unsafe per-iteration in both local and hosted multi-tenant runners.
# An explicit ``ohlcv_limit`` overrides this cap (the caller has opted in).
_MAX_VOL_CANDLE_LIMIT = 10_000


def _derive_ohlcv_base_symbol(token: Any, token_str: str) -> str:
    """Pick the base symbol used as the ``base`` column in the OHLCV frame's
    ``attrs``. Accepts an ``Instrument``, a ``BASE/QUOTE`` string, or a bare
    symbol; falls back to the stringified token.
    """
    if hasattr(token, "base"):
        return token.base
    if isinstance(token, str) and "/" in token:
        return token.split("/")[0].strip()
    return str(token)


def _ohlcv_candles_to_rows(candles: list[Any]) -> list[dict[str, Any]]:
    """Materialize a list of ``OHLCVCandle`` into the row-dict shape that
    ``pd.DataFrame`` expects. Volume is coerced to NaN when the upstream
    didn't supply it.
    """
    return [
        {
            "timestamp": c.timestamp,
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
            "volume": float(c.volume) if c.volume is not None else float("nan"),
        }
        for c in candles
    ]


def _price_oracle_supports_chain_arg(price_oracle: PriceOracle) -> bool:
    """Return True when the callable accepts a third ``chain`` argument."""
    try:
        parameters = inspect.signature(price_oracle).parameters.values()
    except (TypeError, ValueError):
        return True

    for parameter in parameters:
        if parameter.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            return True

    positional_params = [
        parameter
        for parameter in parameters
        if parameter.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    return len(positional_params) >= 3


class _PricesAccessor:
    """Hybrid dict-callable accessor for ``MarketSnapshot.prices``.

    Used to keep BOTH ``market.prices.get("WETH")`` (legacy dict-style
    probing in uniswap_rsi-style backtest adapters) and
    ``market.prices(["ETH"])`` (batch-fetch idiom from the canonical
    Quant Data Layer) working on the same attribute.
    """

    __slots__ = ("_snapshot",)

    def __init__(self, snapshot: MarketSnapshot) -> None:
        self._snapshot = snapshot

    # Dict-like interface — delegates to ``_snapshot._prices``.
    def get(self, key: str, default: Any = None) -> Any:
        return self._snapshot._prices.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._snapshot._prices[key]

    def __contains__(self, key: object) -> bool:
        return key in self._snapshot._prices

    def __iter__(self) -> Any:
        return iter(self._snapshot._prices)

    def __len__(self) -> int:
        return len(self._snapshot._prices)

    def keys(self) -> Any:
        return self._snapshot._prices.keys()

    def items(self) -> Any:
        return self._snapshot._prices.items()

    def values(self) -> Any:
        return self._snapshot._prices.values()

    # Callable interface — batch fetch.
    def __call__(
        self,
        tokens: list[str],
        quote: str = "USD",
        *,
        chain: str | None = None,
    ) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for tok in tokens:
            try:
                out[tok] = self._snapshot.price(tok, quote, chain=chain)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"prices(): skipping {tok}: {exc}")
        return out

    def __eq__(self, other: object) -> bool:
        # Equality compares against the underlying dict (so ``snapshot.prices
        # == {"ETH": ...}`` works as expected).
        if isinstance(other, _PricesAccessor):
            return self._snapshot._prices == other._snapshot._prices
        return self._snapshot._prices == other

    def __repr__(self) -> str:
        return f"_PricesAccessor({self._snapshot._prices!r})"


def _balance_provider_supports_chain_arg(balance_provider: Any) -> bool:
    """Return True when the callable accepts a ``chain=`` kwarg.

    Distinct from ``_price_oracle_supports_chain_arg`` — that one looks at
    positional arity (price oracles take ``(token, quote, chain)``); balance
    providers vary, so we look for an explicit ``chain`` parameter or a
    VAR_KEYWORD acceptor. For built-in / MagicMock callables (signature
    introspection raises) we assume the legacy single-arg shape so callers
    don't accidentally invent a kwarg the provider doesn't understand.
    """
    try:
        signature = inspect.signature(balance_provider)
    except (TypeError, ValueError):
        return False

    for parameter in signature.parameters.values():
        if parameter.name == "chain":
            return True
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            return True
    return False


# Provider-name keywords matched against the oracle's qualified name to
# infer ``PriceData.source`` (VIB-3889). Order matters — first match wins.
_PROVIDER_NAME_HINTS: tuple[tuple[str, str], ...] = (
    ("coingecko", "coingecko"),
    ("chainlink", "chainlink"),
    ("binance", "binance"),
    ("thegraph", "thegraph"),
    ("graph", "thegraph"),
    ("uniswap", "uniswap"),
    ("aggregat", "aggregator"),
    ("dex_twap", "dex_twap"),
    ("twap", "dex_twap"),
    ("gateway_oracle", "aggregator"),
    ("gatewaypriceoracle", "aggregator"),
    ("marketservice", "aggregator"),
    ("gateway", "aggregator"),
)


def _infer_oracle_source(price_oracle: PriceOracle) -> str:
    """Best-effort source-name extraction from a price-oracle callable."""
    candidates: list[str] = []

    def _harvest(obj: Any) -> None:
        for attr in ("__qualname__", "__module__", "__name__"):
            value = getattr(obj, attr, None)
            if isinstance(value, str):
                candidates.append(value)
        cls = type(obj)
        for attr in ("__qualname__", "__module__", "__name__"):
            value = getattr(cls, attr, None)
            if isinstance(value, str):
                candidates.append(value)

    _harvest(price_oracle)
    func = getattr(price_oracle, "func", None)
    if func is not None:
        _harvest(func)
    seen: set[int] = {id(price_oracle)}
    cursor: Any = price_oracle
    while True:
        wrapped = getattr(cursor, "__wrapped__", None)
        if wrapped is None or id(wrapped) in seen:
            break
        seen.add(id(wrapped))
        _harvest(wrapped)
        cursor = wrapped

    haystack = " ".join(candidates).lower()
    for needle, source_name in _PROVIDER_NAME_HINTS:
        if needle in haystack:
            return source_name
    return ""


# =============================================================================
# Market Snapshot
# =============================================================================
class MarketSnapshot:
    """Helper class providing market data access for strategy decisions.

    MarketSnapshot provides a simple interface for strategies to access:
    - Token prices
    - RSI values
    - Wallet balances
    - Position information

    The snapshot is populated with data at the start of each iteration,
    allowing strategies to make decisions based on current market conditions.

    Example:
        def decide(self, market: MarketSnapshot) -> Optional[Intent]:
            # Get ETH price
            eth_price = market.price("ETH")

            # Get RSI
            rsi = market.rsi("ETH", period=14)
            if rsi.is_oversold:
                return Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))

            # Check balance
            balance = market.balance("USDC")
            if balance.balance_usd < Decimal("100"):
                return Intent.hold(reason="Insufficient balance")

            return Intent.hold()
    """

    # =========================================================================
    # VIB-4062 — runtime_surface contract (PRD §4.3)
    #
    # Every snapshot records the builder factory that produced it. Builders
    # MUST set this; direct constructor calls (allow-listed only — see
    # tests/contracts/marketsnapshot_constructor_allowlist.txt) default to
    # "unit_test" so legacy unit tests continue to work without a builder.
    # =========================================================================

    runtime_surface: str = "unit_test"

    def __init__(
        self,
        chain: str | None = None,
        wallet_address: str = "",
        price_oracle: PriceOracle | None = None,
        rsi_provider: RSIProvider | None = None,
        balance_provider: BalanceProvider | None = None,
        timestamp: datetime | None = None,
        wallet_activity_provider: WalletActivityProvider | None = None,
        prediction_provider: Any | None = None,
        indicator_provider: IndicatorProvider | None = None,
        multi_dex_service: Any | None = None,
        rate_monitor: Any | None = None,
        funding_rate_provider: Any | None = None,
        gateway_client: Any | None = None,
        default_timeframe: str | None = None,
        runtime_surface: str = "unit_test",
        chains: tuple[str, ...] | list[str] | None = None,
        gas_oracle: Any | None = None,
        aave_health_factor_provider: Any | None = None,
        # ALM-2696 Quant Data Layer providers (PR #2125, ported in VIB-4062 merge).
        # Optional kwargs — strategies that don't use these features pass None.
        pool_reader_registry: Any | None = None,
        pool_reader: Any | None = None,
        price_aggregator: Any | None = None,
        ohlcv_router: Any | None = None,
        ohlcv_module: Any | None = None,
        pool_history_reader: Any | None = None,
        liquidity_depth_reader: Any | None = None,
        slippage_estimator: Any | None = None,
        pool_analytics_reader: Any | None = None,
        yield_aggregator: Any | None = None,
        il_calculator: Any | None = None,
        volatility_calculator: Any | None = None,
        risk_calculator: Any | None = None,
        rate_history_reader: Any | None = None,
        solana_lst_provider: Any | None = None,
        data_router: Any | None = None,
    ) -> None:
        """Initialize market snapshot.

        Args:
            chain: Chain name (e.g., "arbitrum", "ethereum")
            wallet_address: Wallet address for balance queries
            price_oracle: Function to fetch prices (token, quote) -> price
            rsi_provider: Function to calculate RSI (token, period[, timeframe=]) -> RSIData
            balance_provider: Function to fetch balances (token) -> TokenBalance
            timestamp: Snapshot timestamp (defaults to now)
            wallet_activity_provider: Provider for leader wallet activity signals
            prediction_provider: PredictionProvider for prediction market data
            indicator_provider: IndicatorProvider for calculator-backed TA indicators
            multi_dex_service: MultiDexService for cross-DEX price comparison
            rate_monitor: RateMonitor instance for lending rate queries
            funding_rate_provider: GatewayFundingRateProvider for perpetual funding rate queries
            gateway_client: Connected GatewayClient for gateway-routed on-chain reads
                (used by position_health and other methods that need eth_call).
            default_timeframe: Default OHLCV timeframe from strategy config (e.g., "15m", "1h").
                Used as the default for all indicator methods (rsi, macd, sma, etc.)
                when no explicit timeframe is passed. Falls back to DEFAULT_TIMEFRAME if not set.
        """
        # When ``chains=`` is given without ``chain=``, default the primary
        # chain to the first one (matches the legacy MultiChainMarketSnapshot
        # signature that took ``chains=[...]`` only).
        if chain is None:
            if chains:
                chain = next(iter(chains))
            else:
                raise TypeError("MarketSnapshot requires either chain= or chains= to be set")
        self._chain = chain
        self._wallet_address = wallet_address
        self._price_oracle = price_oracle
        self._rsi_provider = rsi_provider
        self._balance_provider = balance_provider
        self._default_timeframe = default_timeframe
        self._timestamp = timestamp or datetime.now(UTC)
        self._wallet_activity_provider = wallet_activity_provider
        self._prediction_provider = prediction_provider
        self._indicator_provider = indicator_provider
        self._multi_dex_service = multi_dex_service
        self._rate_monitor = rate_monitor
        self._funding_rate_provider = funding_rate_provider
        self._gateway_client = gateway_client

        # PRD §4.3 — every snapshot records the builder factory it came from.
        self.runtime_surface = runtime_surface

        # VIB-4062 — gas oracle ported from data-layer for is_trade_worthwhile
        # and estimate_swap_gas_cost_usd.
        self._gas_oracle = gas_oracle
        self._gas_cache: dict[str, tuple[Any, datetime]] = {}
        self._gas_cache_ttl_seconds: int = 12

        # Multi-chain health-factor provider — kept on the canonical class so
        # `MultiChainMarketSnapshot(... aave_health_factor_provider=...)` (which
        # is a TypeAlias to MarketSnapshot post-VIB-4062) doesn't TypeError at
        # construction time. Used by the multi-chain code path in
        # ``IntentStrategy.create_market_snapshot``.
        self._aave_health_factor_provider = aave_health_factor_provider

        # ALM-2696 Quant Data Layer providers (ported from main 2026-05-06).
        self._pool_reader_registry = pool_reader_registry
        self._pool_reader = pool_reader
        self._price_aggregator = price_aggregator
        self._ohlcv_router = ohlcv_router
        self._ohlcv_module = ohlcv_module
        self._pool_history_reader = pool_history_reader
        self._liquidity_depth_reader = liquidity_depth_reader
        self._slippage_estimator = slippage_estimator
        self._pool_analytics_reader = pool_analytics_reader
        self._yield_aggregator = yield_aggregator
        self._il_calculator = il_calculator
        self._volatility_calculator = volatility_calculator
        self._risk_calculator = risk_calculator
        self._rate_history_reader = rate_history_reader
        self._solana_lst_provider = solana_lst_provider
        self._data_router = data_router

        # PRD §4.2 — multi-chain snapshots configure the full set; single-chain
        # snapshots leave it None and the chains property returns (chain,).
        if chains is not None:
            self._chains: tuple[str, ...] | None = tuple(chains)
            if chain not in self._chains:
                # Primary chain MUST be in the configured set.
                self._chains = (chain, *self._chains)
        else:
            self._chains = None

        # Cache for fetched data
        self._price_cache: dict[str, PriceData] = {}
        self._rsi_cache: dict[tuple[str, str, int], RSIData] = {}
        self._balance_cache: dict[str, TokenBalance] = {}
        # VIB-4843 (Empty≠Zero): cache keys whose ``balance_usd`` is the
        # *unmeasured* coerced sentinel (provider returned a bare balance with
        # no USD). Only these may be (re)filled from a price; a provider that
        # MEASURED ``balance_usd`` — including a measured ``Decimal("0")`` — is
        # authoritative and must never be overwritten.
        self._balance_usd_unmeasured: set[str] = set()
        # Critical data failures observed while strategies queried this
        # snapshot. Runner can use this to avoid treating "HOLD forever because
        # market data is broken" as healthy behavior.
        self._critical_data_failures: dict[tuple[str, str], str] = {}

        # Per-indicator caches (tuple keys for timeframe-aware caching)
        self._macd_cache: dict[tuple[str, str, int, int, int], MACDData] = {}
        self._bollinger_cache: dict[tuple[str, str, int, float], BollingerBandsData] = {}
        self._stochastic_cache: dict[tuple[str, str, int, int], StochasticData] = {}
        self._atr_cache: dict[tuple[str, str, int], ATRData] = {}
        self._ma_cache: dict[tuple[str, str, str, int], MAData] = {}

        # Lending rate cache (populated by lending_rate() or set_lending_rate())
        self._lending_rate_cache: dict[str, Any] = {}

        # Position health cache (populated by position_health() or set_position_health())
        # Keyed by (protocol, market_id) — per-iteration memo, not a TTL cache.
        # Position-health cache keyed by (protocol, market_id, rpc_url,
        # collateral_price_usd, debt_price_usd) so override-aware calls don't
        # alias each other. The set_position_health pre-populator uses the
        # short-key shape (protocol, market_id, "", "", "").
        self._position_health_cache: dict[tuple[str, ...], Any] = {}

        # Pre-populated data (can be set directly)
        self._prices: dict[str, Decimal] = {}
        self._balances: dict[str, TokenBalance] = {}
        self._rsi_values: dict[str, tuple[RSIData, str | None]] = {}

        # Pre-populated indicator data (for all TA indicators)
        # Stored as (data, timeframe) tuples; timeframe=None matches any query
        self._macd_values: dict[str, tuple[MACDData, str | None]] = {}
        self._bollinger_values: dict[str, tuple[BollingerBandsData, str | None]] = {}
        self._stochastic_values: dict[str, tuple[StochasticData, str | None]] = {}
        self._atr_values: dict[str, tuple[ATRData, str | None]] = {}
        self._ma_values: dict[str, tuple[MAData, str | None]] = {}
        self._adx_cache: dict[tuple[str, str, int], ADXData] = {}
        self._obv_cache: dict[tuple[str, str, int], OBVData] = {}
        self._cci_cache: dict[tuple[str, str, int], CCIData] = {}
        self._ichimoku_cache: dict[tuple[str, str, int, int, int], IchimokuData] = {}
        self._adx_values: dict[str, tuple[ADXData, str | None]] = {}
        self._obv_values: dict[str, tuple[OBVData, str | None]] = {}
        self._cci_values: dict[str, tuple[CCIData, str | None]] = {}
        self._ichimoku_values: dict[str, tuple[IchimokuData, str | None]] = {}

        # Fork RPC URL for paper trading on-chain reads (VIB-1956)
        self._fork_rpc_url: str | None = None
        self._fork_block: int | None = None

    def _resolve_timeframe(self, timeframe: str | None) -> str:
        """Resolve the effective OHLCV timeframe for indicator methods.

        Priority: explicit argument > strategy-level default > module constant.

        Args:
            timeframe: Caller-supplied timeframe, or None to use defaults.

        Returns:
            A concrete timeframe string (e.g. "15m", "1h", "4h").
        """
        return timeframe or self._default_timeframe or DEFAULT_TIMEFRAME

    @property
    def chain(self) -> str:
        """Get the (primary) chain name.

        For multi-chain snapshots this returns the first chain in
        ``self.chains``. Strategy code that cares about the active chain in
        a multi-chain context must pass ``chain=`` explicitly to each method
        (PRD §4.2).
        """
        return self._chain

    @property
    def chains(self) -> tuple[str, ...]:
        """Configured chain set (PRD §4.2).

        Single-chain snapshots return ``(self.chain,)``. Multi-chain snapshots
        — folded in from MultiChainMarketSnapshot in commit 3 — return all
        configured chains. Multi-chain helpers consult ``len(self.chains)``
        to decide whether ``chain=None`` defaults to the primary or raises
        ``AmbiguousChainError``.
        """
        configured = getattr(self, "_chains", None)
        if configured:
            return tuple(configured)
        return (self._chain,)

    def _resolve_chain(self, chain: str | None) -> str:
        """Apply PRD §4.2 chain-resolution rules.

        * Single-chain, ``chain=None``                     → returns the only chain.
        * Single-chain, ``chain=`` matches                  → returns it.
        * Single-chain, ``chain=`` mismatches               → ``ChainNotConfiguredError``.
        * Multi-chain,  ``chain=None``                      → ``AmbiguousChainError``.
        * Multi-chain,  ``chain=`` in self.chains           → returns it.
        * Multi-chain,  ``chain=`` not in self.chains       → ``ChainNotConfiguredError``.
        """
        from .errors import AmbiguousChainError, ChainNotConfiguredError

        configured = self.chains
        if chain is None:
            if len(configured) == 1:
                return configured[0]
            raise AmbiguousChainError(
                reason="chain=None on a multi-chain snapshot",
                chains=configured,
            )
        if chain in configured:
            return chain
        raise ChainNotConfiguredError(
            reason=f"chain={chain!r} not in configured chains",
            chain=chain,
            chains=configured,
        )

    @property
    def wallet_address(self) -> str:
        """Get the wallet address."""
        return self._wallet_address

    @property
    def timestamp(self) -> datetime:
        """Get the snapshot timestamp."""
        return self._timestamp

    @property
    def fork_rpc_url(self) -> str | None:
        """Get the Anvil fork RPC URL for on-chain reads (paper trading only).

        Returns the fork's JSON-RPC endpoint when running in paper trading mode,
        allowing strategies to perform protocol-level reads directly against the fork.
        Returns None when not in paper trading mode.

        VIB-1956: Enables strategies to do protocol-level reads (e.g., Aave
        getReserveData, DEX pool state) during paper trading.

        WARNING: This is a paper-trading-only escape hatch. In production,
        this returns None. Do NOT gate trading logic on fork_rpc_url
        availability — strategies that behave differently based on this
        property will diverge between paper trading and production.
        """
        return self._fork_rpc_url

    @property
    def fork_block(self) -> int | None:
        """Get the current fork block number (paper trading only)."""
        return self._fork_block

    def price(self, token: str, quote: str = "USD", *, chain: str | None = None) -> Decimal:
        """Get the price of a token.

        Args:
            token: Token symbol (e.g., "ETH", "WBTC")
            quote: Quote currency (default "USD")
            chain: Optional chain override. ``chain`` is keyword-only (PRD §4.2 R1).
                Strict resolution applies for ``chain=None`` (single-chain
                returns the only chain; multi-chain raises ``AmbiguousChainError``).
                When ``chain=`` is explicitly supplied AND the configured price
                oracle accepts a ``chain`` argument, the value is passed through
                to the oracle as-is (the oracle is the authoritative chain-routing
                surface in that case).

        Returns:
            Token price in quote currency

        Raises:
            AmbiguousChainError: If chain is None on a multi-chain snapshot.
            ChainNotConfiguredError: If chain is not in this snapshot's
                configured chains AND the oracle cannot route by chain.
            ValueError: If price cannot be determined.
        """
        # Chain resolution: oracle-aware. When the oracle supports chain=,
        # let the caller's explicit chain pass through even if it's not in
        # ``self.chains`` — the oracle will handle (or reject) it.
        oracle_supports_chain = self._price_oracle is not None and _price_oracle_supports_chain_arg(self._price_oracle)
        if chain is None or (chain in self.chains):
            requested_chain = self._resolve_chain(chain)
        elif oracle_supports_chain:
            requested_chain = chain
        else:
            requested_chain = self._resolve_chain(chain)
        cache_key = f"{token}/{quote}@{requested_chain}"

        # Check pre-populated prices first. Case-insensitive, matching the
        # balance path (``_cached_price_for``): ``_prices`` is seeded by
        # ``set_price()`` verbatim, so a strategy querying
        # ``market.price("wstETH")`` must find a ``"WSTETH"``-seeded price
        # instead of falling through to an oracle that cannot resolve a
        # non-native token (the silent false-clean lending backtest, where
        # the engine seeds upper-cased symbols but the strategy queries its
        # config casing).
        if chain is None or requested_chain == self._chain:
            seeded = self._seeded_price_for_symbol(token)
            if seeded is not None:
                return seeded

        # Check cache
        if cache_key in self._price_cache:
            return self._price_cache[cache_key].price

        # Use oracle if available. Two protocols:
        #   1. Modern callable oracle: ``oracle(token, quote, chain)`` → Decimal.
        #   2. Legacy aggregator object: ``oracle.get_aggregated_price(token,
        #      quote, chain=chain)`` → awaitable ``PriceResult``.
        # Use aggregator path only when method exists AND oracle is NOT
        # itself callable (a MagicMock with side_effect IS callable).
        if self._price_oracle:
            try:
                use_aggregator = not callable(self._price_oracle) and hasattr(
                    self._price_oracle,
                    "get_aggregated_price",
                )
                if use_aggregator:
                    # ``self._price_oracle`` is typed Callable; mypy can't see
                    # through the runtime hasattr check.
                    method = self._price_oracle.get_aggregated_price  # type: ignore[attr-defined]
                    accepts_chain = False
                    try:
                        sig = inspect.signature(method)
                        accepts_chain = "chain" in sig.parameters or any(
                            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
                        )
                    except (TypeError, ValueError):
                        accepts_chain = False
                    coro = method(token, quote, chain=requested_chain) if accepts_chain else method(token, quote)
                    if asyncio.iscoroutine(coro):
                        result = self._run_async_bridged(coro)
                    else:
                        result = coro
                    price_value = getattr(result, "price", result)
                else:
                    price_value = (
                        self._price_oracle(token, quote, requested_chain)
                        if _price_oracle_supports_chain_arg(self._price_oracle)
                        else self._price_oracle(token, quote)
                    )
                # VIB-3889: stamp the inferred source on the cached entry
                # so ``get_price_oracle_dict(with_sources=True)`` carries
                # the actual provider name into ``price_inputs_json``.
                self._price_cache[cache_key] = PriceData(
                    price=price_value,
                    source=_infer_oracle_source(self._price_oracle),
                )
                self._critical_data_failures.pop(("price", cache_key), None)
                return price_value
            except Exception as e:
                self._record_critical_data_failure("price", cache_key, e)
                logger.warning(f"Price oracle failed for {cache_key}: {e}")

        self._record_critical_data_failure(
            "price",
            cache_key,
            f"Cannot determine price for {token}/{quote} on {requested_chain}",
        )
        raise ValueError(f"Cannot determine price for {token}/{quote} on {requested_chain}")

    @property
    def prices(self) -> _PricesAccessor:
        """Hybrid dict-callable accessor for prices.

        Two usage shapes (both supported, see ALM-2696 deferred-batch NOTE):

            ``market.prices.get("WETH")`` — legacy dict-style access on the
            internal ``_prices`` map (used by uniswap_rsi-style backtest
            adapters that probe ``hasattr(market, "prices")``).

            ``market.prices(["WETH", "USDC"], chain="arbitrum")`` — batch
            fetch via ``price()``, returning ``{symbol: Decimal}``. Per-symbol
            failures are logged and skipped.

        Returning a hybrid accessor preserves both contracts; making
        ``prices`` a plain method broke ``hasattr+get`` callers
        (``'function' object has no attribute 'get'``), and making it a
        plain dict broke the batch-fetch tests.
        """
        return _PricesAccessor(self)

    def price_data(self, token: str, quote: str = "USD", *, chain: str | None = None) -> PriceData:
        """Get full price data for a token.

        Args:
            token: Token symbol
            quote: Quote currency (default "USD")
            chain: Optional chain override (keyword-only, PRD §4.2 R1).

        Returns:
            PriceData with current price and historical data

        Raises:
            ChainNotConfiguredError / AmbiguousChainError: same rules as :meth:`price`.
        """
        requested_chain = self._resolve_chain(chain)
        cache_key = f"{token}/{quote}@{requested_chain}"

        if cache_key in self._price_cache:
            return self._price_cache[cache_key]

        # Get basic price and create PriceData
        current_price = self.price(token, quote, chain=requested_chain)
        return self._price_cache.get(cache_key, PriceData(price=current_price))

    def rsi(self, token: str, period: int = 14, timeframe: str | None = None) -> RSIData:
        """Get RSI (Relative Strength Index) for a token.

        Args:
            token: Token symbol
            period: RSI calculation period (default 14)
            timeframe: OHLCV candle timeframe. Defaults to strategy's data_granularity
                config, or "4h" if not configured.

        Returns:
            RSIData with current RSI value and signal

        Raises:
            ValueError: If RSI cannot be calculated
        """
        timeframe = self._resolve_timeframe(timeframe)
        cache_key = (token, timeframe, period)

        # Check pre-populated RSI first (validate period and timeframe match)
        if token in self._rsi_values:
            pre, stored_tf = self._rsi_values[token]
            if pre.period == period and (stored_tf is None or stored_tf == timeframe):
                return pre
            logger.debug(
                "Pre-populated RSI for %s (period=%d, tf=%s) doesn't match requested (period=%d, tf=%s), skipping",
                token,
                pre.period,
                stored_tf,
                period,
                timeframe,
            )

        # Check cache
        if cache_key in self._rsi_cache:
            return self._rsi_cache[cache_key]

        # Use provider if available
        if self._rsi_provider:
            try:
                rsi_data = self._rsi_provider(token, period, timeframe=timeframe)
                self._rsi_cache[cache_key] = rsi_data
                self._critical_data_failures.pop(("rsi", str(cache_key)), None)
                return rsi_data
            except TypeError:
                # Backward compat: older RSI providers only accept (token, period)
                try:
                    rsi_data = self._rsi_provider(token, period)
                    self._rsi_cache[cache_key] = rsi_data
                    self._critical_data_failures.pop(("rsi", str(cache_key)), None)
                    return rsi_data
                except Exception as e:
                    self._record_critical_data_failure("rsi", str(cache_key), e)
                    logger.warning(f"RSI provider failed for {cache_key}: {e}")
            except Exception as e:
                self._record_critical_data_failure("rsi", str(cache_key), e)
                logger.warning(f"RSI provider failed for {cache_key}: {e}")

        self._record_critical_data_failure(
            "rsi", str(cache_key), f"Cannot calculate RSI for {token} with period {period}"
        )
        raise ValueError(f"Cannot calculate RSI for {token} with period {period}")

    def price_across_dexs(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
        dexs: list[str] | None = None,
    ) -> Any:
        """Get prices from multiple DEXs for comparison.

        Fetches quotes from all configured DEXs and returns a comparison
        of prices and execution details.

        Args:
            token_in: Input token symbol (e.g., "USDC", "WETH")
            token_out: Output token symbol (e.g., "WETH", "USDC")
            amount: Input amount (human-readable)
            dexs: DEXs to query (default: all available on chain)

        Returns:
            MultiDexPriceResult with quotes from each DEX

        Raises:
            NotImplementedError: If multi-DEX service is not configured
        """
        if self._multi_dex_service is None:
            raise NotImplementedError(
                "Multi-DEX price comparison is not available. "
                "The MultiDexService must be configured by the strategy runner."
            )
        import asyncio

        service = self._multi_dex_service

        async def _run() -> Any:
            return await service.get_prices_across_dexs(token_in, token_out, amount, dexs)

        # If there is already a running event loop (e.g., inside asyncio.run()),
        # run_until_complete() would crash. Use a thread pool to bridge safely.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, _run()).result()
        else:
            return asyncio.run(_run())

    def best_dex_price(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
        dexs: list[str] | None = None,
    ) -> Any:
        """Get the best DEX for a trade.

        Compares prices from all configured DEXs and returns the one with
        the highest output amount (best execution).

        Args:
            token_in: Input token symbol (e.g., "USDC", "WETH")
            token_out: Output token symbol (e.g., "WETH", "USDC")
            amount: Input amount (human-readable)
            dexs: DEXs to compare (default: all available on chain)

        Returns:
            BestDexResult with the best DEX and quote

        Raises:
            NotImplementedError: If multi-DEX service is not configured
        """
        if self._multi_dex_service is None:
            raise NotImplementedError(
                "Multi-DEX price comparison is not available. "
                "The MultiDexService must be configured by the strategy runner."
            )
        import asyncio

        service = self._multi_dex_service

        async def _run() -> Any:
            return await service.get_best_dex_price(token_in, token_out, amount, dexs)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, _run()).result()
        else:
            return asyncio.run(_run())

    def macd(
        self,
        token: str,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
        timeframe: str | None = None,
    ) -> MACDData:
        """Get MACD (Moving Average Convergence Divergence) for a token.

        Args:
            token: Token symbol
            fast_period: Fast EMA period (default 12)
            slow_period: Slow EMA period (default 26)
            signal_period: Signal EMA period (default 9)
            timeframe: OHLCV candle timeframe. Defaults to strategy's data_granularity
                config, or "4h" if not configured.

        Returns:
            MACDData with MACD line, signal line, and histogram

        Raises:
            ValueError: If MACD data is not available

        Example:
            macd = market.macd("WETH")
            if macd.is_bullish_crossover:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        timeframe = self._resolve_timeframe(timeframe)
        cache_key = (token, timeframe, fast_period, slow_period, signal_period)

        # Check pre-populated values first (validate params and timeframe)
        if token in self._macd_values:
            pre, stored_tf = self._macd_values[token]
            if (
                pre.fast_period == fast_period
                and pre.slow_period == slow_period
                and pre.signal_period == signal_period
                and (stored_tf is None or stored_tf == timeframe)
            ):
                return pre
            logger.debug(
                "Pre-populated MACD for %s (periods=(%d,%d,%d), tf=%s) doesn't match requested, skipping",
                token,
                pre.fast_period,
                pre.slow_period,
                pre.signal_period,
                stored_tf,
            )

        # Check cache
        if cache_key in self._macd_cache:
            return self._macd_cache[cache_key]

        # Use provider if available
        if self._indicator_provider and self._indicator_provider.macd:
            try:
                macd_data = self._indicator_provider.macd(
                    token,
                    fast_period,
                    slow_period,
                    signal_period,
                    timeframe=timeframe,
                )
                self._macd_cache[cache_key] = macd_data
                self._critical_data_failures.pop(("macd", str(cache_key)), None)
                return macd_data
            except Exception as e:  # noqa: BLE001
                self._record_critical_data_failure("macd", str(cache_key), e)
                logger.warning(f"MACD provider failed for {cache_key}: {e}")

        self._record_critical_data_failure("macd", str(cache_key), f"MACD data not available for {token}")
        raise ValueError(f"MACD data not available for {token}")

    def bollinger_bands(
        self, token: str, period: int = 20, std_dev: float = 2.0, timeframe: str | None = None
    ) -> BollingerBandsData:
        """Get Bollinger Bands for a token.

        Args:
            token: Token symbol
            period: SMA period (default 20)
            std_dev: Standard deviation multiplier (default 2.0)
            timeframe: OHLCV candle timeframe. Defaults to strategy's data_granularity
                config, or "4h" if not configured.

        Returns:
            BollingerBandsData with upper, middle, lower bands and position metrics

        Raises:
            ValueError: If Bollinger Bands data is not available

        Example:
            bb = market.bollinger_bands("WETH")
            if bb.is_oversold:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        timeframe = self._resolve_timeframe(timeframe)
        cache_key = (token, timeframe, period, std_dev)

        # Check pre-populated values first (validate params and timeframe)
        if token in self._bollinger_values:
            pre, stored_tf = self._bollinger_values[token]
            if pre.period == period and pre.std_dev == std_dev and (stored_tf is None or stored_tf == timeframe):
                return pre
            logger.debug(
                "Pre-populated Bollinger for %s (period=%d, std_dev=%.1f, tf=%s) doesn't match requested, skipping",
                token,
                pre.period,
                pre.std_dev,
                stored_tf,
            )

        # Check cache
        if cache_key in self._bollinger_cache:
            return self._bollinger_cache[cache_key]

        # Use provider if available
        if self._indicator_provider and self._indicator_provider.bollinger:
            try:
                bb_data = self._indicator_provider.bollinger(
                    token,
                    period,
                    std_dev,
                    timeframe=timeframe,
                )
                self._bollinger_cache[cache_key] = bb_data
                self._critical_data_failures.pop(("bollinger", str(cache_key)), None)
                return bb_data
            except Exception as e:  # noqa: BLE001
                self._record_critical_data_failure("bollinger", str(cache_key), e)
                logger.warning(f"Bollinger provider failed for {cache_key}: {e}")

        self._record_critical_data_failure(
            "bollinger", str(cache_key), f"Bollinger Bands data not available for {token}"
        )
        raise ValueError(f"Bollinger Bands data not available for {token}")

    def stochastic(
        self, token: str, k_period: int = 14, d_period: int = 3, timeframe: str | None = None
    ) -> StochasticData:
        """Get Stochastic Oscillator for a token.

        Args:
            token: Token symbol
            k_period: %K period (default 14)
            d_period: %D period (default 3)
            timeframe: OHLCV candle timeframe. Defaults to strategy's data_granularity
                config, or "4h" if not configured.

        Returns:
            StochasticData with %K and %D values

        Raises:
            ValueError: If Stochastic data is not available

        Example:
            stoch = market.stochastic("WETH")
            if stoch.is_oversold and stoch.k_value > stoch.d_value:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        timeframe = self._resolve_timeframe(timeframe)
        cache_key = (token, timeframe, k_period, d_period)

        # Check pre-populated values first (validate params and timeframe)
        if token in self._stochastic_values:
            pre, stored_tf = self._stochastic_values[token]
            if pre.k_period == k_period and pre.d_period == d_period and (stored_tf is None or stored_tf == timeframe):
                return pre
            logger.debug(
                "Pre-populated Stochastic for %s (periods=(%d,%d), tf=%s) doesn't match requested, skipping",
                token,
                pre.k_period,
                pre.d_period,
                stored_tf,
            )

        # Check cache
        if cache_key in self._stochastic_cache:
            return self._stochastic_cache[cache_key]

        # Use provider if available
        if self._indicator_provider and self._indicator_provider.stochastic:
            try:
                stoch_data = self._indicator_provider.stochastic(
                    token,
                    k_period,
                    d_period,
                    timeframe=timeframe,
                )
                self._stochastic_cache[cache_key] = stoch_data
                self._critical_data_failures.pop(("stochastic", str(cache_key)), None)
                return stoch_data
            except Exception as e:  # noqa: BLE001
                self._record_critical_data_failure("stochastic", str(cache_key), e)
                logger.warning(f"Stochastic provider failed for {cache_key}: {e}")

        self._record_critical_data_failure("stochastic", str(cache_key), f"Stochastic data not available for {token}")
        raise ValueError(f"Stochastic data not available for {token}")

    def atr(self, token: str, period: int = 14, timeframe: str | None = None) -> ATRData:
        """Get ATR (Average True Range) for a token.

        Args:
            token: Token symbol
            period: ATR period (default 14)
            timeframe: OHLCV candle timeframe. Defaults to strategy's data_granularity
                config, or "4h" if not configured.

        Returns:
            ATRData with ATR value and volatility assessment

        Raises:
            ValueError: If ATR data is not available

        Example:
            atr = market.atr("WETH")
            if atr.is_low_volatility:
                # Safe to trade
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        timeframe = self._resolve_timeframe(timeframe)
        cache_key = (token, timeframe, period)

        # Check pre-populated values first (validate period and timeframe)
        if token in self._atr_values:
            pre, stored_tf = self._atr_values[token]
            if pre.period == period and (stored_tf is None or stored_tf == timeframe):
                return pre
            logger.debug(
                "Pre-populated ATR for %s (period=%d, tf=%s) doesn't match requested (period=%d, tf=%s), skipping",
                token,
                pre.period,
                stored_tf,
                period,
                timeframe,
            )

        # Check cache
        if cache_key in self._atr_cache:
            return self._atr_cache[cache_key]

        # Use provider if available
        if self._indicator_provider and self._indicator_provider.atr:
            try:
                atr_data = self._indicator_provider.atr(
                    token,
                    period,
                    timeframe=timeframe,
                )
                self._atr_cache[cache_key] = atr_data
                self._critical_data_failures.pop(("atr", str(cache_key)), None)
                return atr_data
            except Exception as e:  # noqa: BLE001
                self._record_critical_data_failure("atr", str(cache_key), e)
                logger.warning(f"ATR provider failed for {cache_key}: {e}")

        self._record_critical_data_failure("atr", str(cache_key), f"ATR data not available for {token}")
        raise ValueError(f"ATR data not available for {token}")

    def sma(self, token: str, period: int = 20, timeframe: str | None = None) -> MAData:
        """Get Simple Moving Average for a token.

        Args:
            token: Token symbol
            period: SMA period (default 20)
            timeframe: OHLCV candle timeframe. Defaults to strategy's data_granularity
                config, or "4h" if not configured.

        Returns:
            MAData with SMA value

        Raises:
            ValueError: If SMA data is not available

        Example:
            sma = market.sma("WETH", period=50)
            if sma.is_price_above:
                print("Bullish - price above 50 SMA")
        """
        timeframe = self._resolve_timeframe(timeframe)
        cache_key = (token, timeframe, "SMA", period)

        # Check pre-populated values first (validate params and timeframe)
        for ma_key in (f"{token}:SMA:{period}", token):
            if ma_key in self._ma_values:
                pre, stored_tf = self._ma_values[ma_key]
                if pre.ma_type == "SMA" and pre.period == period and (stored_tf is None or stored_tf == timeframe):
                    return pre

        # Check cache
        if cache_key in self._ma_cache:
            return self._ma_cache[cache_key]

        # Use provider if available
        if self._indicator_provider and self._indicator_provider.sma:
            try:
                sma_data = self._indicator_provider.sma(
                    token,
                    period,
                    timeframe=timeframe,
                )
                self._ma_cache[cache_key] = sma_data
                self._critical_data_failures.pop(("sma", str(cache_key)), None)
                return sma_data
            except Exception as e:  # noqa: BLE001
                self._record_critical_data_failure("sma", str(cache_key), e)
                logger.warning(f"SMA provider failed for {cache_key}: {e}")

        self._record_critical_data_failure(
            "sma", str(cache_key), f"SMA data not available for {token} with period {period}"
        )
        raise ValueError(f"SMA data not available for {token} with period {period}")

    def ema(self, token: str, period: int = 12, timeframe: str | None = None) -> MAData:
        """Get Exponential Moving Average for a token.

        Args:
            token: Token symbol
            period: EMA period (default 12)
            timeframe: OHLCV candle timeframe. Defaults to strategy's data_granularity
                config, or "4h" if not configured.

        Returns:
            MAData with EMA value

        Raises:
            ValueError: If EMA data is not available

        Example:
            ema_12 = market.ema("WETH", period=12)
            ema_26 = market.ema("WETH", period=26)
            if ema_12.value > ema_26.value:
                print("Golden cross - bullish")
        """
        timeframe = self._resolve_timeframe(timeframe)
        cache_key = (token, timeframe, "EMA", period)

        # Check pre-populated values first (validate params and timeframe)
        str_cache_key = f"{token}:EMA:{period}"
        if str_cache_key in self._ma_values:
            pre, stored_tf = self._ma_values[str_cache_key]
            if pre.ma_type == "EMA" and pre.period == period and (stored_tf is None or stored_tf == timeframe):
                return pre

        # Check cache
        if cache_key in self._ma_cache:
            return self._ma_cache[cache_key]

        # Use provider if available
        if self._indicator_provider and self._indicator_provider.ema:
            try:
                ema_data = self._indicator_provider.ema(
                    token,
                    period,
                    timeframe=timeframe,
                )
                self._ma_cache[cache_key] = ema_data
                self._critical_data_failures.pop(("ema", str(cache_key)), None)
                return ema_data
            except Exception as e:  # noqa: BLE001
                self._record_critical_data_failure("ema", str(cache_key), e)
                logger.warning(f"EMA provider failed for {cache_key}: {e}")

        self._record_critical_data_failure(
            "ema", str(cache_key), f"EMA data not available for {token} with period {period}"
        )
        raise ValueError(f"EMA data not available for {token} with period {period}")

    def adx(self, token: str, period: int = 14, timeframe: str | None = None) -> ADXData:
        """Get ADX (Average Directional Index) for a token.

        Args:
            token: Token symbol
            period: ADX period (default 14)
            timeframe: OHLCV candle timeframe. Defaults to strategy's data_granularity
                config, or "4h" if not configured.

        Returns:
            ADXData with ADX, +DI, and -DI values

        Raises:
            ValueError: If ADX data is not available

        Example:
            adx = market.adx("WETH")
            if adx.is_uptrend:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        timeframe = self._resolve_timeframe(timeframe)
        cache_key = (token, timeframe, period)

        if token in self._adx_values:
            pre, stored_tf = self._adx_values[token]
            if pre.period == period and (stored_tf is None or stored_tf == timeframe):
                return pre

        if cache_key in self._adx_cache:
            return self._adx_cache[cache_key]

        if self._indicator_provider and self._indicator_provider.adx:
            try:
                adx_data = self._indicator_provider.adx(
                    token,
                    period=period,
                    timeframe=timeframe,
                )
                self._adx_cache[cache_key] = adx_data
                self._critical_data_failures.pop(("adx", str(cache_key)), None)
                return adx_data
            except Exception as e:  # noqa: BLE001
                self._record_critical_data_failure("adx", str(cache_key), e)
                logger.warning(f"ADX provider failed for {cache_key}: {e}")

        self._record_critical_data_failure("adx", str(cache_key), f"ADX data not available for {token}")
        raise ValueError(f"ADX data not available for {token}")

    def obv(self, token: str, signal_period: int = 21, timeframe: str | None = None) -> OBVData:
        """Get OBV (On-Balance Volume) for a token.

        Args:
            token: Token symbol
            signal_period: OBV signal line period (default 21)
            timeframe: OHLCV candle timeframe. Defaults to strategy's data_granularity
                config, or "4h" if not configured.

        Returns:
            OBVData with OBV and signal line values

        Raises:
            ValueError: If OBV data is not available

        Example:
            obv = market.obv("WETH")
            if obv.is_bullish:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        timeframe = self._resolve_timeframe(timeframe)
        cache_key = (token, timeframe, signal_period)

        if token in self._obv_values:
            pre, stored_tf = self._obv_values[token]
            if pre.signal_period == signal_period and (stored_tf is None or stored_tf == timeframe):
                return pre

        if cache_key in self._obv_cache:
            return self._obv_cache[cache_key]

        if self._indicator_provider and self._indicator_provider.obv:
            try:
                obv_data = self._indicator_provider.obv(
                    token,
                    signal_period=signal_period,
                    timeframe=timeframe,
                )
                self._obv_cache[cache_key] = obv_data
                self._critical_data_failures.pop(("obv", str(cache_key)), None)
                return obv_data
            except Exception as e:  # noqa: BLE001
                self._record_critical_data_failure("obv", str(cache_key), e)
                logger.warning(f"OBV provider failed for {cache_key}: {e}")

        self._record_critical_data_failure("obv", str(cache_key), f"OBV data not available for {token}")
        raise ValueError(f"OBV data not available for {token}")

    def cci(self, token: str, period: int = 20, timeframe: str | None = None) -> CCIData:
        """Get CCI (Commodity Channel Index) for a token.

        Args:
            token: Token symbol
            period: CCI period (default 20)
            timeframe: OHLCV candle timeframe. Defaults to strategy's data_granularity
                config, or "4h" if not configured.

        Returns:
            CCIData with CCI value and overbought/oversold status

        Raises:
            ValueError: If CCI data is not available

        Example:
            cci = market.cci("WETH")
            if cci.is_oversold:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        timeframe = self._resolve_timeframe(timeframe)
        cache_key = (token, timeframe, period)

        if token in self._cci_values:
            pre, stored_tf = self._cci_values[token]
            if pre.period == period and (stored_tf is None or stored_tf == timeframe):
                return pre

        if cache_key in self._cci_cache:
            return self._cci_cache[cache_key]

        if self._indicator_provider and self._indicator_provider.cci:
            try:
                cci_data = self._indicator_provider.cci(
                    token,
                    period=period,
                    timeframe=timeframe,
                )
                self._cci_cache[cache_key] = cci_data
                self._critical_data_failures.pop(("cci", str(cache_key)), None)
                return cci_data
            except Exception as e:  # noqa: BLE001
                self._record_critical_data_failure("cci", str(cache_key), e)
                logger.warning(f"CCI provider failed for {cache_key}: {e}")

        self._record_critical_data_failure("cci", str(cache_key), f"CCI data not available for {token}")
        raise ValueError(f"CCI data not available for {token}")

    def ichimoku(
        self,
        token: str,
        tenkan_period: int = 9,
        kijun_period: int = 26,
        senkou_b_period: int = 52,
        timeframe: str | None = None,
    ) -> IchimokuData:
        """Get Ichimoku Cloud data for a token.

        Args:
            token: Token symbol
            tenkan_period: Conversion line period (default 9)
            kijun_period: Base line period (default 26)
            senkou_b_period: Leading span B period (default 52)
            timeframe: OHLCV candle timeframe. Defaults to strategy's data_granularity
                config, or "4h" if not configured.

        Returns:
            IchimokuData with all Ichimoku components

        Raises:
            ValueError: If Ichimoku data is not available

        Example:
            ich = market.ichimoku("WETH")
            if ich.is_bullish_crossover and ich.is_above_cloud:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        timeframe = self._resolve_timeframe(timeframe)
        cache_key = (token, timeframe, tenkan_period, kijun_period, senkou_b_period)

        if token in self._ichimoku_values:
            pre, stored_tf = self._ichimoku_values[token]
            if (
                pre.tenkan_period == tenkan_period
                and pre.kijun_period == kijun_period
                and pre.senkou_b_period == senkou_b_period
                and (stored_tf is None or stored_tf == timeframe)
            ):
                return pre

        if cache_key in self._ichimoku_cache:
            return self._ichimoku_cache[cache_key]

        if self._indicator_provider and self._indicator_provider.ichimoku:
            try:
                ichimoku_data = self._indicator_provider.ichimoku(
                    token,
                    tenkan_period=tenkan_period,
                    kijun_period=kijun_period,
                    senkou_b_period=senkou_b_period,
                    timeframe=timeframe,
                )
                self._ichimoku_cache[cache_key] = ichimoku_data
                self._critical_data_failures.pop(("ichimoku", str(cache_key)), None)
                return ichimoku_data
            except Exception as e:  # noqa: BLE001
                self._record_critical_data_failure("ichimoku", str(cache_key), e)
                logger.warning(f"Ichimoku provider failed for {cache_key}: {e}")

        self._record_critical_data_failure("ichimoku", str(cache_key), f"Ichimoku data not available for {token}")
        raise ValueError(f"Ichimoku data not available for {token}")

    def invalidate_balance(self, token: str, protocol: str | None = None) -> None:
        """Evict any memoized wallet balance for ``token`` so the next
        :meth:`balance` call re-queries the provider.

        Sequential execution lanes mutate wallet balances mid-snapshot: a
        teardown staircase's REPAY consumes the debt token before a later
        ``amount="all"`` sweep resolves against this same snapshot, and the
        stale memo then over-resolves by exactly the repaid amount (compile
        fails on insufficient balance). The gateway-level cache is already
        invalidated by the commit pipeline after each intent — this clears the
        snapshot-level memo so the fresh value is actually read.

        No-op on provider-less snapshots (paper / dry-run inject simulated
        balances directly into the memo maps): with no provider there is no
        fresher source to re-query, so evicting would turn every subsequent
        read into a ValueError instead of serving the (correct, simulated)
        memo.
        """
        if self._balance_provider is None:
            return
        resolved = self._resolve_protocol_variant(token, protocol)
        stale_keys = [key for key in self._balance_cache if key == resolved or key.startswith(f"{resolved}@")]
        for key in stale_keys:
            self._balance_cache.pop(key, None)
            self._balance_usd_unmeasured.discard(key)
        self._balances.pop(resolved, None)

    def balance(
        self,
        token: str,
        protocol: str | None = None,
        *,
        chain: str | None = None,
        price: Decimal | None = None,
    ) -> TokenBalance:
        """Get wallet balance for a token.

        Args:
            token: Token symbol
            protocol: Optional protocol name for variant disambiguation (VIB-3138).
                When set, resolves generic symbols to the protocol's preferred
                variant via the connector's ``settlement_token_variants``
                capability (read through ``CapabilitiesRegistry``; e.g.,
                ``balance("USDC", protocol="polymarket")`` on Polygon returns
                the pUSD balance — the V2 spendable trading collateral
                wrapped from USDC.e or native USDC at the on-chain Onramp.
                See VIB-3770). When unset, returns the balance for the
                symbol as given.
            chain: Optional chain override (keyword-only, PRD §4.2 R1). Required
                on multi-chain snapshots; on single-chain it must match
                ``self.chain`` or be ``None``.
            price: Optional already-known USD price (keyword-only, VIB-4843
                FR-5003). When supplied, ``balance_usd`` is computed as
                ``balance * price`` WITHOUT a re-fetch. When omitted, the warm
                ``_price_cache`` is consulted (still no oracle call); only when
                neither is available is ``balance_usd`` left as the coerced
                ``Decimal("0")`` for callers to fill via ``price()``. This
                removes the redundant price re-fetch the portfolio valuation
                lane previously incurred per token.

        Returns:
            TokenBalance with current balance (and ``balance_usd`` filled from
            ``price`` / ``_price_cache`` when derivable).

        Raises:
            ChainNotConfiguredError / AmbiguousChainError: same rules as :meth:`price`.
            ValueError: If balance cannot be determined.
        """
        requested_chain = self._resolve_chain(chain)
        # VIB-3138: translate generic symbol to protocol-preferred variant.
        resolved = self._resolve_protocol_variant(token, protocol)
        cache_key = f"{resolved}@{requested_chain}"

        # Check the per-chain cache FIRST when an explicit chain was given —
        # otherwise the chain-agnostic ``_balances`` map can shadow a different
        # chain's pre-populated balance and silently return the wrong number.
        if cache_key in self._balance_cache:
            filled = self._fill_balance_usd(
                self._balance_cache[cache_key], resolved, requested_chain, price, cache_key=cache_key
            )
            # Persist the filled USD back into the cache. _fill_balance_usd
            # discards cache_key from _balance_usd_unmeasured once it measures
            # USD, so without this write-back a later cache hit would treat USD
            # as measured yet return the original, still-unfilled balance.
            self._balance_cache[cache_key] = filled
            return filled

        # Check pre-populated balances (chain-agnostic for back-compat).
        if resolved in self._balances:
            # Pre-populated balances are MEASURED by the caller (set_balance);
            # no cache_key provenance → treated as measured, never overwritten.
            return self._fill_balance_usd(self._balances[resolved], resolved, requested_chain, price)

        # Symbol-only cache fallback for legacy callers.
        if resolved in self._balance_cache:
            filled = self._fill_balance_usd(
                self._balance_cache[resolved], resolved, requested_chain, price, cache_key=resolved
            )
            # Same write-back as the per-chain branch (cache_key=resolved here).
            self._balance_cache[resolved] = filled
            return filled

        # Use provider if available. Two protocols are supported:
        #   1. ``provider.get_balance(token[, chain=...])`` returns
        #      ``BalanceResult`` (sync-bridged via ``_run_async_bridged``). The
        #      ``chain`` kwarg is threaded only when the method declares it
        #      (VIB-5002: ``MultiChainGatewayBalanceProvider`` requires it).
        #   2. Modern callable provider: ``provider(token, chain=...)`` returns
        #      ``TokenBalance`` directly.
        if self._balance_provider:
            try:
                bp = self._balance_provider
                # Prefer legacy ``get_balance(token)`` protocol whenever the
                # provider exposes that method, regardless of whether ``bp``
                # itself is callable. The legacy data-layer fixture uses
                # ``provider.get_balance = AsyncMock(...)`` with provider as
                # a generic ``MagicMock`` (which is callable). If we picked
                # the callable shape, we'd return a Mock instead of awaiting
                # the AsyncMock and get a TypeError downstream.
                use_get_balance = hasattr(bp, "get_balance") and callable(getattr(bp, "get_balance", None))
                if use_get_balance:
                    # VIB-5002: chain-aware providers (e.g.
                    # ``MultiChainGatewayBalanceProvider.get_balance(token,
                    # chain)``) require the chain. Thread ``chain=`` through when
                    # the method's signature accepts it — an explicit ``chain``
                    # param or ``**kwargs`` (which covers ``AsyncMock`` fixtures,
                    # whose signature is ``(*args, **kwargs)`` and harmlessly
                    # ignore the kwarg). Stay single-arg only for providers that
                    # declare neither — e.g. the single-chain gateway provider's
                    # ``get_balance(token, *, force_refresh=...)``.
                    get_balance = bp.get_balance  # type: ignore[attr-defined]
                    if _balance_provider_supports_chain_arg(get_balance):
                        coro = get_balance(resolved, chain=requested_chain)
                    else:
                        coro = get_balance(resolved)
                    if asyncio.iscoroutine(coro):
                        result = self._run_async_bridged(coro)
                    else:
                        result = coro
                elif _balance_provider_supports_chain_arg(bp):
                    result = bp(resolved, chain=requested_chain)  # type: ignore[call-arg]
                else:
                    result = bp(resolved)
                # Classify provider provenance uniformly: a provider-supplied
                # ``TokenBalance`` MEASURED ``balance_usd`` (Empty≠Zero — even a
                # measured ``Decimal("0")`` is authoritative); legacy / bare
                # shapes leave the unmeasured sentinel.
                balance_data, usd_measured = self._coerce_balance_result(resolved, result)
                if not usd_measured:
                    self._balance_usd_unmeasured.add(cache_key)
                # VIB-2364 silent-zero guard: if the resolved token is an
                # address-form (0x..., 42 chars) and balance is zero, log a
                # warning so unregistered LST addresses are visible without
                # crashing the strategy. Matches legacy behaviour.
                bal = getattr(balance_data, "balance", None)
                if bal == Decimal("0") and resolved.startswith("0x") and len(resolved) == 42:
                    try:
                        from almanak.framework.data.tokens import get_token_resolver
                        from almanak.framework.data.tokens.exceptions import TokenResolutionError

                        get_token_resolver().resolve(
                            resolved,
                            self._chain,
                            skip_gateway=True,
                            log_errors=False,
                        )
                    except TokenResolutionError:
                        logger.warning(
                            "balance_zero_unregistered_token: token %s on %s returned 0 and is "
                            "not in the SDK registry. If you expect a non-zero balance, add the "
                            "token to almanak/framework/data/tokens/defaults.py or use the symbol.",
                            resolved,
                            self._chain,
                        )
                    except Exception:  # noqa: BLE001
                        logger.warning(
                            "balance_zero_unregistered_token: token %s on %s returned 0; "
                            "registry lookup itself failed.",
                            resolved,
                            self._chain,
                        )
                balance_data = self._fill_balance_usd(
                    balance_data,
                    resolved,
                    requested_chain,
                    price,
                    cache_key=cache_key,
                    usd_measured=usd_measured,
                )
                self._balance_cache[cache_key] = balance_data
                self._critical_data_failures.pop(("balance", cache_key), None)
                return balance_data
            except Exception as e:
                self._record_critical_data_failure("balance", cache_key, e)
                logger.warning(f"Balance provider failed for {cache_key}: {e}")

        self._record_critical_data_failure("balance", cache_key, f"Cannot determine balance for {cache_key}")
        raise ValueError(f"Cannot determine balance for {cache_key}")

    def _record_critical_data_failure(self, source: str, key: str, error: Exception | str) -> None:
        """Record a market-data failure that can invalidate a HOLD outcome."""
        # Keep the first (most specific) failure detail for each key.
        self._critical_data_failures.setdefault((source, key), str(error))

    def clear_critical_data_failures(self) -> None:
        """Clear all tracked critical data failures.

        Called by the runner after pre-warming the price cache and before
        strategy.decide() runs, so that pre-warm failures (which are retried
        inside decide()) do not incorrectly poison the HOLD-escalation check.
        """
        self._critical_data_failures.clear()

    def has_critical_data_failures(self) -> bool:
        """Return True when this snapshot observed any critical data failures."""
        return bool(self._critical_data_failures)

    def critical_data_failure_count(self) -> int:
        """Number of currently tracked critical failures for this snapshot."""
        return len(self._critical_data_failures)

    def classify_critical_data_failures(self) -> str:
        """Classify observed data failures as transient, permanent, or mixed."""
        if not self._critical_data_failures:
            return "none"

        transient_hints = (
            "timeout",
            "timed out",
            "temporarily unavailable",
            "rate limit",
            "429",
            "connection",
            "connection reset",
            "unavailable",
            "resource exhausted",
            "service unavailable",
            # gRPC status codes that indicate transient upstream failures.
            # StatusCode.INTERNAL is included because the gateway emits it for
            # transient CoinGecko Onchain API blips, not for local bugs.
            "statuscode.internal",
            "statuscode.unavailable",
            "statuscode.resource_exhausted",
            "statuscode.deadline_exceeded",
        )
        permanent_hints = (
            "cannot resolve token",
            "token '",
            "unknown token",
            "no chainlink feed",
            "not found",
            "unsupported",
            "invalid",
            "no pairs found",
            "symbol",
        )

        has_transient = False
        has_permanent = False
        for detail in self._critical_data_failures.values():
            lowered = detail.lower()
            # Strip "Data source '...' unavailable: " boilerplate so the word
            # "unavailable" in the wrapper doesn't cause a false transient hit on
            # permanent failures.  Check both hint sets independently: a combined
            # error string (e.g. "primary: gRPC INTERNAL; last: unknown token for
            # Binance") can match both, which should yield "mixed" — not "permanent".
            stripped = _DSU_BOILERPLATE_RE.sub("", lowered)
            found_permanent = any(hint in stripped for hint in permanent_hints)
            found_transient = any(hint in stripped for hint in transient_hints)
            if found_permanent:
                has_permanent = True
            if found_transient:
                has_transient = True
            elif not found_permanent:
                # Unknown failure class: be conservative and treat as transient
                # so the runner can retry/escalate through the error pipeline.
                has_transient = True

        if has_transient and has_permanent:
            return "mixed"
        if has_permanent:
            return "permanent"
        return "transient"

    def summarize_critical_data_failures(self, *, limit: int = 3) -> str:
        """Create a concise summary for logs/lifecycle error messages."""
        if not self._critical_data_failures:
            return ""

        chunks: list[str] = []
        for idx, ((source, key), detail) in enumerate(self._critical_data_failures.items()):
            if idx >= limit:
                break
            chunks.append(f"{source}({key}): {detail}")

        remaining = len(self._critical_data_failures) - len(chunks)
        if remaining > 0:
            chunks.append(f"... and {remaining} more")
        return "; ".join(chunks)

    def is_quiet_pool_hold(self) -> bool:
        """Liveness backstop for a DEX pool with no recent swaps.

        Returns True iff **every** recorded critical data failure is a DEX
        quiet-pool staleness miss *and* the affected token is still priceable
        from the 24/7 aggregated oracle.

        Rationale: a DEX pool that simply hasn't traded recently returns *stale*
        (not absent) trade-derived OHLCV, so swap-based indicators (RSI, MACD, …)
        can't be computed. But the asset itself is continuously priceable — the
        pool is alive, just quiet. Holding through that is correct and must not
        be escalated to a ``DATA_ERROR`` (which trips the circuit breaker on a
        live pool). A genuinely dead/unreachable feed has no oracle price and
        stays critical, so the dead-pool guard is preserved.

        The check is conservative: any non-quiet-pool failure, any unparseable
        detail, or any token that is not priceable returns False (escalate).
        """
        failures = list(self._critical_data_failures.values())
        if not failures:
            return False

        priceable: dict[str, bool] = {}
        for detail in failures:
            match = _QUIET_POOL_STALE_RE.search(detail)
            if match is None:
                # A failure that is not a DEX quiet-pool staleness miss (e.g. a
                # hard outage, an unknown token, a price-oracle error) — escalate.
                return False
            base = match.group("base")
            chain = match.group("chain")
            probe_key = f"{base}@{chain}"
            if probe_key not in priceable:
                priceable[probe_key] = self._probe_token_priceable(base, chain)
            if not priceable[probe_key]:
                return False
        return True

    def _probe_token_priceable(self, token: str, chain: str | None) -> bool:
        """Liveness probe: True when *token* has a positive current price from
        the 24/7 aggregated oracle, independent of swap-derived OHLCV.

        Never raises — any failure (no oracle, unknown token, chain not
        configured, non-numeric result) is treated as 'not priceable', so the
        caller conservatively escalates the data failure as before.
        """
        try:
            price = self.price(token, chain=chain) if chain else self.price(token)
            return price is not None and Decimal(str(price)) > 0
        except Exception:  # noqa: BLE001 — a liveness probe must never raise
            return False

    def _fill_balance_usd(
        self,
        tb: TokenBalance,
        resolved: str,
        requested_chain: str,
        price: Decimal | None,
        *,
        cache_key: str | None = None,
        usd_measured: bool | None = None,
    ) -> TokenBalance:
        """Fill ``balance_usd`` from a supplied or already-cached price (FR-5003).

        Never issues an oracle call. Resolution order:

        1. The caller-supplied ``price`` (authoritative, no fetch).
        2. A warm ``_price_cache`` / pre-populated ``_prices`` entry for the
           token on this chain (still no fetch).

        Empty≠Zero: USD is (re)computed ONLY when the current ``balance_usd``
        is the *unmeasured* coerced sentinel. "Unmeasured" is determined by
        ``usd_measured`` when the caller knows it (fresh provider path), else
        by membership in ``self._balance_usd_unmeasured`` keyed by
        ``cache_key`` (cache-hit paths). A provider that MEASURED
        ``balance_usd`` — including a measured ``Decimal("0")`` — is
        authoritative and is returned unchanged. When no price is available the
        input is returned unchanged so callers can still fill USD via
        ``price()`` themselves.
        """
        # Decide whether this balance's USD is still unmeasured.
        if usd_measured is not None:
            unmeasured = not usd_measured
        elif cache_key is not None:
            unmeasured = cache_key in self._balance_usd_unmeasured
        else:
            # No provenance available (pre-populated balances reach this
            # branch); their USD is measured by the caller, so leave untouched.
            unmeasured = False
        if not unmeasured:
            return tb
        usd_price = price if price is not None else self._cached_price_for(resolved, requested_chain)
        if usd_price is None:
            return tb
        filled = TokenBalance(
            symbol=tb.symbol,
            balance=tb.balance,
            balance_usd=tb.balance * usd_price,
            address=getattr(tb, "address", "") or "",
        )
        # USD is now measured for this key — stop treating it as unmeasured so
        # later reads don't recompute against a different cached price.
        if cache_key is not None:
            self._balance_usd_unmeasured.discard(cache_key)
        return filled

    def _seeded_price_for_symbol(self, token: str) -> Decimal | None:
        """Case-insensitive lookup in the ``set_price()``-seeded ``_prices`` map.

        ``_prices`` is seeded verbatim (no case normalization), so a mixed-case
        symbol (cbBTC, wstETH, cbETH, ...) seeded under one case must still
        resolve under another, or its price/balance USD is silently left
        unmeasured. Exact key first (the common path, O(1)), then a
        case-insensitive fallback mirroring ``get_price_oracle_dict()``'s
        upper-casing convention. USD-denominated (the only thing ``set_price``
        stores). Returns ``None`` when no seeded price exists.

        Single source of truth for both ``price()`` (the strategy-facing read)
        and ``_cached_price_for()`` (the balance USD fill), so the two never
        diverge on case handling again.
        """
        exact = self._prices.get(token)
        if exact is not None:
            return exact
        token_upper = token.upper()
        for key, val in self._prices.items():
            if key.upper() == token_upper:
                return val
        return None

    def _cached_price_for(self, token: str, requested_chain: str) -> Decimal | None:
        """Return an already-known USD price for ``token`` WITHOUT a fetch.

        Consults the pre-populated ``_prices`` map and the warm
        ``_price_cache`` (the same cache ``price()`` writes). Returns ``None``
        when no cached price exists — the caller must NOT trigger an oracle
        call from a balance lookup.
        """
        if requested_chain == self._chain:
            seeded = self._seeded_price_for_symbol(token)
            if seeded is not None:
                return seeded
        cache_key = f"{token}/USD@{requested_chain}"
        cached = self._price_cache.get(cache_key)
        if cached is not None:
            return cached.price
        return None

    def _coerce_balance_result(self, token: str, raw: Any) -> tuple[TokenBalance, bool]:
        """Normalize a balance-provider return value into a ``TokenBalance``.

        Accepts:
          * an existing ``TokenBalance`` (modern callable protocol),
          * a ``BalanceResult`` with ``.balance`` (legacy async protocol),
          * a bare ``Decimal`` (very old data-layer shape).

        Returns ``(TokenBalance, usd_measured)``. ``usd_measured`` is ``True``
        only when the provider itself supplied a real ``balance_usd`` (the
        modern ``TokenBalance`` shape) — including a MEASURED ``Decimal("0")``
        on a zero holding, which Empty≠Zero forbids overwriting. The legacy /
        bare-Decimal shapes carry no USD, so they return ``Decimal("0")`` as
        the *unmeasured* sentinel (``usd_measured=False``) which callers may
        later fill via ``price()``.

        VIB-4843 couldn't-price sentinel (Codex re-audit): a provider may hand
        back a ``TokenBalance`` whose ``balance_usd`` is ``Decimal("0")`` not
        because the holding is worth $0 but because it FAILED to price the
        token (e.g. ``create_sync_balance_func`` in ``cli/run.py`` swallows a
        price-oracle error and falls back to ``balance_usd=0``). A genuine
        measured zero is only trustworthy when the holding itself is zero
        (``0 * price == 0`` regardless of price). So a ``balance_usd == 0`` is
        treated as MEASURED only when ``balance == 0``; a non-zero holding with
        ``balance_usd == 0`` is treated as UNMEASURED so ``_fill_balance_usd``
        can recompute from an available price instead of reporting a wrong $0.
        """
        if isinstance(raw, TokenBalance):
            # Distinguish a genuine measured zero (zero holding) from a
            # couldn't-price zero on a non-zero holding (see docstring).
            if raw.balance_usd == Decimal("0") and raw.balance != Decimal("0"):
                return raw, False
            return raw, True
        balance = getattr(raw, "balance", raw)
        if isinstance(balance, Decimal):
            return TokenBalance(symbol=token, balance=balance, balance_usd=Decimal("0")), False
        try:
            return TokenBalance(symbol=token, balance=Decimal(str(balance)), balance_usd=Decimal("0")), False
        except Exception:  # noqa: BLE001
            return TokenBalance(symbol=token, balance=Decimal("0"), balance_usd=Decimal("0")), False

    def _resolve_protocol_variant(self, token: str, protocol: str | None) -> str:
        """Translate a generic symbol to the protocol's preferred variant.

        VIB-3138: Polymarket on Polygon settles in USDC.e (not native USDC),
        so strategies calling ``market.balance("USDC", protocol="polymarket")``
        must get the USDC.e balance. Reads the connector's
        ``settlement_token_variants`` capability (via ``CapabilitiesRegistry``)
        keyed by chain for the lookup; unknown mappings pass through.
        """
        if protocol is None:
            return token
        # VIB-4989: settlement-token variants are connector capabilities now, read
        # via CapabilitiesRegistry instead of a framework PROTOCOL_TOKEN_VARIANTS dict.
        from almanak.connectors._strategy_base.capabilities_registry import get_protocol_capabilities

        chain_key = (self._chain or "").lower()
        protocol_key = protocol.lower()
        variants = get_protocol_capabilities(protocol_key).get("settlement_token_variants", {})
        if not isinstance(variants, dict):
            return token
        protocol_map = variants.get(chain_key)
        if protocol_map is not None and not isinstance(protocol_map, dict):
            return token
        if not protocol_map:
            return token
        # Registry keys are canonical uppercase; normalize for lookup but keep
        # the caller-supplied symbol on passthrough.
        resolved = protocol_map.get(token.upper())
        if resolved is None:
            return token
        if resolved != token:
            logger.debug(
                "Protocol variant: %s on %s/%s -> %s",
                token,
                chain_key,
                protocol_key,
                resolved,
            )
        return resolved

    def balance_usd(self, token: str, protocol: str | None = None) -> Decimal:
        """Get wallet balance in USD terms.

        Args:
            token: Token symbol
            protocol: Optional protocol for variant disambiguation (see ``balance``).

        Returns:
            Balance in USD
        """
        return self.balance(token, protocol=protocol).balance_usd

    def collateral_value_usd(self, token: str, amount: Decimal) -> Decimal:
        """Get the USD value of a given amount of collateral.

        Convenience helper for perp position sizing. Multiplies the given
        amount by the token's current price.

        Args:
            token: Token symbol (e.g., "WETH", "USDC", "WBTC")
            amount: Token amount in human-readable units (not wei)

        Returns:
            USD value as a Decimal
        """
        token_price = self.price(token)
        return amount * token_price

    def total_portfolio_usd(self) -> Decimal:
        """Calculate total portfolio value in USD across all known balances.

        Sums ``balance_usd`` for all tokens in pre-populated balances and
        cached balances (tokens queried via ``balance()`` in this snapshot).

        ``_balance_cache`` keys are either the bare symbol (legacy single-arg
        provider path) or ``f"{symbol}@{chain}"`` (multi-chain path). Strip
        the chain suffix before dedup so a token populated via both
        ``set_balance(symbol, tb)`` (which writes both maps) is counted once.
        """
        total = Decimal("0")
        seen: set[str] = set()

        for token, balance in self._balances.items():
            total += balance.balance_usd
            seen.add(token)

        for cache_key, balance in self._balance_cache.items():
            symbol = cache_key.split("@", 1)[0] if "@" in cache_key else cache_key
            if symbol not in seen:
                total += balance.balance_usd
                seen.add(symbol)

        return total

    def set_price(self, token: str, price_value: Decimal) -> None:
        """Pre-populate price for a token.

        Args:
            token: Token symbol
            price_value: Price value in USD
        """
        self._prices[token] = price_value

    def set_price_data(
        self,
        token: str,
        price_data_or_chain: PriceData | str,
        price_data_or_quote: PriceData | str | None = None,
        quote: str = "USD",
        chain: str | None = None,
    ) -> None:
        """Pre-populate enriched price data for a token (useful for testing).

        Accepts both call shapes during the VIB-4062 transition:

        * ``set_price_data(token, price_data, quote="USD", chain=None)`` —
          canonical strategy-layer shape.
        * ``set_price_data(token, chain, price_data, quote="USD")`` — legacy
          multichain shape; chain is the second positional argument.

        The ambiguity is resolved by inspecting the type of the second
        argument: ``PriceData`` → canonical; ``str`` → legacy multichain.
        """
        if not isinstance(price_data_or_chain, PriceData):
            # Legacy multichain shape: (token, chain, price_data, quote)
            chain = price_data_or_chain
            if not isinstance(price_data_or_quote, PriceData):
                raise TypeError(
                    "set_price_data legacy shape requires (token, chain, price_data, ...) "
                    "but third arg is not a PriceData"
                )
            price_data = price_data_or_quote
        else:
            price_data = price_data_or_chain
            # In canonical shape, the third positional was ``quote``.
            if isinstance(price_data_or_quote, str):
                quote = price_data_or_quote
        return self._set_price_data_impl(token, price_data, quote=quote, chain=chain)

    def _set_price_data_impl(
        self,
        token: str,
        price_data: PriceData,
        quote: str = "USD",
        chain: str | None = None,
    ) -> None:
        """Pre-populate enriched price data for a token (useful for testing).

        Unlike set_price() which only sets a scalar price, this sets the full
        PriceData object including change_24h_pct, high_24h, low_24h, etc.

        Args:
            token: Token symbol
            price_data: PriceData with price, change_24h_pct, etc.
            quote: Quote currency (default "USD")
            chain: Optional chain override. Defaults to this snapshot's chain.
        """
        target_chain = chain or self._chain
        # Auto-extend ``self._chains`` so a subsequent ``price(token, chain=X)``
        # on the same chain doesn't raise ``ChainNotConfiguredError``. The
        # legacy multichain class allowed seed-then-query for arbitrary chains;
        # tests in tests/unit/strategies/test_market_snapshot.py rely on this.
        if target_chain != self._chain:
            existing = self._chains or (self._chain,)
            if target_chain not in existing:
                self._chains = (*existing, target_chain)
        cache_key = f"{token}/{quote}@{target_chain}"
        self._price_cache[cache_key] = price_data

    def set_balance(
        self,
        token: str,
        balance_data_or_chain: Any,
        balance_data: TokenBalance | None = None,
    ) -> None:
        """Pre-populate balance for a token.

        Two call shapes:

        1. Canonical (post-VIB-4062): ``set_balance(token, balance_data)``.
        2. Legacy multichain: ``set_balance(token, chain, balance_data)`` —
           kept so multi-chain runners (runner_teardown's simulated-balances
           injector, paper-engine cross-chain seeding) keep working.

        In the multichain shape, ``_balances`` (the chain-agnostic short cache)
        is updated only when ``chain == self._chain`` so cross-chain reads
        stay distinct.
        """
        if isinstance(balance_data_or_chain, TokenBalance):
            tb = balance_data_or_chain
            self._balances[token] = tb
            self._balance_cache[f"{token}@{self._chain}"] = tb
            # VIB-4843 (Codex re-audit): a caller-supplied balance is MEASURED.
            # If an earlier unpriced provider read marked this key as the
            # unmeasured sentinel, clear it so a later ``price=``/warm-cache
            # read does not recompute and clobber the measured value (incl. a
            # measured ``Decimal("0")``). Discard both the ``@chain`` key and
            # the bare-symbol legacy key.
            self._balance_usd_unmeasured.discard(f"{token}@{self._chain}")
            self._balance_usd_unmeasured.discard(token)
            return
        chain = str(balance_data_or_chain)
        if not isinstance(balance_data, TokenBalance):
            raise TypeError(
                "set_balance legacy shape requires (token, chain, balance_data); "
                f"got chain={chain!r} but balance_data is not a TokenBalance",
            )
        if chain == self._chain:
            self._balances[token] = balance_data
        self._balance_cache[f"{token}@{chain}"] = balance_data
        # VIB-4843 (Codex re-audit): clear any stale unmeasured marker for the
        # key we just overwrote with a MEASURED value (see canonical path).
        self._balance_usd_unmeasured.discard(f"{token}@{chain}")
        self._balance_usd_unmeasured.discard(token)

    def set_rsi(self, token: str, rsi_data: RSIData, timeframe: str | None = None) -> None:
        """Pre-populate RSI for a token.

        Args:
            token: Token symbol
            rsi_data: RSI data
            timeframe: OHLCV timeframe this data was computed from (None matches any)
        """
        self._rsi_values[token] = (rsi_data, timeframe)

    def set_macd(self, token: str, macd_data: MACDData, timeframe: str | None = None) -> None:
        """Pre-populate MACD data for a token.

        Args:
            token: Token symbol
            macd_data: MACDData instance
            timeframe: OHLCV timeframe this data was computed from (None matches any)

        Example:
            market.set_macd("WETH", MACDData(
                macd_line=Decimal("0.5"),
                signal_line=Decimal("0.3"),
                histogram=Decimal("0.2"),
            ))
        """
        self._macd_values[token] = (macd_data, timeframe)

    def set_bollinger_bands(self, token: str, bb_data: BollingerBandsData, timeframe: str | None = None) -> None:
        """Pre-populate Bollinger Bands data for a token.

        Args:
            token: Token symbol
            bb_data: BollingerBandsData instance
            timeframe: OHLCV timeframe this data was computed from (None matches any)

        Example:
            market.set_bollinger_bands("WETH", BollingerBandsData(
                upper_band=Decimal("3100"),
                middle_band=Decimal("3000"),
                lower_band=Decimal("2900"),
                percent_b=Decimal("0.5"),
            ))
        """
        self._bollinger_values[token] = (bb_data, timeframe)

    def set_stochastic(self, token: str, stoch_data: StochasticData, timeframe: str | None = None) -> None:
        """Pre-populate Stochastic data for a token.

        Args:
            token: Token symbol
            stoch_data: StochasticData instance
            timeframe: OHLCV timeframe this data was computed from (None matches any)

        Example:
            market.set_stochastic("WETH", StochasticData(
                k_value=Decimal("25"),
                d_value=Decimal("30"),
            ))
        """
        self._stochastic_values[token] = (stoch_data, timeframe)

    def set_atr(self, token: str, atr_data: ATRData, timeframe: str | None = None) -> None:
        """Pre-populate ATR data for a token.

        Args:
            token: Token symbol
            atr_data: ATRData instance
            timeframe: OHLCV timeframe this data was computed from (None matches any)

        Example:
            market.set_atr("WETH", ATRData(
                value=Decimal("50"),
                value_percent=Decimal("2.5"),
            ))
        """
        self._atr_values[token] = (atr_data, timeframe)

    def set_ma(
        self, token: str, ma_data: MAData, ma_type: str = "SMA", period: int = 20, timeframe: str | None = None
    ) -> None:
        """Pre-populate Moving Average data for a token.

        Args:
            token: Token symbol
            ma_data: MAData instance
            ma_type: Type of MA ("SMA" or "EMA")
            period: MA period
            timeframe: OHLCV timeframe this data was computed from (None matches any)

        Example:
            market.set_ma("WETH", MAData(
                value=Decimal("3000"),
                ma_type="SMA",
                period=20,
                current_price=Decimal("3050"),
            ), ma_type="SMA", period=20)
        """
        cache_key = f"{token}:{ma_type}:{period}"
        entry = (ma_data, timeframe)
        self._ma_values[cache_key] = entry
        # Also store under simple token key for convenience
        self._ma_values[token] = entry

    def set_adx(self, token: str, adx_data: ADXData, timeframe: str | None = None) -> None:
        """Pre-populate ADX data for a token.

        Args:
            token: Token symbol
            adx_data: ADXData instance
            timeframe: Optional timeframe (None matches any query)

        Example:
            market.set_adx("WETH", ADXData(
                adx=Decimal("30"),
                plus_di=Decimal("25"),
                minus_di=Decimal("15"),
            ))
        """
        self._adx_values[token] = (adx_data, timeframe)

    def set_obv(self, token: str, obv_data: OBVData, timeframe: str | None = None) -> None:
        """Pre-populate OBV data for a token.

        Args:
            token: Token symbol
            obv_data: OBVData instance
            timeframe: Optional timeframe (None matches any query)

        Example:
            market.set_obv("WETH", OBVData(
                obv=Decimal("1000000"),
                signal_line=Decimal("950000"),
            ))
        """
        self._obv_values[token] = (obv_data, timeframe)

    def set_cci(self, token: str, cci_data: CCIData, timeframe: str | None = None) -> None:
        """Pre-populate CCI data for a token.

        Args:
            token: Token symbol
            cci_data: CCIData instance
            timeframe: Optional timeframe (None matches any query)

        Example:
            market.set_cci("WETH", CCIData(
                value=Decimal("-120"),
            ))
        """
        self._cci_values[token] = (cci_data, timeframe)

    def set_ichimoku(self, token: str, ichimoku_data: IchimokuData, timeframe: str | None = None) -> None:
        """Pre-populate Ichimoku data for a token.

        Args:
            token: Token symbol
            ichimoku_data: IchimokuData instance
            timeframe: Optional timeframe (None matches any query)

        Example:
            market.set_ichimoku("WETH", IchimokuData(
                tenkan_sen=Decimal("3050"),
                kijun_sen=Decimal("3000"),
                senkou_span_a=Decimal("3025"),
                senkou_span_b=Decimal("2950"),
                current_price=Decimal("3100"),
            ))
        """
        self._ichimoku_values[token] = (ichimoku_data, timeframe)

    @staticmethod
    def _lending_cache_key(protocol: str, token: str, side: str) -> str:
        """Normalize lending rate cache key to avoid case-sensitive misses."""
        return f"{protocol.strip().lower()}/{token.strip().upper()}/{side.strip().lower()}"

    def _run_async_bridged(self, coro: Any) -> Any:
        """Bridge an async coroutine to sync, handling running event loops."""
        import asyncio

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            future = executor.submit(asyncio.run, coro)
            try:
                return future.result(timeout=10)
            finally:
                executor.shutdown(wait=False, cancel_futures=True)
        else:
            return asyncio.run(coro)

    def lending_rate(
        self,
        protocol: str,
        token: str,
        side: str = "supply",
        *,
        chain: str | None = None,
    ) -> Any:
        """Get the lending rate for a specific protocol and token.

        **Canonical strategy-side accessor for live lending rates** (VIB-4859 / W7).
        Delegates to the gateway's ``RateHistoryService.GetLendingRateCurrent``
        RPC via :class:`almanak.framework.data.rates.RateMonitor` — all HTTP /
        Web3 egress for rate lookups happens server-side via
        :class:`GatewayLendingRateHistoryCapability` implementations on the
        corresponding connectors.

        Use this in preference to the deprecated
        :class:`almanak.framework.data.rates.RateMonitor` direct surface
        (caller migration tracked in **VIB-4869**).

        Resolution order:

        1. Pre-populated cache (``set_lending_rate(...)``) — hit first for
           strategies that inject rates synthetically (backtests / tests).
        2. The constructor-injected ``rate_monitor`` (legacy path), if set.
        3. A lazily-constructed default ``RateMonitor`` (calls the gateway).

        Args:
            protocol: Protocol identifier (``"aave_v3"``, ``"compound_v3"``,
                ``"morpho_blue"``). Must be registered with
                ``GatewayLendingRateHistoryCapability`` on the gateway side.
            token: Token symbol (e.g. ``"USDC"``, ``"WETH"``).
            side: Rate side — ``"supply"`` (default) or ``"borrow"``.
            chain: Optional chain override (keyword-only, PRD §4.2 R1). When
                omitted the snapshot's resolved chain is used.

        Returns:
            :class:`LendingRate` dataclass with ``apy_percent``, ``apy_ray``,
            ``utilization_percent``, etc.

        Raises:
            ChainNotConfiguredError / AmbiguousChainError: same rules as :meth:`price`.
            ValueError: If the gateway returns no rate and no fallback is configured.

        Example:
            rate = market.lending_rate("aave_v3", "USDC", "supply")
            print(f"Aave USDC Supply APY: {rate.apy_percent:.2f}%")
        """
        from almanak.framework.data.rates import RateSide

        # Normalize ``side`` once, up front, so every downstream consumer
        # (cache key, legacy RateMonitor path, gateway path) sees the same
        # canonical lowercase value. Without this a caller passing "SUPPLY"
        # would build a lowercased cache key here but then crash on
        # ``RateSide("SUPPLY")`` (StrEnum lookup is case-sensitive), and the
        # gateway path would forward the raw mixed-case string — an
        # inconsistency between the two lanes (VIB-4859 re-review).
        side_str = (side.value if isinstance(side, RateSide) else str(side)).strip().lower()
        rate_side = RateSide(side_str)

        # Check pre-populated rates first.
        cache_key = self._lending_cache_key(protocol, token, side_str)
        if cache_key in self._lending_rate_cache:
            return self._lending_rate_cache[cache_key]

        # Use the constructor-injected RateMonitor if present (legacy path
        # — preserves test surfaces that mock the monitor).
        if self._rate_monitor is not None:
            try:
                result = self._run_async_bridged(self._rate_monitor.get_lending_rate(protocol, token, rate_side))
                self._lending_rate_cache[cache_key] = result
                return result
            except ValueError:
                raise
            except Exception as e:
                raise ValueError(f"Failed to get lending rate for {protocol}/{token}/{side_str}: {e}") from e

        # No monitor injected — construct a default gateway-backed one and
        # try it via the gateway lane only (no placeholder-rate fallback).
        # The default RateMonitor is a thin gRPC client of the gateway's
        # RateHistoryService.GetLendingRateCurrent.
        #
        # When the gateway is unreachable AND no monitor was injected, we
        # preserve the pre-W7 contract that the caller sees a ``ValueError``
        # mentioning "rate monitor" (matches
        # ``tests/unit/data/test_market_snapshot_strategy_api.py::
        # TestProviderlessMethodsRaiseValueError::test_raises_value_error``).
        # The strategy must explicitly inject a ``RateMonitor`` or run a
        # gateway to get a real rate — the auto-placeholder fallback that
        # ``RateMonitor.get_lending_rate`` does on its own is intentionally
        # NOT surfaced here, since silently substituting hardcoded numbers
        # for a strategy that asked for a live rate is a bigger footgun than
        # raising loudly.
        from almanak.framework.data.interfaces import DataSourceUnavailable
        from almanak.framework.data.rates.monitor import RateMonitor

        requested_chain = chain if chain is not None else (self._chain if self._chain is not None else "ethereum")

        # ``_internal=True``: this IS the canonical lending-rate lane, not a
        # deprecated strategy-side bypass (VIB-4869 disposition). The monitor
        # is the framework-internal gateway gRPC client backing this method.
        monitor = RateMonitor(chain=requested_chain, _internal=True)

        async def _fetch_via_gateway() -> Any:
            # Bypass the monitor's placeholder-fallback wrapper so an
            # unreachable gateway raises rather than silently returning a
            # hardcoded number. Forward the normalized ``side_str`` so the
            # gateway lane matches the legacy lane and the cache key.
            return await monitor._fetch_lending_rate_via_gateway(protocol, token, side_str)

        try:
            result = self._run_async_bridged(_fetch_via_gateway())
        except DataSourceUnavailable as exc:
            raise ValueError(
                f"No rate monitor configured for MarketSnapshot and the gateway is "
                f"unavailable for {protocol}/{token}/{side_str}: {exc}. "
                "Pass rate_monitor= to MarketSnapshot() or use set_lending_rate() "
                "to pre-populate rates."
            ) from exc
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Failed to get lending rate for {protocol}/{token}/{side_str}: {e}") from e

        self._lending_rate_cache[cache_key] = result
        return result

    def best_lending_rate(
        self,
        token: str,
        side: str = "supply",
        protocols: list[str] | None = None,
        *,
        chain: str | None = None,
    ) -> Any:
        """Get the best lending rate for a token across protocols.

        **Canonical strategy-side accessor** for the best live lending rate
        (VIB-4869), symmetric with :meth:`lending_rate`. For supply rates,
        returns highest APY; for borrow rates, returns lowest APY.

        Resolution order mirrors :meth:`lending_rate`:

        1. The constructor-injected ``rate_monitor`` (legacy path), if set —
           preserves test surfaces that mock the monitor.
        2. A lazily-constructed framework-internal ``RateMonitor``
           (``_internal=True``) that fans out to the gateway. This keeps
           direct instantiations (README-style flows, unit / backtest
           harnesses calling ``create_market_snapshot()`` directly) working
           without an injected monitor, instead of unconditionally raising.

        As with :meth:`lending_rate`, the lazy lane queries the gateway
        directly and **raises loudly** when the gateway is unreachable rather
        than silently substituting placeholder rates — surfacing hardcoded
        numbers for a strategy that asked for the best live rate is a bigger
        footgun than raising.

        Args:
            token: Token symbol (e.g., "USDC", "WETH")
            side: Rate side - "supply" or "borrow" (default "supply")
            protocols: Protocols to compare (default: all available on chain)
            chain: Optional chain override (keyword-only). When omitted the
                snapshot's resolved chain is used.

        Returns:
            BestRateResult with best_rate, all_rates, etc.

        Raises:
            ValueError: If no monitor is injected and the gateway is
                unavailable, or if the best-rate lookup otherwise fails.

        Example:
            result = market.best_lending_rate("USDC", "supply")
            if result.best_rate:
                print(f"Best: {result.best_rate.protocol} at {result.best_rate.apy_percent:.2f}%")
        """
        from almanak.framework.data.rates import RateSide

        # Normalize ``side`` once so both lanes (legacy injected monitor and
        # the lazily-constructed gateway monitor) see the same canonical
        # lowercase value, mirroring :meth:`lending_rate`.
        side_str = (side.value if isinstance(side, RateSide) else str(side)).strip().lower()

        try:
            rate_side = RateSide(side_str)
        except ValueError as exc:
            raise ValueError(f"Invalid lending rate side {side!r}: expected 'supply' or 'borrow'") from exc

        # Use the constructor-injected RateMonitor if present (legacy path —
        # preserves test surfaces that mock the monitor and any placeholder
        # behaviour those callers relied on).
        if self._rate_monitor is not None:
            try:
                result = self._run_async_bridged(self._rate_monitor.get_best_lending_rate(token, rate_side, protocols))
                return result
            except ValueError:
                raise
            except Exception as e:
                raise ValueError(f"Failed to get best lending rate for {token}/{side_str}: {e}") from e

        # No monitor injected — construct a default framework-internal one and
        # fan out to the gateway directly. ``_internal=True`` marks this as the
        # canonical lending-rate lane (VIB-4869), not a deprecated strategy-side
        # bypass. We bypass ``get_best_lending_rate``'s per-protocol placeholder
        # fallback so an unreachable gateway raises rather than silently
        # returning hardcoded numbers — matching :meth:`lending_rate`.
        from almanak.framework.data.interfaces import DataSourceUnavailable
        from almanak.framework.data.rates.monitor import BestRateResult, RateMonitor

        requested_chain = chain if chain is not None else (self._chain if self._chain is not None else "ethereum")
        monitor = RateMonitor(chain=requested_chain, _internal=True)
        target_protocols = protocols if protocols else monitor.protocols

        async def _best_via_gateway() -> Any:
            # Tolerate per-protocol gateway failures (one protocol with no
            # data shouldn't sink the whole comparison), but if *every*
            # protocol is unavailable surface the first failure so the caller
            # raises rather than returning an empty result. Mirrors
            # ``RateMonitor._safe_get_rate``'s per-protocol resilience while
            # keeping :meth:`lending_rate`'s no-placeholder contract.
            settled = await asyncio.gather(
                *(monitor._fetch_lending_rate_via_gateway(protocol, token, side_str) for protocol in target_protocols),
                return_exceptions=True,
            )
            all_rates = [r for r in settled if not isinstance(r, BaseException)]
            if not all_rates:
                first_error = next(
                    (r for r in settled if isinstance(r, BaseException)),
                    None,
                )
                if first_error is not None:
                    raise first_error
            if side_str == RateSide.SUPPLY.value:
                # For supply, higher APY is better.
                best_rate = max(all_rates, key=lambda r: r.apy_percent) if all_rates else None
            else:
                # For borrow, lower APY is better.
                best_rate = min(all_rates, key=lambda r: r.apy_percent) if all_rates else None
            return BestRateResult(
                token=token,
                side=side_str,
                best_rate=best_rate,
                all_rates=all_rates,
            )

        try:
            return self._run_async_bridged(_best_via_gateway())
        except DataSourceUnavailable as exc:
            raise ValueError(
                f"No rate monitor configured for MarketSnapshot and the gateway is "
                f"unavailable for best {side_str} rate on {token}: {exc}. "
                "Pass rate_monitor= to MarketSnapshot() or use set_lending_rate() "
                "to pre-populate rates."
            ) from exc
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Failed to get best lending rate for {token}/{side_str}: {e}") from e

    def set_lending_rate(self, protocol: str, token: str, side: str, rate: Any) -> None:
        """Pre-populate a lending rate for a protocol/token/side.

        Useful for backtesting and testing where you want to inject known rates
        without needing a live RateMonitor.

        Args:
            protocol: Protocol identifier (e.g., "aave_v3")
            token: Token symbol (e.g., "USDC")
            side: Rate side ("supply" or "borrow")
            rate: LendingRate dataclass instance

        Example:
            from almanak.framework.data.rates import LendingRate
            market.set_lending_rate("aave_v3", "USDC", "supply", LendingRate(
                protocol="aave_v3", token="USDC", side="supply",
                apy_ray=Decimal("0"), apy_percent=Decimal("4.25"),
            ))
        """
        cache_key = self._lending_cache_key(protocol, token, side)
        self._lending_rate_cache[cache_key] = rate

    def position_health(
        self,
        protocol: str,
        market_id: str,
        rpc_url: str | None = None,
        collateral_price_usd: Decimal | None = None,
        debt_price_usd: Decimal | None = None,
    ) -> Any:
        """Get health factor for a lending position.

        Reads on-chain position data and computes the health factor for
        Aave V3, Morpho Blue, or Compound V3 positions. Uses gateway-routed
        eth_calls when a gateway_client was wired into this MarketSnapshot.

        Args:
            protocol: "aave_v3", "morpho_blue", or "compound_v3"
            market_id: Protocol-specific market identifier. For Aave V3 this is
                informational (one pool per chain). For Morpho Blue this is the
                bytes32 market id. For Compound V3 this is the Comet market key
                (e.g. "usdc", "weth").
            rpc_url: Optional explicit RPC URL. Strategies should leave this None
                so the gateway-routed path is used. Paper-trading code may pass
                a local anvil URL.
            collateral_price_usd: Optional override for collateral price (Morpho
                cross-asset markets require this).
            debt_price_usd: Optional override for debt-token price (same).

        Returns:
            PositionHealth with .health_factor (Decimal), .collateral_value_usd,
            .debt_value_usd, .max_borrow_usd, .protocol, .market_id.

        Raises:
            ValueError: If health data cannot be retrieved.
        """
        # Cache key includes the override inputs (CodeRabbit 2026-05-06):
        # a second call with different rpc_url / price overrides was reusing
        # the first result and returning stale health data.
        cache_key: tuple[str, ...] = (
            protocol,
            market_id,
            rpc_url or "",
            str(collateral_price_usd) if collateral_price_usd is not None else "",
            str(debt_price_usd) if debt_price_usd is not None else "",
        )
        if cache_key in self._position_health_cache:
            return self._position_health_cache[cache_key]

        from almanak.framework.data.position_health import PositionHealthProvider
        from almanak.framework.market import HealthUnavailableError

        provider = PositionHealthProvider(
            rpc_url=rpc_url or "",
            chain=self._chain,
            price_oracle=self._price_oracle,
            gateway_client=self._gateway_client,
        )
        try:
            health = provider.get_health(
                protocol=protocol,
                market_id=market_id,
                user_address=self._wallet_address,
                collateral_price_usd=collateral_price_usd,
                debt_price_usd=debt_price_usd,
            )
        except HealthUnavailableError:
            raise
        except Exception as e:
            raise HealthUnavailableError(f"Position health unavailable: {e}") from e

        self._position_health_cache[cache_key] = health
        return health

    def set_position_health(self, protocol: str, market_id: str, health: Any) -> None:
        """Pre-populate position health for tests and backtesting.

        Useful where you want to inject a known PositionHealth without needing
        a live gateway_client. Mirrors set_lending_rate.

        Args:
            protocol: Protocol identifier (e.g., "aave_v3")
            market_id: Protocol-specific market identifier
            health: PositionHealth dataclass instance
        """
        # Pre-population uses the short-key shape so any subsequent
        # ``position_health`` call without overrides ALSO hits this entry.
        self._position_health_cache[(protocol, market_id, "", "", "")] = health

    def aave_health_factor(self, *, chain: str | None = None) -> Decimal | None:
        """Aave V3 account health factor for ``chain`` via the wired provider.

        Returns ``None`` when no ``aave_health_factor_provider`` was wired into
        this snapshot (the default multi-chain CLI path wires only a balance
        provider) OR when the provider reports no live Aave position. Callers
        treat ``None`` as "no confirmed live position" and must not infer a
        healthy or unhealthy position from it. This mirrors the
        None-on-no-provider convention of :meth:`prediction_price` /
        :meth:`wallet_activity` so ``decide()`` can branch with ``if hf is
        None`` (see ``docs/internal/blueprints/01-data-layer.md``
        §MarketSnapshot — provider-driven methods).

        The provider (an ``AaveHealthFactorProvider`` — a
        ``(chain) -> Decimal | None`` callable) owns any gateway-routed read;
        this accessor is pure delegation and opens no network connection
        itself, so it does not cross the gateway boundary. Exceptions raised by
        the provider propagate unchanged: a gateway failure must surface, never
        be silently coerced to ``None`` and mistaken for "no position" — the
        leverage-stacking guard in cross-chain strategies depends on that
        distinction.

        Args:
            chain: Chain to query (keyword-only, PRD §4.2). Required on a
                multi-chain snapshot; on a single-chain snapshot it must match
                ``self.chain`` or be ``None``.

        Returns:
            The Aave V3 health factor as a ``Decimal``, or ``None`` when no
            provider is wired / no live position exists.
        """
        if self._aave_health_factor_provider is None:
            return None
        return self._aave_health_factor_provider(self._resolve_chain(chain))

    def funding_rate(self, venue: str, market: str) -> FundingRate:
        """Get the current funding rate for a perpetual market on a specific venue.

        Args:
            venue: Venue identifier (e.g., "gmx_v2", "hyperliquid")
            market: Market symbol (e.g., "ETH-USD")

        Returns:
            FundingRate dataclass with rate_hourly, rate_8h, rate_annualized, etc.

        Raises:
            ValueError: If no funding rate provider is configured or venue is unsupported
        """
        if self._funding_rate_provider is None:
            raise ValueError("No funding rate provider configured for MarketSnapshot")

        from almanak.framework.data.funding import Venue

        venue_enum = Venue(venue)
        return self._run_async_bridged(self._funding_rate_provider.get_funding_rate(venue_enum, market))

    def funding_rate_spread(self, market: str, venue_a: str, venue_b: str) -> FundingRateSpread:
        """Get the funding rate spread between two venues.

        Args:
            market: Market symbol (e.g., "ETH-USD")
            venue_a: First venue identifier
            venue_b: Second venue identifier

        Returns:
            FundingRateSpread dataclass with spread_hourly, spread_annualized, rate_a, rate_b

        Raises:
            ValueError: If no funding rate provider is configured or venue is unsupported
        """
        if self._funding_rate_provider is None:
            raise ValueError("No funding rate provider configured for MarketSnapshot")

        from almanak.framework.data.funding import Venue

        venue_a_enum = Venue(venue_a)
        venue_b_enum = Venue(venue_b)
        return self._run_async_bridged(
            self._funding_rate_provider.get_funding_rate_spread(market, venue_a_enum, venue_b_enum)
        )

    def wallet_activity(
        self,
        leader_address: str | None = None,
        action_types: list[str] | None = None,
        min_usd_value: Decimal | None = None,
        protocols: list[str] | None = None,
    ) -> list:
        """Get leader wallet activity signals for copy trading.

        Returns filtered signals from the WalletActivityProvider. If no
        provider is configured, returns an empty list (graceful degradation).

        Args:
            leader_address: Filter by specific leader wallet address
            action_types: Filter by action types (e.g., ["SWAP"])
            min_usd_value: Minimum USD value filter
            protocols: Filter by protocol names (e.g., ["uniswap_v3"])

        Returns:
            List of CopySignal objects matching the filters
        """
        if self._wallet_activity_provider is None:
            return []
        return self._wallet_activity_provider.get_signals(
            action_types=action_types,
            protocols=protocols,
            min_usd_value=min_usd_value,
            leader_address=leader_address,
        )

    def prediction_price(
        self,
        market_id: str,
        outcome: str,
    ) -> Decimal | None:
        """Get current price for a prediction market outcome.

        Convenience method that extracts the YES or NO price from a market.

        Args:
            market_id: Prediction market ID or URL slug
            outcome: "YES" or "NO"

        Returns:
            Current price as Decimal (0.01 to 0.99), or None if unavailable

        Example:
            yes_price = market.prediction_price("btc-100k", "YES")
            if yes_price is not None and yes_price < Decimal("0.3"):
                return BuyIntent(...)
        """
        if self._prediction_provider is None:
            return None

        try:
            return self._prediction_provider.get_price(market_id, outcome)
        except Exception:
            logger.debug(f"Failed to get prediction price for {market_id}/{outcome}")
            return None

    def get_price_oracle_dict(self, with_sources: bool = False) -> dict[str, Any]:
        """Get all prices as a dict suitable for IntentCompiler.

        Combines pre-populated prices and cached prices from oracle calls.
        Keys are normalized to uppercase to match Token.symbol (which is
        always uppercased by Token.__post_init__).  This prevents
        case-mismatch lookup failures for mixed-case tokens like cbETH,
        wstETH, crvUSD, sUSDe, etc.

        Args:
            with_sources: When ``True`` (VIB-3889), return the canonical
                nested shape ``{symbol: {price_usd, oracle_source,
                fetched_at, confidence}}`` so downstream writers
                (transaction_ledger.price_inputs_json) carry the actual
                provider name rather than "unknown". Default ``False``
                preserves the legacy flat ``{symbol: price}`` return.

        Returns:
            Flat ``dict[str, Decimal]`` (default) or nested
            ``dict[str, dict]`` when ``with_sources=True``.
        """
        if not with_sources:
            prices: dict[str, Decimal] = {}
            # Add pre-populated prices (normalize keys to uppercase)
            for key, val in self._prices.items():
                prices[key.upper()] = val
            # Add cached prices from oracle calls (key format: "TOKEN/USD")
            for cache_key, price_data in self._price_cache.items():
                if "/" in cache_key:
                    token = cache_key.split("/")[0].upper()
                    prices[token] = price_data.price
            return prices

        # VIB-3889 nested shape: {symbol: {price_usd, oracle_source, ...}}
        # — the canonical AttemptNo17 §1.2 G12 shape that ledger.py:529-544
        # propagates verbatim into transaction_ledger.price_inputs_json.
        nested: dict[str, dict[str, Any]] = {}
        for key, val in self._prices.items():
            nested[key.upper()] = {
                "price_usd": str(val),
                "oracle_source": "preloaded",
                "fetched_at": "",
                "confidence": "HIGH",
            }
        for cache_key, price_data in self._price_cache.items():
            if "/" not in cache_key:
                continue
            token = cache_key.split("/")[0].upper()
            source = getattr(price_data, "source", "") or "unknown"
            ts = getattr(price_data, "timestamp", None)
            nested[token] = {
                "price_usd": str(price_data.price),
                "oracle_source": source,
                "fetched_at": ts.isoformat() if ts is not None else "",
                "confidence": "HIGH",
            }
        return nested

    # =========================================================================
    # VIB-4062 — Methods ported from the data-layer copy.
    #
    # gas_price / estimate_swap_gas_cost_usd / is_trade_worthwhile lived
    # exclusively on framework/data/market_snapshot.py. The migration brings
    # them onto the canonical class so the strategy-layer surface no longer
    # short-circuits on missing methods (the original VIB-4062 symptom).
    # =========================================================================

    def gas_price(self, chain: str | None = None) -> Any:
        """Get current gas price for a chain (data-layer port).

        Returns ``GasPrice`` from the configured ``_gas_oracle`` (provider
        attribute). Caches per ``_gas_cache_ttl_seconds`` (default 12s).

        Raises:
            GasUnavailableError: If the gas price cannot be fetched.
            ValueError: If no gas oracle is configured.
        """
        from .errors import GasUnavailableError

        gas_oracle = getattr(self, "_gas_oracle", None)
        if gas_oracle is None:
            raise ValueError("No gas oracle configured for MarketSnapshot")

        # Lower-case chain name to match the chain registry (matches main's
        # data-layer behaviour — TestChainCasingNormalization in
        # tests/unit/data/test_market_snapshot_strategy_api.py).
        target_chain = (chain or self._chain).lower()
        cache: dict = getattr(self, "_gas_cache", None) or {}
        ttl = getattr(self, "_gas_cache_ttl_seconds", 12)

        if target_chain in cache:
            cached_gas, cached_time = cache[target_chain]
            if (datetime.now(UTC) - cached_time).total_seconds() < ttl:
                return cached_gas

        try:
            gp_coro = gas_oracle.get_gas_price(target_chain)
            if asyncio.iscoroutine(gp_coro):
                from .sync_bridge import run_sync

                gas_price_result = run_sync(gp_coro)
            else:
                gas_price_result = gp_coro
            cache[target_chain] = (gas_price_result, datetime.now(UTC))
            self._gas_cache = cache
            return gas_price_result
        except GasUnavailableError:
            raise
        except Exception as exc:
            raise GasUnavailableError(
                chain=target_chain,
                reason=f"Unexpected error: {exc}",
            ) from exc

    def estimate_swap_gas_cost_usd(self, chain: str | None = None) -> Decimal:
        """Estimate USD cost of a typical DEX swap on the given chain.

        Returns ``Decimal("0")`` when the gas oracle is unconfigured or the
        underlying ``estimated_cost_usd`` is zero — strategy authors who need
        a fail-closed gate should call ``gas_price()`` directly and handle
        the typed errors.
        """
        # Lazy imports to avoid framework->intents circular on cold path.
        from almanak.framework.data.defi.gas import STANDARD_GAS_UNITS as _ERC20_TRANSFER_GAS
        from almanak.framework.intents.compiler_constants import get_gas_estimate

        target_chain = (chain or self._chain).lower()
        if getattr(self, "_gas_oracle", None) is None:
            # T3-A (VIB-4844): keep the Decimal("0") return — switching to None
            # is signature-breaking because callers do arithmetic on the result
            # (e.g. ``est + slippage``) — but make the silent fail-open
            # observable. This is a stopgap; T3-E wires a real gas oracle so
            # this branch stops being hit on the live path.
            logger.warning(
                "gas oracle unconfigured; returning Decimal('0') swap gas estimate "
                "for chain '%s' — gas accounting is unavailable for this MarketSnapshot",
                target_chain,
            )
            return Decimal("0")

        gp = self.gas_price(target_chain)
        baseline_gas = _ERC20_TRANSFER_GAS
        swap_gas = get_gas_estimate(target_chain, "swap_simple")
        if baseline_gas <= 0 or swap_gas <= 0 or gp.estimated_cost_usd <= 0:
            return Decimal("0")
        scale = Decimal(swap_gas) / Decimal(baseline_gas)
        return (gp.estimated_cost_usd * scale).quantize(Decimal("0.0001"))

    def is_trade_worthwhile(
        self,
        amount_usd: Decimal,
        chain: str | None = None,
        max_gas_ratio: Decimal = Decimal("0.05"),
    ) -> bool:
        """Whether ``amount_usd`` is worth paying gas for on ``chain``.

        Fail-open: if a gas estimate cannot be obtained (oracle missing,
        returns 0, or raises ``GasUnavailableError``), returns True so the
        strategy is not silently halted by a transient infrastructure issue.
        """
        from .errors import GasUnavailableError

        if amount_usd <= 0:
            return False
        if max_gas_ratio <= 0:
            return False
        target_chain = (chain or self._chain).lower()
        try:
            gas_cost_usd = self.estimate_swap_gas_cost_usd(target_chain)
        except GasUnavailableError:
            # T3-A (VIB-4844): fail-open is intentional (don't halt the
            # strategy on a transient infra issue) but must be observable.
            logger.warning(
                "trade-worthwhile check defaulting to True: gas cost unavailable (GasUnavailableError) for chain '%s'",
                target_chain,
            )
            return True
        if gas_cost_usd <= 0:
            # Reached when the gas oracle is unconfigured (estimate returns
            # Decimal("0")) or the underlying estimate is zero. The
            # unconfigured path already warned in estimate_swap_gas_cost_usd;
            # warn here too so the worthwhile decision itself is traceable.
            logger.warning(
                "trade-worthwhile check defaulting to True: gas cost unavailable (estimate <= 0) for chain '%s'",
                target_chain,
            )
            return True
        return (gas_cost_usd / amount_usd) < max_gas_ratio

    def pool_price(
        self,
        pool_address: str,
        chain: str | None = None,
    ) -> DataEnvelope[PoolPrice]:
        """Get the live price from an on-chain DEX pool.

        Reads slot0() from the pool contract and decodes sqrtPriceX96 into a
        human-readable price. Classification is EXECUTION_GRADE (fail-closed,
        no off-chain fallback).

        Args:
            pool_address: Pool contract address.
            chain: Chain name. Defaults to this snapshot's chain.

        Returns:
            DataEnvelope[PoolPrice] with provenance metadata.

        Raises:
            ValueError: If no pool reader registry is configured.
            PoolPriceUnavailableError: If the pool price cannot be retrieved.
        """
        from almanak.framework.data.market_snapshot import PoolPriceUnavailableError

        if self._pool_reader_registry is None:
            raise ValueError("No pool reader registry configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()
        try:
            protocols = self._pool_reader_registry.protocols_for_chain(target_chain)
            if not protocols:
                raise PoolPriceUnavailableError(
                    pool_address,
                    f"No pool reader protocols registered for chain '{target_chain}'",
                )
            last_error: Exception | None = None
            for protocol in protocols:
                try:
                    reader = self._pool_reader_registry.get_reader(target_chain, protocol)
                    return reader.read_pool_price(pool_address, target_chain)
                except Exception as e:  # noqa: BLE001
                    last_error = e
                    continue
            raise PoolPriceUnavailableError(
                pool_address,
                f"All protocols failed for pool {pool_address} on {target_chain}: {last_error}",
            )
        except PoolPriceUnavailableError:
            raise
        except Exception as e:  # noqa: BLE001
            raise PoolPriceUnavailableError(pool_address, f"Unexpected error: {e}") from e

    def pool_price_by_pair(
        self,
        token_a: str,
        token_b: str,
        chain: str | None = None,
        protocol: str | None = None,
        fee_tier: int = 3000,
    ) -> DataEnvelope[PoolPrice]:
        """Get the live pool price for a token pair.

        Resolves the pool address for the given pair and reads the price.

        Args:
            token_a: Token A symbol or address.
            token_b: Token B symbol or address.
            chain: Chain name. Defaults to this snapshot's chain.
            protocol: Protocol name (e.g. "uniswap_v3"). If None, tries all
                registered protocols.
            fee_tier: Fee tier in basis points (default 3000 = 0.3%).

        Returns:
            DataEnvelope[PoolPrice] with provenance metadata.

        Raises:
            ValueError: If no pool reader registry is configured.
            PoolPriceUnavailableError: If the pool cannot be found or the
                price cannot be read.
        """
        from almanak.framework.data.market_snapshot import PoolPriceUnavailableError

        if self._pool_reader_registry is None:
            raise ValueError("No pool reader registry configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()
        pair_str = f"{token_a}/{token_b}"

        protocols = [protocol] if protocol else self._pool_reader_registry.protocols_for_chain(target_chain)
        if not protocols:
            raise PoolPriceUnavailableError(
                pair_str,
                f"No pool reader protocols registered for chain '{target_chain}'",
            )
        last_error: Exception | None = None
        for proto in protocols:
            try:
                reader = self._pool_reader_registry.get_reader(target_chain, proto)
                pool_addr = reader.resolve_pool_address(token_a, token_b, target_chain, fee_tier)
                if pool_addr is None:
                    continue
                return reader.read_pool_price(pool_addr, target_chain)
            except Exception as e:  # noqa: BLE001
                last_error = e
                continue

        raise PoolPriceUnavailableError(
            pair_str,
            f"No pool found for {pair_str} (fee_tier={fee_tier}) on {target_chain}: {last_error}",
        )

    def pool_reserves(self, pool_address: str, chain: str | None = None) -> PoolReserves:
        """Get DEX pool reserves and state.

        Fetches the current state of a DEX liquidity pool from the blockchain.

        Args:
            pool_address: Pool contract address.
            chain: Chain identifier. Defaults to this snapshot's chain.

        Returns:
            PoolReserves dataclass with reserves, fee tier, sqrtPrice, etc.

        Raises:
            ValueError: If no pool reader is configured.
            PoolReservesUnavailableError: If pool data cannot be retrieved.
        """
        from almanak.framework.data.interfaces import (
            DataSourceError,
            DataSourceUnavailable,
        )
        from almanak.framework.data.market_snapshot import PoolReservesUnavailableError

        if self._pool_reader is None:
            raise ValueError("No pool reader configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()
        try:
            return self._run_async_bridged(self._pool_reader.get_pool_reserves(pool_address, target_chain))
        except DataSourceUnavailable as e:
            raise PoolReservesUnavailableError(pool_address, e.reason) from e
        except DataSourceError as e:
            raise PoolReservesUnavailableError(pool_address, str(e)) from e
        except Exception as e:  # noqa: BLE001
            raise PoolReservesUnavailableError(pool_address, f"Unexpected error: {e}") from e

    # --- Price aggregation ----------------------------------------------------

    def _resolve_pool_decimals_for_twap(
        self,
        pool_address: str,
        chain: str,
        protocol: str,
        token0_decimals: int | None,
        token1_decimals: int | None,
        *,
        explicit_pool: bool,
    ) -> tuple[int, int]:
        """Resolve (token0_decimals, token1_decimals) for an explicit
        pool_address before passing to PriceAggregator.twap().

        Caller-supplied decimals always take precedence. Otherwise we
        consult the pool_reader_registry. If neither path can produce
        decimals, raises ``ValueError`` — silently defaulting to 18/6
        produces TWAPs off by powers of ten for non-WETH/USDC pools and
        is unacceptable for an EXECUTION_GRADE call.

        ``explicit_pool`` distinguishes the two callers:
        - True: caller passed pool_address directly. The registry is
          optional (caller may supply decimals), so a missing registry
          is itself a ValueError with a remediation hint.
        - False: caller passed only token_pair, and the registry was
          already used to resolve the pool. The registry is guaranteed
          to be present in this branch, so a metadata-fetch failure
          surfaces directly.
        """
        if token0_decimals is not None and token1_decimals is not None:
            return token0_decimals, token1_decimals

        if self._pool_reader_registry is None:
            if explicit_pool:
                raise ValueError(
                    f"Cannot derive token decimals for pool_address={pool_address}; "
                    "supply token0_decimals/token1_decimals explicitly or wire a "
                    "pool_reader_registry on the MarketSnapshot."
                )
            # Non-explicit_pool path always has a registry (the caller
            # already used it to resolve pool_address). Defensive guard.
            raise ValueError("No pool reader registry configured for decimals lookup")

        try:
            reader = self._pool_reader_registry.get_reader(chain, protocol)
            md0, md1, _ = reader._get_pool_metadata(pool_address, chain)
        except Exception as e:  # noqa: BLE001
            if explicit_pool:
                raise ValueError(
                    f"Cannot derive token decimals for pool_address={pool_address} "
                    f"on {chain} (protocol={protocol}): {e}. "
                    "Supply token0_decimals/token1_decimals explicitly."
                ) from e
            raise

        return (
            md0 if token0_decimals is None else token0_decimals,
            md1 if token1_decimals is None else token1_decimals,
        )

    def _resolve_token_address(self, token: str, chain: str) -> str | None:
        """Resolve a token symbol/address to a lowercase address (orientation only).

        Address-shaped inputs (``0x`` + 40 hex) pass through; symbols go through
        the registry-backed ``TokenResolver`` (``get_token_resolver()`` — no
        egress, gateway-boundary safe). Returns ``None`` when unresolvable.
        """
        s = token.strip()
        if s.lower().startswith("0x") and len(s) == 42:
            return s.lower()
        try:
            from almanak.framework.data.tokens import get_token_resolver

            # skip_gateway: orientation is a fast static-registry lookup — the
            # canonical pair symbols (WETH/USDC/…) are always registered, and we
            # must not block an EXECUTION_GRADE price on a slow gateway round-trip
            # (or do non-deterministic egress) just to learn token ordering.
            resolved = get_token_resolver().resolve(s, chain, skip_gateway=True, log_errors=False)
        except Exception:  # noqa: BLE001 — unresolvable symbol → orientation unknown
            return None
        addr = getattr(resolved, "address", None)
        return addr.lower() if isinstance(addr, str) and addr else None

    def _orient_to_quote_per_base(
        self,
        envelope: DataEnvelope[AggregatedPrice],
        base: str,
        quote: str,
        chain: str,
        pair_str: str,
    ) -> DataEnvelope[AggregatedPrice]:
        """Re-orient a pool-native price into the requested ``quote/base`` convention.

        Uniswap-family pools sort ``token0 = min(token_addr0, token_addr1)`` and
        the gateway DEX services (``GetDexTwap`` / ``GetDexLwap``) return the
        pool-native ``token1/token0`` price. ``twap()`` / ``lwap()`` promise
        ``quote/base`` (e.g. USDC per WETH for ``"WETH/USDC"``). So:

        - base is token0 (``base_addr < quote_addr``): ``token1/token0`` is
          already ``quote/base`` — return unchanged.
        - base is token1 (``base_addr > quote_addr``): the pool returns
          ``base/quote`` — invert (``1/price``) to yield ``quote/base``.

        Orientation needs both token addresses. When either fails to resolve we
        cannot know the orientation, and returning the raw value would risk a
        confidently-wrong reciprocal on an inverse-ordered pool/chain
        (VIB-4924 B2 — e.g. Ethereum WETH/USDC, where token0 is USDC) — so we
        fail closed with a structured ``PoolPriceUnavailableError``.
        """
        from almanak.framework.data.market_snapshot import PoolPriceUnavailableError

        base_addr = self._resolve_token_address(base, chain)
        quote_addr = self._resolve_token_address(quote, chain)
        if base_addr is None or quote_addr is None or base_addr == quote_addr:
            raise PoolPriceUnavailableError(
                pair_str,
                f"Cannot determine pool token orientation for {pair_str} on {chain}: "
                f"token address resolution failed (base={base_addr}, quote={quote_addr}). "
                "Use tokens registered in the SDK token registry or pass 0x addresses.",
            )
        if base_addr < quote_addr:
            # base is token0 → the pool already returns quote/base.
            return envelope
        return self._invert_price_envelope(envelope, pair_str)

    def _invert_price_envelope(
        self,
        envelope: DataEnvelope[AggregatedPrice],
        pair_str: str,
    ) -> DataEnvelope[AggregatedPrice]:
        """Return a copy of ``envelope`` with aggregate + per-pool prices inverted."""
        import dataclasses

        from almanak.framework.data.market_snapshot import PoolPriceUnavailableError

        aggregated = envelope.value
        if aggregated.price == 0:
            raise PoolPriceUnavailableError(
                pair_str,
                "Pool returned a zero price; cannot invert to the requested orientation",
            )
        one = Decimal(1)
        inverted_sources = [dataclasses.replace(c, price=(one / c.price)) if c.price else c for c in aggregated.sources]
        inverted = dataclasses.replace(aggregated, price=one / aggregated.price, sources=inverted_sources)
        return dataclasses.replace(envelope, value=inverted)

    def twap(
        self,
        token_pair: str | Instrument,
        chain: str | None = None,
        window_seconds: int = 300,
        pool_address: str | None = None,
        protocol: str = "uniswap_v3",
        token0_decimals: int | None = None,
        token1_decimals: int | None = None,
    ) -> DataEnvelope[AggregatedPrice]:
        """Get the time-weighted average price (TWAP) for a token pair.

        Uses the Uniswap V3 oracle's observe() function to compute the TWAP
        over the specified time window. Classification: EXECUTION_GRADE
        (fail-closed, no off-chain fallback).

        Args:
            token_pair: Token pair as "BASE/QUOTE" string or Instrument.
            chain: Chain name. Defaults to this snapshot's chain.
            window_seconds: Time window in seconds (default 300 = 5 min).
            pool_address: Explicit pool address. If None, resolves from pair.
            protocol: Protocol to use (default "uniswap_v3").
            token0_decimals: Decimals for token0. If omitted and
                ``pool_address`` is given, the registry is consulted to
                resolve pool metadata; if neither is available, raises
                ``ValueError``.
            token1_decimals: Decimals for token1. Same fallback as above.

        Returns:
            DataEnvelope[AggregatedPrice] with TWAP price and provenance.

        Raises:
            ValueError: If no price aggregator is configured, or if token
                decimals cannot be derived for an explicit ``pool_address``.
            PoolPriceUnavailableError: If TWAP cannot be calculated.
        """
        from almanak.framework.data.market_snapshot import PoolPriceUnavailableError
        from almanak.framework.data.models import resolve_instrument

        if self._price_aggregator is None:
            raise ValueError("No price aggregator configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()
        inst = resolve_instrument(token_pair, target_chain)
        pair_str = inst.pair
        explicit_pool = pool_address is not None

        try:
            if pool_address is None:
                if self._pool_reader_registry is None:
                    raise ValueError("No pool reader registry configured; provide pool_address explicitly")
                reader = self._pool_reader_registry.get_reader(target_chain, protocol)
                # VIB-4924 C1: resolve the highest-liquidity pool across fee
                # tiers instead of blind-defaulting to fee_tier=3000. On Base the
                # canonical WETH/USDC pool is the 0.05% tier, so a default-3000
                # resolution would feed a thin 0.3% pool into an EXECUTION_GRADE
                # TWAP (a wrong / manipulable price).
                pool_address = reader.resolve_best_pool_address(inst.base, inst.quote, target_chain)
                if pool_address is None:
                    raise PoolPriceUnavailableError(
                        pair_str,
                        f"Cannot resolve pool for {pair_str} on {target_chain} (protocol={protocol})",
                    )

            # VIB-4924 §6.3: aggregators whose price is already human-readable
            # (the gateway GetDexTwap path) declare requires_decimals=False, so
            # skip the extra decimal-resolution eth_calls. Decimal-based
            # aggregators (the direct observe() PriceAggregator) still need them.
            if getattr(self._price_aggregator, "requires_decimals", True):
                token0_decimals, token1_decimals = self._resolve_pool_decimals_for_twap(
                    pool_address=pool_address,
                    chain=target_chain,
                    protocol=protocol,
                    token0_decimals=token0_decimals,
                    token1_decimals=token1_decimals,
                    explicit_pool=explicit_pool,
                )

            envelope = self._price_aggregator.twap(
                pool_address=pool_address,
                chain=target_chain,
                window_seconds=window_seconds,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                protocol=protocol,
            )
            # VIB-4924 B2: the gateway returns the pool-native token1/token0
            # price; re-orient to the requested quote/base convention (inverts
            # on inverse-ordered pools, e.g. Ethereum WETH/USDC where token0 is
            # USDC). No-op when base is token0 (e.g. Base WETH/USDC).
            return self._orient_to_quote_per_base(envelope, inst.base, inst.quote, target_chain, pair_str)
        except PoolPriceUnavailableError:
            raise
        except ValueError:
            raise
        except Exception as e:  # noqa: BLE001
            raise PoolPriceUnavailableError(
                pair_str,
                f"TWAP calculation failed for {pair_str} on {target_chain}: {e}",
            ) from e

    def lwap(
        self,
        token_pair: str | Instrument,
        chain: str | None = None,
        fee_tiers: list[int] | None = None,
        protocols: list[str] | None = None,
    ) -> DataEnvelope[AggregatedPrice]:
        """Get the liquidity-weighted average price (LWAP) for a token pair.

        Reads live prices from all known pools for the pair, filters by
        minimum liquidity, and computes a liquidity-weighted average.

        Args:
            token_pair: Token pair as "BASE/QUOTE" string or Instrument.
            chain: Chain name. Defaults to this snapshot's chain.
            fee_tiers: Fee tiers to search (default: [100, 500, 3000, 10000]).
            protocols: Protocols to search (default: all registered for chain).

        Returns:
            DataEnvelope[AggregatedPrice] with LWAP price and provenance.

        Raises:
            ValueError: If no price aggregator is configured.
            PoolPriceUnavailableError: If LWAP cannot be calculated.
        """
        from almanak.framework.data.market_snapshot import PoolPriceUnavailableError
        from almanak.framework.data.models import resolve_instrument

        if self._price_aggregator is None:
            raise ValueError("No price aggregator configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()
        inst = resolve_instrument(token_pair, target_chain)
        pair_str = inst.pair

        # VIB-4924 F3 / I2: validate explicitly-requested protocols BEFORE
        # delegating. PriceAggregator.lwap swallows an unknown-protocol
        # ValueError (get_reader) and ends in a generic "No pools found", so a
        # known-unsupported protocol could not otherwise surface a specific
        # capability error. Check against the protocols actually supported on
        # THIS chain (a protocol with a reader class but no factory/known-pool
        # for target_chain would pass a global check then die in the generic
        # path), and forward case-normalized names so the downstream exact-match
        # dispatch cannot miss on a mixed-case input.
        # Normalize unconditionally (CodeRabbit): a snapshot with a
        # price_aggregator but no registry must still forward lowercase protocol
        # names so the downstream exact-match dispatch cannot miss on a
        # mixed-case input. The registry-gated block below only adds the
        # supported-on-this-chain capability check.
        normalized_protocols = protocols
        if protocols:
            normalized_protocols = [p.lower() for p in protocols]
            if self._pool_reader_registry is not None:
                supported = {p.lower() for p in self._pool_reader_registry.protocols_for_chain(target_chain)}
                unknown = [
                    orig for orig, low in zip(protocols, normalized_protocols, strict=True) if low not in supported
                ]
                if unknown:
                    raise PoolPriceUnavailableError(
                        pair_str,
                        f"LWAP unsupported protocol(s) {unknown} on {target_chain}; supported: {sorted(supported)}",
                    )

        try:
            envelope = self._price_aggregator.lwap(
                token_a=inst.base,
                token_b=inst.quote,
                chain=target_chain,
                fee_tiers=fee_tiers,
                protocols=normalized_protocols,
            )
            # VIB-4924 B2: re-orient the pool-native token1/token0 price to the
            # requested quote/base convention (same inversion as twap()).
            return self._orient_to_quote_per_base(envelope, inst.base, inst.quote, target_chain, pair_str)
        except PoolPriceUnavailableError:
            raise
        except Exception as e:  # noqa: BLE001
            raise PoolPriceUnavailableError(
                pair_str,
                f"LWAP calculation failed for {pair_str} on {target_chain}: {e}",
            ) from e

    # --- Pool history / depth / slippage -------------------------------------

    def pool_history(
        self,
        pool_address: str,
        chain: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        resolution: str = "1h",
        *,
        protocol: str,
    ) -> DataEnvelope[list[PoolSnapshot]]:
        """Get historical pool state snapshots for backtesting and analytics.

        Args:
            pool_address: Pool contract address.
            chain: Chain name. Defaults to this snapshot's chain.
            start_date: Start of the history window. Defaults to 90 days
                before ``end_date``.
            end_date: End of the history window. Defaults to the snapshot's
                iteration ``timestamp`` (NOT ``datetime.now()``) so backtests,
                paper runs, and historical snapshots stay deterministic and
                never leak future data.
            resolution: "1h" / "4h" / "1d". Default "1h".
            protocol: REQUIRED keyword-only. Protocol slug — e.g.
                ``"uniswap_v3"``, ``"aerodrome"``. NO default — closes the
                silent cross-protocol surface flagged by VIB-4755 Phase 0b
                Round-4: a defaulted ``protocol="uniswap_v3"`` would let a
                caller on a Base Aerodrome pool address get CoinGecko Onchain-
                served Aerodrome data labelled as ``uniswap_v3`` in any
                audit log. Forgetting this kwarg raises ``TypeError`` at
                the framework boundary BEFORE any gateway round-trip. See
                ``docs/internal/uat-cards/VIB-4755.md`` §D-2.

        Returns:
            DataEnvelope[list[PoolSnapshot]] with INFORMATIONAL classification.

        Raises:
            ValueError: If no pool history reader is configured.
            PoolHistoryUnavailableError: If historical data cannot be retrieved.
            TypeError: If ``protocol`` is omitted (Python's missing-required-
                keyword-only signal).
        """
        from datetime import timedelta as _timedelta

        from almanak.framework.data.market_snapshot import PoolHistoryUnavailableError

        if self._pool_history_reader is None:
            raise ValueError("No pool history reader configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()
        # Anchor the default window on the snapshot timestamp so this method
        # is consistent with the rest of the snapshot (price/balance/etc.)
        # and never leaks future data in deterministic replay paths
        # (backtests, paper trading, manually-constructed historical
        # snapshots). See PR #2125 / VIB-4065 follow-up.
        effective_end = end_date if end_date is not None else self._timestamp
        effective_start = start_date if start_date is not None else effective_end - _timedelta(days=90)

        try:
            return self._pool_history_reader.get_pool_history(
                pool_address=pool_address,
                chain=target_chain,
                start_date=effective_start,
                end_date=effective_end,
                resolution=resolution,
                protocol=protocol,
            )
        except Exception as e:  # noqa: BLE001
            raise PoolHistoryUnavailableError(
                pool_address,
                f"Failed to fetch pool history for {pool_address} on {target_chain}: {e}",
            ) from e

    def liquidity_depth(
        self,
        pool_address: str,
        chain: str | None = None,
    ) -> DataEnvelope[LiquidityDepth]:
        """Get tick-level liquidity depth for a concentrated-liquidity pool.

        Args:
            pool_address: Pool contract address.
            chain: Chain name. Defaults to this snapshot's chain.

        Returns:
            DataEnvelope[LiquidityDepth] with tick-level liquidity data.

        Raises:
            ValueError: If no liquidity depth reader is configured.
            LiquidityDepthUnavailableError: If liquidity data cannot be read.
        """
        from almanak.framework.data.market_snapshot import LiquidityDepthUnavailableError

        if self._liquidity_depth_reader is None:
            raise ValueError("No liquidity depth reader configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()
        try:
            return self._liquidity_depth_reader.read_liquidity_depth(
                pool_address=pool_address,
                chain=target_chain,
            )
        except LiquidityDepthUnavailableError:
            raise
        except Exception as e:  # noqa: BLE001
            raise LiquidityDepthUnavailableError(
                pool_address,
                f"Failed to read liquidity depth for {pool_address} on {target_chain}: {e}",
            ) from e

    def estimate_slippage(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
        chain: str | None = None,
        protocol: str | None = None,
    ) -> DataEnvelope[SlippageEstimate]:
        """Estimate price impact and slippage for a potential swap.

        Simulates the swap through tick ranges using actual on-chain liquidity.

        Args:
            token_in: Input token symbol or address.
            token_out: Output token symbol or address.
            amount: Amount of token_in to swap (human-readable units).
            chain: Chain name. Defaults to this snapshot's chain.
            protocol: Protocol name. Auto-detected if None.

        Returns:
            DataEnvelope[SlippageEstimate] with price impact data.

        Raises:
            ValueError: If no slippage estimator is configured.
            SlippageEstimateUnavailableError: If slippage cannot be estimated.
        """
        from almanak.framework.data.market_snapshot import SlippageEstimateUnavailableError

        if self._slippage_estimator is None:
            raise ValueError("No slippage estimator configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()
        try:
            return self._slippage_estimator.estimate_slippage(
                token_in=token_in,
                token_out=token_out,
                amount=amount,
                chain=target_chain,
                protocol=protocol,
            )
        except SlippageEstimateUnavailableError:
            raise
        except Exception as e:  # noqa: BLE001
            raise SlippageEstimateUnavailableError(
                f"{token_in}/{token_out}",
                f"Slippage estimation failed: {e}",
            ) from e

    # --- Pool analytics & yield ----------------------------------------------

    def pool_analytics(
        self,
        pool_address: str,
        chain: str | None = None,
        protocol: str | None = None,
    ) -> DataEnvelope[PoolAnalytics]:
        """Get real-time analytics for a pool (TVL, volume, fee APR/APY).

        Args:
            pool_address: Pool contract address.
            chain: Chain name. Defaults to this snapshot's chain.
            protocol: Optional protocol hint (e.g. "uniswap_v3").

        Returns:
            DataEnvelope[PoolAnalytics] with INFORMATIONAL classification.

        Raises:
            ValueError: If no pool analytics reader is configured.
            PoolAnalyticsUnavailableError: If analytics cannot be retrieved.
        """
        from almanak.framework.data.market_snapshot import PoolAnalyticsUnavailableError

        if self._pool_analytics_reader is None:
            raise ValueError("No pool analytics reader configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()
        try:
            return self._pool_analytics_reader.get_pool_analytics(
                pool_address=pool_address,
                chain=target_chain,
                protocol=protocol,
            )
        except Exception as e:  # noqa: BLE001
            raise PoolAnalyticsUnavailableError(
                pool_address,
                f"Failed to fetch analytics for {pool_address} on {target_chain}: {e}",
            ) from e

    def best_pool(
        self,
        token_a: str,
        token_b: str,
        chain: str | None = None,
        metric: str = "fee_apr",
        protocols: list[str] | None = None,
    ) -> DataEnvelope[PoolAnalyticsResult]:
        """Find the best pool for a token pair based on a metric.

        Args:
            token_a: First token symbol.
            token_b: Second token symbol.
            chain: Chain name. Defaults to this snapshot's chain.
            metric: "fee_apr" / "fee_apy" / "tvl_usd" / "volume_24h_usd".
            protocols: Optional list of protocols to filter by.

        Returns:
            DataEnvelope[PoolAnalyticsResult] with the best pool.

        Raises:
            ValueError: If no pool analytics reader is configured.
            PoolAnalyticsUnavailableError: If no pools found or all providers fail.
        """
        from almanak.framework.data.market_snapshot import PoolAnalyticsUnavailableError

        if self._pool_analytics_reader is None:
            raise ValueError("No pool analytics reader configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()
        try:
            return self._pool_analytics_reader.best_pool(
                token_a=token_a,
                token_b=token_b,
                chain=target_chain,
                metric=metric,
                protocols=protocols,
            )
        except Exception as e:  # noqa: BLE001
            raise PoolAnalyticsUnavailableError(
                f"{token_a}/{token_b}",
                f"Failed to find best pool on {target_chain}: {e}",
            ) from e

    def yield_opportunities(
        self,
        token: str,
        chains: list[str] | None = None,
        min_tvl: float = 100_000,
        sort_by: str = "apy",
    ) -> DataEnvelope[list[YieldOpportunity]]:
        """Find yield opportunities for a token across protocols and chains.

        Args:
            token: Token symbol.
            chains: Optional list of chains to filter. None means all.
            min_tvl: Minimum TVL in USD. Default $100k.
            sort_by: "apy" / "tvl" / "risk_score". Default "apy".

        Returns:
            DataEnvelope[list[YieldOpportunity]] sorted by chosen metric.

        Raises:
            ValueError: If no yield aggregator is configured.
            YieldOpportunitiesUnavailableError: If data cannot be retrieved.
        """
        from almanak.framework.data.market_snapshot import YieldOpportunitiesUnavailableError

        if self._yield_aggregator is None:
            raise ValueError("No yield aggregator configured for MarketSnapshot")

        try:
            return self._yield_aggregator.get_yield_opportunities(
                token=token,
                chains=chains,
                min_tvl=min_tvl,
                sort_by=sort_by,
            )
        except Exception as e:  # noqa: BLE001
            raise YieldOpportunitiesUnavailableError(
                token,
                f"Failed to fetch yield opportunities: {e}",
            ) from e

    # --- Rate history --------------------------------------------------------

    def lending_rate_history(
        self,
        protocol: str,
        token: str,
        chain: str | None = None,
        days: int = 90,
    ) -> DataEnvelope[list[LendingRateSnapshot]]:
        """Get historical lending rate snapshots for backtesting.

        Args:
            protocol: Lending protocol (e.g. "aave_v3").
            token: Token symbol.
            chain: Chain name. Defaults to this snapshot's chain.
            days: Number of days of history. Default 90.

        Returns:
            DataEnvelope[list[LendingRateSnapshot]] sorted ascending.

        Raises:
            ValueError: If no rate history reader is configured.
            LendingRateHistoryUnavailableError: If data cannot be retrieved.
        """
        from almanak.framework.data.market_snapshot import LendingRateHistoryUnavailableError

        if self._rate_history_reader is None:
            raise ValueError("No rate history reader configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()
        try:
            return self._rate_history_reader.get_lending_rate_history(
                protocol=protocol,
                token=token,
                chain=target_chain,
                days=days,
            )
        except Exception as e:  # noqa: BLE001
            raise LendingRateHistoryUnavailableError(
                protocol,
                token,
                f"Failed to fetch lending rate history: {e}",
            ) from e

    def funding_rate_history(
        self,
        venue: str,
        market_symbol: str,
        hours: int = 168,
    ) -> DataEnvelope[list[FundingRateSnapshot]]:
        """Get historical funding rate snapshots for backtesting.

        Args:
            venue: Perps venue (e.g. "hyperliquid").
            market_symbol: Market symbol (e.g. "ETH-USD").
            hours: Number of hours of history. Default 168 (7 days).

        Returns:
            DataEnvelope[list[FundingRateSnapshot]] sorted ascending.

        Raises:
            ValueError: If no rate history reader is configured.
            FundingRateHistoryUnavailableError: If data cannot be retrieved.
        """
        from almanak.framework.data.market_snapshot import FundingRateHistoryUnavailableError

        if self._rate_history_reader is None:
            raise ValueError("No rate history reader configured for MarketSnapshot")

        try:
            return self._rate_history_reader.get_funding_rate_history(
                venue=venue,
                market_symbol=market_symbol,
                hours=hours,
            )
        except Exception as e:  # noqa: BLE001
            raise FundingRateHistoryUnavailableError(
                venue,
                market_symbol,
                f"Failed to fetch funding rate history: {e}",
            ) from e

    # --- Impermanent loss ----------------------------------------------------

    def il_exposure(
        self,
        position_id: str,
        fees_earned: Decimal = Decimal("0"),
    ) -> ILExposure:
        """Get impermanent loss exposure for a tracked LP position.

        Requires an ILCalculator with the position already registered via
        ``add_position()``.

        Args:
            position_id: Unique identifier for the LP position.
            fees_earned: Optional fees earned (for net PnL).

        Returns:
            ILExposure with current IL metrics.

        Raises:
            ValueError: If no IL calculator is configured.
            ILExposureUnavailableError: If exposure cannot be calculated.
        """
        from almanak.framework.data.lp import (
            ILExposureUnavailableError as CalcILExposureError,
        )
        from almanak.framework.data.lp import (
            PositionNotFoundError,
        )
        from almanak.framework.data.market_snapshot import (
            ILExposureUnavailableError,
            PriceUnavailableError,
        )

        if self._il_calculator is None:
            raise ValueError("No IL calculator configured for MarketSnapshot")

        try:
            position = self._il_calculator.get_position(position_id)

            current_price_a: Decimal | None = None
            current_price_b: Decimal | None = None

            if self._price_oracle is not None:
                # Use this snapshot's ``price`` (carries cache + critical-failure
                # plumbing) rather than the raw oracle so behaviour matches
                # the documented API.
                try:
                    current_price_a = self.price(position.token_a)
                except (PriceUnavailableError, ValueError):
                    pass
                try:
                    current_price_b = self.price(position.token_b)
                except (PriceUnavailableError, ValueError):
                    pass

            return self._il_calculator.calculate_il_exposure(
                position_id=position_id,
                current_price_a=current_price_a,
                current_price_b=current_price_b,
                fees_earned=fees_earned,
            )
        except PositionNotFoundError as e:
            raise ILExposureUnavailableError(position_id, f"Position not found: {e}") from e
        except CalcILExposureError as e:
            raise ILExposureUnavailableError(position_id, e.reason) from e
        except Exception as e:  # noqa: BLE001
            raise ILExposureUnavailableError(position_id, f"Unexpected error: {e}") from e

    def projected_il(
        self,
        token_a: str,
        token_b: str,
        price_change_pct: Decimal,
        weight_a: Decimal = Decimal("0.5"),
        weight_b: Decimal = Decimal("0.5"),
    ) -> ProjectedILResult:
        """Project impermanent loss for a hypothetical price change.

        Args:
            token_a: Symbol of token A (the volatile token).
            token_b: Symbol of token B (often a stablecoin).
            price_change_pct: Price change percentage (50 for +50%).
            weight_a: Weight of token A in the pool (default 0.5).
            weight_b: Weight of token B in the pool (default 0.5).

        Returns:
            ProjectedILResult with il_ratio / il_percent / il_bps.

        Raises:
            ValueError: If no IL calculator is configured or invalid args.
        """
        from almanak.framework.data.lp import InvalidPriceError, InvalidWeightError

        if self._il_calculator is None:
            raise ValueError("No IL calculator configured for MarketSnapshot")

        try:
            return self._il_calculator.project_il(
                price_change_pct=price_change_pct,
                weight_a=weight_a,
                weight_b=weight_b,
            )
        except InvalidPriceError as e:
            raise ValueError(f"Invalid price change: {e.reason}") from e
        except InvalidWeightError as e:
            raise ValueError(f"Invalid weights: {e.reason}") from e
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"Failed to project IL: {e}") from e

    # --- Volatility & risk ---------------------------------------------------

    def realized_vol(
        self,
        token: str,
        window_days: int = 30,
        timeframe: str = "1h",
        estimator: str = "close_to_close",
        *,
        ohlcv_limit: int | None = None,
    ) -> DataEnvelope[VolatilityResult]:
        """Calculate realized volatility for a token.

        Args:
            token: Token symbol.
            window_days: Lookback window in calendar days. Default 30.
            timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d). Default "1h".
            estimator: "close_to_close" (default) or "parkinson".
            ohlcv_limit: Override for number of candles to fetch.

        Returns:
            DataEnvelope[VolatilityResult] with INFORMATIONAL classification.

        Raises:
            ValueError: If no volatility calculator is configured.
            VolatilityUnavailableError: If volatility cannot be calculated.
        """
        from almanak.framework.data.market_snapshot import VolatilityUnavailableError
        from almanak.framework.data.models import (
            DataClassification,
            DataEnvelope,
            DataMeta,
        )

        if self._volatility_calculator is None:
            raise ValueError("No volatility calculator configured for MarketSnapshot")

        try:
            candles = self._fetch_candles_for_vol(token, window_days, timeframe, ohlcv_limit)
            result = self._volatility_calculator.realized_vol(
                candles=candles,
                window_days=window_days,
                timeframe=timeframe,
                estimator=estimator,
            )
            meta = DataMeta(
                source="computed",
                observed_at=self._timestamp,
                finality="off_chain",
                confidence=1.0,
                cache_hit=False,
            )
            return DataEnvelope(value=result, meta=meta, classification=DataClassification.INFORMATIONAL)
        except VolatilityUnavailableError:
            raise
        except ValueError:
            # Surface contract / cap errors verbatim — wrapping them as
            # VolatilityUnavailableError would obscure the actionable hint
            # (e.g. "pass ohlcv_limit explicitly", "unsupported timeframe").
            raise
        except Exception as e:  # noqa: BLE001
            raise VolatilityUnavailableError(token, str(e)) from e

    def vol_cone(
        self,
        token: str,
        windows: list[int] | None = None,
        timeframe: str = "1h",
        estimator: str = "close_to_close",
        *,
        ohlcv_limit: int | None = None,
    ) -> DataEnvelope[VolConeResult]:
        """Compute volatility cone: current vol vs historical percentile.

        Args:
            token: Token symbol.
            windows: Lookback windows in days. Default [7, 14, 30, 90].
            timeframe: Candle timeframe. Default "1h".
            estimator: "close_to_close" or "parkinson".
            ohlcv_limit: Override for number of candles to fetch.

        Returns:
            DataEnvelope[VolConeResult] with INFORMATIONAL classification.

        Raises:
            ValueError: If no volatility calculator is configured.
            VolConeUnavailableError: If vol cone cannot be calculated.
        """
        from almanak.framework.data.market_snapshot import VolConeUnavailableError
        from almanak.framework.data.models import (
            DataClassification,
            DataEnvelope,
            DataMeta,
        )

        if self._volatility_calculator is None:
            raise ValueError("No volatility calculator configured for MarketSnapshot")

        if windows is None:
            windows = [7, 14, 30, 90]

        try:
            max_window = max(windows)
            candles = self._fetch_candles_for_vol(token, max_window * 3, timeframe, ohlcv_limit)
            result = self._volatility_calculator.vol_cone(
                candles=candles,
                windows=windows,
                timeframe=timeframe,
                estimator=estimator,
                token=token,
            )
            meta = DataMeta(
                source="computed",
                observed_at=self._timestamp,
                finality="off_chain",
                confidence=1.0,
                cache_hit=False,
            )
            return DataEnvelope(value=result, meta=meta, classification=DataClassification.INFORMATIONAL)
        except VolConeUnavailableError:
            raise
        except ValueError:
            # Surface contract / cap errors verbatim — wrapping them as
            # VolConeUnavailableError would obscure the actionable hint
            # (e.g. "pass ohlcv_limit explicitly", "unsupported timeframe").
            raise
        except Exception as e:  # noqa: BLE001
            raise VolConeUnavailableError(token, str(e)) from e

    def portfolio_risk(
        self,
        pnl_series: list[float],
        total_value_usd: Decimal | None = None,
        return_interval: str = "1d",
        risk_free_rate: Decimal = Decimal("0"),
        var_method: str = "parametric",
        timestamps: list[datetime] | None = None,
        benchmark_eth_returns: list[float] | None = None,
        benchmark_btc_returns: list[float] | None = None,
    ) -> DataEnvelope[PortfolioRisk]:
        """Calculate portfolio risk metrics from a PnL return series.

        Computes Sharpe ratio, Sortino ratio, VaR, CVaR, and drawdown.

        Args:
            pnl_series: Periodic returns as fractions (0.01 = 1% gain).
            total_value_usd: Current portfolio value in USD.
            return_interval: Periodicity of returns (1d, 1h, etc.).
            risk_free_rate: Risk-free rate per period as a decimal.
            var_method: "parametric" / "historical" / "cornish_fisher".
            timestamps: Optional timestamps for each return.
            benchmark_eth_returns: Optional ETH returns for beta.
            benchmark_btc_returns: Optional BTC returns for beta.

        Returns:
            DataEnvelope[PortfolioRisk] with INFORMATIONAL classification.

        Raises:
            ValueError: If no risk calculator is configured.
            PortfolioRiskUnavailableError: If risk metrics cannot be calculated.
        """
        from almanak.framework.data.market_snapshot import PortfolioRiskUnavailableError
        from almanak.framework.data.models import (
            DataClassification,
            DataEnvelope,
            DataMeta,
        )

        if self._risk_calculator is None:
            raise ValueError("No risk calculator configured for MarketSnapshot")

        try:
            from almanak.framework.data.risk.metrics import VaRMethod

            method_map = {
                "parametric": VaRMethod.PARAMETRIC,
                "historical": VaRMethod.HISTORICAL,
                "cornish_fisher": VaRMethod.CORNISH_FISHER,
            }
            vm = method_map.get(var_method)
            if vm is None:
                from almanak.framework.market.errors import PortfolioRiskUnavailableError

                raise PortfolioRiskUnavailableError(
                    f"Unknown var_method '{var_method}'. Use: {list(method_map.keys())}",
                )

            result = self._risk_calculator.portfolio_risk(
                pnl_series=pnl_series,
                total_value_usd=total_value_usd or Decimal("0"),
                return_interval=return_interval,
                risk_free_rate=risk_free_rate,
                var_method=vm,
                timestamps=timestamps,
                benchmark_eth_returns=benchmark_eth_returns,
                benchmark_btc_returns=benchmark_btc_returns,
            )
            meta = DataMeta(
                source="computed",
                observed_at=self._timestamp,
                finality="off_chain",
                confidence=1.0,
                cache_hit=False,
            )
            return DataEnvelope(value=result, meta=meta, classification=DataClassification.INFORMATIONAL)
        except PortfolioRiskUnavailableError:
            raise
        except ValueError:
            raise
        except Exception as e:  # noqa: BLE001
            raise PortfolioRiskUnavailableError(str(e)) from e

    def rolling_sharpe(
        self,
        pnl_series: list[float],
        window_days: int = 30,
        return_interval: str = "1d",
        risk_free_rate: Decimal = Decimal("0"),
        timestamps: list[datetime] | None = None,
    ) -> DataEnvelope[RollingSharpeResult]:
        """Compute rolling Sharpe ratio over a PnL series.

        Args:
            pnl_series: Periodic returns as fractions.
            window_days: Rolling window in days. Default 30.
            return_interval: Periodicity of returns.
            risk_free_rate: Risk-free rate per period.
            timestamps: Optional timestamps aligned with pnl_series.

        Returns:
            DataEnvelope[RollingSharpeResult] with INFORMATIONAL classification.

        Raises:
            ValueError: If no risk calculator is configured.
            RollingSharpeUnavailableError: If rolling Sharpe cannot be computed.
        """
        from almanak.framework.data.market_snapshot import RollingSharpeUnavailableError
        from almanak.framework.data.models import (
            DataClassification,
            DataEnvelope,
            DataMeta,
        )

        if self._risk_calculator is None:
            raise ValueError("No risk calculator configured for MarketSnapshot")

        try:
            result = self._risk_calculator.rolling_sharpe(
                pnl_series=pnl_series,
                window_days=window_days,
                return_interval=return_interval,
                risk_free_rate=risk_free_rate,
                timestamps=timestamps,
            )
            meta = DataMeta(
                source="computed",
                observed_at=self._timestamp,
                finality="off_chain",
                confidence=1.0,
                cache_hit=False,
            )
            return DataEnvelope(value=result, meta=meta, classification=DataClassification.INFORMATIONAL)
        except RollingSharpeUnavailableError:
            raise
        except Exception as e:  # noqa: BLE001
            raise RollingSharpeUnavailableError(str(e)) from e

    def _fetch_candles_for_vol(
        self,
        token: str,
        window_days: int,
        timeframe: str,
        ohlcv_limit: int | None,
    ) -> list:
        """Fetch OHLCV candles for volatility calculations.

        Internal helper used by ``realized_vol`` / ``vol_cone``.
        """
        from almanak.framework.data.interfaces import OHLCVCandle
        from almanak.framework.data.market_snapshot import VolatilityUnavailableError

        hours_per_candle = {
            "1m": 1 / 60,
            "5m": 5 / 60,
            "15m": 0.25,
            "1h": 1.0,
            "4h": 4.0,
            "1d": 24.0,
        }
        if timeframe not in hours_per_candle:
            raise ValueError(f"Unsupported timeframe '{timeframe}'")

        # Validate explicit ``ohlcv_limit`` strictly: ``0`` and negatives are not
        # "use the default" — they are caller bugs that would otherwise propagate
        # silently as empty fetches or as ``MAX_VOL_CANDLE_LIMIT`` checks bypassed.
        if ohlcv_limit is not None:
            if ohlcv_limit <= 0:
                raise ValueError(
                    f"ohlcv_limit must be > 0 (got {ohlcv_limit!r}) for "
                    f"token={token!r} timeframe={timeframe!r} window_days={window_days}"
                )
            limit = ohlcv_limit
        else:
            limit = max(int(window_days * 24 / hours_per_candle[timeframe]), 100)

        # Guard against runaway implicit fetches at sub-hourly resolutions
        # (see ``_MAX_VOL_CANDLE_LIMIT`` rationale). Explicit ``ohlcv_limit``
        # bypasses the cap — the caller has measured the cost of the call.
        if ohlcv_limit is None and limit > _MAX_VOL_CANDLE_LIMIT:
            raise ValueError(
                f"Volatility request for token={token!r} with timeframe={timeframe!r} "
                f"and window_days={window_days} would fetch {limit} candles, exceeding "
                f"the safe implicit cap of {_MAX_VOL_CANDLE_LIMIT}. Pass ``ohlcv_limit`` "
                f"explicitly to opt in, or use a coarser timeframe."
            )

        df = self.ohlcv(token, timeframe=timeframe, limit=limit)

        if df.empty:
            raise VolatilityUnavailableError(token, "No OHLCV data available")

        candles = []
        for _, row in df.iterrows():
            candles.append(
                OHLCVCandle(
                    timestamp=row["timestamp"],
                    open=Decimal(str(row["open"])),
                    high=Decimal(str(row["high"])),
                    low=Decimal(str(row["low"])),
                    close=Decimal(str(row["close"])),
                    volume=(
                        Decimal(str(row["volume"]))
                        if not (hasattr(row["volume"], "__float__") and str(row["volume"]) == "nan")
                        else None
                    ),
                )
            )
        return candles

    # --- OHLCV / gas / batch helpers / health --------------------------------

    def ohlcv(
        self,
        token: str | Instrument,
        timeframe: str = "1h",
        limit: int = 100,
        quote: str = "USD",
        gap_strategy: GapStrategy = "nan",
        *,
        pool_address: str | None = None,
    ) -> pd.DataFrame:
        """Get OHLCV (candlestick) data for a token.

        Routes through ``ohlcv_router`` (preferred) or legacy ``ohlcv_module``.

        Args:
            token: Token symbol, "BASE/QUOTE" string, or Instrument.
            timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d). Default "1h".
            limit: Maximum number of candles. Default 100.
            quote: Quote currency. Default "USD".
            gap_strategy: "nan" / "ffill" / "drop". Default "nan".
            pool_address: Explicit pool address for DEX providers.

        Returns:
            pandas DataFrame with columns timestamp, open, high, low, close, volume.

        Raises:
            ValueError: If no OHLCV module/router is configured.
            OHLCVUnavailableError: If OHLCV data cannot be retrieved.
        """
        token_str = token if isinstance(token, str) else token.pair

        if self._ohlcv_router is not None:
            envelope = self._fetch_ohlcv_via_router(token, timeframe, limit, pool_address, quote, token_str)
            return self._envelope_to_ohlcv_df(envelope, token, token_str, quote, timeframe, gap_strategy)

        if self._ohlcv_module is None:
            raise ValueError("No OHLCV module or router configured for MarketSnapshot")

        # The legacy ``OHLCVModule.get_ohlcv`` is strictly token-scoped (CEX
        # tape via ``OHLCVProvider``); it has no ``pool_address`` parameter.
        # Silently dropping ``pool_address`` would let a pool-scoped call
        # appear to succeed while returning candles for a different market —
        # the worst-class failure for an indicator-driven strategy. Fail
        # loudly so the caller wires the OHLCV router instead.
        if pool_address is not None:
            raise ValueError(
                "pool_address requires an OHLCV router (DEX-aware path); the "
                "legacy ohlcv_module is token-scoped only and cannot fetch "
                "pool-scoped candles. Wire ohlcv_router= on MarketSnapshot."
            )

        return self._fetch_ohlcv_legacy(token, timeframe, limit, quote, gap_strategy)

    def _fetch_ohlcv_via_router(
        self,
        token: str | Instrument,
        timeframe: str,
        limit: int,
        pool_address: str | None,
        quote: str,
        token_str: str,
    ) -> Any:
        """Router-backed OHLCV fetch with the documented error contract.

        Precondition: ``self._ohlcv_router`` is non-None (the public ``ohlcv``
        method gates this branch).
        """
        from almanak.framework.data.interfaces import DataSourceError
        from almanak.framework.data.market_snapshot import OHLCVUnavailableError

        assert self._ohlcv_router is not None
        try:
            return self._ohlcv_router.get_ohlcv(
                token=token,
                chain=self._chain,
                timeframe=timeframe,
                limit=limit,
                pool_address=pool_address,
                quote=quote,
            )
        except DataSourceError as e:
            raise OHLCVUnavailableError(token_str, str(e)) from e
        except Exception as e:  # noqa: BLE001
            raise OHLCVUnavailableError(token_str, f"Unexpected error: {e}") from e

    def _envelope_to_ohlcv_df(
        self,
        envelope: Any,
        token: str | Instrument,
        token_str: str,
        quote: str,
        timeframe: str,
        gap_strategy: GapStrategy,
    ) -> pd.DataFrame:
        """Materialize a router envelope into the documented DataFrame shape."""
        import pandas as pd

        candles = envelope.value
        attrs = {
            "base": _derive_ohlcv_base_symbol(token, token_str),
            "quote": quote,
            "timeframe": timeframe,
            "source": envelope.meta.source,
            "chain": self._chain,
            "fetched_at": datetime.now(UTC).isoformat(),
            "confidence": envelope.meta.confidence,
        }
        if not candles:
            df = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
            df.attrs = {**attrs, "base": token_str}
            return df

        df = pd.DataFrame(_ohlcv_candles_to_rows(candles))
        if gap_strategy == "ffill":
            df = df.ffill()
        elif gap_strategy == "drop":
            df = df.dropna()
        df.attrs = attrs
        return df

    def _fetch_ohlcv_legacy(
        self,
        token: str | Instrument,
        timeframe: str,
        limit: int,
        quote: str,
        gap_strategy: GapStrategy,
    ) -> pd.DataFrame:
        """Legacy ``OHLCVModule`` token-scoped fetch with the documented error contract.

        Precondition: ``self._ohlcv_module`` is non-None (the public ``ohlcv``
        method gates this branch).
        """
        from almanak.framework.data.interfaces import DataSourceError
        from almanak.framework.data.market_snapshot import OHLCVUnavailableError

        assert self._ohlcv_module is not None
        legacy_token = token if isinstance(token, str) else token.base
        try:
            return self._ohlcv_module.get_ohlcv(
                token=legacy_token,
                timeframe=timeframe,
                limit=limit,
                quote=quote,
                gap_strategy=gap_strategy,
            )
        except ValueError:
            raise
        except DataSourceError as e:
            raise OHLCVUnavailableError(legacy_token, str(e)) from e
        except Exception as e:  # noqa: BLE001
            raise OHLCVUnavailableError(legacy_token, f"Unexpected error: {e}") from e

    # and ``balances(tokens)`` documented in SKILL.md exist on the deprecated
    # data-layer ``MarketSnapshot`` (paper / backtest) but are deliberately NOT
    # lifted onto the canonical class in this PR. They collide with legacy
    # ``hasattr(market, "prices")`` / ``market.prices.get(...)`` patterns in
    # ``almanak/framework/runner/runner_state.py:371`` and a handful of tests
    # that historically relied on the canonical class NOT having these names —
    # the absence was load-bearing. Lifting them safely requires updating those
    # callers in lockstep, which belongs in the Phase 2 consolidation
    # (VIB-4065 / GH#2126). Use ``price(symbol)`` and ``balance(symbol)`` per
    # token until then.

    def health(self) -> HealthReport:
        """Get a health report for all registered data providers.

        Returns:
            HealthReport with per-source health, cache stats, overall status.
        """
        from almanak.framework.data.health import (
            CacheStats,
            HealthReport,
            SourceHealth,
        )

        sources: dict[str, SourceHealth] = {}
        now = datetime.now(UTC)

        def _add(name: str, present: bool) -> None:
            if not present:
                return
            sources[name] = SourceHealth(
                name=name,
                success_rate=1.0,
                latency_p50_ms=0.0,
                latency_p95_ms=0.0,
                error_count=0,
                last_success=now,
            )

        _add("price_oracle", self._price_oracle is not None)
        _add("balance_provider", self._balance_provider is not None)
        _add("rsi_provider", self._rsi_provider is not None)
        _add("indicator_provider", self._indicator_provider is not None)
        _add("multi_dex_service", self._multi_dex_service is not None)
        _add("rate_monitor", self._rate_monitor is not None)
        _add("funding_rate_provider", self._funding_rate_provider is not None)
        _add("gateway_client", self._gateway_client is not None)
        _add("pool_reader_registry", self._pool_reader_registry is not None)
        _add("pool_reader", self._pool_reader is not None)
        _add("price_aggregator", self._price_aggregator is not None)
        _add("ohlcv_router", self._ohlcv_router is not None)
        _add("ohlcv_module", self._ohlcv_module is not None)
        _add("gas_oracle", self._gas_oracle is not None)
        _add("pool_history_reader", self._pool_history_reader is not None)
        _add("liquidity_depth_reader", self._liquidity_depth_reader is not None)
        _add("slippage_estimator", self._slippage_estimator is not None)
        _add("pool_analytics_reader", self._pool_analytics_reader is not None)
        _add("yield_aggregator", self._yield_aggregator is not None)
        _add("il_calculator", self._il_calculator is not None)
        _add("volatility_calculator", self._volatility_calculator is not None)
        _add("risk_calculator", self._risk_calculator is not None)
        _add("rate_history_reader", self._rate_history_reader is not None)
        _add("solana_lst_provider", self._solana_lst_provider is not None)

        total_cache_size = (
            len(self._price_cache)
            + len(self._balance_cache)
            + len(self._rsi_cache)
            + len(self._lending_rate_cache)
            + len(self._position_health_cache)
        )

        return HealthReport(
            timestamp=now,
            sources=sources,
            cache_stats=CacheStats(hits=0, misses=0, size=total_cache_size, max_size=None),
            overall_status=HealthReport.calculate_overall_status(sources),
        )

    # --- Prediction markets --------------------------------------------------

    def prediction(self, market_id: str) -> PredictionMarket:
        """Get prediction market data.

        Args:
            market_id: Prediction market ID or URL slug.

        Returns:
            PredictionMarket with full market details.

        Raises:
            ValueError: If no prediction provider is configured.
            PredictionUnavailableError: If market data cannot be retrieved.
        """
        from almanak.framework.data.market_snapshot import PredictionUnavailableError

        if self._prediction_provider is None:
            raise ValueError("No prediction provider configured for MarketSnapshot")

        try:
            return self._prediction_provider.get_market(market_id)
        except Exception as e:  # noqa: BLE001
            raise PredictionUnavailableError(market_id, str(e)) from e

    def prediction_positions(
        self,
        market_id: str | None = None,
    ) -> list[PredictionPosition]:
        """Get all open prediction market positions.

        Args:
            market_id: Optional market ID or slug to filter by.

        Returns:
            List of PredictionPosition objects.

        Raises:
            ValueError: If no prediction provider is configured.
            PredictionUnavailableError: If positions cannot be retrieved.
        """
        from almanak.framework.data.market_snapshot import PredictionUnavailableError

        if self._prediction_provider is None:
            raise ValueError("No prediction provider configured for MarketSnapshot")

        try:
            if market_id:
                market = self._prediction_provider.get_market(market_id)
                return self._prediction_provider.get_positions(
                    wallet=self._wallet_address,
                    market_id=market.market_id,
                )
            return self._prediction_provider.get_positions(wallet=self._wallet_address)
        except Exception as e:  # noqa: BLE001
            raise PredictionUnavailableError(market_id or "all", f"Failed to get positions: {e}") from e

    def prediction_orders(
        self,
        market_id: str | None = None,
    ) -> list[PredictionOrder]:
        """Get all open prediction market orders.

        Args:
            market_id: Optional market ID or slug to filter by.

        Returns:
            List of PredictionOrder objects.

        Raises:
            ValueError: If no prediction provider is configured.
            PredictionUnavailableError: If orders cannot be retrieved.
        """
        from almanak.framework.data.market_snapshot import PredictionUnavailableError

        if self._prediction_provider is None:
            raise ValueError("No prediction provider configured for MarketSnapshot")

        try:
            return self._prediction_provider.get_open_orders(market_id)
        except Exception as e:  # noqa: BLE001
            raise PredictionUnavailableError(market_id or "all", f"Failed to get orders: {e}") from e

    # --- Solana LSTs ---------------------------------------------------------

    def lst_exchange_rate(self, symbol: str) -> LSTExchangeRate:
        """Get Solana LST exchange rate vs SOL.

        Args:
            symbol: LST symbol (e.g. "jitoSOL", "mSOL", "bSOL", "INF").

        Returns:
            LSTExchangeRate with rate vs SOL and APY data.

        Raises:
            ValueError: If no LST provider is configured.
            LSTDataUnavailableError: If data cannot be retrieved.
        """
        from almanak.framework.data.market_snapshot import LSTDataUnavailableError

        if self._solana_lst_provider is None:
            raise ValueError("No Solana LST provider configured for MarketSnapshot")

        try:
            return self._run_async_bridged(self._solana_lst_provider.get_exchange_rate(symbol))
        except ValueError:
            raise
        except Exception as e:  # noqa: BLE001
            raise LSTDataUnavailableError(symbol, f"Failed to fetch LST exchange rate: {e}") from e

    def lst_all_rates(self) -> dict[str, LSTExchangeRate]:
        """Get exchange rates for all tracked Solana LSTs.

        Returns:
            dict mapping symbol -> LSTExchangeRate.

        Raises:
            ValueError: If no LST provider is configured.
            LSTDataUnavailableError: If data cannot be retrieved.
        """
        from almanak.framework.data.market_snapshot import LSTDataUnavailableError

        if self._solana_lst_provider is None:
            raise ValueError("No Solana LST provider configured for MarketSnapshot")

        try:
            return self._run_async_bridged(self._solana_lst_provider.get_all_rates())
        except Exception as e:  # noqa: BLE001
            raise LSTDataUnavailableError("all", f"Failed to fetch LST rates: {e}") from e

    # --- PT-collateral position health ---------------------------------------

    def pt_position_health(
        self,
        morpho_market_id: str,
        principal_token_market_address: str | None = None,
        rpc_url: str | None = None,
        collateral_price_usd: Decimal | None = None,
        debt_price_usd: Decimal | None = None,
        *,
        principal_token_protocol: str | None = None,
        pendle_market_address: str | None = None,
    ) -> PTPositionHealth:
        """Get extended health data for a PT-collateral position.

        Combines Morpho Blue position data with principal-token market metrics
        (implied APY, maturity risk) for comprehensive risk assessment.

        Args:
            morpho_market_id: Morpho Blue market ID.
            principal_token_market_address: Principal-token market address for
                the PT collateral.
            rpc_url: RPC endpoint (uses gateway-routed path when None).
            collateral_price_usd: Override for PT collateral price.
            debt_price_usd: Override for debt token price.
            principal_token_protocol: Optional connector protocol key. When
                omitted, the sole registered principal-token reader is used for
                backward compatibility.
            pendle_market_address: Deprecated alias for
                ``principal_token_market_address``.

        Returns:
            PTPositionHealth with Morpho + principal-token risk metrics.

        Raises:
            HealthUnavailableError: If health data cannot be retrieved or
                if neither a connected ``GatewayClient`` nor an explicit
                ``rpc_url`` is available (the provider has no transport
                otherwise — fail fast with an actionable contract error
                instead of letting a downstream provider call surface a
                less specific exception).
        """
        from almanak.framework.data.market_snapshot import HealthUnavailableError
        from almanak.framework.data.position_health import PositionHealthProvider

        # Fail fast when no transport is available: PositionHealthProvider
        # needs either an explicit rpc_url or a connected GatewayClient to
        # issue on-chain reads. Constructing it with ``rpc_url=""`` and a
        # missing/disconnected gateway just produces a less specific
        # downstream provider error at call time.
        gateway_connected = self._gateway_client is not None and getattr(self._gateway_client, "is_connected", False)
        if not rpc_url and not gateway_connected:
            raise HealthUnavailableError(
                "pt_position_health requires either a connected GatewayClient "
                "or an explicit rpc_url; neither was available on this "
                "MarketSnapshot. Wire the gateway client or pass rpc_url=..."
            )

        provider = PositionHealthProvider(
            rpc_url=rpc_url or "",
            chain=self._chain,
            price_oracle=self._price_oracle,
            gateway_client=self._gateway_client,
        )
        try:
            return provider.get_pt_position_health(
                morpho_market_id=morpho_market_id,
                principal_token_market_address=principal_token_market_address or pendle_market_address,
                user_address=self._wallet_address,
                collateral_price_usd=collateral_price_usd,
                debt_price_usd=debt_price_usd,
                principal_token_protocol=principal_token_protocol,
            )
        except HealthUnavailableError:
            raise
        except Exception as e:  # noqa: BLE001
            raise HealthUnavailableError(f"PT position health unavailable: {e}") from e

    def to_dict(self) -> dict[str, Any]:
        """Convert snapshot to dictionary."""
        return {
            "chain": self._chain,
            "wallet_address": self._wallet_address,
            "timestamp": self._timestamp.isoformat(),
            "prices": {k: str(v) for k, v in self._prices.items()},
            "balances": {
                k: {
                    "symbol": v.symbol,
                    "balance": str(v.balance),
                    "balance_usd": str(v.balance_usd),
                }
                for k, v in self._balances.items()
            },
            "rsi_values": {
                k: {"value": str(data.value), "period": data.period, "timeframe": tf}
                for k, (data, tf) in self._rsi_values.items()
            },
        }

    # =========================================================================
    # VIB-4062 — Public seed API (PRD §4.6)
    #
    # The ``seed_*`` methods are the canonical, validated, public path for
    # populating the snapshot in unit tests. The legacy ``set_*`` methods are
    # preserved as aliases until commit 6 of the migration. After commit 6
    # callers must use ``seed_*``; the codemod (commit 4) rewrites them
    # automatically.
    # =========================================================================

    def seed_price(self, token: str, price: Decimal | float | int) -> None:
        """Seed a scalar price into the snapshot's cache.

        Goes through the same internal chokepoint as ``set_price`` so the
        validation behavior is identical.
        """
        self.set_price(token, Decimal(price) if not isinstance(price, Decimal) else price)

    def seed_price_data(
        self,
        token: str,
        data: PriceData,
        *,
        quote: str = "USD",
        chain: str | None = None,
    ) -> None:
        """Seed a full ``PriceData`` record (with source / timestamp / block).

        ``chain`` is keyword-only and lets multi-chain tests / backtests seed
        different ``PriceData`` for the same symbol on different chains —
        matches the legacy multichain ``set_price_data`` shape.
        """
        self.set_price_data(token, data, quote=quote, chain=chain)

    def seed_balance(self, token: str, balance: TokenBalance) -> None:
        """Seed a ``TokenBalance`` for a token."""
        self.set_balance(token, balance)

    def seed_rsi(
        self,
        token: str,
        data: RSIData,
        timeframe: str | None = None,
    ) -> None:
        """Seed an ``RSIData`` instance (timeframe-keyed)."""
        self.set_rsi(token, data, timeframe=timeframe)

    def seed_macd(
        self,
        token: str,
        data: MACDData,
        timeframe: str | None = None,
    ) -> None:
        self.set_macd(token, data, timeframe=timeframe)

    def seed_bollinger_bands(
        self,
        token: str,
        data: BollingerBandsData,
        timeframe: str | None = None,
    ) -> None:
        self.set_bollinger_bands(token, data, timeframe=timeframe)

    def seed_lending_rate(self, protocol: str, token: str, side: str, rate: Any) -> None:
        self.set_lending_rate(protocol, token, side, rate)

    def seed_position_health(self, protocol: str, market_id: str, health: Any) -> None:
        self.set_position_health(protocol, market_id, health)
