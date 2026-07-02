"""PnL Backtesting Engine - Historical simulation for strategy evaluation.

This module provides the main PnLBacktester engine that orchestrates historical
simulation of trading strategies. It uses historical price data to evaluate
strategy performance without executing real transactions.

Key Components:
    - PnLBacktester: Main backtesting engine class
    - BacktestableStrategy: Protocol for strategies compatible with backtesting

Examples:
    Basic usage with default settings:

        from almanak.framework.backtesting.pnl import PnLBacktester, PnLBacktestConfig
        from almanak.framework.backtesting.pnl.providers import CoinGeckoDataProvider

        # Create data provider
        data_provider = CoinGeckoDataProvider()

        # Create fee/slippage models
        fee_models = {"default": DefaultFeeModel()}
        slippage_models = {"default": DefaultSlippageModel()}

        # Create backtester
        backtester = PnLBacktester(data_provider, fee_models, slippage_models)

        # Run backtest
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            token_funding=[...],
        )
        result = await backtester.backtest(strategy, config)
        print(result.summary())

    Institutional mode for production-grade compliance:

        # Institutional mode enforces strict data quality, reproducibility,
        # and compliance requirements for institutional trading operations.
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            token_funding=[...],
            institutional_mode=True,  # Enables strict requirements
            random_seed=42,  # Required for reproducibility
        )
        result = await backtester.backtest(strategy, config)

        # Check institutional compliance
        if result.institutional_compliance:
            print("Backtest meets institutional standards")
        else:
            print(f"Compliance violations: {result.compliance_violations}")

        # Access data quality metrics
        if result.data_quality:
            print(f"Data coverage: {result.data_quality.coverage_ratio:.1%}")
            print(f"Sources used: {result.data_quality.source_breakdown}")
"""

import logging
import uuid
from dataclasses import dataclass

# Note: There's a naming conflict between fee_models.py (module) and fee_models/ (package)
# Python prefers the package, so we need to use an alternative import approach
# We define the protocols inline and create simple default implementations
from dataclasses import dataclass as _dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

from almanak.core.chains import DEFAULT_CHAIN, ChainRegistry
from almanak.core.chains._helpers import native_symbols_for
from almanak.framework.backtesting.adapters.base import StrategyBacktestAdapter

# Import adapter registry for strategy type detection
from almanak.framework.backtesting.adapters.registry import (
    StrategyTypeHint,
    detect_strategy_type,
    get_adapter_for_strategy_with_config,
)
from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.exceptions import DataSourceUnavailableError, UnsupportedIntentError
from almanak.framework.backtesting.models import (
    BacktestMetrics,
    BacktestResult,
    GasPriceRecord,
    GasPriceSummary,
    IntentType,
    ParameterSource,
    ParameterSourceTracker,
    PreflightCheckResult,
    PreflightReport,
    TradeRecord,
)

# Phase helpers extracted from _run_backtest (Phase 6C.2).
from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import (
    HistoricalDataCapability,
    HistoricalDataProvider,
    MarketState,
    TokenRef,
    is_address_like,
    normalize_token_ref,
    token_ref_display,
)
from almanak.framework.backtesting.pnl.data_quality import DataQualityTracker
from almanak.framework.backtesting.pnl.error_handling import (
    BacktestErrorHandler,
)
from almanak.framework.backtesting.pnl.indicator_engine import BacktestIndicatorEngine
from almanak.framework.backtesting.pnl.logging_utils import (
    BacktestLogger,
    log_trade_execution,
)
from almanak.framework.backtesting.pnl.mev_simulator import (
    MEVSimulator,
    MEVSimulatorConfig,
)
from almanak.framework.backtesting.pnl.portfolio import (
    CASH_EQUIVALENT_STABLECOINS,
    SimulatedFill,
    SimulatedPortfolio,
    SimulatedPosition,
)
from almanak.framework.backtesting.pnl.providers.gas import GasPrice, GasPriceProvider
from almanak.framework.backtesting.pnl.simulated_result import (
    SimulatedExecutionResult,
    build_simulated_result,
)

# Import strategy-related types
from almanak.framework.market import MarketSnapshot, TokenBalance

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _TokenAvailabilityConfig:
    use_membership: bool
    resolution_based: bool
    membership_upper: frozenset[str]


@dataclass(frozen=True)
class _ProviderCapabilityPreflight:
    check: PreflightCheckResult
    capabilities: dict[str, str]
    recommendations: list[str]


@dataclass(frozen=True)
class _ArchiveAccessPreflight:
    check: PreflightCheckResult | None
    accessible: bool | None
    recommendations: list[str]


@dataclass(frozen=True)
class _TimeRangePreflight:
    check: PreflightCheckResult
    recommendations: list[str]


# =============================================================================
# Fee and Slippage Model Protocols (duplicated due to import conflict)
# =============================================================================
# Note: These are duplicated from fee_models.py because there's a naming
# conflict with the fee_models/ package. Python prefers packages over modules.


@runtime_checkable
class FeeModel(Protocol):
    """Protocol for calculating protocol/exchange fees."""

    def calculate_fee(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate the fee for an action."""
        ...

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        ...


@runtime_checkable
class SlippageModel(Protocol):
    """Protocol for estimating trade slippage."""

    def calculate_slippage(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate the slippage for an action."""
        ...

    @property
    def model_name(self) -> str:
        """Return the unique name of this slippage model."""
        ...


# Zero-fee intent types
_ZERO_FEE_INTENTS: frozenset[IntentType] = frozenset(
    {
        IntentType.HOLD,
    }
)

# Zero-slippage intent types
_ZERO_SLIPPAGE_INTENTS: frozenset[IntentType] = frozenset(
    {
        IntentType.HOLD,
        IntentType.SUPPLY,
        IntentType.WITHDRAW,
        IntentType.REPAY,
        IntentType.BORROW,
        IntentType.VAULT_DEPOSIT,
        IntentType.VAULT_REDEEM,
    }
)


@_dataclass
class DefaultFeeModel:
    """Default fee model with configurable percentage-based fees."""

    fee_pct: Decimal = Decimal("0.003")  # 0.3% default

    def calculate_fee(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate fee using configured percentages."""
        if intent_type in _ZERO_FEE_INTENTS:
            return Decimal("0")
        return amount_usd * self.fee_pct

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        return "default"


@_dataclass
class DefaultSlippageModel:
    """Default slippage model with configurable percentage-based slippage."""

    slippage_pct: Decimal = Decimal("0.001")  # 0.1% base
    max_slippage_pct: Decimal = Decimal("0.05")  # 5% max

    def calculate_slippage(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate slippage."""
        if intent_type in _ZERO_SLIPPAGE_INTENTS:
            return Decimal("0")
        return min(self.slippage_pct, self.max_slippage_pct)

    @property
    def model_name(self) -> str:
        """Return the unique name of this slippage model."""
        return "default"


@_dataclass
class LinearImpactSlippageModel:
    """Depth-aware slippage model with linear market impact scaling.

    Models slippage as a fixed base spread plus a market-impact term that
    grows linearly with trade size. This captures the intuition that large
    trades move the price more than small ones.

    Formula (in basis points)::

        slippage_bps = base_bps + impact_bps_per_million * (amount_usd / 1_000_000)
        slippage_pct = min(slippage_bps / 10_000, max_slippage_pct)

    Default parameters model a mid-tier DEX (e.g., Uniswap V3 on Arbitrum):
    - ``base_bps = 10`` — 0.10% fixed spread (bid-ask + rounding)
    - ``impact_bps_per_million = 5`` — 0.05% extra per $1 M notional
    - ``max_slippage_pct = 0.05`` — hard cap at 5%

    A $100 k trade incurs ~0.105%; a $1 M trade incurs ~0.15%; a $10 M trade
    incurs ~0.60% (still below the 5% cap).

    Per-protocol calibration examples:

    .. code-block:: python

        # Uniswap V3 ETH/USDC 0.05% pool (deep, low impact)
        slippage_models = {
            "uniswap_v3": LinearImpactSlippageModel(
                base_bps=Decimal("5"),
                impact_bps_per_million=Decimal("2"),
            ),
            # Smaller mid-cap DEX (shallower liquidity)
            "traderjoe_v2": LinearImpactSlippageModel(
                base_bps=Decimal("20"),
                impact_bps_per_million=Decimal("15"),
            ),
            "default": LinearImpactSlippageModel(),
        }

    Attributes:
        base_bps: Fixed slippage component in basis points (1 bps = 0.01%).
            Represents the bid-ask spread and routing overhead.
        impact_bps_per_million: Market-impact coefficient in bps per $1 M.
            Controls how steeply slippage grows with trade size.
        max_slippage_pct: Hard cap on slippage as a fraction (e.g., 0.05 = 5%).
    """

    base_bps: Decimal = Decimal("10")  # 0.10% base spread
    impact_bps_per_million: Decimal = Decimal("5")  # 0.05% per $1 M
    max_slippage_pct: Decimal = Decimal("0.05")  # 5% hard cap

    _BPS_DIVISOR: Decimal = Decimal("10000")
    _MILLION: Decimal = Decimal("1000000")

    def __post_init__(self) -> None:
        """Validate model parameters on initialization."""
        if self.base_bps < 0:
            raise ValueError("base_bps cannot be negative.")
        if self.impact_bps_per_million < 0:
            raise ValueError("impact_bps_per_million cannot be negative.")
        if not (0 <= self.max_slippage_pct <= 1):
            raise ValueError("max_slippage_pct must be a fraction between 0 and 1.")

    def calculate_slippage(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate depth-aware slippage.

        Returns zero for intent types that don't incur price impact
        (e.g., HOLD, SUPPLY, WITHDRAW). For all other intents, applies
        the linear impact formula.

        Args:
            intent_type: Type of intent being executed.
            amount_usd: Trade notional in USD.
            market_state: Current market data (unused by this model but
                required by the SlippageModel protocol).
            protocol: Protocol name (unused; use per-protocol model keys instead).
            **kwargs: Ignored extra arguments.

        Returns:
            Slippage as a fraction (e.g., 0.001 = 0.1%).
        """
        if intent_type in _ZERO_SLIPPAGE_INTENTS:
            return Decimal("0")

        # Guard against negative amounts (shouldn't happen, but be defensive)
        safe_amount = max(amount_usd, Decimal("0"))
        impact_bps = self.impact_bps_per_million * (safe_amount / self._MILLION)
        total_bps = self.base_bps + impact_bps
        slippage_pct = total_bps / self._BPS_DIVISOR
        return min(slippage_pct, self.max_slippage_pct)

    @property
    def model_name(self) -> str:
        """Return the unique name of this slippage model."""
        return "linear_impact"


# =============================================================================
# Protocol for Backtest-Compatible Strategies
# =============================================================================


@runtime_checkable
class BacktestableStrategy(Protocol):
    """Protocol defining the interface for strategies that can be backtested.

    Strategies must implement:
    - deployment_id: Unique identifier for the strategy
    - decide(market): Method that returns an intent based on market data

    The decide method can return:
    - An Intent object (SwapIntent, LPIntent, etc.)
    - None (equivalent to HOLD)
    - A DecideResult (for IntentStrategy compatibility)
    """

    @property
    def deployment_id(self) -> str:
        """Return the unique identifier for this strategy."""
        ...

    def decide(self, market: MarketSnapshot) -> Any:
        """Make a trading decision based on current market state.

        Args:
            market: MarketSnapshot containing current prices, balances, indicators

        Returns:
            An Intent object, None (hold), or DecideResult
        """
        ...


# =============================================================================
# Market Snapshot Factory
# =============================================================================


def create_market_snapshot_from_state(
    market_state: MarketState,
    chain: str = DEFAULT_CHAIN,
    wallet_address: str = "",
    portfolio: SimulatedPortfolio | None = None,
    token_aliases: dict[str, str] | None = None,
) -> MarketSnapshot:
    """Create a MarketSnapshot from historical MarketState data.

    This function bridges the historical data provider's MarketState format
    to the MarketSnapshot format expected by strategies' decide() methods.

    Args:
        market_state: Historical market state from data provider
        chain: Chain identifier
        wallet_address: Wallet address (can be empty for simulation)
        portfolio: Optional portfolio for balance simulation

    Returns:
        MarketSnapshot populated with historical price data
    """
    # Create market snapshot with historical timestamp
    snapshot = MarketSnapshot(
        chain=chain,
        wallet_address=wallet_address,
        timestamp=market_state.timestamp,
    )
    _ = token_aliases
    _seed_snapshot_prices(snapshot, market_state)
    if portfolio:
        _seed_snapshot_balances(snapshot, market_state, portfolio)
    return snapshot


def _seed_snapshot_prices(snapshot: MarketSnapshot, market_state: MarketState) -> None:
    chain = _market_state_chain(market_state)
    for token in _market_state_price_tokens(market_state):
        snapshot_token = _snapshot_token_key(token, chain)
        try:
            price = market_state.get_price(token)
        except KeyError:
            try:
                price = market_state.get_price(snapshot_token)
            except KeyError:
                continue
        snapshot.set_price(snapshot_token, price)


def _market_state_chain(market_state: MarketState) -> str:
    chain = getattr(market_state, "chain", None)
    return chain if isinstance(chain, str) and chain else DEFAULT_CHAIN


def _market_state_price_tokens(market_state: MarketState) -> list[TokenRef]:
    tokens: list[TokenRef] = []
    seen: set[TokenRef] = set()
    for attr in ("prices", "ohlcv"):
        mapping = getattr(market_state, attr, None)
        if not isinstance(mapping, dict):
            continue
        for token in mapping:
            if token not in seen:
                tokens.append(token)
                seen.add(token)
    if tokens:
        return tokens

    available = getattr(market_state, "available_tokens", [])
    try:
        return list(available)
    except TypeError:
        return []


def _snapshot_token_key(token: TokenRef, chain: str) -> str:
    return token_ref_display(normalize_token_ref(token, chain))


def _seed_snapshot_balances(
    snapshot: MarketSnapshot,
    market_state: MarketState,
    portfolio: SimulatedPortfolio,
) -> None:
    _seed_portfolio_token_balances(snapshot, market_state, portfolio)
    _seed_cash_balances(snapshot, portfolio)
    _seed_zero_balances_for_unheld_tokens(snapshot, market_state, portfolio)


def _seed_portfolio_token_balances(
    snapshot: MarketSnapshot,
    market_state: MarketState,
    portfolio: SimulatedPortfolio,
) -> None:
    chain = _portfolio_chain(portfolio, _market_state_chain(market_state))
    for token, amount in portfolio.tokens.items():
        display_token = _snapshot_token_key(token, chain)
        balance = _token_balance_from_market_state(token, amount, market_state, display_token)
        snapshot.set_balance(display_token, balance)


def _token_balance_from_market_state(
    token: TokenRef,
    amount: Decimal,
    market_state: MarketState,
    symbol: str,
) -> TokenBalance:
    try:
        balance_usd = amount * market_state.get_price(token)
    except KeyError:
        balance_usd = Decimal("0")
    return TokenBalance(symbol=symbol, balance=amount, balance_usd=balance_usd)


def _seed_cash_balances(snapshot: MarketSnapshot, portfolio: SimulatedPortfolio) -> None:
    snapshot.set_balance("USD", _face_value_balance("USD", portfolio.cash_usd))
    for stable in CASH_EQUIVALENT_STABLECOINS:
        if stable not in portfolio.tokens:
            snapshot.set_balance(stable, _face_value_balance(stable, portfolio.cash_usd))
    for stable_key in _portfolio_cash_equivalent_keys(portfolio):
        display_token = token_ref_display(stable_key)
        if stable_key in portfolio.tokens or display_token in portfolio.tokens:
            continue
        snapshot.set_price(display_token, Decimal("1"))
        snapshot.set_balance(display_token, _face_value_balance(display_token, portfolio.cash_usd))


def _portfolio_chain(portfolio: SimulatedPortfolio, fallback: str) -> str:
    chain = getattr(portfolio, "chain", None)
    return chain if isinstance(chain, str) and chain else fallback


def _portfolio_cash_equivalent_keys(portfolio: SimulatedPortfolio) -> tuple[TokenRef, ...]:
    keys = getattr(portfolio, "_cash_equivalent_token_keys", ())
    if not isinstance(keys, set | frozenset | list | tuple):
        return ()
    return tuple(keys)


def _face_value_balance(symbol: str, amount: Decimal) -> TokenBalance:
    return TokenBalance(symbol=symbol, balance=amount, balance_usd=amount)


def _seed_zero_balances_for_unheld_tokens(
    snapshot: MarketSnapshot,
    market_state: MarketState,
    portfolio: SimulatedPortfolio,
) -> None:
    chain = _market_state_chain(market_state)
    for token in market_state.available_tokens:
        if _should_seed_zero_balance(token, portfolio):
            display_token = _snapshot_token_key(token, chain)
            snapshot.set_balance(
                display_token,
                TokenBalance(symbol=display_token, balance=Decimal("0"), balance_usd=Decimal("0")),
            )


def _should_seed_zero_balance(
    token: TokenRef,
    portfolio: SimulatedPortfolio,
) -> bool:
    if token in portfolio.tokens or token in CASH_EQUIVALENT_STABLECOINS or token == "USD":
        return False
    normalized = normalize_token_ref(token, _portfolio_chain(portfolio, DEFAULT_CHAIN))
    if normalized in portfolio.tokens:
        return False
    if normalized in _portfolio_cash_equivalent_keys(portfolio):
        return False
    get_token_balance = getattr(portfolio, "get_token_balance", None)
    if callable(get_token_balance):
        try:
            balance = get_token_balance(token)
        except Exception:  # noqa: BLE001
            balance = None
        if balance is not None:
            try:
                return Decimal(str(balance)) == Decimal("0")
            except Exception:  # noqa: BLE001
                return True
    return True


async def classify_token_availability(
    data_provider: Any,
    tokens: list[TokenRef],
    start_time: datetime,
) -> tuple[list[str], list[str]]:
    """Classify each tracked token as available / unavailable for ``data_provider``.

    Preflight Check-2 helper (blueprint 31 §7: DataConfidence honesty). Three
    strategies, picked per provider:

    - **Membership** -- provider exposes a non-empty ``supported_tokens`` and is
      NOT resolution-based (e.g. chainlink's fixed allowlist): set membership is
      authoritative, no I/O.
    - **Resolution-based probe** (``resolution_based_availability=True``, e.g. the
      CoinGecko provider) -- ``supported_tokens`` is membership-only and not
      authoritative, so probe-fetch a price. ONLY a resolution miss / not-found
      (surfaced as ``ValueError``) marks the token unavailable; a transient error
      (rate limit / network) is NOT a miss and propagates so the caller can retry
      rather than misreporting a priceable token as unpriceable (Refinement R3).
    - **Best-effort probe** -- no membership list and not resolution-based (e.g. a
      minimal test/mock provider): probe with a broad catch so a provider that
      does not implement ``get_price`` degrades to "unavailable" rather than
      crashing preflight.

    Args:
        data_provider: The historical data provider under test.
        tokens: Tracked token symbols from the backtest config.
        start_time: Timestamp to probe (the config start).

    Returns:
        ``(tokens_available, tokens_unavailable)``, both upper-cased.

    Raises:
        Exception: A transient error from a resolution-based provider's
            ``get_price`` (anything that is not ``ValueError``) propagates.
    """
    availability_config = _token_availability_config(data_provider)
    tokens_available: list[str] = []
    tokens_unavailable: list[str] = []

    for token in tokens:
        token_label = token_ref_display(token)
        token_upper = token_label.upper()
        target = (
            tokens_available
            if await _is_token_available(data_provider, token, start_time, availability_config)
            else tokens_unavailable
        )
        target.append(token_upper)

    return tokens_available, tokens_unavailable


def _token_availability_config(data_provider: Any) -> _TokenAvailabilityConfig:
    supported_tokens = getattr(data_provider, "supported_tokens", [])
    resolution_based = getattr(data_provider, "resolution_based_availability", False)
    use_membership = bool(supported_tokens) and not resolution_based
    membership_upper = (
        frozenset(token_ref_display(t).upper() for t in supported_tokens) if use_membership else frozenset()
    )
    return _TokenAvailabilityConfig(
        use_membership=use_membership,
        resolution_based=resolution_based,
        membership_upper=membership_upper,
    )


async def _is_token_available(
    data_provider: Any,
    token: TokenRef,
    start_time: datetime,
    availability_config: _TokenAvailabilityConfig,
) -> bool:
    token_upper = token_ref_display(token).upper()
    if token_upper in CASH_EQUIVALENT_STABLECOINS:
        return True
    if availability_config.use_membership:
        return token_upper in availability_config.membership_upper
    if availability_config.resolution_based:
        return await _resolution_probe_succeeded(data_provider, token, start_time)
    return await _best_effort_probe_succeeded(data_provider, token, start_time)


async def _resolution_probe_succeeded(data_provider: Any, token: TokenRef, start_time: datetime) -> bool:
    try:
        await data_provider.get_price(token, start_time)
    except ValueError as exc:
        _raise_if_provider_level_probe_error(exc)
        return False
    return True


async def _best_effort_probe_succeeded(data_provider: Any, token: TokenRef, start_time: datetime) -> bool:
    try:
        await data_provider.get_price(token, start_time)
    except Exception as exc:
        _raise_if_provider_level_probe_error(exc)
        return False
    return True


def _raise_if_provider_level_probe_error(exc: Exception) -> None:
    # _make_request surfaces transient timeout / network / 5xx failures AND
    # auth failures (401/403) as ValueError too (not just genuine misses), so
    # probe-fetch errors must be screened: a transient failure (retryable) or an
    # auth/config failure (every token, not this one) propagates -- the caller
    # fails with the real cause -- and is NEVER recorded as an unavailable
    # token, which would abort a backtest on a network blip or a bad API key
    # (Refinement R3).
    from almanak.framework.backtesting.pnl.providers.coingecko import _is_auth_error, _is_transient_request_error

    if _is_transient_request_error(exc) or _is_auth_error(exc):
        raise exc


def _build_token_availability_check(
    data_provider: Any,
    tokens_available: list[str],
    tokens_unavailable: list[str],
) -> tuple[PreflightCheckResult, list[str]]:
    """Build the preflight Check-2 result + recommendations (priceability guard).

    Blueprint 31 §7 (DataConfidence honesty): a tracked asset that cannot be
    priced must surface loudly, never as a fabricated $0. The severity decision:

    - The hard-stop (``severity="error"`` -> blocking) is scoped to
      resolution-based providers (the CoinGecko provider sets
      ``resolution_based_availability = True``). Only there is "unavailable" an
      AUTHORITATIVE determination -- a real resolution miss / not-found surfaced
      as ``ValueError`` (Refinement R3). Membership providers and minimal
      test/mock providers stay on the historical non-blocking warning path, so
      escalating their best-effort misses cannot break unrelated backtests.
    - Cash-equivalent stablecoins (USDC/USDT/DAI) are valued at $1 face value via
      the portfolio cash sweep and never need a market price, so an unpriceable
      stablecoin is non-blocking even on the resolution-based path. A NON-cash
      tracked asset that cannot be priced IS blocking: continuing would silently
      mis-value a position the strategy holds (e.g. a bridged L2 token whose
      CoinGecko listing has no history). Mirrors the LP volume honesty guard;
      opt out with --allow-missing-prices (fail_on_preflight_error=False).

    Returns:
        ``(check, recommendations)`` -- the caller appends the check and extends
        its recommendation list. The check is ``severity="error"`` only for a
        blocking miss; everything else is a passed check or a ``"warning"``.
    """
    if not tokens_unavailable:
        return (
            PreflightCheckResult(
                check_name="token_availability",
                passed=True,
                message=f"All {len(tokens_available)} token(s) have price data available",
                details={"available": tokens_available},
            ),
            [],
        )

    resolution_based = getattr(data_provider, "resolution_based_availability", False)
    blocking_unavailable = (
        [t for t in tokens_unavailable if t.upper() not in CASH_EQUIVALENT_STABLECOINS] if resolution_based else []
    )
    if blocking_unavailable:
        return (
            PreflightCheckResult(
                check_name="token_availability",
                passed=False,
                message=(
                    f"No historical price data for tracked token(s) "
                    f"{', '.join(blocking_unavailable)} over the backtest window. "
                    "Refusing to run a backtest that cannot price a tracked asset. "
                    "To proceed: choose a chain/window where the asset has price history "
                    "(mainnet listings usually do; bridged L2 listings often do not), "
                    "remove it from --tokens, or pass --allow-missing-prices to accept a "
                    "degraded run."
                ),
                details={
                    "available": tokens_available,
                    "unavailable": tokens_unavailable,
                    "blocking_unavailable": blocking_unavailable,
                },
                severity="error",
            ),
            [
                f"No price history for: {', '.join(blocking_unavailable)} "
                "(use --allow-missing-prices to override with a degraded run)"
            ],
        )

    # Non-blocking: a non-authoritative provider (membership / mock best-effort),
    # or a resolution-based provider where only cash-equivalent stablecoins are
    # unpriceable. Preserve the historical warning behaviour verbatim.
    return (
        PreflightCheckResult(
            check_name="token_availability",
            passed=False,
            message=f"{len(tokens_unavailable)} token(s) may not have price data available",
            details={"available": tokens_available, "unavailable": tokens_unavailable},
            severity="warning",
        ),
        [f"Check price data availability for: {', '.join(tokens_unavailable)}"],
    )


# =============================================================================
# Data Quality Tracker
# =============================================================================


# DataQualityTracker extracted to data_quality.py (imported at top of file)


# =============================================================================
# PnL Backtester Engine
# =============================================================================


@dataclass(frozen=True)
class _CloseResolution:
    """How a closing intent maps onto the portfolio's open positions.

    Produced by ``PnLBacktester._resolve_position_close``: exactly one of
    ``position_close_id`` (full close) or ``position_reduce_id`` (partial
    reduction, e.g. a partial lending WITHDRAW) is set on success; both are
    None with ``failure_reason`` set when a close-type intent matches no
    open position and the fill must be rejected (crediting the inflow
    without removing the position would mint value).

    Attributes:
        amount_usd: Effective notional for fees, slippage, and token flows.
            Full lending closes resolve this to principal + accrued
            interest so the close credit is value-neutral at the close
            instant (mirroring the perp close credit).
        position_close_id: Position to close in full, if any.
        position_reduce_id: Position to partially reduce, if any.
        interest_usd: Accrued interest realized by a lending close OR a
            boundary partial reduce (amount covers all principal + part of the
            accrued interest). Recorded as the trade's realized PnL via
            metadata. Positive for a WITHDRAW (interest earned), NEGATIVE for a
            REPAY (borrow interest owed -- VIB-5098).
        reduce_amounts: Explicit per-token principal to remove on a partial
            reduce. Set only for a BOUNDARY reduce (``interest_usd`` also set):
            the fill's flow covers principal + part of the accrued interest, so
            the principal to remove (the position's full held principal) is
            SMALLER than the flow. None means an ordinary sub-principal partial
            whose reduce debits the full flow (engine derives it from
            tokens_in / tokens_out). Only meaningful with ``position_reduce_id``.
        failure_reason: Set when the fill must be rejected unapplied.
    """

    amount_usd: Decimal
    position_close_id: str | None = None
    position_reduce_id: str | None = None
    interest_usd: Decimal = Decimal("0")
    reduce_amounts: dict[TokenRef, Decimal] | None = None
    failure_reason: str | None = None


@dataclass(frozen=True)
class _PositionValue:
    principal_usd: Decimal
    total_usd: Decimal


@dataclass(frozen=True)
class _GasGweiResolution:
    gas_price_gwei: Decimal
    source: str


@dataclass(frozen=True)
class _GenericIntentDetails:
    intent_type: IntentType
    protocol: str
    tokens: list[TokenRef]
    amount_usd: Decimal
    close_resolution: _CloseResolution


@dataclass(frozen=True)
class _GenericExecutionCosts:
    fee_usd: Decimal
    slippage_pct: Decimal
    slippage_usd: Decimal
    gas_cost_usd: Decimal
    gas_price_gwei: Decimal | None
    gas_gwei_source: str | None
    estimated_mev_cost_usd: Decimal | None


@dataclass
class PnLBacktester:
    """Main PnL backtesting engine for historical strategy simulation.

    The PnLBacktester simulates strategy execution against historical price data
    to evaluate performance. It:

    1. Iterates through historical market data at configured intervals
    2. Calls strategy.decide() with a MarketSnapshot for each time step
    3. Simulates intent execution with configurable fee/slippage models
    4. Tracks portfolio state and builds an equity curve
    5. Calculates comprehensive performance metrics

    Attributes:
        data_provider: Historical data provider (e.g., CoinGeckoDataProvider)
        fee_models: Dict mapping protocol -> FeeModel (or "default" for all)
        slippage_models: Dict mapping protocol -> SlippageModel
        gas_provider: Optional gas price provider for historical gas prices.
            When provided and config.use_historical_gas_gwei=True, the engine
            will fetch historical gas prices at each simulation timestamp.
        mev_simulator: Optional MEV simulator (created dynamically based on config)
        strategy_type: Optional explicit strategy type for adapter selection.
            If "auto" (default), the type is detected from strategy metadata.
            Valid values: "lp", "perp", "lending", "arbitrage", "swap", "auto", or None.
        data_config: Optional BacktestDataConfig for controlling historical data
            providers in adapters. When provided, adapters will use historical
            volume, funding rates, and APY data from real sources instead of
            fallback values.

    Example:
        backtester = PnLBacktester(
            data_provider=CoinGeckoDataProvider(),
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        result = await backtester.backtest(my_strategy, config)
        print(result.summary())

        # With explicit strategy type:
        backtester = PnLBacktester(
            data_provider=CoinGeckoDataProvider(),
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
            strategy_type="lp",  # Force LP adapter
        )

        # With BacktestDataConfig for historical data:
        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(
            use_historical_volume=True,
            use_historical_funding=True,
            use_historical_apy=True,
        )
        backtester = PnLBacktester(
            data_provider=CoinGeckoDataProvider(),
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
            data_config=data_config,
        )
    """

    data_provider: HistoricalDataProvider
    fee_models: dict[str, FeeModel]
    slippage_models: dict[str, SlippageModel]
    strategy_type: str | None = "auto"
    gas_provider: GasPriceProvider | None = None
    """Optional gas price provider for historical gas prices.

    When provided and config.use_historical_gas_gwei=True, the engine will
    fetch historical gas prices at each simulation timestamp instead of
    using the static config.gas_price_gwei value.

    Example:
        from almanak.framework.backtesting.pnl.providers import EtherscanGasPriceProvider

        gas_provider = EtherscanGasPriceProvider(
            api_keys={"ethereum": "your-key"},
        )
        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models=fee_models,
            slippage_models=slippage_models,
            gas_provider=gas_provider,
        )
    """
    data_config: BacktestDataConfig | None = None
    """Optional BacktestDataConfig for controlling historical data providers.

    When provided, this configuration is passed to strategy-specific adapters
    (LP, Perp, Lending) to control historical data provider behavior:
    - use_historical_volume: Fetch LP fee data from subgraphs
    - use_historical_funding: Fetch perp funding rates from APIs
    - use_historical_apy: Fetch lending APY from subgraphs
    - strict_historical_mode: Fail if historical data unavailable
    - Fallback values for when historical data is unavailable
    - Rate limiting configuration for CoinGecko and The Graph
    - Cache settings for persistent data storage

    Example:
        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(
            use_historical_volume=True,
            use_historical_funding=True,
            use_historical_apy=True,
            strict_historical_mode=False,
        )
        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models=fee_models,
            slippage_models=slippage_models,
            data_config=data_config,
        )
    """
    token_addresses: dict[str, tuple[str, str]] | None = None
    """Optional ``{SYMBOL_UPPER: (chain, address)}`` map of tracked tokens.

    Supplied by the CLI / hosted service (``build_backtest_token_address_map``)
    - the same map handed to providers that can resolve historical data by
    contract address. ``None`` is reserved for custom fixtures and unresolved
    token labels.
    """
    _mev_simulator: MEVSimulator | None = None
    _current_backtest_id: str = ""
    _adapter: StrategyBacktestAdapter | None = None
    _detected_strategy_type: StrategyTypeHint | None = None
    _error_handler: BacktestErrorHandler | None = None
    _fallback_usage: dict[str, int] | None = None
    _gas_price_records: list["GasPriceRecord"] | None = None

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        # Ensure we have at least a default fee model
        if "default" not in self.fee_models:
            self.fee_models["default"] = DefaultFeeModel()

        # Ensure we have at least a default slippage model
        if "default" not in self.slippage_models:
            self.slippage_models["default"] = DefaultSlippageModel()

        # Initialize fallback usage tracking
        self._fallback_usage = {
            "hardcoded_price": 0,
            "default_gas_price": 0,
            "default_usd_amount": 0,
        }

        # Validate data_config if provided
        if self.data_config is not None:
            # Log data_config settings for transparency
            logger.info(
                "BacktestDataConfig provided: "
                f"price_provider={self.data_config.price_provider}, "
                f"use_historical_volume={self.data_config.use_historical_volume}, "
                f"use_historical_funding={self.data_config.use_historical_funding}, "
                f"use_historical_apy={self.data_config.use_historical_apy}, "
                f"strict_historical_mode={self.data_config.strict_historical_mode}"
            )

    async def close(self) -> None:
        """Close data provider and gas provider HTTP sessions."""
        if hasattr(self.data_provider, "close"):
            await self.data_provider.close()
        if self.gas_provider and hasattr(self.gas_provider, "close"):
            await self.gas_provider.close()

    def _track_fallback(self, fallback_type: str) -> None:
        """Track usage of a fallback value.

        Args:
            fallback_type: Type of fallback used (e.g., "hardcoded_price")
        """
        if self._fallback_usage is None:
            self._fallback_usage = {}
        if fallback_type in self._fallback_usage:
            self._fallback_usage[fallback_type] += 1
        else:
            self._fallback_usage[fallback_type] = 1

    def _build_callback_result(
        self,
        intent: Any,
        trade_record: TradeRecord | None,
        success: bool,
        error: str | None = None,
    ) -> SimulatedExecutionResult:
        """Build the result object passed to ``strategy.on_intent_executed``.

        VIB-2916: For LP_OPEN intents the real ``SimulatedPosition.position_id``
        is read from the trade record (populated by
        ``SimulatedFill.to_trade_record``) so a later
        ``Intent.lp_close(position_id=self._position_id)`` resolves against
        the open position the engine actually tracks.
        """
        return build_simulated_result(
            intent=intent,
            trade_record=trade_record,
            success=success,
            error=error,
        )

    def _notify_intent_failure(self, strategy: Any, intent: Any, error: Exception) -> None:
        """Notify the strategy that an intent failed to execute.

        Shared by the missing-data (``DataSourceUnavailableError``) and generic
        execution-error paths so a strategy state machine never advances past an
        intent that silently vanished. Best-effort: a raising callback is logged
        at debug, never propagated (a notification failure must not mask the
        original execution error).
        """
        if strategy is None or not hasattr(strategy, "on_intent_executed"):
            return
        try:
            callback_result = self._build_callback_result(intent, None, success=False, error=str(error))
            strategy.on_intent_executed(intent, False, callback_result)
        except Exception as notify_err:
            logger.debug(f"on_intent_executed (failure) raised: {notify_err}")

    def _create_parameter_source_tracker(
        self,
        config: PnLBacktestConfig,
    ) -> ParameterSourceTracker:
        """Create a tracker with sources for all configuration parameters.

        This method tracks where each configuration value came from for audit
        purposes. It examines the config and records the source of each parameter.

        Args:
            config: The backtest configuration to analyze

        Returns:
            ParameterSourceTracker populated with source records
        """
        tracker = ParameterSourceTracker()
        default_config = self._default_source_tracking_config(config)
        self._record_config_parameter_sources(tracker, config, default_config)
        self._record_gas_parameter_sources(tracker, config)
        self._record_liquidation_parameter_sources(tracker, config, default_config)
        self._record_adapter_runtime_rate_sources(tracker, config)
        return tracker

    @staticmethod
    def _default_source_tracking_config(config: PnLBacktestConfig) -> PnLBacktestConfig:
        return PnLBacktestConfig(
            start_time=config.start_time,
            end_time=config.end_time,
        )

    def _record_config_parameter_sources(
        self,
        tracker: ParameterSourceTracker,
        config: PnLBacktestConfig,
        default_config: PnLBacktestConfig,
    ) -> None:
        for name, value, default_value in self._config_parameter_values(config, default_config):
            tracker.record_parameter(
                name,
                value,
                self._parameter_source_for_value(value, default_value),
                category="config",
            )
        self._record_gas_price_gwei_source(tracker, config)

    @staticmethod
    def _config_parameter_values(
        config: PnLBacktestConfig,
        default_config: PnLBacktestConfig,
    ) -> list[tuple[str, Any, Any]]:
        return [
            ("token_funding", config.token_funding, default_config.token_funding),
            ("interval_seconds", config.interval_seconds, default_config.interval_seconds),
            ("fee_model", config.fee_model, default_config.fee_model),
            ("slippage_model", config.slippage_model, default_config.slippage_model),
            ("include_gas_costs", config.include_gas_costs, default_config.include_gas_costs),
            ("inclusion_delay_blocks", config.inclusion_delay_blocks, default_config.inclusion_delay_blocks),
            ("chain", config.chain, default_config.chain),
            ("benchmark_token", config.benchmark_token, default_config.benchmark_token),
            ("risk_free_rate", config.risk_free_rate, default_config.risk_free_rate),
            ("trading_days_per_year", config.trading_days_per_year, default_config.trading_days_per_year),
            ("mev_simulation_enabled", config.mev_simulation_enabled, default_config.mev_simulation_enabled),
            ("auto_correct_positions", config.auto_correct_positions, default_config.auto_correct_positions),
            ("strict_reproducibility", config.strict_reproducibility, default_config.strict_reproducibility),
            ("institutional_mode", config.institutional_mode, default_config.institutional_mode),
            ("min_data_coverage", config.min_data_coverage, default_config.min_data_coverage),
            ("allow_hardcoded_fallback", config.allow_hardcoded_fallback, default_config.allow_hardcoded_fallback),
            ("use_historical_gas_prices", config.use_historical_gas_prices, default_config.use_historical_gas_prices),
            ("use_historical_gas_gwei", config.use_historical_gas_gwei, default_config.use_historical_gas_gwei),
            ("preflight_validation", config.preflight_validation, default_config.preflight_validation),
        ]

    @staticmethod
    def _parameter_source_for_value(value: Any, default_value: Any) -> ParameterSource:
        return ParameterSource.DEFAULT if value == default_value else ParameterSource.EXPLICIT

    @staticmethod
    def _record_gas_price_gwei_source(
        tracker: ParameterSourceTracker,
        config: PnLBacktestConfig,
    ) -> None:
        # gas_price_gwei provenance comes from the config's own flag rather
        # than the value-equality heuristic above: the default is chain-aware
        # (VIB-5088), so comparing against a default_config built on
        # DEFAULT_CHAIN would mislabel defaults on other chains as explicit.
        tracker.record_parameter(
            "gas_price_gwei",
            config.gas_price_gwei,
            ParameterSource.DEFAULT if config.gas_price_gwei_is_default else ParameterSource.EXPLICIT,
            category="config",
        )

    @staticmethod
    def _record_gas_parameter_sources(
        tracker: ParameterSourceTracker,
        config: PnLBacktestConfig,
    ) -> None:
        if config.gas_eth_price_override is not None:
            tracker.record_parameter(
                "gas_eth_price_override",
                config.gas_eth_price_override,
                ParameterSource.EXPLICIT,
                category="gas",
            )
            return
        tracker.record_parameter(
            "gas_eth_price",
            "runtime_determined",
            ParameterSource.HISTORICAL if config.use_historical_gas_prices else ParameterSource.PROVIDER,
            category="gas",
            fallback_chain=["historical_provider", "current_provider", "raise_if_unavailable"],
        )

    def _record_liquidation_parameter_sources(
        self,
        tracker: ParameterSourceTracker,
        config: PnLBacktestConfig,
        default_config: PnLBacktestConfig,
    ) -> None:
        for name in (
            "initial_margin_ratio",
            "maintenance_margin_ratio",
            "reconciliation_alert_threshold_pct",
        ):
            value = getattr(config, name)
            default_value = getattr(default_config, name)
            tracker.record_parameter(
                name,
                value,
                self._parameter_source_for_value(value, default_value),
                category="liquidation",
            )

    def _record_adapter_runtime_rate_sources(
        self,
        tracker: ParameterSourceTracker,
        config: PnLBacktestConfig,
    ) -> None:
        if self._adapter is None:
            return
        adapter_type = type(self._adapter).__name__.lower()
        rate_source = ParameterSource.HISTORICAL if config.strict_reproducibility else ParameterSource.PROVIDER
        rate_value = "historical" if config.strict_reproducibility else "provider"
        if "perp" in adapter_type:
            tracker.record_parameter("funding_rate_source", rate_value, rate_source, category="apy_funding")
        if "lending" in adapter_type:
            tracker.record_parameter("apy_source", rate_value, rate_source, category="apy_funding")

    def _detect_strategy_type(
        self,
        strategy: BacktestableStrategy,
    ) -> StrategyTypeHint:
        """Detect the strategy type from a strategy object.

        Uses the adapter registry's detection system to determine the
        appropriate adapter type for backtesting. Detection priority:
        1. Explicit strategy_type in backtester config (if not "auto")
        2. Strategy metadata tags
        3. Supported protocols
        4. Intent types used
        5. Fallback to None (generic backtesting)

        Args:
            strategy: Strategy to detect type for

        Returns:
            StrategyTypeHint with detected type and confidence
        """
        # Build config dict for detection
        config: dict[str, Any] | None = None
        if self.strategy_type is not None and self.strategy_type != "auto":
            config = {"strategy_type": self.strategy_type}

        hint = detect_strategy_type(strategy, config)
        self._detected_strategy_type = hint

        if hint.strategy_type:
            logger.info(
                f"Detected strategy type '{hint.strategy_type}' (confidence={hint.confidence}, source={hint.source})"
            )
            if hint.details:
                logger.debug(f"Detection details: {hint.details}")
        else:
            logger.debug("No strategy type detected, will use generic backtesting")

        return hint

    def _init_adapter(
        self,
        strategy: BacktestableStrategy,
    ) -> None:
        """Initialize the strategy adapter based on detected type.

        This method detects the strategy type and loads the appropriate
        adapter for strategy-specific backtesting behavior. The adapter
        is stored in self._adapter for use during the backtest.

        When data_config is provided to the PnLBacktester, it will be passed
        to the adapter to control historical data provider behavior.

        Args:
            strategy: Strategy to initialize adapter for
        """
        # Build config dict for adapter lookup
        config: dict[str, Any] | None = None
        if self.strategy_type is not None and self.strategy_type != "auto":
            config = {"strategy_type": self.strategy_type}

        # Use get_adapter_for_strategy_with_config to pass data_config to adapters
        adapter = get_adapter_for_strategy_with_config(
            strategy,
            data_config=self.data_config,
            config=config,
        )

        if adapter:
            self._adapter = adapter
            adapter_info = f"'{adapter.adapter_name}' for strategy '{strategy.deployment_id}'"
            if self.data_config is not None:
                logger.info(f"Loaded adapter {adapter_info} with BacktestDataConfig")
            else:
                logger.info(f"Loaded adapter {adapter_info}")
        else:
            self._adapter = None
            logger.debug(f"No adapter loaded for strategy '{strategy.deployment_id}', using generic backtesting")

    def _init_mev_simulator(self, config: PnLBacktestConfig) -> None:
        """Initialize MEV simulator based on config.

        Args:
            config: Backtest configuration with mev_simulation_enabled flag
        """
        if config.mev_simulation_enabled:
            self._mev_simulator = MEVSimulator(config=MEVSimulatorConfig())
            logger.info("MEV simulation enabled - trades may incur simulated MEV costs")
        else:
            self._mev_simulator = None

    @staticmethod
    def _create_indicator_engine(strategy: BacktestableStrategy) -> BacktestIndicatorEngine:
        """Create indicator engine based on strategy's declared requirements.

        Reads ``required_indicators`` from the strategy (if present) and
        configures the engine accordingly.  Falls back to the default set
        (RSI, MACD, Bollinger Bands) when the attribute is absent.

        Args:
            strategy: The strategy being backtested.

        Returns:
            Configured BacktestIndicatorEngine.
        """
        required = getattr(strategy, "required_indicators", None)
        if required is not None:
            indicator_set = set(required) if not isinstance(required, set) else required
        else:
            indicator_set = None  # will use defaults

        return BacktestIndicatorEngine(required_indicators=indicator_set)

    @staticmethod
    def _get_strategy_config_dict(strategy: BacktestableStrategy) -> dict:
        """Extract a plain dict config from a strategy for indicator parameter lookup.

        Tries ``strategy.config.to_dict()``, ``dict(strategy.config)``, and
        direct dict access.  Returns an empty dict if nothing works.
        """
        config = getattr(strategy, "config", None)
        if config is None:
            return {}
        if isinstance(config, dict):
            return config
        if hasattr(config, "to_dict"):
            try:
                return config.to_dict()
            except Exception:
                pass
        # Try dict() constructor (works for Mapping-like objects)
        try:
            return dict(config)
        except (TypeError, ValueError):
            return {}

    def _get_data_provider_info(self) -> dict[str, Any]:
        """Get information about the data provider for metadata.

        Collects provider name, version, and capabilities for reproducibility
        tracking in the backtest result. Uses error handler for non-critical
        errors during metadata collection.

        Returns:
            Dictionary with data provider information
        """
        info: dict[str, Any] = {
            "name": getattr(self.data_provider, "provider_name", "unknown"),
            "data_fetched_at": datetime.now(UTC).isoformat(),
        }

        # Helper function for safely getting attributes with error handling
        def safe_get_attr(attr_name: str, default: Any = None) -> Any:
            try:
                return getattr(self.data_provider, attr_name, default)
            except Exception as e:
                if self._error_handler:
                    self._error_handler.handle_error(
                        e,
                        context=f"get_data_provider_info:{attr_name}",
                    )
                return default

        # Try to get supported tokens/chains
        info["supported_tokens"] = safe_get_attr("supported_tokens", [])
        info["supported_chains"] = safe_get_attr("supported_chains", [])

        # Try to get provider version if available
        info["version"] = safe_get_attr("version", None)

        # Try to get min/max timestamps if available
        min_ts = safe_get_attr("min_timestamp", None)
        if min_ts:
            info["min_timestamp"] = min_ts.isoformat() if hasattr(min_ts, "isoformat") else str(min_ts)

        max_ts = safe_get_attr("max_timestamp", None)
        if max_ts:
            info["max_timestamp"] = max_ts.isoformat() if hasattr(max_ts, "isoformat") else str(max_ts)

        return info

    def _collect_data_source_capabilities(
        self,
        bt_logger: BacktestLogger,
    ) -> tuple[dict[str, HistoricalDataCapability], list[str]]:
        """Collect data source capabilities and generate warnings for limited providers.

        This method inspects the data provider to determine its historical data capability
        and generates appropriate warnings if the provider has limitations that may affect
        backtest accuracy.

        Args:
            bt_logger: BacktestLogger for logging warnings

        Returns:
            Tuple of (capabilities dict, warnings list):
            - capabilities: Dict mapping provider name to HistoricalDataCapability
            - warnings: List of warning messages about data limitations
        """
        capabilities: dict[str, HistoricalDataCapability] = {}
        warnings: list[str] = []

        provider_name = getattr(self.data_provider, "provider_name", "unknown")

        # Try to get the historical capability from the provider
        try:
            capability = getattr(
                self.data_provider,
                "historical_capability",
                HistoricalDataCapability.FULL,  # Default to FULL if not specified
            )
            capabilities[provider_name] = capability

            # Generate warnings for providers with limited capabilities
            if capability == HistoricalDataCapability.CURRENT_ONLY:
                warning_msg = (
                    f"Data provider '{provider_name}' has CURRENT_ONLY capability. "
                    "Historical prices will be fetched at backtest runtime, not at simulation timestamps. "
                    "This may significantly affect backtest accuracy for historical analysis."
                )
                warnings.append(warning_msg)
                bt_logger.warning(warning_msg)

            elif capability == HistoricalDataCapability.PRE_CACHE:
                warning_msg = (
                    f"Data provider '{provider_name}' has PRE_CACHE capability. "
                    "Historical data must be pre-fetched before backtest. "
                    "Ensure data cache is populated for the full backtest period."
                )
                warnings.append(warning_msg)
                bt_logger.warning(warning_msg)

        except Exception as e:
            # Handle errors gracefully using error handler
            if self._error_handler:
                self._error_handler.handle_error(
                    e,
                    context=f"collect_data_source_capabilities:{provider_name}",
                )
            # Default to unknown capability
            bt_logger.debug(f"Could not determine capability for provider {provider_name}: {e}")

        return capabilities, warnings

    async def run_preflight_validation(
        self,
        config: PnLBacktestConfig,
    ) -> PreflightReport:
        """Run preflight validation checks before starting a backtest.

        Performs validation checks to ensure data requirements can be met:
        - Checks price data availability for all tokens in config
        - Verifies data provider capabilities match requirements
        - Tests archive node accessibility if historical TWAP/Chainlink needed
        - Estimates data coverage based on provider capabilities

        Args:
            config: Backtest configuration specifying tokens, time range, etc.

        Returns:
            PreflightReport with pass/fail status and detailed check results.

        Example:
            preflight = await backtester.run_preflight_validation(config)
            if not preflight.passed:
                print(preflight.summary())
                # Handle validation failure
            else:
                result = await backtester.backtest(strategy, config)
        """
        import time

        validation_started = time.time()
        provider_name = getattr(self.data_provider, "provider_name", "unknown")

        provider_result = self._preflight_provider_capability(provider_name)
        (
            tokens_available,
            tokens_unavailable,
            token_check,
            token_recommendations,
        ) = await self._preflight_token_availability(config)
        archive_result = await self._preflight_archive_access()
        time_range_result = self._preflight_time_range(config)
        institutional_check, institutional_recommendations = self._preflight_institutional_compliance(
            config,
            provider_result.capabilities,
        )

        checks = [provider_result.check, token_check]
        if archive_result.check is not None:
            checks.append(archive_result.check)
        checks.append(time_range_result.check)
        if institutional_check is not None:
            checks.append(institutional_check)

        recommendations = [
            *provider_result.recommendations,
            *token_recommendations,
            *archive_result.recommendations,
            *time_range_result.recommendations,
            *institutional_recommendations,
        ]

        return PreflightReport(
            passed=self._preflight_passed(checks),
            checks=checks,
            estimated_coverage=self._estimated_token_coverage(tokens_available, tokens_unavailable),
            tokens_available=tokens_available,
            tokens_unavailable=tokens_unavailable,
            provider_capabilities=provider_result.capabilities,
            archive_node_accessible=archive_result.accessible,
            recommendations=recommendations,
            validation_time_seconds=time.time() - validation_started,
        )

    def _preflight_provider_capability(self, provider_name: str) -> _ProviderCapabilityPreflight:
        try:
            capability = getattr(self.data_provider, "historical_capability", HistoricalDataCapability.FULL)
            capability_value = capability.value
        except Exception as exc:
            return _ProviderCapabilityPreflight(
                check=PreflightCheckResult(
                    check_name="provider_capability",
                    passed=False,
                    message=f"Failed to check provider capability: {exc}",
                    details={"error": str(exc)},
                ),
                capabilities={},
                recommendations=[],
            )

        check, recommendations = self._provider_capability_check(provider_name, capability, capability_value)
        return _ProviderCapabilityPreflight(
            check=check,
            capabilities={provider_name: capability_value},
            recommendations=recommendations,
        )

    @staticmethod
    def _provider_capability_check(
        provider_name: str,
        capability: HistoricalDataCapability,
        capability_value: str,
    ) -> tuple[PreflightCheckResult, list[str]]:
        details = {"provider": provider_name, "capability": capability_value}
        if capability == HistoricalDataCapability.FULL:
            return (
                PreflightCheckResult(
                    check_name="provider_capability",
                    passed=True,
                    message=f"Provider '{provider_name}' has FULL historical data capability",
                    details=details,
                ),
                [],
            )
        if capability == HistoricalDataCapability.CURRENT_ONLY:
            return (
                PreflightCheckResult(
                    check_name="provider_capability",
                    passed=False,
                    message=f"Provider '{provider_name}' has CURRENT_ONLY capability. Historical prices may be inaccurate.",
                    details=details,
                    severity="warning",
                ),
                [
                    "Consider using a provider with FULL historical capability (e.g., CoinGecko) for accurate backtesting"
                ],
            )
        return (
            PreflightCheckResult(
                check_name="provider_capability",
                passed=True,
                message=f"Provider '{provider_name}' requires PRE_CACHE. Ensure historical data is pre-fetched.",
                details=details,
                severity="warning",
            ),
            ["Pre-fetch historical data before running backtest for optimal performance"],
        )

    async def _preflight_token_availability(
        self,
        config: PnLBacktestConfig,
    ) -> tuple[list[str], list[str], PreflightCheckResult, list[str]]:
        from almanak.framework.backtesting.pnl.initial_portfolio import funded_token_refs

        tokens_to_check: list[TokenRef] = list(config.tokens)
        existing = {normalize_token_ref(token, config.chain) for token in tokens_to_check}
        if config.token_funding:
            for funded_token in funded_token_refs(config.token_funding, chain=config.chain):
                identity = normalize_token_ref(funded_token, config.chain)
                if identity not in existing:
                    tokens_to_check.append(funded_token)
                    existing.add(identity)
        tokens_available, tokens_unavailable = await classify_token_availability(
            self.data_provider,
            tokens_to_check,
            config.start_time,
        )
        token_check, token_recommendations = _build_token_availability_check(
            self.data_provider,
            tokens_available,
            tokens_unavailable,
        )
        return tokens_available, tokens_unavailable, token_check, token_recommendations

    async def _preflight_archive_access(self) -> _ArchiveAccessPreflight:
        if not hasattr(self.data_provider, "verify_archive_access"):
            return _ArchiveAccessPreflight(check=None, accessible=None, recommendations=[])
        try:
            archive_accessible = await self.data_provider.verify_archive_access()
        except Exception as exc:
            return _ArchiveAccessPreflight(
                check=PreflightCheckResult(
                    check_name="archive_node_access",
                    passed=False,
                    message=f"Failed to verify archive node access: {exc}",
                    details={"error": str(exc)},
                    severity="warning",
                ),
                accessible=None,
                recommendations=[],
            )
        if archive_accessible:
            return _ArchiveAccessPreflight(
                check=PreflightCheckResult(
                    check_name="archive_node_access",
                    passed=True,
                    message="Archive node is accessible for historical queries",
                ),
                accessible=True,
                recommendations=[],
            )
        return _ArchiveAccessPreflight(
            check=PreflightCheckResult(
                check_name="archive_node_access",
                passed=False,
                message="Archive node is not accessible; historical queries may fail",
                severity="warning",
            ),
            accessible=False,
            recommendations=["Configure an archive node RPC URL for accurate historical TWAP/Chainlink data"],
        )

    def _preflight_time_range(self, config: PnLBacktestConfig) -> _TimeRangePreflight:
        provider_min_ts = getattr(self.data_provider, "min_timestamp", None)
        provider_max_ts = getattr(self.data_provider, "max_timestamp", None)
        time_range_valid = True
        time_range_details: dict[str, Any] = {
            "requested_start": config.start_time.isoformat(),
            "requested_end": config.end_time.isoformat(),
        }
        if provider_min_ts is not None and config.start_time < provider_min_ts:
            time_range_valid = False
            time_range_details["provider_min"] = provider_min_ts.isoformat()
        if provider_max_ts is not None and config.end_time > provider_max_ts:
            time_range_valid = False
            time_range_details["provider_max"] = provider_max_ts.isoformat()
        if time_range_valid:
            return _TimeRangePreflight(
                check=PreflightCheckResult(
                    check_name="time_range_coverage",
                    passed=True,
                    message="Requested time range is within provider's data range",
                    details=time_range_details,
                ),
                recommendations=[],
            )
        return _TimeRangePreflight(
            check=PreflightCheckResult(
                check_name="time_range_coverage",
                passed=False,
                message="Requested time range extends beyond provider's data availability",
                details=time_range_details,
                severity="warning",
            ),
            recommendations=["Adjust backtest time range to match data provider's coverage"],
        )

    @staticmethod
    def _preflight_institutional_compliance(
        config: PnLBacktestConfig,
        provider_capabilities: dict[str, str],
    ) -> tuple[PreflightCheckResult | None, list[str]]:
        if not config.institutional_mode:
            return None, []
        if any(cap == "current_only" for cap in provider_capabilities.values()):
            return (
                PreflightCheckResult(
                    check_name="institutional_compliance",
                    passed=False,
                    message="CURRENT_ONLY provider not allowed in institutional mode",
                    details={"provider_capabilities": provider_capabilities},
                    severity="error",
                ),
                ["Use a provider with FULL historical capability for institutional mode"],
            )
        return (
            PreflightCheckResult(
                check_name="institutional_compliance",
                passed=True,
                message="Provider meets institutional mode requirements",
            ),
            [],
        )

    @staticmethod
    def _estimated_token_coverage(tokens_available: list[str], tokens_unavailable: list[str]) -> Decimal:
        total_tokens = len(tokens_available) + len(tokens_unavailable)
        return Decimal(len(tokens_available)) / Decimal(total_tokens) if total_tokens > 0 else Decimal("1.0")

    @staticmethod
    def _preflight_passed(checks: list[PreflightCheckResult]) -> bool:
        return not any(not check.passed and check.severity == "error" for check in checks)

    async def backtest(
        self,
        strategy: BacktestableStrategy,
        config: PnLBacktestConfig,
    ) -> BacktestResult:
        """Run a backtest for a strategy over the configured period.

        This method:
        1. Initializes a simulated portfolio with initial capital
        2. Creates a HistoricalDataConfig from the backtest config
        3. Iterates through historical market states
        4. For each time step:
           a. Creates a MarketSnapshot from MarketState
           b. Calls strategy.decide(snapshot) to get intent
           c. Queues intent for execution (with inclusion delay)
           d. Executes queued intents
           e. Marks portfolio to market
        5. Calculates final metrics and returns BacktestResult

        Args:
            strategy: Strategy to backtest (must implement BacktestableStrategy)
            config: Backtest configuration (time range, capital, models, etc.)

        Returns:
            BacktestResult with metrics, trades, and equity curve

        Raises:
            ValueError: If strategy is not compatible with backtesting
        """
        run_started_at = datetime.now(UTC)

        # Generate unique backtest_id for correlation across all log messages
        backtest_id = str(uuid.uuid4())
        self._current_backtest_id = backtest_id  # Store for use in _execute_intent

        # Create backtest logger with phase timing support
        bt_logger = BacktestLogger(
            backtest_id=backtest_id,
            json_format=False,  # Use text format by default, can be configured
            logger=logger,
        )

        bt_logger.info(
            f"Starting backtest for {strategy.deployment_id} "
            f"from {config.start_time} to {config.end_time} "
            "with token_funding startup balances"
        )

        # Wrap entire backtest body in try/finally to guarantee async resource cleanup.
        # Without this, exceptions from the data quality gate (ValueError) or preflight
        # validation (PreflightValidationError) can exit without closing the aiohttp
        # session, causing "RuntimeError: Event loop is closed" when GC collects the
        # leaked session after asyncio.run() shuts down the event loop.
        try:
            return await self._run_backtest(strategy, config, backtest_id, bt_logger, run_started_at)
        finally:
            try:
                await self.close()
            except (OSError, RuntimeError):
                logger.debug("Error during async resource cleanup", exc_info=True)

    async def _run_backtest(
        self,
        strategy: BacktestableStrategy,
        config: PnLBacktestConfig,
        backtest_id: str,
        bt_logger: BacktestLogger,
        run_started_at: datetime,
    ) -> BacktestResult:
        """Internal backtest implementation. Called by backtest() with guaranteed cleanup.

        Orchestrates the phase helpers extracted in Phase 6C.2. The body is a
        thin sequencer: preflight -> initialization -> simulation loop ->
        (error path | data quality gate + finalization). All semantics are
        preserved byte-for-byte by ``_engine_helpers``; see that module's
        docstring for details.
        """
        # Run preflight validation if enabled (no BacktestState yet, so a
        # PreflightValidationError propagates straight to the caller -- matches
        # pre-extraction behavior).
        preflight_report, preflight_passed = await _engine_helpers.run_preflight(
            backtester=self,
            config=config,
            bt_logger=bt_logger,
        )

        # Initialization phase: build the shared BacktestState.
        state = _engine_helpers.initialize_backtest(
            backtester=self,
            strategy=strategy,
            config=config,
            bt_logger=bt_logger,
        )

        # Simulation phase
        try:
            await _engine_helpers.execute_iteration_loop(
                backtester=self,
                strategy=strategy,
                config=config,
                bt_logger=bt_logger,
                state=state,
            )
        except Exception as e:
            return _engine_helpers.build_error_result(
                backtester=self,
                strategy=strategy,
                config=config,
                backtest_id=backtest_id,
                bt_logger=bt_logger,
                run_started_at=run_started_at,
                state=state,
                preflight_report=preflight_report,
                preflight_passed=preflight_passed,
                error=e,
            )

        # Data quality gate enforcement - check coverage ratio after simulation.
        # Raises ValueError in institutional mode; otherwise appends compliance
        # violation + logs warning.
        _engine_helpers.enforce_data_quality_gate(
            config=config,
            bt_logger=bt_logger,
            state=state,
        )

        # Metrics calculation + BacktestResult assembly
        return _engine_helpers.finalize_backtest_result(
            backtester=self,
            strategy=strategy,
            config=config,
            backtest_id=backtest_id,
            bt_logger=bt_logger,
            run_started_at=run_started_at,
            state=state,
            preflight_report=preflight_report,
            preflight_passed=preflight_passed,
        )

    def _extract_intent(self, decide_result: Any) -> Any:
        """Extract the intent from a decide() result. Delegates to intent_extraction module."""
        from .intent_extraction import extract_intent

        return extract_intent(decide_result)

    def _is_hold_intent(self, intent: Any) -> bool:
        """Check if an intent is a HOLD intent. Delegates to intent_extraction module."""
        from .intent_extraction import is_hold_intent

        return is_hold_intent(intent)

    def _update_positions_via_adapter(
        self,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        timestamp: datetime,
    ) -> None:
        """Update all positions via the strategy-specific adapter.

        This method calls the adapter's update_position() method for each position
        in the portfolio. This allows strategy-specific position updates such as:
        - LP: Fee accrual, impermanent loss tracking
        - Perp: Funding payments, liquidation price updates
        - Lending: Interest accrual, health factor updates

        Args:
            portfolio: The portfolio containing positions to update
            market_state: Current market state with prices
            timestamp: Current simulation timestamp

        Note:
            If no adapter is set, this method does nothing.
            The adapter's update_position modifies positions in-place.
        """
        if self._adapter is None:
            return

        elapsed_seconds = self._portfolio_elapsed_since_last_mark(portfolio, timestamp)
        for position in portfolio.positions:
            self._update_single_position_via_adapter(position, market_state, timestamp, elapsed_seconds)

    @staticmethod
    def _portfolio_elapsed_since_last_mark(portfolio: SimulatedPortfolio, timestamp: datetime) -> float:
        if not portfolio.equity_curve:
            return 0.0
        elapsed_seconds = (timestamp - portfolio.equity_curve[-1].timestamp).total_seconds()
        return max(elapsed_seconds, 0.0)

    def _update_single_position_via_adapter(
        self,
        position: SimulatedPosition,
        market_state: MarketState,
        timestamp: datetime,
        portfolio_elapsed_seconds: float,
    ) -> None:
        assert self._adapter is not None
        try:
            self._adapter.update_position(
                position,
                market_state,
                self._position_update_elapsed(position, timestamp, portfolio_elapsed_seconds),
                timestamp,
            )
        except DataSourceUnavailableError as exc:
            self._handle_adapter_update_missing_data(position, timestamp, exc)
        except Exception as exc:
            self._handle_adapter_update_error(position, timestamp, exc)

    @staticmethod
    def _position_update_elapsed(
        position: SimulatedPosition,
        timestamp: datetime,
        portfolio_elapsed_seconds: float,
    ) -> float:
        if position.last_updated is None:
            return portfolio_elapsed_seconds
        position_elapsed = (timestamp - position.last_updated).total_seconds()
        return min(portfolio_elapsed_seconds, position_elapsed) if position_elapsed >= 0 else portfolio_elapsed_seconds

    def _handle_adapter_update_missing_data(
        self,
        position: SimulatedPosition,
        timestamp: datetime,
        exc: DataSourceUnavailableError,
    ) -> None:
        logger.error(
            "Missing data source while updating position %s: %s",
            position.position_id,
            exc,
        )
        if self._error_handler is None:
            raise exc
        result = self._error_handler.handle_error(
            exc,
            context=self._adapter_update_context(position, timestamp),
        )
        if result.should_stop:
            raise exc

    def _handle_adapter_update_error(
        self,
        position: SimulatedPosition,
        timestamp: datetime,
        exc: Exception,
    ) -> None:
        if self._error_handler is None:
            logger.debug(
                "Adapter update_position failed for %s: %s",
                position.position_id,
                exc,
            )
            return
        result = self._error_handler.handle_error(exc, context=self._adapter_update_context(position, timestamp))
        if result.should_stop:
            logger.error(f"Fatal error updating position {position.position_id}: {exc}")
            raise exc

    @staticmethod
    def _adapter_update_context(position: SimulatedPosition, timestamp: datetime) -> str:
        return f"adapter_update_position:{position.position_id}:{timestamp.isoformat()}"

    async def _process_pending_intents(
        self,
        pending_intents: list[tuple[Any, datetime, int]],
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        config: PnLBacktestConfig,
        data_quality_tracker: DataQualityTracker | None = None,
        strategy: Any = None,
    ) -> list[tuple[Any, datetime, int]]:
        """Process pending intents, executing those ready and decrementing others.

        Implements inclusion delay simulation. Intents are queued when decided
        and executed after config.inclusion_delay_blocks ticks have passed.

        Args:
            pending_intents: List of (intent, decision_time, blocks_remaining)
            portfolio: Current portfolio state
            market_state: Current market state for execution
            config: Backtest config
            data_quality_tracker: Optional tracker for data quality metrics
            strategy: Optional strategy instance to notify via on_intent_executed

        Returns:
            Updated list of pending intents (those still waiting)
        """
        remaining: list[tuple[Any, datetime, int]] = []

        for intent, decision_time, blocks_remaining in pending_intents:
            updated = await self._process_one_pending_intent(
                intent=intent,
                decision_time=decision_time,
                blocks_remaining=blocks_remaining,
                portfolio=portfolio,
                market_state=market_state,
                config=config,
                data_quality_tracker=data_quality_tracker,
                strategy=strategy,
            )
            if updated is not None:
                remaining.append(updated)

        return remaining

    async def _process_one_pending_intent(
        self,
        intent: Any,
        decision_time: datetime,
        blocks_remaining: int,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        config: PnLBacktestConfig,
        data_quality_tracker: DataQualityTracker | None,
        strategy: Any,
    ) -> tuple[Any, datetime, int] | None:
        if blocks_remaining > 0:
            return intent, decision_time, blocks_remaining - 1
        await self._execute_ready_pending_intent(
            intent=intent,
            decision_time=decision_time,
            portfolio=portfolio,
            market_state=market_state,
            config=config,
            data_quality_tracker=data_quality_tracker,
            strategy=strategy,
        )
        return None

    async def _execute_ready_pending_intent(
        self,
        intent: Any,
        decision_time: datetime,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        config: PnLBacktestConfig,
        data_quality_tracker: DataQualityTracker | None,
        strategy: Any,
    ) -> None:
        try:
            trade_record = await self._execute_intent(
                intent=intent,
                portfolio=portfolio,
                market_state=market_state,
                timestamp=market_state.timestamp,
                config=config,
                data_quality_tracker=data_quality_tracker,
            )
        except DataSourceUnavailableError as exc:
            self._handle_pending_missing_data(intent, strategy, market_state.timestamp, exc)
            return
        except Exception as exc:
            self._handle_pending_execution_error(intent, strategy, market_state.timestamp, exc)
            return

        self._log_pending_trade_outcome(trade_record, decision_time, market_state.timestamp)
        # Notify strategy with the real outcome so state machines do not advance
        # past a trade that never applied.
        _engine_helpers.notify_intent_outcome(self, strategy, intent, trade_record, logger)

    def _log_pending_trade_outcome(
        self,
        trade_record: TradeRecord,
        decision_time: datetime,
        timestamp: datetime,
    ) -> None:
        # The portfolio may reject a fill (insufficient balance,
        # producer-failed) -- trade_record.success is authoritative.
        if trade_record.success:
            if self._error_handler:
                self._error_handler.record_success()
            logger.debug(
                f"Executed intent at {timestamp} "
                f"(decided at {decision_time}): "
                f"type={trade_record.intent_type.value}, "
                f"amount=${trade_record.amount_usd:,.2f}, "
                f"fee=${trade_record.fee_usd:,.2f}"
            )
            return
        logger.warning(
            f"Intent rejected by portfolio at {timestamp} "
            f"(decided at {decision_time}): "
            f"type={trade_record.intent_type.value}, "
            f"reason={trade_record.metadata.get('failure_reason', 'fill rejected')}"
        )

    def _handle_pending_missing_data(
        self,
        intent: Any,
        strategy: Any,
        timestamp: datetime,
        exc: DataSourceUnavailableError,
    ) -> None:
        # VIB-5088 (pattern from VIB-4849): missing data is a deliberate
        # fail-loud signal, not a warn-and-skip. Notify the strategy before
        # consulting the error policy so state machines do not silently stall.
        logger.error(
            "Missing data source while executing intent at %s: %s",
            timestamp.isoformat(),
            exc,
        )
        self._notify_intent_failure(strategy, intent, exc)
        if self._error_handler is None:
            raise exc
        result = self._error_handler.handle_error(
            exc,
            context=self._execute_intent_context(timestamp, intent),
        )
        if result.should_stop:
            raise exc

    def _handle_pending_execution_error(
        self,
        intent: Any,
        strategy: Any,
        timestamp: datetime,
        exc: Exception,
    ) -> None:
        self._notify_intent_failure(strategy, intent, exc)
        if self._error_handler is None:
            logger.warning(f"Failed to execute intent at {timestamp}: {exc}")
            return
        result = self._error_handler.handle_error(
            exc,
            context=self._execute_intent_context(timestamp, intent),
        )
        if result.should_stop:
            logger.error(f"Fatal error executing intent at {timestamp}: {exc}")
            raise exc
        logger.warning(f"Failed to execute intent at {timestamp}: {exc} - skipping")

    @staticmethod
    def _execute_intent_context(timestamp: datetime, intent: Any) -> str:
        return f"execute_intent:{timestamp.isoformat()}:{type(intent).__name__}"

    def get_fee_model(self, protocol: str) -> FeeModel:
        """Get the fee model for a protocol.

        Args:
            protocol: Protocol name (e.g., "uniswap_v3", "aave_v3")

        Returns:
            FeeModel for the protocol, or default if not found
        """
        return self.fee_models.get(protocol, self.fee_models["default"])

    def get_slippage_model(self, protocol: str) -> SlippageModel:
        """Get the slippage model for a protocol.

        Args:
            protocol: Protocol name (e.g., "uniswap_v3", "gmx")

        Returns:
            SlippageModel for the protocol, or default if not found
        """
        return self.slippage_models.get(protocol, self.slippage_models["default"])

    async def _execute_intent(
        self,
        intent: Any,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        timestamp: datetime,
        config: PnLBacktestConfig,
        delayed_at_end: bool = False,
        data_quality_tracker: DataQualityTracker | None = None,
    ) -> TradeRecord:
        """Execute an intent by simulating the trade with fees and slippage.

        This method simulates the execution of a trading intent by:
        1. Trying adapter-specific execution first (if adapter exists)
        2. Falling back to generic execution if adapter returns None
        3. Extracting intent details (type, protocol, tokens, amount)
        4. Looking up the appropriate fee and slippage models
        5. Calculating fee_usd based on intent type and amount
        6. Calculating slippage_pct and slippage_usd
        7. Calculating gas_cost_usd if include_gas_costs is True
        8. Creating a SimulatedFill with all execution details
        9. Applying the fill to the portfolio
        10. Returning a TradeRecord

        Args:
            intent: The intent to execute (SwapIntent, LPIntent, etc.)
            portfolio: The portfolio to update
            market_state: Current market state with prices
            timestamp: Time of execution
            config: Backtest configuration
            delayed_at_end: Whether this intent was executed at simulation end from pending queue
            data_quality_tracker: Optional tracker for data quality metrics

        Returns:
            TradeRecord with all execution details including fees, slippage, and gas
        """
        adapter_record = await self._execute_with_adapter_if_available(
            intent=intent,
            portfolio=portfolio,
            market_state=market_state,
            timestamp=timestamp,
            config=config,
            delayed_at_end=delayed_at_end,
            data_quality_tracker=data_quality_tracker,
        )
        if adapter_record is not None:
            return adapter_record

        self._refuse_unsupported_intent(intent)

        details = self._generic_intent_details(intent, portfolio, market_state, config)
        costs = await self._generic_execution_costs(
            details=details,
            market_state=market_state,
            timestamp=timestamp,
            config=config,
            data_quality_tracker=data_quality_tracker,
        )
        executed_price = self._get_executed_price(
            intent,
            market_state,
            costs.slippage_pct,
            details.intent_type,
        )

        # Calculate token flows
        tokens_in, tokens_out = self._calculate_token_flows(
            intent=intent,
            intent_type=details.intent_type,
            amount_usd=details.amount_usd,
            executed_price=executed_price,
            fee_usd=costs.fee_usd,
            slippage_usd=costs.slippage_usd,
            market_state=market_state,
        )

        # Create position delta if this intent creates/modifies a position
        position_delta = self._create_position_delta(
            intent=intent,
            intent_type=details.intent_type,
            protocol=details.protocol,
            tokens=details.tokens,
            executed_price=executed_price,
            timestamp=timestamp,
            market_state=market_state,
            strict_reproducibility=config.strict_reproducibility,
        )
        fill = self._build_generic_fill(
            intent=intent,
            details=details,
            costs=costs,
            timestamp=timestamp,
            executed_price=executed_price,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            position_delta=position_delta,
            delayed_at_end=delayed_at_end,
        )

        return self._apply_fill_and_return_trade(fill, portfolio, market_state, timestamp)

    async def _execute_with_adapter_if_available(
        self,
        intent: Any,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        timestamp: datetime,
        config: PnLBacktestConfig,
        delayed_at_end: bool,
        data_quality_tracker: DataQualityTracker | None,
    ) -> TradeRecord | None:
        if self._adapter is None:
            return None

        adapter_fill = self._adapter.execute_intent(intent, portfolio, market_state)
        if adapter_fill is None:
            logger.debug(f"Adapter '{self._adapter.adapter_name}' returned None, using generic execution")
            return None

        logger.debug(f"Using adapter '{self._adapter.adapter_name}' execution for intent")
        await self._prepare_adapter_fill_for_engine(
            adapter_fill,
            market_state,
            timestamp,
            config,
            delayed_at_end,
            data_quality_tracker,
        )
        return self._apply_fill_and_return_trade(adapter_fill, portfolio, market_state, timestamp)

    async def _prepare_adapter_fill_for_engine(
        self,
        adapter_fill: SimulatedFill,
        market_state: MarketState,
        timestamp: datetime,
        config: PnLBacktestConfig,
        delayed_at_end: bool,
        data_quality_tracker: DataQualityTracker | None,
    ) -> None:
        adapter_fill.delayed_at_end = delayed_at_end
        # Gas is engine-owned: adapters own protocol math but not the gas lane.
        # Failed fills skip gas because apply_fill zeroes their execution costs,
        # and gas resolution can raise when no chain-native gas asset price exists.
        if not adapter_fill.success:
            return
        (
            adapter_fill.gas_cost_usd,
            adapter_fill.gas_price_gwei,
            gas_gwei_source,
        ) = await self._resolve_gas_cost(
            intent_type=adapter_fill.intent_type,
            market_state=market_state,
            timestamp=timestamp,
            config=config,
            data_quality_tracker=data_quality_tracker,
        )
        if adapter_fill.metadata is None:
            adapter_fill.metadata = {}
        adapter_fill.metadata["gas_price_source"] = gas_gwei_source

    def _generic_intent_details(
        self,
        intent: Any,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        config: PnLBacktestConfig,
    ) -> _GenericIntentDetails:
        intent_type = self._get_intent_type(intent)
        protocol = self._get_intent_protocol(intent)
        chain = str(getattr(market_state, "chain", config.chain))
        tokens = self._get_intent_tokens(intent, chain=chain)
        amount_usd = self._get_intent_amount_usd(
            intent,
            market_state,
            strict_reproducibility=config.strict_reproducibility,
        )

        # Close targets resolve before fee/slippage: a perp full close
        # (size_usd=None) and a lending withdraw_all take their fee notional
        # from the matched position.
        close_resolution = self._resolve_position_close(intent, intent_type, portfolio, amount_usd, market_state)
        return _GenericIntentDetails(
            intent_type=intent_type,
            protocol=protocol,
            tokens=tokens,
            amount_usd=close_resolution.amount_usd,
            close_resolution=close_resolution,
        )

    async def _generic_execution_costs(
        self,
        details: _GenericIntentDetails,
        market_state: MarketState,
        timestamp: datetime,
        config: PnLBacktestConfig,
        data_quality_tracker: DataQualityTracker | None,
    ) -> _GenericExecutionCosts:
        fee_usd = self.get_fee_model(details.protocol).calculate_fee(
            intent_type=details.intent_type,
            amount_usd=details.amount_usd,
            market_state=market_state,
            protocol=details.protocol,
        )
        slippage_pct = self.get_slippage_model(details.protocol).calculate_slippage(
            intent_type=details.intent_type,
            amount_usd=details.amount_usd,
            market_state=market_state,
            protocol=details.protocol,
        )
        if details.close_resolution.failure_reason is not None:
            # Failed closes are recorded as rejected trades. Skip MEV/slippage/gas
            # resolution so missing ETH data cannot hide the failed-trade record.
            return _GenericExecutionCosts(
                fee_usd=fee_usd,
                slippage_pct=slippage_pct,
                slippage_usd=Decimal("0"),
                gas_cost_usd=Decimal("0"),
                gas_price_gwei=None,
                gas_gwei_source="rejected_fill",
                estimated_mev_cost_usd=None,
            )

        estimated_mev_cost_usd, slippage_pct = self._simulate_mev_impact(
            intent_type=details.intent_type,
            tokens=details.tokens,
            amount_usd=details.amount_usd,
            slippage_pct=slippage_pct,
            config=config,
        )
        gas_cost_usd, gas_price_gwei, gas_gwei_source = await self._resolve_gas_cost(
            intent_type=details.intent_type,
            market_state=market_state,
            timestamp=timestamp,
            config=config,
            data_quality_tracker=data_quality_tracker,
        )
        return _GenericExecutionCosts(
            fee_usd=fee_usd,
            slippage_pct=slippage_pct,
            slippage_usd=details.amount_usd * slippage_pct,
            gas_cost_usd=gas_cost_usd,
            gas_price_gwei=gas_price_gwei,
            gas_gwei_source=gas_gwei_source,
            estimated_mev_cost_usd=estimated_mev_cost_usd,
        )

    def _build_generic_fill(
        self,
        intent: Any,
        details: _GenericIntentDetails,
        costs: _GenericExecutionCosts,
        timestamp: datetime,
        executed_price: Decimal,
        tokens_in: dict[TokenRef, Decimal],
        tokens_out: dict[TokenRef, Decimal],
        position_delta: SimulatedPosition | None,
        delayed_at_end: bool,
    ) -> SimulatedFill:
        close_resolution = details.close_resolution
        return SimulatedFill(
            timestamp=timestamp,
            intent_type=details.intent_type,
            protocol=details.protocol,
            tokens=details.tokens,
            executed_price=executed_price,
            amount_usd=details.amount_usd,
            fee_usd=costs.fee_usd,
            slippage_usd=costs.slippage_usd,
            gas_cost_usd=costs.gas_cost_usd,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            success=close_resolution.failure_reason is None,
            position_delta=position_delta,
            position_close_id=close_resolution.position_close_id,
            position_reduce_id=close_resolution.position_reduce_id,
            position_reduce_amounts=self._generic_position_reduce_amounts(
                details.intent_type,
                close_resolution,
                tokens_in,
                tokens_out,
            ),
            metadata=self._generic_fill_metadata(intent, costs.slippage_pct, costs.gas_gwei_source, close_resolution),
            gas_price_gwei=costs.gas_price_gwei,
            estimated_mev_cost_usd=costs.estimated_mev_cost_usd,
            delayed_at_end=delayed_at_end,
        )

    @staticmethod
    def _generic_position_reduce_amounts(
        intent_type: IntentType,
        close_resolution: _CloseResolution,
        tokens_in: dict[TokenRef, Decimal],
        tokens_out: dict[TokenRef, Decimal],
    ) -> dict[TokenRef, Decimal]:
        # Ordinary partial lending reductions debit the position by the fill's
        # flow on the position side. Boundary reductions carry an explicit
        # principal map because the flow also realizes accrued interest.
        if close_resolution.reduce_amounts is not None:
            return close_resolution.reduce_amounts
        if not close_resolution.position_reduce_id:
            return {}
        return dict(tokens_out if intent_type == IntentType.REPAY else tokens_in)

    @staticmethod
    def _generic_fill_metadata(
        intent: Any,
        slippage_pct: Decimal,
        gas_gwei_source: str | None,
        close_resolution: _CloseResolution,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "intent": str(intent),
            "slippage_pct": str(slippage_pct),
            "gas_price_source": gas_gwei_source,
        }
        if close_resolution.failure_reason is not None:
            metadata["failure_reason"] = close_resolution.failure_reason
        if close_resolution.interest_usd != Decimal("0"):
            # str() round-trips Decimal losslessly; _calculate_trade_pnl reads
            # this back as the trade's realized interest PnL.
            metadata["interest_usd"] = str(close_resolution.interest_usd)
        return metadata

    def _apply_fill_and_return_trade(
        self,
        fill: SimulatedFill,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        timestamp: datetime,
    ) -> TradeRecord:
        # A False return means apply_fill already recorded a rejected trade
        # (with zeroed costs) and logged the reason at WARNING, so the success
        # execution log is skipped to avoid double-logging.
        applied = portfolio.apply_fill(fill, market_state=market_state, adapter=self._adapter)
        if applied:
            self._log_applied_fill(fill, timestamp)
        # Return the canonical record apply_fill appended to portfolio.trades.
        # It carries realized pnl_usd attribution, keeping callbacks and metrics
        # in lockstep instead of fabricating a second record with pnl=0.
        return portfolio.trades[-1]

    def _log_applied_fill(self, fill: SimulatedFill, timestamp: datetime) -> None:
        log_trade_execution(
            logger=logger,
            backtest_id=self._current_backtest_id,
            timestamp=timestamp,
            intent_type=fill.intent_type.value,
            protocol=fill.protocol,
            tokens=[token_ref_display(token) for token in fill.tokens],
            amount_usd=fill.amount_usd,
            fee_usd=fill.fee_usd,
            slippage_usd=fill.slippage_usd,
            gas_cost_usd=fill.gas_cost_usd,
            executed_price=fill.executed_price,
            mev_cost_usd=fill.estimated_mev_cost_usd,
        )

    def _simulate_mev_impact(
        self,
        intent_type: IntentType,
        tokens: list[TokenRef],
        amount_usd: Decimal,
        slippage_pct: Decimal,
        config: PnLBacktestConfig,
    ) -> tuple[Decimal | None, Decimal]:
        """Simulate MEV costs for an intent if the MEV simulator is enabled.

        Args:
            intent_type: Type of intent
            tokens: Tokens involved in the intent
            amount_usd: USD amount of the trade
            slippage_pct: Base slippage from the slippage model
            config: Backtest configuration

        Returns:
            Tuple of (estimated_mev_cost_usd or None when no simulator is
            configured, slippage_pct increased by MEV-induced slippage when
            the trade is sandwiched)
        """
        if self._mev_simulator is None:
            return None, slippage_pct

        # Get token symbols for MEV simulation
        token_in = token_ref_display(tokens[0]) if tokens else ""
        token_out = token_ref_display(tokens[1]) if len(tokens) > 1 else ""

        # Get gas price for inclusion delay simulation
        mev_gas_price = config.gas_price_gwei if config.include_gas_costs else None

        # Simulate MEV impact
        mev_result = self._mev_simulator.simulate_mev_cost(
            trade_amount_usd=amount_usd,
            token_in=token_in,
            token_out=token_out,
            gas_price_gwei=mev_gas_price,
            intent_type=intent_type,
        )

        # Add MEV-induced slippage to base slippage
        if mev_result.is_sandwiched:
            slippage_pct = slippage_pct + mev_result.additional_slippage_pct
            logger.debug(
                f"MEV simulation: Trade sandwiched, additional slippage "
                f"{mev_result.additional_slippage_pct * 100:.2f}%, "
                f"MEV cost ${mev_result.mev_cost_usd:.2f}"
            )

        return mev_result.mev_cost_usd, slippage_pct

    async def _resolve_gas_cost(
        self,
        intent_type: IntentType,
        market_state: MarketState,
        timestamp: datetime,
        config: PnLBacktestConfig,
        data_quality_tracker: DataQualityTracker | None,
    ) -> tuple[Decimal, Decimal | None, str | None]:
        """Resolve the simulated gas cost for an intent.

        Estimates gas units for the intent type, resolves the native gas
        asset price (:meth:`_resolve_gas_eth_price`) and the gas price in gwei
        (:meth:`_resolve_gas_price_gwei`), records tracking side effects
        (data-quality source, fallback usage, gas price records), and
        computes the final USD cost.

        Args:
            intent_type: Type of intent
            market_state: Current market state with prices
            timestamp: Time of execution
            config: Backtest configuration
            data_quality_tracker: Optional tracker for data quality metrics

        Returns:
            Tuple of (gas_cost_usd, gas_price_gwei, gas_gwei_source).
            Returns (Decimal("0"), None, None) when config.include_gas_costs
            is False.
        """
        if not config.include_gas_costs:
            return Decimal("0"), None, None

        # Estimate gas used based on intent type
        gas_used = self._estimate_gas_for_intent(intent_type)

        gas_asset_price, gas_price_source = self._resolve_gas_eth_price(config, market_state, timestamp)

        # Track gas price source in data quality metrics
        if data_quality_tracker is not None:
            data_quality_tracker.record_gas_price_source(gas_price_source)

        gas_price_gwei, gas_gwei_source = await self._resolve_gas_price_gwei(config, market_state, timestamp)

        # Track gas gwei source in fallback usage. Both static lanes count:
        # "chain_default" (chain-aware registry default, VIB-5088) is an
        # honest fabrication but a fabrication nonetheless, and "config"
        # (user-set static value) is still not historical data -- the
        # compliance flag means "gas did not track historical conditions".
        if gas_gwei_source in ("config", "chain_default"):
            self._track_fallback("default_gas_price")

        # Calculate gas cost: gas_used * gas_price_gwei * native_gas_asset_price / 1e9
        gas_cost_native = Decimal(gas_used) * gas_price_gwei / Decimal("1000000000")
        gas_cost_usd = gas_cost_native * gas_asset_price

        # Log gas cost details at debug level for troubleshooting
        logger.debug(
            "Gas cost: %d gas used × %.1f gwei (%s) × $%.2f gas asset (%s) = $%.4f",
            gas_used,
            gas_price_gwei,
            gas_gwei_source,
            gas_asset_price,
            gas_price_source,
            gas_cost_usd,
        )

        # Record gas price if tracking is enabled
        if self._gas_price_records is not None and gas_price_gwei is not None:
            self._gas_price_records.append(
                GasPriceRecord(
                    timestamp=timestamp,
                    gwei=gas_price_gwei,
                    source=gas_gwei_source or "unknown",
                    usd_cost=gas_cost_usd,
                    # Back-compat field name; the value is the chain's
                    # native gas asset price after VIB-5509.
                    eth_price_usd=gas_asset_price,
                )
            )

        return gas_cost_usd, gas_price_gwei, gas_gwei_source

    def _resolve_gas_eth_price(
        self,
        config: PnLBacktestConfig,
        market_state: MarketState,
        timestamp: datetime,
    ) -> tuple[Decimal, str]:
        """Resolve the native gas asset price used for gas-cost conversion.

        Priority order:
        1. gas_eth_price_override (takes precedence for reproducibility/testing)
        2. Historical native gas asset price (if use_historical_gas_prices enabled)
        3. Current market price (native or wrapped-native from market_state)
        No silent fallback - fail if price unavailable.

        Args:
            config: Backtest configuration
            market_state: Current market state with prices
            timestamp: Time of execution

        Returns:
            Tuple of (gas_asset_price, gas_price_source)

        Raises:
            ValueError: If no native gas asset price is available from the selected
                source and no gas_eth_price_override is set.
        """
        if config.gas_eth_price_override is not None:
            gas_asset_price = config.gas_eth_price_override
            logger.debug(
                "Gas asset price: Using override value $%.2f",
                gas_asset_price,
            )
            return gas_asset_price, "override"

        gas_symbols = self._gas_asset_price_symbols(config.chain)
        if not gas_symbols:
            raise ValueError(
                f"Gas asset price: no registered native gas asset for chain {config.chain!r}. "
                "Set gas_eth_price_override to provide an explicit gas asset price."
            )

        gas_price_source = "historical" if config.use_historical_gas_prices else "market"
        for symbol in gas_symbols:
            resolved_price = self._market_gas_asset_price(market_state, symbol)
            if resolved_price is None:
                continue
            logger.debug(
                "Gas asset price: Using %s %s price $%.2f at %s",
                gas_price_source,
                symbol,
                resolved_price,
                timestamp.isoformat(),
            )
            return resolved_price, gas_price_source

        joined_symbols = "/".join(gas_symbols)
        if config.use_historical_gas_prices:
            prefix = "Historical price requested but "
        else:
            prefix = ""
        strict_detail = (
            " In strict_reproducibility mode, gas asset price must be available."
            if config.strict_reproducibility
            else ""
        )
        raise ValueError(
            f"Gas asset price: {prefix}{joined_symbols} not available in market state at "
            f"{timestamp.isoformat()}.{strict_detail} Set gas_eth_price_override to provide "
            "an explicit gas asset price for gas calculations."
        ) from None

    def _market_gas_asset_price(self, market_state: MarketState, symbol: str) -> Decimal | None:
        """Price a gas-asset symbol from ``market_state``.

        Address-native market states (VIB-5508) keep plain-symbol reads an
        honest miss, so after the symbol lookup misses, retry through the
        engine's registered ``{SYMBOL: (chain, address)}`` map — the engine
        must be able to consume the data it registered itself (the gas lane's
        analogue of the providers' ``register_token_addresses`` ingress).
        """
        try:
            return market_state.get_price(symbol)
        except KeyError:
            pass
        token_key = (self.token_addresses or {}).get(symbol.upper())
        if token_key is None:
            return None
        try:
            return market_state.get_price(token_key)
        except KeyError:
            return None

    def _gas_asset_price_symbols(self, chain: str) -> tuple[str, ...]:
        """Return ordered symbols that can price ``chain``'s native gas asset."""
        descriptor = ChainRegistry.try_resolve(chain)
        if descriptor is None:
            return ()

        symbols: list[str] = [descriptor.native.symbol, *descriptor.native.accepted_symbols]
        if descriptor.native.wrapped_symbol:
            symbols.append(descriptor.native.wrapped_symbol)
        accepted = native_symbols_for(chain)
        symbols.extend(accepted)

        ordered: list[str] = []
        seen: set[str] = set()
        for symbol in symbols:
            upper = symbol.upper()
            if upper in seen:
                continue
            seen.add(upper)
            ordered.append(upper)
        return tuple(ordered)

    async def _resolve_gas_price_gwei(
        self,
        config: PnLBacktestConfig,
        market_state: MarketState,
        timestamp: datetime,
    ) -> tuple[Decimal, str]:
        """Resolve the gas price in gwei for gas-cost simulation.

        Priority order:
        1. Historical gas price from gas_provider (if use_historical_gas_gwei=True)
        2. MarketState.gas_price_gwei (if populated by data provider)
        3. config.gas_price_gwei -- source "config" when user-set, or
           "chain_default" when it is the chain-aware registry default
           (VIB-5088). Both are static fabrications for compliance purposes;
           "chain_default" additionally refuses to resolve in institutional
           mode (no user value + no historical datum = raise, never fabricate).

        Args:
            config: Backtest configuration
            market_state: Current market state
            timestamp: Time of execution

        Returns:
            Tuple of (gas_price_gwei, gas_gwei_source)

        Raises:
            DataSourceUnavailableError: In institutional mode when the gas
                price would fall back to the chain-registry default (i.e. no
                user-set value, no historical/market datum).
        """
        # config.gas_price_gwei is resolved to a Decimal in __post_init__.
        assert config.gas_price_gwei is not None
        default_resolution = _GasGweiResolution(
            gas_price_gwei=config.gas_price_gwei,
            source="chain_default" if config.gas_price_gwei_is_default else "config",
        )
        if config.use_historical_gas_gwei and self.gas_provider is not None:
            resolution = await self._historical_gas_gwei_or_fallback(
                config, market_state, timestamp, default_resolution
            )
        elif config.use_historical_gas_gwei:
            resolution = self._market_state_gas_gwei_or_default(
                market_state,
                default_resolution,
                missing_provider_warning=True,
            )
        else:
            resolution = self._market_state_gas_gwei_or_default(market_state, default_resolution)

        self._raise_if_institutional_gas_default(config, timestamp, resolution)
        return resolution.gas_price_gwei, resolution.source

    async def _historical_gas_gwei_or_fallback(
        self,
        config: PnLBacktestConfig,
        market_state: MarketState,
        timestamp: datetime,
        default_resolution: _GasGweiResolution,
    ) -> _GasGweiResolution:
        gas_provider = self.gas_provider
        assert gas_provider is not None
        try:
            historical_gas: GasPrice = await gas_provider.get_gas_price(
                timestamp=timestamp,
                chain=config.chain,
            )
        except Exception as exc:
            logger.warning(
                "Failed to get historical gas price, falling back to market_state/config: %s",
                str(exc),
            )
            return self._market_state_gas_gwei_or_default(market_state, default_resolution)

        logger.debug(
            "Gas gwei: Using historical gas price %.1f gwei (source: %s) at %s",
            historical_gas.effective_gas_price_gwei,
            historical_gas.source,
            timestamp.isoformat(),
        )
        return _GasGweiResolution(
            gas_price_gwei=historical_gas.effective_gas_price_gwei,
            source=f"historical_gas:{historical_gas.source}",
        )

    @staticmethod
    def _market_state_gas_gwei_or_default(
        market_state: MarketState,
        default_resolution: _GasGweiResolution,
        *,
        missing_provider_warning: bool = False,
    ) -> _GasGweiResolution:
        if market_state.gas_price_gwei is not None:
            return _GasGweiResolution(gas_price_gwei=market_state.gas_price_gwei, source="market_state")
        if missing_provider_warning:
            logger.warning(
                "use_historical_gas_gwei=True but no gas_provider and "
                "no gas_price_gwei in market_state, using config default %.1f gwei",
                default_resolution.gas_price_gwei,
            )
        return default_resolution

    @staticmethod
    def _raise_if_institutional_gas_default(
        config: PnLBacktestConfig,
        timestamp: datetime,
        resolution: _GasGweiResolution,
    ) -> None:
        if resolution.source != "chain_default" or not config.institutional_mode:
            return
        raise DataSourceUnavailableError(
            data_type="gas_price",
            identifier=config.chain,
            remediation=(
                "Set config.gas_price_gwei explicitly, enable "
                "use_historical_gas_gwei with a gas_provider, or use a "
                "data provider that populates MarketState.gas_price_gwei."
            ),
            message=(
                f"Institutional mode: gas price for '{config.chain}' at "
                f"{timestamp.isoformat()} would fall back to the chain-registry "
                f"default ({config.gas_price_gwei} gwei) and institutional mode "
                f"refuses to fabricate execution costs"
            ),
        )

    def _refuse_unsupported_intent(self, intent: Any) -> None:
        """Fail loud when the generic lane cannot simulate ``intent``.

        Runs after adapter dispatch declined the intent, so an adapter that
        genuinely handles a type outside the generic envelope is unaffected.
        Anything else outside :data:`GENERIC_SIMULATED_INTENT_TYPES` used to
        become a costed no-op (fees/gas charged, zero token flows, no
        position); it is now a fatal, run-stopping error — the design
        decision is that no backtest runs past an intent it cannot simulate.
        """
        hint: str | None = None
        if isinstance(intent, list | tuple):
            # A collection is never a simulable intent — refuse without
            # running type classification on it.
            label = f"{type(intent).__name__} of {len(intent)} intents"
            hint = "decide() returned multiple intents; the PnL engine executes a single intent per tick (VIB-5094)."
        else:
            intent_type = self._get_intent_type(intent)
            if intent_type in _engine_helpers.GENERIC_SIMULATED_INTENT_TYPES:
                return
            declared = getattr(intent, "intent_type", None)
            declared_label = getattr(declared, "value", declared)
            label = f"{declared_label or intent_type.value} ({type(intent).__name__})"

        raise UnsupportedIntentError(
            label,
            tuple(sorted(t.value for t in _engine_helpers.GENERIC_SIMULATED_INTENT_TYPES)),
            hint,
        )

    def _get_intent_type(self, intent: Any) -> IntentType:
        """Extract the IntentType from an intent object. Delegates to intent_extraction module."""
        from .intent_extraction import get_intent_type

        return get_intent_type(intent)

    def _get_intent_protocol(self, intent: Any) -> str:
        """Extract the protocol from an intent object. Delegates to intent_extraction module."""
        from .intent_extraction import get_intent_protocol

        return get_intent_protocol(intent)

    def _get_intent_tokens(
        self,
        intent: Any,
        aliases: dict[str, str] | None = None,
        chain: str | None = None,
    ) -> list[TokenRef]:
        """Extract the tokens involved in an intent. Delegates to intent_extraction module.

        Address string tokens are normalized to their ``(chain, address)``
        identity; legacy symbol-only backtests keep their symbol labels.
        """
        from .intent_extraction import get_intent_tokens

        tokens: list[TokenRef] = []
        tokens.extend(get_intent_tokens(intent))
        _ = aliases
        return [
            _engine_helpers._normalize_token(token, chain)
            if isinstance(token, str) and is_address_like(token)
            else token
            for token in tokens
        ]

    def _get_intent_amount_usd(
        self,
        intent: Any,
        market_state: MarketState,
        strict_reproducibility: bool = False,
    ) -> Decimal:
        """Extract or calculate the USD amount for an intent. Delegates to intent_extraction module."""
        from .intent_extraction import get_intent_amount_usd

        return get_intent_amount_usd(
            intent,
            market_state,
            strict_reproducibility=strict_reproducibility,
            track_fallback=self._track_fallback,
        )

    def _estimate_gas_for_intent(self, intent_type: IntentType) -> int:
        """Estimate gas usage for an intent type. Delegates to intent_extraction module."""
        from .intent_extraction import estimate_gas_for_intent

        return estimate_gas_for_intent(intent_type)

    def _get_executed_price(
        self,
        intent: Any,
        market_state: MarketState,
        slippage_pct: Decimal,
        intent_type: IntentType,
    ) -> Decimal:
        """Get the executed price for an intent after slippage. Delegates to intent_extraction module."""
        from .intent_extraction import get_executed_price

        return get_executed_price(intent, market_state, slippage_pct, intent_type)

    def _calculate_token_flows(
        self,
        intent: Any,
        intent_type: IntentType,
        amount_usd: Decimal,
        executed_price: Decimal,
        fee_usd: Decimal,
        slippage_usd: Decimal,
        market_state: MarketState,
    ) -> tuple[dict[TokenRef, Decimal], dict[TokenRef, Decimal]]:
        """Calculate the token inflows and outflows for an intent.

        Dispatches to per-intent-type helpers in
        :mod:`almanak.framework.backtesting.pnl._engine_helpers` (Phase 6C.3).
        Unmatched intent types (HOLD, PERP, ...) return empty flow dicts --
        their balances are tracked via collateral rather than explicit token
        flows.

        Args:
            intent: Intent object
            intent_type: Type of intent
            amount_usd: USD amount of the trade
            executed_price: Executed price (unused; kept for API stability)
            fee_usd: Fee in USD (only consumed by SWAP)
            slippage_usd: Slippage cost in USD (only consumed by SWAP)
            market_state: Market state for prices

        Returns:
            Tuple of (tokens_in, tokens_out) dicts
        """
        del executed_price  # kept for backwards-compatible signature
        return _engine_helpers.calculate_token_flows(
            intent=intent,
            intent_type=intent_type,
            amount_usd=amount_usd,
            fee_usd=fee_usd,
            slippage_usd=slippage_usd,
            market_state=market_state,
        )

    def _create_position_delta(
        self,
        intent: Any,
        intent_type: IntentType,
        protocol: str,
        tokens: list[TokenRef],
        executed_price: Decimal,
        timestamp: datetime,
        market_state: MarketState,
        strict_reproducibility: bool = False,
    ) -> SimulatedPosition | None:
        """Create a position delta for intents that create positions.

        Dispatches to per-intent-type handlers through
        :data:`_POSITION_DELTA_HANDLERS` (mirroring
        :func:`_engine_helpers.calculate_token_flows`). Intent types without
        a handler (SWAP, HOLD, closes, ...) do not create positions and
        return ``None`` without extracting a USD amount.

        Args:
            intent: Intent object
            intent_type: Type of intent
            protocol: Protocol name
            tokens: Tokens involved
            executed_price: Executed price
            timestamp: Time of execution
            market_state: Market state
            strict_reproducibility: If True, raise on missing USD amount

        Returns:
            SimulatedPosition if a position is created, None otherwise
        """
        handler_name = _POSITION_DELTA_HANDLERS.get(intent_type)
        if handler_name is None:
            return None
        handler = getattr(self, handler_name)
        return handler(intent, protocol, tokens, executed_price, timestamp, market_state, strict_reproducibility)

    def _lp_open_delta(
        self,
        intent: Any,
        protocol: str,
        tokens: list[TokenRef],
        executed_price: Decimal,
        timestamp: datetime,
        market_state: MarketState,
        strict_reproducibility: bool,
    ) -> SimulatedPosition:
        """Create the simulated LP position for an LP_OPEN intent.

        Stores true Uniswap V3 liquidity (L) derived from the intent's USD
        notional at the entry price ratio: ``_mark_lp_position`` feeds
        ``position.liquidity`` into ``calculate_il_v3``, so any other unit
        in that field mints or burns value at the open tick (blueprint 31
        section 4 conservation; VIB-5096). Entry token amounts come from the
        same V3 math and ``entry_price`` is the token0/token1 ratio, matching
        the ``SimulatedPosition.lp`` contract. Tokens whose price is missing
        from ``market_state`` (or non-positive: bad data) raise in strict
        mode and otherwise fall back to a tracked $1 price, keeping the V3
        math anchored -- the L-units field never holds a USD notional.
        """
        from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
            ImpermanentLossCalculator,
        )
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        from .intent_extraction import get_lp_tick_range

        del executed_price  # the position stores the token0/token1 ratio instead

        token0 = tokens[0] if len(tokens) > 0 else "WETH"
        token1 = tokens[1] if len(tokens) > 1 else "USDC"

        amount_usd = self._get_intent_amount_usd(intent, market_state, strict_reproducibility=strict_reproducibility)

        # Get fee tier
        fee_tier = getattr(intent, "fee_tier", Decimal("0.003"))
        if isinstance(fee_tier, int | float):
            fee_tier = Decimal(str(fee_tier))

        calculator = ImpermanentLossCalculator()
        tick_lower, tick_upper = get_lp_tick_range(intent, calculator.price_to_tick)
        if tick_upper <= tick_lower:
            # Degenerate range: widen by one tick so the position has a valid
            # V3 range and non-zero value (same handling as the adapter lane).
            tick_upper = tick_lower + 1

        def price_or_fallback(token: TokenRef) -> Decimal:
            try:
                price: Decimal | None = market_state.get_price(token)
            except KeyError:
                price = None
            if price is not None and price > 0:
                return price
            if strict_reproducibility:
                msg = (
                    f"Cannot determine the LP entry price ratio: no positive price available for "
                    f"'{token_ref_display(token)}'. "
                    "Set strict_reproducibility=False to fall back to $1."
                )
                raise ValueError(msg)
            self._track_fallback("hardcoded_price")
            return Decimal("1")

        price0 = price_or_fallback(token0)
        price1 = price_or_fallback(token1)

        # Entry price ratio: token0 in terms of token1 -- the unit every LP
        # valuation path compares against (SimulatedPosition.lp contract).
        entry_price_ratio = price0 / price1

        liquidity = calculator.liquidity_for_target_value(
            value_token1=amount_usd / price1,
            price=entry_price_ratio,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )
        _, amount0, amount1 = calculator.calculate_il_v3(
            entry_price=entry_price_ratio,
            current_price=entry_price_ratio,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=liquidity,
        )

        position = SimulatedPosition.lp(
            token0=token0,
            token1=token1,
            amount0=amount0,
            amount1=amount1,
            liquidity=liquidity,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            fee_tier=fee_tier,
            entry_price=entry_price_ratio,
            entry_time=timestamp,
            protocol=protocol,
        )
        # Entry amounts anchor the IL hold-value baseline (same contract as
        # the adapter lane's _execute_lp_open).
        position.metadata["entry_amounts"] = {
            token_ref_display(token0): str(amount0),
            token_ref_display(token1): str(amount1),
        }
        position.metadata["entry_price_ratio"] = str(entry_price_ratio)
        return position

    def _supply_delta(
        self,
        intent: Any,
        protocol: str,
        tokens: list[TokenRef],
        executed_price: Decimal,
        timestamp: datetime,
        market_state: MarketState,
        strict_reproducibility: bool,
    ) -> SimulatedPosition:
        """Create the simulated lending position for a SUPPLY intent."""
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        token = tokens[0] if tokens else "WETH"
        amount_usd = self._get_intent_amount_usd(intent, market_state, strict_reproducibility=strict_reproducibility)

        try:
            price = market_state.get_price(token)
            amount = amount_usd / price
        except KeyError:
            amount = amount_usd

        # Get APY if available
        apy = getattr(intent, "apy", Decimal("0.05"))
        if isinstance(apy, int | float):
            apy = Decimal(str(apy))

        return SimulatedPosition.supply(
            token=token,
            amount=amount,
            apy=apy,
            entry_price=executed_price,
            entry_time=timestamp,
            protocol=protocol,
        )

    def _vault_deposit_delta(
        self,
        intent: Any,
        protocol: str,
        tokens: list[TokenRef],
        executed_price: Decimal,
        timestamp: datetime,
        market_state: MarketState,
        strict_reproducibility: bool,
    ) -> SimulatedPosition:
        """Create the simulated vault supply position for a VAULT_DEPOSIT intent."""
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        deposit_tok = getattr(intent, "deposit_token", None)
        chain = str(getattr(market_state, "chain", DEFAULT_CHAIN))
        if deposit_tok:
            token = _engine_helpers._normalize_token(deposit_tok, chain)
        else:
            token = tokens[0] if tokens else _engine_helpers._normalize_token("USDC", chain)
            logger.warning(
                "Vault deposit missing deposit_token, defaulting to %s — set deposit_token for accurate backtesting",
                token_ref_display(token),
            )
        amount_usd = self._get_intent_amount_usd(intent, market_state, strict_reproducibility=strict_reproducibility)

        try:
            price = market_state.get_price(token)
            amount = amount_usd / price if price > 0 else amount_usd
        except KeyError:
            amount = amount_usd

        # ERC-4626 vault yield: pending PPFS-curve replay via gateway
        # MarketService.GetSharePriceHistory (VIB-3367). Until that ships,
        # honour the strategy-supplied `apy` field on the intent and fall
        # back to a neutral 5% surrogate so existing backtests keep running.
        apy = getattr(intent, "apy", Decimal("0.05"))
        if isinstance(apy, int | float):
            apy = Decimal(str(apy))

        return SimulatedPosition.supply(
            token=token,
            amount=amount,
            apy=apy,
            entry_price=executed_price,
            entry_time=timestamp,
            protocol=protocol,
        )

    def _borrow_delta(
        self,
        intent: Any,
        protocol: str,
        tokens: list[TokenRef],
        executed_price: Decimal,
        timestamp: datetime,
        market_state: MarketState,
        strict_reproducibility: bool,
    ) -> SimulatedPosition:
        """Create the simulated borrow position for a BORROW intent."""
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        token = tokens[0] if tokens else "USDC"
        amount_usd = self._get_intent_amount_usd(intent, market_state, strict_reproducibility=strict_reproducibility)

        try:
            price = market_state.get_price(token)
            amount = amount_usd / price
        except KeyError:
            amount = amount_usd

        # Get APY if available
        apy = getattr(intent, "apy", getattr(intent, "borrow_apy", Decimal("0.08")))
        if isinstance(apy, int | float):
            apy = Decimal(str(apy))

        return SimulatedPosition.borrow(
            token=token,
            amount=amount,
            apy=apy,
            entry_price=executed_price,
            entry_time=timestamp,
            protocol=protocol,
        )

    def _perp_open_delta(
        self,
        intent: Any,
        protocol: str,
        tokens: list[TokenRef],
        executed_price: Decimal,
        timestamp: datetime,
        market_state: MarketState,
        strict_reproducibility: bool,
    ) -> SimulatedPosition:
        """Create the simulated position for a PERP_OPEN intent.

        The position's collateral comes from the intent's declared collateral
        (or size_usd / leverage), NOT from amount_usd — amount_usd is the
        notional and is only the legacy fallback for duck-typed intents
        without perp fields.
        """
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        from .intent_extraction import get_perp_open_params, intent_is_long

        token = tokens[0] if tokens else "WETH"
        amount_usd = self._get_intent_amount_usd(intent, market_state, strict_reproducibility=strict_reproducibility)
        collateral_usd, leverage = get_perp_open_params(
            intent,
            market_state,
            fallback_amount_usd=amount_usd,
            strict_reproducibility=strict_reproducibility,
            track_fallback=self._track_fallback,
        )

        factory = SimulatedPosition.perp_long if intent_is_long(intent) else SimulatedPosition.perp_short
        return factory(
            token=token,
            collateral_usd=collateral_usd,
            leverage=leverage,
            entry_price=executed_price,
            entry_time=timestamp,
            protocol=protocol,
        )

    def _resolve_position_close(
        self,
        intent: Any,
        intent_type: IntentType,
        portfolio: SimulatedPortfolio,
        amount_usd: Decimal,
        market_state: MarketState,
    ) -> _CloseResolution:
        """Resolve the position a closing intent targets and its notional.

        Perp closes and lending withdraws match against the portfolio's
        open positions (venue position ids never equal simulated ids);
        every other intent type keeps the attribute-based id lookup and
        its extracted notional.
        """
        if intent_type == IntentType.PERP_CLOSE:
            position_close_id, amount_usd = self._resolve_perp_close(intent, portfolio, amount_usd)
            return _CloseResolution(amount_usd=amount_usd, position_close_id=position_close_id)
        if intent_type == IntentType.WITHDRAW:
            return self._resolve_withdraw_close(intent, portfolio, amount_usd, market_state)
        if intent_type == IntentType.REPAY:
            return self._resolve_repay_close(intent, portfolio, amount_usd, market_state)
        return _CloseResolution(amount_usd=amount_usd, position_close_id=self._get_position_close_id(intent))

    def _resolve_withdraw_close(
        self,
        intent: Any,
        portfolio: SimulatedPortfolio,
        amount_usd: Decimal,
        market_state: MarketState,
    ) -> _CloseResolution:
        """Resolve the SUPPLY position a WITHDRAW intent closes or reduces.

        The withdrawn principal must come OUT of the matched supply
        position (VIB-5097): without this linkage the inflow double-counts
        against the still-open position. Matching follows the perp-close
        pattern (exact-id precedence, then FIFO by (token, protocol) via
        :func:`find_lending_close_position_id`).

        Semantics:

        - No open supply position matches -> the fill is rejected
          (``failure_reason``), zero state mutation.
        - ``withdraw_all`` / unresolvable amount / amount >= principal ->
          full close: notional = principal value + accrued interest, the
          interest is realized PnL (mirroring the perp close credit).
        - amount < principal -> partial: the position's principal is
          reduced by the withdrawn token amount; accrued interest stays on
          the position and is realized when it eventually closes in full.
        """
        from .intent_extraction import find_lending_close_position_id

        position_close_id = find_lending_close_position_id(intent, portfolio.positions)
        position = portfolio.get_position(position_close_id) if position_close_id else None
        if position is None:
            return _CloseResolution(
                amount_usd=amount_usd,
                failure_reason="WITHDRAW matched no open supply position to withdraw from",
            )

        return self._resolve_interest_bearing_close(
            action="WITHDRAW",
            amount_usd=amount_usd,
            position=position,
            value=self._position_value_with_interest(position, market_state),
            force_full=bool(getattr(intent, "withdraw_all", False)),
            interest_sign=Decimal("1"),
            matched_label="matched supply",
            full_label="full balance",
            interest_label="accrued interest",
            interest_only_label="supply position",
            threshold_action="withdraw",
        )

    def _resolve_repay_close(
        self,
        intent: Any,
        portfolio: SimulatedPortfolio,
        amount_usd: Decimal,
        market_state: MarketState,
    ) -> _CloseResolution:
        """Resolve the BORROW position a REPAY intent closes or reduces.

        Debt-side mirror of :meth:`_resolve_withdraw_close` (VIB-5098): the
        repaid principal must come OUT of the matched BORROW position --
        without this linkage the outflow debits cash while the debt keeps
        counting against equity (a $2,000 repay burned ~$2,000). Matching
        targets BORROW positions only via
        :func:`find_borrow_close_position_id` (exact-id precedence, then
        FIFO by (token, protocol), fail closed).

        Semantics:

        - No open borrow position matches -> the fill is rejected
          (``failure_reason``), zero state mutation.
        - ``repay_full`` / unresolvable amount / amount >= debt principal ->
          full close: notional = debt principal value + accrued borrow
          interest, the interest realizing as NEGATIVE PnL (the cost of
          having borrowed -- sign-mirrored from the withdraw credit).
        - amount < debt principal -> partial: the position's principal is
          reduced by the repaid token amount; accrued borrow interest stays
          on the position and is realized when it eventually closes in full.
        """
        from .intent_extraction import find_borrow_close_position_id

        position_close_id = find_borrow_close_position_id(intent, portfolio.positions)
        position = portfolio.get_position(position_close_id) if position_close_id else None
        if position is None:
            return _CloseResolution(
                amount_usd=amount_usd,
                failure_reason="REPAY matched no open borrow position to repay",
            )

        return self._resolve_interest_bearing_close(
            action="REPAY",
            amount_usd=amount_usd,
            position=position,
            value=self._position_value_with_interest(position, market_state),
            force_full=bool(getattr(intent, "repay_full", False)) or bool(getattr(intent, "repay_all", False)),
            interest_sign=Decimal("-1"),
            matched_label="matched debt",
            full_label="full debt",
            interest_label="accrued borrow interest",
            interest_only_label="borrow position",
            threshold_action="repay",
        )

    def _position_value_with_interest(self, position: SimulatedPosition, market_state: MarketState) -> _PositionValue:
        price = self._position_principal_price(position, market_state)
        principal_usd = position.total_amount * price
        return _PositionValue(
            principal_usd=principal_usd,
            total_usd=principal_usd + position.interest_accrued,
        )

    @staticmethod
    def _position_principal_price(position: SimulatedPosition, market_state: MarketState) -> Decimal:
        try:
            price = market_state.get_price(position.primary_token)
        except KeyError:
            price = position.entry_price
        return price if price > Decimal("0") else position.entry_price

    def _resolve_interest_bearing_close(
        self,
        *,
        action: str,
        amount_usd: Decimal,
        position: SimulatedPosition,
        value: _PositionValue,
        force_full: bool,
        interest_sign: Decimal,
        matched_label: str,
        full_label: str,
        interest_label: str,
        interest_only_label: str,
        threshold_action: str,
    ) -> _CloseResolution:
        full_close_floor = value.total_usd * (Decimal("1") - Decimal("1E-9"))
        if force_full or amount_usd <= Decimal("0") or amount_usd >= full_close_floor:
            self._warn_if_close_amount_exceeds_full_value(
                action,
                amount_usd,
                value,
                matched_label,
                interest_label,
                full_label,
                position.interest_accrued,
            )
            return _CloseResolution(
                amount_usd=value.total_usd,
                position_close_id=position.position_id,
                interest_usd=interest_sign * position.interest_accrued,
            )

        if amount_usd <= value.principal_usd:
            return _CloseResolution(amount_usd=amount_usd, position_reduce_id=position.position_id)

        return self._resolve_boundary_interest_partial(
            action=action,
            amount_usd=amount_usd,
            position=position,
            principal_usd=value.principal_usd,
            interest_sign=interest_sign,
            interest_only_label=interest_only_label,
            interest_label=interest_label,
            threshold_action=threshold_action,
        )

    @staticmethod
    def _warn_if_close_amount_exceeds_full_value(
        action: str,
        amount_usd: Decimal,
        value: _PositionValue,
        matched_label: str,
        interest_label: str,
        full_label: str,
        interest_accrued: Decimal,
    ) -> None:
        if amount_usd <= value.total_usd:
            return
        logger.warning(
            "%s amount $%s exceeds %s $%s (principal $%s + %s $%s); capping to the %s",
            action,
            amount_usd,
            matched_label,
            value.total_usd,
            value.principal_usd,
            interest_label,
            interest_accrued,
            full_label,
        )

    def _resolve_boundary_interest_partial(
        self,
        *,
        action: str,
        amount_usd: Decimal,
        position: SimulatedPosition,
        principal_usd: Decimal,
        interest_sign: Decimal,
        interest_only_label: str,
        interest_label: str,
        threshold_action: str,
    ) -> _CloseResolution:
        principal_tokens = self._positive_position_amounts(position)
        if not principal_tokens:
            return _CloseResolution(
                amount_usd=amount_usd,
                failure_reason=(
                    f"{action} ${amount_usd} cannot partially settle an interest-only {interest_only_label} "
                    f"(0 principal, ${position.interest_accrued} {interest_label}); "
                    f"{threshold_action} >= the accrued interest to close it"
                ),
            )
        interest_paid = amount_usd - principal_usd
        return _CloseResolution(
            amount_usd=amount_usd,
            position_reduce_id=position.position_id,
            interest_usd=interest_sign * interest_paid,
            reduce_amounts=principal_tokens,
        )

    @staticmethod
    def _positive_position_amounts(position: SimulatedPosition) -> dict[TokenRef, Decimal]:
        return {token: amount for token, amount in position.amounts.items() if amount > Decimal("0")}

    def _resolve_perp_close(
        self,
        intent: Any,
        portfolio: SimulatedPortfolio,
        amount_usd: Decimal,
    ) -> tuple[str | None, Decimal]:
        """Resolve the simulated position a PERP_CLOSE targets and its notional.

        The simulated close machinery is all-or-nothing: a matched position is
        closed in full. A partial ``size_usd`` is honoured as the fee notional
        but logged, since the position itself still closes entirely.

        Args:
            intent: PERP_CLOSE intent object
            portfolio: Portfolio whose open positions are matched against
            amount_usd: Notional extracted from the intent (0 for full closes)

        Returns:
            Tuple of (matched position id or None, effective close notional)
        """
        from .intent_extraction import find_perp_close_position_id

        position_close_id = find_perp_close_position_id(intent, portfolio.positions)
        if position_close_id is None:
            return None, amount_usd
        position = portfolio.get_position(position_close_id)
        if position is None:
            return position_close_id, amount_usd
        if amount_usd <= 0:
            amount_usd = position.notional_usd
        elif amount_usd < position.notional_usd:
            logger.warning(
                "PERP_CLOSE size_usd=%s is below position notional %s; the simulated close "
                "is all-or-nothing and closes the full position",
                amount_usd,
                position.notional_usd,
            )
        elif amount_usd > position.notional_usd:
            # Fees/slippage cannot be charged on notional that does not exist.
            logger.warning(
                "PERP_CLOSE size_usd=%s exceeds matched position notional %s; "
                "capping the close notional to the position",
                amount_usd,
                position.notional_usd,
            )
            amount_usd = position.notional_usd
        return position_close_id, amount_usd

    def _get_position_close_id(self, intent: Any) -> str | None:
        """Get the position ID to close for closing intents.

        Args:
            intent: Intent object

        Returns:
            Position ID string if closing a position, None otherwise
        """
        # Check for position_id attribute
        for attr in ["position_id", "position_to_close", "close_position_id"]:
            if hasattr(intent, attr):
                value = getattr(intent, attr)
                if value and isinstance(value, str):
                    position_id: str = value
                    return position_id

        return None

    def _calculate_metrics(
        self,
        portfolio: SimulatedPortfolio,
        trades: list[TradeRecord],
        config: PnLBacktestConfig,
    ) -> BacktestMetrics:
        """Calculate comprehensive backtest metrics. Delegates to metrics_calculator module."""
        from .metrics_calculator import calculate_metrics

        return calculate_metrics(portfolio, trades, config)

    def _calculate_returns(self, values: list[Decimal]) -> list[Decimal]:
        """Calculate period-over-period returns. Delegates to metrics_calculator module."""
        from .metrics_calculator import calculate_returns

        return calculate_returns(values)

    def _create_gas_price_summary(
        self,
        trades: list[TradeRecord],
    ) -> GasPriceSummary | None:
        """Create gas price summary from trade records. Delegates to metrics_calculator module."""
        from .metrics_calculator import create_gas_price_summary

        return create_gas_price_summary(trades)

    def _calculate_volatility(
        self,
        returns: list[Decimal],
        trading_days: Decimal,
    ) -> Decimal:
        """Calculate annualized volatility. Delegates to metrics_calculator module."""
        from .metrics_calculator import calculate_volatility

        return calculate_volatility(returns, trading_days)

    def _calculate_sharpe_ratio(
        self,
        returns: list[Decimal],
        volatility: Decimal,
        risk_free_rate: Decimal,
        trading_days: Decimal,
    ) -> Decimal:
        """Calculate the Sharpe ratio. Delegates to metrics_calculator module."""
        from .metrics_calculator import calculate_sharpe_ratio

        return calculate_sharpe_ratio(returns, volatility, risk_free_rate, trading_days)

    def _calculate_sortino_ratio(
        self,
        returns: list[Decimal],
        risk_free_rate: Decimal,
        trading_days: Decimal,
    ) -> Decimal:
        """Calculate the Sortino ratio. Delegates to metrics_calculator module."""
        from .metrics_calculator import calculate_sortino_ratio

        return calculate_sortino_ratio(returns, risk_free_rate, trading_days)

    def _calculate_max_drawdown(self, values: list[Decimal]) -> Decimal:
        """Calculate maximum drawdown. Delegates to metrics_calculator module."""
        from .metrics_calculator import calculate_max_drawdown

        return calculate_max_drawdown(values)

    def _decimal_sqrt(self, n: Decimal) -> Decimal:
        """Calculate square root of a Decimal. Delegates to metrics_calculator module."""
        from .metrics_calculator import decimal_sqrt

        return decimal_sqrt(n)


# Dispatch table for PnLBacktester._create_position_delta — per-intent-type
# position factories, mirroring _engine_helpers._SIMPLE_FLOW_HANDLERS
# (Phase 6C.3). Module-level rather than a class attribute: PnLBacktester is
# a @dataclass, where an annotated class-level dict would become a field with
# a mutable default. Values are method NAMES resolved via ``getattr(self, ...)``
# at dispatch time so subclass overrides are honoured; the characterization
# test ``test_position_delta_handler_table_keys`` asserts every name resolves
# to a PnLBacktester callable. Intent types absent from the table do not
# create positions.
_POSITION_DELTA_HANDLERS: dict[IntentType, str] = {
    IntentType.LP_OPEN: "_lp_open_delta",
    IntentType.SUPPLY: "_supply_delta",
    IntentType.VAULT_DEPOSIT: "_vault_deposit_delta",
    IntentType.BORROW: "_borrow_delta",
    IntentType.PERP_OPEN: "_perp_open_delta",
}


__all__ = [
    "PnLBacktester",
    "BacktestableStrategy",
    "DataQualityTracker",
    "create_market_snapshot_from_state",
]
