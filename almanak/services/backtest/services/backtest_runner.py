"""Backtest runner — wraps PnLBacktester for HTTP use.

Translates StrategySpec (HTTP input) into the internal BacktestableStrategy +
PnLBacktestConfig, runs the backtest, and reports progress to JobManager.

The runner wires the **full** backtesting engine:
- Protocol-specific fee models (Uniswap V3, Aave V3, GMX, etc.)
- Strategy-type adapters (LP fee accrual, lending interest, perp funding)
- BacktestDataConfig for historical volume / APY / funding rate data
- Strategy metadata so the engine auto-detects LP / lending / perp / swap
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.models import BacktestResult
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.providers.coingecko import CoinGeckoDataProvider
from almanak.framework.data.market_snapshot import MarketSnapshot
from almanak.framework.intents.vocabulary import (
    BorrowIntent,
    HoldIntent,
    LPOpenIntent,
    SupplyIntent,
    SwapIntent,
)
from almanak.services.backtest.models import (
    BacktestRequest,
    QuickBacktestRequest,
    StrategySpec,
    TimeframeSpec,
)
from almanak.services.backtest.services.job_manager import JobManager

logger = logging.getLogger(__name__)

# Sentinel wallet address for backtesting (not used on-chain)
_BACKTEST_WALLET = "0x0000000000000000000000000000000000000000"

# ---------------------------------------------------------------------------
# Supported actions and their required/optional parameters
# ---------------------------------------------------------------------------
SUPPORTED_ACTIONS = {
    "swap": {"required": [], "optional": ["from_token", "to_token", "amount_usd", "max_slippage"]},
    "provide_liquidity": {
        "required": ["pool"],
        "optional": ["amount0", "amount1", "range_lower", "range_upper"],
    },
    "lend": {"required": [], "optional": ["token", "amount"]},
    "supply": {"required": [], "optional": ["token", "amount"]},
    "borrow": {
        "required": [],
        "optional": ["collateral_token", "collateral_amount", "borrow_token", "borrow_amount"],
    },
}


class SpecBacktestStrategy:
    """Adapter: converts a StrategySpec into a BacktestableStrategy.

    Translates the HTTP-provided StrategySpec into the appropriate Intent
    type based on the ``action`` field. The PnLBacktester calls
    ``decide(market)`` on each tick and simulates execution + fees.

    Exposes ``get_metadata()`` so the engine's adapter registry auto-detects
    the strategy type (LP / lending / perp / swap) and loads the right
    adapter for position-level simulation (fee accrual, interest, funding).

    Supported actions:
        swap              - SwapIntent  (token exchange)
        provide_liquidity - LPOpenIntent (concentrated LP position)
        lend / supply     - SupplyIntent (lending deposit)
        borrow            - BorrowIntent (collateralized borrow)
    """

    # Maps action → (tags, intent_types) for adapter detection
    _ACTION_METADATA: dict[str, tuple[list[str], list[str]]] = {
        "swap": (["swap", "trading"], ["SWAP"]),
        "provide_liquidity": (["lp", "liquidity", "concentrated-liquidity"], ["LP_OPEN", "LP_CLOSE"]),
        "lend": (["lending", "supply"], ["SUPPLY", "WITHDRAW"]),
        "supply": (["lending", "supply"], ["SUPPLY", "WITHDRAW"]),
        "borrow": (["lending", "borrow"], ["BORROW", "REPAY"]),
    }

    def __init__(self, spec: StrategySpec) -> None:
        self._spec = spec
        self._strategy_id = f"spec_{spec.protocol}_{spec.action}_{spec.chain}"
        self._tick_count = 0
        self._metadata = self._build_metadata(spec)

    def _build_metadata(self, spec: StrategySpec) -> Any:
        """Build StrategyMetadata from the spec for adapter auto-detection."""
        from almanak.framework.strategies.intent_strategy import StrategyMetadata

        action = spec.action.lower()
        if action not in self._ACTION_METADATA:
            raise ValueError(f"Unknown action '{action}'. Supported actions: {', '.join(self._ACTION_METADATA)}")
        tags, intent_types = self._ACTION_METADATA[action]

        return StrategyMetadata(
            name=self._strategy_id,
            description=f"{spec.action} on {spec.protocol} ({spec.chain})",
            tags=tags,
            supported_chains=[spec.chain],
            supported_protocols=[spec.protocol.replace("-", "_")],
            intent_types=intent_types,
        )

    def get_metadata(self) -> Any:
        """Return strategy metadata for adapter auto-detection."""
        return self._metadata

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    # ------------------------------------------------------------------
    # decide() — called by PnLBacktester on every tick
    # ------------------------------------------------------------------

    def decide(self, market: MarketSnapshot) -> Any:
        """Generate an intent from the StrategySpec parameters.

        The first tick opens the position (swap / LP / supply / borrow).
        Subsequent ticks hold — the PnL backtester marks the position
        to market each tick automatically.
        """
        self._tick_count += 1
        params = self._spec.parameters
        action = self._spec.action.lower()

        # Only execute on the first tick — afterwards we hold and let
        # the backtester simulate PnL via fee/slippage models.
        if self._tick_count > 1:
            return HoldIntent(reason=f"Holding {action} position")

        if action == "swap":
            return self._build_swap_intent(params)
        if action == "provide_liquidity":
            return self._build_lp_intent(params)
        if action in ("lend", "supply"):
            return self._build_supply_intent(params)
        if action == "borrow":
            return self._build_borrow_intent(params)

        # Unknown action — hold and let fee model simulate
        logger.warning("Unknown action '%s', falling back to HoldIntent", action)
        return HoldIntent(reason=f"Unsupported action: {action}")

    # ------------------------------------------------------------------
    # Intent builders
    # ------------------------------------------------------------------

    def _build_swap_intent(self, params: dict[str, Any]) -> SwapIntent:
        from_token = params.get("from_token", "USDC")
        to_token = params.get("to_token", "WETH")
        amount_usd = Decimal(str(params.get("amount_usd", "1000")))
        max_slippage = Decimal(str(params.get("max_slippage", "0.005")))
        return SwapIntent(
            from_token=from_token,
            to_token=to_token,
            amount_usd=amount_usd,
            max_slippage=max_slippage,
            protocol=self._spec.protocol,
            chain=self._spec.chain,
        )

    def _build_lp_intent(self, params: dict[str, Any]) -> LPOpenIntent:
        pool = params.get("pool", "")
        if not pool:
            # Construct a reasonable pool identifier from tokens
            token0 = params.get("token0", params.get("from_token", "WETH"))
            token1 = params.get("token1", params.get("to_token", "USDC"))
            pool = f"{token0}/{token1}"

        amount0 = Decimal(str(params.get("amount0", "1")))
        amount1 = Decimal(str(params.get("amount1", "1000")))
        range_lower = Decimal(str(params.get("range_lower", "1500")))
        range_upper = Decimal(str(params.get("range_upper", "2500")))

        return LPOpenIntent(
            pool=pool,
            amount0=amount0,
            amount1=amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol=self._spec.protocol,
            chain=self._spec.chain,
        )

    def _build_supply_intent(self, params: dict[str, Any]) -> SupplyIntent:
        token = params.get("token", params.get("from_token", "USDC"))
        amount = Decimal(str(params.get("amount", params.get("amount_usd", "10000"))))
        return SupplyIntent(
            protocol=self._spec.protocol,
            token=token,
            amount=amount,
            chain=self._spec.chain,
        )

    def _build_borrow_intent(self, params: dict[str, Any]) -> BorrowIntent:
        collateral_token = params.get("collateral_token", "WETH")
        collateral_amount = Decimal(str(params.get("collateral_amount", "1")))
        borrow_token = params.get("borrow_token", "USDC")
        borrow_amount = Decimal(str(params.get("borrow_amount", "1000")))
        return BorrowIntent(
            protocol=self._spec.protocol,
            collateral_token=collateral_token,
            collateral_amount=collateral_amount,
            borrow_token=borrow_token,
            borrow_amount=borrow_amount,
            chain=self._spec.chain,
        )


def create_backtester() -> PnLBacktester:
    """Create a PnLBacktester wired with the full engine capabilities.

    Wires:
    - Default fee/slippage models for the generic execution path
    - ``strategy_type="auto"`` so the engine auto-detects LP/lending/perp/swap
      from strategy metadata and loads the right adapter (LPBacktestAdapter,
      LendingBacktestAdapter, PerpBacktestAdapter)
    - ``BacktestDataConfig`` enabling historical volume, APY, and funding rate
      data for adapter-level simulation (LP fee accrual, lending interest,
      perp funding). Falls back to configurable defaults when historical
      data is unavailable (non-strict mode).

    The adapters handle protocol-specific fee calculation internally. The
    ``fee_models`` dict provides the fallback for the generic execution
    path (intents that don't match a registered adapter).

    The CoinGecko provider works standalone (no gateway required). It reads
    COINGECKO_API_KEY from the environment for the pro tier; falls back to the
    free tier when the key is absent.
    """
    data_provider = CoinGeckoDataProvider()

    # Default models for the generic execution path. Protocol-specific fee
    # calculation happens inside the adapters (LP, lending, perp).
    fee_models: dict[str, Any] = {"default": DefaultFeeModel()}
    slippage_models: dict[str, Any] = {"default": DefaultSlippageModel()}

    # Enable historical data sources for adapters:
    # - LP adapter: historical pool volume for fee accrual
    # - Lending adapter: historical APY for interest accrual
    # - Perp adapter: historical funding rates
    # Non-strict mode uses fallback values when historical data is unavailable.
    data_config = BacktestDataConfig(
        use_historical_volume=True,
        use_historical_funding=True,
        use_historical_apy=True,
        use_historical_liquidity=True,
        strict_historical_mode=False,
    )

    return PnLBacktester(
        data_provider=data_provider,
        fee_models=fee_models,
        slippage_models=slippage_models,
        strategy_type="auto",
        data_config=data_config,
    )


def _extract_tokens(spec: StrategySpec) -> list[str]:
    """Extract the relevant token list from a StrategySpec.

    Different actions reference tokens via different parameter names.
    This helper normalises them into a de-duplicated list.
    """
    params = spec.parameters
    action = spec.action.lower()

    # Explicit tokens list always wins
    if "tokens" in params:
        tokens = params["tokens"]
        return [tokens] if isinstance(tokens, str) else list(tokens)

    token_set: list[str] = []

    if action == "swap":
        token_set = [params.get("from_token", "USDC"), params.get("to_token", "WETH")]
    elif action == "provide_liquidity":
        token_set = [
            params.get("token0", params.get("from_token", "WETH")),
            params.get("token1", params.get("to_token", "USDC")),
        ]
    elif action in ("lend", "supply"):
        token_set = [params.get("token", params.get("from_token", "USDC"))]
    elif action == "borrow":
        token_set = [
            params.get("collateral_token", "WETH"),
            params.get("borrow_token", "USDC"),
        ]
    else:
        token_set = ["WETH", "USDC"]

    # De-duplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for t in token_set:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return unique


def load_named_strategy(strategy_name: str, chain: str, config: dict | None = None) -> Any:
    """Load a registered strategy by name and instantiate it.

    Tries the same flexible initialization as the CLI:
    1. IntentStrategy signature: (config, chain, wallet_address)
    2. Simple config: (config,)
    3. No-arg constructor
    """
    from almanak.framework.strategies import get_strategy

    strategy_class = get_strategy(strategy_name)
    if strategy_class is None:
        raise ValueError(f"Strategy '{strategy_name}' not found in registry")

    strategy_config = config or {}

    # Try IntentStrategy signature first
    try:
        return strategy_class(strategy_config, chain, _BACKTEST_WALLET)
    except TypeError:
        pass

    # Try simple config
    try:
        return strategy_class(strategy_config)
    except TypeError:
        pass

    # No-arg
    return strategy_class()


def list_available_strategies() -> list[str]:
    """Return names of all registered strategies."""
    from almanak.framework.strategies import list_strategies

    return list_strategies()


def build_backtest_config(
    spec: StrategySpec | None,
    timeframe: TimeframeSpec,
    *,
    quick: bool = False,
    chain: str | None = None,
    initial_capital_usd: Decimal | None = None,
    tokens: list[str] | None = None,
) -> PnLBacktestConfig:
    """Build PnLBacktestConfig from HTTP request parameters.

    Works with both StrategySpec (declarative) and named strategies.
    When using a named strategy, chain must be provided explicitly.
    """
    start = datetime(timeframe.start.year, timeframe.start.month, timeframe.start.day, tzinfo=UTC)
    end = datetime(timeframe.end.year, timeframe.end.month, timeframe.end.day, tzinfo=UTC)

    if spec is not None:
        params = spec.parameters
        capital = initial_capital_usd or Decimal(str(params.get("amount_usd", "10000")))
        resolved_tokens = tokens or _extract_tokens(spec)
        resolved_chain = chain or spec.chain
        fee_model = spec.protocol.replace("-", "_")
    else:
        if not chain:
            raise ValueError("chain is required when using strategy_name (no strategy_spec to infer it from)")
        if not tokens:
            raise ValueError("tokens is required when using strategy_name (no strategy_spec to infer it from)")
        capital = initial_capital_usd or Decimal("10000")
        resolved_tokens = tokens
        resolved_chain = chain
        fee_model = "realistic"

    # Quick mode: shorter interval, simplified
    interval = 3600 if not quick else 86400  # 1h vs 1d

    return PnLBacktestConfig(
        start_time=start,
        end_time=end,
        interval_seconds=interval,
        initial_capital_usd=capital,
        chain=resolved_chain,
        tokens=resolved_tokens,
        fee_model=fee_model,
        slippage_model="realistic",
        include_gas_costs=not quick,
        # Service runs standalone without gateway — use forgiving defaults
        preflight_validation=False,
        allow_hardcoded_fallback=True,
        allow_degraded_data=True,
    )


def serialize_result(result: BacktestResult) -> dict[str, Any]:
    """Serialize BacktestResult to a JSON-compatible dict.

    Uses BacktestMetrics.to_dict() to expose the full metric set. All Decimal
    values are stringified for JSON safety. The field names match the SDK's
    internal BacktestMetrics dataclass exactly.
    """
    return {
        "metrics": result.metrics.to_dict(),
        "equity_curve": [
            {"timestamp": str(pt.timestamp), "value_usd": str(pt.value_usd)} for pt in (result.equity_curve or [])
        ],
        "trades": [
            {
                "timestamp": str(t.timestamp),
                "intent_type": str(t.intent_type),
                "amount_usd": str(t.amount_usd),
                "fee_usd": str(t.fee_usd),
                "slippage_usd": str(t.slippage_usd),
                "pnl_usd": str(t.pnl_usd),
            }
            for t in (result.trades or [])
        ],
        "duration_seconds": result.run_duration_seconds or 0.0,
    }


def resolve_strategy(
    request: BacktestRequest | QuickBacktestRequest,
    *,
    quick: bool = False,
) -> tuple[Any, PnLBacktestConfig]:
    """Resolve a strategy and config from a backtest request.

    Supports two modes:
    1. strategy_name — loads a registered strategy class from the SDK registry
    2. strategy_spec — builds a single-action strategy from the declarative spec

    Args:
        request: The backtest request (full or quick).
        quick: Whether this is a quick backtest (1-day intervals, no gas costs).

    Returns (strategy_instance, backtest_config).
    """
    timeframe = getattr(request, "timeframe", None) or build_quick_timeframe()
    # Respect explicit quick param; also check request.mode for BacktestRequest
    is_quick = quick or getattr(request, "mode", "full") == "quick"
    chain = request.chain
    tokens = getattr(request, "tokens", None)
    capital = request.initial_capital_usd

    if request.strategy_name:
        # Named strategy from registry — chain and tokens are required
        if not chain:
            raise ValueError("chain is required when using strategy_name")
        if not tokens:
            raise ValueError('tokens is required when using strategy_name (e.g. ["WETH", "USDC"])')
        strategy = load_named_strategy(request.strategy_name, chain)
        config = build_backtest_config(
            spec=None,
            timeframe=timeframe,
            quick=is_quick,
            chain=chain,
            initial_capital_usd=capital,
            tokens=tokens,
        )
        return strategy, config

    if request.strategy_spec:
        strategy = SpecBacktestStrategy(request.strategy_spec)
        config = build_backtest_config(
            spec=request.strategy_spec,
            timeframe=timeframe,
            quick=is_quick,
            chain=chain,
            initial_capital_usd=capital,
        )
        return strategy, config

    raise ValueError("Either strategy_name or strategy_spec must be provided")


async def run_backtest_job(
    job_id: str,
    request: BacktestRequest,
    job_manager: JobManager,
) -> None:
    """Run a backtest job in the background, updating progress in job_manager.

    This is called from a BackgroundTask. It:
    1. Marks the job as RUNNING
    2. Resolves strategy (named or spec-based) + config
    3. Runs PnLBacktester.backtest()
    4. Stores results (or error) in the job manager
    """
    job_manager.mark_running(job_id)
    job_manager.update_progress(job_id, 5.0, "Initializing backtest...")

    try:
        strategy, config = resolve_strategy(request)

        job_manager.update_progress(job_id, 10.0, "Running simulation...")

        backtester = create_backtester()
        try:
            result = await backtester.backtest(strategy, config)
        finally:
            await backtester.close()

        serialized = serialize_result(result)
        job_manager.complete_job(job_id, serialized)

    except Exception:
        logger.exception("Backtest job %s failed", job_id)
        job_manager.fail_job(job_id, "Backtest failed. Check server logs for details.")


def build_quick_timeframe() -> TimeframeSpec:
    """Build a 7-day timeframe ending today for quick backtests."""
    from datetime import date

    end = date.today()
    start = end - timedelta(days=7)
    return TimeframeSpec(start=start, end=end)
