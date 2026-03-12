"""Backtest runner — wraps PnLBacktester for HTTP use.

Translates StrategySpec (HTTP input) into the internal BacktestableStrategy +
PnLBacktestConfig, runs the backtest, and reports progress to JobManager.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.models import BacktestMetrics, BacktestResult
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
    StrategySpec,
    TimeframeSpec,
)
from almanak.services.backtest.services.job_manager import JobManager

logger = logging.getLogger(__name__)

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

    Supported actions:
        swap              - SwapIntent  (token exchange)
        provide_liquidity - LPOpenIntent (concentrated LP position)
        lend / supply     - SupplyIntent (lending deposit)
        borrow            - BorrowIntent (collateralized borrow)
    """

    def __init__(self, spec: StrategySpec) -> None:
        self._spec = spec
        self._strategy_id = f"spec_{spec.protocol}_{spec.action}_{spec.chain}"
        self._tick_count = 0

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
    """Create a PnLBacktester wired with CoinGecko data and default models.

    The CoinGecko provider works standalone (no gateway required). It reads
    COINGECKO_API_KEY from the environment for the pro tier; falls back to the
    free tier when the key is absent.
    """
    data_provider = CoinGeckoDataProvider()
    fee_models: dict[str, Any] = {"default": DefaultFeeModel()}
    slippage_models: dict[str, Any] = {"default": DefaultSlippageModel()}

    return PnLBacktester(
        data_provider=data_provider,
        fee_models=fee_models,
        slippage_models=slippage_models,
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


def build_backtest_config(
    spec: StrategySpec,
    timeframe: TimeframeSpec,
    *,
    quick: bool = False,
) -> PnLBacktestConfig:
    """Build PnLBacktestConfig from HTTP request parameters."""
    start = datetime(timeframe.start.year, timeframe.start.month, timeframe.start.day, tzinfo=UTC)
    end = datetime(timeframe.end.year, timeframe.end.month, timeframe.end.day, tzinfo=UTC)

    params = spec.parameters
    initial_capital = Decimal(str(params.get("amount_usd", "10000")))
    tokens = _extract_tokens(spec)

    # Quick mode: shorter interval, simplified
    interval = 3600 if not quick else 86400  # 1h vs 1d

    return PnLBacktestConfig(
        start_time=start,
        end_time=end,
        interval_seconds=interval,
        initial_capital_usd=initial_capital,
        chain=spec.chain,
        tokens=tokens,
        fee_model=spec.protocol.replace("-", "_"),
        slippage_model="realistic",
        include_gas_costs=not quick,
        # Service runs standalone without gateway — use forgiving defaults
        preflight_validation=False,
        allow_hardcoded_fallback=True,
        allow_degraded_data=True,
    )


def serialize_result(result: BacktestResult) -> dict[str, Any]:
    """Serialize BacktestResult to a JSON-compatible dict."""
    metrics = result.metrics
    return {
        "metrics": _serialize_metrics(metrics),
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


def _serialize_metrics(m: BacktestMetrics) -> dict[str, Any]:
    """Serialize BacktestMetrics to dict with string-encoded Decimals."""
    return {
        "net_pnl_usd": str(m.net_pnl_usd),
        "total_return_pct": str(m.total_return_pct),
        "sharpe_ratio": str(m.sharpe_ratio),
        "max_drawdown_pct": str(m.max_drawdown_pct),
        "win_rate": str(m.win_rate),
        "total_trades": m.total_trades,
        "total_fees_usd": str(m.total_fees_usd),
        "sortino_ratio": str(m.sortino_ratio),
        "calmar_ratio": str(m.calmar_ratio),
        "profit_factor": str(m.profit_factor),
    }


async def run_backtest_job(
    job_id: str,
    spec: StrategySpec,
    timeframe: TimeframeSpec,
    job_manager: JobManager,
    *,
    quick: bool = False,
) -> None:
    """Run a backtest job in the background, updating progress in job_manager.

    This is called from a BackgroundTask. It:
    1. Marks the job as RUNNING
    2. Builds config + strategy from the spec
    3. Runs PnLBacktester.backtest()
    4. Stores results (or error) in the job manager
    """
    job_manager.mark_running(job_id)
    job_manager.update_progress(job_id, 5.0, "Initializing backtest...")

    try:
        config = build_backtest_config(spec, timeframe, quick=quick)
        strategy = SpecBacktestStrategy(spec)

        job_manager.update_progress(job_id, 10.0, "Running simulation...")

        backtester = create_backtester()
        try:
            result = await backtester.backtest(strategy, config)  # type: ignore[arg-type]
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
