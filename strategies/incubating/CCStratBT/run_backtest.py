"""Backtest runner for Triple Signal Momentum Strategy.

Runs the strategy against 3 parameter sets across 5 time periods,
then picks the best set based on risk-adjusted returns.

Usage:
    uv run python strategies/incubating/CCStratBT/run_backtest.py
"""

import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

# Add project root to path for strategy auto-discovery
project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))

from almanak.framework.backtesting import CoinGeckoDataProvider
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester
from almanak.framework.cli.run import DictConfigWrapper
from strategies.incubating.CCStratBT.strategy import TripleSignalStrategy

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Show strategy trade signals
logging.getLogger("strategies.incubating.CCStratBT.strategy").setLevel(logging.WARNING)


# =============================================================================
# Parameter Sets
# =============================================================================

PARAM_SETS = {
    "Conservative": {
        "trade_size_usd": "500",
        "rsi_period": 14,
        "rsi_oversold": 30,
        "rsi_overbought": 70,
        "macd_fast": 12,
        "macd_slow": 26,
        "macd_signal": 9,
        "bb_period": 20,
        "bb_std_dev": 2.0,
        "bb_buy_threshold": 0.15,
        "bb_sell_threshold": 0.85,
        "min_signals_to_trade": 3,
        "cooldown_ticks": 6,
        "max_slippage_pct": 1.0,
        "base_token": "WETH",
        "quote_token": "USDC",
        "chain": "arbitrum",
    },
    "Balanced": {
        "trade_size_usd": "500",
        "rsi_period": 10,
        "rsi_oversold": 35,
        "rsi_overbought": 65,
        "macd_fast": 8,
        "macd_slow": 21,
        "macd_signal": 5,
        "bb_period": 15,
        "bb_std_dev": 1.8,
        "bb_buy_threshold": 0.2,
        "bb_sell_threshold": 0.8,
        "min_signals_to_trade": 2,
        "cooldown_ticks": 4,
        "max_slippage_pct": 1.0,
        "base_token": "WETH",
        "quote_token": "USDC",
        "chain": "arbitrum",
    },
    "Aggressive": {
        "trade_size_usd": "500",
        "rsi_period": 7,
        "rsi_oversold": 40,
        "rsi_overbought": 60,
        "macd_fast": 6,
        "macd_slow": 13,
        "macd_signal": 4,
        "bb_period": 10,
        "bb_std_dev": 1.5,
        "bb_buy_threshold": 0.25,
        "bb_sell_threshold": 0.75,
        "min_signals_to_trade": 2,
        "cooldown_ticks": 2,
        "max_slippage_pct": 1.0,
        "base_token": "WETH",
        "quote_token": "USDC",
        "chain": "arbitrum",
    },
}


# =============================================================================
# Time Periods
# =============================================================================

TIME_PERIODS = {
    "2024-Q1 (Jan-Mar)": (datetime(2024, 1, 1), datetime(2024, 3, 31)),
    "2024-Q2 (Apr-Jun)": (datetime(2024, 4, 1), datetime(2024, 6, 30)),
    "2024-Q3 (Jul-Sep)": (datetime(2024, 7, 1), datetime(2024, 9, 30)),
    "2024-Q4 (Oct-Dec)": (datetime(2024, 10, 1), datetime(2024, 12, 31)),
    "2025-Q1 (Jan-Feb)": (datetime(2025, 1, 1), datetime(2025, 2, 10)),
}


# =============================================================================
# Results Collector
# =============================================================================

@dataclass
class BacktestRun:
    param_set: str
    period: str
    sharpe: float
    total_return_pct: float
    max_drawdown_pct: float
    total_trades: int
    win_rate: float
    net_pnl_usd: float
    data_coverage: float


async def run_single_backtest(
    backtester: PnLBacktester,
    param_name: str,
    params: dict,
    period_name: str,
    start: datetime,
    end: datetime,
) -> BacktestRun | None:
    """Run a single backtest and return the results."""
    config = PnLBacktestConfig(
        start_time=start,
        end_time=end,
        interval_seconds=3600,  # 1-hour ticks
        initial_capital_usd=Decimal("10000"),
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        fee_model="realistic",
        slippage_model="realistic",
        include_gas_costs=True,
        gas_price_gwei=Decimal("0.1"),  # Arbitrum L2 gas
        allow_degraded_data=True,
        preflight_validation=False,
        fail_on_preflight_error=False,
    )

    try:
        strategy = TripleSignalStrategy(
            config=DictConfigWrapper(params),
            chain="arbitrum",
            wallet_address="0x" + "0" * 40,
        )

        result = await backtester.backtest(strategy, config)
        metrics = result.metrics

        return BacktestRun(
            param_set=param_name,
            period=period_name,
            sharpe=float(metrics.sharpe_ratio) if metrics.sharpe_ratio else 0.0,
            total_return_pct=float(metrics.total_return_pct) if metrics.total_return_pct else 0.0,
            max_drawdown_pct=float(metrics.max_drawdown_pct) if metrics.max_drawdown_pct else 0.0,
            total_trades=metrics.total_trades if metrics.total_trades else 0,
            win_rate=float(metrics.win_rate) if metrics.win_rate else 0.0,
            net_pnl_usd=float(metrics.net_pnl_usd) if metrics.net_pnl_usd else 0.0,
            data_coverage=float(result.data_quality.coverage_ratio) if result.data_quality else 0.0,
        )
    except Exception as e:
        logger.error(f"  FAILED: {param_name} x {period_name}: {e}")
        return None


async def main():
    """Run all backtests and produce comparison report."""
    print("=" * 80)
    print("  TRIPLE SIGNAL MOMENTUM STRATEGY - PARAMETER OPTIMIZATION")
    print("  3 Parameter Sets x 5 Time Periods = 15 Backtests")
    print("=" * 80)
    print()

    data_provider = CoinGeckoDataProvider()
    fee_models = {"default": DefaultFeeModel(fee_pct=Decimal("0.003"))}  # 30bps
    slippage_models = {"default": DefaultSlippageModel(slippage_pct=Decimal("0.001"))}

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models=fee_models,
        slippage_models=slippage_models,
    )

    results: list[BacktestRun] = []
    total_runs = len(PARAM_SETS) * len(TIME_PERIODS)
    run_count = 0
    start_time = time.time()

    for param_name, params in PARAM_SETS.items():
        print(f"\n--- Parameter Set: {param_name} ---")
        for period_name, (start, end) in TIME_PERIODS.items():
            run_count += 1
            print(f"  [{run_count}/{total_runs}] {period_name}...", end=" ", flush=True)

            run = await run_single_backtest(
                backtester, param_name, params, period_name, start, end,
            )
            if run:
                results.append(run)
                print(
                    f"Return={run.total_return_pct:+.2f}%, "
                    f"Sharpe={run.sharpe:.2f}, "
                    f"Trades={run.total_trades}"
                )
            else:
                print("FAILED")

            # Small delay to respect CoinGecko rate limits
            await asyncio.sleep(1)

    elapsed = time.time() - start_time

    # =============================================================================
    # ANALYSIS & REPORT
    # =============================================================================
    print(f"\n{'=' * 80}")
    print(f"  RESULTS SUMMARY  (completed in {elapsed:.0f}s)")
    print(f"{'=' * 80}")

    # Aggregate by parameter set
    for param_name in PARAM_SETS:
        param_results = [r for r in results if r.param_set == param_name]
        if not param_results:
            continue

        n = len(param_results)
        avg_sharpe = sum(r.sharpe for r in param_results) / n
        avg_return = sum(r.total_return_pct for r in param_results) / n
        avg_dd = sum(r.max_drawdown_pct for r in param_results) / n
        avg_trades = sum(r.total_trades for r in param_results) / n
        avg_wr = sum(r.win_rate for r in param_results) / n
        total_pnl = sum(r.net_pnl_usd for r in param_results)

        print(f"\n  [{param_name}] ({n} periods)")
        print(f"    Avg Sharpe Ratio:    {avg_sharpe:+.3f}")
        print(f"    Avg Return:          {avg_return:+.2f}%")
        print(f"    Avg Max Drawdown:    {avg_dd:.2f}%")
        print(f"    Avg Trades/Period:   {avg_trades:.1f}")
        print(f"    Avg Win Rate:        {avg_wr:.1f}%")
        print(f"    Cumulative PnL:      ${total_pnl:+,.2f}")

    # Pick winner based on average Sharpe ratio
    param_scores = {}
    for param_name in PARAM_SETS:
        param_results = [r for r in results if r.param_set == param_name]
        if param_results:
            param_scores[param_name] = sum(r.sharpe for r in param_results) / len(param_results)

    if param_scores:
        winner = max(param_scores, key=param_scores.get)
        print(f"\n{'=' * 80}")
        print(f"  WINNER: {winner} (Avg Sharpe: {param_scores[winner]:+.3f})")
        print(f"{'=' * 80}")
    else:
        winner = "Balanced"
        print("\n  No valid results. Defaulting to Balanced.")

    # Detailed per-period table
    print(f"\n{'=' * 80}")
    print("  DETAILED RESULTS TABLE")
    print(f"{'=' * 80}")
    header = f"{'Param Set':<15} {'Period':<22} {'Return%':>9} {'Sharpe':>8} {'MaxDD%':>8} {'Trades':>7} {'WinRate':>8} {'PnL$':>10}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.param_set:<15} {r.period:<22} {r.total_return_pct:>+8.2f}% "
            f"{r.sharpe:>+7.2f} {r.max_drawdown_pct:>7.2f}% "
            f"{r.total_trades:>7} {r.win_rate:>7.1f}% {r.net_pnl_usd:>+9.2f}"
        )

    # Save results to JSON
    output_path = Path(__file__).parent / "backtest_results.json"
    output_data = {
        "winner": winner,
        "winner_params": PARAM_SETS[winner],
        "param_scores": param_scores,
        "runs": [
            {
                "param_set": r.param_set,
                "period": r.period,
                "sharpe": r.sharpe,
                "total_return_pct": r.total_return_pct,
                "max_drawdown_pct": r.max_drawdown_pct,
                "total_trades": r.total_trades,
                "win_rate": r.win_rate,
                "net_pnl_usd": r.net_pnl_usd,
            }
            for r in results
        ],
    }
    output_path.write_text(json.dumps(output_data, indent=2))
    print(f"\nResults saved to: {output_path}")

    return winner


if __name__ == "__main__":
    asyncio.run(main())
