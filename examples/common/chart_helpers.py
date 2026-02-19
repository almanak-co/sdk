"""Chart generation helpers for backtest examples.

Provides publication-quality visualizations for backtest results including:
- Equity curves with benchmarks
- Drawdown charts
- Trade signal markers
- Metrics summary tables
"""

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

# Import matplotlib only when needed for chart generation
try:
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.dates import DateFormatter

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False


@dataclass
class EquityPoint:
    """A point on the equity curve."""

    timestamp: datetime
    portfolio_value: float
    benchmark_value: float


@dataclass
class TradeSignal:
    """Record of a trade signal for visualization."""

    timestamp: datetime
    signal_type: str  # "BUY" or "SELL"
    price: Decimal
    indicator_value: float | None = None


@dataclass
class BacktestMetricsSummary:
    """Summary of backtest metrics for display."""

    total_return_pct: float
    sharpe_ratio: float
    sortino_ratio: float
    max_drawdown_pct: float
    win_rate: float
    profit_factor: float
    total_trades: int
    avg_trade_pnl_pct: float


def calculate_equity_curve(
    prices: list[Decimal],
    signals: list[TradeSignal],
    start_time: datetime,
    initial_capital: float = 10000.0,
    trade_amount: float = 500.0,
) -> list[EquityPoint]:
    """Calculate equity curve from strategy signals.

    Args:
        prices: List of price values (hourly)
        signals: List of trade signals with timestamps
        start_time: Start time of the backtest
        initial_capital: Starting capital in USD
        trade_amount: Amount traded per signal

    Returns:
        List of EquityPoint with portfolio and benchmark values
    """
    equity_curve: list[EquityPoint] = []

    # Strategy state
    cash = initial_capital
    asset_held = 0.0
    start_price = float(prices[0]) if prices else 1.0

    # Buy and hold benchmark: invest 50% at start
    benchmark_asset = (initial_capital * 0.5) / start_price
    benchmark_cash = initial_capital * 0.5

    # Build signal lookup by timestamp
    signal_times = {s.timestamp: s for s in signals}

    for i, price in enumerate(prices):
        timestamp = start_time + timedelta(hours=i)
        price_float = float(price)

        # Check for signals at this timestamp
        if timestamp in signal_times:
            signal = signal_times[timestamp]
            if signal.signal_type == "BUY" and cash >= trade_amount:
                # Buy asset
                asset_bought = trade_amount / price_float
                cash -= trade_amount
                asset_held += asset_bought
            elif signal.signal_type == "SELL" and asset_held > 0:
                # Sell asset worth trade_amount or all held
                asset_to_sell = min(asset_held, trade_amount / price_float)
                cash += asset_to_sell * price_float
                asset_held -= asset_to_sell

        # Calculate current values
        portfolio_value = cash + (asset_held * price_float)
        benchmark_value = benchmark_cash + (benchmark_asset * price_float)

        equity_curve.append(
            EquityPoint(
                timestamp=timestamp,
                portfolio_value=portfolio_value,
                benchmark_value=benchmark_value,
            )
        )

    return equity_curve


def calculate_drawdown(equity_curve: list[EquityPoint]) -> list[float]:
    """Calculate drawdown percentage from equity curve.

    Args:
        equity_curve: List of equity points

    Returns:
        List of drawdown percentages (0 to -100 scale)
    """
    drawdowns: list[float] = []
    peak = equity_curve[0].portfolio_value if equity_curve else 0.0

    for point in equity_curve:
        if point.portfolio_value > peak:
            peak = point.portfolio_value
        drawdown = ((point.portfolio_value - peak) / peak) * 100 if peak > 0 else 0.0
        drawdowns.append(drawdown)

    return drawdowns


def calculate_trade_pnls(
    signals: list[TradeSignal],
    trade_amount: float = 500.0,
) -> list[float]:
    """Calculate PnL for each completed trade (buy followed by sell).

    Args:
        signals: List of trade signals
        trade_amount: Amount traded per signal

    Returns:
        List of PnL percentages for each completed trade
    """
    pnls: list[float] = []
    buy_price: float | None = None

    for signal in signals:
        if signal.signal_type == "BUY":
            buy_price = float(signal.price)
        elif signal.signal_type == "SELL" and buy_price is not None:
            sell_price = float(signal.price)
            # PnL = (sell - buy) / buy * 100 (percentage)
            pnl_pct = ((sell_price - buy_price) / buy_price) * 100
            pnls.append(pnl_pct)
            buy_price = None

    return pnls


def calculate_metrics(
    equity_curve: list[EquityPoint],
    trade_pnls: list[float],
    initial_capital: float = 10000.0,
    risk_free_rate: float = 0.05,
) -> BacktestMetricsSummary:
    """Calculate comprehensive backtest metrics.

    Args:
        equity_curve: List of equity points
        trade_pnls: List of trade PnL percentages
        initial_capital: Starting capital
        risk_free_rate: Annual risk-free rate (default 5%)

    Returns:
        BacktestMetricsSummary with all metrics
    """
    if not equity_curve:
        return BacktestMetricsSummary(
            total_return_pct=0.0,
            sharpe_ratio=0.0,
            sortino_ratio=0.0,
            max_drawdown_pct=0.0,
            win_rate=0.0,
            profit_factor=0.0,
            total_trades=0,
            avg_trade_pnl_pct=0.0,
        )

    # Calculate returns
    final_value = equity_curve[-1].portfolio_value
    total_return_pct = ((final_value - initial_capital) / initial_capital) * 100

    # Calculate daily returns for Sharpe/Sortino
    daily_values = equity_curve[::24]  # Sample every 24 hours
    if len(daily_values) < 2:
        daily_values = equity_curve

    daily_returns = []
    for i in range(1, len(daily_values)):
        prev_val = daily_values[i - 1].portfolio_value
        curr_val = daily_values[i].portfolio_value
        if prev_val > 0:
            daily_returns.append((curr_val - prev_val) / prev_val)

    # Sharpe Ratio: (mean_return - rf) / std * sqrt(365)
    if daily_returns and len(daily_returns) > 1:
        mean_return = sum(daily_returns) / len(daily_returns)
        variance = sum((r - mean_return) ** 2 for r in daily_returns) / (len(daily_returns) - 1)
        std_dev = math.sqrt(variance) if variance > 0 else 0.0001
        daily_rf = risk_free_rate / 365
        sharpe_ratio = ((mean_return - daily_rf) / std_dev) * math.sqrt(365) if std_dev > 0 else 0.0
    else:
        sharpe_ratio = 0.0

    # Sortino Ratio: (mean_return - rf) / downside_std * sqrt(365)
    if daily_returns:
        downside_returns = [r for r in daily_returns if r < 0]
        if downside_returns:
            mean_return = sum(daily_returns) / len(daily_returns)
            downside_variance = sum(r**2 for r in downside_returns) / len(downside_returns)
            downside_std = math.sqrt(downside_variance) if downside_variance > 0 else 0.0001
            daily_rf = risk_free_rate / 365
            sortino_ratio = ((mean_return - daily_rf) / downside_std) * math.sqrt(365)
        else:
            sortino_ratio = sharpe_ratio * 2  # No downside, great performance
    else:
        sortino_ratio = 0.0

    # Max Drawdown
    drawdowns = calculate_drawdown(equity_curve)
    max_drawdown_pct = min(drawdowns) if drawdowns else 0.0

    # Trade statistics
    total_trades = len(trade_pnls)
    if trade_pnls:
        winning_trades = [p for p in trade_pnls if p > 0]
        losing_trades = [p for p in trade_pnls if p <= 0]
        win_rate = len(winning_trades) / len(trade_pnls) * 100

        total_wins = sum(winning_trades) if winning_trades else 0.0
        total_losses = abs(sum(losing_trades)) if losing_trades else 0.0001
        profit_factor = total_wins / total_losses if total_losses > 0 else float("inf")

        avg_trade_pnl_pct = sum(trade_pnls) / len(trade_pnls)
    else:
        win_rate = 0.0
        profit_factor = 0.0
        avg_trade_pnl_pct = 0.0

    return BacktestMetricsSummary(
        total_return_pct=total_return_pct,
        sharpe_ratio=sharpe_ratio,
        sortino_ratio=sortino_ratio,
        max_drawdown_pct=max_drawdown_pct,
        win_rate=win_rate,
        profit_factor=profit_factor if profit_factor != float("inf") else 999.9,
        total_trades=total_trades,
        avg_trade_pnl_pct=avg_trade_pnl_pct,
    )


def generate_complete_chart(
    prices: list[Decimal],
    signals: list[TradeSignal],
    equity_curve: list[EquityPoint],
    indicator_values: list[float | None],
    start_time: datetime,
    output_path: Path,
    title: str = "Strategy Backtest",
    indicator_name: str = "Indicator",
    indicator_thresholds: tuple[float, float] | None = None,
) -> bool:
    """Generate complete 3-panel backtest visualization.

    Panel 1: Equity curve with benchmark and drawdown
    Panel 2: Trade PnL histogram
    Panel 3: Price with indicator and signals

    Args:
        prices: List of price values
        signals: List of trade signals
        equity_curve: List of equity points
        indicator_values: List of indicator values (same length as prices)
        start_time: Start time of the backtest
        output_path: Path to save the PNG file
        title: Chart title
        indicator_name: Name of the indicator for legend
        indicator_thresholds: Optional (lower, upper) thresholds to draw

    Returns:
        True if chart generated successfully, False otherwise
    """
    if not MATPLOTLIB_AVAILABLE:
        print("matplotlib not installed. Run: uv add matplotlib")
        return False

    num_hours = len(prices)
    timestamps = [start_time + timedelta(hours=i) for i in range(num_hours)]
    prices_float = [float(p) for p in prices]

    # Calculate metrics for display
    trade_pnls = calculate_trade_pnls(signals)
    drawdowns = calculate_drawdown(equity_curve)

    # Create figure with three subplots
    fig, (ax1, ax2, ax3) = plt.subplots(
        3, 1, figsize=(14, 12), height_ratios=[2, 1, 1.5]
    )
    fig.suptitle(title, fontsize=14, fontweight="bold")

    # =========================================================================
    # Panel 1: Equity curve with benchmark and drawdown
    # =========================================================================
    eq_timestamps = [p.timestamp for p in equity_curve]
    portfolio_values = [p.portfolio_value for p in equity_curve]
    benchmark_values = [p.benchmark_value for p in equity_curve]

    # Plot equity curves
    ax1.plot(
        eq_timestamps,
        portfolio_values,
        linewidth=2,
        color="#2196F3",
        label="Strategy",
    )
    ax1.plot(
        eq_timestamps,
        benchmark_values,
        linewidth=1.5,
        color="#FF9800",
        linestyle="--",
        label="Buy & Hold (50%)",
    )

    # Fill area between strategy and benchmark
    ax1.fill_between(
        eq_timestamps,
        portfolio_values,
        benchmark_values,
        where=np.array(portfolio_values) >= np.array(benchmark_values),
        interpolate=True,
        alpha=0.2,
        color="green",
    )
    ax1.fill_between(
        eq_timestamps,
        portfolio_values,
        benchmark_values,
        where=np.array(portfolio_values) < np.array(benchmark_values),
        interpolate=True,
        alpha=0.2,
        color="red",
    )

    # Create secondary axis for drawdown
    ax1_dd = ax1.twinx()
    ax1_dd.fill_between(eq_timestamps, drawdowns, 0, alpha=0.3, color="red")
    ax1_dd.set_ylabel("Drawdown (%)", fontsize=10, color="red")
    ax1_dd.tick_params(axis="y", labelcolor="red")
    ax1_dd.set_ylim(-50, 10)

    # Add trade markers on equity curve
    for signal in signals:
        delta = (signal.timestamp - start_time).total_seconds() / 3600
        idx = int(delta)
        if 0 <= idx < len(portfolio_values):
            marker = "^" if signal.signal_type == "BUY" else "v"
            color = "green" if signal.signal_type == "BUY" else "red"
            ax1.scatter(
                [signal.timestamp],
                [portfolio_values[idx]],
                marker=marker,
                s=80,
                c=color,
                zorder=5,
                edgecolors="black",
                linewidths=1,
            )

    # Final return annotation
    initial_capital = portfolio_values[0] if portfolio_values else 10000
    final_value = portfolio_values[-1] if portfolio_values else initial_capital
    strategy_return = ((final_value - initial_capital) / initial_capital) * 100
    benchmark_return = ((benchmark_values[-1] - initial_capital) / initial_capital) * 100 if benchmark_values else 0

    ax1.annotate(
        f"Strategy: {strategy_return:+.1f}%\nBenchmark: {benchmark_return:+.1f}%",
        xy=(0.98, 0.05),
        xycoords="axes fraction",
        fontsize=10,
        ha="right",
        va="bottom",
        bbox={"boxstyle": "round", "facecolor": "wheat", "alpha": 0.8},
    )

    ax1.set_ylabel("Portfolio Value ($)", fontsize=11)
    ax1.legend(loc="upper left", fontsize=9)
    ax1.grid(True, alpha=0.3)
    ax1.set_title("Equity Curve vs Benchmark", fontsize=12)

    # =========================================================================
    # Panel 2: Trade PnL histogram
    # =========================================================================
    if trade_pnls:
        colors = ["green" if pnl >= 0 else "red" for pnl in trade_pnls]
        ax2.bar(range(len(trade_pnls)), trade_pnls, color=colors, edgecolor="black", linewidth=0.5)
        ax2.axhline(y=0, color="black", linewidth=1)

        avg_pnl = sum(trade_pnls) / len(trade_pnls)
        winning = [p for p in trade_pnls if p >= 0]
        win_rate = len(winning) / len(trade_pnls) * 100 if trade_pnls else 0

        stats_text = f"Trades: {len(trade_pnls)} | Win Rate: {win_rate:.0f}% | Avg: {avg_pnl:+.2f}%"
        ax2.set_title(f"Trade PnL Distribution - {stats_text}", fontsize=12)
        ax2.set_xlabel("Trade #", fontsize=10)
        ax2.set_ylabel("PnL (%)", fontsize=10)
        ax2.grid(True, alpha=0.3, axis="y")
    else:
        ax2.text(0.5, 0.5, "No completed trades", ha="center", va="center", fontsize=12, transform=ax2.transAxes)
        ax2.set_title("Trade PnL Distribution", fontsize=12)

    # =========================================================================
    # Panel 3: Price with indicator and signals
    # =========================================================================
    # Plot price on primary axis
    ax3.plot(timestamps, prices_float, linewidth=1.5, color="#2196F3", label="Price")

    # Add buy/sell markers on price
    buy_times = [s.timestamp for s in signals if s.signal_type == "BUY"]
    buy_prices = [float(s.price) for s in signals if s.signal_type == "BUY"]
    sell_times = [s.timestamp for s in signals if s.signal_type == "SELL"]
    sell_prices = [float(s.price) for s in signals if s.signal_type == "SELL"]

    if buy_times:
        ax3.scatter(buy_times, buy_prices, marker="^", s=120, c="green", label="Buy", zorder=5, edgecolors="darkgreen", linewidths=1.5)
    if sell_times:
        ax3.scatter(sell_times, sell_prices, marker="v", s=120, c="red", label="Sell", zorder=5, edgecolors="darkred", linewidths=1.5)

    ax3.set_ylabel("Price (USD)", fontsize=11)
    ax3.legend(loc="upper left", fontsize=9)
    ax3.grid(True, alpha=0.3)

    # Create secondary axis for indicator
    if indicator_values and any(v is not None for v in indicator_values):
        ax3_ind = ax3.twinx()
        # Ensure we only iterate over indices that exist in both lists
        min_len = min(len(indicator_values), len(timestamps))
        valid_timestamps = [timestamps[i] for i, v in enumerate(indicator_values[:min_len]) if v is not None]
        valid_values = [v for i, v in enumerate(indicator_values[:min_len]) if v is not None]

        ax3_ind.plot(valid_timestamps, valid_values, linewidth=1.5, color="#9C27B0", label=indicator_name)
        ax3_ind.set_ylabel(indicator_name, fontsize=10, color="#9C27B0")
        ax3_ind.tick_params(axis="y", labelcolor="#9C27B0")

        # Add threshold lines if provided
        if indicator_thresholds:
            lower, upper = indicator_thresholds
            ax3_ind.axhline(y=upper, color="red", linestyle="--", linewidth=1, alpha=0.7)
            ax3_ind.axhline(y=lower, color="green", linestyle="--", linewidth=1, alpha=0.7)
            ax3_ind.fill_between(valid_timestamps, upper, max(valid_values) * 1.1, alpha=0.1, color="red")
            ax3_ind.fill_between(valid_timestamps, 0, lower, alpha=0.1, color="green")

    ax3.set_xlabel("Date", fontsize=11)
    ax3.set_title(f"Price with {indicator_name} Signals", fontsize=12)

    # Format x-axis
    ax3.xaxis.set_major_formatter(DateFormatter("%m/%d"))
    plt.xticks(rotation=45, ha="right")

    plt.tight_layout()

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"Chart saved to: {output_path}")
    return True


def generate_metrics_table(
    metrics: BacktestMetricsSummary,
    output_path: Path | None = None,
) -> str:
    """Generate a formatted metrics table.

    Args:
        metrics: BacktestMetricsSummary with all metrics
        output_path: Optional path to save as PNG

    Returns:
        Formatted string table of metrics
    """
    lines = [
        "=" * 50,
        "             BACKTEST METRICS SUMMARY",
        "=" * 50,
        "",
        f"  Total Return:     {metrics.total_return_pct:+.2f}%",
        f"  Sharpe Ratio:     {metrics.sharpe_ratio:.2f}",
        f"  Sortino Ratio:    {metrics.sortino_ratio:.2f}",
        f"  Max Drawdown:     {metrics.max_drawdown_pct:.2f}%",
        "",
        "-" * 50,
        "              TRADE STATISTICS",
        "-" * 50,
        "",
        f"  Total Trades:     {metrics.total_trades}",
        f"  Win Rate:         {metrics.win_rate:.1f}%",
        f"  Profit Factor:    {metrics.profit_factor:.2f}",
        f"  Avg Trade PnL:    {metrics.avg_trade_pnl_pct:+.2f}%",
        "",
        "=" * 50,
    ]

    table = "\n".join(lines)

    if output_path and MATPLOTLIB_AVAILABLE:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.axis("off")
        ax.text(
            0.1, 0.5, table,
            family="monospace",
            fontsize=11,
            verticalalignment="center",
            transform=ax.transAxes,
            bbox={"boxstyle": "round", "facecolor": "white", "edgecolor": "gray"},
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
        plt.close(fig)

    return table


def print_metrics_with_verification(
    metrics: BacktestMetricsSummary,
    equity_curve: list[EquityPoint],
    trade_pnls: list[float],
    risk_free_rate: float = 0.05,
) -> None:
    """Print metrics with verification formulas.

    Args:
        metrics: Calculated metrics
        equity_curve: Equity curve data
        trade_pnls: Trade PnL values
        risk_free_rate: Annual risk-free rate
    """
    print("\n" + "=" * 60)
    print("          BACKTEST METRICS (WITH VERIFICATION)")
    print("=" * 60)

    print(f"\n  Total Return: {metrics.total_return_pct:+.2f}%")
    if equity_curve:
        initial = equity_curve[0].portfolio_value
        final = equity_curve[-1].portfolio_value
        print(f"    Formula: ({final:.2f} - {initial:.2f}) / {initial:.2f} * 100")

    print(f"\n  Sharpe Ratio: {metrics.sharpe_ratio:.2f}")
    print(f"    Formula: (mean_daily_return - {risk_free_rate/365:.6f}) / std_dev * sqrt(365)")

    print(f"\n  Sortino Ratio: {metrics.sortino_ratio:.2f}")
    print("    Formula: (mean_daily_return - rf) / downside_std * sqrt(365)")

    print(f"\n  Max Drawdown: {metrics.max_drawdown_pct:.2f}%")
    print("    Formula: min((portfolio_value - peak) / peak * 100)")

    print(f"\n  Win Rate: {metrics.win_rate:.1f}%")
    if trade_pnls:
        wins = len([p for p in trade_pnls if p > 0])
        print(f"    Formula: {wins} wins / {len(trade_pnls)} trades * 100")

    print(f"\n  Profit Factor: {metrics.profit_factor:.2f}")
    if trade_pnls:
        total_wins = sum(p for p in trade_pnls if p > 0)
        total_losses = abs(sum(p for p in trade_pnls if p <= 0))
        print(f"    Formula: {total_wins:.2f} / {total_losses:.2f}")

    print("\n" + "=" * 60)
