"""Visualization module for backtest results.

This module provides charting and visualization capabilities for backtest results,
including equity curves, drawdown highlighting, and trade markers.

Example:
    from almanak.framework.backtesting.visualization import plot_equity_curve, save_chart

    # Generate equity curve plot
    result = plot_equity_curve(backtest_result)
    if result.success:
        print(f"Chart saved to: {result.file_path}")

    # Generate equity curve with benchmark comparison
    benchmark = [EquityPoint(...), ...]  # ETH hold, for example
    result = plot_equity_curve(
        backtest_result,
        benchmark_curve=benchmark,
        benchmark_label="ETH Hold",
        show_drawdown=True,
    )

    # Generate chart with trade markers
    result = plot_equity_curve(
        backtest_result,
        show_trades=True,
    )

    # Save chart in different formats (PNG or interactive HTML)
    save_chart(backtest_result, format="html", path="report.html")
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.backtesting.models import BacktestResult, EquityPoint, TradeRecord

logger = logging.getLogger(__name__)


@dataclass
class DistributionStats:
    """Statistics for trade return distribution.

    Provides statistical measures to understand the shape and behavior
    of the trade return distribution, including skewness and kurtosis.

    Attributes:
        mean: Average return per trade
        median: Middle value of returns
        std_dev: Standard deviation of returns
        skewness: Measure of distribution asymmetry.
            Positive = right tail longer (more large gains)
            Negative = left tail longer (more large losses)
            Zero = symmetric distribution
        kurtosis: Measure of distribution "tailedness".
            Excess kurtosis (kurtosis - 3):
            Positive = fat tails (more extreme values)
            Negative = thin tails (fewer extreme values)
            Zero = normal distribution
        min_return: Smallest return in the distribution
        max_return: Largest return in the distribution
        percentile_5: 5th percentile (worst 5% threshold)
        percentile_95: 95th percentile (best 5% threshold)
        count: Number of trades in the distribution
    """

    mean: float
    median: float
    std_dev: float
    skewness: float
    kurtosis: float
    min_return: float
    max_return: float
    percentile_5: float
    percentile_95: float
    count: int


@dataclass
class TradeMarker:
    """Represents a trade marker on the equity curve chart.

    Trade markers visually indicate when trades were executed, with different
    markers for entry (buy) and exit (sell) points.

    Attributes:
        timestamp: When the trade was executed
        value_usd: Portfolio value at the time of trade
        is_entry: True for entry/buy trades, False for exit/sell trades
        trade_type: Type of trade (e.g., "SWAP", "LP_OPEN", "LP_CLOSE")
        pnl_usd: PnL from this trade (positive = profit, negative = loss)
    """

    timestamp: datetime
    value_usd: Decimal
    is_entry: bool
    trade_type: str
    pnl_usd: Decimal | None = None


@dataclass
class ChartConfig:
    """Configuration for chart styling.

    Attributes:
        figure_width: Width of the figure in inches (default 12)
        figure_height: Height of the figure in inches (default 6)
        dpi: Resolution for saved images (default 150)
        font_size: Base font size for labels (default 10)
        title_size: Font size for titles (default 14)
        line_width: Width of main plot lines (default 2.0)
        line_color: Color for the equity curve line (default blue)
        fill_alpha: Alpha for fill under curve (default 0.1)
        grid_alpha: Alpha for grid lines (default 0.3)
        benchmark_color: Color for benchmark line (default gray)
        benchmark_line_style: Line style for benchmark (default dashed)
        drawdown_color: Color for drawdown shading (default red)
        drawdown_alpha: Alpha for drawdown shading (default 0.2)
        entry_marker: Marker style for entry/buy trades (default "^" triangle up)
        exit_marker: Marker style for exit/sell trades (default "v" triangle down)
        entry_color: Color for entry markers (default green)
        exit_color: Color for exit markers (default red)
        profit_color: Color for profitable trades (default green)
        loss_color: Color for losing trades (default red)
        marker_size: Size of trade markers (default 80)
    """

    figure_width: float = 12.0
    figure_height: float = 6.0
    dpi: int = 150
    font_size: int = 10
    title_size: int = 14
    line_width: float = 2.0
    line_color: str = "#2196F3"
    fill_alpha: float = 0.1
    grid_alpha: float = 0.3
    benchmark_color: str = "#757575"
    benchmark_line_style: str = "--"
    drawdown_color: str = "#F44336"
    drawdown_alpha: float = 0.2
    entry_marker: str = "^"
    exit_marker: str = "v"
    entry_color: str = "#4CAF50"
    exit_color: str = "#F44336"
    profit_color: str = "#4CAF50"
    loss_color: str = "#F44336"
    marker_size: int = 80


@dataclass
class DrawdownPeriod:
    """Represents a drawdown period for visualization.

    Attributes:
        start: Start timestamp of the drawdown
        end: End timestamp of the drawdown (when recovered or latest)
        peak_value: Portfolio value at the peak before drawdown
        trough_value: Lowest portfolio value during drawdown
        drawdown_pct: Percentage decline from peak to trough
    """

    start: datetime
    end: datetime
    peak_value: Decimal
    trough_value: Decimal
    drawdown_pct: Decimal


@dataclass
class ChartResult:
    """Result of a chart generation operation.

    Attributes:
        chart_type: Type of chart generated (e.g., "equity_curve")
        file_path: Path to the saved chart file (None if failed)
        success: Whether the chart was generated successfully
        error: Error message if generation failed (None if success)
        drawdown_periods: Drawdown periods that were highlighted (if show_drawdown=True)
        trade_markers: Trade markers that were plotted (if show_trades=True)
        format: Output format ("png" or "html")
    """

    chart_type: str
    file_path: Path | None
    success: bool
    error: str | None = None
    drawdown_periods: list[DrawdownPeriod] = field(default_factory=list)
    trade_markers: list[TradeMarker] = field(default_factory=list)
    format: str = "png"


_MISSING_MATPLOTLIB_ERROR = "matplotlib not installed. Run: pip install 'almanak[backtest]'"
_MISSING_PLOTLY_ERROR = "plotly not installed. Run: pip install 'almanak[backtest]'"
_ENTRY_TRADE_TYPES = {"SWAP", "LP_OPEN", "PERP_OPEN", "BORROW", "SUPPLY", "BRIDGE"}
_EXIT_TRADE_TYPES = {"LP_CLOSE", "PERP_CLOSE", "REPAY", "WITHDRAW"}
_SKIPPED_MARKER_TYPES = {"HOLD", "UNKNOWN"}
_INTENT_COLORS = {
    "SWAP": "#2196F3",
    "LP_OPEN": "#4CAF50",
    "LP_CLOSE": "#F44336",
    "PERP_OPEN": "#9C27B0",
    "PERP_CLOSE": "#E91E63",
    "BORROW": "#FF9800",
    "REPAY": "#FFEB3B",
    "SUPPLY": "#00BCD4",
    "WITHDRAW": "#795548",
    "BRIDGE": "#607D8B",
    "ENSURE_BALANCE": "#9E9E9E",
}


@dataclass(frozen=True)
class _DurationScatterData:
    durations: list[float]
    pnl_values: list[float]
    colors: list[str]


def _chart_failure(chart_type: str, error: str, *, format: str = "png") -> ChartResult:
    return ChartResult(chart_type=chart_type, file_path=None, success=False, error=error, format=format)


def _load_matplotlib(chart_type: str) -> tuple[Any | None, ChartResult | None]:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        logger.error(_MISSING_MATPLOTLIB_ERROR)
        return None, _chart_failure(chart_type, _MISSING_MATPLOTLIB_ERROR)
    return plt, None


def _load_plotly_graph_objects(warning_context: str) -> Any | None:
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.warning("%s - cannot generate %s", _MISSING_PLOTLY_ERROR, warning_context)
        return None
    return go


def _safe_deployment_id(deployment_id: str) -> str:
    return deployment_id.replace("/", "_").replace("\\", "_")


def _chart_output_path(
    result: "BacktestResult",
    output_path: Path | str | None,
    *,
    prefix: str,
    suffix: str,
) -> Path:
    if output_path is None:
        return Path(f"{prefix}_{_safe_deployment_id(result.deployment_id)}.{suffix}")
    if isinstance(output_path, str):
        return Path(output_path)
    return output_path


def _ensure_output_parent(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)


def _as_float(value: Decimal | float | int) -> float:
    return float(value)


def _trade_intent_name(trade: "TradeRecord") -> str:
    intent_type = trade.intent_type
    return intent_type.value if hasattr(intent_type, "value") else str(intent_type)


def _result_trades(result: "BacktestResult") -> list["TradeRecord"]:
    trades = getattr(result, "trades", None)
    return trades if trades else []


def _trade_pnl_values(trades: list["TradeRecord"]) -> list[float]:
    pnl_values: list[float] = []
    for trade in trades:
        if hasattr(trade, "pnl_usd") and trade.pnl_usd is not None:
            pnl_values.append(_as_float(trade.pnl_usd))
    return pnl_values


def _marker_is_entry(trade_type: str) -> bool:
    return trade_type in _ENTRY_TRADE_TYPES and trade_type not in _EXIT_TRADE_TYPES


def _closest_equity_value(
    trade_time: datetime,
    equity_lookup: dict[datetime, Decimal],
    sorted_timestamps: list[datetime],
) -> Decimal:
    exact_value = equity_lookup.get(trade_time)
    if exact_value is not None:
        return exact_value
    closest_time = min(sorted_timestamps, key=lambda timestamp: abs((timestamp - trade_time).total_seconds()))
    return equity_lookup[closest_time]


def _detect_drawdown_periods(
    timestamps: list[datetime],
    values: list[float],
    min_drawdown_pct: float = 0.01,
) -> list[DrawdownPeriod]:
    """Detect significant drawdown periods in equity curve.

    A drawdown period starts when the value drops below a previous peak
    and ends when the value returns to that peak level.

    Args:
        timestamps: List of timestamps
        values: List of portfolio values
        min_drawdown_pct: Minimum drawdown to highlight (default 1%)

    Returns:
        List of DrawdownPeriod instances
    """
    if len(values) < 2:
        return []

    drawdown_periods: list[DrawdownPeriod] = []
    peak_value = values[0]
    peak_idx = 0
    in_drawdown = False
    trough_value = peak_value
    drawdown_start_idx = 0

    for i, value in enumerate(values):
        if value >= peak_value:
            # We've recovered or made new high
            if in_drawdown:
                # End the current drawdown period
                drawdown_pct = (peak_value - trough_value) / peak_value
                if drawdown_pct >= min_drawdown_pct:
                    drawdown_periods.append(
                        DrawdownPeriod(
                            start=timestamps[drawdown_start_idx],
                            end=timestamps[i],
                            peak_value=Decimal(str(peak_value)),
                            trough_value=Decimal(str(trough_value)),
                            drawdown_pct=Decimal(str(drawdown_pct)),
                        )
                    )
                in_drawdown = False
            # Update peak
            peak_value = value
            peak_idx = i
            trough_value = value
        else:
            # We're in a drawdown
            if not in_drawdown:
                in_drawdown = True
                drawdown_start_idx = peak_idx
                trough_value = value
            else:
                trough_value = min(trough_value, value)

    # Handle case where we end in a drawdown
    if in_drawdown:
        drawdown_pct = (peak_value - trough_value) / peak_value
        if drawdown_pct >= min_drawdown_pct:
            drawdown_periods.append(
                DrawdownPeriod(
                    start=timestamps[drawdown_start_idx],
                    end=timestamps[-1],
                    peak_value=Decimal(str(peak_value)),
                    trough_value=Decimal(str(trough_value)),
                    drawdown_pct=Decimal(str(drawdown_pct)),
                )
            )

    return drawdown_periods


def _extract_trade_markers(
    trades: list["TradeRecord"],
    equity_curve: list["EquityPoint"],
) -> list[TradeMarker]:
    """Extract trade markers from trade records aligned with equity curve values.

    Maps each trade to the closest equity curve timestamp and creates a
    TradeMarker for visualization.

    Args:
        trades: List of TradeRecord instances from backtest
        equity_curve: List of EquityPoint instances (the equity curve)

    Returns:
        List of TradeMarker instances for visualization
    """
    if not trades or not equity_curve:
        return []

    equity_lookup: dict[datetime, Decimal] = {point.timestamp: point.value_usd for point in equity_curve}
    sorted_timestamps = sorted(equity_lookup.keys())
    markers: list[TradeMarker] = []

    for trade in trades:
        trade_type = _trade_intent_name(trade)
        if trade_type in _SKIPPED_MARKER_TYPES:
            continue

        markers.append(
            TradeMarker(
                timestamp=trade.timestamp,
                value_usd=_closest_equity_value(trade.timestamp, equity_lookup, sorted_timestamps),
                is_entry=_marker_is_entry(trade_type),
                trade_type=trade_type,
                pnl_usd=trade.pnl_usd if hasattr(trade, "pnl_usd") else None,
            )
        )

    return markers


def calculate_distribution_stats(pnl_values: list[float]) -> DistributionStats | None:
    """Calculate distribution statistics for trade returns.

    Computes skewness, kurtosis, and other statistical measures for a list
    of trade PnL values. Uses Fisher's definitions for skewness and kurtosis.

    Args:
        pnl_values: List of PnL values (floats) from trades

    Returns:
        DistributionStats with computed statistics, or None if insufficient data

    Example:
        pnl_values = [100.0, -50.0, 200.0, -25.0, 150.0]
        stats = calculate_distribution_stats(pnl_values)
        if stats:
            print(f"Skewness: {stats.skewness:.2f}")
            print(f"Kurtosis: {stats.kurtosis:.2f}")
    """
    if len(pnl_values) < 3:
        # Need at least 3 values for meaningful statistics
        return None

    n = len(pnl_values)
    sorted_values = sorted(pnl_values)

    # Basic statistics
    mean = sum(pnl_values) / n

    # Median
    if n % 2 == 0:
        median = (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
    else:
        median = sorted_values[n // 2]

    # Standard deviation (sample)
    variance = sum((x - mean) ** 2 for x in pnl_values) / (n - 1) if n > 1 else 0
    std_dev = variance**0.5

    # Skewness (Fisher's definition, adjusted for sample size)
    # Skewness = E[(X - mean)^3] / std^3
    if std_dev > 0 and n > 2:
        m3 = sum((x - mean) ** 3 for x in pnl_values) / n
        skewness = m3 / (std_dev**3)
        # Apply sample size adjustment (Fisher-Pearson)
        skewness = skewness * (n * (n - 1)) ** 0.5 / (n - 2)
    else:
        skewness = 0.0

    # Kurtosis (Fisher's definition - excess kurtosis, so normal = 0)
    # Kurtosis = E[(X - mean)^4] / std^4 - 3
    if std_dev > 0 and n > 3:
        m4 = sum((x - mean) ** 4 for x in pnl_values) / n
        kurtosis = m4 / (std_dev**4) - 3
        # Apply sample size adjustment
        kurtosis = ((n + 1) * kurtosis + 6) * (n - 1) / ((n - 2) * (n - 3))
    else:
        kurtosis = 0.0

    # Percentiles (linear interpolation)
    def percentile(values: list[float], p: float) -> float:
        """Calculate percentile using linear interpolation."""
        if not values:
            return 0.0
        k = (len(values) - 1) * p / 100
        f = int(k)
        c = f + 1 if f + 1 < len(values) else f
        return values[f] + (values[c] - values[f]) * (k - f)

    return DistributionStats(
        mean=mean,
        median=median,
        std_dev=std_dev,
        skewness=skewness,
        kurtosis=kurtosis,
        min_return=sorted_values[0],
        max_return=sorted_values[-1],
        percentile_5=percentile(sorted_values, 5),
        percentile_95=percentile(sorted_values, 95),
        count=n,
    )


def _equity_drawdowns(
    show_drawdown: bool,
    timestamps: list[datetime],
    values: list[float],
    min_drawdown_pct: float,
) -> list[DrawdownPeriod]:
    if not show_drawdown:
        return []
    return _detect_drawdown_periods(timestamps, values, min_drawdown_pct)


def _add_static_drawdown_regions(
    ax: Any,
    cfg: ChartConfig,
    timestamps: list[datetime],
    drawdown_periods: list[DrawdownPeriod],
) -> None:
    for period in drawdown_periods:
        ax.axvspan(period.start, period.end, color=cfg.drawdown_color, alpha=cfg.drawdown_alpha, label=None)
    if drawdown_periods:
        ax.axvspan(
            timestamps[0],
            timestamps[0],
            color=cfg.drawdown_color,
            alpha=cfg.drawdown_alpha,
            label="Drawdown Period",
        )


def _add_static_benchmark_trace(
    ax: Any,
    cfg: ChartConfig,
    benchmark_curve: list["EquityPoint"] | None,
    benchmark_label: str,
) -> None:
    if not benchmark_curve:
        return
    benchmark_timestamps, benchmark_values = _extract_equity_chart_series(benchmark_curve)
    ax.plot(
        benchmark_timestamps,
        benchmark_values,
        linewidth=cfg.line_width,
        color=cfg.benchmark_color,
        linestyle=cfg.benchmark_line_style,
        label=benchmark_label,
    )


def _add_static_strategy_equity_trace(
    ax: Any,
    cfg: ChartConfig,
    timestamps: list[datetime],
    values: list[float],
) -> None:
    ax.plot(timestamps, values, linewidth=cfg.line_width, color=cfg.line_color, label="Strategy")
    ax.fill_between(timestamps, values, alpha=cfg.fill_alpha, color=cfg.line_color)


def _static_marker_color(marker: TradeMarker, cfg: ChartConfig, color_by_pnl: bool) -> str:
    if color_by_pnl and marker.pnl_usd is not None:
        return cfg.profit_color if marker.pnl_usd >= 0 else cfg.loss_color
    return cfg.entry_color if marker.is_entry else cfg.exit_color


def _add_static_marker_group(
    ax: Any,
    cfg: ChartConfig,
    markers: list[TradeMarker],
    *,
    marker: str,
    label: str,
    color_by_pnl: bool,
) -> None:
    if not markers:
        return
    ax.scatter(
        [trade_marker.timestamp for trade_marker in markers],
        [_as_float(trade_marker.value_usd) for trade_marker in markers],
        c=[_static_marker_color(trade_marker, cfg, color_by_pnl) for trade_marker in markers],
        marker=marker,
        s=cfg.marker_size,
        zorder=5,
        label=label,
        edgecolors="white",
        linewidths=0.5,
    )


def _add_static_trade_markers(
    ax: Any,
    cfg: ChartConfig,
    trade_markers: list[TradeMarker],
    *,
    color_by_pnl: bool,
) -> None:
    entry_markers = [marker for marker in trade_markers if marker.is_entry]
    exit_markers = [marker for marker in trade_markers if not marker.is_entry]
    _add_static_marker_group(
        ax,
        cfg,
        entry_markers,
        marker=cfg.entry_marker,
        label="Entry",
        color_by_pnl=color_by_pnl,
    )
    _add_static_marker_group(
        ax,
        cfg,
        exit_markers,
        marker=cfg.exit_marker,
        label="Exit",
        color_by_pnl=color_by_pnl,
    )


def _static_trade_markers(result: "BacktestResult", show_trades: bool) -> list[TradeMarker]:
    trades = _result_trades(result)
    if not show_trades or not trades:
        return []
    return _extract_trade_markers(trades, result.equity_curve)


def _style_static_equity_axes(ax: Any, plt: Any, cfg: ChartConfig, chart_title: str) -> None:
    ax.set_title(chart_title, fontsize=cfg.title_size, fontweight="bold")
    ax.set_xlabel("Time", fontsize=cfg.font_size)
    ax.set_ylabel("Portfolio Value (USD)", fontsize=cfg.font_size)
    ax.tick_params(labelsize=cfg.font_size)
    ax.grid(True, alpha=cfg.grid_alpha)
    ax.legend(loc="upper left")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()


def _save_static_chart(fig: Any, plt: Any, output_path: Path, cfg: ChartConfig) -> None:
    fig.savefig(output_path, dpi=cfg.dpi, bbox_inches="tight")
    plt.close(fig)


def _add_static_pnl_histogram_bars(ax: Any, cfg: ChartConfig, pnl_values: list[float], bins: int) -> None:
    _, bin_edges, patches = ax.hist(pnl_values, bins=bins, edgecolor="white", linewidth=0.5)
    for i, patch in enumerate(patches):
        bin_mid = (bin_edges[i] + bin_edges[i + 1]) / 2
        patch.set_facecolor(cfg.profit_color if bin_mid >= 0 else cfg.loss_color)


def _format_distribution_stats(stats: DistributionStats) -> str:
    return (
        f"Mean: ${stats.mean:,.2f}\n"
        f"Median: ${stats.median:,.2f}\n"
        f"Std Dev: ${stats.std_dev:,.2f}\n"
        f"Skewness: {stats.skewness:.3f}\n"
        f"Kurtosis: {stats.kurtosis:.3f}\n"
        f"5th %ile: ${stats.percentile_5:,.2f}\n"
        f"95th %ile: ${stats.percentile_95:,.2f}"
    )


def _add_static_pnl_stats(
    ax: Any, cfg: ChartConfig, pnl_values: list[float], show_stats: bool
) -> DistributionStats | None:
    if not show_stats:
        return None
    stats = calculate_distribution_stats(pnl_values)
    if stats is None:
        return None
    ax.text(
        0.02,
        0.98,
        _format_distribution_stats(stats),
        transform=ax.transAxes,
        fontsize=cfg.font_size - 1,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "wheat", "alpha": 0.8},
        family="monospace",
    )
    ax.axvline(x=stats.mean, color="#1976D2", linestyle="-.", linewidth=1.2, label=f"Mean (${stats.mean:,.0f})")
    ax.axvline(
        x=stats.median,
        color="#7B1FA2",
        linestyle=":",
        linewidth=1.2,
        label=f"Median (${stats.median:,.0f})",
    )
    return stats


def _style_static_pnl_axes(ax: Any, plt: Any, cfg: ChartConfig, chart_title: str) -> None:
    ax.set_title(chart_title, fontsize=cfg.title_size, fontweight="bold")
    ax.set_xlabel("PnL (USD)", fontsize=cfg.font_size)
    ax.set_ylabel("Number of Trades", fontsize=cfg.font_size)
    ax.tick_params(labelsize=cfg.font_size)
    ax.grid(True, alpha=cfg.grid_alpha, axis="y")
    ax.legend(loc="upper right")
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    plt.tight_layout()


def _duration_from_metadata(metadata: dict[str, Any]) -> float | None:
    if "duration_hours" in metadata:
        return float(metadata["duration_hours"])
    if "duration_minutes" in metadata:
        return float(metadata["duration_minutes"]) / 60
    if "hold_time_seconds" in metadata:
        return float(metadata["hold_time_seconds"]) / 3600
    return None


def _trade_duration_hours(sorted_trades: list["TradeRecord"], index: int) -> float | None:
    trade = sorted_trades[index]
    metadata = getattr(trade, "metadata", None)
    if metadata:
        duration_hours = _duration_from_metadata(metadata)
        if duration_hours is not None:
            return duration_hours
    if index == 0:
        return None
    previous_trade = sorted_trades[index - 1]
    time_diff = (trade.timestamp - previous_trade.timestamp).total_seconds() / 3600
    return max(0.1, time_diff)


def _duration_scatter_data(trades: list["TradeRecord"], cfg: ChartConfig) -> _DurationScatterData:
    durations: list[float] = []
    pnl_values: list[float] = []
    colors: list[str] = []
    sorted_trades = sorted(trades, key=lambda trade: trade.timestamp)

    for i, trade in enumerate(sorted_trades):
        if not hasattr(trade, "pnl_usd") or trade.pnl_usd is None:
            continue
        duration_hours = _trade_duration_hours(sorted_trades, i)
        if duration_hours is None:
            continue
        pnl = _as_float(trade.pnl_usd)
        durations.append(duration_hours)
        pnl_values.append(pnl)
        colors.append(cfg.profit_color if pnl >= 0 else cfg.loss_color)

    return _DurationScatterData(durations=durations, pnl_values=pnl_values, colors=colors)


def _style_duration_axes(ax: Any, plt: Any, cfg: ChartConfig, chart_title: str) -> None:
    ax.set_title(chart_title, fontsize=cfg.title_size, fontweight="bold")
    ax.set_xlabel("Duration (hours)", fontsize=cfg.font_size)
    ax.set_ylabel("PnL (USD)", fontsize=cfg.font_size)
    ax.tick_params(labelsize=cfg.font_size)
    ax.grid(True, alpha=cfg.grid_alpha)
    ax.legend(loc="upper right")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    plt.tight_layout()


def _intent_counts(trades: list["TradeRecord"]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for trade in trades:
        if not hasattr(trade, "intent_type"):
            continue
        intent_name = _trade_intent_name(trade)
        if intent_name == "HOLD":
            continue
        counts[intent_name] = counts.get(intent_name, 0) + 1
    return counts


def _intent_pie_data(intent_counts: dict[str, int]) -> tuple[list[str], list[int], list[str]]:
    sorted_intents = sorted(intent_counts.items(), key=lambda item: item[1], reverse=True)
    labels = [intent for intent, _ in sorted_intents]
    sizes = [count for _, count in sorted_intents]
    colors = [_INTENT_COLORS.get(label, "#757575") for label in labels]
    return labels, sizes, colors


def _style_intent_pie_text(texts: Any, autotexts: Any, cfg: ChartConfig) -> None:
    for text in texts:
        text.set_fontsize(cfg.font_size)
    for autotext in autotexts:
        autotext.set_fontsize(cfg.font_size - 1)
        autotext.set_color("white")
        autotext.set_fontweight("bold")


# crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
def plot_equity_curve(  # noqa: C901
    result: "BacktestResult",
    output_path: Path | str | None = None,
    config: ChartConfig | None = None,
    title: str | None = None,
    benchmark_curve: list["EquityPoint"] | None = None,
    benchmark_label: str = "Benchmark",
    show_drawdown: bool = False,
    min_drawdown_pct: float = 0.01,
    show_trades: bool = False,
    color_by_pnl: bool = True,
) -> ChartResult:
    """Generate an equity curve plot from backtest results.

    Creates a line chart showing portfolio value over time. The chart
    includes proper axis labels, a title, and fills under the curve
    for visual clarity. Supports optional benchmark comparison,
    drawdown highlighting, and trade markers.

    Args:
        result: BacktestResult containing equity curve data
        output_path: Path to save the PNG file. If None, saves to
            current directory as 'equity_curve_{deployment_id}.png'
        config: Optional chart styling configuration. Uses defaults if None.
        title: Optional custom title. If None, auto-generates from deployment_id.
        benchmark_curve: Optional benchmark equity curve (e.g., ETH hold) for comparison.
            Should have the same timestamps as the strategy equity curve.
        benchmark_label: Label for the benchmark in the legend (default "Benchmark")
        show_drawdown: If True, highlight drawdown periods with shading (default False)
        min_drawdown_pct: Minimum drawdown to highlight as decimal (default 0.01 = 1%)
        show_trades: If True, mark trade entry/exit points on the chart (default False)
        color_by_pnl: If True, color trade markers by profit/loss (green/red).
            If False, color by entry/exit (default True)

    Returns:
        ChartResult with file path, success status, detected drawdown periods, and trade markers

    Example:
        result = await backtester.backtest(strategy, config)
        chart = plot_equity_curve(result, output_path=Path("charts/equity.png"))
        if chart.success:
            print(f"Saved to: {chart.file_path}")

        # With benchmark comparison and drawdown highlighting
        chart = plot_equity_curve(
            result,
            benchmark_curve=eth_hold_curve,
            benchmark_label="ETH Hold",
            show_drawdown=True,
        )

        # With trade markers
        chart = plot_equity_curve(
            result,
            show_trades=True,
            color_by_pnl=True,  # Green for profit, red for loss
        )
    """
    plt, failure = _load_matplotlib("equity_curve")
    if failure is not None:
        return failure
    assert plt is not None

    if not result.equity_curve:
        return _chart_failure("equity_curve", "No equity curve data in backtest result")

    cfg = config or ChartConfig()
    output_path = _chart_output_path(result, output_path, prefix="equity_curve", suffix="png")
    _ensure_output_parent(output_path)

    try:
        timestamps, values = _extract_equity_chart_series(result.equity_curve)
        drawdown_periods = _equity_drawdowns(show_drawdown, timestamps, values, min_drawdown_pct)
        fig, ax = plt.subplots(figsize=(cfg.figure_width, cfg.figure_height))

        _add_static_drawdown_regions(ax, cfg, timestamps, drawdown_periods)
        _add_static_benchmark_trace(ax, cfg, benchmark_curve, benchmark_label)
        _add_static_strategy_equity_trace(ax, cfg, timestamps, values)
        trade_markers = _static_trade_markers(result, show_trades)
        _add_static_trade_markers(ax, cfg, trade_markers, color_by_pnl=color_by_pnl)
        chart_title = title or f"Equity Curve - {result.deployment_id}"
        _style_static_equity_axes(ax, plt, cfg, chart_title)
        _save_static_chart(fig, plt, output_path, cfg)

        logger.info("Created equity curve plot: %s", output_path)
        if benchmark_curve:
            logger.info("Added benchmark comparison: %s", benchmark_label)
        if show_drawdown:
            logger.info("Highlighted %d drawdown period(s)", len(drawdown_periods))
        if show_trades:
            logger.info("Marked %d trade(s) on chart", len(trade_markers))

        return ChartResult(
            chart_type="equity_curve",
            file_path=output_path,
            success=True,
            drawdown_periods=drawdown_periods,
            trade_markers=trade_markers,
            format="png",
        )

    except Exception as e:
        logger.exception("Failed to create equity curve plot")
        return _chart_failure("equity_curve", str(e))


def _interactive_equity_output_path(
    result: "BacktestResult",
    output_path: Path | str | None,
) -> Path:
    if output_path is None:
        safe_id = result.deployment_id.replace("/", "_").replace("\\", "_")
        return Path(f"equity_curve_{safe_id}.html")
    if isinstance(output_path, str):
        return Path(output_path)
    return output_path


def _interactive_equity_drawdowns(
    show_drawdown: bool,
    timestamps: list[datetime],
    values: list[float],
    min_drawdown_pct: float,
) -> list[DrawdownPeriod]:
    if not show_drawdown:
        return []
    return _detect_drawdown_periods(timestamps, values, min_drawdown_pct)


def _add_interactive_equity_drawdowns(fig: Any, drawdown_periods: list[DrawdownPeriod]) -> None:
    for period in drawdown_periods:
        fig.add_vrect(
            x0=period.start,
            x1=period.end,
            fillcolor="rgba(244, 67, 54, 0.2)",
            layer="below",
            line_width=0,
        )


def _add_interactive_benchmark_trace(
    fig: Any,
    go: Any,
    benchmark_curve: list["EquityPoint"] | None,
    benchmark_label: str,
) -> None:
    if not benchmark_curve:
        return

    benchmark_timestamps, benchmark_values = _extract_equity_chart_series(benchmark_curve)
    fig.add_trace(
        go.Scatter(
            x=benchmark_timestamps,
            y=benchmark_values,
            name=benchmark_label,
            line={"color": "#757575", "dash": "dash"},
            hovertemplate="<b>%{x}</b><br>" + benchmark_label + ": $%{y:,.2f}<extra></extra>",
        )
    )


def _add_interactive_strategy_trace(
    fig: Any,
    go: Any,
    timestamps: list[datetime],
    values: list[float],
) -> None:
    fig.add_trace(
        go.Scatter(
            x=timestamps,
            y=values,
            name="Strategy",
            line={"color": "#2196F3", "width": 2},
            fill="tozeroy",
            fillcolor="rgba(33, 150, 243, 0.1)",
            hovertemplate="<b>%{x}</b><br>Value: $%{y:,.2f}<extra></extra>",
        )
    )


def _interactive_trade_markers(result: "BacktestResult", show_trades: bool) -> list[TradeMarker]:
    if show_trades and hasattr(result, "trades") and result.trades:
        return _extract_trade_markers(result.trades, result.equity_curve)
    return []


def _interactive_marker_color(marker: TradeMarker, color_by_pnl: bool) -> str:
    if color_by_pnl and marker.pnl_usd is not None:
        return "#4CAF50" if marker.pnl_usd >= 0 else "#F44336"
    return "#4CAF50" if marker.is_entry else "#F44336"


def _interactive_marker_customdata(markers: list[TradeMarker]) -> list[dict[str, float | str | None]]:
    return [
        {
            "type": marker.trade_type,
            "pnl": float(marker.pnl_usd) if marker.pnl_usd is not None else None,
        }
        for marker in markers
    ]


def _add_interactive_trade_marker_trace(
    fig: Any,
    go: Any,
    markers: list[TradeMarker],
    *,
    name: str,
    symbol: str,
    colors: list[str],
    hovertemplate: str,
) -> None:
    fig.add_trace(
        go.Scatter(
            x=[marker.timestamp for marker in markers],
            y=[float(marker.value_usd) for marker in markers],
            mode="markers",
            name=name,
            marker={
                "symbol": symbol,
                "size": 12,
                "color": colors,
                "line": {"color": "white", "width": 1},
            },
            hovertemplate=hovertemplate,
            customdata=_interactive_marker_customdata(markers),
        )
    )


def _add_interactive_trade_traces(
    fig: Any,
    go: Any,
    trade_markers: list[TradeMarker],
    color_by_pnl: bool,
) -> None:
    entry_markers, exit_markers = _split_trade_markers(trade_markers)
    if entry_markers:
        _add_interactive_trade_marker_trace(
            fig,
            go,
            entry_markers,
            name="Entry",
            symbol="triangle-up",
            colors=[_interactive_marker_color(marker, color_by_pnl) for marker in entry_markers],
            hovertemplate="<b>%{x}</b><br>Entry<br>Value: $%{y:,.2f}<br><extra></extra>",
        )
    if exit_markers:
        _add_interactive_trade_marker_trace(
            fig,
            go,
            exit_markers,
            name="Exit",
            symbol="triangle-down",
            colors=[_interactive_marker_color(marker, color_by_pnl) for marker in exit_markers],
            hovertemplate="<b>%{x}</b><br>Exit<br>Value: $%{y:,.2f}<br><extra></extra>",
        )


def _apply_interactive_equity_layout(fig: Any, chart_title: str) -> None:
    fig.update_layout(
        title={
            "text": chart_title,
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 18, "color": "#333"},
        },
        xaxis_title="Time",
        yaxis_title="Portfolio Value (USD)",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        hovermode="x unified",
        legend={"yanchor": "top", "y": 0.99, "xanchor": "left", "x": 0.01},
        template="plotly_white",
        margin={"l": 60, "r": 30, "t": 60, "b": 60},
    )
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector={
            "buttons": [
                {"count": 7, "label": "1w", "step": "day", "stepmode": "backward"},
                {"count": 1, "label": "1m", "step": "month", "stepmode": "backward"},
                {"count": 3, "label": "3m", "step": "month", "stepmode": "backward"},
                {"step": "all", "label": "All"},
            ]
        },
    )


def _build_interactive_equity_figure(
    go: Any,
    result: "BacktestResult",
    title: str | None,
    benchmark_curve: list["EquityPoint"] | None,
    benchmark_label: str,
    show_drawdown: bool,
    min_drawdown_pct: float,
    show_trades: bool,
    color_by_pnl: bool,
) -> tuple[Any, list[DrawdownPeriod], list[TradeMarker]]:
    timestamps, values = _extract_equity_chart_series(result.equity_curve)
    drawdown_periods = _interactive_equity_drawdowns(show_drawdown, timestamps, values, min_drawdown_pct)
    trade_markers = _interactive_trade_markers(result, show_trades)

    fig = go.Figure()
    _add_interactive_equity_drawdowns(fig, drawdown_periods)
    _add_interactive_benchmark_trace(fig, go, benchmark_curve, benchmark_label)
    _add_interactive_strategy_trace(fig, go, timestamps, values)
    _add_interactive_trade_traces(fig, go, trade_markers, color_by_pnl)
    _apply_interactive_equity_layout(fig, title or f"Equity Curve - {result.deployment_id}")
    return fig, drawdown_periods, trade_markers


def _log_interactive_equity_result(
    output_path: Path,
    benchmark_curve: list["EquityPoint"] | None,
    benchmark_label: str,
    show_drawdown: bool,
    drawdown_periods: list[DrawdownPeriod],
    show_trades: bool,
    trade_markers: list[TradeMarker],
) -> None:
    logger.info("Created interactive equity curve plot: %s", output_path)
    if benchmark_curve:
        logger.info("Added benchmark comparison: %s", benchmark_label)
    if show_drawdown:
        logger.info("Highlighted %d drawdown period(s)", len(drawdown_periods))
    if show_trades:
        logger.info("Marked %d trade(s) on chart", len(trade_markers))


def plot_equity_curve_interactive(
    result: "BacktestResult",
    output_path: Path | str | None = None,
    title: str | None = None,
    benchmark_curve: list["EquityPoint"] | None = None,
    benchmark_label: str = "Benchmark",
    show_drawdown: bool = False,
    min_drawdown_pct: float = 0.01,
    show_trades: bool = False,
    color_by_pnl: bool = True,
) -> ChartResult:
    """Generate an interactive equity curve plot using Plotly.

    Creates an interactive HTML chart with zoom, pan, and hover tooltips.
    This is ideal for detailed analysis and sharing via web.

    Args:
        result: BacktestResult containing equity curve data
        output_path: Path to save the HTML file. If None, saves to
            current directory as 'equity_curve_{deployment_id}.html'
        title: Optional custom title. If None, auto-generates from deployment_id.
        benchmark_curve: Optional benchmark equity curve for comparison.
        benchmark_label: Label for the benchmark in the legend (default "Benchmark")
        show_drawdown: If True, highlight drawdown periods with shading (default False)
        min_drawdown_pct: Minimum drawdown to highlight as decimal (default 0.01 = 1%)
        show_trades: If True, mark trade entry/exit points on the chart (default False)
        color_by_pnl: If True, color trade markers by profit/loss (default True)

    Returns:
        ChartResult with file path and success status

    Example:
        chart = plot_equity_curve_interactive(
            backtest_result,
            show_trades=True,
            show_drawdown=True,
        )
        if chart.success:
            print(f"Interactive chart saved to: {chart.file_path}")
    """
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.error("plotly not installed. Run: pip install 'almanak[backtest]'")
        return ChartResult(
            chart_type="equity_curve",
            file_path=None,
            success=False,
            error="plotly not installed. Run: pip install 'almanak[backtest]'",
            format="html",
        )

    # Validate input
    if not result.equity_curve:
        return ChartResult(
            chart_type="equity_curve",
            file_path=None,
            success=False,
            error="No equity curve data in backtest result",
            format="html",
        )

    output_path = _interactive_equity_output_path(result, output_path)

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        fig, drawdown_periods, trade_markers = _build_interactive_equity_figure(
            go,
            result,
            title,
            benchmark_curve,
            benchmark_label,
            show_drawdown,
            min_drawdown_pct,
            show_trades,
            color_by_pnl,
        )
        fig.write_html(str(output_path), include_plotlyjs=True, full_html=True)
        _log_interactive_equity_result(
            output_path,
            benchmark_curve,
            benchmark_label,
            show_drawdown,
            drawdown_periods,
            show_trades,
            trade_markers,
        )

        return ChartResult(
            chart_type="equity_curve",
            file_path=output_path,
            success=True,
            drawdown_periods=drawdown_periods,
            trade_markers=trade_markers,
            format="html",
        )

    except Exception as e:
        logger.exception("Failed to create interactive equity curve plot")
        return ChartResult(
            chart_type="equity_curve",
            file_path=None,
            success=False,
            error=str(e),
            format="html",
        )


def plot_pnl_histogram(
    result: "BacktestResult",
    output_path: Path | str | None = None,
    config: ChartConfig | None = None,
    title: str | None = None,
    bins: int = 20,
    show_stats: bool = False,
) -> ChartResult:
    """Generate a histogram of trade PnL distribution.

    Creates a histogram showing the distribution of trade profits and losses.
    Helps identify whether the strategy has more winning or losing trades,
    and the typical magnitude of gains vs losses. Optionally displays
    distribution statistics (skewness, kurtosis, etc.) on the chart.

    Args:
        result: BacktestResult containing trades data
        output_path: Path to save the PNG file. If None, saves to
            current directory as 'pnl_histogram_{deployment_id}.png'
        config: Optional chart styling configuration. Uses defaults if None.
        title: Optional custom title. If None, auto-generates from deployment_id.
        bins: Number of histogram bins (default 20)
        show_stats: If True, display distribution statistics (skewness, kurtosis,
            mean, std dev) on the chart (default False)

    Returns:
        ChartResult with file path and success status

    Example:
        result = await backtester.backtest(strategy, config)
        chart = plot_pnl_histogram(result, output_path=Path("charts/pnl.png"), show_stats=True)
        if chart.success:
            print(f"Saved to: {chart.file_path}")
    """
    plt, failure = _load_matplotlib("pnl_histogram")
    if failure is not None:
        return failure
    assert plt is not None

    trades = _result_trades(result)
    if not trades:
        return _chart_failure("pnl_histogram", "No trades data in backtest result")

    cfg = config or ChartConfig()
    output_path = _chart_output_path(result, output_path, prefix="pnl_histogram", suffix="png")
    _ensure_output_parent(output_path)

    try:
        pnl_values = _trade_pnl_values(trades)
        if not pnl_values:
            return _chart_failure("pnl_histogram", "No PnL data in trades")

        fig, ax = plt.subplots(figsize=(cfg.figure_width, cfg.figure_height))
        _add_static_pnl_histogram_bars(ax, cfg, pnl_values, bins)
        ax.axvline(x=0, color="#757575", linestyle="--", linewidth=1.5, label="Break-even")
        stats = _add_static_pnl_stats(ax, cfg, pnl_values, show_stats)
        chart_title = title or f"Trade PnL Distribution - {result.deployment_id}"
        _style_static_pnl_axes(ax, plt, cfg, chart_title)
        _save_static_chart(fig, plt, output_path, cfg)

        logger.info("Created PnL histogram: %s", output_path)
        if show_stats and stats:
            logger.info("Skewness: %.3f, Kurtosis: %.3f", stats.skewness, stats.kurtosis)

        return ChartResult(
            chart_type="pnl_histogram",
            file_path=output_path,
            success=True,
            format="png",
        )

    except Exception as e:
        logger.exception("Failed to create PnL histogram")
        return _chart_failure("pnl_histogram", str(e))


def _pnl_histogram_bin_spec(pnl_values: list[float], bins: int) -> tuple[float, float, float]:
    all_min = min(pnl_values)
    all_max = max(pnl_values)
    bin_size = (all_max - all_min) / bins if all_max != all_min else 1
    return all_min, all_max, bin_size


def _add_interactive_pnl_histogram_trace(
    fig: Any,
    go: Any,
    values: list[float],
    *,
    name: str,
    color: str,
    bin_spec: tuple[float, float, float],
) -> None:
    if not values:
        return
    all_min, all_max, bin_size = bin_spec
    fig.add_trace(
        go.Histogram(
            x=values,
            name=name,
            marker_color=color,
            xbins={"start": all_min, "end": all_max, "size": bin_size},
            hovertemplate="PnL: $%{x:,.2f}<br>Count: %{y}<extra></extra>",
        )
    )


def _add_interactive_pnl_histogram_traces(fig: Any, go: Any, pnl_values: list[float], bins: int) -> None:
    bin_spec = _pnl_histogram_bin_spec(pnl_values, bins)
    _add_interactive_pnl_histogram_trace(
        fig,
        go,
        [value for value in pnl_values if value < 0],
        name="Losses",
        color="rgba(244, 67, 54, 0.7)",
        bin_spec=bin_spec,
    )
    _add_interactive_pnl_histogram_trace(
        fig,
        go,
        [value for value in pnl_values if value >= 0],
        name="Profits",
        color="rgba(76, 175, 80, 0.7)",
        bin_spec=bin_spec,
    )


def _add_interactive_pnl_reference_lines(fig: Any, stats: DistributionStats | None) -> None:
    fig.add_vline(
        x=0,
        line_dash="dash",
        line_color="#757575",
        line_width=2,
        annotation_text="Break-even",
        annotation_position="top",
    )
    if stats is None:
        return
    fig.add_vline(
        x=stats.mean,
        line_dash="dashdot",
        line_color="#1976D2",
        line_width=1.5,
        annotation_text=f"Mean (${stats.mean:,.0f})",
        annotation_position="bottom right",
    )
    fig.add_vline(
        x=stats.median,
        line_dash="dot",
        line_color="#7B1FA2",
        line_width=1.5,
        annotation_text=f"Median (${stats.median:,.0f})",
        annotation_position="bottom left",
    )


def _skewness_interpretation(skewness: float) -> str:
    if skewness > 0.5:
        return "Right-skewed (more large gains)"
    if skewness < -0.5:
        return "Left-skewed (more large losses)"
    return "Approximately symmetric"


def _kurtosis_interpretation(kurtosis: float) -> str:
    if kurtosis > 1:
        return "Fat tails (extreme values likely)"
    if kurtosis < -1:
        return "Thin tails (extreme values rare)"
    return "Normal-like tails"


def _interactive_pnl_stats_annotation(stats: DistributionStats | None, show_stats: bool) -> str:
    if not show_stats or stats is None:
        return ""
    return (
        f"<b>Distribution Statistics</b><br>"
        f"Mean: ${stats.mean:,.2f}<br>"
        f"Median: ${stats.median:,.2f}<br>"
        f"Std Dev: ${stats.std_dev:,.2f}<br>"
        f"<b>Skewness: {stats.skewness:.3f}</b> ({_skewness_interpretation(stats.skewness)})<br>"
        f"<b>Kurtosis: {stats.kurtosis:.3f}</b> ({_kurtosis_interpretation(stats.kurtosis)})<br>"
        f"Range: ${stats.min_return:,.2f} to ${stats.max_return:,.2f}<br>"
        f"5th-95th %ile: ${stats.percentile_5:,.2f} to ${stats.percentile_95:,.2f}<br>"
        f"Trade Count: {stats.count}"
    )


def _apply_interactive_pnl_layout(fig: Any, chart_title: str) -> None:
    fig.update_layout(
        title={"text": chart_title, "x": 0.5, "xanchor": "center", "font": {"size": 18, "color": "#333"}},
        xaxis_title="PnL (USD)",
        yaxis_title="Number of Trades",
        xaxis_tickprefix="$",
        xaxis_tickformat=",.0f",
        barmode="overlay",
        hovermode="x unified",
        legend={"yanchor": "top", "y": 0.99, "xanchor": "right", "x": 0.99},
        template="plotly_white",
        margin={"l": 60, "r": 30, "t": 80, "b": 60},
    )


def _add_interactive_pnl_stats_annotation(fig: Any, stats_annotation: str) -> None:
    if not stats_annotation:
        return
    fig.add_annotation(
        text=stats_annotation,
        xref="paper",
        yref="paper",
        x=0.02,
        y=0.98,
        showarrow=False,
        font={"size": 11, "family": "monospace"},
        align="left",
        bgcolor="rgba(255, 255, 255, 0.9)",
        bordercolor="#ccc",
        borderwidth=1,
        borderpad=8,
    )


def _build_interactive_pnl_histogram_figure(
    go: Any,
    result: "BacktestResult",
    pnl_values: list[float],
    *,
    title: str | None,
    bins: int,
    show_stats: bool,
) -> tuple[Any, DistributionStats | None]:
    stats = calculate_distribution_stats(pnl_values)
    fig = go.Figure()
    _add_interactive_pnl_histogram_traces(fig, go, pnl_values, bins)
    _add_interactive_pnl_reference_lines(fig, stats)
    _apply_interactive_pnl_layout(fig, title or f"Trade PnL Distribution - {result.deployment_id}")
    _add_interactive_pnl_stats_annotation(fig, _interactive_pnl_stats_annotation(stats, show_stats))
    return fig, stats


# crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
def plot_pnl_histogram_interactive(  # noqa: C901
    result: "BacktestResult",
    output_path: Path | str | None = None,
    title: str | None = None,
    bins: int = 20,
    show_stats: bool = True,
) -> ChartResult:
    """Generate an interactive PnL histogram using Plotly.

    Creates an interactive HTML histogram with hover tooltips showing
    distribution statistics (skewness, kurtosis, etc.). Ideal for
    detailed analysis and inclusion in HTML reports.

    Args:
        result: BacktestResult containing trades data
        output_path: Path to save the HTML file. If None, saves to
            current directory as 'pnl_histogram_{deployment_id}.html'
        title: Optional custom title. If None, auto-generates from deployment_id.
        bins: Number of histogram bins (default 20)
        show_stats: If True, display distribution statistics on the chart (default True)

    Returns:
        ChartResult with file path and success status

    Example:
        chart = plot_pnl_histogram_interactive(result, show_stats=True)
        if chart.success:
            print(f"Interactive histogram saved to: {chart.file_path}")
    """
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.error(_MISSING_PLOTLY_ERROR)
        return _chart_failure("pnl_histogram", _MISSING_PLOTLY_ERROR, format="html")

    trades = _result_trades(result)
    if not trades:
        return _chart_failure("pnl_histogram", "No trades data in backtest result", format="html")

    output_path = _chart_output_path(result, output_path, prefix="pnl_histogram", suffix="html")
    _ensure_output_parent(output_path)

    try:
        pnl_values = _trade_pnl_values(trades)
        if not pnl_values:
            return _chart_failure("pnl_histogram", "No PnL data in trades", format="html")

        fig, stats = _build_interactive_pnl_histogram_figure(
            go,
            result,
            pnl_values,
            title=title,
            bins=bins,
            show_stats=show_stats,
        )
        fig.write_html(str(output_path), include_plotlyjs=True, full_html=True)

        logger.info("Created interactive PnL histogram: %s", output_path)
        if stats:
            logger.info("Skewness: %.3f, Kurtosis: %.3f", stats.skewness, stats.kurtosis)

        return ChartResult(
            chart_type="pnl_histogram",
            file_path=output_path,
            success=True,
            format="html",
        )

    except Exception as e:
        logger.exception("Failed to create interactive PnL histogram")
        return _chart_failure("pnl_histogram", str(e), format="html")


def plot_duration_scatter(
    result: "BacktestResult",
    output_path: Path | str | None = None,
    config: ChartConfig | None = None,
    title: str | None = None,
) -> ChartResult:
    """Generate a scatter plot of trade duration vs PnL.

    Creates a scatter plot showing the relationship between how long a trade
    was held and its resulting profit or loss. Helps identify whether longer
    or shorter holding periods tend to be more profitable.

    Args:
        result: BacktestResult containing trades data
        output_path: Path to save the PNG file. If None, saves to
            current directory as 'duration_scatter_{deployment_id}.png'
        config: Optional chart styling configuration. Uses defaults if None.
        title: Optional custom title. If None, auto-generates from deployment_id.

    Returns:
        ChartResult with file path and success status

    Example:
        chart = plot_duration_scatter(result, output_path=Path("charts/duration.png"))
        if chart.success:
            print(f"Saved to: {chart.file_path}")
    """
    plt, failure = _load_matplotlib("duration_scatter")
    if failure is not None:
        return failure
    assert plt is not None

    trades = _result_trades(result)
    if not trades:
        return _chart_failure("duration_scatter", "No trades data in backtest result")

    cfg = config or ChartConfig()
    output_path = _chart_output_path(result, output_path, prefix="duration_scatter", suffix="png")
    _ensure_output_parent(output_path)

    try:
        scatter_data = _duration_scatter_data(trades, cfg)
        if not scatter_data.durations:
            return _chart_failure("duration_scatter", "No duration/PnL data available for scatter plot")

        fig, ax = plt.subplots(figsize=(cfg.figure_width, cfg.figure_height))
        ax.scatter(
            scatter_data.durations,
            scatter_data.pnl_values,
            c=scatter_data.colors,
            s=cfg.marker_size,
            alpha=0.7,
            edgecolors="white",
            linewidths=0.5,
        )
        ax.axhline(y=0, color="#757575", linestyle="--", linewidth=1.5, label="Break-even")
        chart_title = title or f"Trade Duration vs PnL - {result.deployment_id}"
        _style_duration_axes(ax, plt, cfg, chart_title)
        _save_static_chart(fig, plt, output_path, cfg)

        logger.info("Created duration scatter plot: %s", output_path)

        return ChartResult(
            chart_type="duration_scatter",
            file_path=output_path,
            success=True,
            format="png",
        )

    except Exception as e:
        logger.exception("Failed to create duration scatter plot")
        return _chart_failure("duration_scatter", str(e))


def plot_intent_pie(
    result: "BacktestResult",
    output_path: Path | str | None = None,
    config: ChartConfig | None = None,
    title: str | None = None,
) -> ChartResult:
    """Generate a pie chart of trades by intent type.

    Creates a pie chart showing the distribution of trades across different
    intent types (SWAP, LP_OPEN, LP_CLOSE, etc.). Helps understand the
    composition of trading activity.

    Args:
        result: BacktestResult containing trades data
        output_path: Path to save the PNG file. If None, saves to
            current directory as 'intent_pie_{deployment_id}.png'
        config: Optional chart styling configuration. Uses defaults if None.
        title: Optional custom title. If None, auto-generates from deployment_id.

    Returns:
        ChartResult with file path and success status

    Example:
        chart = plot_intent_pie(result, output_path=Path("charts/intents.png"))
        if chart.success:
            print(f"Saved to: {chart.file_path}")
    """
    plt, failure = _load_matplotlib("intent_pie")
    if failure is not None:
        return failure
    assert plt is not None

    trades = _result_trades(result)
    if not trades:
        return _chart_failure("intent_pie", "No trades data in backtest result")

    cfg = config or ChartConfig()
    output_path = _chart_output_path(result, output_path, prefix="intent_pie", suffix="png")
    _ensure_output_parent(output_path)

    try:
        intent_counts = _intent_counts(trades)
        if not intent_counts:
            return _chart_failure("intent_pie", "No intent type data in trades")

        labels, sizes, colors = _intent_pie_data(intent_counts)
        fig, ax = plt.subplots(figsize=(cfg.figure_width, cfg.figure_height))
        _, texts, autotexts = ax.pie(
            sizes,
            labels=labels,
            colors=colors,
            autopct=lambda pct: f"{pct:.1f}%\n({int(pct * sum(sizes) / 100)})",
            startangle=90,
            pctdistance=0.75,
        )

        _style_intent_pie_text(texts, autotexts, cfg)
        chart_title = title or f"Trades by Intent Type - {result.deployment_id}"
        ax.set_title(chart_title, fontsize=cfg.title_size, fontweight="bold")
        ax.axis("equal")
        plt.tight_layout()
        _save_static_chart(fig, plt, output_path, cfg)

        logger.info("Created intent pie chart: %s", output_path)

        return ChartResult(
            chart_type="intent_pie",
            file_path=output_path,
            success=True,
            format="png",
        )

    except Exception as e:
        logger.exception("Failed to create intent pie chart")
        return _chart_failure("intent_pie", str(e))


def _load_equity_chart_plotly() -> Any | None:
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.warning("plotly not installed (pip install 'almanak[backtest]') - cannot generate equity chart")
        return None
    return go


def _extract_equity_chart_series(equity_curve: list["EquityPoint"]) -> tuple[list[datetime], list[float]]:
    timestamps: list[datetime] = []
    values: list[float] = []
    for point in equity_curve:
        timestamps.append(point.timestamp)
        values.append(float(point.value_usd) if isinstance(point.value_usd, Decimal) else point.value_usd)
    return timestamps, values


def _equity_chart_drawdowns(
    show_drawdown: bool,
    timestamps: list[datetime],
    values: list[float],
) -> list[DrawdownPeriod]:
    if not show_drawdown:
        return []
    return _detect_drawdown_periods(timestamps, values, 0.01)


def _add_embedded_equity_drawdowns(fig: Any, drawdown_periods: list[DrawdownPeriod]) -> None:
    for period in drawdown_periods:
        fig.add_vrect(
            x0=period.start,
            x1=period.end,
            fillcolor="rgba(244, 67, 54, 0.15)",
            layer="below",
            line_width=0,
        )


def _add_embedded_equity_curve_trace(
    fig: Any,
    go: Any,
    timestamps: list[datetime],
    values: list[float],
) -> None:
    fig.add_trace(
        go.Scatter(
            x=timestamps,
            y=values,
            name="Portfolio Value",
            line={"color": "#2196F3", "width": 2},
            fill="tozeroy",
            fillcolor="rgba(33, 150, 243, 0.1)",
            hovertemplate="<b>%{x}</b><br>Value: $%{y:,.2f}<extra></extra>",
        )
    )


def _embedded_equity_trade_markers(result: "BacktestResult", show_trades: bool) -> list[TradeMarker]:
    trades = getattr(result, "trades", None)
    if not show_trades or not trades:
        return []
    return _extract_trade_markers(trades, result.equity_curve)


def _split_trade_markers(trade_markers: list[TradeMarker]) -> tuple[list[TradeMarker], list[TradeMarker]]:
    entry_markers = [marker for marker in trade_markers if marker.is_entry]
    exit_markers = [marker for marker in trade_markers if not marker.is_entry]
    return entry_markers, exit_markers


def _entry_marker_colors(markers: list[TradeMarker]) -> list[str]:
    return ["#4CAF50" if (marker.pnl_usd is None or marker.pnl_usd >= 0) else "#F44336" for marker in markers]


def _exit_marker_colors(markers: list[TradeMarker]) -> list[str]:
    return ["#4CAF50" if (marker.pnl_usd is not None and marker.pnl_usd >= 0) else "#F44336" for marker in markers]


def _add_embedded_trade_marker_trace(
    fig: Any,
    go: Any,
    markers: list[TradeMarker],
    *,
    name: str,
    symbol: str,
    colors: list[str],
    hovertemplate: str,
) -> None:
    fig.add_trace(
        go.Scatter(
            x=[marker.timestamp for marker in markers],
            y=[float(marker.value_usd) for marker in markers],
            mode="markers",
            name=name,
            marker={
                "symbol": symbol,
                "size": 10,
                "color": colors,
                "line": {"color": "white", "width": 1},
            },
            hovertemplate=hovertemplate,
        )
    )


def _add_embedded_equity_trade_traces(fig: Any, go: Any, trade_markers: list[TradeMarker]) -> None:
    entry_markers, exit_markers = _split_trade_markers(trade_markers)
    if entry_markers:
        _add_embedded_trade_marker_trace(
            fig,
            go,
            entry_markers,
            name="Entry",
            symbol="triangle-up",
            colors=_entry_marker_colors(entry_markers),
            hovertemplate="<b>%{x}</b><br>Entry<br>Value: $%{y:,.2f}<extra></extra>",
        )
    if exit_markers:
        _add_embedded_trade_marker_trace(
            fig,
            go,
            exit_markers,
            name="Exit",
            symbol="triangle-down",
            colors=_exit_marker_colors(exit_markers),
            hovertemplate="<b>%{x}</b><br>Exit<br>Value: $%{y:,.2f}<extra></extra>",
        )


def _apply_embedded_equity_layout(fig: Any, title: str | None, height: int) -> None:
    chart_title = title or "Equity Curve"
    fig.update_layout(
        title={"text": chart_title, "x": 0.5, "xanchor": "center", "font": {"size": 16}},
        xaxis_title="Time",
        yaxis_title="Portfolio Value (USD)",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        hovermode="x unified",
        legend={"yanchor": "top", "y": 0.99, "xanchor": "left", "x": 0.01},
        template="plotly_white",
        height=height,
        margin={"l": 60, "r": 30, "t": 50, "b": 50},
    )
    fig.update_xaxes(rangeslider_visible=True, rangeslider_thickness=0.05)


def _build_embedded_equity_chart_figure(
    go: Any,
    result: "BacktestResult",
    title: str | None,
    show_drawdown: bool,
    show_trades: bool,
    height: int,
) -> Any:
    timestamps, values = _extract_equity_chart_series(result.equity_curve)
    drawdown_periods = _equity_chart_drawdowns(show_drawdown, timestamps, values)
    trade_markers = _embedded_equity_trade_markers(result, show_trades)

    fig = go.Figure()
    _add_embedded_equity_drawdowns(fig, drawdown_periods)
    _add_embedded_equity_curve_trace(fig, go, timestamps, values)
    _add_embedded_equity_trade_traces(fig, go, trade_markers)
    _apply_embedded_equity_layout(fig, title, height)
    return fig


def generate_equity_chart_html(
    result: "BacktestResult",
    title: str | None = None,
    show_drawdown: bool = True,
    show_trades: bool = True,
    height: int = 400,
) -> str:
    """Generate embedded HTML for equity curve chart (for use in reports).

    Creates an HTML div containing a Plotly chart that can be embedded directly
    in an HTML report. Unlike plot_equity_curve_interactive(), this returns just
    the chart HTML without full page structure.

    Args:
        result: BacktestResult containing equity curve data
        title: Optional custom title. If None, uses "Equity Curve".
        show_drawdown: If True, highlight drawdown periods (default True)
        show_trades: If True, mark trade entry/exit points (default True)
        height: Chart height in pixels (default 400)

    Returns:
        HTML string containing the embedded Plotly chart, or empty string on error

    Example:
        chart_html = generate_equity_chart_html(result)
        # Use in Jinja2 template: {{ chart_html | safe }}
    """
    go = _load_equity_chart_plotly()
    if go is None:
        return ""

    if not result.equity_curve:
        logger.warning("No equity curve data - cannot generate chart")
        return ""

    try:
        fig = _build_embedded_equity_chart_figure(go, result, title, show_drawdown, show_trades, height)
        return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="equity-chart")

    except Exception as e:
        logger.exception("Failed to generate embedded equity chart: %s", e)
        return ""


def _load_pnl_distribution_plotly() -> Any | None:
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.warning(
            "plotly not installed (pip install 'almanak[backtest]') - cannot generate PnL distribution chart"
        )
        return None
    return go


def _trade_pnl_value(trade: Any) -> Any | None:
    pnl_usd = getattr(trade, "pnl_usd", None)
    if pnl_usd is None:
        return None
    return float(pnl_usd)


def _extract_pnl_distribution_values(trades: Any) -> list[float]:
    pnl_values: list[float] = []
    for trade in trades:
        pnl_usd = _trade_pnl_value(trade)
        if pnl_usd is not None:
            pnl_values.append(pnl_usd)
    return pnl_values


def _split_pnl_distribution_values(pnl_values: list[float]) -> tuple[list[float], list[float]]:
    profits = [value for value in pnl_values if value >= 0]
    losses = [value for value in pnl_values if value < 0]
    return profits, losses


def _pnl_distribution_xbins(pnl_values: list[float], bins: int) -> dict[str, float]:
    all_min = min(pnl_values)
    all_max = max(pnl_values)
    bin_size = (all_max - all_min) / bins if all_max != all_min else 1
    return {"start": all_min, "end": all_max, "size": bin_size}


def _add_pnl_distribution_histogram_trace(
    fig: Any,
    go: Any,
    values: list[float],
    *,
    name: str,
    marker_color: str,
    xbins: dict[str, float],
) -> None:
    fig.add_trace(
        go.Histogram(
            x=values,
            name=name,
            marker_color=marker_color,
            xbins=xbins,
            hovertemplate="PnL: $%{x:,.2f}<br>Count: %{y}<extra></extra>",
        )
    )


def _add_pnl_distribution_histograms(fig: Any, go: Any, pnl_values: list[float], bins: int) -> None:
    profits, losses = _split_pnl_distribution_values(pnl_values)
    xbins = _pnl_distribution_xbins(pnl_values, bins)

    if losses:
        _add_pnl_distribution_histogram_trace(
            fig,
            go,
            losses,
            name="Losses",
            marker_color="rgba(244, 67, 54, 0.7)",
            xbins=xbins,
        )
    if profits:
        _add_pnl_distribution_histogram_trace(
            fig,
            go,
            profits,
            name="Profits",
            marker_color="rgba(76, 175, 80, 0.7)",
            xbins=xbins,
        )


def _add_pnl_distribution_reference_lines(fig: Any, stats: DistributionStats | None) -> None:
    fig.add_vline(x=0, line_dash="dash", line_color="#757575", line_width=2)
    if stats:
        fig.add_vline(x=stats.mean, line_dash="dashdot", line_color="#1976D2", line_width=1.5)


def _apply_pnl_distribution_layout(fig: Any, title: str | None, height: int) -> None:
    chart_title = title or "PnL Distribution"
    fig.update_layout(
        title={"text": chart_title, "x": 0.5, "xanchor": "center", "font": {"size": 16}},
        xaxis_title="PnL (USD)",
        yaxis_title="Number of Trades",
        xaxis_tickprefix="$",
        xaxis_tickformat=",.0f",
        barmode="overlay",
        hovermode="x unified",
        legend={"yanchor": "top", "y": 0.99, "xanchor": "right", "x": 0.99},
        template="plotly_white",
        height=height,
        margin={"l": 60, "r": 30, "t": 50, "b": 50},
    )


def _pnl_distribution_stats_text(stats: DistributionStats) -> str:
    return (
        f"<b>Statistics</b><br>"
        f"Mean: ${stats.mean:,.2f}<br>"
        f"Median: ${stats.median:,.2f}<br>"
        f"Std Dev: ${stats.std_dev:,.2f}<br>"
        f"Skew: {stats.skewness:.2f} | Kurt: {stats.kurtosis:.2f}"
    )


def _add_pnl_distribution_stats_annotation(fig: Any, stats: DistributionStats | None) -> None:
    if not stats:
        return

    fig.add_annotation(
        text=_pnl_distribution_stats_text(stats),
        xref="paper",
        yref="paper",
        x=0.02,
        y=0.98,
        showarrow=False,
        font={"size": 10, "family": "monospace"},
        align="left",
        bgcolor="rgba(255, 255, 255, 0.9)",
        bordercolor="#ccc",
        borderwidth=1,
        borderpad=6,
    )


def _build_pnl_distribution_figure(
    go: Any,
    trades: Any,
    title: str | None,
    bins: int,
    height: int,
) -> Any | None:
    pnl_values = _extract_pnl_distribution_values(trades)
    if not pnl_values:
        return None

    stats = calculate_distribution_stats(pnl_values)
    fig = go.Figure()
    _add_pnl_distribution_histograms(fig, go, pnl_values, bins)
    _add_pnl_distribution_reference_lines(fig, stats)
    _apply_pnl_distribution_layout(fig, title, height)
    _add_pnl_distribution_stats_annotation(fig, stats)
    return fig


def generate_pnl_distribution_html(
    result: "BacktestResult",
    title: str | None = None,
    bins: int = 20,
    height: int = 350,
) -> str:
    """Generate embedded HTML for PnL distribution histogram (for use in reports).

    Creates an HTML div containing a Plotly histogram that can be embedded directly
    in an HTML report. Shows the distribution of trade PnL values with statistics.

    Args:
        result: BacktestResult containing trades data
        title: Optional custom title. If None, uses "PnL Distribution".
        bins: Number of histogram bins (default 20)
        height: Chart height in pixels (default 350)

    Returns:
        HTML string containing the embedded Plotly chart, or empty string on error

    Example:
        chart_html = generate_pnl_distribution_html(result)
        # Use in Jinja2 template: {{ chart_html | safe }}
    """
    go = _load_pnl_distribution_plotly()
    if go is None:
        return ""

    trades = getattr(result, "trades", None)
    if not trades:
        logger.warning("No trades data - cannot generate PnL distribution chart")
        return ""

    try:
        fig = _build_pnl_distribution_figure(go, trades, title, bins, height)
        if fig is None:
            return ""

        return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="pnl-distribution-chart")

    except Exception as e:
        logger.exception("Failed to generate embedded PnL distribution chart: %s", e)
        return ""


def _drawdown_chart_series(equity_curve: list["EquityPoint"]) -> tuple[list[datetime], list[float]]:
    timestamps: list[datetime] = []
    drawdowns: list[float] = []
    peak_value = 0.0

    for point in equity_curve:
        value = _as_float(point.value_usd)
        peak_value = max(peak_value, value)
        drawdown_pct = ((peak_value - value) / peak_value * 100) if peak_value > 0 else 0.0
        timestamps.append(point.timestamp)
        drawdowns.append(-drawdown_pct)

    return timestamps, drawdowns


def _build_drawdown_chart_figure(
    go: Any,
    timestamps: list[datetime],
    drawdowns: list[float],
    *,
    title: str | None,
    height: int,
) -> Any:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=timestamps,
            y=drawdowns,
            name="Drawdown",
            fill="tozeroy",
            fillcolor="rgba(244, 67, 54, 0.3)",
            line={"color": "#F44336", "width": 1},
            hovertemplate="<b>%{x}</b><br>Drawdown: %{y:.2f}%<extra></extra>",
        )
    )
    max_dd = min(drawdowns) if drawdowns else 0
    fig.update_layout(
        title={"text": title or "Drawdown", "x": 0.5, "xanchor": "center", "font": {"size": 16}},
        xaxis_title="Time",
        yaxis_title="Drawdown (%)",
        yaxis_ticksuffix="%",
        yaxis_range=[max_dd * 1.1 if max_dd < 0 else -1, 0.5],
        hovermode="x unified",
        template="plotly_white",
        height=height,
        margin={"l": 60, "r": 30, "t": 50, "b": 50},
        showlegend=False,
    )
    return fig


# crap-allowlist: #2703 mechanical extras-message string change in existing high-CRAP function (pre-existing cov ~4%)
def generate_drawdown_chart_html(
    result: "BacktestResult",
    title: str | None = None,
    height: int = 300,
) -> str:
    """Generate embedded HTML for drawdown chart (for use in reports).

    Creates an HTML div containing a Plotly chart showing drawdown over time.
    The chart displays drawdown as a percentage from peak equity.

    Args:
        result: BacktestResult containing equity curve data
        title: Optional custom title. If None, uses "Drawdown".
        height: Chart height in pixels (default 300)

    Returns:
        HTML string containing the embedded Plotly chart, or empty string on error

    Example:
        chart_html = generate_drawdown_chart_html(result)
        # Use in Jinja2 template: {{ chart_html | safe }}
    """
    go = _load_plotly_graph_objects("drawdown chart")
    if go is None:
        return ""

    if not result.equity_curve:
        logger.warning("No equity curve data - cannot generate drawdown chart")
        return ""

    try:
        timestamps, drawdowns = _drawdown_chart_series(result.equity_curve)
        fig = _build_drawdown_chart_figure(go, timestamps, drawdowns, title=title, height=height)
        return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="drawdown-chart")

    except Exception as e:
        logger.exception("Failed to generate embedded drawdown chart: %s", e)
        return ""


def save_chart(
    result: "BacktestResult",
    format: str = "png",  # noqa: A002
    path: Path | str | None = None,
    title: str | None = None,
    benchmark_curve: list["EquityPoint"] | None = None,
    benchmark_label: str = "Benchmark",
    show_drawdown: bool = False,
    min_drawdown_pct: float = 0.01,
    show_trades: bool = False,
    color_by_pnl: bool = True,
    config: ChartConfig | None = None,
) -> ChartResult:
    """Save an equity curve chart in the specified format.

    This is a convenience function that dispatches to the appropriate
    chart generation function based on the requested format.

    Args:
        result: BacktestResult containing equity curve data
        format: Output format - "png" for static image, "html" for interactive (default "png")
        path: Path to save the chart file. If None, auto-generates based on format.
        title: Optional custom title
        benchmark_curve: Optional benchmark equity curve for comparison
        benchmark_label: Label for the benchmark in the legend
        show_drawdown: If True, highlight drawdown periods
        min_drawdown_pct: Minimum drawdown percentage to highlight
        show_trades: If True, mark trade entry/exit points
        color_by_pnl: If True, color trade markers by profit/loss
        config: Chart styling configuration (only used for PNG format)

    Returns:
        ChartResult with file path and success status

    Example:
        # Save as PNG
        save_chart(result, format="png", path="charts/equity.png")

        # Save as interactive HTML
        save_chart(result, format="html", path="charts/equity.html", show_trades=True)
    """
    format_lower = format.lower()

    if format_lower == "html":
        return plot_equity_curve_interactive(
            result=result,
            output_path=path,
            title=title,
            benchmark_curve=benchmark_curve,
            benchmark_label=benchmark_label,
            show_drawdown=show_drawdown,
            min_drawdown_pct=min_drawdown_pct,
            show_trades=show_trades,
            color_by_pnl=color_by_pnl,
        )
    elif format_lower == "png":
        return plot_equity_curve(
            result=result,
            output_path=path,
            config=config,
            title=title,
            benchmark_curve=benchmark_curve,
            benchmark_label=benchmark_label,
            show_drawdown=show_drawdown,
            min_drawdown_pct=min_drawdown_pct,
            show_trades=show_trades,
            color_by_pnl=color_by_pnl,
        )
    else:
        return ChartResult(
            chart_type="equity_curve",
            file_path=None,
            success=False,
            error=f"Unsupported format: {format}. Use 'png' or 'html'.",
            format=format_lower,
        )


def generate_attribution_pie_chart_html(
    attribution_data: dict[str, Decimal],
    title: str = "PnL Attribution",
    height: int = 350,
) -> str:
    """Generate embedded HTML for a pie chart showing PnL attribution.

    Creates an HTML div containing a Plotly pie chart that can be embedded directly
    in an HTML report. Shows the breakdown of PnL by category (protocol, intent type, or asset).

    Args:
        attribution_data: Dictionary mapping category names to PnL values (Decimal)
        title: Chart title (default "PnL Attribution")
        height: Chart height in pixels (default 350)

    Returns:
        HTML string containing the embedded Plotly chart, or empty string on error

    Example:
        chart_html = generate_attribution_pie_chart_html(
            {"uniswap_v3": Decimal("100"), "aave_v3": Decimal("-50")},
            title="PnL by Protocol"
        )
        # Use in Jinja2 template: {{ chart_html | safe }}
    """
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.warning("plotly not installed (pip install 'almanak[backtest]') - cannot generate attribution pie chart")
        return ""

    if not attribution_data:
        logger.warning("No attribution data - cannot generate pie chart")
        return ""

    try:
        # Prepare data - separate positive and negative values for better visualization
        labels = list(attribution_data.keys())
        values = [float(v) for v in attribution_data.values()]

        # Handle negative values by taking absolute values and marking them
        abs_values = [abs(v) for v in values]
        colors = ["#00d26a" if v >= 0 else "#ff4757" for v in values]

        # Create pie chart
        fig = go.Figure()

        fig.add_trace(
            go.Pie(
                labels=labels,
                values=abs_values,
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>PnL: $%{value:,.2f}<br>%{percent}<extra></extra>",
                marker={"colors": colors},
                textposition="inside",
                insidetextorientation="radial",
                customdata=values,  # Store actual values for hover
            )
        )

        # Configure layout
        fig.update_layout(
            title={"text": title, "x": 0.5, "xanchor": "center", "font": {"size": 16}},
            height=height,
            margin={"l": 20, "r": 20, "t": 50, "b": 20},
            showlegend=True,
            legend={"orientation": "h", "yanchor": "bottom", "y": -0.2, "xanchor": "center", "x": 0.5},
        )

        return fig.to_html(
            include_plotlyjs="cdn", full_html=False, div_id=f"attribution-pie-{title.lower().replace(' ', '-')}"
        )

    except Exception as e:
        logger.exception("Failed to generate attribution pie chart: %s", e)
        return ""


def generate_attribution_bar_chart_html(
    attribution_data: dict[str, Decimal],
    title: str = "PnL Attribution",
    height: int = 350,
) -> str:
    """Generate embedded HTML for a bar chart showing PnL attribution.

    Creates an HTML div containing a Plotly bar chart that can be embedded directly
    in an HTML report. Shows the breakdown of PnL by category with positive/negative
    bars colored differently.

    Args:
        attribution_data: Dictionary mapping category names to PnL values (Decimal)
        title: Chart title (default "PnL Attribution")
        height: Chart height in pixels (default 350)

    Returns:
        HTML string containing the embedded Plotly chart, or empty string on error

    Example:
        chart_html = generate_attribution_bar_chart_html(
            {"uniswap_v3": Decimal("100"), "aave_v3": Decimal("-50")},
            title="PnL by Protocol"
        )
        # Use in Jinja2 template: {{ chart_html | safe }}
    """
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.warning("plotly not installed (pip install 'almanak[backtest]') - cannot generate attribution bar chart")
        return ""

    if not attribution_data:
        logger.warning("No attribution data - cannot generate bar chart")
        return ""

    try:
        # Sort by absolute value (largest first)
        sorted_items = sorted(attribution_data.items(), key=lambda x: abs(float(x[1])), reverse=True)
        labels = [item[0] for item in sorted_items]
        values = [float(item[1]) for item in sorted_items]

        # Color bars based on positive/negative
        colors = ["#00d26a" if v >= 0 else "#ff4757" for v in values]

        # Create bar chart
        fig = go.Figure()

        fig.add_trace(
            go.Bar(
                x=labels,
                y=values,
                marker_color=colors,
                hovertemplate="<b>%{x}</b><br>PnL: $%{y:,.2f}<extra></extra>",
                text=[f"${v:,.2f}" for v in values],
                textposition="outside",
                textfont={"size": 10},
            )
        )

        # Add zero line
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

        # Configure layout
        fig.update_layout(
            title={"text": title, "x": 0.5, "xanchor": "center", "font": {"size": 16}},
            xaxis_title="Category",
            yaxis_title="PnL (USD)",
            yaxis_tickprefix="$",
            height=height,
            margin={"l": 60, "r": 30, "t": 50, "b": 80},
            showlegend=False,
            template="plotly_white",
            xaxis_tickangle=-45,
        )

        return fig.to_html(
            include_plotlyjs="cdn", full_html=False, div_id=f"attribution-bar-{title.lower().replace(' ', '-')}"
        )

    except Exception as e:
        logger.exception("Failed to generate attribution bar chart: %s", e)
        return ""


def _display_labeled_attribution(
    attribution_data: dict[str, Decimal],
    display_labels: dict[str, str],
) -> dict[str, Decimal]:
    """Project attribution keys to display labels without merging collisions."""
    if not display_labels:
        return attribution_data

    from almanak.framework.backtesting.pnl.calculators import collision_safe_display_labels

    label_by_key = collision_safe_display_labels(attribution_data.keys(), display_labels)
    return {label_by_key[key]: value for key, value in attribution_data.items()}


def generate_attribution_charts_html(
    result: "BacktestResult",
    chart_type: str = "bar",
    height: int = 350,
) -> dict[str, str]:
    """Generate all attribution charts for a backtest result.

    Creates HTML charts for PnL attribution by protocol, intent type, and asset.
    Returns a dictionary with chart HTML that can be used in report templates.

    Args:
        result: BacktestResult containing attribution metrics
        chart_type: Type of chart to generate - "bar" or "pie" (default "bar")
        height: Chart height in pixels (default 350)

    Returns:
        Dictionary with keys:
            - "by_protocol": Chart showing PnL by protocol
            - "by_intent_type": Chart showing PnL by intent type
            - "by_asset": Chart showing PnL by asset

    Example:
        charts = generate_attribution_charts_html(result)
        # Use in template: {{ charts.by_protocol | safe }}
    """
    generator = generate_attribution_bar_chart_html if chart_type == "bar" else generate_attribution_pie_chart_html

    charts = {
        "by_protocol": "",
        "by_intent_type": "",
        "by_asset": "",
    }

    if hasattr(result, "metrics") and result.metrics:
        metrics = result.metrics

        if hasattr(metrics, "pnl_by_protocol") and metrics.pnl_by_protocol:
            charts["by_protocol"] = generator(
                metrics.pnl_by_protocol,
                title="PnL by Protocol",
                height=height,
            )

        if hasattr(metrics, "pnl_by_intent_type") and metrics.pnl_by_intent_type:
            charts["by_intent_type"] = generator(
                metrics.pnl_by_intent_type,
                title="PnL by Intent Type",
                height=height,
            )

        if hasattr(metrics, "pnl_by_asset") and metrics.pnl_by_asset:
            charts["by_asset"] = generator(
                _display_labeled_attribution(
                    metrics.pnl_by_asset,
                    getattr(metrics, "pnl_by_asset_display_labels", {}),
                ),
                title="PnL by Asset",
                height=height,
            )

    return charts
