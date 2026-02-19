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
from typing import TYPE_CHECKING

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

    # Build a timestamp -> value lookup from equity curve
    equity_lookup: dict[datetime, Decimal] = {point.timestamp: point.value_usd for point in equity_curve}
    sorted_timestamps = sorted(equity_lookup.keys())

    markers: list[TradeMarker] = []

    # Entry intent types (opening positions)
    entry_types = {"SWAP", "LP_OPEN", "PERP_OPEN", "BORROW", "SUPPLY", "BRIDGE"}
    # Exit intent types (closing positions)
    exit_types = {"LP_CLOSE", "PERP_CLOSE", "REPAY", "WITHDRAW"}

    for trade in trades:
        # Determine if this is an entry or exit trade
        trade_type_str = trade.intent_type.value if hasattr(trade.intent_type, "value") else str(trade.intent_type)
        is_entry = trade_type_str in entry_types
        is_exit = trade_type_str in exit_types

        # For SWAP, check metadata or context if available to determine direction
        # By default, we mark SWAPs as entry trades
        if trade_type_str == "HOLD" or trade_type_str == "UNKNOWN":
            # Skip HOLD and UNKNOWN intents - they don't represent actual trades
            continue

        # Find the closest equity curve value for this trade timestamp
        trade_time = trade.timestamp
        equity_value = equity_lookup.get(trade_time)

        if equity_value is None:
            # Find the closest timestamp
            closest_time = min(
                sorted_timestamps,
                key=lambda t: abs((t - trade_time).total_seconds()),
            )
            equity_value = equity_lookup[closest_time]

        markers.append(
            TradeMarker(
                timestamp=trade_time,
                value_usd=equity_value,
                is_entry=is_entry and not is_exit,
                trade_type=trade_type_str,
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


def plot_equity_curve(
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
            current directory as 'equity_curve_{strategy_id}.png'
        config: Optional chart styling configuration. Uses defaults if None.
        title: Optional custom title. If None, auto-generates from strategy_id.
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
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        logger.error("matplotlib not installed. Run: uv add matplotlib")
        return ChartResult(
            chart_type="equity_curve",
            file_path=None,
            success=False,
            error="matplotlib not installed. Run: uv add matplotlib",
        )

    # Validate input
    if not result.equity_curve:
        return ChartResult(
            chart_type="equity_curve",
            file_path=None,
            success=False,
            error="No equity curve data in backtest result",
        )

    # Use defaults
    cfg = config or ChartConfig()

    # Determine output path
    if output_path is None:
        safe_id = result.strategy_id.replace("/", "_").replace("\\", "_")
        output_path = Path(f"equity_curve_{safe_id}.png")
    elif isinstance(output_path, str):
        output_path = Path(output_path)

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Extract data from equity curve
        timestamps: list[datetime] = []
        values: list[float] = []

        for point in result.equity_curve:
            timestamps.append(point.timestamp)
            # Convert Decimal to float for matplotlib
            if isinstance(point.value_usd, Decimal):
                values.append(float(point.value_usd))
            else:
                values.append(point.value_usd)

        # Detect drawdown periods if requested
        drawdown_periods: list[DrawdownPeriod] = []
        if show_drawdown:
            drawdown_periods = _detect_drawdown_periods(timestamps, values, min_drawdown_pct)

        # Create figure
        fig, ax = plt.subplots(figsize=(cfg.figure_width, cfg.figure_height))

        # Plot drawdown periods first (so they appear behind the lines)
        if show_drawdown and drawdown_periods:
            for period in drawdown_periods:
                ax.axvspan(
                    period.start,  # type: ignore[arg-type]
                    period.end,  # type: ignore[arg-type]
                    color=cfg.drawdown_color,
                    alpha=cfg.drawdown_alpha,
                    label=None,  # Don't add each period to legend
                )
            # Add one dummy patch for the legend
            ax.axvspan(
                timestamps[0],  # type: ignore[arg-type]
                timestamps[0],  # type: ignore[arg-type]
                color=cfg.drawdown_color,
                alpha=cfg.drawdown_alpha,
                label="Drawdown Period",
            )

        # Plot benchmark curve if provided
        if benchmark_curve:
            benchmark_timestamps: list[datetime] = []
            benchmark_values: list[float] = []
            for point in benchmark_curve:
                benchmark_timestamps.append(point.timestamp)
                if isinstance(point.value_usd, Decimal):
                    benchmark_values.append(float(point.value_usd))
                else:
                    benchmark_values.append(point.value_usd)

            ax.plot(
                benchmark_timestamps,  # type: ignore[arg-type]
                benchmark_values,
                linewidth=cfg.line_width,
                color=cfg.benchmark_color,
                linestyle=cfg.benchmark_line_style,
                label=benchmark_label,
            )

        # Plot strategy equity curve (on top of benchmark)
        ax.plot(
            timestamps,  # type: ignore[arg-type]
            values,
            linewidth=cfg.line_width,
            color=cfg.line_color,
            label="Strategy",
        )

        # Fill under the strategy curve
        ax.fill_between(timestamps, values, alpha=cfg.fill_alpha, color=cfg.line_color)  # type: ignore[arg-type]

        # Extract and plot trade markers if requested
        trade_markers: list[TradeMarker] = []
        if show_trades and hasattr(result, "trades") and result.trades:
            trade_markers = _extract_trade_markers(result.trades, result.equity_curve)

            if trade_markers:
                # Separate markers into groups for plotting
                entry_times: list[datetime] = []
                entry_values: list[float] = []
                entry_colors: list[str] = []

                exit_times: list[datetime] = []
                exit_values: list[float] = []
                exit_colors: list[str] = []

                for marker in trade_markers:
                    marker_value = float(marker.value_usd)

                    if color_by_pnl:
                        # Color by profit/loss
                        if marker.pnl_usd is not None:
                            color = cfg.profit_color if marker.pnl_usd >= 0 else cfg.loss_color
                        else:
                            color = cfg.entry_color if marker.is_entry else cfg.exit_color
                    else:
                        # Color by entry/exit
                        color = cfg.entry_color if marker.is_entry else cfg.exit_color

                    if marker.is_entry:
                        entry_times.append(marker.timestamp)
                        entry_values.append(marker_value)
                        entry_colors.append(color)
                    else:
                        exit_times.append(marker.timestamp)
                        exit_values.append(marker_value)
                        exit_colors.append(color)

                # Plot entry markers (triangles pointing up)
                if entry_times:
                    ax.scatter(
                        entry_times,  # type: ignore[arg-type]
                        entry_values,
                        c=entry_colors,
                        marker=cfg.entry_marker,
                        s=cfg.marker_size,
                        zorder=5,
                        label="Entry",
                        edgecolors="white",
                        linewidths=0.5,
                    )

                # Plot exit markers (triangles pointing down)
                if exit_times:
                    ax.scatter(
                        exit_times,  # type: ignore[arg-type]
                        exit_values,
                        c=exit_colors,
                        marker=cfg.exit_marker,
                        s=cfg.marker_size,
                        zorder=5,
                        label="Exit",
                        edgecolors="white",
                        linewidths=0.5,
                    )

        # Set title
        chart_title = title or f"Equity Curve - {result.strategy_id}"
        ax.set_title(chart_title, fontsize=cfg.title_size, fontweight="bold")

        # Set labels
        ax.set_xlabel("Time", fontsize=cfg.font_size)
        ax.set_ylabel("Portfolio Value (USD)", fontsize=cfg.font_size)

        # Apply styling
        ax.tick_params(labelsize=cfg.font_size)
        ax.grid(True, alpha=cfg.grid_alpha)
        ax.legend(loc="upper left")

        # Format y-axis with dollar values
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))

        # Rotate x-axis labels for readability
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        # Save plot
        fig.savefig(output_path, dpi=cfg.dpi, bbox_inches="tight")
        plt.close(fig)

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
        return ChartResult(
            chart_type="equity_curve",
            file_path=None,
            success=False,
            error=str(e),
        )


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
            current directory as 'equity_curve_{strategy_id}.html'
        title: Optional custom title. If None, auto-generates from strategy_id.
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
        logger.error("plotly not installed. Run: uv add plotly")
        return ChartResult(
            chart_type="equity_curve",
            file_path=None,
            success=False,
            error="plotly not installed. Run: uv add plotly",
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

    # Determine output path
    if output_path is None:
        safe_id = result.strategy_id.replace("/", "_").replace("\\", "_")
        output_path = Path(f"equity_curve_{safe_id}.html")
    elif isinstance(output_path, str):
        output_path = Path(output_path)

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Extract data from equity curve
        timestamps: list[datetime] = []
        values: list[float] = []

        for point in result.equity_curve:
            timestamps.append(point.timestamp)
            if isinstance(point.value_usd, Decimal):
                values.append(float(point.value_usd))
            else:
                values.append(point.value_usd)

        # Detect drawdown periods
        drawdown_periods: list[DrawdownPeriod] = []
        if show_drawdown:
            drawdown_periods = _detect_drawdown_periods(timestamps, values, min_drawdown_pct)

        # Create figure
        fig = go.Figure()

        # Add drawdown shading first (behind other elements)
        if show_drawdown and drawdown_periods:
            for period in drawdown_periods:
                fig.add_vrect(
                    x0=period.start,
                    x1=period.end,
                    fillcolor="rgba(244, 67, 54, 0.2)",
                    layer="below",
                    line_width=0,
                )

        # Add benchmark curve if provided
        if benchmark_curve:
            benchmark_timestamps: list[datetime] = []
            benchmark_values: list[float] = []
            for point in benchmark_curve:
                benchmark_timestamps.append(point.timestamp)
                if isinstance(point.value_usd, Decimal):
                    benchmark_values.append(float(point.value_usd))
                else:
                    benchmark_values.append(point.value_usd)

            fig.add_trace(
                go.Scatter(
                    x=benchmark_timestamps,
                    y=benchmark_values,
                    name=benchmark_label,
                    line={"color": "#757575", "dash": "dash"},
                    hovertemplate="<b>%{x}</b><br>" + benchmark_label + ": $%{y:,.2f}<extra></extra>",
                )
            )

        # Add main strategy equity curve
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

        # Add trade markers
        trade_markers: list[TradeMarker] = []
        if show_trades and hasattr(result, "trades") and result.trades:
            trade_markers = _extract_trade_markers(result.trades, result.equity_curve)

            if trade_markers:
                # Group markers by type for different symbols
                entry_markers = [m for m in trade_markers if m.is_entry]
                exit_markers = [m for m in trade_markers if not m.is_entry]

                def get_color(marker: TradeMarker) -> str:
                    if color_by_pnl and marker.pnl_usd is not None:
                        return "#4CAF50" if marker.pnl_usd >= 0 else "#F44336"
                    return "#4CAF50" if marker.is_entry else "#F44336"

                # Entry markers
                if entry_markers:
                    fig.add_trace(
                        go.Scatter(
                            x=[m.timestamp for m in entry_markers],
                            y=[float(m.value_usd) for m in entry_markers],
                            mode="markers",
                            name="Entry",
                            marker={
                                "symbol": "triangle-up",
                                "size": 12,
                                "color": [get_color(m) for m in entry_markers],
                                "line": {"color": "white", "width": 1},
                            },
                            hovertemplate=("<b>%{x}</b><br>Entry<br>Value: $%{y:,.2f}<br><extra></extra>"),
                            customdata=[
                                {
                                    "type": m.trade_type,
                                    "pnl": float(m.pnl_usd) if m.pnl_usd else None,
                                }
                                for m in entry_markers
                            ],
                        )
                    )

                # Exit markers
                if exit_markers:
                    fig.add_trace(
                        go.Scatter(
                            x=[m.timestamp for m in exit_markers],
                            y=[float(m.value_usd) for m in exit_markers],
                            mode="markers",
                            name="Exit",
                            marker={
                                "symbol": "triangle-down",
                                "size": 12,
                                "color": [get_color(m) for m in exit_markers],
                                "line": {"color": "white", "width": 1},
                            },
                            hovertemplate=("<b>%{x}</b><br>Exit<br>Value: $%{y:,.2f}<br><extra></extra>"),
                            customdata=[
                                {
                                    "type": m.trade_type,
                                    "pnl": float(m.pnl_usd) if m.pnl_usd else None,
                                }
                                for m in exit_markers
                            ],
                        )
                    )

        # Set layout
        chart_title = title or f"Equity Curve - {result.strategy_id}"
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

        # Add range slider and buttons for time navigation
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

        # Save as HTML
        fig.write_html(str(output_path), include_plotlyjs=True, full_html=True)

        logger.info("Created interactive equity curve plot: %s", output_path)
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
            current directory as 'pnl_histogram_{strategy_id}.png'
        config: Optional chart styling configuration. Uses defaults if None.
        title: Optional custom title. If None, auto-generates from strategy_id.
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
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        logger.error("matplotlib not installed. Run: uv add matplotlib")
        return ChartResult(
            chart_type="pnl_histogram",
            file_path=None,
            success=False,
            error="matplotlib not installed. Run: uv add matplotlib",
        )

    # Validate input
    if not hasattr(result, "trades") or not result.trades:
        return ChartResult(
            chart_type="pnl_histogram",
            file_path=None,
            success=False,
            error="No trades data in backtest result",
        )

    # Use defaults
    cfg = config or ChartConfig()

    # Determine output path
    if output_path is None:
        safe_id = result.strategy_id.replace("/", "_").replace("\\", "_")
        output_path = Path(f"pnl_histogram_{safe_id}.png")
    elif isinstance(output_path, str):
        output_path = Path(output_path)

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Extract PnL values from trades
        pnl_values: list[float] = []
        for trade in result.trades:
            if hasattr(trade, "pnl_usd") and trade.pnl_usd is not None:
                if isinstance(trade.pnl_usd, Decimal):
                    pnl_values.append(float(trade.pnl_usd))
                else:
                    pnl_values.append(trade.pnl_usd)

        if not pnl_values:
            return ChartResult(
                chart_type="pnl_histogram",
                file_path=None,
                success=False,
                error="No PnL data in trades",
            )

        # Create figure
        fig, ax = plt.subplots(figsize=(cfg.figure_width, cfg.figure_height))

        # Color bars by profit/loss
        n, bin_edges, patches = ax.hist(pnl_values, bins=bins, edgecolor="white", linewidth=0.5)

        # Color each bar based on whether it represents profit or loss
        for i, patch in enumerate(patches):  # type: ignore[arg-type]
            # Use the midpoint of the bin to determine color
            bin_mid = (bin_edges[i] + bin_edges[i + 1]) / 2
            if bin_mid >= 0:
                patch.set_facecolor(cfg.profit_color)
            else:
                patch.set_facecolor(cfg.loss_color)

        # Add vertical line at zero
        ax.axvline(x=0, color="#757575", linestyle="--", linewidth=1.5, label="Break-even")

        # Calculate and display distribution statistics if requested
        stats = None
        if show_stats:
            stats = calculate_distribution_stats(pnl_values)
            if stats:
                # Create statistics text box
                stats_text = (
                    f"Mean: ${stats.mean:,.2f}\n"
                    f"Median: ${stats.median:,.2f}\n"
                    f"Std Dev: ${stats.std_dev:,.2f}\n"
                    f"Skewness: {stats.skewness:.3f}\n"
                    f"Kurtosis: {stats.kurtosis:.3f}\n"
                    f"5th %ile: ${stats.percentile_5:,.2f}\n"
                    f"95th %ile: ${stats.percentile_95:,.2f}"
                )

                # Add text box with stats
                props = {"boxstyle": "round", "facecolor": "wheat", "alpha": 0.8}
                ax.text(
                    0.02,
                    0.98,
                    stats_text,
                    transform=ax.transAxes,
                    fontsize=cfg.font_size - 1,
                    verticalalignment="top",
                    bbox=props,
                    family="monospace",
                )

                # Add vertical lines for mean and median
                ax.axvline(
                    x=stats.mean,
                    color="#1976D2",
                    linestyle="-.",
                    linewidth=1.2,
                    label=f"Mean (${stats.mean:,.0f})",
                )
                ax.axvline(
                    x=stats.median,
                    color="#7B1FA2",
                    linestyle=":",
                    linewidth=1.2,
                    label=f"Median (${stats.median:,.0f})",
                )

        # Set title
        chart_title = title or f"Trade PnL Distribution - {result.strategy_id}"
        ax.set_title(chart_title, fontsize=cfg.title_size, fontweight="bold")

        # Set labels
        ax.set_xlabel("PnL (USD)", fontsize=cfg.font_size)
        ax.set_ylabel("Number of Trades", fontsize=cfg.font_size)

        # Apply styling
        ax.tick_params(labelsize=cfg.font_size)
        ax.grid(True, alpha=cfg.grid_alpha, axis="y")
        ax.legend(loc="upper right")

        # Format x-axis with dollar values
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))

        plt.tight_layout()

        # Save plot
        fig.savefig(output_path, dpi=cfg.dpi, bbox_inches="tight")
        plt.close(fig)

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
        return ChartResult(
            chart_type="pnl_histogram",
            file_path=None,
            success=False,
            error=str(e),
        )


def plot_pnl_histogram_interactive(
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
            current directory as 'pnl_histogram_{strategy_id}.html'
        title: Optional custom title. If None, auto-generates from strategy_id.
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
        logger.error("plotly not installed. Run: uv add plotly")
        return ChartResult(
            chart_type="pnl_histogram",
            file_path=None,
            success=False,
            error="plotly not installed. Run: uv add plotly",
            format="html",
        )

    # Validate input
    if not hasattr(result, "trades") or not result.trades:
        return ChartResult(
            chart_type="pnl_histogram",
            file_path=None,
            success=False,
            error="No trades data in backtest result",
            format="html",
        )

    # Determine output path
    if output_path is None:
        safe_id = result.strategy_id.replace("/", "_").replace("\\", "_")
        output_path = Path(f"pnl_histogram_{safe_id}.html")
    elif isinstance(output_path, str):
        output_path = Path(output_path)

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Extract PnL values from trades
        pnl_values: list[float] = []
        for trade in result.trades:
            if hasattr(trade, "pnl_usd") and trade.pnl_usd is not None:
                if isinstance(trade.pnl_usd, Decimal):
                    pnl_values.append(float(trade.pnl_usd))
                else:
                    pnl_values.append(trade.pnl_usd)

        if not pnl_values:
            return ChartResult(
                chart_type="pnl_histogram",
                file_path=None,
                success=False,
                error="No PnL data in trades",
                format="html",
            )

        # Calculate distribution statistics
        stats = calculate_distribution_stats(pnl_values)

        # Create figure
        fig = go.Figure()

        # Separate profits and losses for coloring
        profits = [x for x in pnl_values if x >= 0]
        losses = [x for x in pnl_values if x < 0]

        # Calculate bin edges for consistent binning
        all_min = min(pnl_values)
        all_max = max(pnl_values)
        bin_size = (all_max - all_min) / bins if all_max != all_min else 1

        # Add histogram for losses (red)
        if losses:
            fig.add_trace(
                go.Histogram(
                    x=losses,
                    name="Losses",
                    marker_color="rgba(244, 67, 54, 0.7)",
                    xbins={"start": all_min, "end": all_max, "size": bin_size},
                    hovertemplate="PnL: $%{x:,.2f}<br>Count: %{y}<extra></extra>",
                )
            )

        # Add histogram for profits (green)
        if profits:
            fig.add_trace(
                go.Histogram(
                    x=profits,
                    name="Profits",
                    marker_color="rgba(76, 175, 80, 0.7)",
                    xbins={"start": all_min, "end": all_max, "size": bin_size},
                    hovertemplate="PnL: $%{x:,.2f}<br>Count: %{y}<extra></extra>",
                )
            )

        # Add vertical line at zero
        fig.add_vline(
            x=0,
            line_dash="dash",
            line_color="#757575",
            line_width=2,
            annotation_text="Break-even",
            annotation_position="top",
        )

        # Add mean and median lines if stats available
        if stats:
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

        # Create annotation text with stats
        stats_annotation = ""
        if show_stats and stats:
            # Interpret skewness
            if stats.skewness > 0.5:
                skew_interp = "Right-skewed (more large gains)"
            elif stats.skewness < -0.5:
                skew_interp = "Left-skewed (more large losses)"
            else:
                skew_interp = "Approximately symmetric"

            # Interpret kurtosis
            if stats.kurtosis > 1:
                kurt_interp = "Fat tails (extreme values likely)"
            elif stats.kurtosis < -1:
                kurt_interp = "Thin tails (extreme values rare)"
            else:
                kurt_interp = "Normal-like tails"

            stats_annotation = (
                f"<b>Distribution Statistics</b><br>"
                f"Mean: ${stats.mean:,.2f}<br>"
                f"Median: ${stats.median:,.2f}<br>"
                f"Std Dev: ${stats.std_dev:,.2f}<br>"
                f"<b>Skewness: {stats.skewness:.3f}</b> ({skew_interp})<br>"
                f"<b>Kurtosis: {stats.kurtosis:.3f}</b> ({kurt_interp})<br>"
                f"Range: ${stats.min_return:,.2f} to ${stats.max_return:,.2f}<br>"
                f"5th-95th %ile: ${stats.percentile_5:,.2f} to ${stats.percentile_95:,.2f}<br>"
                f"Trade Count: {stats.count}"
            )

        # Set layout
        chart_title = title or f"Trade PnL Distribution - {result.strategy_id}"
        fig.update_layout(
            title={
                "text": chart_title,
                "x": 0.5,
                "xanchor": "center",
                "font": {"size": 18, "color": "#333"},
            },
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

        # Add stats annotation box if requested
        if show_stats and stats_annotation:
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

        # Save as HTML
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
        return ChartResult(
            chart_type="pnl_histogram",
            file_path=None,
            success=False,
            error=str(e),
            format="html",
        )


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
            current directory as 'duration_scatter_{strategy_id}.png'
        config: Optional chart styling configuration. Uses defaults if None.
        title: Optional custom title. If None, auto-generates from strategy_id.

    Returns:
        ChartResult with file path and success status

    Example:
        chart = plot_duration_scatter(result, output_path=Path("charts/duration.png"))
        if chart.success:
            print(f"Saved to: {chart.file_path}")
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        logger.error("matplotlib not installed. Run: uv add matplotlib")
        return ChartResult(
            chart_type="duration_scatter",
            file_path=None,
            success=False,
            error="matplotlib not installed. Run: uv add matplotlib",
        )

    # Validate input
    if not hasattr(result, "trades") or not result.trades:
        return ChartResult(
            chart_type="duration_scatter",
            file_path=None,
            success=False,
            error="No trades data in backtest result",
        )

    # Use defaults
    cfg = config or ChartConfig()

    # Determine output path
    if output_path is None:
        safe_id = result.strategy_id.replace("/", "_").replace("\\", "_")
        output_path = Path(f"duration_scatter_{safe_id}.png")
    elif isinstance(output_path, str):
        output_path = Path(output_path)

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Extract duration and PnL from trades
        # Duration is calculated as time between consecutive trades or estimated from metadata
        durations: list[float] = []  # in hours
        pnl_values: list[float] = []
        colors: list[str] = []

        # Sort trades by timestamp
        sorted_trades = sorted(result.trades, key=lambda t: t.timestamp)

        for i, trade in enumerate(sorted_trades):
            if not hasattr(trade, "pnl_usd") or trade.pnl_usd is None:
                continue

            pnl = float(trade.pnl_usd) if isinstance(trade.pnl_usd, Decimal) else trade.pnl_usd

            # Calculate duration from metadata or estimate from trade timing
            duration_hours = None

            # Check for duration in metadata
            if hasattr(trade, "metadata") and trade.metadata:
                if "duration_hours" in trade.metadata:
                    duration_hours = float(trade.metadata["duration_hours"])
                elif "duration_minutes" in trade.metadata:
                    duration_hours = float(trade.metadata["duration_minutes"]) / 60
                elif "hold_time_seconds" in trade.metadata:
                    duration_hours = float(trade.metadata["hold_time_seconds"]) / 3600

            # If no duration in metadata, estimate from trade sequence
            # For position-closing trades, use time since last related trade
            if duration_hours is None and i > 0:
                prev_trade = sorted_trades[i - 1]
                time_diff = (trade.timestamp - prev_trade.timestamp).total_seconds() / 3600
                duration_hours = max(0.1, time_diff)  # Minimum 0.1 hours (6 min)

            if duration_hours is not None:
                durations.append(duration_hours)
                pnl_values.append(pnl)
                colors.append(cfg.profit_color if pnl >= 0 else cfg.loss_color)

        if not durations:
            return ChartResult(
                chart_type="duration_scatter",
                file_path=None,
                success=False,
                error="No duration/PnL data available for scatter plot",
            )

        # Create figure
        fig, ax = plt.subplots(figsize=(cfg.figure_width, cfg.figure_height))

        # Create scatter plot
        ax.scatter(
            durations,
            pnl_values,
            c=colors,
            s=cfg.marker_size,
            alpha=0.7,
            edgecolors="white",
            linewidths=0.5,
        )

        # Add horizontal line at zero
        ax.axhline(y=0, color="#757575", linestyle="--", linewidth=1.5, label="Break-even")

        # Set title
        chart_title = title or f"Trade Duration vs PnL - {result.strategy_id}"
        ax.set_title(chart_title, fontsize=cfg.title_size, fontweight="bold")

        # Set labels
        ax.set_xlabel("Duration (hours)", fontsize=cfg.font_size)
        ax.set_ylabel("PnL (USD)", fontsize=cfg.font_size)

        # Apply styling
        ax.tick_params(labelsize=cfg.font_size)
        ax.grid(True, alpha=cfg.grid_alpha)
        ax.legend(loc="upper right")

        # Format y-axis with dollar values
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))

        plt.tight_layout()

        # Save plot
        fig.savefig(output_path, dpi=cfg.dpi, bbox_inches="tight")
        plt.close(fig)

        logger.info("Created duration scatter plot: %s", output_path)

        return ChartResult(
            chart_type="duration_scatter",
            file_path=output_path,
            success=True,
            format="png",
        )

    except Exception as e:
        logger.exception("Failed to create duration scatter plot")
        return ChartResult(
            chart_type="duration_scatter",
            file_path=None,
            success=False,
            error=str(e),
        )


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
            current directory as 'intent_pie_{strategy_id}.png'
        config: Optional chart styling configuration. Uses defaults if None.
        title: Optional custom title. If None, auto-generates from strategy_id.

    Returns:
        ChartResult with file path and success status

    Example:
        chart = plot_intent_pie(result, output_path=Path("charts/intents.png"))
        if chart.success:
            print(f"Saved to: {chart.file_path}")
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        logger.error("matplotlib not installed. Run: uv add matplotlib")
        return ChartResult(
            chart_type="intent_pie",
            file_path=None,
            success=False,
            error="matplotlib not installed. Run: uv add matplotlib",
        )

    # Validate input
    if not hasattr(result, "trades") or not result.trades:
        return ChartResult(
            chart_type="intent_pie",
            file_path=None,
            success=False,
            error="No trades data in backtest result",
        )

    # Use defaults
    cfg = config or ChartConfig()

    # Determine output path
    if output_path is None:
        safe_id = result.strategy_id.replace("/", "_").replace("\\", "_")
        output_path = Path(f"intent_pie_{safe_id}.png")
    elif isinstance(output_path, str):
        output_path = Path(output_path)

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Count trades by intent type
        intent_counts: dict[str, int] = {}
        for trade in result.trades:
            if hasattr(trade, "intent_type"):
                intent_type = trade.intent_type
                if hasattr(intent_type, "value"):
                    intent_name = intent_type.value
                else:
                    intent_name = str(intent_type)

                # Skip HOLD intents as they're not actual trades
                if intent_name == "HOLD":
                    continue

                intent_counts[intent_name] = intent_counts.get(intent_name, 0) + 1

        if not intent_counts:
            return ChartResult(
                chart_type="intent_pie",
                file_path=None,
                success=False,
                error="No intent type data in trades",
            )

        # Sort by count for consistent ordering
        sorted_intents = sorted(intent_counts.items(), key=lambda x: x[1], reverse=True)
        labels = [item[0] for item in sorted_intents]
        sizes = [item[1] for item in sorted_intents]

        # Define colors for different intent types
        intent_colors = {
            "SWAP": "#2196F3",  # Blue
            "LP_OPEN": "#4CAF50",  # Green
            "LP_CLOSE": "#F44336",  # Red
            "PERP_OPEN": "#9C27B0",  # Purple
            "PERP_CLOSE": "#E91E63",  # Pink
            "BORROW": "#FF9800",  # Orange
            "REPAY": "#FFEB3B",  # Yellow
            "SUPPLY": "#00BCD4",  # Cyan
            "WITHDRAW": "#795548",  # Brown
            "BRIDGE": "#607D8B",  # Blue Grey
            "ENSURE_BALANCE": "#9E9E9E",  # Grey
        }

        # Get colors for each label, using a default for unknown types
        colors = [intent_colors.get(label, "#757575") for label in labels]

        # Create figure
        fig, ax = plt.subplots(figsize=(cfg.figure_width, cfg.figure_height))

        # Create pie chart
        _, texts, autotexts = ax.pie(  # type: ignore[misc]
            sizes,
            labels=labels,
            colors=colors,
            autopct=lambda pct: f"{pct:.1f}%\n({int(pct * sum(sizes) / 100)})",
            startangle=90,
            pctdistance=0.75,
        )

        # Style the text
        for text in texts:
            text.set_fontsize(cfg.font_size)
        for autotext in autotexts:
            autotext.set_fontsize(cfg.font_size - 1)
            autotext.set_color("white")
            autotext.set_fontweight("bold")

        # Set title
        chart_title = title or f"Trades by Intent Type - {result.strategy_id}"
        ax.set_title(chart_title, fontsize=cfg.title_size, fontweight="bold")

        # Equal aspect ratio ensures circular pie
        ax.axis("equal")

        plt.tight_layout()

        # Save plot
        fig.savefig(output_path, dpi=cfg.dpi, bbox_inches="tight")
        plt.close(fig)

        logger.info("Created intent pie chart: %s", output_path)

        return ChartResult(
            chart_type="intent_pie",
            file_path=output_path,
            success=True,
            format="png",
        )

    except Exception as e:
        logger.exception("Failed to create intent pie chart")
        return ChartResult(
            chart_type="intent_pie",
            file_path=None,
            success=False,
            error=str(e),
        )


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
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.warning("plotly not installed - cannot generate equity chart")
        return ""

    if not result.equity_curve:
        logger.warning("No equity curve data - cannot generate chart")
        return ""

    try:
        # Extract data
        timestamps: list[datetime] = []
        values: list[float] = []
        for point in result.equity_curve:
            timestamps.append(point.timestamp)
            values.append(float(point.value_usd) if isinstance(point.value_usd, Decimal) else point.value_usd)

        # Detect drawdown periods
        drawdown_periods: list[DrawdownPeriod] = []
        if show_drawdown:
            drawdown_periods = _detect_drawdown_periods(timestamps, values, 0.01)

        # Create figure
        fig = go.Figure()

        # Add drawdown shading
        if show_drawdown and drawdown_periods:
            for period in drawdown_periods:
                fig.add_vrect(
                    x0=period.start,
                    x1=period.end,
                    fillcolor="rgba(244, 67, 54, 0.15)",
                    layer="below",
                    line_width=0,
                )

        # Add equity curve
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

        # Add trade markers
        if show_trades and hasattr(result, "trades") and result.trades:
            trade_markers = _extract_trade_markers(result.trades, result.equity_curve)
            if trade_markers:
                entry_markers = [m for m in trade_markers if m.is_entry]
                exit_markers = [m for m in trade_markers if not m.is_entry]

                if entry_markers:
                    colors = ["#4CAF50" if (m.pnl_usd is None or m.pnl_usd >= 0) else "#F44336" for m in entry_markers]
                    fig.add_trace(
                        go.Scatter(
                            x=[m.timestamp for m in entry_markers],
                            y=[float(m.value_usd) for m in entry_markers],
                            mode="markers",
                            name="Entry",
                            marker={
                                "symbol": "triangle-up",
                                "size": 10,
                                "color": colors,
                                "line": {"color": "white", "width": 1},
                            },
                            hovertemplate="<b>%{x}</b><br>Entry<br>Value: $%{y:,.2f}<extra></extra>",
                        )
                    )

                if exit_markers:
                    colors = [
                        "#4CAF50" if (m.pnl_usd is not None and m.pnl_usd >= 0) else "#F44336" for m in exit_markers
                    ]
                    fig.add_trace(
                        go.Scatter(
                            x=[m.timestamp for m in exit_markers],
                            y=[float(m.value_usd) for m in exit_markers],
                            mode="markers",
                            name="Exit",
                            marker={
                                "symbol": "triangle-down",
                                "size": 10,
                                "color": colors,
                                "line": {"color": "white", "width": 1},
                            },
                            hovertemplate="<b>%{x}</b><br>Exit<br>Value: $%{y:,.2f}<extra></extra>",
                        )
                    )

        # Configure layout
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

        # Add range slider
        fig.update_xaxes(rangeslider_visible=True, rangeslider_thickness=0.05)

        # Return embedded HTML (not full page)
        return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="equity-chart")

    except Exception as e:
        logger.exception("Failed to generate embedded equity chart: %s", e)
        return ""


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
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.warning("plotly not installed - cannot generate PnL distribution chart")
        return ""

    if not hasattr(result, "trades") or not result.trades:
        logger.warning("No trades data - cannot generate PnL distribution chart")
        return ""

    try:
        # Extract PnL values
        pnl_values: list[float] = []
        for trade in result.trades:
            if hasattr(trade, "pnl_usd") and trade.pnl_usd is not None:
                pnl_values.append(float(trade.pnl_usd) if isinstance(trade.pnl_usd, Decimal) else trade.pnl_usd)

        if not pnl_values:
            return ""

        # Calculate statistics
        stats = calculate_distribution_stats(pnl_values)

        # Separate profits and losses
        profits = [x for x in pnl_values if x >= 0]
        losses = [x for x in pnl_values if x < 0]

        # Calculate bin parameters
        all_min = min(pnl_values)
        all_max = max(pnl_values)
        bin_size = (all_max - all_min) / bins if all_max != all_min else 1

        # Create figure
        fig = go.Figure()

        # Add losses histogram (red)
        if losses:
            fig.add_trace(
                go.Histogram(
                    x=losses,
                    name="Losses",
                    marker_color="rgba(244, 67, 54, 0.7)",
                    xbins={"start": all_min, "end": all_max, "size": bin_size},
                    hovertemplate="PnL: $%{x:,.2f}<br>Count: %{y}<extra></extra>",
                )
            )

        # Add profits histogram (green)
        if profits:
            fig.add_trace(
                go.Histogram(
                    x=profits,
                    name="Profits",
                    marker_color="rgba(76, 175, 80, 0.7)",
                    xbins={"start": all_min, "end": all_max, "size": bin_size},
                    hovertemplate="PnL: $%{x:,.2f}<br>Count: %{y}<extra></extra>",
                )
            )

        # Add vertical lines
        fig.add_vline(x=0, line_dash="dash", line_color="#757575", line_width=2)

        if stats:
            fig.add_vline(x=stats.mean, line_dash="dashdot", line_color="#1976D2", line_width=1.5)

        # Configure layout
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

        # Add stats annotation
        if stats:
            stats_text = (
                f"<b>Statistics</b><br>"
                f"Mean: ${stats.mean:,.2f}<br>"
                f"Median: ${stats.median:,.2f}<br>"
                f"Std Dev: ${stats.std_dev:,.2f}<br>"
                f"Skew: {stats.skewness:.2f} | Kurt: {stats.kurtosis:.2f}"
            )
            fig.add_annotation(
                text=stats_text,
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

        return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="pnl-distribution-chart")

    except Exception as e:
        logger.exception("Failed to generate embedded PnL distribution chart: %s", e)
        return ""


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
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.warning("plotly not installed - cannot generate drawdown chart")
        return ""

    if not result.equity_curve:
        logger.warning("No equity curve data - cannot generate drawdown chart")
        return ""

    try:
        # Extract data and calculate drawdown
        timestamps: list[datetime] = []
        drawdowns: list[float] = []
        peak_value = 0.0

        for point in result.equity_curve:
            value = float(point.value_usd) if isinstance(point.value_usd, Decimal) else point.value_usd
            peak_value = max(peak_value, value)
            drawdown_pct = ((peak_value - value) / peak_value * 100) if peak_value > 0 else 0.0
            timestamps.append(point.timestamp)
            drawdowns.append(-drawdown_pct)  # Negative for visual representation

        # Create figure
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

        # Configure layout
        chart_title = title or "Drawdown"
        max_dd = min(drawdowns) if drawdowns else 0
        fig.update_layout(
            title={"text": chart_title, "x": 0.5, "xanchor": "center", "font": {"size": 16}},
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
        logger.warning("plotly not installed - cannot generate attribution pie chart")
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
        logger.warning("plotly not installed - cannot generate attribution bar chart")
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
                metrics.pnl_by_asset,
                title="PnL by Asset",
                height=height,
            )

    return charts
