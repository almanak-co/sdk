"""Plot Generator for QA Framework.

This module provides visualization capabilities for QA test results,
including price charts and RSI indicator plots using matplotlib.

Example:
    from almanak.framework.data.qa.reporting.plots import PlotGenerator

    generator = PlotGenerator(output_dir=Path("reports/qa-data/plots"))
    generator.create_price_plot("ETH", candles, "USD")
    generator.create_rsi_plot("ETH", rsi_history)
    generator.create_summary_grid([plot1, plot2, ...], rows=5, cols=2)
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class PlotConfig:
    """Configuration for plot styling.

    Attributes:
        dark_theme: Whether to use dark theme (default False)
        figure_width: Width of single plot figures in inches
        figure_height: Height of single plot figures in inches
        dpi: Resolution of saved plots
        font_size: Base font size for labels
        title_size: Font size for titles
        line_width: Width of plot lines
        rsi_oversold: RSI oversold threshold (default 30)
        rsi_overbought: RSI overbought threshold (default 70)
    """

    dark_theme: bool = False
    figure_width: float = 10.0
    figure_height: float = 6.0
    dpi: int = 150
    font_size: int = 10
    title_size: int = 12
    line_width: float = 1.5
    rsi_oversold: float = 30.0
    rsi_overbought: float = 70.0


@dataclass
class PlotResult:
    """Result of a plot generation operation.

    Attributes:
        token: Token symbol for the plot
        plot_type: Type of plot (price, rsi, summary)
        file_path: Path to the saved plot file
        success: Whether the plot was generated successfully
        error: Error message if generation failed
    """

    token: str
    plot_type: str
    file_path: Path | None
    success: bool
    error: str | None = None


class PlotGenerator:
    """Generates visualizations for QA test results.

    This class creates matplotlib plots for:
    - Price charts from OHLCV candle data
    - RSI indicator charts with overbought/oversold zones
    - Summary grids combining multiple token plots

    Attributes:
        output_dir: Directory to save generated plots
        config: Plot styling configuration

    Example:
        generator = PlotGenerator(output_dir=Path("reports/qa-data/plots"))

        # Create price plot
        result = generator.create_price_plot("ETH", candles, "USD")
        print(f"Saved to: {result.file_path}")

        # Create RSI plot
        result = generator.create_rsi_plot("ETH", rsi_history)
        print(f"Saved to: {result.file_path}")
    """

    def __init__(
        self,
        output_dir: Path,
        config: PlotConfig | None = None,
    ) -> None:
        """Initialize the plot generator.

        Args:
            output_dir: Directory to save generated plots
            config: Optional plot styling configuration.
                   If None, uses default PlotConfig.
        """
        self.output_dir = output_dir
        self.config = config or PlotConfig()
        self._ensure_output_dir()

    def _ensure_output_dir(self) -> None:
        """Ensure the output directory exists."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _apply_style(self, ax: Any) -> None:
        """Apply consistent styling to an axis.

        Args:
            ax: Matplotlib axis to style
        """
        ax.tick_params(labelsize=self.config.font_size)
        ax.grid(True, alpha=0.3)

    def create_price_plot(
        self,
        token: str,
        candles: list,
        quote: str,
        title: str | None = None,
    ) -> PlotResult:
        """Create a line chart of close prices over time.

        Args:
            token: Token symbol (e.g., "ETH")
            candles: List of OHLCVCandle objects with timestamp and close
            quote: Quote currency (e.g., "USD", "WETH")
            title: Optional custom title. If None, auto-generated.

        Returns:
            PlotResult with file path and success status
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            logger.error("matplotlib not installed. Run: uv add matplotlib")
            return PlotResult(
                token=token,
                plot_type="price",
                file_path=None,
                success=False,
                error="matplotlib not installed",
            )

        if not candles:
            return PlotResult(
                token=token,
                plot_type="price",
                file_path=None,
                success=False,
                error="No candle data provided",
            )

        try:
            # Extract data
            timestamps = [c.timestamp for c in candles]
            close_prices = [float(c.close) for c in candles]

            # Create figure
            fig, ax = plt.subplots(figsize=(self.config.figure_width, self.config.figure_height))

            # Plot price line
            ax.plot(
                timestamps,
                close_prices,
                linewidth=self.config.line_width,
                color="#2196F3",
                label=f"{token}/{quote}",
            )

            # Fill under the line
            ax.fill_between(timestamps, close_prices, alpha=0.1, color="#2196F3")

            # Set title and labels
            plot_title = title or f"{token} Price ({quote})"
            ax.set_title(plot_title, fontsize=self.config.title_size, fontweight="bold")
            ax.set_xlabel("Time", fontsize=self.config.font_size)
            ax.set_ylabel(f"Price ({quote})", fontsize=self.config.font_size)

            # Apply styling
            self._apply_style(ax)
            ax.legend(loc="upper left")

            # Rotate x-axis labels for readability
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()

            # Save plot
            filename = f"{token.lower()}_price_{quote.lower()}.png"
            file_path = self.output_dir / filename
            fig.savefig(file_path, dpi=self.config.dpi, bbox_inches="tight")
            plt.close(fig)

            logger.debug("Created price plot for %s: %s", token, file_path)

            return PlotResult(
                token=token,
                plot_type="price",
                file_path=file_path,
                success=True,
            )

        except Exception as e:
            logger.error("Failed to create price plot for %s: %s", token, str(e))
            return PlotResult(
                token=token,
                plot_type="price",
                file_path=None,
                success=False,
                error=str(e),
            )

    def create_rsi_plot(
        self,
        token: str,
        rsi_history: list,
        title: str | None = None,
    ) -> PlotResult:
        """Create an RSI line chart with overbought/oversold zones.

        Args:
            token: Token symbol (e.g., "ETH")
            rsi_history: List of RSIDataPoint objects with index and rsi
            title: Optional custom title. If None, auto-generated.

        Returns:
            PlotResult with file path and success status
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            logger.error("matplotlib not installed. Run: uv add matplotlib")
            return PlotResult(
                token=token,
                plot_type="rsi",
                file_path=None,
                success=False,
                error="matplotlib not installed",
            )

        if not rsi_history:
            return PlotResult(
                token=token,
                plot_type="rsi",
                file_path=None,
                success=False,
                error="No RSI history data provided",
            )

        try:
            # Extract data
            indices = [p.index for p in rsi_history]
            rsi_values = [p.rsi for p in rsi_history]

            # Create figure
            fig, ax = plt.subplots(figsize=(self.config.figure_width, self.config.figure_height))

            # Plot RSI line
            ax.plot(
                indices,
                rsi_values,
                linewidth=self.config.line_width,
                color="#9C27B0",
                label="RSI",
            )

            # Draw reference lines
            ax.axhline(
                y=self.config.rsi_oversold,
                color="#4CAF50",
                linestyle="--",
                linewidth=1,
                label=f"Oversold ({self.config.rsi_oversold})",
            )
            ax.axhline(
                y=self.config.rsi_overbought,
                color="#F44336",
                linestyle="--",
                linewidth=1,
                label=f"Overbought ({self.config.rsi_overbought})",
            )
            ax.axhline(
                y=50,
                color="#9E9E9E",
                linestyle=":",
                linewidth=0.5,
                alpha=0.5,
            )

            # Shade neutral zone (30-70)
            ax.fill_between(
                indices,
                self.config.rsi_oversold,
                self.config.rsi_overbought,
                alpha=0.1,
                color="#9E9E9E",
                label="Neutral Zone",
            )

            # Shade oversold zone (0-30)
            ax.fill_between(
                indices,
                0,
                self.config.rsi_oversold,
                alpha=0.1,
                color="#4CAF50",
            )

            # Shade overbought zone (70-100)
            ax.fill_between(
                indices,
                self.config.rsi_overbought,
                100,
                alpha=0.1,
                color="#F44336",
            )

            # Set title and labels
            plot_title = title or f"{token} RSI Indicator"
            ax.set_title(plot_title, fontsize=self.config.title_size, fontweight="bold")
            ax.set_xlabel("Period", fontsize=self.config.font_size)
            ax.set_ylabel("RSI", fontsize=self.config.font_size)

            # Set y-axis limits
            ax.set_ylim(0, 100)

            # Apply styling
            self._apply_style(ax)
            ax.legend(loc="upper right", fontsize=self.config.font_size - 1)

            plt.tight_layout()

            # Save plot
            filename = f"{token.lower()}_rsi.png"
            file_path = self.output_dir / filename
            fig.savefig(file_path, dpi=self.config.dpi, bbox_inches="tight")
            plt.close(fig)

            logger.debug("Created RSI plot for %s: %s", token, file_path)

            return PlotResult(
                token=token,
                plot_type="rsi",
                file_path=file_path,
                success=True,
            )

        except Exception as e:
            logger.error("Failed to create RSI plot for %s: %s", token, str(e))
            return PlotResult(
                token=token,
                plot_type="rsi",
                file_path=None,
                success=False,
                error=str(e),
            )

    def create_weth_price_plot(
        self,
        token: str,
        weth_prices: list,
        title: str | None = None,
    ) -> PlotResult:
        """Create a line chart of WETH-denominated prices over time.

        Args:
            token: Token symbol (e.g., "LINK")
            weth_prices: List of WETHPricePoint objects with timestamp and price_weth
            title: Optional custom title. If None, auto-generated.

        Returns:
            PlotResult with file path and success status
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            logger.error("matplotlib not installed. Run: uv add matplotlib")
            return PlotResult(
                token=token,
                plot_type="weth_price",
                file_path=None,
                success=False,
                error="matplotlib not installed",
            )

        if not weth_prices:
            return PlotResult(
                token=token,
                plot_type="weth_price",
                file_path=None,
                success=False,
                error="No WETH price data provided",
            )

        try:
            # Extract data
            timestamps = [p.timestamp for p in weth_prices]
            prices = [float(p.price_weth) for p in weth_prices]

            # Create figure
            fig, ax = plt.subplots(figsize=(self.config.figure_width, self.config.figure_height))

            # Plot price line
            ax.plot(
                timestamps,
                prices,
                linewidth=self.config.line_width,
                color="#FF9800",
                label=f"{token}/WETH",
            )

            # Fill under the line
            ax.fill_between(timestamps, prices, alpha=0.1, color="#FF9800")

            # Set title and labels
            plot_title = title or f"{token} Price (WETH)"
            ax.set_title(plot_title, fontsize=self.config.title_size, fontweight="bold")
            ax.set_xlabel("Time", fontsize=self.config.font_size)
            ax.set_ylabel("Price (WETH)", fontsize=self.config.font_size)

            # Apply styling
            self._apply_style(ax)
            ax.legend(loc="upper left")

            # Rotate x-axis labels for readability
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()

            # Save plot
            filename = f"{token.lower()}_price_weth.png"
            file_path = self.output_dir / filename
            fig.savefig(file_path, dpi=self.config.dpi, bbox_inches="tight")
            plt.close(fig)

            logger.debug("Created WETH price plot for %s: %s", token, file_path)

            return PlotResult(
                token=token,
                plot_type="weth_price",
                file_path=file_path,
                success=True,
            )

        except Exception as e:
            logger.error("Failed to create WETH price plot for %s: %s", token, str(e))
            return PlotResult(
                token=token,
                plot_type="weth_price",
                file_path=None,
                success=False,
                error=str(e),
            )

    def create_summary_grid(
        self,
        plots: list[tuple[str, list, str]],
        rows: int = 5,
        cols: int = 2,
        title: str | None = None,
    ) -> PlotResult:
        """Create a grid combining multiple token price plots.

        Args:
            plots: List of tuples (token, candles/data, plot_type)
                   where plot_type is "price", "rsi", or "weth_price"
            rows: Number of rows in the grid
            cols: Number of columns in the grid
            title: Optional title for the grid figure

        Returns:
            PlotResult with file path and success status
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            logger.error("matplotlib not installed. Run: uv add matplotlib")
            return PlotResult(
                token="summary",
                plot_type="grid",
                file_path=None,
                success=False,
                error="matplotlib not installed",
            )

        if not plots:
            return PlotResult(
                token="summary",
                plot_type="grid",
                file_path=None,
                success=False,
                error="No plots provided",
            )

        try:
            # Calculate figure size based on grid dimensions
            fig_width = self.config.figure_width * cols * 0.6
            fig_height = self.config.figure_height * rows * 0.5

            fig, axes = plt.subplots(
                rows,
                cols,
                figsize=(fig_width, fig_height),
                squeeze=False,
            )

            # Flatten axes for easy iteration
            axes_flat = axes.flatten()

            for idx, (token, data, plot_type) in enumerate(plots):
                if idx >= len(axes_flat):
                    break

                ax = axes_flat[idx]

                if not data:
                    ax.text(
                        0.5,
                        0.5,
                        f"{token}\nNo data",
                        ha="center",
                        va="center",
                        fontsize=self.config.font_size,
                    )
                    ax.set_axis_off()
                    continue

                if plot_type == "price":
                    self._plot_price_on_axis(ax, token, data, "USD")
                elif plot_type == "weth_price":
                    self._plot_weth_price_on_axis(ax, token, data)
                elif plot_type == "rsi":
                    self._plot_rsi_on_axis(ax, token, data)
                else:
                    ax.text(
                        0.5,
                        0.5,
                        f"{token}\nUnknown type: {plot_type}",
                        ha="center",
                        va="center",
                        fontsize=self.config.font_size,
                    )
                    ax.set_axis_off()

            # Hide any unused subplots
            for idx in range(len(plots), len(axes_flat)):
                axes_flat[idx].set_visible(False)

            # Set overall title
            grid_title = title or "QA Data Summary"
            fig.suptitle(
                grid_title,
                fontsize=self.config.title_size + 2,
                fontweight="bold",
            )

            plt.tight_layout()

            # Save plot
            filename = "summary_grid.png"
            file_path = self.output_dir / filename
            fig.savefig(file_path, dpi=self.config.dpi, bbox_inches="tight")
            plt.close(fig)

            logger.debug("Created summary grid: %s", file_path)

            return PlotResult(
                token="summary",
                plot_type="grid",
                file_path=file_path,
                success=True,
            )

        except Exception as e:
            logger.error("Failed to create summary grid: %s", str(e))
            return PlotResult(
                token="summary",
                plot_type="grid",
                file_path=None,
                success=False,
                error=str(e),
            )

    def _plot_price_on_axis(
        self,
        ax: Any,
        token: str,
        candles: list,
        quote: str,
    ) -> None:
        """Plot price data on a given axis (for grid layout).

        Args:
            ax: Matplotlib axis to plot on
            token: Token symbol
            candles: List of OHLCVCandle objects
            quote: Quote currency
        """
        from matplotlib.ticker import MaxNLocator

        timestamps = [c.timestamp for c in candles]
        close_prices = [float(c.close) for c in candles]

        ax.plot(
            timestamps,
            close_prices,
            linewidth=self.config.line_width * 0.7,
            color="#2196F3",
        )
        ax.fill_between(timestamps, close_prices, alpha=0.1, color="#2196F3")
        ax.set_title(f"{token}/{quote}", fontsize=self.config.font_size)
        ax.tick_params(labelsize=self.config.font_size - 2)
        ax.grid(True, alpha=0.3)
        # Simplify x-axis for grid
        ax.xaxis.set_major_locator(MaxNLocator(4))

    def _plot_weth_price_on_axis(
        self,
        ax: Any,
        token: str,
        weth_prices: list,
    ) -> None:
        """Plot WETH price data on a given axis (for grid layout).

        Args:
            ax: Matplotlib axis to plot on
            token: Token symbol
            weth_prices: List of WETHPricePoint objects
        """
        from matplotlib.ticker import MaxNLocator

        timestamps = [p.timestamp for p in weth_prices]
        prices = [float(p.price_weth) for p in weth_prices]

        ax.plot(
            timestamps,
            prices,
            linewidth=self.config.line_width * 0.7,
            color="#FF9800",
        )
        ax.fill_between(timestamps, prices, alpha=0.1, color="#FF9800")
        ax.set_title(f"{token}/WETH", fontsize=self.config.font_size)
        ax.tick_params(labelsize=self.config.font_size - 2)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_locator(MaxNLocator(4))

    def _plot_rsi_on_axis(
        self,
        ax: Any,
        token: str,
        rsi_history: list,
    ) -> None:
        """Plot RSI data on a given axis (for grid layout).

        Args:
            ax: Matplotlib axis to plot on
            token: Token symbol
            rsi_history: List of RSIDataPoint objects
        """
        indices = [p.index for p in rsi_history]
        rsi_values = [p.rsi for p in rsi_history]

        ax.plot(
            indices,
            rsi_values,
            linewidth=self.config.line_width * 0.7,
            color="#9C27B0",
        )
        ax.axhline(
            y=self.config.rsi_oversold,
            color="#4CAF50",
            linestyle="--",
            linewidth=0.5,
        )
        ax.axhline(
            y=self.config.rsi_overbought,
            color="#F44336",
            linestyle="--",
            linewidth=0.5,
        )
        ax.fill_between(
            indices,
            self.config.rsi_oversold,
            self.config.rsi_overbought,
            alpha=0.1,
            color="#9E9E9E",
        )
        ax.set_ylim(0, 100)
        ax.set_title(f"{token} RSI", fontsize=self.config.font_size)
        ax.tick_params(labelsize=self.config.font_size - 2)
        ax.grid(True, alpha=0.3)


__all__ = [
    "PlotConfig",
    "PlotGenerator",
    "PlotResult",
]
