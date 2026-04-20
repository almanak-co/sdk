"""Data quality tracking for PnL backtesting.

Provides the DataQualityTracker class which accumulates statistics about
price lookups and data quality throughout a backtest run. These statistics
are then used to populate the DataQualityReport in the BacktestResult.

Extracted from pnl/engine.py for module size management.
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from almanak.framework.backtesting.models import DataQualityReport


@dataclass
class DataQualityTracker:
    """Tracks data quality metrics during backtest execution.

    This class accumulates statistics about price lookups and data quality
    throughout the backtest, which are then used to populate the
    DataQualityReport in the BacktestResult.

    Attributes:
        total_price_lookups: Total number of price lookup attempts
        successful_lookups: Number of successful price lookups
        failed_lookups: Number of failed price lookups (KeyError)
        source_counts: Count of price lookups by provider/source name
        stale_data_count: Number of prices marked as stale
        interpolation_count: Number of interpolated/estimated data points
        staleness_threshold_seconds: Threshold for marking data as stale
        unresolved_token_count: Number of token addresses that could not be resolved
        missing_price_count: Number of unique tokens with missing prices during valuation
    """

    total_price_lookups: int = 0
    successful_lookups: int = 0
    failed_lookups: int = 0
    source_counts: dict[str, int] = field(default_factory=dict)
    stale_data_count: int = 0
    interpolation_count: int = 0
    staleness_threshold_seconds: int = 3600
    unresolved_token_count: int = 0
    _unresolved_tokens: set[str] = field(default_factory=set)
    gas_price_source_counts: dict[str, int] = field(default_factory=dict)
    missing_price_count: int = 0
    _missing_price_tokens: set[str] = field(default_factory=set)

    def record_lookup(
        self,
        success: bool,
        source: str = "unknown",
        is_stale: bool = False,
        is_interpolated: bool = False,
    ) -> None:
        """Record a price lookup attempt.

        Args:
            success: Whether the lookup was successful
            source: Name of the data source/provider
            is_stale: Whether the data was older than staleness threshold
            is_interpolated: Whether the data was interpolated/estimated
        """
        self.total_price_lookups += 1

        if success:
            self.successful_lookups += 1
            # Track source breakdown for successful lookups
            self.source_counts[source] = self.source_counts.get(source, 0) + 1

            if is_stale:
                self.stale_data_count += 1

            if is_interpolated:
                self.interpolation_count += 1
        else:
            self.failed_lookups += 1

    def record_successful_tick(self, source: str, token_count: int = 1) -> None:
        """Record a successful tick with prices from a source.

        Args:
            source: Name of the data source/provider
            token_count: Number of tokens with prices in this tick
        """
        for _ in range(token_count):
            self.record_lookup(success=True, source=source)

    def record_failed_tick(self, token_count: int = 1) -> None:
        """Record a failed tick (no price data available).

        Args:
            token_count: Number of tokens expected but missing
        """
        for _ in range(token_count):
            self.record_lookup(success=False)

    def record_unresolved_token(self, token_key: str, chain_id: int | None = None) -> None:
        """Record a token address that could not be resolved to a symbol.

        Tracks unique unresolved tokens to avoid counting the same token multiple times.

        Args:
            token_key: The token address or key that could not be resolved
            chain_id: Optional chain ID for context (included in tracking key)
        """
        # Create a unique key combining chain and token
        tracking_key = f"{chain_id or 'unknown'}:{token_key.lower()}"
        if tracking_key not in self._unresolved_tokens:
            self._unresolved_tokens.add(tracking_key)
            self.unresolved_token_count += 1

    def record_gas_price_source(self, source: str) -> None:
        """Record the source used for gas ETH price in a trade.

        Args:
            source: The gas price source used. Valid values:
                - "override": User-provided gas_eth_price_override value
                - "historical": Historical ETH price from data provider
                - "market": Current market ETH price from market state
        """
        self.gas_price_source_counts[source] = self.gas_price_source_counts.get(source, 0) + 1

    def record_missing_price(
        self,
        token: str,
        timestamp: datetime | None = None,
        chain_id: int | None = None,
    ) -> None:
        """Record a missing price lookup during portfolio valuation.

        Tracks unique (token, chain) pairs to avoid counting the same token multiple times.
        Increments both failed_lookups and the missing_price_tokens set.

        Args:
            token: The token symbol or address for which price was not found
            timestamp: Optional timestamp when the price lookup was attempted
            chain_id: Optional chain ID for context
        """
        # Record as failed lookup
        self.record_lookup(success=False, source="missing")

        # Track unique missing tokens
        tracking_key = f"{chain_id or 'unknown'}:{token.lower()}"
        if tracking_key not in self._missing_price_tokens:
            self._missing_price_tokens.add(tracking_key)
            self.missing_price_count += 1

    @property
    def missing_price_tokens(self) -> list[str]:
        """Get list of unique tokens with missing prices.

        Returns:
            List of unique tokens (as chain_id:token strings) that had missing prices
        """
        return list(self._missing_price_tokens)

    @property
    def coverage_ratio(self) -> Decimal:
        """Calculate the price data coverage ratio.

        Returns:
            Ratio of successful lookups to total lookups (0.0 to 1.0)
        """
        if self.total_price_lookups == 0:
            return Decimal("1.0")  # No lookups means perfect coverage

        return Decimal(str(self.successful_lookups)) / Decimal(str(self.total_price_lookups))

    def to_data_quality_report(self) -> DataQualityReport:
        """Convert tracker statistics to a DataQualityReport.

        Returns:
            DataQualityReport with coverage_ratio, source_breakdown,
            stale_data_count, interpolation_count, unresolved_token_count,
            gas_price_source_counts, missing_price_count, and missing_price_tokens populated
        """
        return DataQualityReport(
            coverage_ratio=self.coverage_ratio,
            source_breakdown=dict(self.source_counts),
            stale_data_count=self.stale_data_count,
            interpolation_count=self.interpolation_count,
            unresolved_token_count=self.unresolved_token_count,
            gas_price_source_counts=dict(self.gas_price_source_counts),
            missing_price_count=self.missing_price_count,
            missing_price_tokens=self.missing_price_tokens,
        )
