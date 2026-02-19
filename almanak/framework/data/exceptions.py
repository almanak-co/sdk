"""Quant Data Layer exception classes.

These exceptions extend the existing data module hierarchy
(DataSourceError from interfaces.py) with semantics specific
to the provenance-aware data layer.
"""

from __future__ import annotations

from .interfaces import DataSourceError


class DataUnavailableError(DataSourceError):
    """Raised when required data cannot be obtained from any provider.

    Used by the DataRouter when all providers (primary + fallbacks) have
    failed or been circuit-broken for a given request.

    Attributes:
        data_type: What kind of data was requested (e.g. 'pool_price', 'ohlcv').
        instrument: Instrument identifier (symbol or address).
        reason: Human-readable explanation.
    """

    def __init__(self, data_type: str, instrument: str, reason: str) -> None:
        self.data_type = data_type
        self.instrument = instrument
        self.reason = reason
        super().__init__(f"Data unavailable for {data_type}({instrument}): {reason}")


class StaleDataError(DataSourceError):
    """Raised when the only available data is too old for its classification.

    For EXECUTION_GRADE data, staleness beyond the configured threshold
    is a hard failure -- strategies must not trade on stale prices.

    Attributes:
        source: Provider that returned stale data.
        staleness_ms: How stale the data is in milliseconds.
        threshold_ms: The configured maximum acceptable staleness.
    """

    def __init__(self, source: str, staleness_ms: int, threshold_ms: int) -> None:
        self.source = source
        self.staleness_ms = staleness_ms
        self.threshold_ms = threshold_ms
        super().__init__(f"Stale data from '{source}': {staleness_ms}ms old (threshold: {threshold_ms}ms)")


class LowConfidenceError(DataSourceError):
    """Raised when data confidence falls below the acceptable threshold.

    Typically caused by excessive fallback (CEX data used for DeFi pair)
    or partial provider degradation.

    Attributes:
        source: Provider that returned low-confidence data.
        confidence: The reported confidence value (0.0-1.0).
        threshold: The minimum acceptable confidence.
    """

    def __init__(self, source: str, confidence: float, threshold: float) -> None:
        self.source = source
        self.confidence = confidence
        self.threshold = threshold
        super().__init__(f"Low confidence from '{source}': {confidence:.2f} (threshold: {threshold:.2f})")
