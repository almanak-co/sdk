"""Portfolio tracking module for the Almanak Strategy Framework.

Provides generic data structures for tracking portfolio value and positions
across all strategy types.

Example:
    from almanak.framework.portfolio import PortfolioSnapshot, ValueConfidence

    snapshot = PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id="my_strategy",
        total_value_usd=Decimal("15234.50"),
        available_cash_usd=Decimal("1000.00"),
        value_confidence=ValueConfidence.HIGH,
    )
"""

from almanak.framework.portfolio.models import (
    BaselineProvenance,
    BaselineProvenanceError,
    PortfolioMetrics,
    PortfolioSnapshot,
    PositionValue,
    TokenBalance,
    ValueConfidence,
    ValueConfidenceParseError,
    canonicalize_metrics_positions_json,
    decode_baseline_provenance,
    encode_baseline_provenance,
    enforce_open_position_value_invariant,
    find_zero_valued_open_positions,
    is_measured_accounting_snapshot,
    serialize_value_confidence,
    validate_baseline_provenance_initial_value,
    validate_immutable_baseline_update,
)

__all__ = [
    "BaselineProvenance",
    "BaselineProvenanceError",
    "canonicalize_metrics_positions_json",
    "PortfolioSnapshot",
    "PortfolioMetrics",
    "PositionValue",
    "TokenBalance",
    "ValueConfidence",
    "ValueConfidenceParseError",
    "decode_baseline_provenance",
    "encode_baseline_provenance",
    "enforce_open_position_value_invariant",
    "find_zero_valued_open_positions",
    "is_measured_accounting_snapshot",
    "serialize_value_confidence",
    "validate_baseline_provenance_initial_value",
    "validate_immutable_baseline_update",
]
