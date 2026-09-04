"""Explicit JSON-boundary adapters for full platform backtest artifacts.

The FastAPI service uses typed Pydantic response models instead. Platform
Cloud Run jobs upload a wider artifact to GCS, so they cross their JSON
boundary through :func:`serialize_result` immediately before upload.
"""

from __future__ import annotations

from typing import Any

from almanak.core.intent_types import IntentType
from almanak.framework.backtesting.intent_types import BacktestIntentType, UnrecognizedIntentType
from almanak.framework.backtesting.models import BacktestResult, decimal_str


def intent_type_wire_value(intent_type: BacktestIntentType | str) -> str:
    """Return a validated wire value for canonical, unknown, or legacy string intent types."""
    if isinstance(intent_type, IntentType | UnrecognizedIntentType):
        return intent_type.value
    if isinstance(intent_type, str):
        if not intent_type.strip():
            raise ValueError("intent_type must be a non-empty string")
        return intent_type
    raise TypeError(f"intent_type must be IntentType, UnrecognizedIntentType, or str; got {type(intent_type).__name__}")


def _serialize_equity_point(point: Any) -> dict[str, Any]:
    # Artifact compatibility intentionally preserves scale for historical USD
    # fields while normalizing projected prices. Converging these renderers
    # requires a separately versioned artifact schema change.
    payload: dict[str, Any] = {"timestamp": str(point.timestamp), "value_usd": str(point.value_usd)}
    if point.numeraire_price_usd is not None and point.numeraire_price_usd > 0:
        payload["numeraire_price_usd"] = decimal_str(point.numeraire_price_usd)
        payload["value_numeraire"] = decimal_str(point.value_usd / point.numeraire_price_usd)
    return payload


def serialize_result(result: BacktestResult) -> dict[str, Any]:
    """Serialize the established full result schema for GCS/callback JSON."""
    payload: dict[str, Any] = {
        "success": result.success,
        "error": result.error,
        "errors": result.errors or [],
        "data_quality": result.data_quality.to_dict() if result.data_quality is not None else None,
        "institutional_compliance": result.institutional_compliance,
        "compliance_violations": result.compliance_violations or [],
        "metrics": result.metrics.to_dict(),
        "decision_input_failures": result.decision_input_failures or [],
        "run_validity": result.run_validity.to_dict() if result.run_validity is not None else None,
        "equity_curve": [_serialize_equity_point(point) for point in (result.equity_curve or [])],
        "trades": [
            {
                "timestamp": str(trade.timestamp),
                "intent_type": intent_type_wire_value(trade.intent_type),
                "amount_usd": str(trade.amount_usd),
                "fee_usd": str(trade.fee_usd),
                "slippage_usd": str(trade.slippage_usd),
                "pnl_usd": str(trade.pnl_usd) if trade.pnl_usd is not None else None,
                "status": "filled" if trade.success else "rejected",
                "rejection_reason": trade.error if not trade.success and trade.error else None,
            }
            for trade in (result.trades or [])
        ],
        "duration_seconds": result.run_duration_seconds or 0.0,
    }
    if result.numeraire is not None:
        payload["numeraire"] = result.numeraire
    if result.initial_capital_numeraire is not None:
        payload["initial_capital_numeraire"] = str(result.initial_capital_numeraire)
    if result.final_capital_numeraire is not None:
        payload["final_capital_numeraire"] = str(result.final_capital_numeraire)
    if result.price_series:
        payload["price_series"] = [
            {
                "timestamp": str(point.timestamp),
                "prices": {key: decimal_str(price) for key, price in point.prices.items()},
            }
            for point in result.price_series
        ]
        payload["price_series_display_labels"] = dict(result.price_series_display_labels)
    if result.data_manifest is not None:
        payload["data_manifest"] = result.data_manifest
    if result.decision_summary is not None:
        # Aggregate only — the per-tick decision_events ship as the sidecar
        # decisions.jsonl artifact, never inline in result.json.
        payload["decision_summary"] = result.decision_summary
    return payload
