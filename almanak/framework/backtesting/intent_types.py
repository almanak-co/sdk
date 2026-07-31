"""Backtesting boundary types for canonical intent identity.

Backtesting executes the canonical :class:`almanak.core.intent_types.IntentType`
vocabulary.  Historical artifacts and duck-typed strategy integrations can,
however, contain values produced by a newer SDK or by external code.  Those
values are represented explicitly at the ingestion boundary rather than being
added to the execution vocabulary or silently coerced to a fake canonical
member.
"""

from __future__ import annotations

from dataclasses import dataclass

from almanak.core.intent_types import IntentType

__all__ = [
    "BacktestIntentType",
    "UNKNOWN_INTENT_TYPE",
    "UnrecognizedIntentType",
    "UnrecognizedIntentTypeError",
    "parse_backtest_intent_type",
]


@dataclass(frozen=True, slots=True)
class UnrecognizedIntentType:
    """A non-canonical value read from an artifact or integration boundary.

    ``raw_value`` is preserved for lossless round-tripping and diagnostics.  It
    is deliberately not an enum member: an unknown value has no execution
    semantics and must never become part of the canonical intent vocabulary.
    """

    raw_value: str

    def __post_init__(self) -> None:
        if not isinstance(self.raw_value, str) or not self.raw_value.strip():
            raise ValueError("UnrecognizedIntentType.raw_value must be a non-empty string")

    @property
    def value(self) -> str:
        """Wire-compatible value used by existing result serializers."""
        return self.raw_value

    def __str__(self) -> str:
        return self.raw_value


UNKNOWN_INTENT_TYPE = UnrecognizedIntentType("UNKNOWN")

type BacktestIntentType = IntentType | UnrecognizedIntentType


class UnrecognizedIntentTypeError(ValueError):
    """Raised when execution receives an intent with no canonical identity."""

    def __init__(self, intent_type: UnrecognizedIntentType) -> None:
        self.intent_type = intent_type
        super().__init__(f"Unrecognized intent type: {intent_type.value}")


def parse_backtest_intent_type(value: object) -> BacktestIntentType:
    """Parse a canonical value or preserve an unknown boundary value.

    Known strings retain the historical uppercase wire contract.  Unknown
    non-empty strings round-trip verbatim; missing/empty/non-string values use
    the legacy ``"UNKNOWN"`` artifact label without inventing a canonical
    enum member.
    """

    if isinstance(value, UnrecognizedIntentType):
        return value
    parsed = IntentType.try_parse(value)
    if parsed is not None:
        return parsed

    raw = getattr(value, "value", value)
    parsed = IntentType.try_parse(raw)
    if parsed is not None:
        return parsed
    if isinstance(raw, str) and raw.strip():
        if raw.strip().upper() == UNKNOWN_INTENT_TYPE.value:
            return UNKNOWN_INTENT_TYPE
        return UnrecognizedIntentType(raw)
    return UNKNOWN_INTENT_TYPE
