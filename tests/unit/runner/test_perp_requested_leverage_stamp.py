"""``StrategyRunner._stamp_perp_requested_leverage`` — VIB-5724.

The connector enrich hook has no access to the executed intent, so the runner
copies the intent's REQUESTED leverage onto ``result.extracted_data`` at the one
seam where both are in scope (perp opens only). The hook reads it back to (a)
record it as metadata and (b) warn on divergence from the venue-observed
leverage. These tests pin the stamp's contract: perp opens only, no-op
otherwise, never overwrites, never fabricates.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from almanak.framework.runner.strategy_runner import StrategyRunner


def _result(extracted: Any) -> SimpleNamespace:
    return SimpleNamespace(extracted_data=extracted)


def _intent(intent_type: str, leverage: Any) -> SimpleNamespace:
    return SimpleNamespace(intent_type=intent_type, leverage=leverage)


def test_stamps_requested_leverage_for_perp_open() -> None:
    result = _result({})
    StrategyRunner._stamp_perp_requested_leverage(result, _intent("PERP_OPEN", "2.0"))
    assert result.extracted_data["leverage_requested"] == "2.0"


def test_stamps_for_perp_increase() -> None:
    result = _result({})
    StrategyRunner._stamp_perp_requested_leverage(result, _intent("PERP_INCREASE", 3))
    assert result.extracted_data["leverage_requested"] == "3"


def test_noop_for_non_perp_intent() -> None:
    result = _result({})
    StrategyRunner._stamp_perp_requested_leverage(result, _intent("SWAP", "2.0"))
    assert "leverage_requested" not in result.extracted_data


def test_noop_for_perp_close() -> None:
    """A CLOSE carries no meaningful requested leverage to stamp."""
    result = _result({})
    StrategyRunner._stamp_perp_requested_leverage(result, _intent("PERP_CLOSE", "2.0"))
    assert "leverage_requested" not in result.extracted_data


def test_noop_when_leverage_missing() -> None:
    result = _result({})
    StrategyRunner._stamp_perp_requested_leverage(result, _intent("PERP_OPEN", None))
    assert "leverage_requested" not in result.extracted_data


def test_noop_when_extracted_not_dict() -> None:
    result = _result(None)  # not a dict → guarded
    StrategyRunner._stamp_perp_requested_leverage(result, _intent("PERP_OPEN", "2.0"))
    assert result.extracted_data is None


def test_setdefault_does_not_overwrite_existing() -> None:
    result = _result({"leverage_requested": "already"})
    StrategyRunner._stamp_perp_requested_leverage(result, _intent("PERP_OPEN", "2.0"))
    assert result.extracted_data["leverage_requested"] == "already"


def test_accepts_enum_like_intent_type() -> None:
    """An intent_type with a ``.value`` (StrEnum) is normalised via that value."""
    result = _result({})
    enum_like = SimpleNamespace(value="PERP_OPEN")
    StrategyRunner._stamp_perp_requested_leverage(result, _intent(enum_like, "2.0"))
    assert result.extracted_data["leverage_requested"] == "2.0"
