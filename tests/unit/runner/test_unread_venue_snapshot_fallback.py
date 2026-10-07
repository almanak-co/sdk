"""An unread venue account keeps the snapshot UNAVAILABLE; the wallet-only fallback never replaces it."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.portfolio.models import PortfolioSnapshot, PositionValue, ValueConfidence
from almanak.framework.runner.runner_state import _value_via_strategy_fallback
from almanak.framework.teardown.models import PositionType


def _snapshot(confidence: ValueConfidence, positions: list[PositionValue]) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id="deployment:abc",
        total_value_usd=Decimal("0"),
        available_cash_usd=Decimal("9.12"),
        value_confidence=confidence,
        positions=positions,
        chain="bsc",
    )


def _venue_row(details: dict) -> PositionValue:
    return PositionValue(
        position_type=PositionType.PERP,
        protocol="aster_perps",
        chain="bsc",
        value_usd=Decimal("0"),
        label="aster_perps PERP",
        details=details,
    )


class _Strategy:
    deployment_id = "deployment:abc"

    def __init__(self) -> None:
        self.calls = 0

    def get_portfolio_snapshot(self) -> PortfolioSnapshot:
        self.calls += 1
        return _snapshot(ValueConfidence.HIGH, [])


def test_unread_venue_account_is_not_replaced_by_the_wallet_only_fallback() -> None:
    strategy = _Strategy()
    current = _snapshot(
        ValueConfidence.UNAVAILABLE, [_venue_row({"venue_account_unread": True, "valuation_status": "no_path"})]
    )
    result = _value_via_strategy_fallback(strategy, 3, current)
    assert result is current and result.value_confidence == ValueConfidence.UNAVAILABLE
    assert strategy.calls == 0


def test_other_unavailable_snapshots_still_use_the_strategy_fallback() -> None:
    strategy = _Strategy()
    current = _snapshot(ValueConfidence.UNAVAILABLE, [_venue_row({"valuation_status": "no_path"})])
    result = _value_via_strategy_fallback(strategy, 3, current)
    assert strategy.calls == 1 and result.value_confidence == ValueConfidence.HIGH
