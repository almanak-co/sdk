"""Measured boot endpoint fixture for post-startup runner tests (VIB-5854)."""

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence


def measured_boot_snapshot(deployment_id: str = "test-strategy") -> PortfolioSnapshot:
    """Return a real measured-zero endpoint, never an untyped sentinel."""
    return PortfolioSnapshot(
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        deployment_id=deployment_id,
        total_value_usd=Decimal("0"),
        available_cash_usd=Decimal("0"),
        value_confidence=ValueConfidence.HIGH,
        iteration_number=0,
        cycle_id=f"boot-{deployment_id}",
        execution_mode="live",
    )
