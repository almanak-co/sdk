"""Unit tests for ``TeardownManager._generate_warnings``.

The method reads only its arguments, so the manager seam is
``object.__new__(TeardownManager)`` (same pattern as
``test_target_token_resolution.py``) — no runner, gateway, or compiler wiring.

Covers every warning branch: liquidation risk, emergency-mode-without-risk,
large position value, and multi-chain teardown. Also asserts parity with the
dashboard API preview (``almanak/framework/api/teardown.py``): both surfaces
render the same teardown to the operator, so the "large position" warning must
fire at the same total value (``LARGE_POSITION_WARNING_THRESHOLD_USD``) —
pre-PR #3401 the API warned above $100K while the manager warned above $500K.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.api.teardown import _generate_warnings as api_generate_warnings
from almanak.framework.teardown.models import (
    LARGE_POSITION_WARNING_THRESHOLD_USD,
    PositionInfo,
    PositionType,
    TeardownMode,
    TeardownPositionSummary,
)
from almanak.framework.teardown.teardown_manager import TeardownManager

LARGE_POSITION_WARNING = "Large position value. Extra care will be taken to minimize slippage."


def _manager() -> TeardownManager:
    return object.__new__(TeardownManager)


def _position(
    chain: str = "arbitrum",
    value_usd: str = "1000",
    liquidation_risk: bool = False,
) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id="pos-1",
        chain=chain,
        protocol="uniswap_v3",
        value_usd=Decimal(value_usd),
        liquidation_risk=liquidation_risk,
    )


def _summary(positions: list[PositionInfo]) -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="deployment:abc123",
        timestamp=datetime.now(UTC),
        positions=positions,
    )


class TestGenerateWarnings:
    def test_small_healthy_soft_teardown_has_no_warnings(self) -> None:
        summary = _summary([_position()])
        assert _manager()._generate_warnings(summary, TeardownMode.SOFT) == []

    def test_liquidation_risk_warns(self) -> None:
        summary = _summary([_position(liquidation_risk=True)])

        warnings = _manager()._generate_warnings(summary, TeardownMode.SOFT)

        assert warnings == ["Some positions have low health factors and may be at liquidation risk"]

    def test_hard_mode_without_risk_suggests_graceful(self) -> None:
        summary = _summary([_position()])

        warnings = _manager()._generate_warnings(summary, TeardownMode.HARD)

        assert len(warnings) == 1
        assert "Emergency mode selected but no immediate liquidation risk" in warnings[0]
        assert "graceful mode" in warnings[0]

    def test_hard_mode_with_risk_does_not_suggest_graceful(self) -> None:
        summary = _summary([_position(liquidation_risk=True)])

        warnings = _manager()._generate_warnings(summary, TeardownMode.HARD)

        assert len(warnings) == 1
        assert "liquidation risk" in warnings[0]
        assert "Emergency mode selected" not in warnings[0]

    def test_large_position_value_warns_above_shared_threshold(self) -> None:
        summary = _summary([_position(value_usd=str(LARGE_POSITION_WARNING_THRESHOLD_USD + 1))])

        warnings = _manager()._generate_warnings(summary, TeardownMode.SOFT)

        assert warnings == [LARGE_POSITION_WARNING]

    def test_value_at_threshold_does_not_warn(self) -> None:
        summary = _summary([_position(value_usd=str(LARGE_POSITION_WARNING_THRESHOLD_USD))])
        assert _manager()._generate_warnings(summary, TeardownMode.SOFT) == []

    def test_multi_chain_warns_with_chain_count(self) -> None:
        summary = _summary([_position(chain="arbitrum"), _position(chain="base")])

        warnings = _manager()._generate_warnings(summary, TeardownMode.SOFT)

        assert len(warnings) == 1
        assert "Multi-chain teardown across 2 chains" in warnings[0]

    def test_all_warnings_stack(self) -> None:
        summary = _summary(
            [
                _position(chain="arbitrum", value_usd="400000", liquidation_risk=True),
                _position(chain="base", value_usd="300000"),
            ]
        )

        warnings = _manager()._generate_warnings(summary, TeardownMode.SOFT)

        assert len(warnings) == 3
        assert any("liquidation risk" in w for w in warnings)
        assert any("Large position value" in w for w in warnings)
        assert any("Multi-chain teardown across 2 chains" in w for w in warnings)


class TestPreviewSurfaceParity:
    def test_large_position_threshold_matches_api_preview(self) -> None:
        """Both preview surfaces must agree on when a position is "large"."""
        for total_value, expect_warning in [
            (LARGE_POSITION_WARNING_THRESHOLD_USD + 1, True),
            (LARGE_POSITION_WARNING_THRESHOLD_USD, False),
            # The pre-alignment API threshold ($100K) must not have crept back.
            (Decimal("150_000"), False),
        ]:
            manager_warns = LARGE_POSITION_WARNING in _manager()._generate_warnings(
                _summary([_position(value_usd=str(total_value))]), TeardownMode.SOFT
            )
            api_warns = LARGE_POSITION_WARNING in api_generate_warnings(
                {"total_value_usd": float(total_value)}, mode="graceful"
            )
            assert manager_warns == api_warns == expect_warning, (
                f"Preview surfaces disagree at ${total_value}: "
                f"manager={manager_warns}, api={api_warns}, expected={expect_warning}"
            )
