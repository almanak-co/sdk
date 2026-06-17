from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.runner.runner_gateway import _strategy_display_name, collect_position_snapshot
from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary


@pytest.mark.parametrize(
    ("strategy", "expected"),
    [
        (
            SimpleNamespace(
                strategy_display_name="display",
                config=SimpleNamespace(strategy_display_name="config-display"),
                STRATEGY_NAME="canonical",
                STRATEGY_METADATA=SimpleNamespace(name="metadata-name"),
            ),
            "display",
        ),
        (
            SimpleNamespace(
                config=SimpleNamespace(strategy_display_name="config-display"),
                STRATEGY_NAME="canonical",
                STRATEGY_METADATA=SimpleNamespace(name="metadata-name"),
            ),
            "config-display",
        ),
        (
            SimpleNamespace(
                config=SimpleNamespace(strategy_display_name=""),
                STRATEGY_NAME="canonical",
                STRATEGY_METADATA=SimpleNamespace(name="metadata-name"),
            ),
            "canonical",
        ),
        (
            SimpleNamespace(
                config=SimpleNamespace(strategy_display_name=""),
                STRATEGY_NAME="",
                STRATEGY_METADATA=SimpleNamespace(name="metadata-name"),
            ),
            "metadata-name",
        ),
        (
            SimpleNamespace(
                config=SimpleNamespace(strategy_display_name=""),
                STRATEGY_NAME="",
                STRATEGY_METADATA={"canonical_name": "metadata-canonical"},
            ),
            "metadata-canonical",
        ),
    ],
)
def test_strategy_display_name_prefers_human_readable_metadata(strategy, expected) -> None:
    assert _strategy_display_name(strategy) == expected


def test_strategy_display_name_falls_back_to_class_name() -> None:
    class DemoStrategy:
        pass

    assert _strategy_display_name(DemoStrategy()) == "DemoStrategy"


def _gateway_runner() -> SimpleNamespace:
    return SimpleNamespace(_get_gateway_client=lambda: object())


def _summary(*positions: PositionInfo) -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="deployment:test",
        timestamp=datetime.now(UTC),
        positions=list(positions),
    )


def test_collect_position_snapshot_returns_none_without_gateway_client() -> None:
    runner = SimpleNamespace(_get_gateway_client=lambda: None)
    strategy = SimpleNamespace(get_open_positions=MagicMock())

    assert collect_position_snapshot(runner, strategy) is None
    strategy.get_open_positions.assert_not_called()


def test_collect_position_snapshot_returns_none_when_strategy_has_no_hook() -> None:
    assert collect_position_snapshot(_gateway_runner(), SimpleNamespace()) is None


@pytest.mark.parametrize(
    "summary",
    [
        None,
        SimpleNamespace(),
        SimpleNamespace(positions=[]),
    ],
)
def test_collect_position_snapshot_returns_none_for_empty_summaries(summary) -> None:
    strategy = SimpleNamespace(get_open_positions=MagicMock(return_value=summary))

    assert collect_position_snapshot(_gateway_runner(), strategy) is None
    strategy.get_open_positions.assert_called_once_with()


def test_collect_position_snapshot_converts_required_and_optional_fields() -> None:
    position = PositionInfo(
        position_type=PositionType.PERP,
        position_id="gmx-eth-long",
        chain=SimpleNamespace(value="arbitrum"),
        protocol="GMX V2",
        value_usd=Decimal("123.45"),
        liquidation_risk=True,
        health_factor=Decimal("1.234"),
        details={"market": "ETH", "size": Decimal("2.5")},
        entry_price=Decimal("3000"),
        current_price=Decimal("3100"),
        unrealized_pnl_usd=Decimal("50"),
        unrealized_pnl_pct=Decimal("0.10"),
        direction="LONG",
        size_usd=Decimal("500"),
        collateral_usd=Decimal("100"),
        leverage=Decimal("5"),
    )
    strategy = SimpleNamespace(get_open_positions=MagicMock(return_value=_summary(position)))

    protos = collect_position_snapshot(_gateway_runner(), strategy)

    assert protos is not None
    assert len(protos) == 1
    proto = protos[0]
    assert proto.position_type == "PERP"
    assert proto.position_id == "gmx-eth-long"
    assert proto.chain == "arbitrum"
    assert proto.protocol == "GMX V2"
    assert proto.value_usd == "123.45"
    assert proto.liquidation_risk is True
    assert proto.health_factor == "1.234"
    assert dict(proto.details) == {"market": "ETH", "size": "2.5"}
    assert proto.entry_price == "3000"
    assert proto.current_price == "3100"
    assert proto.unrealized_pnl_usd == "50"
    assert proto.unrealized_pnl_pct == "0.10"
    assert proto.direction == "LONG"
    assert proto.size_usd == "500"
    assert proto.collateral_usd == "100"
    assert proto.leverage == "5"


def test_collect_position_snapshot_leaves_missing_optional_fields_empty() -> None:
    position = PositionInfo(
        position_type=PositionType.LP,
        position_id="uni-v3-weth-usdc",
        chain="base",
        protocol="Uniswap V3",
        value_usd=Decimal("10"),
    )
    strategy = SimpleNamespace(get_open_positions=MagicMock(return_value=_summary(position)))

    protos = collect_position_snapshot(_gateway_runner(), strategy)

    assert protos is not None
    proto = protos[0]
    assert proto.health_factor == ""
    assert dict(proto.details) == {}
    assert proto.entry_price == ""
    assert proto.current_price == ""
    assert proto.unrealized_pnl_usd == ""
    assert proto.unrealized_pnl_pct == ""
    assert proto.direction == ""
    assert proto.size_usd == ""
    assert proto.collateral_usd == ""
    assert proto.leverage == ""


def test_collect_position_snapshot_returns_none_when_strategy_hook_raises() -> None:
    strategy = SimpleNamespace(get_open_positions=MagicMock(side_effect=RuntimeError("boom")))

    assert collect_position_snapshot(_gateway_runner(), strategy) is None
