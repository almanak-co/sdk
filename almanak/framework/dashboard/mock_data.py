"""Mock data generators for the Almanak Operator Dashboard.

DEPRECATED: Mock data is being removed in favor of real data sources.
This file now returns empty or minimal valid data structures to prevent crashes
while migration to real data sources is in progress.
"""

from decimal import Decimal

from almanak.framework.dashboard.models import (
    OperatorCard,
    PnLDataPoint,
    PositionSummary,
    Severity,
    Strategy,
    StrategyConfig,
    StuckReason,
    TimelineEvent,
)


def generate_mock_pnl_history(days: int = 7, base_value: Decimal = Decimal("0")) -> list[PnLDataPoint]:
    return []


def generate_mock_timeline_events(strategy_id: str) -> list[TimelineEvent]:
    return []


def generate_mock_position(strategy_type: str) -> PositionSummary:
    return PositionSummary(
        token_balances=[],
        lp_positions=[],
        total_lp_value_usd=Decimal("0"),
    )


def generate_mock_operator_card(_strategy_id: str, _reason: StuckReason, _severity: Severity) -> OperatorCard | None:
    return None


def get_mock_strategies() -> list[Strategy]:
    return []


def generate_mock_multi_chain_strategy() -> Strategy | None:
    return None


def generate_extended_timeline_events(strategy_id: str, page: int = 0, page_size: int = 20) -> list[TimelineEvent]:
    return []


def generate_mock_strategy_config(strategy_id: str, strategy_name: str) -> StrategyConfig:
    return StrategyConfig(
        strategy_id=strategy_id,
        strategy_name=strategy_name,
        max_slippage=Decimal("0.005"),
        trade_size_usd=Decimal("1000"),
        rebalance_threshold=Decimal("0.05"),
        min_health_factor=Decimal("1.5"),
        max_leverage=Decimal("3"),
        daily_loss_limit_usd=Decimal("500"),
    )
