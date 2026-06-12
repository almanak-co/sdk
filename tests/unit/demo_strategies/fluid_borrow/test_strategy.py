"""Tests for the Fluid vault borrow demo strategy's one-shot state machine.

Pins the CodeRabbit round-2 fix: in a CONTINUOUS run, the forced
``supply`` / ``repay`` actions (like the forced ``open``) must dispatch
exactly once and then HOLD — re-emitting the same intent every decide()
would keep adding collateral / repaying on every cycle. A failed dispatch
re-arms the machine so the next cycle retries.

To run:
    uv run pytest tests/unit/demo_strategies/fluid_borrow/ -v
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.demo_strategies.fluid_borrow.strategy import DEFAULT_VAULT, FluidBorrowStrategy


@pytest.fixture
def mock_market():
    market = MagicMock()
    market.price.side_effect = lambda token: {"ETH": Decimal("2500"), "USDC": Decimal("1")}[token]
    return market


def create_strategy(config: dict | None = None) -> FluidBorrowStrategy:
    default_config = {
        "market_id": DEFAULT_VAULT,
        "collateral_token": "ETH",
        "collateral_amount": "0.2",
        "borrow_token": "USDC",
        "ltv_target": "0.3",
        "repay_amount": "50",
        "force_action": "",
    }
    if config:
        default_config.update(config)

    with patch.object(FluidBorrowStrategy, "__init__", lambda self, *args, **kwargs: None):
        strategy = FluidBorrowStrategy.__new__(FluidBorrowStrategy)

    strategy.config = default_config
    strategy._chain = "arbitrum"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"

    strategy.market_id = str(default_config["market_id"])
    strategy.collateral_token = default_config["collateral_token"]
    strategy.collateral_amount = Decimal(str(default_config["collateral_amount"]))
    strategy.borrow_token = default_config["borrow_token"]
    strategy.ltv_target = Decimal(str(default_config["ltv_target"]))
    strategy.repay_amount = Decimal(str(default_config["repay_amount"]))
    strategy.force_action = str(default_config["force_action"]).lower()
    strategy._loop_state = "idle"
    return strategy


def _executed(strategy: FluidBorrowStrategy, intent, success: bool) -> None:
    strategy.on_intent_executed(intent, success, result=None)


class TestForcedActionsAreOneShot:
    """Forced supply/repay/open dispatch ONCE per success, hold afterwards."""

    @pytest.mark.parametrize(
        ("action", "intent_type"),
        [("supply", "SUPPLY"), ("repay", "REPAY"), ("open", "BORROW")],
    )
    def test_forced_action_dispatches_once_then_holds(self, mock_market, action, intent_type):
        strategy = create_strategy({"force_action": action})

        first = strategy.decide(mock_market)
        assert first.intent_type.value == intent_type
        assert strategy._loop_state == "opening"

        _executed(strategy, first, success=True)
        assert strategy._loop_state == "complete"

        # Continuous run: every later decide() must HOLD, never re-emit.
        for _ in range(3):
            held = strategy.decide(mock_market)
            assert held.intent_type.value == "HOLD"

    @pytest.mark.parametrize(
        ("action", "intent_type"),
        [("supply", "SUPPLY"), ("repay", "REPAY"), ("open", "BORROW")],
    )
    def test_forced_action_failure_rearms_for_retry(self, mock_market, action, intent_type):
        strategy = create_strategy({"force_action": action})

        first = strategy.decide(mock_market)
        assert first.intent_type.value == intent_type

        # While the dispatch is in flight, decide() holds (no double-fire).
        assert strategy.decide(mock_market).intent_type.value == "HOLD"

        _executed(strategy, first, success=False)
        assert strategy._loop_state == "idle"

        retry = strategy.decide(mock_market)
        assert retry.intent_type.value == intent_type


class TestOrganicOpenUnchanged:
    def test_organic_open_dispatches_once_then_completes(self, mock_market):
        strategy = create_strategy()
        first = strategy.decide(mock_market)
        assert first.intent_type.value == "BORROW"
        assert strategy._loop_state == "opening"
        _executed(strategy, first, success=True)
        assert strategy.decide(mock_market).intent_type.value == "HOLD"

    def test_organic_open_price_guard_hold_does_not_consume_one_shot(self, mock_market):
        # A zero/invalid oracle answer makes _create_open_intent return a
        # price-guard HOLD — the one-shot must NOT be consumed (mirror of the
        # forced-open fix), so the open stays retryable when prices recover.
        strategy = create_strategy()
        bad_market = MagicMock()
        bad_market.price.side_effect = lambda token: Decimal("0")

        held = strategy.decide(bad_market)
        assert held.intent_type.value == "HOLD"
        assert strategy._loop_state == "idle", "a price-guard HOLD must not consume the one-shot"

        # Prices recover -> the open is still emitted on the next cycle.
        first = strategy.decide(mock_market)
        assert first.intent_type.value == "BORROW"
        assert strategy._loop_state == "opening"

    def test_organic_open_failure_rearms_for_retry(self, mock_market):
        # Existing failed-open retry semantics stay intact alongside the fix.
        strategy = create_strategy()
        first = strategy.decide(mock_market)
        assert first.intent_type.value == "BORROW"
        _executed(strategy, first, success=False)
        assert strategy._loop_state == "idle"
        retry = strategy.decide(mock_market)
        assert retry.intent_type.value == "BORROW"
