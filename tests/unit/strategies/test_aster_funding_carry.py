"""Unit tests for the Aster funding-carry incubating strategy."""

from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.connectors.aster_perps.execution import AsterOrderHandler
from almanak.connectors.aster_perps.proto import aster_perps_pb2
from almanak.framework.execution.offchain_venue import offchain_execution_result
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult
from almanak.framework.execution.submission import SubmissionProvenance
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.teardown import PositionType, TeardownMode
from almanak.framework.teardown.chain_validation import teardown_swap_chain_error
from strategies.incubating.aster_funding_carry.strategy import (
    AsterFundingCarryStrategy,
    CarryConfig,
)

_CONFIG_PATH = Path(__file__).resolve().parents[3] / "strategies/incubating/aster_funding_carry/config.json"
ETH = "0x2170Ed0880ac9A755fd29B2688956BD959F933F8"
USDT = "0x55d398326f99059fF775485246999027B3197955"


def _config(**overrides: Any) -> dict[str, Any]:
    config = json.loads(_CONFIG_PATH.read_text())
    config.update(overrides)
    return config


def _strategy(state: dict[str, Any] | None = None, **overrides: Any) -> AsterFundingCarryStrategy:
    strategy = AsterFundingCarryStrategy.__new__(AsterFundingCarryStrategy)
    strategy.config = _config(**overrides)
    strategy.carry = CarryConfig(strategy.config)
    strategy.state = dict(state or {"phase": "deposit"})
    strategy._deployment_id = "test-aster-carry"
    strategy._chain = "bsc"
    return strategy


def _rate(rate_8h: str, *, live: bool = True) -> SimpleNamespace:
    return SimpleNamespace(rate_8h=Decimal(rate_8h), is_live_data=live)


def _market(
    *,
    rate_8h: str | None = "0.0002",
    price: str | None = "2566",
    live: bool = True,
    balance: str | None = "0",
) -> MagicMock:
    market = MagicMock()
    if rate_8h is None:
        market.funding_rate.side_effect = RuntimeError("Funding rate unavailable for aster_perps/ETH-USD")
    else:
        market.funding_rate.return_value = _rate(rate_8h, live=live)
    if price is None:
        market.price.side_effect = RuntimeError("price unavailable")
    else:
        market.price.return_value = Decimal(price)
    if balance is not None:
        market.balance.return_value = SimpleNamespace(balance=Decimal(balance))
    return market


def _settled() -> dict[str, Any]:
    return {"phase": "settle", "deposit_sent_at": 1.0, "deposited_at": time.time() - 1000}


def _open_result(executed: str | None = "0.002", avg: str = "2565.5", quote: str = "5.131") -> SimpleNamespace:
    order = {} if executed is None else {"executed_qty": executed, "avg_price": avg, "cum_quote": quote}
    return SimpleNamespace(extracted_data={"aster_order": order})


def _swap_result(out: str | None, inp: str | None = "5.14") -> SimpleNamespace:
    amounts = SimpleNamespace(
        amount_out_decimal=Decimal(out) if out is not None else None,
        amount_in_decimal=Decimal(inp) if inp is not None else None,
    )
    return SimpleNamespace(swap_amounts=amounts)


def _intent(kind: IntentType, **fields: Any) -> SimpleNamespace:
    return SimpleNamespace(intent_type=kind, **fields)


def test_shipped_config_is_valid() -> None:
    carry = CarryConfig(_config())
    assert carry.spot_usd == Decimal("5")
    assert carry.deposit_usd == Decimal("3")
    assert carry.leverage == Decimal("3")
    assert carry.force_entry is False


@pytest.mark.parametrize(
    "overrides",
    [
        {"leverage": "2.5"},
        {"leverage": "0"},
        {"leverage": "126"},
        {"spot_usd": "4"},
        {"spot_usd": "-5"},
        {"deposit_usd": "1"},
        {"entry_funding_rate_threshold": "0.00001"},
        {"max_slippage": "0"},
        {"max_slippage": "0.2"},
        {"force_entry": "yes"},
        {"deposit_settle_seconds": -1},
        {"max_spot_leg_attempts": 0},
        {"spot_token": "ETH"},
        {"quote_token": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"},
        {"perp_qty_step": "0"},
        {"spot_usd": "NaN"},
        {"spot_usd": True},
    ],
)
def test_invalid_config_is_refused_at_construction(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        CarryConfig(_config(**overrides))


def test_hedge_quantity_rounds_up_to_the_step_and_clears_the_minimum_notional() -> None:
    strategy = _strategy()
    # $5 / 2566 = 0.00195 -> 0.002 ETH = $5.13, above $5 x 1.02.
    assert strategy.perp_quantity(Decimal("2566")) == Decimal("0.002")
    # At $2500, 0.002 ETH = $5.00 does not clear the margin, so one more step.
    assert strategy.perp_quantity(Decimal("2500")) == Decimal("0.003")


def test_order_notional_lands_on_the_quantity_after_the_venue_rounds_down() -> None:
    strategy = _strategy()
    qty, price = Decimal("0.002"), Decimal("2566")
    notional = strategy.perp_order_notional(qty, price)
    for venue_mark in (price * Decimal("0.9"), price, price * Decimal("1.1")):
        venue_qty = (notional / venue_mark / Decimal("0.001")).to_integral_value(rounding="ROUND_DOWN") * Decimal(
            "0.001"
        )
        assert venue_qty == qty


def test_deposit_is_first_and_marks_the_send_before_broadcast() -> None:
    strategy = _strategy()
    intent = strategy.decide(_market())
    assert intent.intent_type == IntentType.PERP_DEPOSIT
    assert intent.amount == Decimal("3")
    assert "deposit_sent_at" in strategy.state
    assert strategy.state["phase"] == "deposit"

    strategy.on_intent_executed(intent, True, None)
    assert strategy.state["phase"] == "settle"


def test_settle_waits_before_any_trade() -> None:
    strategy = _strategy({"phase": "settle", "deposited_at": time.time()})
    assert strategy.decide(_market()).intent_type == IntentType.HOLD


def test_funding_above_threshold_opens_the_short_first() -> None:
    strategy = _strategy(_settled())
    intent = strategy.decide(_market(rate_8h="0.00015"))

    assert intent.intent_type == IntentType.PERP_OPEN
    assert intent.is_long is False
    assert intent.protocol == "aster_perps"
    assert intent.leverage == Decimal("3")
    assert intent.max_slippage == Decimal("0.005")
    assert intent.size_usd == Decimal("6.41")  # (0.002 + 0.0005) x 2566, rounded down to cents
    assert strategy.state["phase"] == "settle"


@pytest.mark.parametrize("rate", ["0.0001", "0.00005", "-0.0003"])
def test_funding_at_or_below_threshold_holds(rate: str) -> None:
    assert _strategy(_settled()).decide(_market(rate_8h=rate)).intent_type == IntentType.HOLD


@pytest.mark.parametrize("market", [_market(rate_8h=None), _market(live=False)], ids=["unavailable", "not-live"])
def test_unavailable_funding_holds(market: MagicMock) -> None:
    assert _strategy(_settled()).decide(market).intent_type == IntentType.HOLD


def test_a_hedge_beyond_the_margin_capacity_holds() -> None:
    # At $2500 the 0.002 lot is under the $5 minimum; 0.003 ETH = $7.50 exceeds 2 x 3 x 0.9.
    assert _strategy(_settled(), deposit_usd="2").decide(_market(price="2500")).intent_type == IntentType.HOLD
    # 0.002 ETH at $4100 = $8.20 exceeds the shipped 3 x 3 x 0.9 = $8.10.
    assert _strategy(_settled()).decide(_market(price="4100")).intent_type == IntentType.HOLD


@pytest.mark.parametrize("price", ["1700", "2500", "2566", "4000"])
def test_the_shipped_deposit_margins_the_hedge_across_the_eth_range(price: str) -> None:
    assert _strategy(_settled()).decide(_market(price=price)).intent_type == IntentType.PERP_OPEN


def test_unavailable_price_holds_even_when_funding_qualifies() -> None:
    assert _strategy(_settled()).decide(_market(price=None)).intent_type == IntentType.HOLD


def test_force_entry_enters_regardless_of_the_rate_once() -> None:
    strategy = _strategy(_settled(), force_entry=True)
    intent = strategy.decide(_market(rate_8h=None))
    assert intent.intent_type == IntentType.PERP_OPEN

    strategy.on_intent_executed(intent, True, _open_result())
    assert strategy.state["force_entry_used"] is True

    strategy.state = {"phase": "idle", "force_entry_used": True}
    assert strategy.decide(_market(rate_8h="0.00001")).intent_type == IntentType.HOLD


def test_force_entry_still_needs_a_price() -> None:
    strategy = _strategy(_settled(), force_entry=True)
    assert strategy.decide(_market(price=None)).intent_type == IntentType.HOLD


def test_spot_is_bought_after_the_short_and_sized_to_its_measured_fill() -> None:
    strategy = _strategy(_settled())
    strategy.on_intent_executed(_intent(IntentType.PERP_OPEN), True, _open_result(executed="0.002"))
    assert strategy.state["phase"] == "hedge_spot"
    assert strategy.state["perp_qty"] == "0.002"

    buy = strategy.decide(_market(price="2570"))
    assert buy.intent_type == IntentType.SWAP
    assert buy.from_token == USDT and buy.to_token == ETH
    assert buy.amount == Decimal("5.14")

    strategy.on_intent_executed(buy, True, _swap_result("0.00199", "5.14"))
    assert strategy.state["phase"] == "carry"
    assert strategy.state["spot_amount"] == "0.00199"


def test_a_partially_filled_open_hedges_only_what_filled() -> None:
    strategy = _strategy(_settled())
    strategy.on_intent_executed(_intent(IntentType.PERP_OPEN), True, _open_result(executed="0.001"))
    assert strategy.decide(_market(price="2500")).amount == Decimal("2.50")


def test_an_unreadable_open_fill_unwinds_instead_of_guessing() -> None:
    strategy = _strategy(_settled())
    strategy.on_intent_executed(_intent(IntentType.PERP_OPEN), True, _open_result(executed=None))
    assert strategy.state["phase"] == "exit_perp"
    assert strategy.decide(_market()).intent_type == IntentType.PERP_CLOSE


def test_a_failed_open_with_a_partial_fill_unwinds_it() -> None:
    strategy = _strategy(_settled())
    strategy.on_intent_executed(_intent(IntentType.PERP_OPEN), False, _open_result(executed="0.001"))
    assert strategy.state["phase"] == "exit_perp"


def test_a_failed_open_with_no_fill_stays_flat() -> None:
    strategy = _strategy(_settled())
    strategy.on_intent_executed(_intent(IntentType.PERP_OPEN), False, SimpleNamespace(extracted_data={}))
    assert strategy.state["phase"] == "settle"


def test_spot_price_outage_holds_the_short_for_its_spot_leg() -> None:
    strategy = _strategy({"phase": "hedge_spot", "perp_qty": "0.002"})
    assert strategy.decide(_market(price=None)).intent_type == IntentType.HOLD


def test_repeated_spot_failures_unwind_the_short() -> None:
    strategy = _strategy({"phase": "hedge_spot", "perp_qty": "0.002"})
    buy = _intent(IntentType.SWAP, to_token=ETH)
    for _ in range(2):
        strategy.on_intent_executed(buy, False, None)
        assert strategy.state["phase"] == "hedge_spot"
    strategy.on_intent_executed(buy, False, None)
    assert strategy.state["phase"] == "exit_perp"

    strategy.on_intent_executed(_intent(IntentType.PERP_CLOSE), True, None)
    assert strategy.state["phase"] == "idle"


def _carrying(**extra: Any) -> dict[str, Any]:
    return {
        "phase": "carry",
        "deposit_sent_at": 1.0,
        "perp_qty": "0.002",
        "perp_entry_price": "2565.5",
        "perp_notional": "5.131",
        "spot_amount": "0.00199",
        "spot_cost": "5.14",
        **extra,
    }


def test_carry_holds_while_funding_stays_above_exit() -> None:
    assert _strategy(_carrying()).decide(_market(rate_8h="0.00003")).intent_type == IntentType.HOLD


def test_carry_holds_when_funding_is_unavailable() -> None:
    assert _strategy(_carrying()).decide(_market(rate_8h=None)).intent_type == IntentType.HOLD


@pytest.mark.parametrize("rate", ["0.00001", "-0.0002"])
def test_funding_drop_closes_the_perp_then_sells_the_spot(rate: str) -> None:
    strategy = _strategy(_carrying())
    close = strategy.decide(_market(rate_8h=rate))
    assert close.intent_type == IntentType.PERP_CLOSE
    assert close.is_long is False

    strategy.on_intent_executed(close, True, None)
    assert strategy.state["phase"] == "exit_spot"

    sell = strategy.decide(_market())
    assert sell.intent_type == IntentType.SWAP
    assert sell.from_token == ETH and sell.to_token == USDT
    assert sell.amount == Decimal("0.00199")

    strategy.on_intent_executed(sell, True, _swap_result("5.10"))
    assert strategy.state["phase"] == "idle"
    assert strategy.state["cycles"] == 1
    assert "spot_amount" not in strategy.state and "perp_qty" not in strategy.state


def test_an_exit_decision_survives_a_released_partial_close_without_a_callback() -> None:
    strategy = _strategy(_carrying())
    assert strategy.decide(_market(rate_8h="0.00001")).intent_type == IntentType.PERP_CLOSE
    assert "exit_requested_at" in strategy.get_persistent_state()

    # Off-chain recovery books a partial close and releases the barrier with no
    # on_intent_executed, so the phase is still carry; funding has recovered.
    assert strategy.decide(_market(rate_8h="0.0005")).intent_type == IntentType.PERP_CLOSE

    strategy.on_intent_executed(_intent(IntentType.PERP_CLOSE), True, None)
    assert "exit_requested_at" not in strategy.state


def test_a_failed_partial_close_stays_in_position_and_retries_whatever_funding_does() -> None:
    strategy = _strategy(_carrying())
    close = strategy.decide(_market(rate_8h="0.00001"))
    strategy.on_intent_executed(close, False, None)

    assert strategy.state["phase"] == "exit_perp"
    assert strategy.state["spot_amount"] == "0.00199"
    assert strategy.decide(_market(rate_8h="0.0005")).intent_type == IntentType.PERP_CLOSE


def test_an_unmeasured_spot_fill_sells_only_what_the_buy_added() -> None:
    strategy = _strategy({"phase": "hedge_spot", "perp_qty": "0.002"})
    buy = strategy.decide(_market(balance="0.0005"))
    assert strategy.state["spot_balance_before"] == "0.0005"
    strategy.on_intent_executed(buy, True, _swap_result(None, None))
    assert strategy.state["spot_amount"] is None and strategy.state["spot_unmeasured"] is True

    strategy.state["phase"] = "exit_spot"
    assert strategy.decide(_market(balance="0.0026")).amount == Decimal("0.0021")


@pytest.mark.parametrize("balance", ["0.0005", "0.0001"])
def test_an_unmeasured_spot_fill_with_no_measured_gain_sells_nothing(balance: str) -> None:
    strategy = _strategy(
        {"phase": "exit_spot", "spot_amount": None, "spot_unmeasured": True, "spot_balance_before": "0.0005"}
    )
    assert strategy.decide(_market(balance=balance)).intent_type == IntentType.HOLD


def test_the_spot_buy_waits_for_a_balance_baseline() -> None:
    strategy = _strategy({"phase": "hedge_spot", "perp_qty": "0.002"})
    assert strategy.decide(_market(balance=None)).intent_type == IntentType.HOLD
    assert "spot_balance_before" not in strategy.state


def test_an_open_with_an_unknown_outcome_is_visible_to_teardown() -> None:
    strategy = _strategy(_settled())
    assert strategy.decide(_market()).intent_type == IntentType.PERP_OPEN
    assert "open_sent_at" in strategy.get_persistent_state()

    # The outcome is unknown and teardown runs before the barrier reconciles it.
    positions = strategy.get_open_positions().positions
    assert [p.position_type for p in positions] == [PositionType.PERP]
    assert positions[0].details["open_outcome_unknown"] is True
    intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
    assert [i.intent_type for i in intents] == [IntentType.PERP_CLOSE, IntentType.PERP_WITHDRAW]

    strategy.on_intent_executed(_intent(IntentType.PERP_OPEN), True, _open_result(executed="0.002"))
    assert "open_sent_at" not in strategy.state


def test_an_open_the_venue_proved_never_executed_releases_its_marker() -> None:
    strategy = _strategy(_settled())
    strategy.decide(_market())
    # The barrier is released with no on_intent_executed; funding has since dropped.
    assert strategy.decide(_market(rate_8h="0.00001")).intent_type == IntentType.HOLD
    assert "open_sent_at" not in strategy.state
    assert _teardown(strategy.state) == [IntentType.PERP_WITHDRAW]


def _no_position(error: str, order: dict[str, Any] | None = None) -> SimpleNamespace:
    return SimpleNamespace(success=False, error=error, extracted_data={"aster_order": order or {}})


def _already_flat_close() -> Any:
    response = aster_perps_pb2.AsterOrderResponse(
        success=True, already_flat=True, client_order_id="almc1", executed_qty="0", requested_qty="0", cum_quote="0"
    )
    order_request = {"symbol": "ETHUSDT", "is_long": False, "close_position": True, "client_order_id": "almc1"}
    return offchain_execution_result(AsterOrderHandler._to_result(response, order_request))


def test_a_close_the_venue_answers_already_flat_moves_on_to_the_spot() -> None:
    strategy = _strategy({**_carrying(), "phase": "exit_perp"})
    strategy.on_intent_executed(_intent(IntentType.PERP_CLOSE), True, _already_flat_close())
    assert strategy.state["phase"] == "exit_spot"
    assert strategy.decide(_market()).intent_type == IntentType.SWAP


@pytest.mark.parametrize(
    "result",
    [
        _no_position("close requested for a short but the ETHUSDT position is long"),
        _no_position("order status EXPIRED"),
        _no_position("position only partly closed: 0.001 closed, 0.001 still open", {"executed_qty": "0.001"}),
        None,
    ],
    ids=["wrong-side", "expired", "partly-filled", "no-result"],
)
def test_any_other_close_failure_keeps_retrying_the_close(result: Any) -> None:
    strategy = _strategy({**_carrying(), "phase": "exit_perp"})
    strategy.on_intent_executed(_intent(IntentType.PERP_CLOSE), False, result)
    assert strategy.state["phase"] == "exit_perp"


@pytest.mark.parametrize(
    ("result", "cleared"),
    [
        (
            ExecutionResult(
                success=False,
                phase=ExecutionPhase.VALIDATION,
                error="compile failed",
                submission_provenance=SubmissionProvenance.NOT_ATTEMPTED,
            ),
            True,
        ),
        (SimpleNamespace(error="execution raised"), False),
        (None, False),
    ],
    ids=["never-sent", "unproven", "no-result"],
)
def test_a_deposit_proven_to_move_nothing_releases_its_marker(result: Any, cleared: bool) -> None:
    strategy = _strategy()
    deposit = strategy.decide(_market())
    strategy.on_intent_executed(deposit, False, result)
    assert ("deposit_sent_at" not in strategy.state) is cleared
    assert _teardown(strategy.state) == ([] if cleared else [IntentType.PERP_WITHDRAW])


def _teardown(state: dict[str, Any], market: Any = None) -> list[IntentType]:
    return [i.intent_type for i in _strategy(state).generate_teardown_intents(TeardownMode.SOFT, market)]


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ({"phase": "deposit"}, []),
        ({"phase": "deposit", "deposit_sent_at": 1.0}, [IntentType.PERP_WITHDRAW]),
        ({"phase": "settle", "deposit_sent_at": 1.0}, [IntentType.PERP_WITHDRAW]),
        ({"phase": "idle", "deposit_sent_at": 1.0}, [IntentType.PERP_WITHDRAW]),
        ({"phase": "hedge_spot", "perp_qty": "0.002"}, [IntentType.PERP_CLOSE, IntentType.PERP_WITHDRAW]),
        (_carrying(), [IntentType.PERP_CLOSE, IntentType.SWAP, IntentType.PERP_WITHDRAW]),
        ({**_carrying(), "phase": "exit_perp"}, [IntentType.PERP_CLOSE, IntentType.SWAP, IntentType.PERP_WITHDRAW]),
        ({"phase": "exit_perp", "perp_qty": "0.001"}, [IntentType.PERP_CLOSE, IntentType.PERP_WITHDRAW]),
        ({"phase": "exit_spot", "spot_amount": "0.00199"}, [IntentType.SWAP, IntentType.PERP_WITHDRAW]),
    ],
    ids=[
        "never-deposited",
        "deposit-sent",
        "settling",
        "flat",
        "short-only",
        "carrying",
        "partial-close",
        "short-only-unwinding",
        "spot-only",
    ],
)
def test_teardown_follows_where_the_money_is(state: dict[str, Any], expected: list[IntentType]) -> None:
    assert _teardown(state) == expected


def test_teardown_sells_exactly_the_recorded_spot() -> None:
    intents = _strategy(_carrying()).generate_teardown_intents(TeardownMode.SOFT)
    sell = intents[1]
    assert sell.from_token == ETH and sell.to_token == USDT and sell.amount == Decimal("0.00199")
    assert intents[-1].amount == "all"


@pytest.mark.parametrize("phase", ["hedge_spot", "exit_spot"])
def test_both_spot_legs_pass_the_teardown_swap_chain_guard(phase: str) -> None:
    strategy = _strategy({**_carrying(), "phase": phase})
    swaps = [i for i in strategy.generate_teardown_intents(TeardownMode.SOFT) if i.intent_type == IntentType.SWAP]
    swaps.append(_strategy({"phase": "hedge_spot", "perp_qty": "0.002"}).decide(_market()))
    assert swaps and all(teardown_swap_chain_error(i, strategy, None) is None for i in swaps)


def test_a_spot_buy_with_an_unknown_outcome_is_sold_by_teardown_from_its_baseline() -> None:
    strategy = _strategy({"phase": "hedge_spot", "perp_qty": "0.002", "deposit_sent_at": 1.0})
    assert strategy.decide(_market(balance="0.0005")).intent_type == IntentType.SWAP
    assert "buy_sent_at" in strategy.get_persistent_state()

    # Teardown runs before the runner reconciles the buy.
    [perp, spot] = strategy.get_open_positions().positions
    assert spot.position_type == PositionType.TOKEN and spot.details["buy_outcome_unknown"] is True
    intents = strategy.generate_teardown_intents(TeardownMode.SOFT, _market(balance="0.0025"))
    assert [i.intent_type for i in intents] == [IntentType.PERP_CLOSE, IntentType.SWAP, IntentType.PERP_WITHDRAW]
    assert intents[1].from_token == ETH and intents[1].amount == Decimal("0.0020")
    # Nothing landed: nothing is sold.
    flat = strategy.generate_teardown_intents(TeardownMode.SOFT, _market(balance="0.0005"))
    assert [i.intent_type for i in flat] == [IntentType.PERP_CLOSE, IntentType.PERP_WITHDRAW]


def test_a_full_teardown_of_an_unresolved_buy_leaves_no_position_reported() -> None:
    strategy = _strategy({"phase": "hedge_spot", "perp_qty": "0.002", "deposit_sent_at": 1.0})
    strategy.decide(_market(balance="0.0005"))
    for intent in strategy.generate_teardown_intents(TeardownMode.SOFT, _market(balance="0.0025")):
        strategy.on_intent_executed(
            intent, True, _swap_result("5.1") if intent.intent_type == IntentType.SWAP else None
        )
    assert strategy.get_open_positions().positions == []


def test_a_close_with_an_unresolved_buy_moves_to_selling_the_gain() -> None:
    strategy = _strategy({"phase": "hedge_spot", "perp_qty": "0.002", "deposit_sent_at": 1.0})
    strategy.decide(_market(balance="0.0005"))
    close, sell, _ = strategy.generate_teardown_intents(TeardownMode.SOFT, _market(balance="0.0025"))
    strategy.on_intent_executed(close, True, None)
    # The teardown sale failed: the normal loop must still sell the gain.
    assert strategy.state["phase"] == "exit_spot"
    retry = strategy.decide(_market(balance="0.0025"))
    assert retry.intent_type == IntentType.SWAP and retry.amount == Decimal("0.0020")


def test_an_exit_with_an_unresolved_buy_that_added_nothing_returns_to_flat() -> None:
    strategy = _strategy({"phase": "exit_spot", "spot_balance_before": "0.0005", "buy_sent_at": 1.0})
    assert strategy.decide(_market(balance="0.0005")).intent_type == IntentType.HOLD
    assert strategy.state["phase"] == "idle" and "buy_sent_at" not in strategy.state
    unread = _strategy({"phase": "exit_spot", "spot_balance_before": "0.0005", "buy_sent_at": 1.0})
    unread.decide(_market(balance=None))
    assert unread.state["phase"] == "exit_spot" and "buy_sent_at" in unread.state


def test_a_spot_buy_resolves_its_marker_by_callback_or_proven_non_execution() -> None:
    strategy = _strategy({"phase": "hedge_spot", "perp_qty": "0.002"})
    buy = strategy.decide(_market(balance="0"))
    strategy.on_intent_executed(buy, True, _swap_result("0.00199", "5.14"))
    assert "buy_sent_at" not in strategy.state

    strategy = _strategy({"phase": "hedge_spot", "perp_qty": "0.002"})
    strategy.decide(_market(balance="0"))
    # Released with no callback: the next decision re-plans the buy from a fresh baseline.
    strategy.decide(_market(price=None))
    assert "buy_sent_at" not in strategy.state
    assert not any(p.position_type == PositionType.TOKEN for p in strategy.get_open_positions().positions)


def test_a_successful_close_clears_the_open_marker() -> None:
    strategy = _strategy({**_carrying(), "phase": "exit_perp", "open_sent_at": 1.0})
    strategy.on_intent_executed(_intent(IntentType.PERP_CLOSE), True, None)
    assert "open_sent_at" not in strategy.state


def test_teardown_with_unmeasured_spot_and_no_market_does_not_guess_a_sale() -> None:
    state = {**_carrying(), "spot_amount": None, "spot_unmeasured": True, "spot_balance_before": "0"}
    assert _teardown(state) == [IntentType.PERP_CLOSE, IntentType.PERP_WITHDRAW]
    assert _teardown({**state, "spot_balance_before": None}, _market(balance="0.002")) == [
        IntentType.PERP_CLOSE,
        IntentType.PERP_WITHDRAW,
    ]
    assert _teardown(state, _market(balance="0.002")) == [
        IntentType.PERP_CLOSE,
        IntentType.SWAP,
        IntentType.PERP_WITHDRAW,
    ]


def test_open_positions_report_the_short_and_the_spot_holding() -> None:
    positions = _strategy(_carrying()).get_open_positions().positions

    perp = next(p for p in positions if p.position_type == PositionType.PERP)
    assert perp.protocol == "aster_perps"
    assert perp.chain == "bsc"
    assert perp.details["market"] == "ETH/USD"
    assert perp.details["is_long"] is False
    assert perp.value_usd == Decimal("5.131")
    assert perp.direction == "SHORT"

    spot = next(p for p in positions if p.position_type == PositionType.TOKEN)
    assert spot.details["asset"] == ETH
    assert spot.details["amount"] == "0.00199"
    assert spot.value_usd == Decimal("5.14")


@pytest.mark.parametrize(
    ("phase", "types"),
    [
        ("deposit", []),
        ("idle", []),
        ("hedge_spot", [PositionType.PERP]),
        ("exit_spot", [PositionType.TOKEN]),
    ],
)
def test_open_positions_by_phase(phase: str, types: list[PositionType]) -> None:
    state = {**_carrying(), "phase": phase}
    if phase == "hedge_spot":
        state.pop("spot_amount")
    assert [p.position_type for p in _strategy(state).get_open_positions().positions] == types


def test_the_strategy_builds_from_the_runners_config_wrapper() -> None:
    """The CLI hands strategies a DictConfigWrapper, not a dict (a mainnet boot failed on `dict(wrapper)`)."""
    import json
    from pathlib import Path

    from almanak.framework.cli._strategy_config import DictConfigWrapper
    from strategies.incubating.aster_funding_carry.strategy import _config_mapping

    shipped = json.loads(
        (Path(__file__).resolve().parents[3] / "strategies/incubating/aster_funding_carry/config.json").read_text()
    )
    assert _config_mapping(DictConfigWrapper(shipped)) == shipped
    assert _config_mapping(shipped) == shipped
