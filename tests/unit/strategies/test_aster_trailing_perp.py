"""Money-path regression tests for the aster_trailing_perp demo.

Fill results are built with the real Aster execution handler and the runner's
off-chain result conversion, so the strategy is exercised against the exact
``extracted_data`` shape a mainnet fill produces, not a hand-written dict.
Emitted intents are compiled with the real Aster compiler.
"""

from __future__ import annotations

import importlib.util
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.base.compiler import PerpCompilerContext
from almanak.connectors.aster_perps.compiler import AsterPerpsCompiler
from almanak.connectors.aster_perps.execution import AsterOrderHandler
from almanak.connectors.aster_perps.proto import aster_perps_pb2
from almanak.framework.data import PriceUnavailableError
from almanak.framework.execution.offchain_venue import offchain_execution_result
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.teardown import PositionType, TeardownMode
from almanak.framework.teardown.completeness import check_intent_coverage

USDT = "0x55d398326f99059fF775485246999027B3197955"
_DEMO_DIR = Path(__file__).resolve().parents[3] / "strategies" / "incubating" / "aster_trailing_perp"


@pytest.fixture(scope="module")
def module():
    spec = importlib.util.spec_from_file_location("aster_trailing_perp_demo", _DEMO_DIR / "strategy.py")
    assert spec is not None and spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def _config() -> dict[str, Any]:
    return json.loads((_DEMO_DIR / "config.json").read_text(encoding="utf-8"))


def _make(module, **overrides: Any):
    cfg = {**_config(), **overrides}
    cls = module.AsterTrailingPerp
    with patch("almanak.framework.strategies.intent_strategy.IntentStrategy.__init__", return_value=None):
        strategy = cls.__new__(cls)
        strategy.get_config = lambda key, default=None: cfg.get(key, default)
        cls.__init__(strategy)
    strategy._deployment_id = "test-deployment"
    strategy._chain = "bsc"
    return strategy


def _market(price: str | Exception) -> MagicMock:
    market = MagicMock()
    if isinstance(price, Exception):
        market.price.side_effect = price
    else:
        market.price.return_value = Decimal(price)
    return market


def _order_request(*, is_long: bool = True, close: bool = False) -> dict[str, Any]:
    return {
        "symbol": "ETHUSDT",
        "is_long": is_long,
        "notional_usd": "" if close else "6",
        "close_position": close,
        "leverage": 0 if close else 3,
        "client_order_id": "almotest",
        "max_slippage": "0.01",
    }


def _venue_result(*, close: bool = False, is_long: bool = True, **fields: Any):
    """A fill as the runner hands it to ``on_intent_executed``."""
    response_fields = {
        "success": True,
        "order_id": 42,
        "client_order_id": "almotest",
        "status": "FILLED",
        "side": "BUY" if is_long != close else "SELL",
        "executed_qty": "0.003",
        "requested_qty": "0.003",
        "avg_price": "2000",
        "cum_quote": "6",
        "fee": "0.0021",
        "fee_asset": "USDT",
        "realized_pnl": "0",
    }
    response_fields.update(fields)
    response = aster_perps_pb2.AsterOrderResponse(**response_fields)
    clob = AsterOrderHandler._to_result(response, _order_request(is_long=is_long, close=close))
    return offchain_execution_result(clob)


def _in_position(strategy, *, side: str = "long", entry: str = "100", high_water: str = "0") -> None:
    strategy.state.update(
        phase="in_position",
        position_side=side,
        entry_price=entry,
        filled_qty="0.06",
        filled_notional_usd="6",
        high_water_pnl=high_water,
        close_retry=False,
    )


def _type(intent: Any) -> str:
    return intent.intent_type.value


def _ctx() -> PerpCompilerContext:
    return PerpCompilerContext(
        chain="bsc",
        wallet_address="0x" + "ab" * 20,
        rpc_url=None,
        rpc_timeout=10.0,
        permission_discovery=False,
        allow_placeholder_prices=False,
        token_resolver=None,
        gateway_client=None,
        price_oracle=None,
        cache={},
        services=MagicMock(),
        default_protocol="aster_perps",
        protocol="aster_perps",
    )


class TestConstruction:
    def test_shipped_config_constructs(self, module):
        strategy = _make(module)
        assert strategy.state["phase"] == "deposit"
        assert strategy.leverage == Decimal("3") and strategy.is_long is True
        assert strategy.reenter_after_close is False

    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            ({"size_usd": "4.99"}, "size_usd"),
            ({"leverage": "2.5"}, "whole number"),
            ({"leverage": "0"}, "whole number"),
            ({"leverage": "126"}, "whole number"),
            ({"deposit_usd": "1", "leverage": "5"}, "must cover size_usd"),
            ({"deposit_usd": "0"}, "deposit_usd"),
            ({"take_profit_pct": "0"}, "take_profit_pct"),
            ({"stop_loss_pct": "1"}, "stop_loss_pct"),
            ({"trail_pct": "-0.01"}, "trail_pct"),
            ({"max_slippage": "0"}, "max_slippage"),
            ({"trail_activation_pct": "0.02"}, "trailing stop can never engage"),
            ({"is_long": "maybe"}, "is_long"),
            ({"base_token_address": "ETH"}, "base_token_address"),
            ({"deposit_settle_seconds": -1}, "deposit_settle_seconds"),
        ],
    )
    def test_rejects_invalid_config(self, module, overrides, message):
        with pytest.raises(ValueError, match=message):
            _make(module, **overrides)

    def test_deposit_times_leverage_exactly_covering_size_is_accepted(self, module):
        assert _make(module, deposit_usd="2", leverage="3", size_usd="6").size_usd == Decimal("6")

    def test_string_false_direction_is_short_not_truthy(self, module):
        assert _make(module, is_long="false").is_long is False


class TestDepositSettleOpen:
    def test_full_sequence(self, module, monkeypatch):
        strategy = _make(module)
        clock = {"now": 1_000.0}
        monkeypatch.setattr(module.time, "time", lambda: clock["now"])

        deposit = strategy.decide(_market("2000"))
        assert _type(deposit) == "PERP_DEPOSIT"
        assert deposit.amount == Decimal("3") and deposit.asset == USDT and deposit.protocol == "aster_perps"
        assert strategy.state["deposit_sent_at"] == 1_000.0
        assert strategy.state["phase"] == "deposit"

        strategy.on_intent_executed(deposit, True, MagicMock(extracted_data={}))
        assert strategy.state["phase"] == "settle"

        clock["now"] += 89
        assert _type(strategy.decide(_market("2000"))) == "HOLD"

        clock["now"] += 1
        open_intent = strategy.decide(_market("2000"))
        assert _type(open_intent) == "PERP_OPEN"
        assert open_intent.market == "ETH/USD" and open_intent.collateral_token == USDT
        assert open_intent.size_usd == Decimal("6") and open_intent.leverage == Decimal("3")
        assert open_intent.collateral_amount == Decimal("2") and open_intent.is_long is True
        assert open_intent.max_slippage == Decimal("0.01") and open_intent.protocol == "aster_perps"
        assert strategy.state["phase"] == "settle"

        strategy.on_intent_executed(open_intent, True, _venue_result(avg_price="2001.5"))
        assert strategy.state["phase"] == "in_position"
        assert strategy.state["position_side"] == "long"
        assert strategy.state["entry_price"] == "2001.5"
        assert strategy.state["filled_qty"] == "0.003" and strategy.state["filled_notional_usd"] == "6"

    def test_unfilled_open_stays_flat_and_retries(self, module, monkeypatch):
        strategy = _make(module)
        strategy.state.update(phase="settle", deposited_at=0.0)
        monkeypatch.setattr(module.time, "time", lambda: 1_000.0)
        open_intent = strategy.decide(_market("2000"))
        unfilled = _venue_result(success=False, executed_qty="0", avg_price="", cum_quote="0")
        strategy.on_intent_executed(open_intent, False, unfilled)
        assert strategy.state["phase"] == "settle" and strategy.state["position_side"] is None
        assert _type(strategy.decide(_market("2000"))) == "PERP_OPEN"

    def test_failed_deposit_keeps_the_deposit_phase(self, module):
        strategy = _make(module)
        deposit = strategy.decide(_market("2000"))
        strategy.on_intent_executed(deposit, False, None)
        assert strategy.state["phase"] == "deposit"

    def test_emitted_intents_compile_with_the_aster_compiler(self, module):
        strategy = _make(module)
        strategy.state.update(phase="flat")
        compiler = AsterPerpsCompiler()
        opened = compiler.compile_perp_open(_ctx(), strategy.decide(_market("2000")))
        assert opened.status == CompilationStatus.SUCCESS, opened.error
        assert opened.action_bundle.metadata["order_request"]["leverage"] == 3

        _in_position(strategy)
        closed = compiler.compile_perp_close(_ctx(), strategy.decide(_market("103")))
        assert closed.status == CompilationStatus.SUCCESS, closed.error
        assert closed.action_bundle.metadata["order_request"]["close_position"] is True

        withdraw = strategy.generate_teardown_intents(TeardownMode.SOFT)[-1]
        withdrawn = compiler.compile_perp_withdraw(_ctx(), withdraw)
        assert withdrawn.status == CompilationStatus.SUCCESS, withdrawn.error
        assert withdrawn.action_bundle.metadata["withdraw_request"]["amount"] == "all"


class TestPartialOpen:
    def test_partial_fill_records_what_filled(self, module):
        strategy = _make(module)
        strategy.state.update(phase="flat")
        open_intent = strategy.decide(_market("2000"))
        partial = _venue_result(executed_qty="0.002", requested_qty="0.003", cum_quote="4.002", avg_price="2001")
        strategy.on_intent_executed(open_intent, True, partial)
        assert strategy.state["phase"] == "in_position"
        assert strategy.state["filled_qty"] == "0.002"
        assert strategy.state["filled_notional_usd"] == "4.002"
        assert strategy.state["entry_price"] == "2001"
        assert strategy.get_open_positions().positions[0].value_usd == Decimal("4.002")

    def test_entry_falls_back_to_quote_over_quantity(self, module):
        strategy = _make(module)
        strategy.state.update(phase="flat")
        open_intent = strategy.decide(_market("2000"))
        strategy.on_intent_executed(open_intent, True, _venue_result(avg_price="", cum_quote="6.003"))
        assert Decimal(strategy.state["entry_price"]) == Decimal("2001")

    def test_open_without_any_measured_entry_is_flattened(self, module):
        strategy = _make(module)
        strategy.state.update(phase="flat")
        open_intent = strategy.decide(_market("2000"))
        strategy.on_intent_executed(open_intent, True, MagicMock(extracted_data={}))
        assert strategy.state["phase"] == "in_position" and strategy.state["entry_price"] is None
        assert _type(strategy.decide(_market("2000"))) == "PERP_CLOSE"


class TestExits:
    """Shipped config: TP 2%, stop 2%, trail activates at +0.8% and gives back 0.6%."""

    @pytest.mark.parametrize(
        ("side", "price", "expected"),
        [
            ("long", "102", "PERP_CLOSE"),
            ("long", "98", "PERP_CLOSE"),
            ("long", "100.5", "HOLD"),
            ("long", "98.1", "HOLD"),
            ("short", "98", "PERP_CLOSE"),
            ("short", "102", "PERP_CLOSE"),
            ("short", "99.5", "HOLD"),
            ("short", "101.9", "HOLD"),
        ],
    )
    def test_take_profit_and_hard_stop(self, module, side, price, expected):
        strategy = _make(module)
        _in_position(strategy, side=side)
        intent = strategy.decide(_market(price))
        assert _type(intent) == expected
        if expected == "PERP_CLOSE":
            assert intent.is_long is (side == "long") and intent.size_usd is None

    def test_long_trailing_stop_ratchets_then_fires(self, module):
        strategy = _make(module)
        _in_position(strategy, side="long")
        assert _type(strategy.decide(_market("101.5"))) == "HOLD"
        assert Decimal(strategy.state["high_water_pnl"]) == Decimal("0.015")
        assert _type(strategy.decide(_market("101"))) == "HOLD"  # gave back 0.5% < 0.6%
        assert Decimal(strategy.state["high_water_pnl"]) == Decimal("0.015")
        assert _type(strategy.decide(_market("100.9"))) == "PERP_CLOSE"  # gave back 0.6%

    def test_short_trailing_stop_ratchets_then_fires(self, module):
        strategy = _make(module)
        _in_position(strategy, side="short")
        assert _type(strategy.decide(_market("98.5"))) == "HOLD"
        assert Decimal(strategy.state["high_water_pnl"]) == Decimal("0.015")
        close = strategy.decide(_market("99.1"))
        assert _type(close) == "PERP_CLOSE" and close.is_long is False

    def test_trailing_stop_is_inert_before_activation(self, module):
        strategy = _make(module)
        _in_position(strategy, side="long")
        assert _type(strategy.decide(_market("100.7"))) == "HOLD"  # peak +0.7% < 0.8% activation
        assert _type(strategy.decide(_market("99.5"))) == "HOLD"  # gave back 1.2%, trail not armed

    def test_stop_is_measured_from_the_fill_price(self, module):
        strategy = _make(module)
        strategy.state.update(phase="flat")
        open_intent = strategy.decide(_market("100"))
        fill = _venue_result(avg_price="99", cum_quote="5.94", executed_qty="0.06", requested_qty="0.06")
        strategy.on_intent_executed(open_intent, True, fill)
        assert _type(strategy.decide(_market("98"))) == "HOLD"  # -2% from the decide price, -1.01% from the fill
        assert _type(strategy.decide(_market("97.02"))) == "PERP_CLOSE"  # -2% from the 99 fill


class TestClose:
    def test_partial_close_keeps_the_position_and_retries(self, module):
        strategy = _make(module)
        _in_position(strategy, side="long")
        close = strategy.decide(_market("98"))
        partial = _venue_result(
            close=True,
            success=False,
            error="position only partly closed: 0.03 closed, 0.03 still open",
            executed_qty="0.03",
            requested_qty="0.06",
            cum_quote="2.94",
            avg_price="98",
        )
        strategy.on_intent_executed(close, False, partial)
        assert strategy.state["phase"] == "in_position"
        assert strategy.state["position_side"] == "long" and strategy.state["close_retry"] is True
        assert len(strategy.get_open_positions().positions) == 1

        # Retried even when the price recovered into the band or is unreadable.
        assert _type(strategy.decide(_market("100.5"))) == "PERP_CLOSE"
        retry = strategy.decide(_market(PriceUnavailableError("ETH", "down")))
        assert _type(retry) == "PERP_CLOSE"

        strategy.on_intent_executed(retry, True, _venue_result(close=True, realized_pnl="-0.06"))
        assert strategy.state["phase"] == "done" and strategy.state["position_side"] is None
        assert strategy.state["close_retry"] is False
        assert _type(strategy.decide(_market("100"))) == "HOLD"

    def test_close_with_reentry_returns_to_flat_and_reopens(self, module):
        strategy = _make(module, reenter_after_close=True)
        _in_position(strategy, side="long")
        close = strategy.decide(_market("102"))
        strategy.on_intent_executed(close, True, _venue_result(close=True))
        assert strategy.state["phase"] == "flat"
        assert _type(strategy.decide(_market("102"))) == "PERP_OPEN"


class TestStateDiscipline:
    def test_decide_never_commits_position_state(self, module):
        strategy = _make(module)
        _in_position(strategy, side="long")
        before = {k: v for k, v in strategy.state.items() if k != "high_water_pnl"}
        assert _type(strategy.decide(_market("102"))) == "PERP_CLOSE"
        after = {k: v for k, v in strategy.state.items() if k != "high_water_pnl"}
        assert before == after

        strategy.state.update(phase="flat", position_side=None, entry_price=None)
        snapshot = dict(strategy.state)
        assert _type(strategy.decide(_market("100"))) == "PERP_OPEN"
        # Only the pre-broadcast marker is written by decide(), never the position.
        assert strategy.state.pop("open_sent_at") is not None
        snapshot.pop("open_sent_at")
        assert strategy.state == snapshot

    def test_persistent_state_round_trips(self, module):
        strategy = _make(module)
        _in_position(strategy, side="short", entry="2000", high_water="0.004")
        restored = _make(module)
        restored.load_persistent_state(json.loads(json.dumps(strategy.get_persistent_state())))
        assert restored.state == strategy.state

    def test_unknown_persisted_phase_is_rejected(self, module):
        with pytest.raises(ValueError, match="unknown persisted phase"):
            _make(module).load_persistent_state({"phase": "close"})


class TestDataUnavailable:
    def test_open_holds_when_price_is_unavailable(self, module):
        strategy = _make(module)
        strategy.state.update(phase="flat")
        assert _type(strategy.decide(_market(PriceUnavailableError("ETH", "no oracle")))) == "HOLD"

    def test_manage_holds_when_price_is_unavailable(self, module):
        strategy = _make(module)
        _in_position(strategy)
        assert _type(strategy.decide(_market(PriceUnavailableError("ETH", "no oracle")))) == "HOLD"

    def test_unexpected_errors_propagate(self, module):
        strategy = _make(module)
        _in_position(strategy)
        with pytest.raises(RuntimeError):
            strategy.decide(_market(RuntimeError("bug")))


class TestTeardown:
    @pytest.mark.parametrize(
        ("state", "expected"),
        [
            ({"phase": "deposit"}, []),
            ({"phase": "deposit", "deposit_sent_at": 1.0}, [IntentType.PERP_WITHDRAW]),
            ({"phase": "settle"}, [IntentType.PERP_WITHDRAW]),
            ({"phase": "flat"}, [IntentType.PERP_WITHDRAW]),
            (
                {"phase": "in_position", "position_side": "long", "entry_price": "100"},
                [IntentType.PERP_CLOSE, IntentType.PERP_WITHDRAW],
            ),
            ({"phase": "done"}, [IntentType.PERP_WITHDRAW]),
            ({"phase": "withdrawn"}, []),
        ],
    )
    def test_teardown_follows_where_the_money_can_be(self, module, state, expected):
        strategy = _make(module)
        strategy.state.update(state)
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert [i.intent_type for i in intents] == expected
        if expected and expected[-1] == IntentType.PERP_WITHDRAW:
            assert intents[-1].amount == "all" and intents[-1].asset == USDT

    def test_deposit_sent_but_never_booked_is_withdrawn(self, module):
        strategy = _make(module)
        strategy.decide(_market("2000"))
        types = [i.intent_type for i in strategy.generate_teardown_intents(TeardownMode.SOFT)]
        assert types == [IntentType.PERP_WITHDRAW]

    def test_hard_teardown_widens_the_close_band(self, module):
        strategy = _make(module)
        _in_position(strategy, side="short")
        close = strategy.generate_teardown_intents(TeardownMode.HARD)[0]
        assert close.is_long is False and close.max_slippage == Decimal("0.02") and close.size_usd is None
        soft = strategy.generate_teardown_intents(TeardownMode.SOFT)[0]
        assert soft.max_slippage == Decimal("0.01")

    def test_withdraw_success_ends_the_lifecycle(self, module):
        strategy = _make(module)
        strategy.state.update(phase="done")
        withdraw = strategy.generate_teardown_intents(TeardownMode.SOFT)[0]
        strategy.on_intent_executed(withdraw, True, MagicMock(extracted_data={"aster_withdraw": {"amount": "2.89"}}))
        assert strategy.state["phase"] == "withdrawn"
        assert strategy.generate_teardown_intents(TeardownMode.SOFT) == []


class TestOpenPositions:
    def test_open_position_shape(self, module):
        strategy = _make(module)
        _in_position(strategy, side="short", entry="2000")
        [position] = strategy.get_open_positions().positions
        assert position.position_type == PositionType.PERP
        assert position.protocol == "aster_perps" and position.chain == "bsc"
        assert position.position_id == "aster:ETH/USD"
        assert position.value_usd == Decimal("6")
        assert position.details["market"] == "ETH/USD" and position.details["is_long"] is False
        assert "value_usd_unknown" not in position.details

    def test_unmeasured_notional_is_flagged(self, module):
        strategy = _make(module)
        _in_position(strategy)
        strategy.state["filled_notional_usd"] = None
        [position] = strategy.get_open_positions().positions
        assert position.value_usd == Decimal("6") and position.details["value_usd_unknown"] is True

    @pytest.mark.parametrize("phase", ["deposit", "settle", "flat", "done", "withdrawn"])
    def test_no_position_outside_in_position(self, module, phase):
        strategy = _make(module)
        strategy.state.update(phase=phase)
        assert strategy.get_open_positions().positions == []

    def test_tracks_only_the_margin_token(self, module):
        assert _make(module)._get_tracked_tokens() == ["0x55d398326f99059fF775485246999027B3197955"]


class TestReviewFixes:
    def test_a_withdraw_while_a_close_failed_keeps_the_position_tracked(self, module):
        strategy = _make(module)
        _in_position(strategy, side="long")
        withdraw = strategy.generate_teardown_intents(TeardownMode.SOFT)[-1]
        strategy.on_intent_executed(withdraw, True, SimpleNamespace(extracted_data={"aster_withdraw": {"amount": "1"}}))
        assert strategy.state["phase"] == "in_position"
        assert [p.position_id for p in strategy.get_open_positions().positions] == ["aster:ETH/USD"]

    def test_an_open_whose_outcome_never_arrived_is_still_reported_for_teardown(self, module):
        strategy = _make(module)
        strategy.state.update(phase="flat")
        open_intent = strategy.decide(_market("100"))
        [position] = strategy.get_open_positions().positions
        assert position.details["market"] == "ETH/USD" and position.details["open_outcome_unknown"]
        strategy.on_intent_executed(open_intent, False, SimpleNamespace(extracted_data={}))
        assert strategy.state["open_sent_at"] is None and strategy.get_open_positions().positions == []

    @pytest.mark.parametrize("phase", ["flat", "withdrawn"])
    def test_an_open_the_venue_proved_never_executed_releases_its_marker(self, module, phase):
        strategy = _make(module)
        strategy.state.update(phase="flat")
        strategy.decide(_market("100"))
        # The barrier is released with no on_intent_executed; the next decide finds no position.
        strategy.state["phase"] = phase
        strategy.decide(_market(PriceUnavailableError("ETH", "no oracle")))
        assert strategy.state["open_sent_at"] is None and strategy.get_open_positions().positions == []

    @pytest.mark.parametrize("mode", [TeardownMode.SOFT, TeardownMode.HARD])
    @pytest.mark.parametrize("is_long", [True, False])
    def test_a_teardown_while_an_open_is_held_closes_it_before_withdrawing(self, module, is_long, mode):
        strategy = _make(module, is_long=is_long)
        strategy.state.update(phase="flat")
        strategy.decide(_market("100"))
        intents = strategy.generate_teardown_intents(mode)
        assert [_type(i) for i in intents] == ["PERP_CLOSE", "PERP_WITHDRAW"]
        assert intents[0].is_long is is_long and intents[0].market == "ETH/USD"
        in_position = _make(module, is_long=is_long)
        _in_position(in_position, side="long" if is_long else "short")
        assert intents[0].max_slippage == in_position.generate_teardown_intents(mode)[0].max_slippage
        report = check_intent_coverage(strategy.get_open_positions(), intents)
        assert report.complete, report

    def test_a_flat_close_ends_the_held_open_and_a_later_teardown_has_nothing_to_close(self, module):
        strategy = _make(module)
        strategy.state.update(phase="flat")
        strategy.decide(_market("100"))
        close, withdraw = strategy.generate_teardown_intents(TeardownMode.SOFT)
        flat = _venue_result(close=True, executed_qty="0", requested_qty="0", cum_quote="0", avg_price="")
        strategy.on_intent_executed(close, True, flat)
        strategy.on_intent_executed(withdraw, True, SimpleNamespace(extracted_data={"aster_withdraw": {"amount": "3"}}))
        assert strategy.state["open_sent_at"] is None and strategy.get_open_positions().positions == []
        assert strategy.generate_teardown_intents(TeardownMode.SOFT) == []

    def test_an_open_replayed_after_its_teardown_close_is_closed_again_as_flat(self, module):
        strategy = _make(module)
        strategy.state.update(phase="flat")
        open_intent = strategy.decide(_market("100"))
        checkpoint = strategy.get_persistent_state()
        [close, _] = strategy.generate_teardown_intents(TeardownMode.SOFT)
        flat = _venue_result(close=True, executed_qty="0", requested_qty="0", cum_quote="0", avg_price="")
        strategy.on_intent_executed(close, True, flat)
        # Off-chain recovery restores the pre-open checkpoint and replays the open's success.
        strategy.load_persistent_state(checkpoint)
        strategy.on_intent_executed(open_intent, True, _venue_result())
        assert strategy.state["phase"] == "in_position"
        [close, _] = strategy.generate_teardown_intents(TeardownMode.SOFT)
        flat = _venue_result(close=True, executed_qty="0", requested_qty="0", cum_quote="0", avg_price="")
        strategy.on_intent_executed(close, True, flat)
        assert strategy.state["phase"] != "in_position" and strategy.get_open_positions().positions == []

    @pytest.mark.parametrize(("leverage", "stop"), [("40", "0.02"), ("3", "0.33")])
    def test_a_stop_beyond_liquidation_is_rejected(self, module, leverage, stop):
        with pytest.raises(ValueError, match="liquidation"):
            _make(module, leverage=leverage, stop_loss_pct=stop, deposit_usd="3", size_usd="6")
