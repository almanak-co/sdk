"""Economic and recovery conformance without lending or EVM requirements."""

from datetime import UTC, datetime, timedelta, timezone
from typing import Literal

import pytest
from pydantic import ValidationError

from almanak.backtest.engine import Journal, RunState, Schedule, Simulator, Transition
from almanak.backtest.types import Capability, ConnectorId, UnixTime
from almanak.caip import AssetType, ChainId
from almanak.effects import (
    AssetDelta,
    Balance,
    Effects,
    Portfolio,
    Position,
    PositionChange,
    PositionKey,
    WalletAsset,
)
from almanak.models import Chain, Model
from almanak.strategy_api import Memory, StrategyDecision
from almanak.triggers import IntervalSelection, IntervalTrigger

CHAIN = Chain(chain_id=ChainId("cosmos:cosmoshub-4"), environment="test")
INPUT = WalletAsset(account="fund", chain=CHAIN, asset=AssetType("cosmos:cosmoshub-4/slip44:118"))
OUTPUT = WalletAsset(account="fund", chain=CHAIN, asset=AssetType("cosmos:cosmoshub-4/denom:token"))


class Empty(Model):
    pass


class Loan(Model):
    kind: Literal["loan"] = "loan"
    shares: int


class Range(Model):
    kind: Literal["range"] = "range"
    liquidity: int


class Swap(Model):
    kind: Literal["swap"] = "swap"
    amount: int


class Receipt(Model):
    acquired: int


class Slot(Model):
    height: int
    observed_at: UnixTime


class SupplyExample(Model):
    kind: Literal["supply"] = "supply"


State = RunState[Empty, Swap, Receipt]


def initial(**overrides):
    schedule = Schedule(
        start=UnixTime(100),
        end=UnixTime(130),
        latency_seconds=2,
        end_policy="mark_open",
        cleanup_seconds=0,
    ).model_copy(update=overrides)
    return State(
        run_id="conformance-run",
        time=schedule.start,
        schedule=schedule,
        interval=IntervalSelection(configured=IntervalTrigger(kind="interval", seconds=10)),
        portfolio=Portfolio[Empty](
            balances=(Balance(key=INPUT, amount=10), Balance(key=OUTPUT, amount=0))
        ),
    )


class SwapModel:
    def __init__(self, start=None):
        self.capability = Capability(
            connector=ConnectorId("swap_example"),
            chain=CHAIN,
            model="example-v1",
            actions=("swap",),
            backtest_start_at=start,
        )
        self.calls = []

    def observe(self, state):
        return Slot(height=int(state.time) // 2, observed_at=state.time)

    def decide(self, state, observations, trigger):
        assert isinstance(observations, Slot)
        if state.memory.values.get("bought"):
            return StrategyDecision[Swap](memory=state.memory)
        return StrategyDecision[Swap](memory=state.memory, intent=Swap(amount=3))

    def route(self, intent):
        return self.capability, intent.kind

    def eligible_at(self, intent, time, latency):
        return UnixTime(time + latency)

    def execute(self, state, pending):
        self.calls.append(pending.identity)
        return Transition[Empty, Receipt](
            effects=Effects[Empty](
                assets=(
                    AssetDelta(key=INPUT, amount=-pending.intent.amount),
                    AssetDelta(key=OUTPUT, amount=pending.intent.amount * 2),
                )
            ),
            result=Receipt(acquired=pending.intent.amount * 2),
        )

    def deliver(self, state, result):
        return Memory(values={"bought": result.acquired})

    def closed(self, state, observations):
        return True


class MixedModel(SwapModel):
    def decide(self, state, observations, trigger):
        intent = None
        if not state.memory.values.get("bought"):
            intent = Swap(amount=3)
        elif state.time >= 115 and not state.portfolio.positions:
            intent = SupplyExample()
        return StrategyDecision[Swap | SupplyExample](memory=state.memory, intent=intent)

    def route(self, intent):
        if isinstance(intent, Swap):
            return super().route(intent)
        return Capability(
            connector=ConnectorId("lending_example"),
            chain=CHAIN,
            model="example-v1",
            actions=("supply",),
            backtest_start_at=UnixTime(115),
        ), intent.kind

    def execute(self, state, pending):
        if isinstance(pending.intent, Swap):
            result = super().execute(state, pending)
            return Transition[Loan, Receipt](
                effects=Effects[Loan](assets=result.effects.assets),
                result=result.result,
            )
        position = Position[Loan](
            key=PositionKey(
                connector=ConnectorId("lending_example"),
                chain=CHAIN,
                account="fund",
                reference="market",
            ),
            payload=Loan(shares=6),
        )
        return Transition[Loan, Receipt](
            effects=Effects[Loan](
                assets=(AssetDelta(key=OUTPUT, amount=-6),),
                positions=(PositionChange[Loan](before=None, after=position),),
            ),
            result=Receipt(acquired=0),
        )

    def deliver(self, state, result):
        return state.memory if result.acquired == 0 else super().deliver(state, result)


def test_one_run_combines_swap_and_later_lending_capability(tmp_path):
    state_type = RunState[Loan, Swap | SupplyExample, Receipt]
    original = initial()
    state = state_type(
        run_id=original.run_id,
        time=original.time,
        schedule=original.schedule,
        interval=original.interval,
        portfolio=Portfolio[Loan](balances=original.portfolio.balances),
    )
    journal = Journal(tmp_path / "mixed.sqlite", state_type)
    journal.create(state)
    result = Simulator(journal, MixedModel()).run()
    assert [item.pending.intent.kind for item in result.completed] == ["swap", "supply"]
    assert result.completed[0].pending.eligible_at < 115
    assert result.completed[1].pending.decision_time >= 115
    assert result.portfolio.balance(OUTPUT) == 0
    assert result.portfolio.positions[0].payload.shares == 6


@pytest.mark.parametrize("value", [True, False, -1, 1.5, "100", datetime(2026, 1, 1)])
def test_invalid_clock_values(value):
    with pytest.raises(ValueError):
        UnixTime(value)


def test_unix_time_reads_like_a_datetime_field():
    instant = datetime(2026, 1, 1, tzinfo=UTC)
    value = UnixTime(instant)
    assert value.timestamp() == int(instant.timestamp())
    assert type(value.timestamp()) is int
    assert value.datetime() == instant
    assert UnixTime(253402300799).datetime() == datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)


def test_timezones_and_inclusive_capability_bound():
    utc = UnixTime(datetime(2026, 1, 1, tzinfo=UTC))
    offset = UnixTime(datetime(2026, 1, 1, 8, tzinfo=timezone(timedelta(hours=8))))
    assert utc == offset
    cap = SwapModel(utc).capability
    restored = Capability.model_validate_json(cap.model_dump_json())
    assert restored == cap
    with pytest.raises(ValueError, match="backtest_before_start"):
        restored.require(UnixTime(utc - 1), "swap")
    restored.require(utc, "swap")
    restored.require(UnixTime(utc + 1))
    with pytest.raises(ValueError, match="unsupported backtest action"):
        restored.require(utc, "borrow")
    SwapModel().capability.require(UnixTime(0))


def test_pair_dates_do_not_merge():
    first = SwapModel(UnixTime(100)).capability
    second_chain = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    second = Capability(
        connector=first.connector,
        chain=second_chain,
        model="example-v1",
        actions=("swap",),
        backtest_start_at=UnixTime(200),
    )
    first.require(UnixTime(150))
    with pytest.raises(ValueError):
        second.require(UnixTime(150))


def test_wallet_and_position_scope_and_atomicity():
    before = initial().portfolio
    with pytest.raises(ValueError, match="insufficient"):
        before.apply(
            Effects[Empty](
                assets=(
                    AssetDelta(key=INPUT, amount=-3),
                    AssetDelta(key=INPUT, amount=-8),
                )
            )
        )
    assert before.balance(INPUT) == 10
    other = WalletAsset(account="other", chain=CHAIN, asset=INPUT.asset)
    with pytest.raises(ValueError, match="unbound"):
        before.apply(Effects[Empty](assets=(AssetDelta(key=other, amount=1),)))
    with pytest.raises(ValueError, match="explicitly initialized"):
        before.balance(other)
    with pytest.raises(ValidationError, match="another chain"):
        WalletAsset(account="fund", chain=CHAIN, asset=AssetType("eip155:1/slip44:60"))
    with pytest.raises(ValidationError, match="duplicate"):
        Portfolio[Empty](balances=(*before.balances, before.balances[0]))


def test_mixed_position_types_keep_connector_payloads():
    loan_key = PositionKey(
        connector=ConnectorId("lending"), chain=CHAIN, account="fund", reference="loan"
    )
    lp_key = PositionKey(
        connector=ConnectorId("lp"), chain=CHAIN, account="fund", reference="range"
    )
    loan = Position[Loan | Range](key=loan_key, payload=Loan(shares=7))
    lp = Position[Loan | Range](key=lp_key, payload=Range(liquidity=12))
    portfolio = Portfolio[Loan | Range](balances=(), positions=(loan, lp))
    restored = Portfolio[Loan | Range].model_validate_json(portfolio.model_dump_json())
    assert restored == portfolio
    assert isinstance(restored.positions[1].payload, Range)
    after = restored.apply(
        Effects[Loan | Range](positions=(PositionChange[Loan | Range](before=loan, after=None),))
    )
    assert after.positions == (lp,)
    with pytest.raises(ValueError, match="conflict"):
        after.apply(
            Effects[Loan | Range](
                positions=(PositionChange[Loan | Range](before=loan, after=None),)
            )
        )


@pytest.mark.parametrize(
    "fault_stage",
    ["decision", "pending_commit", "effects", "effects_commit", "outcome", "delivery_commit"],
)
def test_interrupt_and_resume_preserve_effects_and_delivery(tmp_path, fault_stage):
    journal = Journal(tmp_path / "state.sqlite", State)
    journal.create(initial())
    model = SwapModel()

    def fail(stage):
        if stage == fault_stage:
            raise SystemExit("process interrupted")

    with pytest.raises(SystemExit):
        Simulator(journal, model, fault=fail).run()
    result = Simulator(journal, model).run()
    assert result.portfolio.balance(INPUT) == 7
    assert result.portfolio.balance(OUTPUT) == 6
    assert len(result.completed) == result.delivered == 1
    assert result.memory.values == {"bought": 6}
    assert result.completed[0].pending.eligible_at == 102
    assert result.mode == "finished"


def test_uneligible_end_action_remains_unfilled(tmp_path):
    journal = Journal(tmp_path / "state.sqlite", State)
    journal.create(initial(end=UnixTime(101)))
    result = Simulator(journal, SwapModel()).run()
    assert result.pending is not None
    assert result.pending.eligible_at == 102
    assert result.portfolio.balance(INPUT) == 10
    assert result.completed == ()


def test_capability_rejection_does_not_spend(tmp_path):
    journal = Journal(tmp_path / "state.sqlite", State)
    journal.create(initial())
    with pytest.raises(ValueError, match="backtest_before_start"):
        Simulator(journal, SwapModel(UnixTime(101))).run()
    assert journal.load() == initial()


def test_journal_ownership_and_compare_and_swap(tmp_path):
    journal = Journal(tmp_path / "state.sqlite", State)
    with pytest.raises(ValueError, match="missing"):
        journal.load()
    before = initial()
    journal.create(before)
    with pytest.raises(ValueError, match="already exists"):
        journal.create(before)
    with journal.coordinator(), pytest.raises(RuntimeError, match="coordinator"):
        with journal.coordinator():
            pass
    after = before.model_copy(update={"revision": 1})
    journal.save(before, after)
    with pytest.raises(RuntimeError, match="changed"):
        journal.save(before, after)
