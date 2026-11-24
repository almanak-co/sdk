"""The modeled bridge: the whole fee ceiling, a stated latency and no history.

The scripted cases run the real simulator over the real model with a decision plan in
place of the author hook. The reference cases run the shipped reference's own hooks in
the sandbox through the backtest's create, resume and reproduce paths.
"""

import json
from types import SimpleNamespace

import httpx
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.types import AvailabilityError
from almanak.connectors.across.authority import declared
from almanak.connectors.across.economic_binding import AcrossEconomic
from almanak.connectors.across.economic_config import FILL_WINDOW_SECONDS
from almanak.connectors.across.economic_model import (
    ASSUMPTIONS,
    MODEL,
    EconomicModel,
    modeled_transfer,
)
from almanak.connectors.across.economic_prepare import Book, prepare, validate_sources
from almanak.connectors.across.economic_state import ModeledArrival, State
from almanak.connectors.across.quote import fee_ceiling
from almanak.connectors.across.settlement_types import ArrivalResult
from almanak.connectors.across.state import Decision
from almanak.connectors.across.support import BACKTEST_STARTS
from almanak.effects import WalletAsset
from almanak.worker_session import preloaded_workers
from tests.across_economic_support import (
    ETH,
    FUNDED,
    GAS,
    SENT,
    START,
    author,
    inputs,
)
from tests.across_economic_support import settings as backtest_settings
from tests.across_support import ARBITRUM, BASE, BASE_USDC, USDC

SOURCE = WalletAsset(account="arb", chain=ARBITRUM, asset=USDC)
DESTINATION = WalletAsset(account="base", chain=BASE, asset=BASE_USDC)
NATIVE = WalletAsset(account="arb", chain=ARBITRUM, asset=ETH)


class Scripted(EconomicModel):
    """The model with a decision plan: seconds after the start to the amount to bridge."""

    def __init__(self, config, settings, strategy, plan):
        super().__init__(config, settings, strategy, None, authority=declared(config))
        self.plan, self.seen, self.outcomes = plan, [], []

    def decide(self, state, observations, trigger):
        self.seen.append((state.time, observations, self.owned(state)))
        action = self.plan.get(state.time - START)
        if action is None or state.mode != "running":
            return Decision(memory=state.memory)
        intent = self.config.revoke("move") if action == "revoke" else None
        return Decision(memory=state.memory, intent=intent or self.config.bridge("move", action))

    def deliver(self, state, result):
        self.outcomes.append(result)
        return state.memory


def scripted(tmp_path, plan, *, run_id="0" * 32, name="state.sqlite", **changes):
    given_inputs = inputs(tmp_path, **changes)
    model = Scripted(given_inputs.config, given_inputs.settings, given_inputs.strategy, plan)
    initial = AcrossEconomic.initial(
        given_inputs.settings, given_inputs.config, given_inputs.interval
    )
    journal = Journal(tmp_path / name, State)
    journal.create(initial.model_copy(update={"run_id": run_id}))
    return journal, model


def flights(state):
    return tuple(f for p in state.portfolio.positions for f in p.payload.flights)


def balance(state, key):
    return state.portfolio.balance(key)


def test_a_bridge_debits_at_execution_and_credits_at_the_stated_latency(tmp_path):
    journal, model = scripted(tmp_path, {0: SENT}, latency=20)
    result = Simulator(journal, model).run()
    (flight,) = flights(result)
    fee = fee_ceiling(SENT, 10)
    assert (flight.sent_at, flight.arrives_at) == (START + 1, START + 21)
    assert flight.transfer.offered.amount == SENT - fee == 99_900_000
    assert flight.transfer.resolution.received == flight.transfer.offered
    # The fill deadline is the deposit's second plus the fill window, not the arrival.
    assert int(flight.transfer.fill_deadline.timestamp()) == START + 1 + FILL_WINDOW_SECONDS
    assert balance(result, SOURCE) == FUNDED - SENT
    assert balance(result, DESTINATION) == SENT - fee
    (event,) = result.events
    assert event.time == START + 21
    (arrived,) = event.transition.result.arrivals
    assert isinstance(arrived, ArrivalResult) and isinstance(arrived, ModeledArrival)
    assert arrived.measurement_status == "modeled"
    assert arrived.modeled_fee.amount == fee and arrived.received == arrived.offered
    assert model.outcomes[0].transfer == flight.transfer.model_copy(update={"resolution": None})
    assert model.outcomes[1] == arrived


def test_value_in_flight_is_in_neither_balance_and_states_no_received_amount(tmp_path):
    journal, model = scripted(tmp_path, {0: SENT}, latency=25)
    Simulator(journal, model).run()
    during = [(obs, owned) for time, obs, owned in model.seen if START + 1 <= time < START + 26]
    assert during
    for (snapshot,), (owned,) in during:
        assert snapshot.wallet_balance == FUNDED - SENT
        assert owned.inventory.amount == FUNDED - SENT
        (transfer,) = owned.in_flight
        assert transfer.resolution is None
        assert (transfer.sent.amount, transfer.offered.amount) == (SENT, 99_900_000)
        # Nothing a decision sees states when the transfer will arrive.
        assert int(transfer.fill_deadline.timestamp()) == START + 1 + FILL_WINDOW_SECONDS
    after = [owned for time, _, (owned,) in model.seen if time >= START + 26]
    assert after and all(not owned.in_flight for owned in after)


def test_in_flight_value_cannot_be_spent(tmp_path):
    journal, model = scripted(tmp_path, {0: 200_000_000, 10: 100_000_000}, latency=40)
    result = Simulator(journal, model).run()
    first, second = (c.transition.result for c in result.completed)
    assert first.success
    assert not second.success and "does not hold" in second.error
    assert second.status == "failed" and second.transfer is None
    assert balance(result, SOURCE) == FUNDED - 200_000_000
    assert len(flights(result)) == 1


def test_a_run_that_ends_with_a_transfer_in_flight_invents_no_arrival(tmp_path):
    journal, model = scripted(tmp_path, {50: SENT}, latency=30)
    result = Simulator(journal, model).run()
    assert result.mode == "finished" and not result.events
    (flight,) = flights(result)
    assert flight.transfer.resolution is None and flight.arrives_at > result.time
    assert balance(result, DESTINATION) == 0
    report = AcrossEconomic.report(result, model)
    (transfer,) = report["positions"][0]["transfers"]
    assert transfer["state"] == "in_flight"
    assert transfer["received"] is None and transfer["arrived_at"] is None
    assert transfer["sent"]["amount"] == str(SENT)
    assert transfer["measurement_status"] == "modeled"
    assert report["closed"] is False
    position = report["positions"][0]
    # No aggregate reads as a received amount while nothing has arrived.
    assert "received_by_arrived_transfers" not in position and "received" not in position
    assert (position["in_flight"], position["arrived"]) == (1, 0)
    assert position["sent"]["amount"] == position["in_flight_sent"]["amount"] == str(SENT)


def test_a_bridge_eligible_only_at_the_end_is_cancelled_not_filled(tmp_path):
    journal, model = scripted(tmp_path, {50: SENT}, window=51, latency=5)
    result = Simulator(journal, model).run()
    assert not result.completed and not flights(result)
    assert balance(result, SOURCE) == FUNDED


def test_teardown_waits_for_the_arrival_and_sends_nothing(tmp_path):
    journal, model = scripted(
        tmp_path, {50: SENT, 70: SENT}, latency=30, end_policy="teardown", cleanup=60
    )
    result = Simulator(journal, model).run()
    assert result.mode == "finished"
    assert [c.transition.result.success for c in result.completed] == [True]
    assert flights(result)[0].transfer.resolution is not None
    assert model.closed(result, model.observe(result))


def test_teardown_refuses_to_close_over_a_transfer_still_in_flight(tmp_path):
    journal, model = scripted(tmp_path, {50: SENT}, latency=300, end_policy="teardown", cleanup=60)
    with pytest.raises(ValueError, match="unresolved obligations"):
        Simulator(journal, model).run()
    assert balance(journal.load(), DESTINATION) == 0


def test_the_model_keeps_no_approval_and_serves_no_revoke(tmp_path):
    journal, model = scripted(tmp_path, {0: "revoke"})
    with pytest.raises(AvailabilityError):
        Simulator(journal, model).run()
    snapshot = model.observe(journal.load())[0]
    assert snapshot.allowance == 0 and snapshot.simulated is True


def test_a_fixed_native_cost_is_charged_on_the_source_chain_only(tmp_path):
    journal, model = scripted(
        tmp_path, {0: SENT}, costs={"kind": "fixed_native", "bridge": 7 * 10**14}
    )
    result = Simulator(journal, model).run()
    assert balance(result, NATIVE) == GAS - 7 * 10**14
    report = AcrossEconomic.report(result, model)
    assert report["gas_payers"][0]["measurement_status"] != "excluded"
    assert [b.key for b in result.portfolio.balances if b.key.chain == BASE] == [DESTINATION]


def test_a_fee_ceiling_that_leaves_nothing_refuses_the_bridge():
    target = author().target("move")
    with pytest.raises(ValueError, match="nothing to receive"):
        modeled_transfer(target, 1, 10_000, "modeled:x", START + 1)
    assert modeled_transfer(target, 9_999, 10, "modeled:x", START + 1).offered.amount == 9_990


def test_the_latency_is_required_and_the_model_is_named(tmp_path):
    wire = json.loads(backtest_settings().model_dump_json())
    for missing in ("data", "bridge_model"):
        with pytest.raises(ValidationError):
            AcrossEconomic.settings_type.model_validate_json(
                json.dumps({k: v for k, v in wire.items() if k != missing})
            )
    for latency in (0, -5):
        with pytest.raises(ValidationError):
            backtest_settings(latency=latency)
    # The model has no refund, so the arrival must fall inside the fill window.
    backtest_settings(latency=FILL_WINDOW_SECONDS - 1)
    for latency in (FILL_WINDOW_SECONDS, FILL_WINDOW_SECONDS + 1):
        with pytest.raises(ValidationError, match="below the modeled fill window"):
            backtest_settings(latency=latency)


def test_a_window_before_the_declared_start_is_refused(tmp_path, monkeypatch):
    from tests import across_economic_support as support

    early = int(BACKTEST_STARTS[ARBITRUM.chain_id].timestamp()) - 3600
    monkeypatch.setattr(support, "START", early)
    given_inputs = inputs(tmp_path)
    with pytest.raises(AvailabilityError):
        validate_sources(given_inputs.args, given_inputs.author, given_inputs.settings)


def test_a_resumed_run_equals_an_uninterrupted_one(tmp_path):
    plan = {0: SENT, 20: 50_000_000}
    whole, model = scripted(tmp_path, plan, name="whole.sqlite", latency=15)
    expected = Simulator(whole, model).run()

    journal, first = scripted(tmp_path, plan, name="cut.sqlite", latency=15)
    simulator = Simulator(journal, first)
    with journal.coordinator():
        while not flights(journal.load()):
            simulator.tick()
    assert flights(journal.load())[0].transfer.resolution is None
    _, replacement = scripted(tmp_path, plan, name="unused.sqlite", latency=15)
    resumed = Simulator(journal, replacement).run()
    assert resumed == expected
    assert len(resumed.events) == 2


@settings(max_examples=40, deadline=None)
@given(
    plan=st.dictionaries(
        st.integers(0, 5).map(lambda step: step * 10),
        st.integers(1, 120_000_000),
        max_size=6,
    ),
    latency=st.integers(1, 45),
    max_fee_bps=st.integers(1, 500),
    execution_latency=st.integers(1, 9),
)
def test_random_bridge_sequences_conserve_value(
    tmp_path_factory, plan, latency, max_fee_bps, execution_latency
):
    path = tmp_path_factory.mktemp("sequence")
    journal, model = scripted(
        path,
        plan,
        parameters=author(max_fee_bps=max_fee_bps),
        latency=latency,
        execution_latency=execution_latency,
    )
    result = Simulator(journal, model).run()
    again, replay = scripted(
        path,
        plan,
        name="again.sqlite",
        parameters=author(max_fee_bps=max_fee_bps),
        latency=latency,
        execution_latency=execution_latency,
    )
    assert Simulator(again, replay).run() == result

    sent = sum(f.transfer.sent.amount for f in flights(result))
    arrived = [f for f in flights(result) if f.transfer.resolution is not None]
    open_flights = [f for f in flights(result) if f.transfer.resolution is None]
    assert balance(result, SOURCE) == FUNDED - sent >= 0
    assert balance(result, DESTINATION) == sum(f.transfer.offered.amount for f in arrived)
    assert balance(result, NATIVE) == GAS
    for flight in flights(result):
        amount = flight.transfer.sent.amount
        assert flight.transfer.offered.amount == amount - amount * max_fee_bps // 10_000
        assert flight.arrives_at == flight.sent_at + latency
    successes = [c for c in result.completed if c.transition.result.success]
    assert len(successes) == len(flights(result))
    for done in successes:
        assert done.pending.eligible_at == done.pending.decision_time + execution_latency
    assert {e.time for e in result.events} == {f.arrives_at for f in arrived}
    assert all(f.arrives_at > result.time for f in open_flights)
    failed = [c for c in result.completed if not c.transition.result.success]
    assert all(c.transition.effects.assets == () for c in failed)


@pytest.fixture
def offline(monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("a modeled bridge backtest asks no provider and no venue")

    monkeypatch.setattr(httpx.Client, "send", forbid)


def test_the_reference_bridges_once_and_reports_every_amount_as_modeled(tmp_path, offline):
    given_inputs = inputs(tmp_path, end_policy="teardown", cleanup=60, usdc=SENT)
    with preloaded_workers():
        report = AcrossEconomic.create(
            given_inputs.args, given_inputs.author, given_inputs.settings
        )
    assert report["mode"] == "finished" and report["closed"] is True
    assert report["model"] == MODEL and report["provider_requests_during_execution"] == 0
    assert (report["completed_intents"], report["delivered_events"]) == (1, 1)
    assert tuple(report["assumptions"]) == ASSUMPTIONS
    assert report["bridge_model"] == {
        "kind": "full_fee_ceiling_fixed_latency",
        "arrival_latency_seconds": 20,
        "fee": "the market's whole max_fee_bps of the amount sent",
        "refunds": "not modeled",
    }
    (transfer,) = report["positions"][0]["transfers"]
    assert transfer["state"] == "arrived"
    assert transfer["received"]["amount"] == transfer["offered"]["amount"] == "99900000"
    assert transfer["modeled_fee"]["amount"] == "100000"
    assert transfer["arrived_at"] - transfer["sent_at"] == 20
    assert transfer["quote_source"] == "modeled_full_fee_ceiling"
    amounts = {(w["key"]["account"], w["key"]["asset"]): w["amount"] for w in report["wallets"]}
    assert amounts[("arb", str(USDC))] == 0 and amounts[("base", str(BASE_USDC))] == 99_900_000

    run = SimpleNamespace(run=given_inputs.args.output)
    with preloaded_workers():
        resumed = AcrossEconomic.resume(run)
        assert AcrossEconomic.reproduce(run)["reproduced"] is True
    assert resumed["positions"] == report["positions"]
    assert resumed["wallets"] == report["wallets"]


def test_the_dataset_is_the_frozen_model_statement_and_serves_no_other_latency(tmp_path, offline):
    given_inputs = inputs(tmp_path)
    args = SimpleNamespace(**{**vars(given_inputs.args), "output": tmp_path / "dataset"})
    assert prepare(args)["cache_state"] == "modeled_without_acquisition"
    assert prepare(args)["cache_state"] == "completed_dataset"
    descriptor = json.loads((tmp_path / "dataset/dataset.json").read_text())
    assert descriptor["model"] == MODEL and descriptor["inputs"] == "none"
    Book(tmp_path / "dataset", given_inputs.config, given_inputs.settings)
    with pytest.raises(ValueError, match="frozen routes and settings"):
        Book(tmp_path / "dataset", given_inputs.config, backtest_settings(latency=21))


class Insistent(Scripted):
    """A strategy that bridges once while running and asks once more in teardown."""

    def decide(self, state, observations, trigger):
        asked = {c.pending.trigger.kind == "teardown" for c in state.completed}
        if (state.mode == "teardown") in asked:
            return Decision(memory=state.memory)
        return Decision(memory=state.memory, intent=self.config.bridge("move", SENT))


def insistent(tmp_path, **changes):
    given_inputs = inputs(tmp_path, **changes)
    model = Insistent(given_inputs.config, given_inputs.settings, given_inputs.strategy, {})
    journal = Journal(tmp_path / "insistent.sqlite", State)
    journal.create(
        AcrossEconomic.initial(given_inputs.settings, given_inputs.config, given_inputs.interval)
    )
    return journal, model


def test_a_bridge_decided_in_teardown_is_refused_and_moves_nothing(tmp_path):
    journal, model = insistent(tmp_path, window=5, latency=5, end_policy="teardown", cleanup=30)
    result = Simulator(journal, model).run()
    first, second = (c.transition.result for c in result.completed)
    assert first.success and not second.success
    assert "teardown sends nothing" in second.error
    assert result.mode == "finished"
    assert len(flights(result)) == 1 and balance(result, SOURCE) == FUNDED - SENT


def test_a_payer_below_the_native_minimum_bridges_nothing(tmp_path):
    parameters = author(minimum_native_balance=GAS + 1)
    journal, model = scripted(tmp_path, {0: SENT}, parameters=parameters)
    result = Simulator(journal, model).run()
    (failed,) = (c.transition.result for c in result.completed)
    assert not failed.success and "below the configured native minimum" in failed.error
    assert balance(result, SOURCE) == FUNDED and not flights(result)
    enough, funded = scripted(
        tmp_path, {0: SENT}, name="enough.sqlite", parameters=author(minimum_native_balance=GAS)
    )
    assert Simulator(enough, funded).run().completed[0].transition.result.success


def test_an_intent_must_name_its_configured_route_and_a_retained_capability(tmp_path):
    _, model = scripted(tmp_path, {})
    intent = model.config.bridge("move", SENT)
    capability, action = model.route(intent)
    assert (capability.chain, action) == (ARBITRUM, "bridge")
    elsewhere = author(max_fee_bps=11).bridge("move", SENT)
    with pytest.raises(ValueError, match="differs from its configured route"):
        model.route(elsewhere)
    model.authority = declared(author(max_fee_bps=11))
    with pytest.raises(ValueError, match="exactly one retained route capability"):
        model.route(intent)


def test_a_loaded_state_must_match_the_frozen_schedule_and_routes(tmp_path):
    journal, model = scripted(tmp_path, {0: SENT})
    state = journal.load()
    model.validate_state(state)
    later = state.schedule.model_copy(update={"end": state.schedule.end + 1})
    with pytest.raises(ValueError, match="frozen schedule"):
        model.observe(state.model_copy(update={"schedule": later}))
    _, other = scripted(tmp_path, {}, name="other.sqlite", parameters=author(max_fee_bps=11))
    with pytest.raises(ValueError, match="differs from its configured routes"):
        other.observe(state)
    held = state.portfolio.positions[0]
    moved = held.model_copy(update={"key": held.key.model_copy(update={"reference": "elsewhere"})})
    portfolio = state.portfolio.model_copy(update={"positions": (moved,)})
    with pytest.raises(ValueError, match="position key differs"):
        model.observe(state.model_copy(update={"portfolio": portfolio}))


def test_the_position_report_sums_arrived_and_open_transfers_apart(tmp_path):
    journal, model = scripted(tmp_path, {0: SENT, 50: 50_000_000}, latency=20)
    result = Simulator(journal, model).run()
    position = AcrossEconomic.report(result, model)["positions"][0]
    assert (position["in_flight"], position["arrived"]) == (1, 1)
    assert position["sent"]["amount"] == str(SENT + 50_000_000)
    assert position["in_flight_sent"]["amount"] == "50000000"
    received = position["received_by_arrived_transfers"]
    assert received == {"asset": str(BASE_USDC), "amount": "99900000", "unit": "base_units"}
    assert "received" not in position
    assert [t["state"] for t in position["transfers"]] == ["arrived", "in_flight"]


def test_a_zero_fee_ceiling_is_refused_before_anything_is_modeled(tmp_path):
    free = author(max_fee_bps=0)
    with pytest.raises(ValueError, match="a backtest refuses max_fee_bps 0"):
        scripted(tmp_path, {0: SENT}, parameters=free)
    given_inputs = inputs(tmp_path, parameters=free)
    with pytest.raises(ValueError, match="a backtest refuses max_fee_bps 0"):
        validate_sources(given_inputs.args, given_inputs.author, given_inputs.settings)
    assert any("every deposit is admitted" in line for line in ASSUMPTIONS)


def test_both_arrival_types_are_author_facing(tmp_path):
    from almanak.connectors.across import api

    assert issubclass(api.ModeledArrival, api.ArrivalResult)
    assert issubclass(api.MeasuredArrival, api.ArrivalResult)
    measured_only = set(api.MeasuredArrival.model_fields) - set(api.ModeledArrival.model_fields)
    assert measured_only == {"identity", "fill_type", "block", "tx_hash", "accounting_issues"}
