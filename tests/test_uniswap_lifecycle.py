"""Independent checks of captured protocol receipts and the actual author sources."""

import copy
from types import SimpleNamespace

import pytest

from almanak.connectors.composition import connector_for
from almanak.connectors.uniswap_v4 import (
    lifecycle,
    planning,
    receipts,
    reconciliation,
    validation,
)
from almanak.connectors.uniswap_v4.reporting import report
from almanak.connectors.uniswap_v4.state import Burn, Collect, DecisionInput, Mint, OutcomeInput
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.dx_replay import assert_intent, memory_values
from tests.retained_scheduling import synthetic_event
from tests.uniswap_support import ROOT, author, live_state, scope_only_json


@pytest.fixture(params=["trading", "lp"])
def recorded_exchange(request):
    return live_state(request.param)


def author_module(name):
    path = ROOT / f"strategies/uniswap_v4_{name}/strategy.py"
    namespace = {}
    exec(compile(path.read_text(), str(path), "exec"), namespace)
    return SimpleNamespace(**namespace)


def test_retained_calls_receipts_inventory_and_gas_reconcile(recorded_exchange):
    state = recorded_exchange
    for outcome in state.outcomes:
        assert planning.call(state.config, outcome.operation) == outcome.call
        deltas, nft, _ = receipts.movement(outcome.receipt, outcome.operation)
        assert deltas == (outcome.delta0, outcome.delta1)
        growth = outcome.after.owned.position.fee_growth if outcome.after.owned.position else None
        owned = reconciliation.owned_after(outcome, deltas, nft, growth, outcome.success)
        assert owned == outcome.after.owned
        assert (
            reconciliation.validate_balances(
                outcome, outcome.after, deltas, outcome.gas_paid.amount, True
            )
            == ()
        )
    lifecycle.validate_closure(state, state.closure)
    from tests.strategy_fork.uniswap_assertions import assert_closed

    assert_closed(None, state)
    result = report(state)
    assert result["transaction_accounting_complete"]
    assert result["positions"][0]["fund_value_excluding_gas"]["complete"]
    assert result["gas_payers"][0]["submitter"] != result["positions"][0]["target"]["fund"]


def test_actual_author_replays_each_retained_decision_and_timer(recorded_exchange):
    state = recorded_exchange
    name = state.config.allocations[0].name
    source = author_module(name)
    source.validate_config(state.config)
    assert source.capabilities(state.config) == state.capabilities
    memory = source.State()
    for outcome in state.outcomes:
        observations = outcome.decision.observations
        context = DecisionInput(
            config=state.config,
            memory=memory,
            owned=tuple(o.owned for o in observations),
            observations=observations,
            trigger=synthetic_event(),
        )
        assert_intent(state, source.decide(context).intent, outcome)
        memory = source.on_outcome(
            OutcomeInput(config=state.config, memory=memory, result=outcome.strategy_result())
        )
    assert memory_values(memory) == state.memory.values
    context = context.model_copy(
        update={
            "memory": memory,
            "owned": state.owned,
            "observations": state.closure,
            "trigger": synthetic_event(teardown=True),
        }
    )
    assert source.decide(context).intent is None


def test_receipt_reconciliation_reproduces_retained_effects(recorded_exchange, monkeypatch):
    backend = SimpleNamespace(config=recorded_exchange.config)
    for outcome in recorded_exchange.outcomes:

        def observe(_backend, allocation, block, owned, *, quotes, residual, outcome=outcome):
            assert allocation == outcome.operation.allocation
            assert block == outcome.after.block and owned == outcome.after.owned
            assert quotes is False
            # Only an exit leg measures the counter balance it leaves behind.
            assert residual == (outcome.decision.intent.kind == "exit_exchange")
            return outcome.after

        monkeypatch.setattr(reconciliation, "snapshot", observe)
        monkeypatch.setattr(
            reconciliation,
            "retained_growth",
            lambda *_, outcome=outcome: (outcome.after.position_growth, ()),
        )
        actual = reconciliation.reconcile(backend, outcome, outcome.receipt, outcome.after.block)
        assert actual.gas_paid == outcome.gas_paid
        assert (actual.delta0, actual.delta1) == (outcome.delta0, outcome.delta1)
        assert actual.after.owned == outcome.after.owned
        if isinstance(outcome.operation, Burn):
            # The retained snapshot loses the position growth after burning.
            # Reconciliation must preserve measured proceeds without inventing fees.
            assert (actual.fees0, actual.fees1) == (None, None)
            assert actual.accounting_issues == (
                "close proceeds are known; principal/fee split is unmeasured",
            )
        else:
            assert (actual.fees0, actual.fees1) == (outcome.fees0, outcome.fees1)
            assert actual.accounting_issues == outcome.accounting_issues


def admission_state(state, outcome, outcomes):
    return state.model_copy(
        update={
            "mode": "running",
            "decision": outcome.decision,
            "owned": (outcome.before.owned,),
            "outcomes": outcomes,
            "delivered": len(outcomes),
        }
    )


def test_gateway_admits_exact_retained_steps_without_a_spending_history(recorded_exchange):
    state = recorded_exchange
    for index, outcome in enumerate(state.outcomes):
        validation.validate_operation(
            SimpleNamespace(config=state.config),
            admission_state(state, outcome, state.outcomes[:index]),
            outcome.operation,
            outcome.before,
        )
    # Scope-only authority (blueprint 27): an entry repeated after the whole retained
    # lifecycle, which spent the former quota, is still admitted on its own quote.
    entry = next(
        o
        for o in state.outcomes
        if o.operation.kind == "swap"
        and o.operation.amount.asset == o.operation.target.funding_currency
    )
    validation.validate_operation(
        SimpleNamespace(config=state.config),
        admission_state(state, entry, state.outcomes),
        entry.operation,
        entry.before,
    )
    assert not hasattr(validation, "validate_quota")


@pytest.mark.parametrize("kind", ["swap", "mint", "collect", "burn"])
@pytest.mark.parametrize("mutation", ["emitter", "pool", "duplicate", "recipient"])
def test_forged_receipt_events_cannot_establish_effects(kind, mutation):
    outcome = next(o for o in live_state().outcomes if o.operation.kind == kind)
    receipt = copy.deepcopy(outcome.receipt)
    topic = receipts.SWAP if kind == "swap" else receipts.MODIFY
    event = next(e for e in receipt["logs"] if e["topics"][0] == topic)
    if mutation == "emitter":
        event["address"] = "0x" + "12" * 20
    elif mutation == "pool":
        event["topics"][1] = "0x" + "12" * 32
    elif mutation == "duplicate":
        receipt["logs"].append(copy.deepcopy(event))
    else:
        event = next(
            e
            for e in receipt["logs"]
            if e["topics"][0] == receipts.TRANSFER
            and e["address"].lower() in {a.reference for a in outcome.operation.target.assets}
        )
        event["topics"][2] = "0x" + "00" * 12 + "12" * 20
    with pytest.raises(DataError):
        receipts.movement(receipt, outcome.operation)


def test_unrelated_nft_missing_inventory_and_changed_position_fail_closed():
    state = live_state()
    outcome = next(o for o in state.outcomes if isinstance(o.operation, Collect))
    observed = outcome.before
    changed = observed.model_copy(update={"position_liquidity": observed.position_liquidity + 1})
    with pytest.raises(DataError, match="liquidity"):
        validation.validate_owned(observed.owned, changed)
    changed = observed.model_copy(
        update={"wallet1": AssetAmount(asset=observed.wallet1.asset, amount=0)}
    )
    with pytest.raises(DataError, match="inventory"):
        validation.validate_owned(observed.owned, changed)
    from almanak.caip import AssetId

    other = AssetId(str(outcome.operation.nft.asset_type) + "/1")
    changed = outcome.operation.model_copy(update={"nft": other})
    with pytest.raises(DataError, match="NFT"):
        receipts.movement(outcome.receipt, changed)


def test_missing_fee_growth_keeps_close_proceeds_visible_and_allows_closure():
    state = live_state()
    closed = next(o for o in state.outcomes if isinstance(o.operation, Burn))
    before = closed.before.model_copy(
        update={
            "owned": closed.before.owned.model_copy(
                update={
                    "position": closed.before.owned.position.model_copy(update={"fee_growth": None})
                }
            )
        }
    )
    pending = closed.model_copy(update={"before": before})
    deltas, nft, _ = receipts.movement(closed.receipt, closed.operation)
    earned, issues = reconciliation.fees(pending, deltas, None, True)
    assert earned == (None, None) and issues
    owned = reconciliation.owned_after(pending, deltas, nft, None, True)
    assert owned.position is None and owned.inventory0 == closed.after.owned.inventory0
    assert not lifecycle.increases_exposure(closed.decision.intent)
    validation.validate_operation(
        SimpleNamespace(config=state.config),
        state.model_copy(
            update={
                "mode": "teardown",
                "owned": (before.owned,),
                "decision": closed.decision,
                "outcomes": (closed.model_copy(update={"accounting_issues": issues}),),
            }
        ),
        closed.operation,
        before,
    )


def test_trading_and_lp_use_distinct_validated_checkpoint_codecs(recorded):
    from almanak.connectors.composition import checkpoint_from_json

    for state in (recorded, live_state("trading"), live_state("lp")):
        codec = connector_for(state.config).codec
        assert checkpoint_from_json(state.model_dump_json()) == state
        assert isinstance(state, codec.checkpoint)
    initial = lifecycle.UniswapLifecycle.initial(
        live_state().config, author("lp"), live_state().capabilities, live_state().permissions
    )
    lifecycle.validate_initial(initial, live_state().baseline)


def test_mint_price_movement_and_expired_deadline_are_refused():
    state = live_state()
    outcome = next(o for o in state.outcomes if isinstance(o.operation, Mint))
    before = outcome.before
    changed = before.model_copy(
        update={
            "pool": before.pool.model_copy(
                update={"sqrt_price_x96": before.pool.sqrt_price_x96 * 102 // 100}
            )
        }
    )
    context = state.model_copy(update={"decision": outcome.decision})
    with pytest.raises(DataError, match="price moved"):
        validation.mint_limits(
            context, outcome.decision.intent, changed, state.config.allocations[0]
        )
    intent = outcome.decision.intent.model_copy(update={"deadline": before.block.timestamp})
    with pytest.raises(DataError, match="deadline"):
        validation.deadline(intent, before, state.config.allocations[0])


@pytest.mark.parametrize("kind", ["mint", "collect", "burn"])
def test_retained_position_growth_is_scoped_and_missing_growth_is_visible(monkeypatch, kind):
    outcome = next(o for o in live_state().outcomes if o.operation.kind == kind)
    _, nft, _ = receipts.movement(outcome.receipt, outcome.operation)
    liquidity = outcome.after.position_liquidity
    expected = outcome.after.position_growth or (4, 7)

    def read(_reader, target, token, block, selected=None):
        assert target == outcome.operation.target and block == outcome.after.block
        assert token == int((nft or outcome.operation.nft).token_id)
        assert selected == outcome.operation.range
        return liquidity, expected

    monkeypatch.setattr(reconciliation, "position_growth", read)
    backend = SimpleNamespace(reader=None)
    growth, issues = reconciliation.retained_growth(
        backend, outcome.operation, outcome.after.block, nft, True
    )
    assert growth == expected and not issues
    monkeypatch.setattr(
        reconciliation,
        "position_growth",
        lambda *_: (_ for _ in ()).throw(DataError("missing_history", "unavailable")),
    )
    growth, issues = reconciliation.retained_growth(
        backend, outcome.operation, outcome.after.block, nft, True
    )
    assert growth is None and issues


def test_live_collection_fees_use_new_growth_and_preserve_accounting_failures():
    state = live_state()
    collected = next(o for o in state.outcomes if isinstance(o.operation, Collect))
    deltas = collected.delta0, collected.delta1
    earned, issues = reconciliation.fees(collected, deltas, collected.after.position_growth, True)
    assert earned == (collected.fees0, collected.fees1) and not issues
    assert reconciliation.fees(collected, deltas, None, True) == (deltas, ())
    impossible = tuple(v + 2**200 for v in collected.before.owned.position.fee_growth)
    earned, issues = reconciliation.fees(collected, deltas, impossible, True)
    assert earned == (None, None) and issues
    different = deltas[0] + 1, deltas[1]
    earned, issues = reconciliation.fees(
        collected, different, collected.after.position_growth, True
    )
    assert earned == different and issues


def test_author_fork_parameters_validate_explicit_inventory_and_operator_funding():
    from almanak.connectors.uniswap_v4.fork import ForkConfig

    for name in ("trading", "lp"):
        raw = (ROOT / f"tests/fixtures/author/uniswap_v4_{name}/config.json").read_text()
        config = ForkConfig.model_validate_json(scope_only_json(raw))
        config.validate_funding(config.allocations[0])
        bad = config.model_copy(
            update={"funds": (config.funds[0].model_copy(update={"assets": ()}),)}
        )
        with pytest.raises(ValueError, match="USDC balance"):
            bad.validate_funding(config.allocations[0])
