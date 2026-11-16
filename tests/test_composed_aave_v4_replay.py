"""Current-author replay of immutable V4 fork captures, separate from native proof."""

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v4.models import LendingSnapshot
from almanak.connectors.composed.inventory import cash_evidence
from almanak.connectors.composition import checkpoint_from_json, connector_for
from almanak.memory_codec import freeze
from tests.lending_captures import read_capture
from tests.strategy_fork import aave_v4_composed_assertions as assertions
from tests.strategy_fork.aave_v4_composed_assertions import assert_closed
from tests.test_new_reference_replay import module
from tests.uniswap_support import scope_only_json

ROOT = Path(__file__).parents[1]
EVIDENCE = (
    ROOT
    / "docs/internal/reports/evidence/2026-09-09-rollout/aave-v4-composition-source-5"
    / "artifacts/aave-v4-composition-source-5"
)


@pytest.mark.parametrize("chain", ["ethereum", "avalanche"])
def test_actual_recorded_v4_decisions_conserve_cash_and_native_ownership(chain):
    reference = f"{chain}_aave_v4_borrow_and_trade"
    paths = list(
        (EVIDENCE / f"pinned-{reference}-safe_roles-configured").rglob("checkpoint.json.gz")
    )
    assert len(paths) == 1
    state = checkpoint_from_json(scope_only_json(read_capture(paths[0], reference)))
    assert state.mode == "closed"
    binding = connector_for(state.config)
    current = binding.lifecycle.initial(
        state.config, state.strategy, state.capabilities, state.permissions
    )
    source = module(reference)
    memory = source.State()
    identity = None
    source.validate_config(state.config)
    assert source.capabilities(state.config) == state.capabilities
    for outcome in state.outcomes:
        if outcome.decision.identity != identity:
            context = binding.codec.decision_input(
                config=state.config,
                memory=memory,
                owned=current.owned,
                observations=outcome.decision.observations,
                trigger=outcome.decision.trigger,
            )
            decision = source.decide(context)
            assert decision.intent == outcome.decision.intent
            memory = decision.memory
            identity = outcome.decision.identity
        memory = source.on_outcome(
            binding.codec.outcome_input(
                config=state.config, memory=memory, result=outcome.strategy_result()
            )
        )
        current = current.model_copy(update=binding.lifecycle.ownership(current, outcome))
        current = checkpoint_from_json(current.model_dump_json())
    assert (current.owned, current.inventory) == (state.owned, state.inventory)
    assert freeze(vars(source._module), memory) == state.memory
    binding.lifecycle.validate_closure(current, state.closure)
    permuted = state.model_copy(
        update={
            "closure": tuple(
                next(
                    closed
                    for closed in state.closure
                    if assertions.closure_allocation(closed) == name
                )
                for name in ("trade", "loan", "collateral")
            )
        }
    )
    assert_closed(None, permuted)


def holding(chain):
    reference = f"{chain}_aave_v4_borrow_and_trade"
    paths = list(
        (EVIDENCE / f"pinned-{reference}-safe_roles-configured").rglob("checkpoint.json.gz")
    )
    state = checkpoint_from_json(scope_only_json(read_capture(paths[0], reference)))
    binding = connector_for(state.config)
    current = binding.lifecycle.initial(
        state.config, state.strategy, state.capabilities, state.permissions
    )
    source = module(reference)
    memory = source.State()
    identity = None
    for index, outcome in enumerate(state.outcomes):
        if outcome.decision.intent.kind == "exit_exchange":
            return current.model_copy(
                update={
                    "initialized": True,
                    "outcomes": state.outcomes[:index],
                    "delivered": index,
                    "memory": freeze(vars(source._module), memory),
                }
            ), outcome.decision.observations
        if outcome.decision.identity != identity:
            context = binding.codec.decision_input(
                config=state.config,
                memory=memory,
                owned=current.owned,
                observations=outcome.decision.observations,
                trigger=outcome.decision.trigger,
            )
            decision = source.decide(context)
            memory = decision.memory
            identity = outcome.decision.identity
        memory = source.on_outcome(
            binding.codec.outcome_input(
                config=state.config, memory=memory, result=outcome.strategy_result()
            )
        )
        current = current.model_copy(update=binding.lifecycle.ownership(current, outcome))
    raise AssertionError("retained reference never entered reversal")


@pytest.mark.parametrize("chain", ["ethereum", "avalanche"])
@pytest.mark.parametrize("fault", [None, "operator_position", "fund_shares", "fund_debt"])
def test_holding_assertions_validate_retained_fund_ownership_and_refuse_synthetic_misattribution(
    monkeypatch, chain, fault
):
    state, observations = holding(chain)
    state = state.model_copy(
        update={
            "owned": tuple(
                next(owned for owned in state.owned if owned.target.allocation == name)
                for name in ("trade", "loan", "collateral")
            )
        }
    )
    native = [o for o in observations if isinstance(o, LendingSnapshot)]
    block = native[0].block

    def read_position(reader, reserve, fund, selected):
        observed = next(o.position for o in native if o.target.reserve == reserve)
        if fund == observed.account:
            changes = {}
            if fault == "fund_shares":
                changes["supplied_shares"] = observed.supplied_shares + 1
            if fault == "fund_debt":
                changes["total_debt"] = 0
            return observed.model_copy(update=changes)
        return observed.model_copy(
            update={
                "account": fund,
                "supplied_shares": int(fault == "operator_position"),
                "total_debt": 0,
            }
        )

    def wallet(reader, current, selected):
        evidence = cash_evidence(observations)
        assert all(evidence[b.key][1] == b.amount for b in current.inventory)

    reader = SimpleNamespace(pin_block=lambda tag: block)
    monkeypatch.setattr(assertions, "EthereumReader", lambda *args: nullcontext(reader))
    monkeypatch.setattr(assertions, "read_position", read_position)
    monkeypatch.setattr(assertions, "assert_wallet_balances", wallet)
    node = SimpleNamespace(rpc=SimpleNamespace(endpoint="offline retained observations"))
    if fault is None:
        assertions.assert_holding(node, state)
    else:
        with pytest.raises(AssertionError):
            assertions.assert_holding(node, state)
