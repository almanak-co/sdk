"""Exact captured quotes, integer fee accounting and interrupted offline simulation."""

from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.history import LogScope, ReadRequest, validate_logs_request
from almanak.connectors.uniswap_v4 import history, simulation
from almanak.connectors.uniswap_v4.backtest import State, initial_state, validate_position
from almanak.connectors.uniswap_v4.backtest_binding import UniswapHistorical, result_report
from almanak.connectors.uniswap_v4.calibration import load_calibration, validate_costs
from almanak.connectors.uniswap_v4.math import Q128, UINT256, accrued, amounts
from almanak.connectors.uniswap_v4.reader import quote, sqrt_bounds
from almanak.connectors.uniswap_v4.state import SimulatedLiquidity
from almanak.gateway.ethereum import DataError
from tests.uniswap_support import historical_model


@pytest.mark.parametrize("name", ["trading", "lp", "avalanche_trading", "avalanche_lp"])
def test_same_strategy_reproduces_captured_economic_path_and_costs(tmp_path, name):
    manifest, expected, model = historical_model(name, current=True)
    try:
        assert UniswapHistorical.baselines(model, manifest.initial) == manifest.baselines
        journal = Journal(tmp_path / "state.sqlite", State)
        journal.create(manifest.initial)
        result = Simulator(journal, model).run()
        assert result.portfolio == expected.portfolio
        assert result.memory == expected.memory
        assert result.delivered == len(result.completed) == len(expected.completed)
        assert all(
            c.transition.result.block.timestamp == c.pending.eligible_at for c in result.completed
        )
        assert all(c.pending.eligible_at >= c.pending.decision_time + 30 for c in result.completed)
        assert all(
            (c.pending.decision_time - result.schedule.start) % 3600 == 0 for c in result.completed
        )
        report = result_report(result, model)
        assert report["closed"] and report["positions"][0]["complete"]
        owned = result.portfolio.positions[0].payload.owned
        assert all(
            amount.amount == 0
            for amount in (owned.inventory0, owned.inventory1)
            if amount.asset != owned.target.funding_currency
        )
        assert report["gas_payers"][0]["operation_count"] >= len(result.completed)
        assert (
            report["costs"]["calibration_sha256"]
            == load_calibration(manifest.config.allocations[0].pool.chain.chain_id)[0]
        )
        assert (
            initial_state(manifest.settings, manifest.config, manifest.interval).portfolio
            == manifest.initial.portfolio
        )
    finally:
        for h in model.histories.values():
            h.reader.__exit__()


@pytest.mark.parametrize("name", ["lp", "avalanche_lp"])
def test_fee_collection_resets_checkpoint_and_close_counts_only_new_growth(name):
    _, state, model = historical_model(name)
    try:
        mint = next(c for c in state.completed if c.pending.intent.kind == "open_liquidity")
        collect = next(
            c for c in state.completed if c.pending.intent.kind == "collect_liquidity_fees"
        )
        close = next(c for c in state.completed if c.pending.intent.kind == "close_liquidity")
        position = mint.transition.result.after.position
        assert isinstance(position, SimulatedLiquidity)
        assert position.fee_growth == mint.transition.result.before.fee_growth
        assert position.simulation_id == mint.pending.identity
        for item in (collect, close):
            result = item.transition.result
            expected = simulation.fees_owed(result.before.owned.position, result.before.fee_growth)
            assert (result.fees0, result.fees1) == expected
        collected = collect.transition.result.after.position
        assert simulation.fees_owed(collected, collected.fee_growth) == (0, 0)
        final = state.portfolio.positions[0].payload
        assert final.fees0 == collect.transition.result.fees0 + close.transition.result.fees0
        assert final.fees1 == collect.transition.result.fees1 + close.transition.result.fees1
        assert final.fees0 + final.fees1 > 0
        report = result_report(state, model)["positions"][0]
        assert report["premint_hold_benchmark"]["complete"]
    finally:
        for h in model.histories.values():
            h.reader.__exit__()


def event(lower, upper, change):
    return {
        "data": "0x"
        + encode(("int24", "int24", "int256", "bytes32"), (lower, upper, change, bytes(32))).hex()
    }


@pytest.mark.parametrize(
    "failure", ["uninitialized", "cleared", "reinitialized", "overflow", "interval"]
)
def test_boundary_histories_fail_closed(failure):
    manifest, _, _ = historical_model("lp")
    target = manifest.baselines[0].target
    lower, upper = target.range.lower, target.range.upper
    scenarios = {
        "uninitialized": ((0, 100), ()),
        "cleared": ((100, 100), (event(lower, upper, -100),)),
        "reinitialized": ((100, 100), (event(lower, upper, -100), event(lower, upper, 100))),
        "overflow": ((2**128 - 1, 100), (event(lower, upper, 1),)),
        "interval": ((100, 100), (event(upper, lower, 1),)),
    }
    with pytest.raises(DataError):
        history.continuous_gross(target, *scenarios[failure])


def test_wrapping_fees_and_principal_do_not_invent_growth_or_double_round():
    position = SimulatedLiquidity(
        simulation_id="run:mint", liquidity=Q128 // 2, fee_growth=(UINT256 - 9, 3)
    )
    assert simulation.fees_owed(position, (11, 4)) == (10, 0)
    assert accrued(2, 5, Q128 + 5) == 2 * accrued(1, 5, Q128 + 5)
    target = historical_model("lp")[0].baselines[0].target
    low, high = sqrt_bounds(target)
    for price in (low - 1, low, (low + high) // 2, high, high + 1):
        withdrawal = amounts(100000000000000, price, low, high)
        deposit = amounts(100000000000000, price, low, high, up=True)
        assert all(0 <= paid - got <= 1 for paid, got in zip(deposit, withdrawal, strict=True))


@pytest.mark.parametrize("mutation", ["pool", "address", "wildcard", "range", "negative"])
def test_historical_log_scope_cannot_expand(mutation):
    scope = LogScope(address="0x" + "12" * 20, topics=("0x" + "34" * 32, "0x" + "56" * 32))
    query = {
        "address": scope.address,
        "topics": list(scope.topics),
        "fromBlock": "0x10",
        "toBlock": "0x20",
    }
    validate_logs_request((query,), (scope,))
    if mutation == "pool":
        query["topics"][1] = "0x" + "78" * 32
    if mutation == "address":
        query["address"] = "0x" + "78" * 20
    if mutation == "wildcard":
        query["topics"][1] = None
    if mutation == "range":
        query["toBlock"] = hex(16 + 2000)
    if mutation == "negative":
        query.update(fromBlock="-0x1", toBlock="0x0")
    with pytest.raises(ValueError):
        validate_logs_request((query,), (scope,))


def test_missing_capture_is_explicit_and_cumulative_inputs_bound_nothing():
    manifest, state, model = historical_model("trading")
    reader = next(iter(model.histories.values())).reader
    try:
        payload = state.portfolio.positions[0].payload
        # Scope-only authority (blueprint 27): repeated entries may spend past capital.
        spent = payload.model_copy(
            update={"swap0": 10 * manifest.config.allocations[0].capital.amount}
        )
        validate_position(manifest.config, spent)
        reader.records.clear()
        with pytest.raises(DataError, match="missing recorded"):
            reader._rpc("eth_getBlockByNumber", ["0x0", False])
        changed = manifest.settings.model_copy(
            update={"costs": manifest.settings.costs.model_copy(update={"per_operation": {}})}
        )
        with pytest.raises(ValueError, match="fixed costs"):
            validate_costs(changed)
    finally:
        reader.__exit__()


def test_size_specific_fill_rejects_price_movement_or_failed_quote():
    _, state, model = historical_model("trading")
    result = state.completed[0].transition.result
    before = result.before
    operation = SimpleNamespace(
        target=before.target,
        amount=result.intent.amount,
        minimum=result.intent.minimum_output.model_copy(update={"amount": 2**127}),
    )
    payload = state.completed[0].transition.effects.positions[0].before.payload
    reader = next(iter(model.histories.values())).reader
    try:
        reader.pin_block(before.block.number)
        with pytest.raises(ValueError, match="retained swap minimum"):
            simulation.swapped(reader, operation, before, payload)
        overflow = SimpleNamespace(call=lambda *_args, **_kwargs: (2**128, 0))
        with pytest.raises(DataError, match="exceeds uint128"):
            quote(overflow, before.target, operation.amount, before.block)
        # The V4 Quoter reverts on unfilled input, so a zero result is a measured
        # zero output, which only an exit residual may retain.
        zero = SimpleNamespace(call=lambda *_args, **_kwargs: (0, 0))
        assert quote(zero, before.target, operation.amount, before.block).output.amount == 0
        request = ReadRequest(chain=reader.chain, method="eth_chainId", params=())
        assert request.key in reader.records
    finally:
        reader.__exit__()


@pytest.mark.parametrize("name", ["uniswap_v4_avalanche_trading", "uniswap_v4_trading"])
def test_closure_preserves_funding_inventory_and_refuses_counter_currency(name):
    from almanak.connectors.uniswap_v4.backtest import UniswapModel
    from tests.test_new_reference_replay import proof
    from tests.uniswap_support import live_state

    checkpoint = proof(name) if "avalanche" in name else live_state("trading")
    owned = checkpoint.owned[0]
    assert owned.target.funding_currency in owned.target.assets
    portfolio = SimpleNamespace(
        positions=(
            SimpleNamespace(
                payload=SimpleNamespace(
                    owned=owned,
                    target=owned.target,
                )
            ),
        )
    )
    state = SimpleNamespace(portfolio=portfolio)
    assert UniswapModel.closed(None, state, checkpoint.closure)
    field = (
        "inventory0" if owned.inventory0.asset != owned.target.funding_currency else "inventory1"
    )
    residual = getattr(owned, field).model_copy(update={"amount": 1})
    portfolio.positions[0].payload.owned = owned.model_copy(update={field: residual})
    assert not UniswapModel.closed(None, state, checkpoint.closure)
