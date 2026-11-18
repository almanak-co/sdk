"""Component economics use captured pool state; sandbox and acquisition have separate proofs."""

import gzip
import json
from types import SimpleNamespace

import pytest

from almanak.backtest.engine import Schedule
from almanak.backtest.inventory import Fund
from almanak.connectors.curve import authoring
from almanak.connectors.curve.authority import declared
from almanak.connectors.curve.domain import PoolObservation
from almanak.connectors.curve.economic_data import UNIT, PoolSample, Record, key
from almanak.connectors.curve.economic_model import EconomicModel
from almanak.connectors.curve.economic_parameters import initial, wallet
from almanak.connectors.curve.economic_state import State
from almanak.connectors.curve.models import Allocation, Parameters, Target
from almanak.dex_intents import ExitExchange
from almanak.models import (
    AccountBinding,
    AssetAmount,
    AssetMetadata,
    SimulationAccount,
    StrategyAccounts,
)
from almanak.triggers import IntervalSelection, IntervalTrigger
from tests.exchange_dust import holding, settle_dust
from tests.retained_fixtures import retained
from tests.test_curve_settlement import amounts
from tests.test_curve_settlement import target as target


def component(target):
    path = retained("2026-09-07-rollout") / "curve-initial-discovery/curve-reader-capture.json.gz"
    captured = json.loads(gzip.decompress(path.read_bytes()))
    pool = PoolObservation.model_validate_json(json.dumps(captured["observation"]))
    at = pool.block.timestamp
    capital = amounts(target, (0, 100 * 10**6))
    native = type(target.assets[0])("eip155:1/slip44:60")
    profile = SimulationAccount(fund=target.fund)
    allocation = Allocation(
        name=target.allocation,
        account=target.account,
        pool=target.pool,
        funding_asset=target.funding_asset,
        trading_asset=target.trading_asset,
        liquidity_enabled=True,
        single_coin_exit=target.single_coin_exit,
        capital=capital,
        entry_swap_bps=5000,
        hold_seconds=120,
        slippage_bps=100,
        deadline_seconds=300,
        native_asset=native,
        minimum_native_balance=0,
    )
    config = Parameters(
        decision_trigger=IntervalTrigger(kind="interval", seconds=5),
        allocations=(allocation,),
        accounts=StrategyAccounts(bindings=(AccountBinding(name=target.account, profile=profile),)),
        asset_metadata={
            **{
                asset.identifier: AssetMetadata(decimals=asset.decimals)
                for asset in (*pool.currencies, pool.shares)
            },
            native: AssetMetadata(decimals=18),
        },
    )
    settings = SimpleNamespace(
        schedule=Schedule(
            start=at, end=at + 100, latency_seconds=1, end_policy="teardown", cleanup_seconds=100
        ),
        liquidity_model="fixed_external_unit_balances",
        funds=(
            Fund(
                name=target.account,
                profile=profile,
                balances=(*capital.root, AssetAmount(asset=native, amount=1000)),
            ),
        ),
        payers=(),
        costs=SimpleNamespace(
            kind="fixed_native", swap=3, mint=5, increase=5, decrease=4, close=4, revoke=1
        ),
    )
    record = Record(
        key=key(target),
        unit=UNIT,
        reference=pool.block,
        source_time=at,
        available_at=at,
        retrieved_at=at + 1000,
        evidence=("a" * 64,),
        payload=PoolSample(
            pool=pool,
            contracts=(),
            parameter_path="a" * 64,
            successor=pool.block.model_copy(
                update={"number": pool.block.number + 1, "timestamp": at + 12}
            ),
        ),
    )
    interval = IntervalSelection(configured=config.decision_trigger)
    state = initial(settings, config, interval)
    model = object.__new__(EconomicModel)
    model.config, model.settings, model.strategy = config, settings, None
    model.book = SimpleNamespace(at=lambda target, time: record)
    model.authority = declared(config)
    model.capabilities = (SimpleNamespace(chain=target.fund.chain),)
    return model, state


def execute(model, state, intent):
    pending = SimpleNamespace(
        intent=intent,
        identity=f"step-{state.time}",
        decision_time=state.time,
        eligible_at=state.time + 1,
    )
    transition = model.execute(state, pending)
    after = state.model_copy(
        update={"portfolio": state.portfolio.apply(transition.effects), "time": pending.eligible_at}
    )
    assert State.model_validate_json(after.model_dump_json()) == after
    return after, transition


@pytest.mark.parametrize("single", [False, True])
def test_integer_lp_roundtrip_retains_all_coins_shares_and_payer_costs(target, single):
    if not single:
        target = target.model_copy(update={"single_coin_exit": None})
    model, state = component(target)
    allocation = model.config.allocations[0]
    observed = model.observe(state)[0]
    state, result = execute(
        model, state, authoring.swap(allocation, observed, observed.entry_quote)
    )
    assert result.result.success
    observed = model.observe(state)[0]
    state, result = execute(
        model, state, authoring.open_liquidity(allocation, observed, fraction_bps=5000)
    )
    assert result.result.success and result.result.share_change.amount > 0
    observed = model.observe(state)[0]
    state, result = execute(model, state, authoring.increase_liquidity(allocation, observed))
    assert result.result.success
    observed = model.observe(state)[0]
    state, result = execute(
        model,
        state,
        authoring.decrease_liquidity(
            allocation, observed, observed.owned.position.shares.amount // 2
        ),
    )
    assert result.result.success and result.result.share_change.amount < 0
    intent = ExitExchange[Target](target=target)
    state, result = execute(model, state, intent)
    assert result.result.success and result.result.after.position is None
    if not result.complete:
        state, result = execute(model, state, intent)
    assert result.result.success and result.complete
    assert model.closed(state, model.observe(state))
    assert state.portfolio.balance(wallet(target, allocation.native_asset)) == (
        979 if single else 976
    )


def test_virtual_lp_claims_do_not_mutate_external_pool_history(target):
    model, state = component(target)
    allocation = model.config.allocations[0]
    original = model.book.at(target, state.time)
    state, result = execute(
        model, state, authoring.open_liquidity(allocation, model.observe(state)[0])
    )
    assert result.result.success
    assert model.book.at(target, state.time) == original
    assert model.observe(state)[0].pool == original.payload.pool


def test_failed_economic_minimum_preserves_fund_and_lp_ownership(target):
    model, state = component(target)
    observed = model.observe(state)[0]
    intent = authoring.swap(model.config.allocations[0], observed, observed.entry_quote)
    intent = intent.model_copy(
        update={"minimum_output": AssetAmount(asset=target.trading_asset, amount=10**30)}
    )
    after, result = execute(model, state, intent)
    assert not result.result.success
    assert after.portfolio == state.portfolio
    assert result.result.status == "failed"


def test_exit_holding_only_counter_dust_completes_without_a_step(target):
    model, state = component(target)
    record = model.book.at(target, state.time)

    def quote(at, amount):
        owned = at.portfolio.positions[0].payload.owned
        return model.simulation.quote_at(record, owned, amount, target.funding_asset, at.time)

    dusty = settle_dust(model, state, target, target.trading_asset, quote)
    larger = holding(model, dusty, {target.trading_asset: 10**30})
    assert not model.closed(larger, model.observe(larger))
