"""Exercise the quant fixtures through the real strategy API without network providers."""

import importlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult
from almanak.framework.intents import Intent
from almanak.framework.teardown import PositionType, TeardownMode

ROOT = Path(__file__).resolve().parents[3] / "strategies" / "experiments"
NATIVE = "quant_v4_base_native_roundtrip_20260908"
HISTORICAL = "quant_v4_arb_historical_dynamic_20260908"
LP = "quant_v4_base_weth_lp_20260908"


def make_strategy(name, **overrides):
    module = importlib.import_module(f"strategies.experiments.{name}.strategy")
    config = json.loads((ROOT / name / "config.json").read_text())
    config.update(overrides)
    cls = module.V4ExactKeyLPLifecycle if name == LP else module.V4ExactKeyRoundtrip
    return cls(config=config, chain=config["chain"], wallet_address="0x" + "12" * 20)


def snapshot(**balances):
    market = Mock(spec=["balance", "price"])
    market.balance.side_effect = lambda token: SimpleNamespace(balance=Decimal(str(balances.get(token, 0))))
    market.price.side_effect = lambda token: Decimal("1") if token == "USDC" else Decimal("2500")
    return market


@pytest.mark.parametrize("name", [NATIVE, HISTORICAL])
def test_measured_position_and_teardown_use_canonical_snapshot(name, monkeypatch):
    strategy = make_strategy(name)
    strategy.stage = "holding"
    strategy.acquired = Decimal("0.001")
    market = snapshot(**{strategy.base_token: "0.0012"})
    build = Mock(return_value=market)
    monkeypatch.setattr(strategy, "create_market_snapshot", build)
    assert not hasattr(strategy, "market")
    positions = strategy.get_open_positions().positions
    assert len(positions) == 1
    assert positions[0].value_usd == Decimal("2.500")
    assert positions[0].details["balance"] == "0.001"
    (intent,) = strategy.generate_teardown_intents(TeardownMode.SOFT)
    assert intent.amount == Decimal("0.001")
    assert intent.swap_params == strategy.swap_params
    assert intent.max_slippage == strategy.slippage
    assert build.call_count == 2


@pytest.mark.parametrize("name", [NATIVE, HISTORICAL])
def test_unmeasured_buy_recovers_only_balance_increase(name, monkeypatch):
    strategy = make_strategy(name)
    strategy.load_persistent_state({"stage": "bought_unmeasured", "base_before": "0.002", "acquired": "0"})
    market = snapshot(**{strategy.base_token: "0.003"})
    monkeypatch.setattr(strategy, "create_market_snapshot", lambda: market)
    (position,) = strategy.get_open_positions().positions
    assert position.details["fill_measured"] is False
    assert Decimal(position.details["balance"]) == Decimal("0.001")
    (intent,) = strategy.generate_teardown_intents(TeardownMode.SOFT)
    assert intent.amount == Decimal("0.001")
    assert Decimal("0.003") - intent.amount == strategy.base_before
    assert strategy.decide(market).intent_type.value == "HOLD"


@pytest.mark.parametrize("name", [NATIVE, HISTORICAL])
def test_explicit_snapshot_does_not_build_another(name, monkeypatch):
    strategy = make_strategy(name)
    strategy.stage = "holding"
    strategy.acquired = Decimal("0.001")
    build = Mock(side_effect=AssertionError("Must use supplied snapshot"))
    monkeypatch.setattr(strategy, "create_market_snapshot", build)
    (intent,) = strategy.generate_teardown_intents(TeardownMode.SOFT, snapshot(**{strategy.base_token: "0.002"}))
    assert intent.amount == strategy.acquired
    build.assert_not_called()


@pytest.mark.parametrize(
    "stage,available", [("holding", "0.001"), ("holding", "0.0009"), ("bought_unmeasured", "0.0019")]
)
def test_native_exit_refuses_exhausted_gas_or_missing_inventory(stage, available):
    strategy = make_strategy(NATIVE)
    strategy.stage = stage
    strategy.acquired = Decimal("0.001")
    strategy.base_before = Decimal("0.002")
    with pytest.raises(ValueError):
        strategy.generate_teardown_intents(TeardownMode.SOFT, snapshot(ETH=available))
    assert strategy.stage == stage


def test_wbtc_exit_can_sell_entire_erc20_balance():
    strategy = make_strategy(HISTORICAL)
    strategy.stage = "holding"
    strategy.acquired = Decimal("0.001")
    (intent,) = strategy.generate_teardown_intents(TeardownMode.SOFT, snapshot(WBTC="0.001"))
    assert intent.amount == Decimal("0.001")


@pytest.mark.parametrize("name", [NATIVE, HISTORICAL])
def test_teardown_success_without_sell_does_not_forget_inventory(name):
    strategy = make_strategy(name)
    strategy.stage = "holding"
    strategy.acquired = Decimal("0.001")
    with pytest.raises(ValueError, match="without a measured inventory sale"):
        strategy.on_teardown_completed(True, Decimal("0"))
    assert strategy.acquired == Decimal("0.001")
    assert strategy.stage == "holding"


@pytest.mark.parametrize("name", [NATIVE, HISTORICAL])
def test_buy_fill_roundtrip_survives_resume_and_does_not_repeat(name):
    strategy = make_strategy(name)
    buy = Intent.swap(from_token="USDC", to_token=strategy.base_token, amount=Decimal("3"))
    strategy.on_intent_executed(
        buy, True, SimpleNamespace(swap_amounts=SimpleNamespace(amount_out_decimal=Decimal("0.001")))
    )
    resumed = make_strategy(name)
    resumed.load_persistent_state(strategy.get_persistent_state())
    market = snapshot(**{resumed.base_token: "0.002"})
    assert resumed.decide(market).intent_type.value == "HOLD"
    (sell,) = resumed.generate_teardown_intents(TeardownMode.SOFT, market)
    resumed.on_intent_executed(sell, True, SimpleNamespace())
    resumed.on_teardown_completed(True, Decimal("2.5"))
    assert resumed.get_open_positions().positions == []
    assert resumed.generate_teardown_intents(TeardownMode.SOFT) == []
    assert resumed.decide(market).intent_type.value == "HOLD"


def test_held_lp_positions_and_signal_close_plan(monkeypatch):
    strategy = make_strategy(LP, close_on_signal=True)
    strategy.load_persistent_state({"stage": "holding", "position_id": "17", "base_inventory": "0.00001", "holds": 2})
    market = snapshot(WETH="0.00001")
    build = Mock(return_value=market)
    monkeypatch.setattr(strategy, "create_market_snapshot", build)
    assert not hasattr(strategy, "market")
    positions = strategy.get_open_positions().positions
    assert [p.position_type for p in positions] == [PositionType.LP, PositionType.TOKEN]
    assert positions[0].position_id == "17"
    assert positions[1].value_usd == Decimal("0.025")
    assert strategy.decide(market).intent_type.value == "HOLD"
    close, sale = strategy.generate_teardown_intents(TeardownMode.SOFT)
    assert close.position_id == "17"
    assert close.protocol_params == sale.swap_params == strategy.params
    assert sale.from_token == "WETH" and sale.amount == "all"
    assert build.call_count == 1


def test_lp_liquidation_uses_supplied_or_canonical_snapshot(monkeypatch):
    strategy = make_strategy(LP)
    strategy.stage = "liquidating"
    strategy.base_inventory = Decimal("0.0005")
    market = snapshot(WETH="0.0005", ETH="0.0002")
    build = Mock(return_value=market)
    monkeypatch.setattr(strategy, "create_market_snapshot", build)
    (first,) = strategy.generate_teardown_intents(TeardownMode.SOFT)
    (second,) = strategy.generate_teardown_intents(TeardownMode.SOFT, market)
    assert first.amount == second.amount == Decimal("0.0005")
    assert first.from_token == "WETH"
    assert build.call_count == 1


def test_lp_open_hold_close_liquidation_resume():
    strategy = make_strategy(LP)
    market = snapshot(WETH=strategy.amount0, USDC=strategy.amount1)
    market.price.side_effect = lambda token: Decimal("1") if token == "USDC" else Decimal("2400")
    opened = strategy.decide(market)
    assert opened.intent_type.value == "LP_OPEN"
    deposit = int(strategy.amount0 * 10**18) - 1000
    strategy.on_intent_executed(
        opened,
        True,
        ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            position_id="17",
            extracted_data={"lp_open_data": LPOpenData(position_id=17, amount0=deposit)},
        ),
    )
    for _ in range(strategy.hold_iterations):
        assert strategy.decide(market).intent_type.value == "HOLD"
    close = strategy.decide(market)
    assert close.intent_type.value == "LP_CLOSE"
    strategy.on_intent_executed(
        close,
        True,
        ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, lp_close_data=LPCloseData(amount0_collected=deposit - 100)
        ),
    )
    resumed = make_strategy(LP)
    resumed.load_persistent_state(strategy.get_persistent_state())
    sale = resumed.decide(market)
    assert sale.amount == strategy.amount0 - Decimal(100) / Decimal(10**18)
    assert sale.swap_params == strategy.params
    resumed.on_intent_executed(sale, True, SimpleNamespace())
    assert resumed.decide(market).intent_type.value == "HOLD"
    assert resumed.get_open_positions().positions == []
    assert resumed.generate_teardown_intents(TeardownMode.SOFT) == []


def test_successful_unmeasured_lp_open_never_mints_again():
    strategy = make_strategy(LP)
    opened = Intent.lp_open(pool=strategy.pool, amount0=strategy.amount0, amount1=strategy.amount1)
    strategy.on_intent_executed(
        opened, True, ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE, position_id="17")
    )
    assert strategy.stage == "open_unmeasured"
    assert strategy.decide(snapshot()).intent_type.value == "HOLD"
    close, _ = strategy.generate_teardown_intents(TeardownMode.SOFT)
    assert close.position_id == "17"


@pytest.mark.parametrize("name", [NATIVE, HISTORICAL, LP])
@pytest.mark.parametrize("offset,expected", [(-1, "HOLD"), (0, "SWAP"), (1, "SWAP")])
def test_schedule_boundary_before_any_market_reads(name, offset, expected, monkeypatch):
    module = importlib.import_module(f"strategies.experiments.{name}.strategy")
    now = datetime(2026, 9, 8, 18, 0, 0, tzinfo=UTC)
    clock = Mock(wraps=datetime)
    clock.now.return_value = now.replace(second=1 + offset)
    monkeypatch.setattr(module, "datetime", clock)
    strategy = make_strategy(name, start_at="2026-09-08T18:00:01Z")
    market = snapshot(USDC=4, WETH="0.000623213187480539")
    if name == LP and expected == "SWAP":
        expected = "LP_OPEN"
    assert strategy.decide(market).intent_type.value == expected
    if expected == "HOLD":
        market.balance.assert_not_called()
        market.price.assert_not_called()


@pytest.mark.parametrize("name", [NATIVE, HISTORICAL, LP])
@pytest.mark.parametrize("invalid", ["2026-09-08T18:00:00", "2026-09-08T18:00:00+01:00", "bad", 123])
def test_schedule_rejects_ambiguous_timestamps(name, invalid):
    with pytest.raises(ValueError):
        make_strategy(name, start_at=invalid)


def test_lp_fixture_wallet_token_is_classified_as_existing_wallet_value(monkeypatch):
    from almanak.framework.valuation.portfolio_valuer import _is_wallet_pseudo_position

    strategy = make_strategy(LP)
    monkeypatch.setattr(strategy, "create_market_snapshot", lambda: snapshot(WETH=strategy.base_inventory))
    position = strategy.get_open_positions().positions[0]
    assert position.position_type == PositionType.TOKEN
    assert _is_wallet_pseudo_position(
        position, [SimpleNamespace(symbol="WETH", address=strategy.params["pool_key"]["currency0"])]
    )


@pytest.mark.parametrize("start_at", [None, "2999-01-01T00:00:00Z"])
def test_lp_schedule_never_delays_existing_position_exit(start_at):
    strategy = make_strategy(LP, start_at=start_at)
    strategy.stage = "holding"
    strategy.position_id = "17"
    strategy.holds = strategy.hold_iterations
    assert strategy.decide(snapshot()).intent_type.value == "LP_CLOSE"
    strategy.stage = "liquidating"
    assert strategy.decide(snapshot(WETH=strategy.base_inventory)).intent_type.value == "SWAP"


def test_lp_null_start_is_immediate():
    strategy = make_strategy(LP, start_at=None)
    assert strategy.decide(snapshot(WETH=strategy.amount0, USDC=strategy.amount1)).intent_type.value == "LP_OPEN"


@pytest.mark.parametrize("name", [NATIVE, HISTORICAL])
@pytest.mark.parametrize("pool_id_only", [False, True])
def test_roundtrip_position_address_tracks_base_currency(name, pool_id_only, monkeypatch):
    params = json.loads((ROOT / name / "config.json").read_text())["swap_params"]
    expected = params["pool_key"]["currency1"]
    if pool_id_only:
        params.pop("pool_key")
    strategy = make_strategy(name, base_token="USDC", swap_params=params)
    strategy.stage = "holding"
    strategy.acquired = Decimal("1")
    monkeypatch.setattr(strategy, "create_market_snapshot", lambda: snapshot(USDC="1"))
    (position,) = strategy.get_open_positions().positions
    assert position.details["address"].lower() == expected.lower()


def test_roundtrip_native_address_is_not_wrapped(monkeypatch):
    strategy = make_strategy(NATIVE)
    strategy.swap_params.pop("pool_key")
    strategy.stage = "holding"
    strategy.acquired = Decimal("0.001")
    monkeypatch.setattr(strategy, "create_market_snapshot", lambda: snapshot(ETH="0.002"))
    (position,) = strategy.get_open_positions().positions
    assert position.details["address"] == "0x" + "0" * 40


def test_roundtrip_experiments_have_distinct_registry_entries():
    from almanak.framework.strategies import get_strategy

    for name in (NATIVE, HISTORICAL):
        strategy = make_strategy(name)
        assert get_strategy(name) is type(strategy)


def test_idle_lp_teardown_liquidates_only_declared_prefunded_inventory():
    strategy = make_strategy(LP)
    (sale,) = strategy.generate_teardown_intents(TeardownMode.SOFT, snapshot(WETH=strategy.amount0 * 2))
    assert sale.amount == strategy.amount0
    strategy.on_intent_executed(sale, True, SimpleNamespace())
    strategy.on_teardown_completed(True, Decimal("1"))
    assert strategy.generate_teardown_intents(TeardownMode.SOFT) == []
