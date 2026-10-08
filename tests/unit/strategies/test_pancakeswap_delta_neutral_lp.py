"""PancakeSwap V3 LP + Aster Pro hedge: config, phase machine, sizing, teardown."""

from __future__ import annotations

import json
import math
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.execution.extracted_data import LPOpenData
from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult
from almanak.framework.execution.submission import (
    ReplayPolicy,
    SubmissionProvenance,
    SubmissionTransactionEvidence,
    TransactionRole,
)
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.teardown import PositionType, TeardownMode
from strategies.incubating.pancakeswap_delta_neutral_lp import strategy as mod
from strategies.incubating.pancakeswap_delta_neutral_lp.strategy import (
    BSC_USDT,
    BSC_WBNB,
    BSC_WETH,
    DEPOSIT_UNVERIFIED,
    HEDGE_PENDING,
    HEDGED,
    IDLE,
    LP_OPEN_FAILED,
    LP_OPENED,
    MARGIN_SETTLING,
    RECOVERY_REQUIRED,
    WINDING_DOWN,
    HedgePlan,
    NoHedge,
    PancakeSwapDeltaNeutralLPStrategy,
    plan_hedge,
    v3_volatile_amount,
)

_CONFIG_PATH = Path(mod.__file__).with_name("config.json")
_WALLET = "0x" + "aa" * 20
ETH_PRICE = Decimal("2567")
# Real ETH/USDT 0.05% tick at ETH ≈ $2567 (token0 = ETH, both 18 decimals).
MINT_TICK = 78509


def _config(**overrides: Any) -> dict[str, Any]:
    cfg = json.loads(_CONFIG_PATH.read_text())
    cfg.update(overrides)
    return cfg


def _strategy(**overrides: Any) -> PancakeSwapDeltaNeutralLPStrategy:
    return PancakeSwapDeltaNeutralLPStrategy(config=_config(**overrides), chain="bsc", wallet_address=_WALLET)


class _Market:
    def __init__(self, eth: Decimal = ETH_PRICE, bnb: Decimal = Decimal("771")) -> None:
        self.prices = {BSC_WETH.lower(): eth, BSC_WBNB.lower(): bnb, BSC_USDT.lower(): Decimal("1")}

    def price(self, token: str) -> Decimal:
        return self.prices[token.lower()]


def _result(**extracted: Any) -> SimpleNamespace:
    return SimpleNamespace(extracted_data=extracted, error=None, position_id=None, lp_open_data=None)


def _fill(qty: str, avg: str, **extra: Any) -> dict[str, Any]:
    return {"executed_qty": qty, "avg_price": avg, "cum_quote": str(Decimal(qty) * Decimal(avg)), **extra}


def _lp_open_result(liquidity: int, tick_lower: int, tick_upper: int, amount0: int, amount1: int) -> SimpleNamespace:
    data = LPOpenData(
        position_id=4242,
        tick_lower=tick_lower,
        tick_upper=tick_upper,
        liquidity=liquidity,
        amount0=amount0,
        amount1=amount1,
    )
    return SimpleNamespace(extracted_data={"lp_open_data": data}, position_id=4242, lp_open_data=data, error=None)


def _eth_mint(volatile_eth: Decimal = Decimal("0.00195")) -> SimpleNamespace:
    """An ETH/USDT ±10% mint whose liquidity holds ``volatile_eth`` at ETH_PRICE."""
    tick_lower, tick_upper = MINT_TICK - 1054, MINT_TICK + 953
    unit = v3_volatile_amount(
        liquidity=10**18,
        tick_lower=tick_lower,
        tick_upper=tick_upper,
        price_token0_in_token1=ETH_PRICE,
        decimals0=18,
        decimals1=18,
        volatile_is_token0=True,
    )
    liquidity = int(volatile_eth / unit * 10**18)
    return _lp_open_result(liquidity, tick_lower, tick_upper, int(volatile_eth * 10**18), 5 * 10**18)


def _kinds(intents: list[Any]) -> list[IntentType]:
    return [i.intent_type for i in intents]


def _hedged(strategy: PancakeSwapDeltaNeutralLPStrategy, qty: str = "0.002", entry: str = "2567") -> None:
    strategy.state.update(
        phase=HEDGED,
        lp_position_id="4242",
        lp_volatile_minted="0.00195",
        deposited_at=1.0,
        hedge_qty=qty,
        hedge_entry_price=entry,
        hedge_reference_price=entry,
    )


def test_default_config_is_the_small_eth_mainnet_test() -> None:
    cfg = _config()
    assert cfg["pool"] == f"{BSC_WETH}/{BSC_USDT}/500"
    assert cfg["margin_token"] == BSC_USDT
    assert (cfg["hedge_margin_usd"], cfg["leverage"], cfg["min_hedge_fraction"]) == ("3", "4", "0.5")
    s = _strategy()
    assert (s.volatile_token, s.stable_token, s.fee_tier) == (BSC_WETH, BSC_USDT, 500)
    assert s.perp_market == "ETH/USD"
    assert s.qty_step == Decimal("0.001")
    assert s.max_hedge_notional_usd == Decimal("12")
    assert s._get_tracked_tokens() == [BSC_WETH, BSC_USDT]


def test_code_defaults_match_the_shipped_config() -> None:
    minimal = {"pool": f"{BSC_WETH}/{BSC_USDT}/500", "amount0": "0.00195", "amount1": "5"}
    s = PancakeSwapDeltaNeutralLPStrategy(config=minimal, chain="bsc", wallet_address=_WALLET)
    assert (s.hedge_margin_usd, s.leverage, s.min_hedge_fraction) == (Decimal(3), Decimal(4), Decimal("0.5"))
    assert s.margin_token == BSC_USDT
    assert s.pool == f"{BSC_WETH}/{BSC_USDT}/500"


@pytest.mark.parametrize("pool", [f"{BSC_WBNB}/{BSC_USDT}/500", "WBNB/USDT/500"])
def test_wbnb_pool_derives_the_bnb_market_and_step(pool: str) -> None:
    s = PancakeSwapDeltaNeutralLPStrategy(
        config={k: v for k, v in _config(pool=pool, hedge_margin_usd="4").items() if k != "perp_market"},
        chain="bsc",
        wallet_address=_WALLET,
    )
    assert s.perp_market == "BNB/USD"
    assert s.qty_step == Decimal("0.01")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"pool": "WETH/USDT"}, "VOLATILE/STABLE/FEE"),
        ({"leverage": "2.5"}, "whole number"),
        ({"leverage": "126"}, "whole number"),
        ({"leverage": "0"}, "whole number"),
        ({"range_width_pct": "0"}, "range_width_pct"),
        ({"range_width_pct": "2"}, "range_width_pct"),
        ({"max_slippage": "1"}, "max_slippage"),
        ({"delta_rebalance_threshold_pct": "0"}, "delta_rebalance_threshold_pct"),
        ({"amount0": "0"}, "amount0 and amount1"),
        ({"hedge_margin_usd": "1", "leverage": "3"}, "cannot carry the $5 minimum"),
        ({"perp_market": "SOL/USD"}, "hedge_qty_step is required for SOLUSDT"),
        ({"hedge_margin_utilization": "1.5"}, "hedge_margin_utilization"),
        ({"min_hedge_fraction": "0"}, "min_hedge_fraction"),
        ({"min_hedge_fraction": "1.5"}, "min_hedge_fraction"),
    ],
)
def test_invalid_config_is_rejected_at_construction(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message.replace("$", r"\$")):
        _strategy(**overrides)


def test_unknown_market_is_accepted_with_an_explicit_step() -> None:
    assert _strategy(perp_market="SOL/USD", hedge_qty_step="0.01").qty_step == Decimal("0.01")


@pytest.mark.parametrize(
    ("volatile", "price", "step", "budget", "qty", "over", "capped"),
    [
        # ETH: 0.00195 rounds to the 0.002 step ($5.13, above the $5 minimum).
        ("0.00195", "2567", "0.001", "7.5", "0.002", False, False),
        # ETH: 0.0012 ($3.08) is above half the 0.002 minimum order ($2.57), so 0.002 is used.
        ("0.0012", "2567", "0.001", "7.5", "0.002", True, False),
        # ETH below $2500: 0.002 is under $5, so the minimum becomes 0.003.
        ("0.00195", "2430", "0.001", "7.5", "0.003", True, False),
        # BNB: 0.006 ($4.63) is above half the 0.01 minimum ($7.71); it step-rounds to 0.01.
        ("0.006", "771", "0.01", "30", "0.01", False, False),
        # BNB: 0.0263 rounds to 0.03.
        ("0.0263", "771", "0.01", "30", "0.03", False, False),
        # Above the margin budget the short is capped.
        ("0.01", "2567", "0.001", "7.5", "0.002", False, True),
    ],
)
def test_plan_hedge_rounds_to_the_step_and_enforces_the_minimum(
    volatile: str, price: str, step: str, budget: str, qty: str, over: bool, capped: bool
) -> None:
    plan = plan_hedge(Decimal(volatile), Decimal(price), Decimal(step), Decimal("5"), Decimal(budget), Decimal("0.5"))
    assert isinstance(plan, HedgePlan)
    assert plan.qty == Decimal(qty)
    assert (plan.over_hedged, plan.capped) == (over, capped)
    assert plan.notional_usd >= Decimal("5")
    # The venue rounds size_usd / mark DOWN; the padding must land on qty at this price.
    venue_qty = (plan.size_usd / Decimal(price) / Decimal(step)).to_integral_value(rounding="ROUND_DOWN") * Decimal(
        step
    )
    assert venue_qty == plan.qty


def test_plan_hedge_refuses_when_no_hedge_is_possible() -> None:
    half = Decimal("0.5")
    none = plan_hedge(Decimal(0), ETH_PRICE, Decimal("0.001"), Decimal(5), Decimal(10), half)
    assert isinstance(none, NoHedge) and none.exposure_too_small
    # Budget below the minimum order at this price (BNB's 0.01 step is $7.71).
    broke = plan_hedge(Decimal("0.01"), Decimal("771"), Decimal("0.01"), Decimal(5), Decimal("7.5"), half)
    assert isinstance(broke, NoHedge) and not broke.exposure_too_small


@pytest.mark.parametrize(
    ("volatile", "price", "step", "fraction", "hedged"),
    [
        # 0.0009 ETH = $2.31, below half of the 0.002 ETH minimum order ($5.13).
        ("0.0009", "2567", "0.001", "0.5", False),
        ("0.0009", "2567", "0.001", "0.25", True),
        # 0.004 BNB = $3.08, below half of the 0.01 BNB minimum order ($7.71).
        ("0.004", "771", "0.01", "0.5", False),
    ],
)
def test_exposure_below_the_floor_holds_no_hedge(
    volatile: str, price: str, step: str, fraction: str, hedged: bool
) -> None:
    plan = plan_hedge(Decimal(volatile), Decimal(price), Decimal(step), Decimal(5), Decimal(100), Decimal(fraction))
    assert isinstance(plan, HedgePlan) is hedged
    if not hedged:
        assert isinstance(plan, NoHedge) and plan.exposure_too_small


@pytest.mark.parametrize("fraction", ["0.5", "0.25"])
@pytest.mark.parametrize(("price", "step"), [("2567", "0.001"), ("2430", "0.001"), ("771", "0.01")])
def test_an_over_hedge_never_exceeds_one_over_the_fraction(fraction: str, price: str, step: str) -> None:
    px, stp, frac = Decimal(price), Decimal(step), Decimal(fraction)
    for i in range(1, 400):
        volatile = stp * Decimal(i) / 40
        plan = plan_hedge(volatile, px, stp, Decimal(5), Decimal(1000), frac)
        if isinstance(plan, HedgePlan) and plan.over_hedged:
            assert plan.notional_usd / (volatile * px) <= 1 / frac


def test_v3_amount_is_exact_at_the_range_edges_for_both_orientations() -> None:
    kwargs = {"liquidity": 10**18, "tick_lower": -1000, "tick_upper": 1000, "decimals0": 18, "decimals1": 18}
    full0 = v3_volatile_amount(price_token0_in_token1=Decimal("0.5"), volatile_is_token0=True, **kwargs)
    assert v3_volatile_amount(price_token0_in_token1=Decimal("2"), volatile_is_token0=True, **kwargs) == 0
    mid0 = v3_volatile_amount(price_token0_in_token1=Decimal("1"), volatile_is_token0=True, **kwargs)
    assert 0 < mid0 < full0
    # Volatile as token1 grows with the token0 price and is zero below the range.
    assert v3_volatile_amount(price_token0_in_token1=Decimal("0.5"), volatile_is_token0=False, **kwargs) == 0
    assert v3_volatile_amount(price_token0_in_token1=Decimal("1"), volatile_is_token0=False, **kwargs) > 0


def test_phase_sequence_lp_open_deposit_settle_open(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [1000.0]
    monkeypatch.setattr(mod.time, "time", lambda: clock[0])
    s = _strategy()
    market = _Market()

    lp_open = s.decide(market)
    assert lp_open.intent_type == IntentType.LP_OPEN
    assert lp_open.protocol == "pancakeswap_v3"
    assert lp_open.pool == f"{BSC_WETH}/{BSC_USDT}/500"
    assert lp_open.range_lower == ETH_PRICE * Decimal("0.9")
    assert lp_open.range_upper == ETH_PRICE * Decimal("1.1")
    s.on_intent_executed(lp_open, True, _eth_mint())
    assert s.state["phase"] == LP_OPENED
    assert s.state["lp_position_id"] == "4242"
    assert s.state["lp_volatile_is_token0"] is True
    assert Decimal(s.state["lp_value_usd"]) == (Decimal("0.00195") * ETH_PRICE + 5).quantize(Decimal("0.01"))

    deposit = s.decide(market)
    assert deposit.intent_type == IntentType.PERP_DEPOSIT
    assert (deposit.amount, deposit.asset, deposit.protocol) == (Decimal("3"), BSC_USDT, "aster_perps")
    assert s.state["deposit_sent_at"] == 1000.0
    s.on_intent_executed(deposit, True, _result())
    assert s.state["phase"] == MARGIN_SETTLING

    clock[0] += 30
    assert s.decide(market).intent_type == IntentType.HOLD

    clock[0] += 61
    perp_open = s.decide(market)
    assert perp_open.intent_type == IntentType.PERP_OPEN
    assert perp_open.protocol == "aster_perps"
    assert perp_open.market == "ETH/USD"
    assert perp_open.is_long is False
    assert perp_open.leverage == Decimal("4")
    assert perp_open.max_slippage == Decimal("0.01")
    assert perp_open.collateral_token == BSC_USDT
    assert s.state["pending_hedge"]["qty"] == "0.002"
    assert s.state["pending_hedge"]["lp_delta_source"] == "v3_liquidity"


def test_entry_is_recorded_from_the_venue_fill() -> None:
    s = _strategy()
    s.state.update(phase=HEDGE_PENDING, lp_position_id="4242", lp_volatile_minted="0.00195", deposited_at=1.0)
    perp_open = s.decide(_Market())
    s.on_intent_executed(
        perp_open, True, _result(aster_order=_fill("0.002", "2571.4", fee="0.0018", fee_asset=BSC_USDT))
    )
    assert s.state["phase"] == HEDGED
    assert s.state["hedge_qty"] == "0.002"
    assert s.state["hedge_entry_price"] == "2571.4"
    assert s.state["hedge_reference_price"] == "2571.4"
    assert s.state["hedge_fill_measured"] is True
    assert "pending_hedge" not in s.state


def test_a_partial_open_records_the_executed_quantity() -> None:
    s = _strategy()
    s.state.update(phase=HEDGE_PENDING, lp_position_id="4242", lp_volatile_minted="0.004", deposited_at=1.0)
    s.on_intent_executed(s.decide(_Market()), True, _result(aster_order=_fill("0.001", "2567")))
    assert s.state["hedge_qty"] == "0.001"


def test_an_unmeasured_fill_keeps_the_entry_empty_not_zero() -> None:
    s = _strategy()
    s.state.update(phase=HEDGE_PENDING, lp_position_id="4242", lp_volatile_minted="0.00195", deposited_at=1.0)
    s.on_intent_executed(s.decide(_Market()), True, None)
    assert s.state["phase"] == HEDGED
    assert s.state["hedge_entry_price"] is None
    assert s.state["hedge_fill_measured"] is False
    assert s.state["hedge_qty"] == "0.002"
    assert s.state["hedge_reference_price"] == str(ETH_PRICE)


def test_drift_closes_then_reopens_at_the_new_size() -> None:
    s = _strategy()
    _hedged(s, qty="0.002", entry="2567")
    s.state["lp_volatile_minted"] = "0.0035"

    assert s.decide(_Market(eth=Decimal("2600"))).intent_type == IntentType.HOLD

    drifted = _Market(eth=Decimal("2430"))
    close = s.decide(drifted)
    assert close.intent_type == IntentType.PERP_CLOSE
    assert (close.market, close.is_long, close.protocol, close.position_id) == ("ETH/USD", False, "aster_perps", None)
    s.on_intent_executed(close, True, _result(aster_order=_fill("0.002", "2430", realized_pnl="0.27")))
    assert s.state["phase"] == HEDGE_PENDING
    assert "hedge_qty" not in s.state

    reopen = s.decide(drifted)
    assert reopen.intent_type == IntentType.PERP_OPEN
    assert s.state["pending_hedge"]["qty"] == "0.004"
    s.on_intent_executed(reopen, True, _result(aster_order=_fill("0.004", "2429.5")))
    assert s.state["phase"] == HEDGED
    assert s.state["hedge_reference_price"] == "2429.5"


def test_drift_with_an_unchanged_size_re_anchors_without_trading() -> None:
    s = _strategy()
    _hedged(s)
    hold = s.decide(_Market(eth=Decimal("2700")))
    assert hold.intent_type == IntentType.HOLD
    assert s.state["hedge_reference_price"] == "2700"
    assert s.state["phase"] == HEDGED


def test_a_partly_filled_close_stays_hedged_and_retries() -> None:
    s = _strategy()
    _hedged(s, qty="0.003")
    drifted = _Market(eth=Decimal("2800"))
    close = s.decide(drifted)
    assert close.intent_type == IntentType.PERP_CLOSE
    failed = _result(aster_order=_fill("0.001", "2800"))
    failed.error = "position only partly closed: 0.001 closed, 0.002 still open"
    s.on_intent_executed(close, False, failed)
    assert s.state["phase"] == HEDGED
    assert s.state["hedge_qty"] == "0.002"
    assert s.state["close_pending"] is True
    # The latch retries even after the price returns to the reference.
    assert s.decide(_Market()).intent_type == IntentType.PERP_CLOSE
    assert _kinds(s.generate_teardown_intents(TeardownMode.SOFT))[0] == IntentType.PERP_CLOSE


def test_lp_open_without_a_position_id_fails_closed() -> None:
    s = _strategy()
    lp_open = s.decide(_Market())
    s.on_intent_executed(lp_open, True, _result())
    assert s.state["phase"] == LP_OPEN_FAILED
    assert s.decide(_Market()).intent_type == IntentType.HOLD
    assert s.generate_teardown_intents(TeardownMode.SOFT) == []


def test_diamond_era_state_enters_recovery() -> None:
    s = _strategy()
    s.load_persistent_state({"phase": HEDGED, "perp_trade_hash": "0x" + "ab" * 32, "lp_position_id": "7"})
    assert s.state["phase"] == RECOVERY_REQUIRED
    assert s.decide(_Market()).intent_type == IntentType.HOLD
    assert s.generate_teardown_intents(TeardownMode.SOFT) == []
    legacy = [p for p in s.get_open_positions().positions if p.protocol == "pancakeswap_perps"]
    assert legacy and legacy[0].details["state"] == "unknown"


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ({"phase": IDLE}, []),
        ({"phase": LP_OPENED, "lp_position_id": "1"}, [IntentType.LP_CLOSE]),
        (
            {"phase": LP_OPENED, "lp_position_id": "1", "deposit_sent_at": 5.0},
            [IntentType.LP_CLOSE, IntentType.PERP_WITHDRAW],
        ),
        (
            {"phase": MARGIN_SETTLING, "lp_position_id": "1", "deposit_sent_at": 5.0, "deposited_at": 6.0},
            [IntentType.LP_CLOSE, IntentType.PERP_WITHDRAW],
        ),
        (
            {"phase": HEDGE_PENDING, "lp_position_id": "1", "deposited_at": 6.0},
            [IntentType.LP_CLOSE, IntentType.PERP_WITHDRAW],
        ),
        (
            {"phase": HEDGED, "lp_position_id": "1", "deposited_at": 6.0, "hedge_qty": "0.002"},
            [IntentType.PERP_CLOSE, IntentType.LP_CLOSE, IntentType.PERP_WITHDRAW],
        ),
    ],
)
def test_teardown_intents_follow_where_the_money_is(state: dict[str, Any], expected: list[IntentType]) -> None:
    s = _strategy()
    s.load_persistent_state(state)
    intents = s.generate_teardown_intents(TeardownMode.SOFT)
    assert _kinds(intents) == expected
    withdraws = [i for i in intents if i.intent_type == IntentType.PERP_WITHDRAW]
    assert all(w.amount == "all" and w.asset == BSC_USDT and w.protocol == "aster_perps" for w in withdraws)


def test_a_deposit_broadcast_but_never_confirmed_is_withdrawn() -> None:
    s = _strategy()
    s.state.update(phase=LP_OPENED, lp_position_id="1")
    s.decide(_Market())
    restored = _strategy()
    restored.load_persistent_state(dict(s.state))
    assert _kinds(restored.generate_teardown_intents(TeardownMode.SOFT)) == [
        IntentType.LP_CLOSE,
        IntentType.PERP_WITHDRAW,
    ]


def test_teardown_callbacks_walk_to_done() -> None:
    s = _strategy()
    _hedged(s)
    for intent in s.generate_teardown_intents(TeardownMode.SOFT):
        s.on_intent_executed(intent, True, None)
    assert s.state["phase"] == "DONE"
    assert s.generate_teardown_intents(TeardownMode.SOFT) == []
    assert s.decide(_Market()).intent_type == IntentType.HOLD


def test_open_positions_report_the_lp_and_the_aster_short() -> None:
    s = _strategy()
    _hedged(s, qty="0.002", entry="2500")
    s.state["lp_value_usd"] = "10.01"
    positions = {p.position_type: p for p in s.get_open_positions().positions}
    assert set(positions) == {PositionType.LP, PositionType.PERP}
    lp, perp = positions[PositionType.LP], positions[PositionType.PERP]
    assert (lp.position_id, lp.protocol, lp.value_usd) == ("4242", "pancakeswap_v3", Decimal("10.01"))
    assert perp.protocol == "aster_perps"
    assert perp.position_id == "aster:ETH/USD"
    assert perp.details["market"] == "ETH/USD"
    assert perp.details["is_long"] is False
    assert perp.value_usd == Decimal("5.00")


def test_open_positions_omit_the_perp_when_flat() -> None:
    s = _strategy()
    s.state.update(phase=HEDGE_PENDING, lp_position_id="4242", deposited_at=1.0)
    assert [p.position_type for p in s.get_open_positions().positions] == [PositionType.LP]


def test_the_v3_delta_tracks_price_through_the_range() -> None:
    s = _strategy()
    s.on_intent_executed(s.decide(_Market()), True, _eth_mint())
    at_mint, source = s._lp_volatile_amount(ETH_PRICE, Decimal(1))
    assert source == "v3_liquidity"
    assert abs(at_mint - Decimal("0.00195")) < Decimal("0.0000001")
    lower, _ = s._lp_volatile_amount(Decimal("2400"), Decimal(1))
    assert lower > at_mint
    above_range, _ = s._lp_volatile_amount(Decimal("2900"), Decimal(1))
    assert above_range == 0


def test_a_hedge_whose_exposure_falls_below_the_floor_is_closed_without_drift() -> None:
    s = _strategy()
    _hedged(s, qty="0.002", entry=str(ETH_PRICE))
    s.state["lp_volatile_minted"] = "0.0009"
    close = s.decide(_Market())
    assert close.intent_type == IntentType.PERP_CLOSE
    s.on_intent_executed(close, True, _result(aster_order=_fill("0.002", "2567")))
    assert s.state["phase"] == HEDGE_PENDING
    assert s.decide(_Market()).intent_type == IntentType.HOLD


def test_a_lower_fraction_keeps_the_small_hedge() -> None:
    s = _strategy(min_hedge_fraction="0.25")
    _hedged(s, qty="0.002", entry=str(ETH_PRICE))
    s.state["lp_volatile_minted"] = "0.0009"
    assert s.decide(_Market()).intent_type == IntentType.HOLD


def _receipt(tx_hash: str, status: int) -> dict[str, object]:
    return {
        "tx_hash": tx_hash,
        "block_number": 42,
        "block_hash": "0xblock",
        "gas_used": 50_000,
        "effective_gas_price": "1",
        "status": status,
        "logs": [],
    }


def _deposit_failure(*, approve_status: int, deposit_status: int) -> GatewayExecutionResult:
    return GatewayExecutionResult(
        success=False,
        tx_hashes=["0xapprove", "0xdeposit"],
        total_gas_used=100_000,
        receipts=[_receipt("0xapprove", approve_status), _receipt("0xdeposit", deposit_status)],
        execution_id="exec-deposit",
        error="deposit reverted",
        submission_provenance=SubmissionProvenance.ATTEMPTED,
        execution_plan_hash="c" * 64,
        submission_transactions=[
            SubmissionTransactionEvidence("0xapprove", TransactionRole.SETUP_APPROVAL, ReplayPolicy.RECOMPILE_ONLY),
            SubmissionTransactionEvidence("0xdeposit", TransactionRole.ACTION, ReplayPolicy.NEVER),
        ],
    )


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
        (_deposit_failure(approve_status=1, deposit_status=0), True),
        (_deposit_failure(approve_status=0, deposit_status=0), True),
        # The deposit landed but the runner still reports failure.
        (_deposit_failure(approve_status=1, deposit_status=1), False),
        # An enforced reconciliation incident: the transaction succeeded.
        (SimpleNamespace(success=True, error="reconciliation incident", extracted_data={}), False),
        # Execution was entered but no result was retained.
        (SimpleNamespace(error="execution raised"), False),
        (None, False),
    ],
)
def test_a_failed_deposit_clears_the_withdraw_marker_only_when_nothing_moved(result: Any, cleared: bool) -> None:
    s = _strategy()
    s.state.update(phase=LP_OPENED, lp_position_id="1")
    deposit = s.decide(_Market())
    s.on_intent_executed(deposit, False, result)
    assert ("deposit_sent_at" not in s.state) is cleared
    assert s.state["phase"] == (LP_OPENED if cleared else DEPOSIT_UNVERIFIED)
    expected = [IntentType.LP_CLOSE] if cleared else [IntentType.LP_CLOSE, IntentType.PERP_WITHDRAW]
    assert _kinds(s.generate_teardown_intents(TeardownMode.SOFT)) == expected
    # Only a proven non-execution re-sends the deposit.
    expected_next = IntentType.PERP_DEPOSIT if cleared else IntentType.HOLD
    assert s.decide(_Market()).intent_type == expected_next


def _venue_refusal(**extracted: Any) -> ExecutionResult:
    result = ExecutionResult(
        success=False,
        phase=ExecutionPhase.COMPLETE,
        error="venue refused",
        submission_provenance=SubmissionProvenance.NOT_ATTEMPTED,
    )
    result.extracted_data = dict(extracted)
    return result


def _pending_open(s: PancakeSwapDeltaNeutralLPStrategy) -> Any:
    s.state.update(phase=HEDGE_PENDING, lp_position_id="4242", lp_volatile_minted="0.00195", deposited_at=1.0)
    perp_open = s.decide(_Market())
    assert perp_open.intent_type == IntentType.PERP_OPEN
    assert "hedge_open_sent_at" in s.state
    return perp_open


def test_a_venue_refused_open_clears_the_marker_and_retries() -> None:
    s = _strategy()
    perp_open = _pending_open(s)
    s.on_intent_executed(perp_open, False, _venue_refusal())
    assert "hedge_open_sent_at" not in s.state
    assert s.generate_teardown_intents(TeardownMode.SOFT)[0].intent_type == IntentType.LP_CLOSE
    assert s.decide(_Market()).intent_type == IntentType.PERP_OPEN


@pytest.mark.parametrize(
    "result",
    [
        SimpleNamespace(error="execution raised"),
        None,
        _venue_refusal(offchain_filled_size="0.001"),
        _venue_refusal(aster_order={"executed_qty": "0.001"}),
    ],
)
def test_an_unproven_open_stays_visible_to_teardown(result: Any) -> None:
    from almanak.framework.execution.offchain_venue import OFFCHAIN_FILLED_SIZE_KEY

    if isinstance(result, ExecutionResult) and "offchain_filled_size" in result.extracted_data:
        result.extracted_data = {OFFCHAIN_FILLED_SIZE_KEY: "0.001"}
    s = _strategy()
    perp_open = _pending_open(s)
    s.on_intent_executed(perp_open, False, result)
    assert "hedge_open_sent_at" in s.state
    assert s.state["open_unverified"] is True
    assert s.decide(_Market()).intent_type == IntentType.HOLD
    assert "hedge_open_sent_at" in s.state
    perps = [p for p in s.get_open_positions().positions if p.position_type == PositionType.PERP]
    assert len(perps) == 1
    assert perps[0].details["market"] == "ETH/USD"
    assert perps[0].details["is_long"] is False
    assert perps[0].details["open_outcome_unknown"] is True
    assert _kinds(s.generate_teardown_intents(TeardownMode.SOFT)) == [
        IntentType.PERP_CLOSE,
        IntentType.LP_CLOSE,
        IntentType.PERP_WITHDRAW,
    ]


def test_a_successful_open_clears_the_marker() -> None:
    s = _strategy()
    perp_open = _pending_open(s)
    s.on_intent_executed(perp_open, True, _result(aster_order=_fill("0.002", "2567")))
    assert "hedge_open_sent_at" not in s.state
    assert "open_outcome_unknown" not in s.get_open_positions().positions[-1].details


def test_a_withdraw_keeps_the_deposit_markers_while_a_hedge_is_held() -> None:
    s = _strategy()
    _hedged(s)
    s.state["deposit_sent_at"] = 1.0
    withdraw = s.generate_teardown_intents(TeardownMode.SOFT)[-1]
    assert withdraw.intent_type == IntentType.PERP_WITHDRAW
    s.on_intent_executed(withdraw, True, _result(aster_withdraw={"amount": "0.5"}))
    assert s._deposit_may_have_landed()
    assert s.state["phase"] == HEDGED
    close = s.generate_teardown_intents(TeardownMode.SOFT)[0]
    s.on_intent_executed(close, True, _result(aster_order=_fill("0.002", "2567")))
    assert _kinds(s.generate_teardown_intents(TeardownMode.SOFT)) == [IntentType.LP_CLOSE, IntentType.PERP_WITHDRAW]


def test_a_withdraw_keeps_the_deposit_markers_while_an_open_is_unverified() -> None:
    s = _strategy()
    s.on_intent_executed(_pending_open(s), False, None)
    withdraw = s.generate_teardown_intents(TeardownMode.SOFT)[-1]
    s.on_intent_executed(withdraw, True, _result())
    assert s._deposit_may_have_landed()


def test_a_withdraw_with_no_hedge_clears_the_deposit_markers() -> None:
    s = _strategy()
    s.state.update(phase=WINDING_DOWN, deposited_at=1.0, deposit_sent_at=1.0)
    withdraw = s.generate_teardown_intents(TeardownMode.SOFT)[-1]
    s.on_intent_executed(withdraw, True, _result())
    assert not s._deposit_may_have_landed()
    assert s.state["phase"] == "DONE"


def test_a_successful_close_releases_the_latch() -> None:
    s = _strategy()
    _hedged(s, qty="0.003")
    close = s.decide(_Market(eth=Decimal("2800")))
    assert s.state["close_pending"] is True
    s.on_intent_executed(close, True, _result(aster_order=_fill("0.003", "2800")))
    assert "close_pending" not in s.state
    assert s.state["phase"] == HEDGE_PENDING


def test_a_hedge_closed_outside_the_strategy_is_released_by_an_already_flat_close() -> None:
    s = _strategy()
    _hedged(s, qty="0.003")
    close = s.decide(_Market(eth=Decimal("2800")))
    # Liquidated or closed by hand: the venue answers already_flat, a success with no fill.
    s.on_intent_executed(
        close, True, _result(aster_order={"already_flat": True, "executed_qty": "0", "cum_quote": "0"})
    )
    assert s.state["phase"] == HEDGE_PENDING
    assert not any(k in s.state for k in ("hedge_qty", "close_pending", "hedge_open_sent_at"))
    assert [i.intent_type for i in s.generate_teardown_intents(TeardownMode.SOFT)][0] != IntentType.PERP_CLOSE


def test_a_partly_filled_open_is_topped_up_without_drift() -> None:
    s = _strategy()
    s.state.update(phase=HEDGE_PENDING, lp_position_id="4242", lp_volatile_minted="0.004", deposited_at=1.0)
    perp_open = s.decide(_Market())
    assert s.state["pending_hedge"]["qty"] == "0.004"
    s.on_intent_executed(perp_open, True, _result(aster_order=_fill("0.002", "2560")))
    assert s.state["hedge_underfilled"] is True

    top_up = s.decide(_Market())
    assert top_up.intent_type == IntentType.PERP_OPEN
    assert s.state["pending_hedge"]["qty"] == "0.002"
    assert s.state["pending_hedge"]["top_up"] is True
    s.on_intent_executed(top_up, True, _result(aster_order=_fill("0.002", "2570")))
    assert Decimal(s.state["hedge_qty"]) == Decimal("0.004")
    assert Decimal(s.state["hedge_entry_price"]) == Decimal("2565")
    assert s.state["hedge_reference_price"] == "2560"
    assert "hedge_underfilled" not in s.state
    assert s.decide(_Market()).intent_type == IntentType.HOLD


def test_a_shortfall_below_the_minimum_order_is_not_topped_up() -> None:
    s = _strategy()
    s.state.update(phase=HEDGE_PENDING, lp_position_id="4242", lp_volatile_minted="0.003", deposited_at=1.0)
    perp_open = s.decide(_Market())
    s.on_intent_executed(perp_open, True, _result(aster_order=_fill("0.002", "2567")))
    assert s.decide(_Market()).intent_type == IntentType.HOLD
    assert "hedge_underfilled" not in s.state


def test_an_unaffordable_minimum_keeps_the_existing_hedge() -> None:
    # A $6 budget; at $2,430 the smallest ETH order is 0.003 ($7.29).
    s = _strategy(hedge_margin_usd="1.5", leverage="4", hedge_reference_price_usd="900")
    _hedged(s, qty="0.003", entry="2567")
    s.state["lp_volatile_minted"] = "0.004"
    hold = s.decide(_Market(eth=Decimal("2430")))
    assert hold.intent_type == IntentType.HOLD
    assert s.state["hedge_qty"] == "0.003"
    assert "close_pending" not in s.state


def test_wbnb_defaults_are_rejected_for_the_bnb_minimum_order() -> None:
    with pytest.raises(ValueError, match="BNBUSDT's step-rounded minimum order"):
        _strategy(pool=f"{BSC_WBNB}/{BSC_USDT}/500", perp_market="BNB/USD")
    assert _strategy(pool=f"{BSC_WBNB}/{BSC_USDT}/500", perp_market="BNB/USD", hedge_margin_usd="4").leverage == 4
    with pytest.raises(ValueError, match="ETHUSDT's step-rounded minimum order"):
        _strategy(hedge_reference_price_usd="8000")


def test_the_effective_minimum_order_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    s = _strategy()
    s.state.update(phase=HEDGE_PENDING, lp_position_id="4242", lp_volatile_minted="0.00195", deposited_at=1.0)
    with caplog.at_level("INFO", logger=mod.__name__):
        s.decide(_Market(eth=Decimal("2430")))
    assert "effective minimum order 0.003" in caplog.text


def test_v3_amount_scales_mixed_decimals() -> None:
    price = Decimal("2000")
    raw_price = float(price) * 10 ** (6 - 18)
    tick = int(math.log(raw_price) / math.log(1.0001))
    tick_lower, tick_upper = tick - 1000, tick + 1000
    sb, sp = 1.0001 ** (tick_upper / 2), math.sqrt(raw_price)
    liquidity = int(10**18 * sp * sb / (sb - sp))
    eth = v3_volatile_amount(
        liquidity=liquidity,
        tick_lower=tick_lower,
        tick_upper=tick_upper,
        price_token0_in_token1=price,
        decimals0=18,
        decimals1=6,
        volatile_is_token0=True,
    )
    assert abs(eth - 1) < Decimal("0.000001")
    # Volatile as token1 (6-decimal stable token0): y = L (sqrt P - sqrt Pa).
    raw_inv = (1 / float(price)) * 10 ** (18 - 6)
    tick_inv = int(math.log(raw_inv) / math.log(1.0001))
    lo, hi = tick_inv - 1000, tick_inv + 1000
    liquidity_inv = int(10**18 / (math.sqrt(raw_inv) - 1.0001 ** (lo / 2)))
    eth_inv = v3_volatile_amount(
        liquidity=liquidity_inv,
        tick_lower=lo,
        tick_upper=hi,
        price_token0_in_token1=1 / price,
        decimals0=6,
        decimals1=18,
        volatile_is_token0=False,
    )
    assert abs(eth_inv - 1) < Decimal("0.000001")


@pytest.mark.parametrize(("volatile_is_token0", "decimals"), [(True, (18, 6)), (False, (6, 18))])
def test_minted_amounts_use_each_tokens_own_decimals(volatile_is_token0: bool, decimals: tuple[int, int]) -> None:
    s = _strategy()
    s._pool_orientation = lambda: (volatile_is_token0, decimals)  # type: ignore[method-assign]
    volatile_raw, stable_raw = 10**18, 2000 * 10**6
    amount0, amount1 = (volatile_raw, stable_raw) if volatile_is_token0 else (stable_raw, volatile_raw)
    s._record_lp_shape(
        SimpleNamespace(amount0=amount0, amount1=amount1, liquidity=None, tick_lower=None, tick_upper=None)
    )
    assert Decimal(s.state["lp_volatile_minted"]) == 1
    assert Decimal(s.state["lp_stable_minted"]) == 2000


def test_an_unknown_phase_enters_recovery() -> None:
    s = _strategy()
    s.state.update(phase="BOGUS", lp_position_id="1")
    assert s.decide(_Market()).intent_type == IntentType.HOLD
    assert s.state["phase"] == RECOVERY_REQUIRED
    assert s.generate_teardown_intents(TeardownMode.SOFT) == []


@pytest.mark.parametrize(
    ("pool", "token0"),
    [(f"{BSC_WETH}/{BSC_USDT}/500", BSC_WETH), (f"{BSC_WBNB}/{BSC_USDT}/500", BSC_USDT)],
)
def test_lp_details_label_tokens_in_pool_order(pool: str, token0: str) -> None:
    s = _strategy(pool=pool, perp_market="ETH/USD")
    s.state.update(phase=LP_OPENED, lp_position_id="1")
    lp = s.get_open_positions().positions[0]
    assert lp.details["token0"] == token0
    assert {lp.details["token0"], lp.details["token1"]} == {lp.details["volatile_token"], lp.details["stable_token"]}


def test_a_flat_close_releases_an_unverified_top_up() -> None:
    s = _strategy()
    _hedged(s, qty="0.002")
    s.state.update(hedge_underfilled=True, lp_volatile_minted="0.004")
    top_up = s.decide(_Market())
    assert top_up.intent_type == IntentType.PERP_OPEN
    s.on_intent_executed(top_up, False, None)
    assert "hedge_open_sent_at" in s.state
    close = s.decide(_Market(eth=Decimal("2800")))
    assert close.intent_type == IntentType.PERP_CLOSE
    s.on_intent_executed(close, True, _result(aster_order=_fill("0.004", "2800")))
    assert "hedge_open_sent_at" not in s.state
    assert "open_unverified" not in s.state
    assert s.decide(_Market(eth=Decimal("2800"))).intent_type == IntentType.PERP_OPEN


def test_an_open_resolved_without_a_callback_is_re_planned() -> None:
    s = _strategy()
    _pending_open(s)
    # The runner released the barrier on proven non-execution: no callback.
    reopen = s.decide(_Market())
    assert reopen.intent_type == IntentType.PERP_OPEN
    assert "open_unverified" not in s.state


def test_a_successful_open_clears_a_stale_unverified_flag() -> None:
    s = _strategy()
    s.state["open_unverified"] = True
    perp_open = _pending_open(s)
    s.on_intent_executed(perp_open, True, _result(aster_order=_fill("0.002", "2567")))
    assert "open_unverified" not in s.state


def test_a_refused_top_up_is_given_up() -> None:
    s = _strategy()
    _hedged(s, qty="0.002")
    s.state.update(hedge_underfilled=True, lp_volatile_minted="0.004")
    top_up = s.decide(_Market())
    assert top_up.intent_type == IntentType.PERP_OPEN
    s.on_intent_executed(top_up, False, _venue_refusal())
    assert "hedge_underfilled" not in s.state
    assert s.decide(_Market()).intent_type == IntentType.HOLD
    assert s.state["hedge_qty"] == "0.002"
