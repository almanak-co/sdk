"""The phase 4 reference lifecycle, shared by the offline and fork tests (blueprint 30, 10).

The reference strategy trades BTC isolated at 10x with resting and protective orders.
It deposits USDC into the Safe's HyperCore account and sets isolated leverage while flat.
It then rests a `Gtc` buy below the book, which a scripted mark fills, adds isolated
margin, protects half the position with a reduce-only stop-loss and half with a
take-profit (live reduce-only orders together never exceed the owned position), arms the
dead man's switch and lets it fire, protects again, and lets a scripted mark trigger the
take-profit. It then cancels the stop-loss, named by its cloid and signed by its retained
oid, and exits the rest. Teardown is the reverse fault: with a fresh position, a
stop-loss and a resting post-only buy, the first cancel is lost, so teardown halts with
the position and both orders retained and reported; once the cancel is past its expiry it
is released, and teardown cancels both orders, closes the position and withdraws to the
Safe on HyperEVM.

Around that path the driver injects the faults of section 10's evidence row for phase 4:
a withheld answer for a resting order, a restart while it rests, a post-only order the
venue refuses, and the lost cancel. `run` supplies the custody side and the clock, as in
the phase 3 driver. Every HyperCore effect comes from the synthesized model, prices
included: every mark below is scripted, never an observation.
"""

from decimal import Decimal
from pathlib import Path

import pytest

from almanak.connectors.hyperliquid.effects import Book
from almanak.connectors.hyperliquid.trading import (
    RestingOrder,
    Trading,
    TradingPolicy,
    resolve_target,
)
from almanak.connectors.hyperliquid.trading_runtime import Trader, require_complete
from almanak.connectors.hyperliquid.trading_store import HyperCoreState, TradingStore
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import ActiveAssetData, ClearinghouseState, InfoResult, typed
from tests.hypercore_trading_support import COIN, LEVERAGE, SLIPPAGE_BPS, step

SIZE = Decimal("0.002")
# Synthesized prices around the seed's 78394 mark: a resting buy below the book, the mark
# that fills it, the stop-loss and take-profit triggers, and the mark that takes profit.
RESTING_BUY = Decimal("77000")
FILL_MARK = "76900"
STOP = Decimal("75000")
TAKE = Decimal("80000")
PROFIT_MARK = "80500"
MARGIN = Decimal("2")
# The model's register states A4, so an order with no record and no fill past its expiry
# is non-execution here, and A10, so an isolated margin change is admitted although no
# record of it exists. On mainnet T4 and T10 are open and the policy's defaults refuse
# both.
MODEL_POLICY = TradingPolicy(release_unknown_after_expiry=True, margin_without_record=True)


def open_store(
    path: Path,
    deployment: str,
    custody,
    meta: InfoResult,
    start: int,
    margin="isolated",
    policy=MODEL_POLICY,
) -> TradingStore:
    target = resolve_target(meta, COIN, LEVERAGE, SLIPPAGE_BPS, margin)
    trading = Trading(deployment=deployment, safe=custody.safe, targets=(target,), policy=policy)
    store = TradingStore(path)
    store.create(
        HyperCoreState(custody=custody, trading=trading, book=Book(safe=custody.safe, cursor=start))
    )
    return store


def isolated_leverage(run, trader: Trader, report: dict) -> None:
    """Isolated 10x on BTC, set while the Safe holds no position and no order there."""
    leverage = trader.leverage(COIN)
    active = typed(trader.observe(ActiveAssetData(user=trader.state().trading.safe, coin=COIN)))
    assert leverage.status == "applied"
    assert (active.leverage.type, active.leverage.value) == ("isolated", LEVERAGE)
    step(report, "isolated_leverage", attempt=leverage.id)


def resting_entry_withheld(run, trader: Trader, report: dict) -> None:
    """A Gtc buy below the book, its answer withheld: resolved by orderStatus of its
    cloid, after which it is an owned order resting on the venue (D6)."""
    run.model.withhold("order")
    order = RestingOrder(is_buy=True, size=SIZE, price=RESTING_BUY, tif="Gtc")
    entry = trader.rest(COIN, order)
    assert entry.status == "sent"
    trader.require_resolved()
    (owned,) = trader.state().trading.live()
    assert (owned.status, owned.cloid) == ("open", trader.state().trading.attempt(entry.id).cloid)
    step(report, "resting_entry_withheld", attempt=entry.id, oid=owned.oid)


def restart_while_resting(run, trader: Trader, report: dict) -> None:
    """A new trader over the same document restarts past the resting order, which is
    owned state, and decides again while it rests: a post-only buy that would cross is
    refused by the venue and released."""
    fresh = Trader(trader.store, trader.venue, trader.signer)
    (owned,) = fresh.restart().trading.live()
    assert owned.status == "open" and owned.evidence is not None
    crossing = RestingOrder(is_buy=True, size=SIZE, price=Decimal("78400"), tif="Alo")
    refused = fresh.rest(COIN, crossing)
    assert refused.status == "not_applied" and fresh.state().trading.live() == (owned,)
    step(report, "restart_while_resting", oid=owned.oid, refused=refused.answer)


def mark_fills_resting(run, trader: Trader, report: dict) -> None:
    """A scripted mark through the resting buy fills it at its limit: the fill is the
    strategy's own by its cloid and oid, and the owned order is filled."""
    run.model.move_mark(COIN, FILL_MARK, "synthesized drop through the resting buy")
    state = trader.reconcile()
    assert state.book.positions == {COIN: SIZE} and state.book.owned == {COIN: SIZE}
    assert trader.refresh().trading.live() == ()
    step(report, "mark_fills_resting", fills=[e.entry for e in state.book.effects])


def margin_added(run, trader: Trader, report: dict) -> None:
    """2 USDC moves into the isolated position; its answer alone resolves it (T10)."""
    before = isolated_raw(trader)
    added = trader.margin(COIN, MARGIN)
    assert added.status == "applied" and isolated_raw(trader) == before + MARGIN
    trader.reconcile()
    step(report, "margin_added", attempt=added.id, raw_usd=str(before + MARGIN))


def isolated_raw(trader: Trader) -> Decimal:
    account = typed(trader.observe(ClearinghouseState(user=trader.state().trading.safe)))
    (held,) = account.assetPositions
    assert held.position.leverage.type == "isolated"
    return Decimal(held.position.leverage.rawUsd)


def protect(trader: Trader) -> tuple[int, int]:
    """A reduce-only stop-loss and take-profit, each over half the owned position."""
    stop = trader.protect(COIN, "sl", STOP, SIZE / 2)
    take = trader.protect(COIN, "tp", TAKE, SIZE / 2)
    assert stop is not None and take is not None
    assert (stop.status, take.status) == ("applied", "applied")
    return stop.oids[0], take.oids[0]


def dead_mans_switch(run, trader: Trader, report: dict) -> None:
    """Protective orders rest, the dead man's switch is armed and the runtime goes
    quiet past it: HyperCore cancels every open order, and a restart reads both owned
    orders as cancelled by it and protects the position again."""
    protected = protect(trader)
    armed = trader.schedule_cancel(run.now() + 60_000)
    assert armed.status == "applied" and trader.state().trading.schedule is not None
    run.advance(90)
    fresh = Trader(trader.store, trader.venue, trader.signer)
    state = fresh.restart()
    assert {state.trading.owned(oid).status for oid in protected} == {"scheduledCancel"}
    assert not state.trading.armed(run.now()) and state.book.owned == {COIN: SIZE}
    report["protective"] = protect(trader)
    step(report, "dead_mans_switch", cancelled=protected, schedule=armed.id)


def take_profit(run, trader: Trader, report: dict) -> None:
    """A scripted mark through the take-profit triggers it and closes its half. The
    stop-loss, named by its cloid, is cancelled by its retained oid, which frees the
    owned position for a reduce-only exit of the rest."""
    stop, take = report["protective"]
    run.model.move_mark(COIN, PROFIT_MARK, "synthesized rise through the take-profit")
    assert trader.reconcile().book.positions == {COIN: SIZE / 2}
    state = trader.refresh()
    assert (state.trading.owned(take).status, state.trading.owned(stop).status) == (
        "filled",
        "open",
    )
    cancelled = trader.cancel_by_cloid(state.trading.owned(stop).cloid)
    assert cancelled.status == "applied" and cancelled.request.action.oid == stop
    exit_order = trader.exit(COIN)
    assert exit_order is not None and exit_order.status == "applied"
    assert trader.reconcile().book.positions == {} and trader.state().trading.live() == ()
    step(report, "take_profit", take=take, stop=stop, cancel=cancelled.id, exit=exit_order.id)


def teardown_lost_cancel(run, trader: Trader, report: dict) -> None:
    """The reverse fault: teardown's first cancel is lost, so teardown halts with the
    position and both owned orders retained and reported. Once the cancel is past its
    expiry it is released, and teardown cancels both orders and closes the position."""
    held = setup_for_teardown(trader)
    run.model.lose("cancel")
    verdict = trader.teardown()
    assert verdict.positions == {COIN: SIZE} and sorted(verdict.own) == sorted(held)
    assert len(verdict.unresolved) == 1
    with pytest.raises(DataError) as halted:
        require_complete(verdict)
    step(report, "teardown_lost_cancel", halt=halted.value.kind, verdict=halted.value.details)
    run.advance(trader.state().trading.policy.expiry // 1000 + 2)
    completed = trader.teardown()
    require_complete(completed)
    statuses = {trader.state().trading.owned(oid).status for oid in held}
    assert statuses == {"canceled"} and trader.reconcile().book.positions == {}
    step(report, "teardown_completed", verdict=completed.model_dump(mode="json"))


def setup_for_teardown(trader: Trader) -> list[int]:
    """A fresh position with a stop-loss and a resting post-only buy below the book."""
    entry = trader.enter(COIN, True, SIZE)
    assert entry.status == "applied"
    trader.reconcile()
    stop = trader.protect(COIN, "sl", Decimal("78000"))
    order = RestingOrder(is_buy=True, size=SIZE / 2, price=Decimal("79000"), tif="Alo")
    resting = trader.rest(COIN, order)
    assert stop is not None and (stop.status, resting.status) == ("applied", "applied")
    return [stop.oids[0], resting.oids[0]]


STEPS = (
    isolated_leverage,
    resting_entry_withheld,
    restart_while_resting,
    mark_fills_resting,
    margin_added,
    dead_mans_switch,
    take_profit,
    teardown_lost_cancel,
)


def resting_lifecycle(run, trader: Trader) -> dict:
    """Run the phase 4 reference strategy with its faults; returns the run's report."""
    report: dict = {}
    run.fund_perp(report)
    for each in STEPS:
        each(run, trader, report)
    run.withdraw_all(report, trader.reconcile().book.cash)
    state = trader.state()
    report["book"] = state.book.model_dump(mode="json")
    report["attempts"] = [a.model_dump(mode="json") for a in state.trading.attempts]
    report["orders"] = [o.model_dump(mode="json") for o in state.trading.orders]
    return report
