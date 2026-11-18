"""Hyperliquid phase 4 offline: resting and trigger orders, cancels, the dead man's
switch and isolated margin (blueprint 30, sections 5 and 10).

The reference lifecycle here runs against the synthesized model on a fake chain;
`tests/fork/test_hypercore_resting.py` runs it on a HyperEVM fork. The cases after it
cover each admission refusal, answer and recovery rule, and the model's own rules, one by
one. Signing is held to the official SDK's fixtures in `tests/test_hypercore_trading.py`.
"""

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.connectors.hyperliquid import fork_model
from almanak.connectors.hyperliquid import trading as rules
from almanak.connectors.hyperliquid.admission import admit
from almanak.connectors.hyperliquid.custody import Custody
from almanak.connectors.hyperliquid.fork_model import HyperCoreModel
from almanak.connectors.hyperliquid.fork_service import ModelService
from almanak.connectors.hyperliquid.trading import RestingOrder, TradingPolicy
from almanak.connectors.hyperliquid.trading_runtime import Trader, teardown_verdict
from almanak.gateway import hypercore_signing as signing
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_agent_keys import LocalAgentKeys
from almanak.gateway.hypercore_exchange import ANSWER
from almanak.gateway.hypercore_info import (
    ClearinghouseState,
    InfoRequest,
    L2Book,
    OpenOrders,
    OrderStatus,
    typed,
)
from almanak.gateway.signers import VerifiedSigner
from tests.hypercore_resting_support import open_store, resting_lifecycle
from tests.hypercore_trading_support import LocalSigner, ModelVenue, seed_model
from tests.test_hypercore_custody import SAFE, T0, Chain
from tests.test_hypercore_trading import OfflineRun, agent_key, body

COIN = "BTC"


def offline(tmp_path):
    model = HyperCoreModel(tmp_path / "model.json", 10)
    meta = seed_model(model)
    return model, Chain(model), meta


def test_resting_lifecycle_against_the_model(tmp_path):
    model, chain, meta = offline(tmp_path)
    keys = LocalAgentKeys(tmp_path / "agent-keys.json", "offline")
    with ModelService(model) as service:
        venue = ModelVenue(service.url, lambda: chain.blocks[-1]["timestamp"] * 1000)
        store = open_store(
            tmp_path / "hypercore.db", "offline", Custody(safe=SAFE), meta, T0 - 1000
        )
        run = OfflineRun(model, chain, venue.info, keys, store)
        signer = VerifiedSigner(LocalSigner(keys), LocalSigner.address)
        report = resting_lifecycle(run, Trader(store, venue, signer))
        venue.close()
    assert [s["step"] for s in report["steps"]] == [
        "isolated_leverage",
        "resting_entry_withheld",
        "restart_while_resting",
        "mark_fills_resting",
        "margin_added",
        "dead_mans_switch",
        "take_profit",
        "teardown_lost_cancel",
        "teardown_completed",
    ]
    assert report["steps"][7]["halt"] == "teardown_incomplete"
    assert {f["kind"] for f in model.doc["injected"]} >= {"withhold", "lose", "mark"}
    assert len(model.doc["fired"]) == 1


# Admission, answers, recovery and the model, case by case.


@pytest.fixture
def funded(tmp_path):
    """A Safe with 20 USDC on perp and its first agent active, BTC configured isolated at
    10x and already set so, before any trade."""
    yield from funded_at(tmp_path, "isolated", TradingPolicy(margin_without_record=True))


def funded_at(tmp_path, margin, policy):
    model, chain, meta = offline(tmp_path)
    keys = LocalAgentKeys(tmp_path / "agent-keys.json", "offline")
    with ModelService(model) as service:
        venue = ModelVenue(service.url, lambda: chain.blocks[-1]["timestamp"] * 1000)
        custody = Custody(safe=SAFE)
        store = open_store(tmp_path / "hypercore.db", "offline", custody, meta, T0, margin, policy)
        run = OfflineRun(model, chain, venue.info, keys, store)
        run.fund_perp({})
        trader = Trader(store, venue, VerifiedSigner(LocalSigner(keys), LocalSigner.address))
        assert trader.leverage(COIN).status == "applied"
        yield SimpleNamespace(run=run, trader=trader, model=model, store=store, keys=keys)
        venue.close()


@pytest.fixture
def cross(tmp_path):
    """The same Safe with BTC configured cross and the default, mainnet policy."""
    yield from funded_at(tmp_path, "cross", TradingPolicy())


def resting(trader, price="77000", size="0.002", tif="Gtc", is_buy=True, reduce_only=False):
    order = RestingOrder(
        is_buy=is_buy, size=Decimal(size), price=Decimal(price), tif=tif, reduce_only=reduce_only
    )
    return trader.rest(COIN, order)


def holding(funded, mark="76900"):
    """The resting buy, filled by a scripted mark: 0.002 BTC owned."""
    placed = resting(funded.trader)
    funded.model.move_mark(COIN, mark, "synthesized fill")
    funded.trader.reconcile()
    funded.trader.refresh()
    return placed


def refusal(trader, attempt, **changes) -> str:
    state = trader.state()
    authority = trader.authority(state, attempt).model_copy(update=changes)
    with pytest.raises(DataError) as error:
        admit(
            state.trading,
            state.custody.agents,
            attempt,
            authority,
            trader.venue.now(),
            state.book,
        )
    assert error.value.kind == "authority"
    return str(error.value)


def admitted(trader, attempt) -> None:
    state = trader.state()
    authority = trader.authority(state, attempt)
    admit(state.trading, state.custody.agents, attempt, authority, trader.venue.now(), state.book)


def retained(trader, planner):
    return trader.retain(planner)


def changed_action(attempt, **fields):
    action = attempt.request.action
    if isinstance(action, signing.OrderAction):
        action = action.model_copy(update={"order": action.order.model_copy(update=fields)})
    else:
        action = action.model_copy(update=fields)
    request = attempt.request.model_copy(update={"action": action})
    return attempt.model_copy(update={"request": request})


def test_a_resting_order_rests_owned_and_fills_as_its_own(funded):
    placed = resting(funded.trader)
    assert placed.status == "applied" and placed.answer.response.data.statuses[0].resting
    (owned,) = funded.trader.state().trading.live()
    assert (owned.oid, owned.status) == (placed.oids[0], "open")
    funded.model.move_mark(COIN, "76900", "synthesized fill")
    book = funded.trader.reconcile().book
    fill = next(e for e in book.effects if e.kind == "fill")
    assert fill.owned and fill.entry["crossed"] is False and book.owned == {COIN: Decimal("0.002")}
    assert funded.trader.refresh().trading.owned(owned.oid).status == "filled"


def test_a_gtc_order_that_crosses_fills_and_rests_the_rest(funded):
    trader = funded.trader
    ask = rules.best(trader.observe(L2Book(coin=COIN)), True)
    # 0.103 BTC sits at the best ask of the recorded book; a larger buy rests the rest.
    funded.model.doc["seed"]["books"][COIN]["levels"][1] = [{"n": 1, "px": str(ask), "sz": "0.001"}]
    placed = resting(trader, price=str(ask), size="0.002")
    assert placed.status == "applied"
    (owned,) = trader.state().trading.live()
    assert trader.reconcile().book.positions == {COIN: Decimal("0.001")}
    record = typed(trader.observe(OrderStatus(user=SAFE, oid=owned.oid))).order.order
    assert (record.sz, record.origSz) == ("0.001", "0.002")


def test_a_gtc_order_that_fills_whole_is_owned_as_filled(funded):
    trader = funded.trader
    ask = rules.best(trader.observe(L2Book(coin=COIN)), True)
    placed = resting(trader, price=str(ask), size="0.002")
    assert placed.answer.response.data.statuses[0].filled
    assert trader.state().trading.owned(placed.oids[0]).status == "filled"


def test_post_only_and_margin_refusals_release_the_attempt(funded):
    trader = funded.trader
    crossing = resting(trader, price="78400", tif="Alo")
    assert crossing.status == "not_applied"
    assert "Post only" in crossing.answer.response.data.statuses[0].error
    oversized = resting(trader, size="1")
    assert oversized.status == "not_applied" and trader.state().trading.live() == ()
    assert trader.state().trading.released == 2


def test_resting_orders_are_bounded_like_ioc_orders(funded):
    trader = funded.trader
    book = trader.observe(L2Book(coin=COIN))
    now = funded.run.now()
    order = RestingOrder(is_buy=True, size=Decimal("0.002"), price=Decimal("77000"), tif="Gtc")
    attempt = retained(
        trader, lambda s: rules.plan_resting(s.trading, COIN, trader.agent(s), order, book, now)
    )
    assert "slippage" in refusal(trader, changed_action(attempt, price=Decimal("90000")))
    assert "entry or reduce-only" in refusal(trader, changed_action(attempt, reduce_only=True))
    protect = attempt.model_copy(update={"purpose": "protect"})
    assert "entry or reduce-only" in refusal(trader, protect)
    with pytest.raises(ValueError, match="price grid"):
        rules.on_grid(Decimal("77000.5"), 5)
    with pytest.raises(ValueError, match="Gtc or Alo"):
        RestingOrder(is_buy=True, size=Decimal(1), price=Decimal(1), tif="Ioc")


def test_live_reduce_only_orders_together_fit_the_owned_position(funded):
    trader = funded.trader
    holding(funded)
    sell = resting(trader, price="80000", size="0.001", is_buy=False, reduce_only=True, tif="Alo")
    assert sell.status == "applied" and sell.purpose == "exit"
    trader.refresh()
    second = resting(trader, price="80100", size="0.001", is_buy=False, reduce_only=True)
    assert second.status == "applied"
    with pytest.raises(DataError, match="exceed the strategy's own position"):
        trader.protect(COIN, "sl", Decimal("75000"), Decimal("0.001"))
    with pytest.raises(DataError, match="exceed the strategy's own position"):
        trader.exit(COIN)
    book = trader.observe(L2Book(coin=COIN))
    now = funded.run.now()
    order = RestingOrder(
        is_buy=False, size=Decimal("0.003"), price=Decimal("80000"), tif="Gtc", reduce_only=True
    )
    larger = retained(
        trader, lambda s: rules.plan_resting(s.trading, COIN, trader.agent(s), order, book, now)
    )
    assert "strategy's own position" in refusal(trader, larger)


def test_triggers_are_reduce_only_on_their_side_of_the_mark(funded):
    trader = funded.trader
    assert trader.protect(COIN, "sl", Decimal("75000")) is None
    holding(funded)
    stop = trader.protect(COIN, "sl", Decimal("75000"), Decimal("0.001"))
    order = stop.request.action.order
    assert (stop.purpose, order.price, order.size) == ("protect", Decimal(74625), Decimal("0.001"))
    now = funded.run.now()
    attempt = retained(
        trader,
        lambda s: rules.plan_trigger(
            s.trading, COIN, trader.agent(s), Decimal("0.001"), "tp", Decimal("80000"), now
        ),
    )
    assert "wrong side" in refusal(trader, changed_action(attempt, tpsl="sl"))
    assert "wrong side" in refusal(trader, changed_action(attempt, trigger_price=Decimal(70000)))
    # HyperCore triggers on the mark, 76900 here, not on the book's mid just below it: a
    # take-profit between them is refused, and a stop-loss there is admitted.
    mid = rules.mid(trader.observe(L2Book(coin=COIN)))
    between = Decimal(int((mid + Decimal(76900)) / 2) + 1)
    assert mid < between < 76900
    assert "wrong side" in refusal(trader, changed_action(attempt, trigger_price=between))
    limit = rules.limit_price(between, False, 50, 5)
    admitted(trader, changed_action(attempt, tpsl="sl", trigger_price=between, price=limit))
    assert "observed mark" in refusal(trader, attempt, active=None)
    active = trader.authority(trader.state(), attempt).active
    stale = active.model_copy(update={"received_at": active.received_at - 60_000})
    assert "fresh read" in refusal(trader, attempt, active=stale)
    assert "exceed" in refusal(trader, changed_action(attempt, size=Decimal("0.0015")))
    assert "slippage" in refusal(trader, changed_action(attempt, price=Decimal("70000")))
    assert "beyond its trigger" in refusal(trader, changed_action(attempt, price=Decimal("80100")))
    assert "own position" in refusal(trader, changed_action(attempt, size=Decimal("0.003")))
    assert "protective order" in refusal(trader, attempt.model_copy(update={"purpose": "exit"}))
    assert "observed position" in refusal(trader, attempt, account=None)
    with pytest.raises(ValueError, match="no position"):
        rules.plan_trigger(trader.state().trading, COIN, "0x" + "00" * 20, 0, "sl", 1, now)


def test_a_triggered_order_fills_under_its_own_oid_and_cloid(funded):
    trader = funded.trader
    holding(funded)
    take = trader.protect(COIN, "tp", Decimal("80000"))
    funded.model.move_mark(COIN, "80500", "synthesized rise")
    book = trader.reconcile().book
    closing = [e for e in book.effects if e.kind == "fill" and e.size < 0]
    assert len(closing) == 1 and closing[0].owned and closing[0].entry["cloid"] == take.cloid
    assert trader.refresh().trading.owned(take.oids[0]).status == "filled"


def test_a_trigger_that_finds_nothing_left_to_reduce_is_cancelled(funded):
    trader, model = funded.trader, funded.model
    holding(funded)
    stop = trader.protect(COIN, "sl", Decimal("75000"))
    with model.lock:
        # An outside fill closes the position under the stop, as the Safe owners might.
        model.trade(SAFE, COIN, Decimal("-0.002"), Decimal(76900), model.hash(), 1, None, None)
    model.move_mark(COIN, "74000", "synthesized drop")
    assert trader.refresh().trading.owned(stop.oids[0]).status == "reduceOnlyCanceled"


def test_cancels_are_signed_by_the_retained_oid_and_resolve_from_their_answers(funded):
    trader = funded.trader
    first, second = resting(trader), resting(trader, price="76000")
    by_oid = trader.cancel(first.oids[0])
    by_cloid = trader.cancel_by_cloid(second.cloid)
    assert (by_oid.status, by_cloid.status) == ("applied", "applied")
    assert by_cloid.request.action.wire() == {
        "type": "cancel",
        "cancels": [{"a": 0, "o": second.oids[0]}],
    }
    assert {o.status for o in trader.state().trading.orders} == {"canceled"}
    with pytest.raises(ValueError, match="live owned order"):
        trader.cancel(first.oids[0])
    with pytest.raises(DataError, match="not one of the strategy's"):
        trader.cancel(999_999)
    with pytest.raises(DataError, match="not one of the strategy's"):
        trader.cancel_by_cloid("0x" + "ab" * 16)


def test_a_cancel_named_by_cloid_cancels_the_owned_order_whoever_takes_the_cloid(funded):
    trader, model = funded.trader, funded.model
    placed = resting(trader)
    # An outside order takes the same cloid: HyperCore resolves a cloid to its newest
    # order (T5), so only a cancel signed by the retained oid reaches the strategy's.
    outside = model.place_outside_order(SAFE, COIN, True, "60000", "0.001")
    with model.lock:
        model.doc["orders"][str(outside)]["record"]["cloid"] = placed.cloid
        model.doc["cloids"][SAFE][placed.cloid] = outside
    cancelled = trader.cancel_by_cloid(placed.cloid)
    assert cancelled.status == "applied"
    assert model.doc["orders"][str(placed.oids[0])]["status"] == "canceled"
    assert model.doc["orders"][str(outside)]["status"] == "open"


def test_a_cancel_names_a_live_owned_order_of_its_market(funded):
    trader = funded.trader
    resting(trader)
    (owned,) = trader.state().trading.live()
    now = funded.run.now()
    attempt = retained(
        trader, lambda s: rules.plan_cancel(s.trading, owned.oid, trader.agent(s), now)
    )
    assert "own oid" in refusal(trader, changed_action(attempt, oid=owned.oid + 1))
    assert "of its market" in refusal(trader, changed_action(attempt, asset=1))
    assert "names the owned order" in refusal(trader, attempt.model_copy(update={"cancels": None}))
    gone = trader.state().trading.model_copy(
        update={"orders": (owned.model_copy(update={"status": "filled"}),)}
    )
    trader.store.update(lambda s: s.model_copy(update={"trading": gone}))
    assert "live owned order" in refusal(trader, attempt)


def test_a_cancel_recovers_from_the_order_it_names(funded):
    trader = funded.trader
    first, second = resting(trader), resting(trader, price="76000")
    funded.model.withhold("cancel")
    withheld = trader.cancel(first.oids[0])
    assert withheld.status == "sent"
    assert trader.recover() == ()
    assert trader.state().trading.attempt(withheld.id).status == "applied"
    funded.model.lose("cancel")
    lost = trader.cancel(second.oids[0])
    assert trader.recover() == (trader.state().trading.attempt(lost.id),)
    funded.run.advance(32)
    assert trader.recover() == ()
    assert trader.state().trading.attempt(lost.id).status == "not_applied"
    assert trader.state().trading.owned(second.oids[0]).live


def test_a_cancel_of_an_order_that_filled_meanwhile_is_moot(funded):
    trader = funded.trader
    placed = resting(trader)
    funded.model.lose("cancel")
    cancel = trader.cancel(placed.oids[0])
    funded.model.move_mark(COIN, "76900", "synthesized fill")
    trader.recover()
    assert trader.state().trading.attempt(cancel.id).status == "not_applied"
    assert trader.state().trading.owned(placed.oids[0]).status == "filled"
    answered = trader.state().trading.attempt(cancel.id)
    assert answered.evidence and typed(answered.evidence[0]).order.status == "filled"


def test_a_cancel_the_venue_refuses_leaves_the_order_to_its_status(funded):
    trader = funded.trader
    placed = resting(trader)
    funded.model.doc["orders"][str(placed.oids[0])]["status"] = "filled"
    funded.model.doc["resting"][SAFE].remove(placed.oids[0])
    refused = trader.cancel(placed.oids[0])
    assert refused.status == "not_applied"
    assert trader.refresh().trading.owned(placed.oids[0]).status == "filled"


def test_an_owned_order_the_venue_no_longer_knows_halts(funded):
    trader = funded.trader
    placed = resting(trader)
    del funded.model.doc["orders"][str(placed.oids[0])]
    funded.model.doc["resting"][SAFE].remove(placed.oids[0])
    with pytest.raises(DataError, match="no longer reports") as halted:
        trader.refresh()
    assert halted.value.kind == "inconsistent"
    with pytest.raises(ValueError, match="orderStatus of its oid"):
        rules.refreshed(trader.state().trading, trader.observe(OpenOrders(user=SAFE)))


def test_the_dead_mans_switch_is_bounded_and_cleared(funded):
    trader = funded.trader
    now = funded.run.now()
    attempt = retained(trader, lambda s: rules.plan_schedule(s.trading, trader.agent(s), now, now))
    assert "5 seconds past its expiry" in refusal(trader, attempt)
    later = changed_action(attempt, time=now + 2 * 3_600_000)
    assert "5 seconds past its expiry" in refusal(trader, later)
    assert "dead man's switch" in refusal(trader, attempt.model_copy(update={"purpose": "cancel"}))
    trader.trading(lambda t: rules.refused(t, attempt.id))
    armed = trader.schedule_cancel(now + 60_000)
    assert armed.status == "applied" and trader.state().trading.armed(funded.run.now())
    verdict = trader.verdict()
    assert not verdict.complete and verdict.schedule == now + 60_000
    assert trader.disarm().status == "applied" and trader.state().trading.schedule is None
    assert trader.disarm() is None and trader.verdict().complete


def test_a_lost_scheduled_cancel_resolves_on_its_resend_and_a_withheld_one_halts(funded):
    trader = funded.trader
    now = funded.run.now()
    funded.model.lose("scheduleCancel")
    lost = trader.schedule_cancel(now + 60_000)
    assert lost.status == "sent" and trader.recover() == ()
    assert trader.state().trading.schedule == now + 60_000
    funded.model.withhold("scheduleCancel")
    withheld = trader.schedule_cancel(None)
    with pytest.raises(DataError, match="no evidence"):
        trader.require_resolved()
    funded.run.advance(40)
    assert trader.recover() == (trader.state().trading.attempt(withheld.id),)


def test_a_withheld_margin_change_halts_and_is_never_planned_again(funded):
    trader = funded.trader
    holding(funded)
    funded.model.withhold("updateIsolatedMargin")
    added = trader.margin(COIN, Decimal("1"))
    assert added.status == "sent"
    for seconds in (0, 40):
        funded.run.advance(seconds or 1)
        with pytest.raises(DataError, match="no evidence"):
            trader.require_resolved()
    with pytest.raises(ValueError, match="unresolved"):
        trader.margin(COIN, Decimal("1"))


def test_margin_moves_only_an_isolated_position_under_assumption_a10(funded):
    trader = funded.trader
    now = funded.run.now()
    attempt = retained(
        trader, lambda s: rules.plan_margin(s.trading, COIN, trader.agent(s), Decimal("1"), now)
    )
    assert "isolated position" in refusal(trader, attempt)
    assert "observed position" in refusal(trader, attempt, account=None)
    assert "margin change moves" in refusal(trader, changed_action(attempt, asset=1))
    with pytest.raises(ValueError, match="micro-USDC"):
        rules.plan_margin(trader.state().trading, COIN, "0x" + "00" * 20, Decimal("1E-7"), now)
    mainnet = trader.state().trading.model_copy(update={"policy": TradingPolicy()})
    trader.store.update(lambda s: s.model_copy(update={"trading": mainnet}))
    assert "T10" in refusal(trader, attempt)


def test_margin_is_planned_only_on_an_isolated_market(cross):
    trading, now = cross.trader.state().trading, cross.run.now()
    with pytest.raises(ValueError, match="configured isolated"):
        rules.plan_margin(trading, COIN, "0x" + "00" * 20, Decimal(1), now)


def test_removing_margin_keeps_the_initial_margin(funded):
    trader = funded.trader
    holding(funded)
    assert trader.margin(COIN, Decimal("-1")).status == "not_applied"
    assert trader.margin(COIN, Decimal("1")).status == "applied"
    assert trader.margin(COIN, Decimal("-1")).status == "applied"
    assert trader.margin(COIN, Decimal("100")).status == "not_applied"


def test_leverage_waits_for_the_market_to_hold_no_order(funded):
    trader = funded.trader
    resting(trader)
    now = funded.run.now()
    attempt = retained(trader, lambda s: rules.plan_leverage(s.trading, COIN, trader.agent(s), now))
    assert "no position or order" in refusal(trader, attempt)
    assert "observed position and orders" in refusal(trader, attempt, orders=None)


def test_an_isolated_entry_needs_isolated_leverage(cross):
    trader = cross.trader
    book = trader.observe(L2Book(coin=COIN))
    now = cross.run.now()
    order = RestingOrder(is_buy=True, size=Decimal("0.002"), price=Decimal("77000"), tif="Gtc")
    attempt = retained(
        trader, lambda s: rules.plan_resting(s.trading, COIN, trader.agent(s), order, book, now)
    )
    isolated = trader.state().trading.targets[0].model_copy(update={"margin": "isolated"})
    trading = trader.state().trading.model_copy(update={"targets": (isolated,)})
    trader.store.update(lambda s: s.model_copy(update={"trading": trading}))
    assert "leverage and margin mode" in refusal(trader, attempt)


def test_a_withheld_isolated_leverage_change_resolves_from_active_asset_data(cross):
    trader = cross.trader
    isolated = trader.state().trading.targets[0].model_copy(update={"margin": "isolated"})
    trading = trader.state().trading.model_copy(update={"targets": (isolated,)})
    trader.store.update(lambda s: s.model_copy(update={"trading": trading}))
    cross.model.withhold("updateLeverage")
    attempt = trader.leverage(COIN)
    assert attempt.status == "sent" and trader.recover() == ()
    assert trader.state().trading.attempt(attempt.id).status == "applied"


def answer_of(value: dict):
    return ANSWER.validate_json(json.dumps(value))


def order_answer(status: dict):
    return answer_of(
        {"status": "ok", "response": {"type": "order", "data": {"statuses": [status]}}}
    )


RESTING = order_answer({"resting": {"oid": 1}})
FILLED = order_answer({"filled": {"totalSz": "1", "avgPx": "1", "oid": 1}})
CANCELLED = answer_of(
    {"status": "ok", "response": {"type": "cancel", "data": {"statuses": ["success"]}}}
)
DEFAULT = answer_of({"status": "ok", "response": {"type": "default"}})


def test_answers_that_are_not_the_attempts_own_are_refused(funded):
    trader = funded.trader
    placed = resting(trader)
    cancel = trader.cancel(placed.oids[0])
    trading = trader.state().trading
    leverage = trading.attempts[0]
    book = trader.observe(L2Book(coin=COIN))
    _, ioc = rules.plan_entry(
        trading, COIN, leverage.agent, True, Decimal("0.001"), book, funded.run.now()
    )
    with pytest.raises(DataError, match="not the attempt"):
        rules.outcome(ioc, RESTING)
    assert rules.outcome(placed, FILLED) == ("applied", (1,))
    with pytest.raises(DataError, match="without its status"):
        rules.outcome(placed, DEFAULT)
    with pytest.raises(DataError, match="without its status"):
        rules.outcome(trading.attempt(cancel.id), DEFAULT)
    with pytest.raises(DataError, match="another action"):
        rules.outcome(leverage, CANCELLED)


def test_a_filled_answer_to_a_trigger_order_is_not_its_own(funded):
    trader = funded.trader
    holding(funded)
    take = trader.protect(COIN, "tp", Decimal("80000"))
    with pytest.raises(DataError, match="not the attempt"):
        rules.outcome(take, FILLED)


def test_a_withheld_resting_order_found_only_by_its_fill_is_owned_open(funded, monkeypatch):
    trader = funded.trader
    ask = rules.best(trader.observe(L2Book(coin=COIN)), True)
    funded.model.withhold("order")
    placed = resting(trader, price=str(ask))
    original = fork_model.ANSWERS["orderStatus"]

    def missing(model, query):
        return {"status": "unknownOid"} if isinstance(query.oid, str) else original(model, query)

    monkeypatch.setitem(fork_model.ANSWERS, "orderStatus", missing)
    trader.recover()
    attempt = trader.state().trading.attempt(placed.id)
    assert attempt.status == "applied"
    assert trader.state().trading.owned(attempt.oids[0]).status == "open"
    monkeypatch.undo()
    assert trader.refresh().trading.owned(attempt.oids[0]).status == "filled"


def test_a_resting_order_that_fills_after_its_placement_expires_is_found(funded, monkeypatch):
    trader = funded.trader
    funded.model.withhold("order")
    placed = resting(trader)
    funded.run.advance(40)
    funded.model.move_mark(COIN, "76900", "synthesized fill after the placement's expiry")
    original = fork_model.ANSWERS["orderStatus"]

    def missing(model, query):
        return {"status": "unknownOid"} if isinstance(query.oid, str) else original(model, query)

    monkeypatch.setitem(fork_model.ANSWERS, "orderStatus", missing)
    assert trader.recover() == ()
    attempt = trader.state().trading.attempt(placed.id)
    assert attempt.status == "applied" and attempt.oids
    assert trader.reconcile().book.owned == {COIN: Decimal("0.002")}


def test_a_lost_resting_order_is_never_released_on_a_status_miss(funded):
    trader = funded.trader
    funded.model.lose("order")
    lost = resting(trader)
    funded.run.advance(40)
    assert trader.recover() == (trader.state().trading.attempt(lost.id),)
    with pytest.raises(ValueError, match="unresolved"):
        resting(trader)


def test_a_refused_cancel_keeps_the_switch_armed_and_the_position_open(funded):
    trader, model = funded.trader, funded.model
    holding(funded)
    bid = resting(trader, price="75000", size="0.0003", tif="Alo")
    assert bid.status == "applied"
    trader.schedule_cancel(funded.run.now() + 600_000)
    model.refuse("cancel")
    verdict = trader.teardown()
    cancel = trader.state().trading.attempts[-1]
    assert cancel.purpose == "cancel" and cancel.status == "not_applied"
    assert verdict.own == bid.oids and verdict.schedule is not None
    assert verdict.positions == {COIN: Decimal("0.002")} and not verdict.complete
    assert model.doc["schedules"][SAFE] == verdict.schedule
    assert trader.teardown().complete


def test_a_resting_reduce_only_order_needs_no_outside_exposure(funded):
    trader, model = funded.trader, funded.model
    holding(funded)
    model.place_outside_order(SAFE, COIN, True, "76000", "0.0005")
    model.move_mark(COIN, "75900", "synthesized fill of an outside order")
    book = trader.reconcile().book
    assert book.positions == {COIN: Decimal("0.0025")} and book.owned == {COIN: Decimal("0.002")}
    with pytest.raises(DataError, match="no outside exposure"):
        resting(trader, price="80000", size="0.001", is_buy=False, reduce_only=True)
    with pytest.raises(DataError, match="no outside exposure"):
        trader.protect(COIN, "sl", Decimal("70000"), Decimal("0.001"))
    assert trader.exit(COIN).status == "applied"
    assert trader.reconcile().book.positions == {COIN: Decimal("0.0005")}


def test_teardown_cancels_its_own_orders_and_never_an_outside_one(funded):
    trader = funded.trader
    holding(funded)
    trader.protect(COIN, "sl", Decimal("75000"))
    outside = funded.model.place_outside_order(SAFE, COIN, True, "60000", "0.001")
    verdict = trader.teardown()
    assert not verdict.complete and [o.oid for o in verdict.outside] == [outside]
    assert verdict.positions == {} and verdict.own == () and verdict.unresolved == ()
    assert trader.state().trading.live() == ()
    assert funded.model.doc["orders"][str(outside)]["status"] == "open"


def test_a_teardown_verdict_counts_a_scheduled_cancel_only_while_it_lies_ahead(funded):
    trader = funded.trader
    armed = trader.schedule_cancel(funded.run.now() + 60_000)
    state = trader.state()
    account = trader.observe(ClearinghouseState(user=SAFE))
    orders = trader.observe(OpenOrders(user=SAFE))
    assert teardown_verdict(state, account, orders).schedule == state.trading.schedule
    later = state.trading.schedule + 1
    assert teardown_verdict(state, account, orders, later).complete and armed.status == "applied"


# The model's own rules (M3).


def test_the_model_fires_a_scheduled_cancel_at_its_time(funded):
    model, now = funded.model, funded.model.now()
    _, key = agent_key(funded)
    assert "5 seconds" in model.exchange(body(key, schedule(now + 1000), now + 1))["response"]
    resting(funded.trader)
    model.exchange(body(key, schedule(now + 10_000), now + 2))
    model.exchange(body(key, schedule(None), now + 3))
    funded.run.advance(20)
    assert model.doc["fired"] == [] and funded.trader.state().trading.live()[0].live
    model.exchange(body(key, schedule(model.now() + 10_000), model.now() + 4))
    funded.run.advance(20)
    (fired,) = model.doc["fired"]
    assert funded.trader.refresh().trading.live() == () and fired["oids"]


def schedule(time):
    return {"type": "scheduleCancel"} | ({} if time is None else {"time": time})


def test_the_model_refuses_what_its_rules_do_not_admit(funded):
    model, now = funded.model, funded.model.now()
    _, key = agent_key(funded)

    def status(action, nonce):
        answer = model.exchange(body(key, action, nonce))["response"]
        return answer["data"]["statuses"][0] if isinstance(answer, dict) else answer

    trigger = {"triggerPx": "75000", "tpsl": "sl", "isMarket": False}
    order = {"a": 0, "b": False, "p": "74000", "s": "0.001", "r": True, "t": {"trigger": trigger}}
    action = {"type": "order", "orders": [order], "grouping": "na"}
    assert "Reduce only" in status(action, now + 1)["error"]
    market = {**order, "t": {"trigger": {**trigger, "isMarket": True}}}
    assert "Market trigger" in status({**action, "orders": [market]}, now + 2)["error"]
    margin = {"type": "updateIsolatedMargin", "asset": 0, "isBuy": True, "ntli": 1_000_000}
    assert "No isolated position" in status(margin, now + 3)
    holding(funded)
    above = {**order, "t": {"trigger": {**trigger, "triggerPx": "77000"}}}
    assert "Invalid TP/SL" in status({**action, "orders": [above]}, now + 4)["error"]
    resting_sell = {**order, "t": {"limit": {"tif": "Gtc"}}, "b": True, "r": True}
    assert "Reduce only" in status({**action, "orders": [resting_sell]}, now + 5)["error"]
    cancel = {"type": "cancel", "cancels": [{"a": 1, "o": 1}]}
    assert status(cancel, now + 6) == {
        "error": "Order was never placed, already canceled, or filled."
    }


def test_the_model_liquidates_an_isolated_position_alone(funded):
    trader = funded.trader
    holding(funded)
    trader.protect(COIN, "tp", Decimal("80000"))
    funded.model.move_mark(COIN, "69000", "synthesized drop below maintenance margin")
    book = trader.reconcile().book
    liquidation = next(
        e for e in book.effects if e.entry.get("delta", {}).get("type") == "liquidation"
    )
    assert liquidation.entry["delta"]["leverageType"] == "Isolated" and book.positions == {}
    fill = next(e for e in book.effects if e.liquidation)
    assert fill.entry["dir"] == "Liquidated Isolated Long"
    assert {o.status for o in trader.refresh().trading.orders} >= {"liquidatedCanceled"}


def test_an_answer_shape_admits_cancels_and_refuses_other_statuses():
    success = {"status": "ok", "response": {"type": "cancel", "data": {"statuses": ["success"]}}}
    assert answer_of(success).response.data.statuses == ("success",)
    with pytest.raises(ValueError):
        answer_of(
            {"status": "ok", "response": {"type": "cancel", "data": {"statuses": ["waiting"]}}}
        )
    assert InfoRequest(info=OrderStatus(user=SAFE, oid=1))
