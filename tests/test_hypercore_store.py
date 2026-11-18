"""The HyperCore durable document stays bounded (blueprint 30, section 5.5).

Settled attempts, owned orders and applied effects leave the row for the store's
append-only tables at the next change, in its transaction. These cases hold the rules for
what must stay, and run phase 3's and phase 4's decisions, with their faults, against a
store that keeps no recent history and one that archives nothing: recovery,
reconciliation and admission decide alike, and the archive holds everything the
unbounded row would.
"""

import sqlite3
from decimal import Decimal

import pytest

from almanak.connectors.hyperliquid import trading as rules
from almanak.connectors.hyperliquid.custody import Custody
from almanak.connectors.hyperliquid.effects import Book, Effect
from almanak.connectors.hyperliquid.fork_service import ModelService
from almanak.connectors.hyperliquid.trading import Attempt, OwnedOrder, Trading
from almanak.connectors.hyperliquid.trading_runtime import Trader, require_complete
from almanak.connectors.hyperliquid.trading_store import HyperCoreState, TradingStore, settled
from almanak.gateway.hypercore_agent_keys import LocalAgentKeys
from almanak.gateway.hypercore_info import L2Book
from almanak.gateway.hypercore_signing import ActionRequest, Noop
from almanak.gateway.signers import SignerRefused, VerifiedSigner
from tests import hypercore_resting_support as resting
from tests.hypercore_trading_support import (
    COIN,
    LIQUIDATION_MARK,
    SIZE,
    LocalSigner,
    ModelVenue,
    open_store,
)
from tests.test_hypercore_custody import SAFE, T0
from tests.test_hypercore_trading import OfflineRun, offline

AGENT = "0x" + "a1" * 20
CURSOR = T0 + 100_000


def attempt(attempt_id, purpose="entry", status="applied", expires=T0, **fields) -> Attempt:
    request = ActionRequest(action=Noop(), nonce=expires - 30_000, expires_after=expires)
    return Attempt(
        id=attempt_id, purpose=purpose, coin=None, agent=AGENT, request=request, status=status
    ).model_copy(update=fields)


def effect(key: str, time: int) -> Effect:
    return Effect(
        kind="funding", key=key, coin=COIN, time=time, size=Decimal(0), cash=Decimal(0), entry={}
    )


def order(oid: int, attempt_id: str, status="canceled") -> OwnedOrder:
    cloid = rules.cloid_for("store", attempt_id)
    return OwnedOrder(attempt=attempt_id, coin=COIN, oid=oid, cloid=cloid, status=status)


def document(attempts=(), effects=(), archived=0, orders=()) -> HyperCoreState:
    trading = Trading(
        deployment="store",
        safe=SAFE,
        targets=(),
        attempts=attempts,
        orders=orders,
        archived=archived,
    )
    book = Book(safe=SAFE, cursor=CURSOR, effects=effects)
    return HyperCoreState(custody=Custody(safe=SAFE), trading=trading, book=book)


def ids(attempts) -> list[str]:
    return [a.id for a in attempts]


def test_an_attempt_leaves_only_once_nothing_reads_it():
    state = document(
        (
            attempt("1"),
            attempt("2", purpose="exit"),
            attempt("3", status="not_applied"),
            # The re-plan streak walks back over 5 to 4, the last that did not release.
            attempt("4"),
            attempt("5", status="not_applied"),
            # Its fills could still be in a page from the cursor.
            attempt("6", purpose="exit", expires=CURSOR),
        )
    )
    split = settled(state, keep=0)
    assert ids(split.attempts) == ["1", "2", "3"]
    assert ids(split.state.trading.attempts) == ["4", "5", "6"]
    assert split.state.trading.archived == 3
    assert split.state.trading.released == state.trading.released == 1
    assert rules.next_id(split.state.trading) == "7"
    # The newest `keep` stay whatever their state.
    assert ids(settled(state, keep=5).attempts) == ["1"]
    assert settled(split.state, keep=0).attempts == ()


def test_an_unresolved_attempt_and_the_one_it_retires_stay():
    retired = attempt("1", purpose="exit", status="sent")
    retire = attempt("2", purpose="retire", status="sent", retires="1")
    state = document((retired, retire))
    assert settled(state, keep=0).attempts == ()
    # The retired attempt resolved from its own evidence; its noop is still out.
    resolved = document((retired.model_copy(update={"status": "applied"}), retire))
    assert settled(resolved, keep=0).attempts == ()
    done = retire.model_copy(update={"status": "not_applied"})
    both = document((resolved.trading.attempts[0], done))
    assert ids(settled(both, keep=0).attempts) == ["1", "2"]


def test_an_owned_order_and_its_placing_attempt_leave_once_it_cannot_fill():
    state = document(
        (
            attempt("1", purpose="protect"),
            attempt("2", purpose="protect"),
            # The cancel that settled order 12 could have applied until the cursor.
            attempt("3", purpose="cancel", expires=CURSOR, cancels=12),
            attempt("4", purpose="protect"),
            attempt("5", purpose="protect"),
            attempt("6", purpose="cancel", status="sent", cancels=15),
        ),
        orders=(order(11, "1"), order(12, "2"), order(14, "4", "open"), order(15, "5")),
    )
    split = settled(state, keep=0)
    assert [o.oid for o in split.orders] == [11] and ids(split.attempts) == ["1"]
    assert [o.oid for o in split.state.trading.orders] == [12, 14, 15]
    assert ids(split.state.trading.attempts) == ["2", "3", "4", "5", "6"]
    assert settled(state, keep=4).orders == ()


def test_an_effect_leaves_once_no_page_can_return_it():
    effects = (effect("a", CURSOR - 2), effect("b", CURSOR - 1), effect("c", CURSOR))
    state = document(effects=effects)
    split = settled(state, keep=0)
    assert [e.key for e in split.effects] == ["a", "b"]
    assert [e.key for e in split.state.book.effects] == ["c"]
    assert [e.key for e in settled(state, keep=2).effects] == ["a"]


def test_identities_continue_after_archived_attempts():
    with pytest.raises(ValueError, match="above every attempt"):
        Trading(deployment="store", safe=SAFE, targets=(), attempts=(attempt("2"),))
    trading = Trading(
        deployment="store", safe=SAFE, targets=(), attempts=(attempt("2"),), archived=1
    )
    planned, retire = rules.plan_retire(
        trading.model_copy(update={"attempts": (attempt("2", status="sent"),)}), "2", T0
    )
    assert retire.id == "3" and planned.archived == 1


def test_a_change_archives_what_settled_before_it(tmp_path):
    store = TradingStore(tmp_path / "hypercore.db", keep=1)
    old = (attempt("1"), attempt("2", purpose="exit"), attempt("3"))
    store.create(document(old, (effect("a", T0), effect("b", T0 + 1))))
    before = store.history()

    def killed(state):
        raise RuntimeError("killed mid-update")

    with pytest.raises(RuntimeError):
        store.update(killed)
    assert store.history() == before and store.load().trading.archived == 0
    kept = store.update(lambda s: s)
    assert before.attempts == old and before.orders == ()
    assert ids(kept.trading.attempts) == ["3"] and [e.key for e in kept.book.effects] == ["b"]
    assert store.load() == kept and store.history() == before
    rows = sqlite3.connect(store.path).execute("SELECT id FROM hypercore_attempts").fetchall()
    assert rows == [(1,), (2,)]


def test_an_effect_archived_twice_fails_the_change_that_archives_it(tmp_path):
    store = TradingStore(tmp_path / "hypercore.db", keep=0)
    store.create(document(effects=(effect("a", T0),)))
    store.update(lambda s: s)

    def reapplied(state):
        book = state.book.model_copy(update={"effects": (effect("a", T0),)})
        return state.model_copy(update={"book": book})

    store.update(reapplied)
    before = store.load()
    with pytest.raises(sqlite3.IntegrityError):
        store.update(lambda s: s)
    assert store.load() == before


# The same decisions against a bounded and an unbounded store.


class Refusing:
    """The deployment's signer, refusing once when asked to."""

    def __init__(self, signer):
        self.signer, self.refuse = signer, False

    def sign_hyperliquid_action(self, *args):
        if self.refuse:
            self.refuse = False
            raise SignerRefused("signer refused the action")
        return self.signer.sign_hyperliquid_action(*args)


def leverage(run, trader, signer) -> None:
    trader.leverage(COIN)


def round_trips(run, trader, signer) -> None:
    run.model.withhold("order")
    for _ in range(3):
        trader.enter(COIN, True, SIZE)
        trader.require_resolved()
        run.advance(31)
        trader.exit(COIN)
        trader.reconcile()
        run.advance(31)


def released_entries(run, trader, signer) -> None:
    """A lost entry and a refused one: a re-plan streak of two."""
    run.model.lose("order")
    trader.enter(COIN, True, SIZE)
    run.advance(trader.state().trading.policy.expiry // 1000 + 2)
    assert trader.recover() == ()
    signer.refuse = True
    with pytest.raises(SignerRefused):
        trader.enter(COIN, True, SIZE)
    trader.reconcile()


def held_across_hour(run, trader, signer) -> None:
    trader.enter(COIN, True, SIZE)
    run.cross_hour()
    trader.reconcile()
    trader.exit(COIN)
    trader.reconcile()


def sent_past_expiry(run, trader, signer) -> None:
    """An exit prepared and sent only once reconciliation is past its expiry: HyperCore
    refuses it, and the send returns it released."""
    trader.enter(COIN, True, SIZE)
    size = trader.owned_exposure().owned[COIN]
    book = trader.observe(L2Book(coin=COIN))
    now = trader.venue.now()
    late = trader.retain(
        lambda s: rules.plan_exit(s.trading, COIN, trader.agent(s), size, book, now)
    )
    trader.prepare(late.id)
    run.advance(31)
    assert trader.reconcile().book.cursor > late.request.expires_after
    assert trader.send(late.id).status == "not_applied"
    trader.exit(COIN)
    trader.reconcile()


def liquidated(run, trader, signer) -> None:
    trader.enter(COIN, True, SIZE)
    run.advance(31)
    run.model.move_mark(COIN, LIQUIDATION_MARK, "synthesized drop below maintenance margin")
    trader.reconcile()


DECISIONS = (
    leverage,
    round_trips,
    released_entries,
    held_across_hour,
    sent_past_expiry,
    liquidated,
)


def quietly(step):
    """A phase 4 driver step, its report discarded."""
    return lambda run, trader, signer: step(run, trader, {})


def switch_fired(run, trader, signer) -> None:
    """Protective orders rest, the dead man's switch cancels them and a restart reads
    both as cancelled; the position is protected again."""
    resting.protect(trader)
    trader.schedule_cancel(run.now() + 60_000)
    run.advance(90)
    assert Trader(trader.store, trader.venue, trader.signer).restart().trading.live() == ()
    resting.protect(trader)


def took_profit(run, trader, signer) -> None:
    """The take-profit triggers, and the stop-loss is cancelled by its cloid."""
    run.model.move_mark(COIN, resting.PROFIT_MARK, "synthesized rise through the take-profit")
    trader.reconcile()
    (stop,) = trader.refresh().trading.live()
    assert trader.cancel_by_cloid(stop.cloid).status == "applied"
    trader.exit(COIN)
    trader.reconcile()


def torn_down(run, trader, signer) -> None:
    """Teardown's first cancel is lost; once it is released teardown completes."""
    resting.setup_for_teardown(trader)
    run.model.lose("cancel")
    assert not trader.teardown().complete
    run.advance(trader.state().trading.policy.expiry // 1000 + 2)
    require_complete(trader.teardown())
    # Past the cancels' expiry, no fill of either order can still be unreconciled.
    run.advance(trader.state().trading.policy.expiry // 1000 + 2)
    trader.reconcile()


RESTING = (
    *map(
        quietly,
        (
            resting.isolated_leverage,
            resting.resting_entry_withheld,
            resting.restart_while_resting,
            resting.mark_fills_resting,
            resting.margin_added,
        ),
    ),
    switch_fired,
    took_profit,
    torn_down,
)


def decided(state: HyperCoreState) -> tuple:
    """What the next decision rests on, agent keys aside."""
    book, trading = state.book, state.trading
    return (
        trading.released,
        trading.pending,
        rules.next_id(trading),
        tuple(o.oid for o in trading.live()),
        trading.schedule,
        (book.cursor, book.positions, book.owned, book.cash),
    )


def lifecycle(tmp_path, keep: int, opener=open_store, decisions=DECISIONS):
    tmp_path.mkdir()
    model, chain, meta = offline(tmp_path)
    keys = LocalAgentKeys(tmp_path / "agent-keys.json", "offline")
    with ModelService(model) as service:
        venue = ModelVenue(service.url, lambda: chain.blocks[-1]["timestamp"] * 1000)
        opened = opener(tmp_path / "hypercore.db", "offline", Custody(safe=SAFE), meta, T0)
        store = TradingStore(opened.path, keep=keep)
        run = OfflineRun(model, chain, venue.info, keys, store)
        run.fund_perp({})
        signer = Refusing(VerifiedSigner(LocalSigner(keys), LocalSigner.address))
        trader = Trader(store, venue, signer)
        trace = []
        for each in decisions:
            each(run, trader, signer)
            trace.append(decided(trader.state()))
        venue.close()
    return store, trace


def outcome(attempt: Attempt) -> tuple:
    return (attempt.id, attempt.purpose, attempt.status, attempt.cloid, attempt.oids)


def settled_history(history) -> tuple:
    return (
        [outcome(a) for a in history.attempts],
        [(o.oid, o.attempt, o.status) for o in history.orders],
        [e.key for e in history.effects],
    )


def test_a_bounded_store_decides_as_an_unbounded_one(tmp_path):
    bounded, kept = lifecycle(tmp_path / "bounded", keep=0)
    unbounded, everything = lifecycle(tmp_path / "unbounded", keep=10**9)
    assert kept == everything
    assert kept[2][0] == 2 and kept[-1][-1][1] == {}
    history = bounded.history()
    assert settled_history(history) == settled_history(unbounded.history())
    assert {e.kind for e in history.effects} == {"fill", "funding", "ledger"}
    # What the last change settled leaves the row at the next one.
    row, whole = bounded.update(lambda s: s), unbounded.load()
    assert row.trading.archived + len(row.trading.attempts) == len(whole.trading.attempts)
    assert len(row.trading.attempts) <= 3 and row.book.effects == ()


def test_resting_and_protective_orders_decide_alike_when_bounded(tmp_path):
    bounded, kept = lifecycle(tmp_path / "bounded", 0, resting.open_store, RESTING)
    unbounded, everything = lifecycle(tmp_path / "unbounded", 10**9, resting.open_store, RESTING)
    assert kept == everything
    history = bounded.history()
    assert settled_history(history) == settled_history(unbounded.history())
    statuses = {o.status for o in history.orders}
    assert statuses >= {"filled", "scheduledCancel", "canceled"}
    row = bounded.update(lambda s: s)
    assert row.trading.orders == () and len(row.trading.attempts) <= 3
