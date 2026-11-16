"""Measure how the deployment's durable HyperCore document grows (blueprint 30, 5.5).

Real decisions run first against the synthesized model offline, through phase 3's
`Trader` on the fake chain of `tests/test_hypercore_trading.py`: entries and reduce-only
exits that fill, one entry whose answer is withheld and that recovers from `orderStatus`,
and a position held across an hour boundary, which pays funding. The document they
leave is then grown to a long-running deployment's volume by repeating those decisions'
attempts and effects under fresh identities and times: by default one order decision a
minute, alternating entry and exit, one in a hundred recovered from evidence, and one
funding payment an hour. With `--mix resting` each minute also re-arms the dead man's
switch and either places a protective order, owned with its latest `orderStatus`, or
cancels the last one, repeating attempts and an owned order of phase 4's resting
reference lifecycle (`tests/hypercore_resting_support.py`).

At each volume the script writes the grown document to a fresh store, lets the store
run one `update` so whatever it does with settled history is done, and then records:

- the bytes of the durable row, and of any other table the store keeps;
- how long `load` takes;
- how long `update` holds the write lock (from `BEGIN IMMEDIATE` returning to `COMMIT`
  returning) for a decision's first step, retaining one entry attempt;
- whether a second writer, which waits `connect`'s 5 seconds for that lock, gets it.

    uv run --locked python -m tests.hypercore_store_growth --days 1 7 15 30 [--mix resting]
"""

import argparse
import json
import sqlite3
import tempfile
import threading
import time
from pathlib import Path

from almanak.connectors.hyperliquid import trading as rules
from almanak.connectors.hyperliquid.custody import Custody
from almanak.connectors.hyperliquid.effects import Effect
from almanak.connectors.hyperliquid.fork_service import ModelService
from almanak.connectors.hyperliquid.trading import Attempt, OwnedOrder
from almanak.connectors.hyperliquid.trading_runtime import Trader
from almanak.connectors.hyperliquid.trading_store import HyperCoreState, TradingStore
from almanak.gateway.hypercore_agent_keys import LocalAgentKeys
from almanak.gateway.hypercore_signing import OrderAction
from almanak.gateway.signers import VerifiedSigner
from tests import hypercore_resting_support as resting
from tests.hypercore_trading_support import COIN, SIZE, LocalSigner, ModelVenue, open_store
from tests.test_hypercore_custody import SAFE, T0
from tests.test_hypercore_trading import OfflineRun, offline

MINUTE, HOUR, DAY = 60_000, 3_600_000, 86_400_000
DEPLOYMENT = "growth"


class Timed:
    """A connection that marks when the write lock is taken and released."""

    def __init__(self, connection: sqlite3.Connection, marks: dict, taken: threading.Event):
        self.connection, self.marks, self.taken = connection, marks, taken

    def execute(self, sql, *args):
        result = self.connection.execute(sql, *args)
        if sql == "BEGIN IMMEDIATE":
            self.marks["begin"] = time.perf_counter()
            self.taken.set()
        elif sql == "COMMIT":
            self.marks["commit"] = time.perf_counter()
        return result

    def __getattr__(self, name):
        return getattr(self.connection, name)


class TimedStore(TradingStore):
    def __init__(self, path: Path):
        super().__init__(path)
        self.marks: dict = {}
        self.taken = threading.Event()

    def connect(self):
        return Timed(super().connect(), self.marks, self.taken)


# The decisions the model actually answers.


def template(work: Path, cycles: int) -> HyperCoreState:
    model, chain, meta = offline(work)
    keys = LocalAgentKeys(work / "agent-keys.json", DEPLOYMENT)
    with ModelService(model) as service:
        venue = ModelVenue(service.url, lambda: chain.blocks[-1]["timestamp"] * 1000)
        store = open_store(work / "template.db", DEPLOYMENT, Custody(safe=SAFE), meta, T0 - 1000)
        run = OfflineRun(model, chain, venue.info, keys, store)
        run.fund_perp({})
        trader = Trader(store, venue, VerifiedSigner(LocalSigner(keys), LocalSigner.address))
        trader.leverage(COIN)
        for cycle in range(cycles):
            if cycle == 0:
                model.withhold("order")
            trader.enter(COIN, True, SIZE)
            trader.require_resolved()
            run.advance(30)
            if cycle == cycles - 1:
                run.cross_hour()
            trader.exit(COIN)
            trader.reconcile()
            run.advance(30)
        venue.close()
        return store.load()


def resting_template(work: Path) -> HyperCoreState:
    work.mkdir()
    model, chain, meta = offline(work)
    keys = LocalAgentKeys(work / "agent-keys.json", DEPLOYMENT)
    with ModelService(model) as service:
        venue = ModelVenue(service.url, lambda: chain.blocks[-1]["timestamp"] * 1000)
        custody = Custody(safe=SAFE)
        store = resting.open_store(work / "resting.db", DEPLOYMENT, custody, meta, T0 - 1000)
        run = OfflineRun(model, chain, venue.info, keys, store)
        signer = VerifiedSigner(LocalSigner(keys), LocalSigner.address)
        resting.resting_lifecycle(run, Trader(store, venue, signer))
        venue.close()
        return store.load()


class Resting:
    """Phase 4's attempts: a dead man's switch armed, a protective order with the owned
    order it placed, read by `orderStatus`, and a cancel."""

    def __init__(self, state: HyperCoreState):
        trading = state.trading
        self.schedule = purposed(trading.attempts, "schedule")
        self.cancel = purposed(trading.attempts, "cancel")
        self.order = protective(trading)
        self.protect = trading.attempt(self.order.attempt)

    def decision(self, attempts: list, orders: list, at: int, index: int) -> None:
        """Re-arm the switch, then place a protective order or cancel the last one."""
        attempts.append(renamed(self.schedule, str(len(attempts) + 1), at, None))
        attempt_id = str(len(attempts) + 1)
        if index % 2 == 1:
            attempts.append(cancelled(self.cancel, attempt_id, at, orders[-1].oid))
            return
        oid = 2 * 10**9 + index
        attempts.append(renamed(self.protect, attempt_id, at, oid))
        orders.append(placed(self.order, attempt_id, oid))


def protective(trading) -> OwnedOrder:
    """A protective order whose state was read from `orderStatus`."""
    protects = {a.id for a in trading.attempts if a.purpose == "protect"}
    return next(o for o in trading.orders if read_by_status(o, protects))


def read_by_status(order: OwnedOrder, placers: set[str]) -> bool:
    return order.attempt in placers and order.evidence is not None


def purposed(attempts, purpose: str) -> Attempt:
    return next(a for a in attempts if a.purpose == purpose and a.status == "applied")


def placed(order: OwnedOrder, attempt_id: str, oid: int) -> OwnedOrder:
    cloid = rules.cloid_for(DEPLOYMENT, attempt_id)
    update = {"attempt": attempt_id, "oid": oid, "cloid": cloid, "status": "canceled"}
    return order.model_copy(update=update)


class Sample:
    """The template's order attempts and their fills, and one funding payment."""

    def __init__(self, state: HyperCoreState):
        orders = [a for a in state.trading.attempts if isinstance(a.request.action, OrderAction)]
        effects = state.book.effects
        self.recovered = first(orders, ("entry", "exit"), recovered=True)
        self.entry = first(orders, ("entry",), recovered=False)
        self.exit = first(orders, ("exit",), recovered=False)
        self.fills = {a.id: fills_of(effects, a) for a in orders}
        self.funding = fills_of(effects, None, kind="funding")[0]

    def decision(self, index: int, withheld_every: int) -> Attempt:
        if index % withheld_every == 0:
            return self.recovered
        return self.entry if index % 2 == 0 else self.exit


def first(orders: list[Attempt], purposes: tuple[str, ...], recovered: bool) -> Attempt:
    return next(a for a in orders if a.purpose in purposes and bool(a.evidence) == recovered)


def fills_of(effects, attempt: Attempt | None, kind: str = "fill") -> list[Effect]:
    """The effects of `kind`, those of `attempt`'s own order when one is given."""
    return [e for e in effects if e.kind == kind and own(e, attempt)]


def own(effect: Effect, attempt: Attempt | None) -> bool:
    if attempt is None:
        return True
    fill = effect.entry
    assert isinstance(fill, dict)
    return fill.get("cloid") == attempt.cloid or fill.get("oid") in attempt.oids


def renamed(attempt: Attempt, attempt_id: str, at: int, oid: int | None) -> Attempt:
    request = attempt.request.model_copy(update={"nonce": at, "expires_after": at + 30_000})
    action = request.action
    if isinstance(action, OrderAction):
        cloid = rules.cloid_for(DEPLOYMENT, attempt_id)
        order = action.order.model_copy(update={"cloid": cloid})
        request = request.model_copy(update={"action": OrderAction(order=order)})
    signed = attempt.signed and attempt.signed.model_copy(update={"request": request})
    oids = () if oid is None else (oid,)
    return attempt.model_copy(
        update={"id": attempt_id, "request": request, "signed": signed, "oids": oids}
    )


def cancelled(attempt: Attempt, attempt_id: str, at: int, oid: int) -> Attempt:
    """A cancel of the owned order `oid`."""
    attempt = renamed(attempt, attempt_id, at, None)
    action = attempt.request.action.model_copy(update={"oid": oid})
    request = attempt.request.model_copy(update={"action": action})
    signed = attempt.signed and attempt.signed.model_copy(update={"request": request})
    return attempt.model_copy(update={"request": request, "signed": signed, "cancels": oid})


def grown(base: HyperCoreState, sample: Sample, decisions: int, withheld_every: int, extra):
    """`base` with `decisions` more order decisions, one a minute, their effects and, with
    `extra`, phase 4's attempts and owned orders."""
    attempts, effects = list(base.trading.attempts), list(base.book.effects)
    orders = list(base.trading.orders)
    start = base.book.cursor + MINUTE
    for index in range(decisions):
        at = start + index * MINUTE
        original = sample.decision(index, withheld_every)
        attempt_id = str(len(attempts) + 1)
        attempts.append(renamed(original, attempt_id, at, 10**9 + index))
        for offset, fill in enumerate(sample.fills[original.id]):
            key = f"fill:{COIN}:{at + 1}:{10**12 + index * 10 + offset}"
            effects.append(fill.model_copy(update={"key": key, "time": at + 1}))
        if (at - start) % HOUR == 0:
            key = f"funding:{COIN}:{at + 2}"
            effects.append(sample.funding.model_copy(update={"key": key, "time": at + 2}))
        if extra is not None:
            extra.decision(attempts, orders, at, index)
    last = start + decisions * MINUTE
    nonces = {**base.trading.nonces, sample.entry.agent: last}
    trading = base.trading.model_copy(
        update={"attempts": tuple(attempts), "orders": tuple(orders), "nonces": nonces}
    )
    book = base.book.model_copy(update={"effects": tuple(effects), "cursor": last + 3})
    return base.model_copy(update={"trading": trading, "book": book}), last + MINUTE


# One volume, measured.


def table_bytes(path: Path) -> dict:
    connection = sqlite3.connect(path)
    try:
        listed = connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        sizes = {}
        for (table,) in listed.fetchall():
            columns = [r[1] for r in connection.execute(f"PRAGMA table_info({table})")]
            total = " + ".join(f"coalesce(length({c}), 0)" for c in columns)
            query = f"SELECT count(*), coalesce(sum({total}), 0) FROM {table}"
            (rows, size) = connection.execute(query).fetchone()
            sizes[table] = {"rows": rows, "bytes": size}
        return sizes
    finally:
        connection.close()


def retain_entry(sample: Sample, now: int):
    def change(state: HyperCoreState) -> HyperCoreState:
        book = sample.entry.book
        trading, _ = rules.plan_entry(
            state.trading, COIN, sample.entry.agent, True, SIZE, book, now
        )
        return state.model_copy(update={"trading": trading})

    return change


def contended(store: TimedStore, change) -> dict:
    """A second writer starting while the first holds the lock."""
    store.taken.clear()
    outcome: dict = {}
    first = threading.Thread(target=store.update, args=(change,))
    first.start()
    store.taken.wait()
    began = time.perf_counter()
    try:
        TradingStore(store.path).update(lambda s: s)
        outcome["second_writer"] = "committed"
    except sqlite3.OperationalError as error:
        outcome["second_writer"] = str(error)
    outcome["second_writer_waited_s"] = round(time.perf_counter() - began, 3)
    first.join()
    return outcome


def measured(work: Path, state: HyperCoreState, sample: Sample, now: int) -> dict:
    path = work / f"grown-{len(state.trading.attempts)}.db"
    store = TimedStore(path)
    store.create(state)
    # Let the store settle whatever it moves out of the row, then measure a decision.
    store.update(lambda s: s)
    began = time.perf_counter()
    loaded = store.load()
    load_s = time.perf_counter() - began
    began = time.perf_counter()
    store.update(retain_entry(sample, now))
    update_s = time.perf_counter() - began
    hold_s = store.marks["commit"] - store.marks["begin"]
    row = loaded.model_dump_json()
    return {
        "attempts": len(state.trading.attempts),
        "orders": len(state.trading.orders),
        "effects": len(state.book.effects),
        "row_attempts": len(loaded.trading.attempts),
        "row_orders": len(loaded.trading.orders),
        "row_effects": len(loaded.book.effects),
        "row_bytes": len(row),
        "tables": table_bytes(path),
        "file_bytes": path.stat().st_size + wal_size(path),
        "load_s": round(load_s, 3),
        "update_s": round(update_s, 3),
        "lock_hold_s": round(hold_s, 3),
        **contended(store, lambda s: s),
    }


def wal_size(path: Path) -> int:
    wal = path.with_name(path.name + "-wal")
    return wal.stat().st_size if wal.exists() else 0


def size(model) -> int:
    return len(model.model_dump_json())


def resting_parts(extra: Resting) -> dict:
    return {
        "schedule_attempt": size(extra.schedule),
        "protect_attempt": size(extra.protect),
        "owned_order": size(extra.order),
        "cancel_attempt": size(extra.cancel),
    }


def parts(state: HyperCoreState, sample: Sample) -> dict:
    """The bytes of each part of the document, and of one attempt and effect of each kind."""
    return {
        "custody": size(state.custody),
        "entry_attempt": size(sample.entry),
        "entry_attempt_book": size(sample.entry.book),
        "exit_attempt": size(sample.exit),
        "recovered_attempt": size(sample.recovered),
        "recovered_attempt_evidence": sum(size(e) for e in sample.recovered.evidence),
        "fill_effect": size(sample.fills[sample.entry.id][0]),
        "fills_per_order": len(sample.fills[sample.entry.id]),
        "funding_effect": size(sample.funding),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--days", type=float, nargs="+", default=[1, 7, 15, 30])
    parser.add_argument("--per-hour", type=int, default=60, help="order decisions an hour")
    parser.add_argument("--withheld-every", type=int, default=100)
    parser.add_argument("--cycles", type=int, default=4, help="real decisions per kind")
    parser.add_argument("--mix", choices=("orders", "resting"), default="orders")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    report: dict = {"mix": args.mix, "volumes": []}
    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory)
        base = template(work, args.cycles)
        sample = Sample(base)
        extra = Resting(resting_template(work / "resting")) if args.mix == "resting" else None
        report["template"] = {
            "attempts": len(base.trading.attempts),
            "effects": len(base.book.effects),
            "bytes": {**parts(base, sample), **(resting_parts(extra) if extra else {})},
        }
        for days in args.days:
            decisions = int(days * 24 * args.per_hour)
            state, now = grown(base, sample, decisions, args.withheld_every, extra)
            volume = {"days": days, "decisions": decisions, **measured(work, state, sample, now)}
            report["volumes"].append(volume)
            print(json.dumps(volume), flush=True)
    text = json.dumps(report, indent=2)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
