"""The deployment's durable HyperCore document: custody, trading attempts and the book.

One row of one SQLite file, replaced in an immediate transaction and validated before it
commits, as the runtime's checkpoint is (`state.Journal`). Every change that precedes an
external action commits first: an attempt and its nonce before it is signed, its signed
body before it is sent, its maybe-sent mark before the post. A process killed at any point
restarts from the last committed document and reconciles from there.

Each change first moves the history settled before it out of the row, in its own
transaction, to append-only tables of the same file, so the row, and how long each change
holds the write lock, stay bounded however long the deployment runs (blueprint 30, section
5.5). The document a change returns still holds everything that change touched, for the
caller that reads it back. Nothing reads the tables back to decide:

- an owned order moves out once it is no longer live, no unresolved cancel names it and
  the book's cursor is past the last time it could have filled: its final `orderStatus`
  time when one was read, else the latest expiry of the attempts that placed or
  cancelled it;
- an attempt moves out once it is resolved, the cursor is past every time it could have
  filled (`fill_range`; no page still to be reconciled can then carry its cloid or oids),
  no owned order in the row was placed by it, no unresolved attempt names it and the
  re-plan streak does not walk over it;
- an applied effect moves out once its time is before the cursor: every page starts at
  the cursor, so none can return it, and its key stays unique in its table.

The newest `keep` attempts, owned orders and effects stay in the row whatever their
state, as the document's recent history.
"""

import os
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from itertools import takewhile
from pathlib import Path
from urllib.parse import quote

from almanak.connectors.hyperliquid.custody import Custody
from almanak.connectors.hyperliquid.effects import Book, Effect
from almanak.connectors.hyperliquid.trading import Attempt, OwnedOrder, Trading, fill_range
from almanak.gateway.hypercore_info import typed
from almanak.gateway.hypercore_shapes import KnownOrder
from almanak.models import Model

# The settled attempts, owned orders and effects the row keeps as recent history.
KEEP = 100
TABLES = (
    "CREATE TABLE hypercore (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL)",
    "CREATE TABLE hypercore_attempts (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
    "CREATE TABLE hypercore_orders (oid INTEGER PRIMARY KEY, body TEXT NOT NULL)",
    "CREATE TABLE hypercore_effects (seq INTEGER PRIMARY KEY, key TEXT NOT NULL UNIQUE,"
    " time INTEGER NOT NULL, body TEXT NOT NULL)",
)


class AccountingFailure(Model):
    """A reconciliation failure an exit went ahead despite: independent risk reduction
    continues while the accounting failure stays recorded and visible until a later
    reconciliation succeeds."""

    kind: str
    message: str
    # The deployment clock when the exit went ahead.
    at: int


class HyperCoreState(Model):
    custody: Custody
    trading: Trading
    book: Book
    accounting: AccountingFailure | None = None


@dataclass(frozen=True)
class Settled:
    """A document with its settled history split out."""

    state: HyperCoreState
    attempts: tuple[Attempt, ...]
    orders: tuple[OwnedOrder, ...]
    effects: tuple[Effect, ...]


@dataclass(frozen=True)
class History:
    attempts: tuple[Attempt, ...]
    orders: tuple[OwnedOrder, ...]
    effects: tuple[Effect, ...]


def settled(state: HyperCoreState, keep: int = KEEP) -> Settled:
    """The attempts, owned orders and effects older than the newest `keep` that nothing
    still reads."""
    trading, book = state.trading, state.book
    orders = settled_orders(trading, book.cursor, keep)
    oids = {o.oid for o in orders}
    trading = trading.model_copy(
        update={"orders": tuple(o for o in trading.orders if o.oid not in oids)}
    )
    attempts = settled_attempts(trading, book.cursor, keep)
    ids = {a.id for a in attempts}
    trading = trading.model_copy(
        update={
            "attempts": tuple(a for a in trading.attempts if a.id not in ids),
            "archived": trading.archived + len(attempts),
        }
    )
    effects = book.effects[: max(0, len(book.effects) - keep)]
    applied = tuple(takewhile(lambda e: e.time < book.cursor, effects))
    book = book.model_copy(update={"effects": book.effects[len(applied) :]})
    kept = state.model_copy(update={"trading": trading, "book": book})
    return Settled(state=kept, attempts=attempts, orders=orders, effects=applied)


def settled_orders(trading: Trading, cursor: int, keep: int) -> tuple[OwnedOrder, ...]:
    """The owned orders older than the newest `keep` that the row no longer needs: not
    live, named by no unresolved cancel, and with the cursor past their last possible
    fill (`last_fill`)."""
    named = {a.cancels for a in trading.attempts if a.cancels is not None and not a.resolved}
    older = trading.orders[: max(0, len(trading.orders) - keep)]
    return tuple(
        o for o in older if not o.live and o.oid not in named and last_fill(trading, o) < cursor
    )


def last_fill(trading: Trading, order: OwnedOrder) -> int:
    """The last time a settled order could have filled: its final `orderStatus` time, or
    without one the latest expiry of the attempts in the row that placed or cancelled it.
    HyperCore applies neither past its expiry, and an archived cancel is already behind
    the cursor."""
    known = typed(order.evidence) if order.evidence is not None else None
    if isinstance(known, KnownOrder):
        return known.order.statusTimestamp
    return max(
        a.request.expires_after
        for a in trading.attempts
        if a.id == order.attempt or a.cancels == order.oid
    )


def settled_attempts(trading: Trading, cursor: int, keep: int) -> tuple[Attempt, ...]:
    """The attempts older than the newest `keep` that nothing still reads. The re-plan
    streak stays because `released` counts it, the placer of every owned order in the row
    because admission bounds reduce-only orders by it, and any attempt an unresolved
    `noop` retires, whose agent and nonce admission checks that `noop` against."""
    held = {a.id for a in trading.streak()} | {o.attempt for o in trading.orders}
    held |= {a.retires for a in trading.attempts if a.retires and not a.resolved}
    older = trading.attempts[: max(0, len(trading.attempts) - keep)]
    return tuple(a for a in older if a.resolved and fill_range(a)[1] < cursor and a.id not in held)


class TradingStore:
    def __init__(self, path: Path, keep: int = KEEP):
        self.path = path.resolve()
        self.keep = keep

    def connect(self) -> sqlite3.Connection:
        uri = f"file:{quote(str(self.path))}?mode=rw"
        return sqlite3.connect(uri, uri=True, timeout=5, isolation_level=None)

    def create(self, state: HyperCoreState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            # The tables and the one row commit together: a process stopped between them
            # must not leave a table with no document.
            connection.execute("BEGIN IMMEDIATE")
            for table in TABLES:
                connection.execute(table)
            connection.execute("INSERT INTO hypercore VALUES (1, ?)", (state.model_dump_json(),))
            connection.execute("COMMIT")
        finally:
            connection.close()

    def load(self) -> HyperCoreState:
        connection = self.connect()
        try:
            (body,) = connection.execute("SELECT body FROM hypercore WHERE id=1").fetchone()
        finally:
            connection.close()
        return HyperCoreState.model_validate_json(body)

    def update(self, change: Callable[[HyperCoreState], HyperCoreState]) -> HyperCoreState:
        """Apply `change` in one immediate transaction and return the document it made.

        What had settled before the change moves to the archive tables first, so the
        returned document holds everything the change touched, which the caller reads
        back; what the change settles leaves at the next change. A change that raises, or
        that would archive a key already archived, commits nothing."""
        connection = self.connect()
        try:
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("BEGIN IMMEDIATE")
            (body,) = connection.execute("SELECT body FROM hypercore WHERE id=1").fetchone()
            split = settled(HyperCoreState.model_validate_json(body), self.keep)
            changed = change(split.state)
            payload = changed.model_dump_json()
            HyperCoreState.model_validate_json(payload)
            archive(connection, split)
            connection.execute("UPDATE hypercore SET body=? WHERE id=1", (payload,))
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        return changed

    def history(self) -> History:
        """Every attempt in the order it was planned, every owned order in the order it
        was placed and every effect in the order it was applied, archived and retained,
        from one snapshot."""
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            attempts = connection.execute("SELECT body FROM hypercore_attempts").fetchall()
            orders = connection.execute("SELECT body FROM hypercore_orders").fetchall()
            query = "SELECT body FROM hypercore_effects ORDER BY seq"
            effects = connection.execute(query).fetchall()
            (body,) = connection.execute("SELECT body FROM hypercore WHERE id=1").fetchone()
            connection.execute("COMMIT")
        finally:
            connection.close()
        state = HyperCoreState.model_validate_json(body)
        trading, book = state.trading, state.book
        planned = (*(Attempt.model_validate_json(b) for (b,) in attempts), *trading.attempts)
        owned = (*(OwnedOrder.model_validate_json(b) for (b,) in orders), *trading.orders)
        return History(
            attempts=tuple(sorted(planned, key=lambda a: int(a.id))),
            orders=tuple(sorted(owned, key=lambda o: (int(o.attempt), o.oid))),
            effects=(*(Effect.model_validate_json(b) for (b,) in effects), *book.effects),
        )


def archive(connection: sqlite3.Connection, split: Settled) -> None:
    """Append settled history; a key already archived fails the whole change."""
    connection.executemany(
        "INSERT INTO hypercore_attempts (id, body) VALUES (?, ?)",
        [(int(a.id), a.model_dump_json()) for a in split.attempts],
    )
    connection.executemany(
        "INSERT INTO hypercore_orders (oid, body) VALUES (?, ?)",
        [(o.oid, o.model_dump_json()) for o in split.orders],
    )
    connection.executemany(
        "INSERT INTO hypercore_effects (key, time, body) VALUES (?, ?, ?)",
        [(e.key, e.time, e.model_dump_json()) for e in split.effects],
    )
