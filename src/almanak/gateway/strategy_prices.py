"""Live strategy price series, read by the gateway from its chains and retained as evidence.

The gateway, not the sandboxed strategy, reads each declared series from the chain
reader of the binding's chain: the boundary blocks before the decision cutoff, the
pool's state at each and, for swap candles, the swaps between them. A fork reads
pre-fork history through its node, which forwards older blocks to the upstream
provider. Points, boundary blocks and the evidence behind them are kept in a
gateway-owned log, so each decision reads only boundaries it has not seen. Points
build with the same code backtests use, so a live decision and a backtest at the same
instant see the same points. Every served series is appended to the log, holds
included.

A point is retrieved at the timestamp of the chain head its read was pinned to, the
same clock the decision cutoff and the point's availability are measured on: a fork
whose time a test advances past the wall clock still retrieves a boundary only once
its chain has passed it. Reads of one series are serialized, so a decision arriving
while the start-up warm-up reads its window waits for it and then reads only what the
warm-up did not.
"""

import json
import sqlite3
import sys
import threading
import time
from pathlib import Path

from almanak.backtest.pool_prices import (
    PricePool,
    last_boundary,
    series_records,
)
from almanak.backtest.pool_reads import DirectReads, acquire, pool_scope
from almanak.backtest.pricing import PoolPriceRecord, PoolSeriesSource, PriceBinding
from almanak.backtest.strategy_prices import is_series
from almanak.gateway.ethereum import DataError
from almanak.models import Block
from almanak.price_series import PriceSeries, series_at

TABLES = (
    "CREATE TABLE IF NOT EXISTS pools (key TEXT PRIMARY KEY, body TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS boundaries (chain TEXT NOT NULL, time INTEGER NOT NULL, "
    "block TEXT NOT NULL, following TEXT NOT NULL, PRIMARY KEY (chain, time))",
    "CREATE TABLE IF NOT EXISTS points (source TEXT NOT NULL, time INTEGER NOT NULL, "
    "body TEXT NOT NULL, PRIMARY KEY (source, time))",
    "CREATE TABLE IF NOT EXISTS evidence (digest TEXT PRIMARY KEY, body TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS served_series (id INTEGER PRIMARY KEY, "
    "deployment TEXT NOT NULL, cutoff INTEGER NOT NULL, served_at REAL NOT NULL, "
    "series TEXT NOT NULL)",
)


class StrategyPriceSource:
    def __init__(self, backend, log: Path):
        self.backend = backend
        self.lock = threading.Lock()
        self.windows: dict[str, threading.Lock] = {}
        self.log = log
        with self.connect() as db:
            for statement in TABLES:
                db.execute(statement)

    def connect(self):
        return sqlite3.connect(self.log)

    def reader(self, chain):
        from almanak.gateway.routing import backend_for

        try:
            return backend_for(self.backend, chain).reader
        except ValueError:
            raise DataError(
                "missing_provider", f"this gateway has no provider for {chain.chain_id}"
            ) from None

    def series(self, deployment: str, bindings, cutoff: int) -> tuple[PriceSeries, ...]:
        result = tuple(
            self.read(binding, cutoff, wait=DECISION_READ_SECONDS) for binding in bindings
        )
        with self.lock, self.connect() as db:
            for item in result:
                db.execute(
                    "INSERT INTO served_series (deployment, cutoff, served_at, series) "
                    "VALUES (?, ?, ?, ?)",
                    (deployment, cutoff, time.time(), item.model_dump_json()),
                )
        return result

    def window(self, binding: PriceBinding) -> threading.Lock:
        with self.lock:
            return self.windows.setdefault(binding.key.model_dump_json(), threading.Lock())

    def read(self, binding: PriceBinding, cutoff: int, wait: float | None = None) -> PriceSeries:
        """The window as of `cutoff`; waits at most `wait` seconds for another read of it."""
        if not isinstance(binding.source, PoolSeriesSource):
            raise DataError("unsupported", "a live strategy price reads a pool series")
        window = self.window(binding)
        if not window.acquire(timeout=-1 if wait is None else wait):
            raise DataError(
                "price_window_pending",
                f"the {binding.asset} window is still being read; the gateway retains it "
                "for the next decision",
            )
        try:
            return self.read_window(binding, cutoff)
        finally:
            window.release()

    def read_window(self, binding: PriceBinding, cutoff: int) -> PriceSeries:
        source = binding.source
        assert isinstance(source, PoolSeriesSource)
        seconds = source.seconds
        last = last_boundary(binding, cutoff)
        times = range(last - (binding.window - 1) * seconds, last + 1, seconds)
        rows = self.points(binding, times)
        missing = [t for t in times if t not in rows]
        try:
            if missing:
                rows |= self.extend(binding, missing)
            return series_at(binding, rows.values(), cutoff)
        except ValueError as error:
            # Chain data that fails its checks is a failed source for this decision,
            # never a gateway request error.
            raise DataError("pool_data", str(error)) from None

    def extend(self, binding, times) -> dict[int, PoolPriceRecord]:
        """Read and retain the points at `times`; earlier boundaries bracket the search."""
        reader = self.reader(binding.chain)
        with reader.retained_pin():
            # Pinned before the reads, which pin the head after it.
            final = reader.pin_block("finalized").number
            reads = DirectReads(reader)
            pool = self.pool(binding, reads)
            known = self.boundaries(binding, min(times), max(times))
            evidence = acquire(reads, binding, times, known=known, pool=pool)
            rows = series_records(binding, evidence, times, reads.upper.timestamp)
        # Only a boundary whose successor is finalized is retained, and with it its point:
        # a later decision reuses retained boundaries without reading them again, so a
        # reorgable one would be served, or opened against, after it left the chain.
        # Newer points are served now and read again until they are final.
        settled = {
            int(state.time): state
            for state in evidence.states.values()
            if state.following.number <= final
        }
        chain = binding.chain.model_dump_json()
        with self.lock, self.connect() as db:
            for state in settled.values():
                db.execute(
                    "INSERT OR IGNORE INTO boundaries VALUES (?, ?, ?, ?)",
                    (
                        chain,
                        int(state.time),
                        state.block.model_dump_json(),
                        state.following.model_dump_json(),
                    ),
                )
            for digest, body in evidence.bodies.items():
                db.execute("INSERT OR IGNORE INTO evidence VALUES (?, ?)", (digest, body.body))
            for row in rows:
                if int(row.source_time) not in settled:
                    continue
                db.execute(
                    "INSERT OR IGNORE INTO points VALUES (?, ?, ?)",
                    (row.key.model_dump_json(), int(row.source_time), row.model_dump_json()),
                )
        return {int(row.source_time): row for row in rows}

    def points(self, binding, times) -> dict[int, PoolPriceRecord]:
        with self.connect() as db:
            found = db.execute(
                "SELECT time, body FROM points WHERE source=? AND time BETWEEN ? AND ?",
                (binding.key.model_dump_json(), min(times), max(times)),
            ).fetchall()
        return {at: PoolPriceRecord.model_validate_json(body) for at, body in found}

    def boundaries(self, binding, first: int, last: int) -> dict[int, tuple[Block, Block]]:
        """Retained boundary blocks near the requested points, and the newest before them."""
        chain = binding.chain.model_dump_json()
        opening = first - binding.source.seconds
        with self.connect() as db:
            rows = db.execute(
                "SELECT time, block, following FROM boundaries WHERE chain=? AND "
                "(time BETWEEN ? AND ? OR time=(SELECT MAX(time) FROM boundaries "
                "WHERE chain=? AND time<?))",
                (chain, opening, last, chain, opening),
            ).fetchall()
        return {
            at: (Block.model_validate_json(block), Block.model_validate_json(following))
            for at, block, following in rows
        }

    def pool(self, binding, reads) -> PricePool:
        """The pool resolved once at the head, retained; the asset and quote must be two
        of its coins."""
        scope = pool_scope(binding)
        key = scope.model_dump_json()
        with self.connect() as db:
            row = db.execute("SELECT body FROM pools WHERE key=?", (key,)).fetchone()
        if row is not None:
            pool = PricePool.model_validate_json(row[0])
        else:
            pool = reads.pool(scope, reads.upper)
            with self.lock, self.connect() as db:
                db.execute(
                    "INSERT OR IGNORE INTO pools VALUES (?, ?)", (key, pool.model_dump_json())
                )
        pool.pair(binding.asset, binding.source.quote)
        return pool

    def served(self, deployment: str) -> tuple[PriceSeries, ...]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT series FROM served_series WHERE deployment=? ORDER BY id", (deployment,)
            ).fetchall()
        return tuple(PriceSeries.model_validate_json(row[0]) for row in rows)


def strategy_bindings(prepared) -> tuple[PriceBinding, ...] | None:
    """The series a prepared deployment declared, or None without a preparation.

    Only pool series are served live; a single strategy price is backtest-only.
    """
    if prepared is None:
        return None
    return tuple(
        b for b in prepared.dependencies.prices if b.purpose == "strategy" and is_series(b)
    )


def require_declared(requested, declared):
    if declared is not None and set(requested) != set(declared):
        raise DataError("unauthorized", "requested price series differ from the prepared strategy")
    if not all(b.purpose == "strategy" and is_series(b) for b in requested):
        raise DataError("unsupported", "a live strategy price reads a pool series")
    if len({binding.asset for binding in requested}) != len(requested):
        raise DataError("invalid", "a strategy declares at most one price series per asset")


def require_price_chains(bindings, chains) -> None:
    """Refuse declared series the deployment cannot read: a chain it has no provider for,
    or a connector without a reviewed deployment there."""
    for binding in bindings or ():
        if binding.chain not in chains:
            raise ValueError(
                f"strategy price series on {binding.chain.chain_id} needs a provider for its "
                "chain; the deployment has none"
            )
        try:
            pool_scope(binding).require_deployment()
        except DataError as error:
            raise ValueError(f"strategy price pool cannot be read: {error}") from None


def resolve_price_pools(source: StrategyPriceSource, bindings) -> None:
    """Resolve each declared pool before the gateway serves: it must exist on its chain
    and hold the declared asset."""
    for binding in bindings or ():
        try:
            reader = source.reader(binding.chain)
            with reader.retained_pin():
                source.pool(binding, DirectReads(reader))
        except (DataError, ValueError) as error:
            raise ValueError(
                f"strategy price pool {binding.source.pool} cannot serve {binding.asset}: {error}"
            ) from None


# Once the window is retained at start, a decision reads only its newest boundaries.
DECISION_READ_SECONDS = 120.0
SERVING_SECONDS = 30.0


def request_budget(bindings) -> float:
    """How long one decision's price request may take with a warmed cache.

    A request that still overruns (a cold window, a slow provider) fails as a hold; the
    gateway keeps what it read, so the next decision finds the window retained.
    """
    return len(bindings) * DECISION_READ_SECONDS + SERVING_SECONDS


def warm_price_series(source: StrategyPriceSource, bindings) -> None:
    """Read and retain each declared window up to its chain's head before serving.

    A failure leaves the window to the first decisions, which hold until it is read.
    """
    for binding in bindings or ():
        started = time.monotonic()
        line: dict[str, object] = {"asset": str(binding.asset)}
        try:
            head = source.reader(binding.chain).pin_block("latest")
            source.read(binding, int(head.timestamp))
        except (DataError, ValueError) as error:
            line = {"event": "price_window_not_retained", **line, "reason": str(error)}
        else:
            line = {"event": "price_window_retained", **line}
        line["seconds"] = round(time.monotonic() - started, 1)
        print(json.dumps(line), file=sys.stderr, flush=True)


def series_json(series: tuple[PriceSeries, ...]) -> list:
    return [json.loads(item.model_dump_json()) for item in series]
