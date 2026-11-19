"""Durable execution storage and coordinator claims, independent of protocol semantics."""

import fcntl
import hashlib
import json
import os
import sqlite3
import time
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote

from pydantic import Field, model_validator

from almanak.gateway.evm import legacy_account_claim
from almanak.gateway.request_scope import timed
from almanak.models import Block, Model, UInt
from almanak.strategy import StrategyDefinition


class FeeQuote(Model):
    block: Block
    next_base_fee: UInt | None = Field(default=None, exclude_if=lambda v: v is None)
    priority_fee: UInt
    observed_base_fee: UInt | None = Field(default=None, exclude_if=lambda v: v is None)
    # Chains whose fee history carries no tips floor the quote at the node's
    # current gas-price suggestion. That read is not block-addressable, so the
    # admitted value is retained here and revalidation reuses it instead of a
    # moving suggestion.
    node_gas_price: UInt | None = Field(default=None, exclude_if=lambda v: v is None)
    # OP-family transactions pay L1 data and operator fees outside gas * gasPrice.
    # Retaining the admitted bound makes every later candidate and report use the
    # same complete payer exposure without depending on a moving chain head.
    additional_fee_exposure: UInt = Field(default=0, exclude_if=lambda v: not v)

    @model_validator(mode="after")
    def basis(self):
        if (self.next_base_fee is None) == (self.observed_base_fee is None):
            raise ValueError("fee quote requires exactly one explicit base fee observation")
        return self

    @property
    def market_base_fee(self) -> int:
        if self.next_base_fee is not None:
            return self.next_base_fee
        assert self.observed_base_fee is not None
        return self.observed_base_fee


def _configure_connection(connection: sqlite3.Connection) -> None:
    # WAL recovery can return BUSY without invoking SQLite's busy handler.
    # Retry only setup, before any caller transaction or external action exists.
    deadline = time.monotonic() + 5
    while True:
        try:
            connection.execute("PRAGMA synchronous=FULL")
            break
        except sqlite3.OperationalError as error:
            remaining = deadline - time.monotonic()
            if error.sqlite_errorcode & 0xFF != sqlite3.SQLITE_BUSY or remaining <= 0:
                raise
            time.sleep(min(0.01, remaining))
    connection.execute("PRAGMA busy_timeout=5000")


def strategy_authority(config, strategy, permissions, preparation):
    from almanak.runtime import strategy_capabilities

    if preparation is None:
        if getattr(config, "preparation_digest", None) is not None:
            raise ValueError("deployment creation requires its retained preparation")
        capabilities = strategy_capabilities(strategy, config)
    else:
        preparation.require_configuration(config, strategy)
        capabilities = preparation.preparation.prepared.capabilities
    if permissions is not None and (
        permissions.capabilities != capabilities or permissions.source_hash != strategy.sha256
    ):
        raise ValueError("permission generation and deployment capabilities must be identical")
    return capabilities


def operator_capabilities(config, permissions, preparation):
    """Authority of an operator deployment: what the connector declares from the config.

    No author code runs. Prepared configurations and generated Safe permissions both bind
    a strategy today, so an operator deployment with either is refused, not approximated.
    """
    from almanak.connectors.composition import connector_for
    from almanak.operator_deployment import supports_operator

    connector = connector_for(config)
    if not supports_operator(connector.codec.checkpoint):
        raise ValueError("this connector does not support operator deployments")
    if preparation is not None or getattr(config, "preparation_digest", None) is not None:
        raise ValueError("operator deployments do not support prepared configurations")
    if permissions is not None:
        raise ValueError("operator deployments do not support generated Safe permissions")
    return connector.codec.capability.declared(config)


class Journal:
    def __init__(self, path: Path):
        self.path = path.resolve()

    @contextmanager
    def connect(self):
        connection = sqlite3.connect(f"file:{quote(str(self.path))}?mode=rw", uri=True, timeout=0)
        try:
            _configure_connection(connection)
            with connection:
                yield connection
        finally:
            connection.close()

    def create(
        self,
        config: Any,
        strategy: StrategyDefinition | None,
        permissions=None,
        *,
        preparation=None,
    ) -> None:
        """Create the deployment; no strategy means an operator deployment."""
        if strategy is None:
            capabilities = operator_capabilities(config, permissions, preparation)
        else:
            capabilities = strategy_authority(config, strategy, permissions, preparation)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
        from almanak.connectors.composition import connector_for

        initial = connector_for(config).lifecycle.initial(
            config, strategy, capabilities, permissions
        )
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                "CREATE TABLE checkpoint (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL)"
            )
            connection.execute("INSERT INTO checkpoint VALUES (1, ?)", (initial.model_dump_json(),))
            connection.execute(
                "CREATE TABLE coordinator (id INTEGER PRIMARY KEY CHECK(id=1), "
                "identity TEXT, expires REAL NOT NULL)"
            )
            connection.execute("INSERT INTO coordinator VALUES (1, NULL, 0)")
            if preparation is not None:
                connection.execute(
                    "CREATE TABLE configuration_preparation "
                    "(id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO configuration_preparation VALUES (1, ?)",
                    (preparation.model_dump_json(),),
                )
        self._sync_directory()

    def configuration_preparation(self):
        from almanak.configuration.admission import PreparedDeployment

        with self.connect() as connection:
            present = connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='table' AND name='configuration_preparation'"
            ).fetchone()
            if present is None:
                return None
            row = connection.execute(
                "SELECT body FROM configuration_preparation WHERE id=1"
            ).fetchone()
            if row is None:
                raise ValueError("durable configuration preparation is missing")
            return PreparedDeployment.model_validate_json(row[0])

    def _sync_directory(self):
        descriptor = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def load(self) -> Any:
        return decode_checkpoint(self.body())

    def body(self) -> str:
        """The stored checkpoint JSON, not decoded."""
        with self.connect() as connection:
            row = connection.execute("SELECT body FROM checkpoint WHERE id=1").fetchone()
            if row is None:
                raise RuntimeError("checkpoint is missing; refusing an empty resume")
            return row[0]

    def audit(self) -> dict:
        """Read legacy bytes as historical evidence without granting modern authority."""
        with self.connect() as connection:
            row = connection.execute("SELECT body FROM checkpoint WHERE id=1").fetchone()
            if row is None:
                raise RuntimeError("checkpoint is missing")
            payload = json.loads(row[0])
        from almanak.connectors.composition import legacy_checkpoint_info

        legacy = legacy_checkpoint_info(payload)
        if legacy is not None:
            return legacy
        from almanak.runtime import report

        return report(decode_checkpoint(row[0]))

    def update(self, change: Callable[[Any], Any]) -> Any:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT body FROM checkpoint WHERE id=1").fetchone()
            if row is None:
                raise RuntimeError("checkpoint is missing")
            state = decode_checkpoint(row[0])
            changed = change(state)
            payload = changed.model_dump_json()
            decode_checkpoint(payload)
            connection.execute("UPDATE checkpoint SET body=? WHERE id=1", (payload,))
        return changed

    def teardown(self) -> Any:
        return self.update(
            lambda s: s if s.mode == "closed" else s.model_copy(update={"mode": "teardown"})
        )

    def acquire(self, identity: str, duration: float):
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current, expires = connection.execute(
                "SELECT identity, expires FROM coordinator WHERE id=1"
            ).fetchone()
            if current is not None and current != identity and expires > time.time():
                raise RuntimeError("another coordinator holds the deployment lease")
            connection.execute(
                "UPDATE coordinator SET identity=?, expires=? WHERE id=1",
                (identity, time.time() + duration),
            )

    def release(self, identity: str):
        with self.connect() as connection:
            connection.execute(
                "UPDATE coordinator SET identity=NULL, expires=0 WHERE id=1 AND identity=?",
                (identity,),
            )

    @contextmanager
    def authorized(self, identity: str, duration: float):
        """Hold the write lock, with a current lease, for one short fenced commit.

        The same durable lock fences lease changes, checkpoint commits and submission, so
        a superseded coordinator cannot commit or submit after another acquires. Callers
        keep provider and signer calls outside it, re-reading the checkpoint body here to
        detect changes since their `read_fenced`. A gateway killed after broadcast leaves
        the preexisting pending bytes intact.
        """
        connection = sqlite3.connect(f"file:{quote(str(self.path))}?mode=rw", uri=True, timeout=0)
        try:
            _configure_connection(connection)
            with timed("lock_wait"):
                connection.execute("BEGIN IMMEDIATE")
            with timed("lock_held"), connection:
                require_lease(connection, identity)
                yield connection, checkpoint_body(connection)
                connection.execute(
                    "UPDATE coordinator SET expires=? WHERE id=1", (time.time() + duration,)
                )
        finally:
            connection.close()

    def read_fenced(self, identity: str) -> str:
        """The checkpoint body under a current lease, read without taking the write lock."""
        with self.connect() as connection:
            # One WAL read snapshot covers both rows and never waits for a writer.
            connection.execute("BEGIN")
            require_lease(connection, identity)
            return checkpoint_body(connection)

    def renew(self, identity: str, duration: float) -> None:
        with self.authorized(identity, duration):
            pass


def require_lease(connection, identity: str) -> None:
    row = connection.execute("SELECT identity, expires FROM coordinator WHERE id=1").fetchone()
    if row is None or row[0] != identity or row[1] <= time.time():
        raise RuntimeError("coordinator lease is absent, expired or superseded")


def checkpoint_body(connection) -> str:
    row = connection.execute("SELECT body FROM checkpoint WHERE id=1").fetchone()
    if row is None:
        raise RuntimeError("checkpoint is missing")
    return row[0]


class Lease:
    """Persistent local fund claims plus process locks; no cross-host guarantee."""

    def __init__(self, journal: Journal, state: Any):
        self.files = []
        root = claims_root()
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        claims = ["database:" + str(journal.path)]
        claims.extend(
            state.config.accounts.binding(a.account).profile.fund.model_dump_json()
            for a in state.config.allocations
        )
        claims.extend(
            binding.profile.submitter.model_dump_json()
            for binding in state.config.accounts.bindings
        )
        for allocation in state.config.allocations:
            fund = state.config.accounts.binding(allocation.account).profile.fund
            legacy = legacy_account_claim(fund)
            if legacy is not None:
                claims.append(legacy)
        owner = json.dumps(
            {"deployment_id": state.deployment_id, "database": str(journal.path)}, sort_keys=True
        )
        try:
            for claim in sorted(set(claims)):
                path = root / (hashlib.sha256(claim.encode()).hexdigest() + ".claim")
                descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
                handle = os.fdopen(descriptor, "r+")
                self.files.append(handle)
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                previous = handle.read()
                if previous and previous != owner:
                    raise RuntimeError("fund is already claimed by another deployment/database")
                if not previous:
                    handle.write(owner)
                    handle.flush()
                    os.fsync(handle.fileno())
            descriptor = os.open(root, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except BaseException:
            self.close()
            raise

    def close(self):
        for handle in self.files:
            handle.close()
        self.files.clear()


def claims_root() -> Path:
    return Path.home() / ".local/state/almanak-v3/claims"


def claimed_by(database: Path) -> bool:
    """Whether any fund claim still names this journal as its owner."""
    root = claims_root()
    if not root.is_dir():
        return False
    for path in root.glob("*.claim"):
        try:
            owner = json.loads(path.read_text() or "{}")
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            continue
        if isinstance(owner, dict) and owner.get("database") == str(database):
            return True
    return False


def release_claims(database: Path) -> int:
    """Remove the fund claims a stopped deployment's journal still owns.

    A claim names its owning journal. Only claims of this journal are removed, and only
    while no process holds their lock, so a live coordinator keeps its claims.
    """
    released = 0
    root = claims_root()
    if not root.is_dir():
        return 0
    for path in root.glob("*.claim"):
        try:
            with path.open("r") as handle:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    continue
                try:
                    owner = json.loads(handle.read() or "{}")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if isinstance(owner, dict) and owner.get("database") == str(database):
                    path.unlink(missing_ok=True)
                    released += 1
        except FileNotFoundError:
            # Another process released it between the listing and the open.
            continue
    return released


def decode_checkpoint(value):
    from almanak.connectors.composition import checkpoint_from_json

    return checkpoint_from_json(value)
