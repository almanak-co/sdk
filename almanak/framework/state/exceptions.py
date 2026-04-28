"""Typed exceptions for the state layer.

Separate module so both ``state_manager`` and ``gateway_state_manager`` can import
without either one pulling in the other's heavy dependencies.
"""

from __future__ import annotations

from enum import StrEnum


class AccountingWriteKind(StrEnum):
    """Which accounting surface a persistence call was targeting.

    Kept as a ``StrEnum`` so existing callers that still pass the raw string
    values (``"ledger"``, ``"snapshot"``, ``"metrics"``) stay compatible;
    new call sites should prefer the enum for type safety per the project's
    model-type guidelines (``blueprints/18-model-type-selection.md``).
    """

    LEDGER = "ledger"
    SNAPSHOT = "snapshot"
    METRICS = "metrics"
    ACCOUNTING = "accounting"
    OUTBOX = "outbox"


class AccountingPersistenceError(Exception):
    """Raised when a mandatory accounting write fails.

    Covers ledger entries, portfolio snapshots, and portfolio metrics. In live
    mode these writes are the durable record of what happened on-chain --
    silently swallowing failures would leave the books out of sync with the
    chain ("silent accounting loss").

    The runner converts this exception into an ``ACCOUNTING_FAILED`` iteration
    status, halts the cycle, and alerts the operator. Paper/dry-run/backtest
    modes may log + continue but MUST still surface the failure as ERROR.

    Attributes:
        write_kind: Which accounting surface failed. Stored as the string
            value; accepts both :class:`AccountingWriteKind` members and raw
            strings so call sites can migrate incrementally.
        strategy_id: Strategy whose write failed (may be empty when unknown).
        cause: Original exception, if any.
    """

    def __init__(
        self,
        write_kind: AccountingWriteKind | str,
        strategy_id: str = "",
        message: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        # Normalise through the enum so invalid raw strings raise immediately
        # (a typoed ``write_kind`` would otherwise silently propagate into
        # error metadata and break pattern-matching downstream).
        if isinstance(write_kind, AccountingWriteKind):
            normalized = write_kind
        else:
            normalized = AccountingWriteKind(write_kind)  # raises ValueError on typo
        self.write_kind: str = normalized.value
        self.strategy_id = strategy_id
        self.cause = cause
        detail = f" strategy={strategy_id}" if strategy_id else ""
        suffix = f": {cause}" if cause is not None else ""
        super().__init__(message or f"Accounting write failed ({self.write_kind}){detail}{suffix}")


__all__ = ["AccountingPersistenceError", "AccountingWriteKind"]
