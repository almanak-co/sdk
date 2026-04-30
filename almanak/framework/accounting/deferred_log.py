"""Deferred-write log — VIB-3773, plan §4.4 / Phase 1.

Best-effort durable backstop for accounting writes that *failed* during a
teardown. The teardown lane diverges from VIB-3762's halt-on-write-failure
contract because halting mid-unwind would strand a partially-closed
position; instead, the runner records every failed write here so an operator
(or a future ``almanak ax accounting reconcile`` CLI) can replay it later.

Never raises. The log is best-effort: if both the JSONL file and the
structured log path fail, the record is dropped to stderr and execution
continues. The on-chain TX has already happened — there is no decision to
take here other than "keep moving and surface the gap loudly."

Targets
-------
* **Local** (``AGENT_ID`` unset): append a JSON line to
  ``<local_db_dir>/accounting_deferred.jsonl``. Atomic per write because the
  file is opened with ``O_APPEND`` and each line is a single ``write``
  call sized well below ``PIPE_BUF``.
* **Hosted** (``AGENT_ID`` set): emit a stdlib log record at WARNING with a
  stable ``event="accounting_deferred"`` field. Almanak Infra's log
  pipeline already provides durability + queryability; no Postgres DDL is
  added (CLAUDE.md: "metrics_db schema is owned outside this repo").

Schema (kept tiny on purpose — operators reconcile via this + outbox tail):
    {
        "ts":           ISO-8601 UTC,
        "kind":         "ledger" | "outbox" | "enrich" | "sidecar"
                        | "snapshot" | "metrics" | "position_event",
        "strategy_id":  ...,
        "deployment_id": ...,
        "cycle_id":     "teardown-<uuid>",
        "intent_type":  e.g. "LP_CLOSE" | "SWAP" | "REPAY" | None,
        "tx_hash":      "0x..." | None,
        "ledger_entry_id": "..." | None,
        "error":        str (truncated to 1000 chars),
        "extra":        dict | None  (small structured metadata)
    }
"""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFERRED_LOG_FILENAME = "accounting_deferred.jsonl"

# Single-line writes below this size are atomic on POSIX with O_APPEND.
# Real records run ~300–800 bytes; the cap is paranoia.
_MAX_RECORD_BYTES = 4000

# Truncate any free-text error to bound record size and avoid leaking large
# tracebacks into a log file an operator may scan with `cat`.
_ERROR_TRUNCATE = 1000


@dataclass(frozen=True)
class DeferredWrite:
    """A single failed accounting write, recorded for later reconciliation.

    Field order is stable; avoid breaking it (operator scripts may rely on
    column position when rendering JSONL with ``jq -c``).
    """

    ts: str
    kind: str
    strategy_id: str
    deployment_id: str
    cycle_id: str
    intent_type: str | None = None
    tx_hash: str | None = None
    ledger_entry_id: str | None = None
    error: str = ""
    extra: dict[str, Any] | None = None

    @staticmethod
    def now(
        *,
        kind: str,
        strategy_id: str,
        deployment_id: str,
        cycle_id: str,
        intent_type: str | None = None,
        tx_hash: str | None = None,
        ledger_entry_id: str | None = None,
        error: str = "",
        extra: dict[str, Any] | None = None,
    ) -> DeferredWrite:
        """Construct a record stamped with the current UTC time."""
        return DeferredWrite(
            ts=datetime.now(UTC).isoformat(),
            kind=kind,
            strategy_id=strategy_id,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            intent_type=intent_type,
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
            error=(error or "")[:_ERROR_TRUNCATE],
            extra=_sanitize_extra(extra),
        )

    def to_json_line(self) -> str:
        """One self-contained JSON object terminated by ``\\n``."""
        # ``default=str`` keeps the call graceful for Decimal / datetime /
        # other non-JSON-native types that may slip into ``extra``.
        line = json.dumps(asdict(self), default=str, separators=(",", ":"))
        if len(line) + 1 > _MAX_RECORD_BYTES:
            # Defensive: shrink ``extra`` and re-encode once. Better to drop
            # the extras than to risk a torn write.
            stripped = DeferredWrite(
                ts=self.ts,
                kind=self.kind,
                strategy_id=self.strategy_id,
                deployment_id=self.deployment_id,
                cycle_id=self.cycle_id,
                intent_type=self.intent_type,
                tx_hash=self.tx_hash,
                ledger_entry_id=self.ledger_entry_id,
                error=self.error,
                extra={"_truncated": True} if self.extra else None,
            )
            line = json.dumps(asdict(stripped), default=str, separators=(",", ":"))
        return line + "\n"


def _sanitize_extra(extra: dict[str, Any] | None) -> dict[str, Any] | None:
    """Strip obviously-secret keys from ``extra``.

    The deferred log is operator-readable; we never want a private key or
    auth token to land in it via a careless caller. Belt-and-braces: every
    upstream caller is expected to pass intent / receipt metadata, not
    credentials, but a single grep guard here is cheap insurance.
    """
    if not extra:
        return None
    forbidden = ("private_key", "secret", "auth_token", "api_key", "password")
    cleaned: dict[str, Any] = {}
    for k, v in extra.items():
        kl = k.lower()
        if any(token in kl for token in forbidden):
            cleaned[k] = "[REDACTED]"
        else:
            cleaned[k] = v
    return cleaned


def _resolve_local_log_path() -> Path | None:
    """Resolve the local JSONL log path. Returns ``None`` in hosted mode or
    when the local resolver is unavailable.
    """
    try:
        from almanak.framework.local_paths import LocalPathError, local_db_path
    except ImportError:  # pragma: no cover — defensive
        return None
    try:
        db = local_db_path()
    except LocalPathError:
        return None  # hosted mode
    return db.parent / DEFERRED_LOG_FILENAME


def _append_local(path: Path, line: str) -> bool:
    """Atomically append a single line to ``path``. Returns True on success.

    Uses ``O_APPEND`` so concurrent writers from the same machine don't
    interleave. ``flush`` + ``fsync`` are best-effort; we'd rather lose a
    record than block the teardown loop on a slow disk, so we never raise.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Single write call ensures atomicity for line-sized records.
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                # fsync may fail on tmpfs / Docker overlay; the record is
                # still in the page cache and will land on disk shortly.
                pass
        return True
    except OSError as exc:
        logger.warning(
            "DeferredWriteLog: local append to %s failed (%s) — falling back to structured log",
            path,
            exc,
        )
        return False


def _emit_hosted(record: DeferredWrite) -> None:
    """Hosted-mode target: structured log line. Stable ``event`` field for
    the platform's log pipeline to grep on.
    """
    logger.warning(
        "accounting_deferred: kind=%s strategy=%s cycle=%s intent_type=%s tx=%s err=%s",
        record.kind,
        record.strategy_id,
        record.cycle_id,
        record.intent_type or "-",
        record.tx_hash or "-",
        record.error or "-",
        extra={
            "event": "accounting_deferred",
            "deferred_kind": record.kind,
            "strategy_id": record.strategy_id,
            "deployment_id": record.deployment_id,
            "cycle_id": record.cycle_id,
            "intent_type": record.intent_type,
            "tx_hash": record.tx_hash,
            "ledger_entry_id": record.ledger_entry_id,
            "ts_iso": record.ts,
        },
    )


def append(record: DeferredWrite) -> bool:
    """Persist one ``DeferredWrite``. Never raises.

    Returns
    -------
    bool
        ``True`` if the record reached the local JSONL file (in local mode)
        or was emitted as a hosted structured log event. ``False`` only if
        every target failed — in which case we still print the record to
        stderr so it isn't completely lost.
    """
    line = ""
    try:
        line = record.to_json_line()
    except Exception as exc:  # noqa: BLE001 — JSON encoding must never crash teardown
        logger.warning("DeferredWriteLog: serialize failed (%s) — record dropped", exc)
        # Stderr is the last-resort sink. Avoid raising.
        try:
            sys.stderr.write(
                f"accounting_deferred: SERIALIZE_FAILED kind={record.kind} "
                f"strategy={record.strategy_id} cycle={record.cycle_id} err={exc}\n"
            )
        except Exception:  # pragma: no cover — stderr should not fail
            pass
        return False

    path = _resolve_local_log_path()
    if path is not None:
        if _append_local(path, line):
            return True
        # Local target unavailable — fall through to structured log.

    # Hosted mode (or local-target failure): emit structured log.
    try:
        _emit_hosted(record)
        return True
    except Exception as exc:  # noqa: BLE001 — never raise from this module
        logger.warning("DeferredWriteLog: hosted emit failed (%s)", exc)

    # Last resort: stderr. Operators tail the strategy log; this preserves
    # the failure so it isn't entirely silent.
    try:
        sys.stderr.write("accounting_deferred: " + line)
    except Exception:  # pragma: no cover
        pass
    return False


def append_now(
    *,
    kind: str,
    strategy_id: str,
    deployment_id: str,
    cycle_id: str,
    intent_type: str | None = None,
    tx_hash: str | None = None,
    ledger_entry_id: str | None = None,
    error: str = "",
    extra: dict[str, Any] | None = None,
) -> bool:
    """Convenience helper: build a ``DeferredWrite`` stamped with ``now()``
    and append it. Same return contract as :func:`append`.
    """
    return append(
        DeferredWrite.now(
            kind=kind,
            strategy_id=strategy_id,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            intent_type=intent_type,
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
            error=error,
            extra=extra,
        )
    )


# Re-exported for ``from almanak.framework.accounting.deferred_log import *``
# convenience but mostly to nail the public API down.
__all__ = [
    "DEFERRED_LOG_FILENAME",
    "DeferredWrite",
    "append",
    "append_now",
]
