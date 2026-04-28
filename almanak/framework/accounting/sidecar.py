"""AccountingSidecarWriter — best-effort per-strategy .jsonl sidecar file.

After each successful intent execution the runner appends one JSON line to
``~/.almanak/accounting/<strategy_id>.jsonl``.  The dashboard (separate repo)
tails this file to gain real-time visibility into SDK execution data without
touching ``gateway.db`` or ``almanak_state.db``.

Design constraints
------------------
- **Best-effort**: sidecar failures must never crash the live strategy loop.
  All I/O is wrapped in try/except; errors are logged at WARNING and swallowed.
- **Atomic enough**: each call writes exactly one complete JSON line followed by
  a newline before returning.  Mode ``'a'`` guarantees sequential appends even
  when two strategies run concurrently (each writes to its own file, so no
  cross-strategy contention).
- **No external dependencies**: stdlib only (``json``, ``pathlib``, ``datetime``).

Line schema (VIB-3454)
-----------------------
All monetary/amount fields are decimal strings (never float) so consumers can
parse them with ``Decimal(value)`` without precision loss.  Fields that are not
applicable to an intent type are ``null``.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _sidecar_dir() -> Path:
    """Return the directory used to store per-strategy sidecar files.

    Resolution order:

    1. ``ALMANAK_ACCOUNTING_DIR`` env var — explicit override, useful in containers.
    2. ``~/.almanak/accounting`` — default for interactive / local use.
    3. ``<tempdir>/.almanak/accounting`` — fallback when HOME is unset (``/``), as
       happens in some container runtimes; uses :func:`tempfile.gettempdir` rather
       than a hardcoded ``/tmp`` so the OS-appropriate temp directory is used.

    Callers must still call ``path.parent.mkdir(parents=True, exist_ok=True)`` before
    writing; this function only returns the *intended* directory path.
    """
    # Explicit override wins (useful in containers where HOME may be /).
    override = os.environ.get("ALMANAK_ACCOUNTING_DIR")
    if override:
        return Path(override)
    home = Path.home()
    # Path.home() returns "/" when no HOME is set — not a writable user dir.
    if home == Path("/"):
        return Path(tempfile.gettempdir()) / ".almanak" / "accounting"
    return home / ".almanak" / "accounting"


def _sidecar_path(strategy_id: str) -> Path:
    """Return the absolute path for *strategy_id*'s sidecar file."""
    return _sidecar_dir() / f"{strategy_id}.jsonl"


def _or_none(value: Any) -> str | None:
    """Return ``str(value)`` when *value* is truthy, else ``None``."""
    if value is None:
        return None
    s = str(value)
    return s if s else None


class AccountingSidecarWriter:
    """Appends one JSON line per successful execution to the strategy sidecar.

    Usage (strategy_runner.py, after ``_write_ledger_entry`` on success)::

        from almanak.framework.accounting.sidecar import AccountingSidecarWriter

        AccountingSidecarWriter().append(
            strategy_id=strategy.strategy_id,
            intent=intent,
            result=result,
            chain=chain,
        )

    All failures are caught, logged at WARNING, and swallowed — the call is
    unconditionally safe to make from the hot execution path.
    """

    def append(
        self,
        *,
        strategy_id: str,
        intent: Any,
        result: Any,
        chain: str,
    ) -> None:
        """Append one accounting line for a successful intent execution.

        Parameters
        ----------
        strategy_id:
            The owning strategy's ID; used to derive the file path.
        intent:
            The executed intent object (duck-typed; see field extraction below).
        result:
            The execution result object returned by the orchestrator.
        chain:
            Chain name (e.g. ``"arbitrum"``).
        """
        try:
            line = self._build_line(
                strategy_id=strategy_id,
                intent=intent,
                result=result,
                chain=chain,
            )
            path = _sidecar_path(strategy_id)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(line, default=str) + "\n")
        except Exception:  # noqa: BLE001
            logger.warning(
                "AccountingSidecarWriter: failed to write sidecar for strategy=%s",
                strategy_id,
                exc_info=True,
            )

    # ------------------------------------------------------------------
    # Field extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_line(
        *,
        strategy_id: str,
        intent: Any,
        result: Any,
        chain: str,
    ) -> dict[str, str | None]:
        """Extract all sidecar fields from *intent* and *result*."""
        # --- intent_type ---
        it = getattr(intent, "intent_type", None)
        intent_type: str | None = None
        if it is not None:
            intent_type = it.value if hasattr(it, "value") else str(it)

        # --- protocol ---
        protocol = _or_none(getattr(intent, "protocol", None))

        # --- position_id ---
        raw_pos_id = getattr(result, "position_id", None) if result else None
        position_id = _or_none(raw_pos_id)

        # --- tx_hash ---
        tx_hash: str | None = None
        if result:
            tx_results = getattr(result, "transaction_results", None)
            if tx_results and hasattr(tx_results, "__getitem__"):
                tx_hash = _or_none(getattr(tx_results[0], "tx_hash", None))

        # --- gas_usd ---
        gas_usd: str | None = None
        if result:
            gas_cost = getattr(result, "gas_cost_usd", None)
            gas_usd = _or_none(gas_cost)

        # --- token_in / amount_in / token_out / amount_out ---
        token_in: str | None = None
        amount_in: str | None = None
        token_out: str | None = None
        amount_out: str | None = None

        swap_amounts = getattr(result, "swap_amounts", None) if result else None
        if swap_amounts:
            token_in = _or_none(swap_amounts.token_in or getattr(intent, "from_token", None))
            token_out = _or_none(swap_amounts.token_out or getattr(intent, "to_token", None))
            amt_in = getattr(swap_amounts, "amount_in_decimal", None)
            amt_out = getattr(swap_amounts, "amount_out_decimal", None)
            amt_in_resolved = getattr(swap_amounts, "amount_in_decimal_resolved", True)
            amt_out_resolved = getattr(swap_amounts, "amount_out_decimal_resolved", True)
            amount_in = str(amt_in) if amt_in is not None and amt_in_resolved else None
            amount_out = str(amt_out) if amt_out is not None and amt_out_resolved else None
        else:
            # Lending / LP / generic intent fallback
            token_in = _or_none(
                getattr(intent, "from_token", None)
                or getattr(intent, "supply_token", None)
                or getattr(intent, "borrow_token", None)
                or getattr(intent, "token", None)
            )
            token_out = _or_none(getattr(intent, "to_token", None))
            amt = next(
                (
                    v
                    for v in [
                        getattr(intent, "amount", None),
                        getattr(intent, "supply_amount", None),
                        getattr(intent, "borrow_amount", None),
                        getattr(intent, "amount_usd", None),
                    ]
                    if v is not None
                ),
                None,
            )
            amount_in = str(amt) if amt is not None else None

        # --- cost_basis_usd: not yet computed at runner level; always null ---
        cost_basis_usd: str | None = None

        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "intent_type": intent_type,
            "position_id": position_id,
            "tx_hash": tx_hash,
            "cost_basis_usd": cost_basis_usd,
            "token_in": token_in,
            "amount_in": amount_in,
            "token_out": token_out,
            "amount_out": amount_out,
            "gas_usd": gas_usd,
            "chain": chain,
            "protocol": protocol,
            "strategy_id": strategy_id,
        }
