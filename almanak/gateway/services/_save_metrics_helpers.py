"""Phase helpers for ``StateService.SavePortfolioMetrics`` (Phase 8.3b).

This module decomposes the large ``SavePortfolioMetrics`` RPC body into
focused, testable phases:

1. :func:`parse_metrics_inputs` — validates ``strategy_id``, the 4 currency
   fields, and ``initial_timestamp``; raises :class:`MetricsValidationError`
   on bad input and returns a typed :class:`ParsedMetricsInputs` otherwise.
2. :func:`build_pg_upsert_args` — builds the positional argument tuple for
   the PostgreSQL UPSERT query, exactly preserving the order the RPC
   previously passed them in.
3. :func:`resolve_total_value_usd` — best-effort lookup of the latest
   snapshot's ``total_value_usd`` for the SQLite write path; swallows any
   backend exception with a warning log.
4. :func:`build_portfolio_metrics` — constructs the ``PortfolioMetrics``
   dataclass that the warm backend ``save_portfolio_metrics`` consumes.

The RPC itself orchestrates these helpers and owns the gRPC ``set_code`` /
``set_details`` / response proto construction — none of that boilerplate
leaks into this module.

All error wording and ``grpc.StatusCode`` values are preserved byte-for-byte
against the pre-refactor behaviour (downstream observability may grep them).
Characterization coverage lives in
``tests/gateway/test_save_portfolio_metrics_characterization.py``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from almanak.gateway.proto import gateway_pb2

if TYPE_CHECKING:
    from almanak.framework.portfolio.models import PortfolioMetrics

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------


class MetricsValidationError(Exception):
    """Raised when ``SaveMetricsRequest`` inputs are malformed.

    ``message`` is the single human-readable string used for BOTH the proto
    ``error`` field AND ``context.set_details``. Error-path wording is part
    of the RPC contract (downstream observability greps it), so the helper
    and the RPC must agree on the exact string.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


# ---------------------------------------------------------------------------
# Input parsing
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ParsedMetricsInputs:
    """Validated / coerced inputs for SavePortfolioMetrics.

    All decimal-typed fields are converted from request strings via
    ``Decimal(...)`` with the ``"0"`` fallback the proto contract documents;
    ``timestamp`` is tz-aware UTC.
    """

    strategy_id: str  # post-validate_strategy_id (pre-resolve_agent_id)
    initial_value_usd: Decimal
    deposits_usd: Decimal
    withdrawals_usd: Decimal
    gas_spent_usd: Decimal
    timestamp: datetime


def parse_metrics_inputs(
    request: gateway_pb2.SaveMetricsRequest,
    strategy_id: str,
) -> ParsedMetricsInputs:
    """Parse + validate request fields into a typed bundle.

    Args:
        request: The incoming proto request. Only its primitive fields are
            read (decimals, ``initial_timestamp``, ``strategy_id``).
        strategy_id: The already-validated, agent-id-resolved strategy_id.
            Passed in rather than re-derived here because ``validate_strategy_id``
            and ``resolve_agent_id`` live in the validation module and already
            handle their own error-path conversion to ``ValidationError`` in
            the RPC.

    Raises:
        MetricsValidationError: with message matching the pre-refactor
            wording for malformed decimals, negative timestamps, and
            out-of-range timestamps.
    """
    try:
        initial_value_usd = Decimal(request.initial_value_usd or "0")
        deposits_usd = Decimal(request.deposits_usd or "0")
        withdrawals_usd = Decimal(request.withdrawals_usd or "0")
        gas_spent_usd = Decimal(request.gas_spent_usd or "0")
    except InvalidOperation as exc:
        raise MetricsValidationError("metrics fields must be valid decimal strings") from exc

    if request.initial_timestamp < 0:
        raise MetricsValidationError("initial_timestamp must be non-negative")

    try:
        timestamp = (
            datetime.fromtimestamp(request.initial_timestamp, tz=UTC)
            if request.initial_timestamp
            else datetime.now(UTC)
        )
    except (OverflowError, OSError, ValueError) as exc:
        raise MetricsValidationError("initial_timestamp is out of range") from exc

    return ParsedMetricsInputs(
        strategy_id=strategy_id,
        initial_value_usd=initial_value_usd,
        deposits_usd=deposits_usd,
        withdrawals_usd=withdrawals_usd,
        gas_spent_usd=gas_spent_usd,
        timestamp=timestamp,
    )


# ---------------------------------------------------------------------------
# PostgreSQL argument packing
# ---------------------------------------------------------------------------


# Kept as a module-level constant so the RPC body stays short. The SQL itself
# is unchanged from the pre-refactor version.
PG_UPSERT_QUERY = """
                    INSERT INTO portfolio_metrics (
                        agent_id, initial_value_usd, initial_timestamp,
                        deposits_usd, withdrawals_usd, gas_spent_usd,
                        deployment_id, cycle_id, execution_mode, is_complete,
                        updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (agent_id) DO UPDATE SET
                        initial_value_usd = EXCLUDED.initial_value_usd,
                        initial_timestamp = EXCLUDED.initial_timestamp,
                        deposits_usd = EXCLUDED.deposits_usd,
                        withdrawals_usd = EXCLUDED.withdrawals_usd,
                        gas_spent_usd = EXCLUDED.gas_spent_usd,
                        deployment_id = EXCLUDED.deployment_id,
                        cycle_id = EXCLUDED.cycle_id,
                        execution_mode = EXCLUDED.execution_mode,
                        is_complete = EXCLUDED.is_complete,
                        updated_at = EXCLUDED.updated_at
                    RETURNING agent_id
                    """


def build_pg_upsert_args(
    inputs: ParsedMetricsInputs,
    request: gateway_pb2.SaveMetricsRequest,
    now: datetime,
) -> tuple[Any, ...]:
    """Build the positional args tuple for the portfolio_metrics UPSERT.

    Preserves the exact order the pre-refactor RPC passed them:
    ``(agent_id, initial_value_usd, initial_timestamp, deposits, withdrawals,
    gas_spent, deployment_id, cycle_id, execution_mode, is_complete,
    updated_at)``. Do NOT reorder — ``$1..$11`` placeholders depend on it.
    """
    return (
        inputs.strategy_id,
        str(inputs.initial_value_usd),
        inputs.timestamp,
        str(inputs.deposits_usd),
        str(inputs.withdrawals_usd),
        str(inputs.gas_spent_usd),
        request.deployment_id or "",
        request.cycle_id or "",
        request.execution_mode or "",
        request.is_complete,
        now,
    )


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------


async def resolve_total_value_usd(warm_backend: Any, strategy_id: str) -> Decimal:
    """Best-effort lookup of the latest snapshot's ``total_value_usd``.

    VIB-2765: the proto does NOT carry ``total_value_usd`` (it is derived
    from the most recent snapshot that was saved moments before this RPC).
    A broken or missing snapshot backend must NOT abort the metrics write —
    errors are logged and ``Decimal("0")`` is returned.

    Args:
        warm_backend: ``StateManager.warm_backend`` — may be ``None`` or may
            lack ``get_latest_snapshot`` (the ``hasattr`` guard accommodates
            older warm backends that only implement ``save_portfolio_metrics``).
        strategy_id: The (already agent-id-resolved) strategy id.

    Returns:
        The latest snapshot's ``total_value_usd`` or ``Decimal("0")`` if
        unavailable.
    """
    total_value_usd = Decimal("0")
    try:
        if warm_backend and hasattr(warm_backend, "get_latest_snapshot"):
            latest = await warm_backend.get_latest_snapshot(strategy_id)
            if latest is not None:
                total_value_usd = latest.total_value_usd
    except Exception as snap_err:  # noqa: BLE001 — must not abort the write
        logger.warning(
            "Could not resolve total_value_usd from snapshot for %s: %s",
            strategy_id,
            snap_err,
        )
    return total_value_usd


def build_portfolio_metrics(
    inputs: ParsedMetricsInputs,
    request: gateway_pb2.SaveMetricsRequest,
    total_value_usd: Decimal,
) -> PortfolioMetrics:
    """Build a ``PortfolioMetrics`` for the warm backend save path.

    Pins the pre-refactor field mapping:
    - ``cycle_id = request.cycle_id or None`` (empty string -> ``None``)
    - ``deployment_id`` / ``execution_mode`` fall back to ``""``.
    - Phase 4 accounting identity fields (VIB-2835/2837/2839).

    Local import for ``PortfolioMetrics`` mirrors the RPC's lazy import so
    the helper module stays importable in contexts that don't need the full
    framework.portfolio.models surface.
    """
    from almanak.framework.portfolio.models import PortfolioMetrics

    return PortfolioMetrics(
        strategy_id=inputs.strategy_id,
        timestamp=inputs.timestamp,
        total_value_usd=total_value_usd,
        initial_value_usd=inputs.initial_value_usd,
        deposits_usd=inputs.deposits_usd,
        withdrawals_usd=inputs.withdrawals_usd,
        gas_spent_usd=inputs.gas_spent_usd,
        deployment_id=request.deployment_id or "",
        cycle_id=request.cycle_id or None,
        execution_mode=request.execution_mode or "",
        is_complete=request.is_complete,
    )
