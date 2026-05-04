"""Characterization tests for ``StateService.SavePortfolioMetrics`` (Phase 8.3b).

These tests pin the current observable behaviour of the RPC BEFORE the phase-
helper extraction that follows. They document the full validation / persistence
matrix so that the refactor must preserve every branch byte-for-byte:

- Strategy ID validation (missing / whitespace / invalid characters).
- Decimal parsing of the 4 currency fields (``initial_value_usd``, ``deposits_usd``,
  ``withdrawals_usd``, ``gas_spent_usd``) and rejection of malformed strings.
- ``initial_timestamp`` handling: negative (rejected), zero (defaults to
  ``datetime.now(UTC)``), valid epoch-seconds, and out-of-range values that trip
  ``datetime.fromtimestamp``.
- Persistence branch selection via ``_snapshot_pool``:
    * PostgreSQL mode: success (UPSERT INTO portfolio_metrics) and exception
      handler (``INTERNAL`` + ``internal server error`` details).
    * SQLite mode: resolution of ``total_value_usd`` from latest snapshot,
      warm backend dispatch, ``result=True`` / ``result=False`` / no-warm-
      backend / no-``save_portfolio_metrics`` fallbacks, and the exception
      handler.
- Happy-path response shape (``success=True``, no error fields set, no
  ``set_code``).
- Error response shape (``success=False``, matching ``error`` string, and the
  exact ``grpc.StatusCode`` + ``set_details`` wording downstream observability
  may grep).

All tests use the shared harness in ``tests/gateway/grpc_harness.py``
(merged in PR #1807); do NOT duplicate the mock-context builder or error
assertion helper here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.framework.portfolio.models import PortfolioMetrics
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer
from tests.gateway.grpc_harness import (
    assert_grpc_error,
    assert_set_code_not_called,
    make_grpc_context,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def service() -> StateServiceServicer:
    """Fresh StateService with no-op initialisation.

    The service starts with ``_initialized = True`` so ``_ensure_initialized``
    is skipped, and ``_ensure_snapshot_pool`` is patched to a no-op so each
    test controls the ``_snapshot_pool`` attribute directly (None => SQLite
    branch, a MagicMock => PostgreSQL branch).
    """
    svc = StateServiceServicer(GatewaySettings())
    svc._initialized = True
    svc._snapshot_pool_initialized = True
    svc._ensure_initialized = AsyncMock()
    svc._ensure_snapshot_pool = AsyncMock()
    return svc


@pytest.fixture
def context() -> MagicMock:
    """Shared gRPC mock context from the harness."""
    return make_grpc_context()


def _make_request(
    *,
    strategy_id: str = "strat-1",
    initial_value_usd: str = "10000",
    initial_timestamp: int = 1712000000,
    deposits_usd: str = "500",
    withdrawals_usd: str = "100",
    gas_spent_usd: str = "25",
    deployment_id: str = "",
    cycle_id: str = "",
    execution_mode: str = "",
    is_complete: bool = False,
) -> gateway_pb2.SaveMetricsRequest:
    """Build a SaveMetricsRequest with sensible defaults."""
    return gateway_pb2.SaveMetricsRequest(
        strategy_id=strategy_id,
        initial_value_usd=initial_value_usd,
        initial_timestamp=initial_timestamp,
        deposits_usd=deposits_usd,
        withdrawals_usd=withdrawals_usd,
        gas_spent_usd=gas_spent_usd,
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        is_complete=is_complete,
    )


def _install_warm_backend(service: StateServiceServicer, warm: MagicMock | None) -> None:
    """Attach a warm backend (or None) via a mocked StateManager."""
    mock_sm = MagicMock()
    mock_sm.warm_backend = warm
    service._state_manager = mock_sm


# ---------------------------------------------------------------------------
# 1. Strategy ID validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("strategy_id", ["", "   "])
@pytest.mark.asyncio
async def test_missing_strategy_id_returns_invalid_argument(service, context, strategy_id):
    """Empty / whitespace-only strategy_id trips the earliest guard.

    Pins: ``INVALID_ARGUMENT`` + ``success=False`` + ``set_details`` wording
    contains the ``strategy_id`` field identifier produced by ``ValidationError``.
    """
    _install_warm_backend(service, None)  # should never be reached
    request = _make_request(strategy_id=strategy_id)

    response = await service.SavePortfolioMetrics(request, context)

    assert_grpc_error(
        context,
        response,
        expected_status=grpc.StatusCode.INVALID_ARGUMENT,
        error_substring="strategy_id",
    )


@pytest.mark.asyncio
async def test_invalid_strategy_id_format_returns_invalid_argument(service, context):
    """Strategy IDs with disallowed characters (e.g. spaces) are rejected."""
    _install_warm_backend(service, None)
    request = _make_request(strategy_id="has spaces!")

    response = await service.SavePortfolioMetrics(request, context)

    assert_grpc_error(
        context,
        response,
        expected_status=grpc.StatusCode.INVALID_ARGUMENT,
        error_substring="invalid format",
    )


# ---------------------------------------------------------------------------
# 2. Decimal parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field",
    ["initial_value_usd", "deposits_usd", "withdrawals_usd", "gas_spent_usd"],
)
@pytest.mark.asyncio
async def test_malformed_decimal_rejected(service, context, field):
    """Any of the four currency fields being a non-decimal trips InvalidOperation.

    Pins the exact user-facing wording: ``metrics fields must be valid decimal
    strings`` (downstream observability may grep it).
    """
    _install_warm_backend(service, None)
    kwargs = {field: "not-a-number"}
    request = _make_request(**kwargs)

    response = await service.SavePortfolioMetrics(request, context)

    assert response.success is False
    assert response.error == "metrics fields must be valid decimal strings"
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    context.set_details.assert_called_once_with("metrics fields must be valid decimal strings")


@pytest.mark.asyncio
async def test_empty_decimal_strings_coerced_to_zero(service, context):
    """Empty strings across all 4 currency fields are treated as ``"0"``.

    Exercises the ``request.field or "0"`` defensive coalescing before the
    ``Decimal(...)`` call.
    """
    warm = AsyncMock()
    warm.save_portfolio_metrics = AsyncMock(return_value=True)
    _install_warm_backend(service, warm)
    request = gateway_pb2.SaveMetricsRequest(strategy_id="zeros")

    response = await service.SavePortfolioMetrics(request, context)

    assert response.success is True
    assert_set_code_not_called(context)

    saved: PortfolioMetrics = warm.save_portfolio_metrics.call_args[0][0]
    assert saved.initial_value_usd == Decimal("0")
    assert saved.deposits_usd == Decimal("0")
    assert saved.withdrawals_usd == Decimal("0")
    assert saved.gas_spent_usd == Decimal("0")


# ---------------------------------------------------------------------------
# 3. Timestamp handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_negative_initial_timestamp_rejected(service, context):
    """Negative initial_timestamp is rejected before reaching ``fromtimestamp``.

    Pins the exact wording ``initial_timestamp must be non-negative``.
    """
    _install_warm_backend(service, None)
    request = _make_request(initial_timestamp=-1)

    response = await service.SavePortfolioMetrics(request, context)

    assert response.success is False
    assert response.error == "initial_timestamp must be non-negative"
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    context.set_details.assert_called_once_with("initial_timestamp must be non-negative")


@pytest.mark.asyncio
async def test_zero_initial_timestamp_defaults_to_now(service, context):
    """initial_timestamp=0 is falsy and falls through to ``datetime.now(UTC)``."""
    warm = AsyncMock()
    warm.save_portfolio_metrics = AsyncMock(return_value=True)
    _install_warm_backend(service, warm)
    before = datetime.now(UTC)
    request = _make_request(initial_timestamp=0)

    response = await service.SavePortfolioMetrics(request, context)
    after = datetime.now(UTC)

    assert response.success is True
    saved: PortfolioMetrics = warm.save_portfolio_metrics.call_args[0][0]
    # Pinned: when initial_timestamp is 0 the RPC substitutes the call time.
    assert before <= saved.timestamp <= after


@pytest.mark.asyncio
async def test_valid_initial_timestamp_converted_to_utc(service, context):
    """Positive initial_timestamp is converted to a tz-aware UTC datetime."""
    warm = AsyncMock()
    warm.save_portfolio_metrics = AsyncMock(return_value=True)
    _install_warm_backend(service, warm)
    request = _make_request(initial_timestamp=1712000000)

    response = await service.SavePortfolioMetrics(request, context)

    assert response.success is True
    saved: PortfolioMetrics = warm.save_portfolio_metrics.call_args[0][0]
    assert saved.timestamp == datetime.fromtimestamp(1712000000, tz=UTC)


@pytest.mark.asyncio
async def test_out_of_range_timestamp_rejected(service, context):
    """``fromtimestamp`` OverflowError -> INVALID_ARGUMENT with exact wording."""
    _install_warm_backend(service, None)
    # A value that overflows ``datetime.fromtimestamp`` on most platforms.
    request = _make_request(initial_timestamp=10**18)

    response = await service.SavePortfolioMetrics(request, context)

    assert response.success is False
    assert response.error == "initial_timestamp is out of range"
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    context.set_details.assert_called_once_with("initial_timestamp is out of range")


# ---------------------------------------------------------------------------
# 4. PostgreSQL branch (``_snapshot_pool is not None``)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_postgres_branch_success(service, context):
    """When the pg pool is present, the RPC issues the UPSERT and returns success.

    Pins: exactly one ``_snapshot_fetchrow`` call with the agent_id /
    timestamp / metric fields passed positionally in the documented order,
    plus the VIB-3933 finding-#1 fields ``total_value_usd`` and
    ``positions_json`` resolved from the latest snapshot.
    """
    service._snapshot_pool = MagicMock()  # truthy => PostgreSQL branch
    service._snapshot_fetchrow = AsyncMock(return_value={"agent_id": "strat-pg"})

    # VIB-3933 finding #1: PG path now reads latest snapshot for total_value_usd
    # before issuing the UPSERT (parity with SQLite). Provide a warm backend
    # mock that returns a snapshot with a known value.
    snapshot = MagicMock()
    snapshot.total_value_usd = Decimal("12345.67")
    warm = MagicMock()
    warm.get_latest_snapshot = AsyncMock(return_value=snapshot)
    _install_warm_backend(service, warm)

    request = _make_request(
        strategy_id="strat-pg",
        initial_value_usd="10000.50",
        initial_timestamp=1712000000,
        deposits_usd="500",
        withdrawals_usd="100",
        gas_spent_usd="25",
        deployment_id="depl-1",
        cycle_id="cyc-1",
        execution_mode="live",
        is_complete=True,
    )

    response = await service.SavePortfolioMetrics(request, context)

    assert response.success is True
    assert response.error == ""
    assert_set_code_not_called(context)
    service._snapshot_fetchrow.assert_awaited_once()
    # Positional args after the query string: agent_id, initial_value_usd,
    # timestamp, deposits, withdrawals, gas, deployment_id, cycle_id, mode,
    # is_complete, now, total_value_usd, positions_json.
    args = service._snapshot_fetchrow.call_args.args
    assert args[1] == "strat-pg"
    assert args[2] == "10000.50"
    assert isinstance(args[3], datetime) and args[3].tzinfo is not None
    assert args[4] == "500"
    assert args[5] == "100"
    assert args[6] == "25"
    assert args[7] == "depl-1"
    assert args[8] == "cyc-1"
    assert args[9] == "live"
    assert args[10] is True
    assert isinstance(args[11], datetime) and args[11].tzinfo is not None
    # VIB-3933 finding #1: snapshot's total_value_usd carried into the row.
    assert args[12] == "12345.67"
    # positions_json defaults to "[]" — proto carries no positions and SQLite
    # path also writes "[]" via PortfolioMetrics dataclass default.
    assert args[13] == "[]"
    warm.get_latest_snapshot.assert_awaited_once_with("strat-pg")


@pytest.mark.asyncio
async def test_postgres_branch_exception_returns_internal(service, context):
    """PG exception path: ``INTERNAL`` + ``internal server error`` details.

    Pins the error-path wording; downstream observability may grep it.
    """
    service._snapshot_pool = MagicMock()
    service._snapshot_fetchrow = AsyncMock(side_effect=RuntimeError("pg down"))

    response = await service.SavePortfolioMetrics(_make_request(), context)

    assert response.success is False
    assert response.error == "internal server error"
    context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
    context.set_details.assert_called_once_with("internal server error")


# ---------------------------------------------------------------------------
# 5. SQLite branch (``_snapshot_pool is None``)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sqlite_success_with_latest_snapshot_total_value(service, context):
    """SQLite branch: latest snapshot's ``total_value_usd`` is propagated.

    Pins the VIB-2765 carry-over: the proto doesn't transmit ``total_value_usd``
    so the RPC reads it from the last snapshot and stamps the metrics row.
    """
    # Duck-typed stand-in: the RPC only reads ``latest.total_value_usd``.
    # Using a MagicMock avoids coupling this test to the (evolving)
    # ``PortfolioSnapshot`` dataclass signature.
    latest = MagicMock()
    latest.total_value_usd = Decimal("9999.99")
    warm = AsyncMock()
    warm.get_latest_snapshot = AsyncMock(return_value=latest)
    warm.save_portfolio_metrics = AsyncMock(return_value=True)
    _install_warm_backend(service, warm)

    response = await service.SavePortfolioMetrics(
        _make_request(strategy_id="strat-sqlite"), context
    )

    assert response.success is True
    assert response.error == ""
    assert_set_code_not_called(context)

    saved: PortfolioMetrics = warm.save_portfolio_metrics.call_args[0][0]
    assert saved.strategy_id == "strat-sqlite"
    assert saved.total_value_usd == Decimal("9999.99")
    assert saved.initial_value_usd == Decimal("10000")
    assert saved.deposits_usd == Decimal("500")
    assert saved.withdrawals_usd == Decimal("100")
    assert saved.gas_spent_usd == Decimal("25")


@pytest.mark.asyncio
async def test_sqlite_snapshot_lookup_exception_warns_and_continues(service, context, caplog):
    """Snapshot lookup failure is swallowed with a warning; save still proceeds.

    Pins the try/except around ``get_latest_snapshot`` — a broken snapshot
    backend must NOT abort the metrics write. ``total_value_usd`` falls back
    to ``Decimal("0")``.
    """
    warm = AsyncMock()
    warm.get_latest_snapshot = AsyncMock(side_effect=RuntimeError("snap error"))
    warm.save_portfolio_metrics = AsyncMock(return_value=True)
    _install_warm_backend(service, warm)

    response = await service.SavePortfolioMetrics(_make_request(), context)

    assert response.success is True
    saved: PortfolioMetrics = warm.save_portfolio_metrics.call_args[0][0]
    assert saved.total_value_usd == Decimal("0")


@pytest.mark.asyncio
async def test_sqlite_no_latest_snapshot_uses_zero_total_value(service, context):
    """``get_latest_snapshot`` -> None: ``total_value_usd`` = Decimal("0")."""
    warm = AsyncMock()
    warm.get_latest_snapshot = AsyncMock(return_value=None)
    warm.save_portfolio_metrics = AsyncMock(return_value=True)
    _install_warm_backend(service, warm)

    response = await service.SavePortfolioMetrics(_make_request(), context)

    assert response.success is True
    saved: PortfolioMetrics = warm.save_portfolio_metrics.call_args[0][0]
    assert saved.total_value_usd == Decimal("0")


@pytest.mark.asyncio
async def test_sqlite_warm_backend_without_get_latest_snapshot(service, context):
    """Warm backend missing ``get_latest_snapshot`` still saves with total=0.

    The ``hasattr`` guard protects early warm-backend impls that only
    implement ``save_portfolio_metrics``.
    """
    warm = MagicMock(spec=["save_portfolio_metrics"])  # no get_latest_snapshot
    warm.save_portfolio_metrics = AsyncMock(return_value=True)
    _install_warm_backend(service, warm)

    response = await service.SavePortfolioMetrics(_make_request(), context)

    assert response.success is True
    saved: PortfolioMetrics = warm.save_portfolio_metrics.call_args[0][0]
    assert saved.total_value_usd == Decimal("0")


@pytest.mark.asyncio
async def test_sqlite_no_warm_backend(service, context):
    """No warm backend -> success=False with ``No warm backend`` message.

    Note: this is NOT an exception path; the RPC does NOT ``set_code``.
    """
    _install_warm_backend(service, None)

    response = await service.SavePortfolioMetrics(_make_request(), context)

    assert response.success is False
    assert response.error == "No warm backend with portfolio metrics support"
    assert_set_code_not_called(context)


@pytest.mark.asyncio
async def test_sqlite_warm_backend_without_save_metrics_method(service, context):
    """Warm backend lacking ``save_portfolio_metrics`` -> same fallback message."""
    warm = MagicMock(spec=["get_latest_snapshot"])  # no save_portfolio_metrics
    warm.get_latest_snapshot = AsyncMock(return_value=None)
    _install_warm_backend(service, warm)

    response = await service.SavePortfolioMetrics(_make_request(), context)

    assert response.success is False
    assert response.error == "No warm backend with portfolio metrics support"
    assert_set_code_not_called(context)


@pytest.mark.asyncio
async def test_sqlite_backend_returns_false(service, context):
    """Warm ``save_portfolio_metrics`` returning False -> failure response.

    Pins the exact ``Backend save_portfolio_metrics returned False`` wording.
    """
    warm = AsyncMock()
    warm.get_latest_snapshot = AsyncMock(return_value=None)
    warm.save_portfolio_metrics = AsyncMock(return_value=False)
    _install_warm_backend(service, warm)

    response = await service.SavePortfolioMetrics(_make_request(), context)

    assert response.success is False
    assert response.error == "Backend save_portfolio_metrics returned False"
    assert_set_code_not_called(context)


@pytest.mark.asyncio
async def test_sqlite_backend_exception_returns_internal(service, context):
    """Warm backend exception -> INTERNAL + ``internal server error`` details."""
    warm = AsyncMock()
    warm.get_latest_snapshot = AsyncMock(return_value=None)
    warm.save_portfolio_metrics = AsyncMock(side_effect=RuntimeError("boom"))
    _install_warm_backend(service, warm)

    response = await service.SavePortfolioMetrics(_make_request(), context)

    assert response.success is False
    assert response.error == "internal server error"
    context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
    context.set_details.assert_called_once_with("internal server error")


# ---------------------------------------------------------------------------
# 6. PortfolioMetrics serialisation (Phase 4 accounting fields)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_phase4_accounting_fields_propagated(service, context):
    """deployment_id / cycle_id / execution_mode / is_complete round-trip.

    Pins: empty ``cycle_id`` becomes ``None`` (per ``request.cycle_id or None``);
    the other three string fields are passed as empty strings when unset.
    """
    warm = AsyncMock()
    warm.get_latest_snapshot = AsyncMock(return_value=None)
    warm.save_portfolio_metrics = AsyncMock(return_value=True)
    _install_warm_backend(service, warm)

    response = await service.SavePortfolioMetrics(
        _make_request(
            deployment_id="my-deploy",
            cycle_id="cycle-42",
            execution_mode="paper",
            is_complete=True,
        ),
        context,
    )

    assert response.success is True
    saved: PortfolioMetrics = warm.save_portfolio_metrics.call_args[0][0]
    assert saved.deployment_id == "my-deploy"
    assert saved.cycle_id == "cycle-42"
    assert saved.execution_mode == "paper"
    assert saved.is_complete is True


@pytest.mark.asyncio
async def test_empty_cycle_id_becomes_none(service, context):
    """Unset ``cycle_id`` is normalised to ``None`` in the PortfolioMetrics."""
    warm = AsyncMock()
    warm.get_latest_snapshot = AsyncMock(return_value=None)
    warm.save_portfolio_metrics = AsyncMock(return_value=True)
    _install_warm_backend(service, warm)

    response = await service.SavePortfolioMetrics(_make_request(cycle_id=""), context)

    assert response.success is True
    saved: PortfolioMetrics = warm.save_portfolio_metrics.call_args[0][0]
    assert saved.cycle_id is None
