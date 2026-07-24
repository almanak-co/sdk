"""Unit tests for ``GetTradeTape`` and its module-private helpers.

VIB-4079 W2 Sub-D1: lifts ``dashboard_service.py`` coverage by exercising
the previously-uncovered branches of the trade-tape join (cycle-level
event fallback, payload version parsing, ``before_timestamp`` cursor,
per-source backend errors, has_more pagination). The wider gRPC surface
is covered by ``tests/gateway/test_dashboard_service.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import (
    DashboardServiceServicer,
    _parse_trade_tape_payload_versions,
    _resolve_trade_tape_row_event,
)

# ──────────────────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ──────────────────────────────────────────────────────────────────────────────


def _make_servicer() -> DashboardServiceServicer:
    """Build a DashboardServiceServicer without running the full __init__ network setup."""
    svc = DashboardServiceServicer.__new__(DashboardServiceServicer)
    svc.settings = SimpleNamespace()
    svc._state_manager = None
    svc._initialized = True
    svc._strategies_root = None
    svc._cached_positions = {}
    return svc


def _make_ledger_entry(
    *,
    entry_id: str = "L1",
    cycle_id: str = "C1",
    timestamp: datetime | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=entry_id,
        cycle_id=cycle_id,
        timestamp=timestamp or datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        intent_type="SWAP",
        token_in="USDC",
        amount_in="100",
        token_out="WETH",
        amount_out="0.03",
        effective_price="3333.33",
        slippage_bps=5.0,
        gas_used=21000,
        gas_usd="0.50",
        tx_hash="0xabc",
        chain="arbitrum",
        protocol="UniswapV3",
        success=True,
        error="",
        extracted_data_json="",
        price_inputs_json="",
        pre_state_json="",
        post_state_json="",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Pure helpers (1, 2)
# ──────────────────────────────────────────────────────────────────────────────


def test_resolve_trade_tape_row_event_cycle_fallback_is_safe() -> None:
    """Cycle fallback fires only when the cycle has exactly one event (PR #2014 audit)."""
    by_ledger: dict[str, dict] = {}
    by_cycle = {
        "C1": [{"event_type": "SWAP_OUT"}],
        "C2": [{"event_type": "LP_CLOSE"}, {"event_type": "REPAY"}],
    }
    # Single-event cycle → fallback returns that event
    assert _resolve_trade_tape_row_event("L1", "C1", by_ledger, by_cycle) == {"event_type": "SWAP_OUT"}
    # Multi-event cycle (e.g. teardown) → must NOT pick arbitrarily
    assert _resolve_trade_tape_row_event("L2", "C2", by_ledger, by_cycle) is None
    # Direct ledger hit always wins over fallback
    by_ledger["L3"] = {"event_type": "DIRECT"}
    assert _resolve_trade_tape_row_event("L3", "C1", by_ledger, by_cycle) == {"event_type": "DIRECT"}


def test_parse_trade_tape_payload_versions_extracts_or_defaults() -> None:
    """Well-formed payload yields parsed versions; malformed/non-dict collapses to zeros."""
    payload = (
        '{"unavailable_reason": "lp_pre_state_missing",'
        ' "schema_version": 2, "formula_version": 3, "matching_policy_version": 1}'
    )
    assert _parse_trade_tape_payload_versions(payload) == ("lp_pre_state_missing", 2, 3, 1)
    assert _parse_trade_tape_payload_versions("") == ("", 0, 0, 0)
    assert _parse_trade_tape_payload_versions("not json") == ("", 0, 0, 0)
    assert _parse_trade_tape_payload_versions("[1, 2, 3]") == ("", 0, 0, 0)


def test_parse_trade_tape_payload_versions_coerces_non_string_unavailable_reason() -> None:
    """Non-string ``unavailable_reason`` (dict / int / list) must coerce to ``""``.

    ``parsed.get("unavailable_reason") or ""`` would let a truthy non-string
    value (e.g. ``{"code": "x"}``) slip through into ``_build_trade_tape_row``,
    where protobuf rejects non-strings on the proto string field — turning
    ``GetTradeTape`` into INTERNAL.
    """
    payload = '{"unavailable_reason": {"code": "x"}}'
    assert _parse_trade_tape_payload_versions(payload) == ("", 0, 0, 0)
    payload = '{"unavailable_reason": 42}'
    assert _parse_trade_tape_payload_versions(payload) == ("", 0, 0, 0)
    payload = '{"unavailable_reason": ["a", "b"]}'
    assert _parse_trade_tape_payload_versions(payload) == ("", 0, 0, 0)
    # Sanity: the happy-path string still flows through unchanged.
    payload = '{"unavailable_reason": "lp_pre_state_missing"}'
    assert _parse_trade_tape_payload_versions(payload) == ("lp_pre_state_missing", 0, 0, 0)


@pytest.mark.asyncio
async def test_get_trade_tape_coerces_jsonb_payload_json_to_str() -> None:
    """Backend can return ``payload_json`` as JSONB (dict / list), not a string.

    Without coercion, ``accounting_payload_json=payload_raw`` would crash on a
    proto type-error and turn ``GetTradeTape`` into INTERNAL.
    """
    svc = _make_servicer()
    entry = _make_ledger_entry(entry_id="L1", cycle_id="C1")
    sm = MagicMock()
    sm.get_ledger_entries = AsyncMock(return_value=[entry])
    sm.get_accounting_events_for_dashboard = AsyncMock(
        return_value=[
            {
                "ledger_entry_id": "L1",
                "cycle_id": "C1",
                # JSONB shape — would crash proto string assignment without coercion.
                "payload_json": {"schema_version": 4, "unavailable_reason": "lp_pre_state_missing"},
                "confidence": {"score": 0.9},  # also non-string
                "event_type": ["SWAP_OUT"],  # list, not string
                "position_key": None,
            }
        ]
    )
    sm.get_position_events_for_dashboard = AsyncMock(return_value=[])
    svc._state_manager = sm

    request = gateway_pb2.GetTradeTapeRequest(deployment_id="test_strategy", limit=10)
    response = await svc.GetTradeTape(request, MagicMock(spec=grpc.aio.ServicerContext))

    # Row surfaces; all proto string fields are populated cleanly.
    assert len(response.rows) == 1
    row = response.rows[0]
    assert isinstance(row.accounting_payload_json, str)
    # The JSONB payload was serialised into a string proto field.
    assert "lp_pre_state_missing" in row.accounting_payload_json
    # _parse_trade_tape_payload_versions decoded the serialised payload back.
    assert row.schema_version == 4
    assert row.unavailable_reason == "lp_pre_state_missing"
    # Other fields all coerced to str.
    assert isinstance(row.confidence, str)
    assert isinstance(row.accounting_event_type, str)
    assert row.position_key == ""  # None → ""


def test_parse_trade_tape_payload_versions_handles_non_integer_stamps() -> None:
    """Non-integer version stamps must coerce to 0, not raise — bad data must not 500 the RPC.

    Older corrupt rows can carry strings like ``"v1"`` or ``"1.0"`` for the version
    fields. Before the fix these raised ``ValueError`` outside the ``json.loads``
    try/except, turning ``GetTradeTape`` into an INTERNAL gRPC error.
    """
    payload = '{"schema_version": "v1", "formula_version": "1.0", "matching_policy_version": null}'
    assert _parse_trade_tape_payload_versions(payload) == ("", 0, 0, 0)
    payload = '{"schema_version": 2, "formula_version": "garbage", "matching_policy_version": 7}'
    assert _parse_trade_tape_payload_versions(payload) == ("", 2, 0, 7)
    payload = '{"schema_version": [1], "formula_version": {"a": 1}, "matching_policy_version": 1}'
    assert _parse_trade_tape_payload_versions(payload) == ("", 0, 0, 1)


# ──────────────────────────────────────────────────────────────────────────────
# GetTradeTape end-to-end (3, 4, 5, 6)
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_trade_tape_happy_path_joins_all_three_sources() -> None:
    """Ledger × accounting × position events are joined into one row each."""
    svc = _make_servicer()
    entry = _make_ledger_entry(entry_id="L1", cycle_id="C1")
    sm = MagicMock()
    sm.get_ledger_entries = AsyncMock(return_value=[entry])
    sm.get_accounting_events_for_dashboard = AsyncMock(
        return_value=[
            {
                "ledger_entry_id": "L1",
                "cycle_id": "C1",
                "event_type": "SWAP_OUT",
                "payload_json": '{"schema_version": 4, "formula_version": 1, "matching_policy_version": 2}',
                "confidence": "HIGH",
                "position_key": "uni-v3:USDC-WETH",
            }
        ]
    )
    sm.get_position_events_for_dashboard = AsyncMock(
        return_value=[{"ledger_entry_id": "L1", "position_id": "P1", "event_type": "OPEN"}]
    )
    svc._state_manager = sm

    request = gateway_pb2.GetTradeTapeRequest(deployment_id="test_strategy", limit=10)
    response = await svc.GetTradeTape(request, MagicMock(spec=grpc.aio.ServicerContext))

    assert len(response.rows) == 1
    row = response.rows[0]
    assert (row.id, row.cycle_id, row.intent_type) == ("L1", "C1", "SWAP")
    assert row.accounting_event_type == "SWAP_OUT"
    assert row.position_key == "uni-v3:USDC-WETH"
    assert (row.schema_version, row.formula_version, row.matching_policy_version) == (4, 1, 2)
    assert (row.position_id, row.position_event_type) == ("P1", "OPEN")
    assert response.has_more is False


@pytest.mark.asyncio
async def test_get_trade_tape_paginates_with_has_more_when_overfetched() -> None:
    """When the backend returns ``limit + 1`` rows, the response trims to ``limit`` and sets has_more."""
    svc = _make_servicer()
    entries = [_make_ledger_entry(entry_id=f"L{i}", cycle_id=f"C{i}") for i in range(3)]
    sm = MagicMock()
    sm.get_ledger_entries = AsyncMock(return_value=entries)
    sm.get_accounting_events_for_dashboard = AsyncMock(return_value=[])
    sm.get_position_events_for_dashboard = AsyncMock(return_value=[])
    svc._state_manager = sm

    request = gateway_pb2.GetTradeTapeRequest(deployment_id="test_strategy", limit=2)
    response = await svc.GetTradeTape(request, MagicMock(spec=grpc.aio.ServicerContext))

    assert len(response.rows) == 2
    assert response.has_more is True
    # over-fetch math: limit+1 passed through to the backend
    assert sm.get_ledger_entries.await_args.kwargs["limit"] == 3


@pytest.mark.asyncio
async def test_get_trade_tape_drops_entries_at_or_after_before_cursor() -> None:
    """``before_timestamp`` filter excludes rows whose timestamp is >= cursor."""
    svc = _make_servicer()
    cutoff = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
    entries = [
        _make_ledger_entry(entry_id="L_old", timestamp=datetime(2026, 4, 30, tzinfo=UTC)),
        _make_ledger_entry(entry_id="L_at_cutoff", timestamp=cutoff),
        _make_ledger_entry(entry_id="L_newer", timestamp=datetime(2026, 5, 2, tzinfo=UTC)),
    ]
    sm = MagicMock()
    sm.get_ledger_entries = AsyncMock(return_value=entries)
    sm.get_accounting_events_for_dashboard = AsyncMock(return_value=[])
    sm.get_position_events_for_dashboard = AsyncMock(return_value=[])
    svc._state_manager = sm

    request = gateway_pb2.GetTradeTapeRequest(
        deployment_id="test_strategy", limit=10, before_timestamp=int(cutoff.timestamp())
    )
    response = await svc.GetTradeTape(request, MagicMock(spec=grpc.aio.ServicerContext))

    assert [r.id for r in response.rows] == ["L_old"]


@pytest.mark.asyncio
async def test_get_trade_tape_swallows_per_source_backend_errors() -> None:
    """Optional-enrichment failure (accounting / position) degrades gracefully.

    Ledger is the primary source; accounting + position events are optional
    enrichment. A failure on either enrichment source must not abort the RPC —
    the trade tape still renders ledger rows with empty event-payload fields.
    (Ledger-source failures, in contrast, fail the RPC — see
    ``test_get_trade_tape_ledger_failure_returns_unavailable``.)
    """
    svc = _make_servicer()
    entry = _make_ledger_entry(entry_id="L1", cycle_id="C1")
    sm = MagicMock()
    sm.get_ledger_entries = AsyncMock(return_value=[entry])
    sm.get_accounting_events_for_dashboard = AsyncMock(side_effect=RuntimeError("backend down"))
    sm.get_position_events_for_dashboard = AsyncMock(return_value=[])
    svc._state_manager = sm

    request = gateway_pb2.GetTradeTapeRequest(deployment_id="test_strategy", limit=10)
    response = await svc.GetTradeTape(request, MagicMock(spec=grpc.aio.ServicerContext))

    # Ledger row still surfaces; accounting fields default to empty.
    assert len(response.rows) == 1
    row = response.rows[0]
    assert row.id == "L1"
    assert row.accounting_event_type == ""
    assert row.accounting_payload_json == ""
    assert row.schema_version == 0


@pytest.mark.asyncio
async def test_get_trade_tape_invalid_before_timestamp_returns_invalid_argument() -> None:
    """Out-of-range ``before_timestamp`` must surface as INVALID_ARGUMENT, not INTERNAL.

    ``datetime.fromtimestamp(9_999_999_999_999, tz=UTC)`` raises ``OverflowError``
    on most platforms. Without explicit handling, the exception escapes to the
    gRPC framework and surfaces as ``INTERNAL`` — opaque to clients. The same
    validation pattern is already used by ``GetActivityFeed``.
    """
    svc = _make_servicer()
    sm = MagicMock()
    sm.get_ledger_entries = AsyncMock(return_value=[])
    sm.get_accounting_events_for_dashboard = AsyncMock(return_value=[])
    sm.get_position_events_for_dashboard = AsyncMock(return_value=[])
    svc._state_manager = sm

    context = MagicMock(spec=grpc.aio.ServicerContext)
    # Year > ~10 trillion seconds blows past the datetime range on every platform.
    request = gateway_pb2.GetTradeTapeRequest(
        deployment_id="test_strategy", limit=10, before_timestamp=9_999_999_999_999
    )
    response = await svc.GetTradeTape(request, context)

    assert len(response.rows) == 0
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    # The error message refers to the parameter, not implementation internals.
    details_arg = context.set_details.call_args.args[0]
    assert "before_timestamp" in details_arg
    # Backend wasn't even consulted (we failed validation before that).
    sm.get_ledger_entries.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_trade_tape_missing_state_manager_returns_unavailable() -> None:
    """Missing ``StateManager`` (init failed) must surface as UNAVAILABLE, not empty rows.

    Same fail-loud contract as a ledger-backend exception: callers can't tell
    "no trades" apart from "backend not initialized" without the status code.
    """
    svc = _make_servicer()
    svc._state_manager = None  # init failed / never wired

    context = MagicMock(spec=grpc.aio.ServicerContext)
    request = gateway_pb2.GetTradeTapeRequest(deployment_id="test_strategy", limit=10)
    response = await svc.GetTradeTape(request, context)

    assert len(response.rows) == 0
    assert response.has_more is False
    context.set_code.assert_called_once_with(grpc.StatusCode.UNAVAILABLE)


@pytest.mark.asyncio
async def test_get_trade_tape_ledger_failure_returns_unavailable() -> None:
    """Ledger-source failure must surface as gRPC UNAVAILABLE, not empty rows.

    ``GetTradeTapeResponse`` has no error field, so swallowing a ledger backend
    failure would render as ``rows=[]`` / ``has_more=false`` — indistinguishable
    from a genuine empty history. The RPC must fail loudly so callers can retry
    or degrade their UI rather than misreport "no trades".

    Per the gateway-boundary contract, the actual exception message must NOT
    leak across the gRPC response — only a generic operator-friendly summary.
    """
    svc = _make_servicer()
    sm = MagicMock()
    sm.get_ledger_entries = AsyncMock(side_effect=RuntimeError("postgres down: secret-host:5432"))
    sm.get_accounting_events_for_dashboard = AsyncMock(return_value=[])
    sm.get_position_events_for_dashboard = AsyncMock(return_value=[])
    svc._state_manager = sm

    context = MagicMock(spec=grpc.aio.ServicerContext)
    request = gateway_pb2.GetTradeTapeRequest(deployment_id="test_strategy", limit=10)
    response = await svc.GetTradeTape(request, context)

    assert len(response.rows) == 0
    assert response.has_more is False
    context.set_code.assert_called_once_with(grpc.StatusCode.UNAVAILABLE)
    # Generic operator-friendly details, not the underlying exception text.
    details_arg = context.set_details.call_args.args[0]
    assert "secret-host" not in details_arg
    assert "postgres" not in details_arg.lower()


# ──────────────────────────────────────────────────────────────────────────────
# _ensure_initialized
# ──────────────────────────────────────────────────────────────────────────────

_STATE_MANAGER_PATCH = "almanak.framework.state.state_manager.StateManager"
_PORTFOLIO_CHAIN_PATCH = "almanak.gateway.services.dashboard_service.build_portfolio_chain"


def _make_uninitialized_servicer(*, database_url: str | None = None) -> DashboardServiceServicer:
    """Servicer via ``__new__`` with only the state ``_ensure_initialized`` touches."""
    svc = DashboardServiceServicer.__new__(DashboardServiceServicer)
    svc.settings = SimpleNamespace(
        database_url=database_url,
        portfolio_providers="zerion",
        portfolio_api_key="test-key",
        portfolio_api_provider="zerion",
        portfolio_api_cache_ttl=300,
    )
    svc._initialized = False
    svc._state_manager = None
    svc._strategies_root = None
    svc._portfolio_chain = None
    return svc


class TestEnsureInitialized:
    """Branch coverage for DashboardServiceServicer._ensure_initialized."""

    @pytest.mark.asyncio
    async def test_already_initialized_returns_without_side_effects(self):
        svc = _make_uninitialized_servicer()
        svc._initialized = True

        with (
            patch(_STATE_MANAGER_PATCH) as sm_cls,
            patch(_PORTFOLIO_CHAIN_PATCH) as chain_factory,
        ):
            await svc._ensure_initialized()

        sm_cls.assert_not_called()
        chain_factory.assert_not_called()
        assert svc._strategies_root is None

    @pytest.mark.asyncio
    async def test_discovers_repo_strategies_root_and_initializes(self):
        svc = _make_uninitialized_servicer()
        sentinel_chain = MagicMock(name="portfolio-chain")

        with (
            patch(_STATE_MANAGER_PATCH) as sm_cls,
            patch(_PORTFOLIO_CHAIN_PATCH, return_value=sentinel_chain) as chain_factory,
        ):
            sm_cls.return_value.initialize = AsyncMock()
            await svc._ensure_initialized()

        assert svc._initialized is True
        # The repo checkout has a strategies/ directory next to almanak/.
        assert svc._strategies_root is not None
        assert svc._strategies_root.name == "strategies"
        assert svc._strategies_root.exists()
        assert svc._state_manager is sm_cls.return_value
        sm_cls.return_value.initialize.assert_awaited_once()
        assert svc._portfolio_chain is sentinel_chain
        chain_factory.assert_called_once_with(
            portfolio_providers_csv="zerion",
            portfolio_api_key="test-key",
            portfolio_api_provider="zerion",
            portfolio_api_cache_ttl=300,
        )

    @pytest.mark.asyncio
    async def test_missing_strategies_root_falls_back_to_cwd(self, monkeypatch):
        from pathlib import Path

        svc = _make_uninitialized_servicer()
        monkeypatch.setattr(Path, "exists", lambda self, **kwargs: False)

        with (
            patch(_STATE_MANAGER_PATCH) as sm_cls,
            patch(_PORTFOLIO_CHAIN_PATCH, return_value=None),
        ):
            sm_cls.return_value.initialize = AsyncMock()
            await svc._ensure_initialized()

        assert svc._strategies_root == Path.cwd() / "strategies"
        assert svc._initialized is True

    @pytest.mark.asyncio
    async def test_database_url_selects_postgres_backend(self):
        from almanak.framework.state.state_manager import WarmBackendType

        url = "postgresql://user:pw@localhost:5432/gateway"
        svc = _make_uninitialized_servicer(database_url=url)

        with (
            patch(_STATE_MANAGER_PATCH) as sm_cls,
            patch(_PORTFOLIO_CHAIN_PATCH, return_value=None),
        ):
            sm_cls.return_value.initialize = AsyncMock()
            await svc._ensure_initialized()

        config = sm_cls.call_args.args[0]
        assert config.warm_backend is WarmBackendType.POSTGRESQL
        assert config.database_url == url

    @pytest.mark.asyncio
    async def test_no_database_url_selects_sqlite_backend(self):
        from almanak.framework.state.state_manager import WarmBackendType

        svc = _make_uninitialized_servicer(database_url=None)

        with (
            patch(_STATE_MANAGER_PATCH) as sm_cls,
            patch(_PORTFOLIO_CHAIN_PATCH, return_value=None),
        ):
            sm_cls.return_value.initialize = AsyncMock()
            await svc._ensure_initialized()

        config = sm_cls.call_args.args[0]
        assert config.warm_backend is WarmBackendType.SQLITE
        assert config.database_url is None

    @pytest.mark.asyncio
    async def test_state_manager_failure_degrades_to_none_but_still_initializes(self):
        svc = _make_uninitialized_servicer()
        sentinel_chain = MagicMock(name="portfolio-chain")

        with (
            patch(_STATE_MANAGER_PATCH, side_effect=RuntimeError("db down")),
            patch(_PORTFOLIO_CHAIN_PATCH, return_value=sentinel_chain),
        ):
            await svc._ensure_initialized()

        assert svc._state_manager is None
        assert svc._initialized is True
        # Portfolio chain init still runs after the StateManager failure.
        assert svc._portfolio_chain is sentinel_chain

    @pytest.mark.asyncio
    async def test_state_manager_initialize_await_failure_degrades_to_none(self):
        svc = _make_uninitialized_servicer()

        with (
            patch(_STATE_MANAGER_PATCH) as sm_cls,
            patch(_PORTFOLIO_CHAIN_PATCH, return_value=None),
        ):
            sm_cls.return_value.initialize = AsyncMock(side_effect=ConnectionError("refused"))
            await svc._ensure_initialized()

        assert svc._state_manager is None
        assert svc._initialized is True

    @pytest.mark.asyncio
    async def test_portfolio_chain_failure_degrades_to_none(self):
        svc = _make_uninitialized_servicer()

        with (
            patch(_STATE_MANAGER_PATCH) as sm_cls,
            patch(_PORTFOLIO_CHAIN_PATCH, side_effect=ValueError("bad provider csv")),
        ):
            sm_cls.return_value.initialize = AsyncMock()
            await svc._ensure_initialized()

        assert svc._portfolio_chain is None
        assert svc._initialized is True

    @pytest.mark.asyncio
    async def test_second_call_is_a_noop(self):
        svc = _make_uninitialized_servicer()

        with (
            patch(_STATE_MANAGER_PATCH) as sm_cls,
            patch(_PORTFOLIO_CHAIN_PATCH, return_value=None) as chain_factory,
        ):
            sm_cls.return_value.initialize = AsyncMock()
            await svc._ensure_initialized()
            await svc._ensure_initialized()

        assert sm_cls.call_count == 1
        assert chain_factory.call_count == 1
