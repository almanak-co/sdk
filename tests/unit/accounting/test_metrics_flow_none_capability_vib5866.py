"""VIB-5866 leg B (PR-C1) — capital-flow Empty≠Zero capability on the metrics seams.

``PortfolioMetrics.deposits_usd`` / ``withdrawals_usd`` become
``Decimal | None``: ``Decimal("0")`` is a measured zero, ``None`` is
UNMEASURED (blueprint 27 §10.10). Fabricating a zero for an unmeasured flow
books external capital as profit in ``pnl_before_gas``.

This PR ships dark — nothing writes ``None`` yet (the producer lands in a later
PR). What it locks is that ``None`` is *survivable and round-trippable* on every
serialization seam, and that legacy ``'0'`` data is unaffected:

* the model (defaults, ``__post_init__``, PnL propagation, ``to_dict`` /
  ``from_dict``);
* the SQLite backend — both write seams and the read seam, asserted against the
  RAW stored column text (the literal ``"None"`` must never be persisted);
* the framework Postgres row reader;
* the gateway wire — ``SaveMetricsRequest`` parse, PG UPSERT args, and both
  ``PortfolioMetricsData`` response builders.

The storage / wire sentinel for UNMEASURED is the empty string. No DDL is
involved: the columns are ``TEXT DEFAULT '0'`` in both backends and the proto
fields are plain strings, so ``''`` is data, not schema.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.portfolio.models import (
    PortfolioMetrics,
    decode_optional_flow,
    encode_optional_flow,
)


def _metrics(
    deposits: Decimal | None = Decimal("0"),
    withdrawals: Decimal | None = Decimal("0"),
    total_value: Decimal | None = Decimal("10"),
) -> PortfolioMetrics:
    """Build a REAL PortfolioMetrics (not a SimpleNamespace) per CLAUDE.md."""
    return PortfolioMetrics(
        deployment_id="deployment:abc123def456",
        timestamp=datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
        total_value_usd=total_value,
        initial_value_usd=Decimal("4"),
        deposits_usd=deposits,
        withdrawals_usd=withdrawals,
        gas_spent_usd=Decimal("0.50"),
    )


# ---------------------------------------------------------------------------
# 1. Codec helpers
# ---------------------------------------------------------------------------


def test_encode_optional_flow_never_emits_the_literal_none() -> None:
    assert encode_optional_flow(None) == ""
    assert encode_optional_flow(Decimal("0")) == "0"
    assert encode_optional_flow(Decimal("12.5")) == "12.5"


def test_decode_optional_flow_maps_only_empty_and_null_to_none() -> None:
    assert decode_optional_flow("") is None
    assert decode_optional_flow(None) is None
    # Legacy rows: a measured zero stays a measured zero.
    assert decode_optional_flow("0") == Decimal("0")
    assert decode_optional_flow("12.5") == Decimal("12.5")


# ---------------------------------------------------------------------------
# 2. Model: defaults, __post_init__, PnL propagation, dict round-trip
# ---------------------------------------------------------------------------


def test_default_flows_are_measured_zero() -> None:
    """Behaviour-preserving: the defaults are unchanged by this PR."""
    m = PortfolioMetrics(
        deployment_id="deployment:abc123def456",
        timestamp=datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
        total_value_usd=Decimal("10"),
        initial_value_usd=Decimal("4"),
    )
    assert m.deposits_usd == Decimal("0")
    assert m.withdrawals_usd == Decimal("0")
    assert m.pnl_before_gas == Decimal("6")


def test_post_init_passes_none_through_uncoerced() -> None:
    m = _metrics(deposits=None, withdrawals=None)
    assert m.deposits_usd is None
    assert m.withdrawals_usd is None


def test_post_init_still_coerces_strings_and_numbers() -> None:
    m = _metrics(deposits="3.5", withdrawals=2)  # type: ignore[arg-type]
    assert m.deposits_usd == Decimal("3.5")
    assert m.withdrawals_usd == Decimal("2")


@pytest.mark.parametrize(
    ("deposits", "withdrawals"),
    [(None, Decimal("0")), (Decimal("0"), None), (None, None)],
)
def test_unmeasured_flow_propagates_none_through_pnl(deposits: Decimal | None, withdrawals: Decimal | None) -> None:
    """An unmeasured flow poisons the whole PnL — it is never treated as 0."""
    m = _metrics(deposits=deposits, withdrawals=withdrawals)
    assert m.pnl_before_gas is None
    assert m.pnl_after_gas is None
    assert m.roi_percent is None


def test_measured_zero_flows_compute_pnl_unchanged() -> None:
    """Empty≠Zero: measured-zero flows still produce a real PnL."""
    m = _metrics()
    assert m.pnl_before_gas == Decimal("6")
    assert m.pnl_after_gas == Decimal("5.50")
    assert m.roi_percent == Decimal("150")


def test_measured_flows_compute_pnl_unchanged() -> None:
    m = _metrics(deposits=Decimal("2"), withdrawals=Decimal("1"))
    assert m.pnl_before_gas == Decimal("5")


def test_to_dict_from_dict_round_trips_none_as_json_null() -> None:
    d = _metrics(deposits=None, withdrawals=None).to_dict()
    assert d["deposits_usd"] is None
    assert d["withdrawals_usd"] is None

    restored = PortfolioMetrics.from_dict(d)
    assert restored.deposits_usd is None
    assert restored.withdrawals_usd is None
    assert restored.pnl_before_gas is None


def test_to_dict_from_dict_round_trips_measured_values() -> None:
    d = _metrics(deposits=Decimal("7.25"), withdrawals=Decimal("0")).to_dict()
    assert d["deposits_usd"] == "7.25"
    assert d["withdrawals_usd"] == "0"

    restored = PortfolioMetrics.from_dict(d)
    assert restored.deposits_usd == Decimal("7.25")
    assert restored.withdrawals_usd == Decimal("0")


def test_from_dict_missing_key_stays_measured_zero() -> None:
    """A MISSING key predates the field — it is not an "unmeasured" claim."""
    d = _metrics().to_dict()
    del d["deposits_usd"]
    del d["withdrawals_usd"]

    restored = PortfolioMetrics.from_dict(d)
    assert restored.deposits_usd == Decimal("0")
    assert restored.withdrawals_usd == Decimal("0")


def test_from_dict_empty_string_is_unmeasured() -> None:
    d = _metrics().to_dict()
    d["deposits_usd"] = ""
    assert PortfolioMetrics.from_dict(d).deposits_usd is None


# ---------------------------------------------------------------------------
# 3. SQLite backend round-trips (raw column text asserted)
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path) -> str:
    return str(tmp_path / "state.db")


@pytest.fixture
def store(db_path):
    """A real initialized SQLiteStore over a temp DB file."""
    import asyncio

    from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

    backend = SQLiteStore(SQLiteConfig(db_path=db_path))
    # ``asyncio.get_event_loop()`` raises in a worker thread that has no
    # current loop (deprecated since 3.10) — under xdist this errored the whole
    # fixture at setup. Own an explicit loop for the fixture's lifetime.
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(backend.initialize())
        yield backend
        loop.run_until_complete(backend.close())
    finally:
        loop.close()


def _raw_flows(db_path: str, deployment_id: str) -> tuple[str | None, str | None]:
    """Read the stored column TEXT verbatim — no Decimal coercion in the way."""
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT deposits_usd, withdrawals_usd FROM portfolio_metrics WHERE deployment_id = ?",
            (deployment_id,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    return row[0], row[1]


@pytest.mark.asyncio
async def test_sqlite_round_trips_unmeasured_flows_as_empty_text(store, db_path) -> None:
    metrics = _metrics(deposits=None, withdrawals=None)
    assert await store.save_portfolio_metrics(metrics) is True

    # The literal "None" must never be persisted.
    assert _raw_flows(db_path, metrics.deployment_id) == ("", "")

    loaded = await store.get_portfolio_metrics(metrics.deployment_id)
    assert loaded is not None
    assert loaded.deposits_usd is None
    assert loaded.withdrawals_usd is None
    assert loaded.pnl_before_gas is None


@pytest.mark.asyncio
async def test_sqlite_round_trips_measured_zero_flows(store, db_path) -> None:
    metrics = _metrics()
    assert await store.save_portfolio_metrics(metrics) is True
    assert _raw_flows(db_path, metrics.deployment_id) == ("0", "0")

    loaded = await store.get_portfolio_metrics(metrics.deployment_id)
    assert loaded is not None
    assert loaded.deposits_usd == Decimal("0")
    assert loaded.withdrawals_usd == Decimal("0")


@pytest.mark.asyncio
async def test_sqlite_round_trips_measured_nonzero_flows(store, db_path) -> None:
    metrics = _metrics(deposits=Decimal("12.5"), withdrawals=Decimal("3"))
    assert await store.save_portfolio_metrics(metrics) is True
    assert _raw_flows(db_path, metrics.deployment_id) == ("12.5", "3")

    loaded = await store.get_portfolio_metrics(metrics.deployment_id)
    assert loaded is not None
    assert loaded.deposits_usd == Decimal("12.5")
    assert loaded.withdrawals_usd == Decimal("3")


@pytest.mark.asyncio
async def test_sqlite_reads_legacy_zero_row_as_measured_zero(store, db_path) -> None:
    """A pre-sentinel row written straight to the table still reads as 0."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO portfolio_metrics (
                deployment_id, initial_value_usd, initial_timestamp,
                deposits_usd, withdrawals_usd, gas_spent_usd, total_value_usd,
                updated_at
            ) VALUES (?, '4', ?, '0', '0', '0.5', '10', ?)
            """,
            (
                "deployment:legacy000000",
                datetime(2026, 7, 19, tzinfo=UTC).isoformat(),
                datetime(2026, 7, 19, tzinfo=UTC).isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    loaded = await store.get_portfolio_metrics("deployment:legacy000000")
    assert loaded is not None
    assert loaded.deposits_usd == Decimal("0")
    assert loaded.withdrawals_usd == Decimal("0")
    assert loaded.pnl_before_gas == Decimal("6")


@pytest.mark.asyncio
async def test_sqlite_reads_sql_null_as_legacy_measured_zero(store, db_path) -> None:
    """SQL NULL predates the sentinel -> measured 0, matching the PG reader."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO portfolio_metrics (
                deployment_id, initial_value_usd, initial_timestamp,
                deposits_usd, withdrawals_usd, gas_spent_usd, total_value_usd,
                updated_at
            ) VALUES (?, '4', ?, NULL, NULL, '0.5', '10', ?)
            """,
            (
                "deployment:nullrow00000",
                datetime(2026, 7, 19, tzinfo=UTC).isoformat(),
                datetime(2026, 7, 19, tzinfo=UTC).isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    loaded = await store.get_portfolio_metrics("deployment:nullrow00000")
    assert loaded is not None
    assert loaded.deposits_usd == Decimal("0")
    assert loaded.withdrawals_usd == Decimal("0")


@pytest.mark.asyncio
async def test_sqlite_cowrite_seam_persists_unmeasured_flows_as_empty_text(store, db_path) -> None:
    """``save_snapshot_and_metrics`` shares the sentinel with the plain writer."""
    from almanak.framework.portfolio.models import PortfolioSnapshot

    metrics = _metrics(deposits=None, withdrawals=None)
    snapshot = PortfolioSnapshot(
        deployment_id=metrics.deployment_id,
        timestamp=metrics.timestamp,
        total_value_usd=Decimal("10"),
        available_cash_usd=Decimal("10"),
    )
    await store.save_snapshot_and_metrics(snapshot, metrics)

    assert _raw_flows(db_path, metrics.deployment_id) == ("", "")


# ---------------------------------------------------------------------------
# 4. Framework Postgres row reader
# ---------------------------------------------------------------------------


def _pg_row(**overrides) -> dict:
    row = {
        "initial_timestamp": datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
        "initial_value_usd": "4",
        "total_value_usd": "10",
        "deposits_usd": "0",
        "withdrawals_usd": "0",
        "gas_spent_usd": "0.5",
        "positions_text": "[]",
        "cycle_id": "c1",
        "deployment_id": "deployment:abc123def456",
        "execution_mode": "live",
        "is_complete": True,
    }
    row.update(overrides)
    return row


def test_pg_row_reader_maps_empty_string_to_unmeasured() -> None:
    from almanak.framework.state.state_manager import _pg_row_to_portfolio_metrics

    m = _pg_row_to_portfolio_metrics(_pg_row(deposits_usd="", withdrawals_usd=""))
    assert m.deposits_usd is None
    assert m.withdrawals_usd is None
    assert m.pnl_before_gas is None


def test_pg_row_reader_keeps_legacy_zero_and_missing_column_measured() -> None:
    """Only an EXPLICIT '' is unmeasured; NULL / absent stays Decimal("0")."""
    from almanak.framework.state.state_manager import _pg_row_to_portfolio_metrics

    assert _pg_row_to_portfolio_metrics(_pg_row()).deposits_usd == Decimal("0")
    assert _pg_row_to_portfolio_metrics(_pg_row(deposits_usd=None)).deposits_usd == Decimal("0")

    row = _pg_row()
    del row["withdrawals_usd"]
    assert _pg_row_to_portfolio_metrics(row).withdrawals_usd == Decimal("0")


def test_pg_row_reader_parses_measured_values() -> None:
    from almanak.framework.state.state_manager import _pg_row_to_portfolio_metrics

    m = _pg_row_to_portfolio_metrics(_pg_row(deposits_usd="12.5", withdrawals_usd="3"))
    assert m.deposits_usd == Decimal("12.5")
    assert m.withdrawals_usd == Decimal("3")


# ---------------------------------------------------------------------------
# 5. Gateway wire seams
# ---------------------------------------------------------------------------


def test_save_metrics_request_parse_maps_empty_to_unmeasured() -> None:
    from almanak.gateway.proto import gateway_pb2
    from almanak.gateway.services._save_metrics_helpers import parse_metrics_inputs

    request = gateway_pb2.SaveMetricsRequest(
        deployment_id="deployment:abc123def456",
        initial_value_usd="4",
        deposits_usd="",
        withdrawals_usd="",
        gas_spent_usd="0.5",
    )
    inputs = parse_metrics_inputs(request, "deployment:abc123def456")
    assert inputs.deposits_usd is None
    assert inputs.withdrawals_usd is None
    # The non-flow fields keep their "0" coalescing.
    assert inputs.initial_value_usd == Decimal("4")
    assert inputs.gas_spent_usd == Decimal("0.5")


def test_save_metrics_request_parse_keeps_measured_zero() -> None:
    from almanak.gateway.proto import gateway_pb2
    from almanak.gateway.services._save_metrics_helpers import parse_metrics_inputs

    request = gateway_pb2.SaveMetricsRequest(
        deployment_id="deployment:abc123def456",
        initial_value_usd="4",
        deposits_usd="0",
        withdrawals_usd="0",
    )
    inputs = parse_metrics_inputs(request, "deployment:abc123def456")
    assert inputs.deposits_usd == Decimal("0")
    assert inputs.withdrawals_usd == Decimal("0")


def test_pg_upsert_args_write_empty_string_for_unmeasured_flows() -> None:
    from almanak.gateway.proto import gateway_pb2
    from almanak.gateway.services._save_metrics_helpers import (
        ParsedMetricsInputs,
        build_pg_upsert_args,
    )

    now = datetime(2026, 7, 19, 13, 0, tzinfo=UTC)
    inputs = ParsedMetricsInputs(
        deployment_id="deployment:abc123def456",
        initial_value_usd=Decimal("4"),
        deposits_usd=None,
        withdrawals_usd=None,
        gas_spent_usd=Decimal("0.5"),
        timestamp=datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
    )
    args = build_pg_upsert_args(inputs, gateway_pb2.SaveMetricsRequest(), now, Decimal("10"))
    # $4 / $5 in PG_UPSERT_QUERY — the literal "None" must never be written.
    assert args[3] == ""
    assert args[4] == ""


def test_sqlite_metrics_to_proto_emits_empty_string_for_unmeasured_flows() -> None:
    from almanak.gateway.services.state_service import StateServiceServicer as StateService

    data = StateService._sqlite_portfolio_metrics_to_proto(_metrics(deposits=None, withdrawals=None))
    assert data.deposits_usd == ""
    assert data.withdrawals_usd == ""

    measured = StateService._sqlite_portfolio_metrics_to_proto(_metrics(deposits=Decimal("2")))
    assert measured.deposits_usd == "2"
    assert measured.withdrawals_usd == "0"


def test_pg_metrics_to_proto_passes_the_sentinel_through() -> None:
    from almanak.gateway.services.state_service import StateServiceServicer as StateService

    row = _pg_row(deposits_usd="", withdrawals_usd="0")
    row["updated_at"] = datetime(2026, 7, 19, 13, 0, tzinfo=UTC)
    data = StateService._pg_portfolio_metrics_to_proto(row)
    assert data.deposits_usd == ""  # NOT coerced to "0"
    assert data.withdrawals_usd == "0"

    # SQL NULL keeps the historical "0" — legacy rows predate the sentinel.
    null_row = _pg_row(deposits_usd=None)
    null_row["updated_at"] = datetime(2026, 7, 19, 13, 0, tzinfo=UTC)
    assert StateService._pg_portfolio_metrics_to_proto(null_row).deposits_usd == "0"


@pytest.mark.asyncio
async def test_gateway_state_manager_wire_round_trip_preserves_unmeasured(monkeypatch) -> None:
    """save → (fake wire) → get keeps ``None`` as ``None``, never 0 or "None"."""
    from almanak.framework.state.gateway_state_manager import GatewayStateManager
    from almanak.gateway.proto import gateway_pb2
    from almanak.gateway.services.state_service import StateServiceServicer as StateService

    captured: dict[str, gateway_pb2.SaveMetricsRequest] = {}

    class _FakeStateStub:
        def SavePortfolioMetrics(self, request, timeout=None):  # noqa: N802 — gRPC stub name
            captured["request"] = request
            return gateway_pb2.SaveMetricsResponse(success=True)

        def GetPortfolioMetrics(self, request, timeout=None):  # noqa: N802 — gRPC stub name
            saved = captured["request"]
            # Echo the round-trip through the real server-side response builder.
            return StateService._sqlite_portfolio_metrics_to_proto(
                _metrics(
                    deposits=decode_optional_flow(saved.deposits_usd),
                    withdrawals=decode_optional_flow(saved.withdrawals_usd),
                )
            )

    manager = GatewayStateManager.__new__(GatewayStateManager)
    manager._client = SimpleNamespace(state=_FakeStateStub())
    manager._timeout = 5.0

    async def _no_snapshot(_deployment_id):
        return None

    monkeypatch.setattr(manager, "get_latest_snapshot", _no_snapshot, raising=False)

    assert await manager.save_portfolio_metrics(_metrics(deposits=None, withdrawals=None)) is True
    assert captured["request"].deposits_usd == ""
    assert captured["request"].withdrawals_usd == ""

    loaded = await manager.get_portfolio_metrics("deployment:abc123def456")
    assert loaded is not None
    assert loaded.deposits_usd is None
    assert loaded.withdrawals_usd is None


@pytest.mark.asyncio
async def test_gateway_state_manager_wire_round_trip_preserves_measured_zero(monkeypatch) -> None:
    from almanak.framework.state.gateway_state_manager import GatewayStateManager
    from almanak.gateway.proto import gateway_pb2
    from almanak.gateway.services.state_service import StateServiceServicer as StateService

    captured: dict[str, gateway_pb2.SaveMetricsRequest] = {}

    class _FakeStateStub:
        def SavePortfolioMetrics(self, request, timeout=None):  # noqa: N802 — gRPC stub name
            captured["request"] = request
            return gateway_pb2.SaveMetricsResponse(success=True)

        def GetPortfolioMetrics(self, request, timeout=None):  # noqa: N802 — gRPC stub name
            saved = captured["request"]
            return StateService._sqlite_portfolio_metrics_to_proto(
                _metrics(
                    deposits=decode_optional_flow(saved.deposits_usd),
                    withdrawals=decode_optional_flow(saved.withdrawals_usd),
                )
            )

    manager = GatewayStateManager.__new__(GatewayStateManager)
    manager._client = SimpleNamespace(state=_FakeStateStub())
    manager._timeout = 5.0

    async def _no_snapshot(_deployment_id):
        return None

    monkeypatch.setattr(manager, "get_latest_snapshot", _no_snapshot, raising=False)

    assert await manager.save_portfolio_metrics(_metrics()) is True
    assert captured["request"].deposits_usd == "0"

    loaded = await manager.get_portfolio_metrics("deployment:abc123def456")
    assert loaded is not None
    assert loaded.deposits_usd == Decimal("0")
    assert loaded.withdrawals_usd == Decimal("0")


# ---------------------------------------------------------------------------
# 6. Consumers must survive a None flow (PR-C1 does not change their behaviour)
# ---------------------------------------------------------------------------


def test_strat_pnl_leveraged_consumer_does_not_crash_on_none_flows() -> None:
    """``_dec(None)`` → 0 is an accepted stopgap here; PR-C2 owns suppression."""
    from almanak.framework.cli.strat_pnl import _dec

    m = _metrics(deposits=None, withdrawals=None)
    assert _dec(m.deposits_usd) == Decimal("0")
    assert _dec(m.withdrawals_usd) == Decimal("0")


def test_dashboard_quant_aggregation_consumer_does_not_crash_on_none_flows() -> None:
    from almanak.framework.dashboard.quant_aggregations import _to_decimal

    m = _metrics(deposits=None, withdrawals=None)
    assert _to_decimal(m.deposits_usd) == Decimal("0")
    assert _to_decimal(m.withdrawals_usd) == Decimal("0")
