"""Unit coverage for the helpers split out of ``SavePortfolioSnapshot``
on PR #2162.

The orchestrator (``SavePortfolioSnapshot``) is exercised end-to-end by
``test_snapshot_identity_roundtrip.py`` and
``test_state_service_characterization.py``. This file pins the
*individual contracts* of the three extracted helpers so a future change
to one of them cannot regress in isolation:

* ``_validate_save_snapshot_payload`` — input shape contract.
* ``_build_sqlite_snapshot``         — wire→domain rebuild contract.
* ``_save_snapshot_postgres``        — PG fetchrow plumbing.

Helpers under test were extracted to drop CRAP=33 on the orchestrator.
Without dedicated coverage, a refactor that quietly changed (for
example) the envelope-vs-legacy branch in ``_build_sqlite_snapshot``
would only be caught by a full round-trip test — too coarse a net.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer

# ──────────────────────────────────────────────────────────────────────────────
# Helpers / fixtures
# ──────────────────────────────────────────────────────────────────────────────


def _make_settings(database_url: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(database_url=database_url, standalone=False)


def _position(label: str = "uniswap_v3:42") -> dict:
    """Minimal-but-valid Position dict that survives ``PortfolioSnapshot.from_dict``."""
    return {
        "position_type": "LP",
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "value_usd": "100",
        "label": label,
    }


def _request(
    *,
    timestamp: int = 1_725_000_000,
    positions_json: bytes = b"[]",
    deployment_id: str = "",
    cycle_id: str = "",
    execution_mode: str = "",
    value_confidence: str = "HIGH",
    total_value_usd: str = "1000.00",
    available_cash_usd: str = "500.00",
    chain: str = "arbitrum",
    iteration_number: int = 1,
) -> gateway_pb2.SaveSnapshotRequest:
    return gateway_pb2.SaveSnapshotRequest(
        strategy_id="Strat:abc",
        timestamp=timestamp,
        iteration_number=iteration_number,
        total_value_usd=total_value_usd,
        available_cash_usd=available_cash_usd,
        value_confidence=value_confidence,
        positions_json=positions_json,
        chain=chain,
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
    )


# ──────────────────────────────────────────────────────────────────────────────
# _validate_save_snapshot_payload
# ──────────────────────────────────────────────────────────────────────────────


class TestValidateSaveSnapshotPayload:
    def test_happy_legacy_list_returns_none(self):
        req = _request(positions_json=b'[{"symbol": "USDC"}]')
        assert StateServiceServicer._validate_save_snapshot_payload(req) is None

    def test_happy_envelope_returns_none(self):
        req = _request(positions_json=b'{"positions": [], "metadata": {}}')
        assert StateServiceServicer._validate_save_snapshot_payload(req) is None

    def test_empty_positions_returns_none(self):
        # Empty bytes → caller will write ``"[]"`` into PG; helper accepts.
        req = _request(positions_json=b"")
        assert StateServiceServicer._validate_save_snapshot_payload(req) is None

    def test_zero_timestamp_returns_error(self):
        req = _request(timestamp=0)
        assert StateServiceServicer._validate_save_snapshot_payload(req) == "timestamp must be positive"

    def test_negative_timestamp_returns_error(self):
        req = _request(timestamp=-1)
        assert StateServiceServicer._validate_save_snapshot_payload(req) == "timestamp must be positive"

    def test_invalid_json_returns_error(self):
        req = _request(positions_json=b"{not-json")
        assert StateServiceServicer._validate_save_snapshot_payload(req) == "positions_json must be valid JSON"

    def test_top_level_string_rejected(self):
        # Valid JSON, wrong shape (string at top level).
        req = _request(positions_json=b'"hello"')
        err = StateServiceServicer._validate_save_snapshot_payload(req)
        assert err == "positions_json must be a list or {positions: list, metadata: object}"

    def test_envelope_missing_positions_rejected(self):
        # Dict at top level but neither ``positions`` (list) nor empty.
        req = _request(positions_json=b'{"foo": "bar"}')
        err = StateServiceServicer._validate_save_snapshot_payload(req)
        # ``positions`` defaults to ``[]`` (passes), ``metadata`` defaults to
        # ``{}`` (passes) — this dict therefore PASSES the envelope check.
        # That documents the (loose) shape the gateway accepts today.
        assert err is None

    def test_envelope_with_non_list_positions_rejected(self):
        req = _request(positions_json=b'{"positions": "not-a-list", "metadata": {}}')
        err = StateServiceServicer._validate_save_snapshot_payload(req)
        assert err == "positions_json must be a list or {positions: list, metadata: object}"

    def test_envelope_with_non_dict_metadata_rejected(self):
        req = _request(positions_json=b'{"positions": [], "metadata": "no"}')
        err = StateServiceServicer._validate_save_snapshot_payload(req)
        assert err == "positions_json must be a list or {positions: list, metadata: object}"

    def test_invalid_utf8_returns_error(self):
        # Non-UTF8 bytes raise UnicodeDecodeError on json.loads(bytes) →
        # treated as malformed JSON.
        req = _request(positions_json=b"\xff\xfe\xfd")
        err = StateServiceServicer._validate_save_snapshot_payload(req)
        assert err == "positions_json must be valid JSON"


# ──────────────────────────────────────────────────────────────────────────────
# _build_sqlite_snapshot
# ──────────────────────────────────────────────────────────────────────────────


class TestBuildSqliteSnapshot:
    @pytest.fixture
    def ts(self) -> datetime:
        return datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)

    def test_bare_request_yields_minimal_snapshot(self, ts: datetime):
        req = _request(positions_json=b"")
        snap = StateServiceServicer._build_sqlite_snapshot("Strat:abc", ts, req)
        assert isinstance(snap, PortfolioSnapshot)
        assert snap.strategy_id == "Strat:abc"
        assert snap.timestamp == ts
        assert snap.total_value_usd == Decimal("1000.00")
        assert snap.available_cash_usd == Decimal("500.00")
        assert snap.value_confidence == ValueConfidence.HIGH
        assert snap.positions == []
        assert snap.deployment_id == ""
        assert snap.cycle_id == ""
        assert snap.execution_mode == ""

    def test_identity_fields_propagate_to_snapshot(self, ts: datetime):
        req = _request(deployment_id="Strat:abc", cycle_id="cycle-7", execution_mode="live")
        snap = StateServiceServicer._build_sqlite_snapshot("Strat:abc", ts, req)
        assert snap.deployment_id == "Strat:abc"
        assert snap.cycle_id == "cycle-7"
        assert snap.execution_mode == "live"

    def test_legacy_list_positions_pass_through(self, ts: datetime):
        req = _request(positions_json=json.dumps([_position("uniswap_v3:42")]).encode())
        snap = StateServiceServicer._build_sqlite_snapshot("Strat:abc", ts, req)
        assert len(snap.positions) == 1
        assert snap.positions[0].label == "uniswap_v3:42"

    def test_envelope_unpacks_positions_and_metadata(self, ts: datetime):
        envelope = {
            "positions": [_position("aave:USDC")],
            "metadata": {"some_meta": "value"},
        }
        req = _request(positions_json=json.dumps(envelope).encode())
        snap = StateServiceServicer._build_sqlite_snapshot("Strat:abc", ts, req)
        assert len(snap.positions) == 1
        assert snap.positions[0].label == "aave:USDC"

    def test_smuggled_cash_split_lifted_from_metadata(self, ts: datetime):
        # VIB-3894 — proto wire is missing deployed_capital_usd /
        # wallet_total_value_usd. They ride in metadata under double-
        # underscore keys; helper MUST lift them onto the rebuilt snapshot.
        envelope = {
            "positions": [],
            "metadata": {
                "__deployed_capital_usd__": "750.00",
                "__wallet_total_value_usd__": "250.00",
            },
        }
        req = _request(positions_json=json.dumps(envelope).encode())
        snap = StateServiceServicer._build_sqlite_snapshot("Strat:abc", ts, req)
        assert snap.deployed_capital_usd == Decimal("750.00")
        assert snap.wallet_total_value_usd == Decimal("250.00")

    def test_smuggled_cash_keys_are_consumed_not_persisted(self, ts: datetime):
        # The double-underscore keys should NOT remain in snapshot_metadata
        # — they're transport-only, not domain data.
        envelope = {
            "positions": [],
            "metadata": {
                "__deployed_capital_usd__": "1.00",
                "__wallet_total_value_usd__": "2.00",
                "real_meta": "kept",
            },
        }
        req = _request(positions_json=json.dumps(envelope).encode())
        snap = StateServiceServicer._build_sqlite_snapshot("Strat:abc", ts, req)
        meta = snap.snapshot_metadata
        assert "__deployed_capital_usd__" not in meta
        assert "__wallet_total_value_usd__" not in meta
        assert meta.get("real_meta") == "kept"

    def test_envelope_token_prices_lifted(self, ts: datetime):
        envelope = {
            "positions": [],
            "metadata": {},
            "token_prices": {"USDC": {"usd": 1.0}},
        }
        req = _request(positions_json=json.dumps(envelope).encode())
        snap = StateServiceServicer._build_sqlite_snapshot("Strat:abc", ts, req)
        assert snap.token_prices == {"USDC": {"usd": 1.0}}

    def test_envelope_wallet_balances_lifted(self, ts: datetime):
        envelope = {
            "positions": [],
            "metadata": {},
            "wallet_balances": [
                {"symbol": "USDC", "balance": "100", "value_usd": "100", "price_usd": "1.0"},
            ],
        }
        req = _request(positions_json=json.dumps(envelope).encode())
        snap = StateServiceServicer._build_sqlite_snapshot("Strat:abc", ts, req)
        assert len(snap.wallet_balances) == 1
        assert snap.wallet_balances[0].symbol == "USDC"
        assert snap.wallet_balances[0].balance == Decimal("100")

    def test_legacy_request_omits_envelope_extras(self, ts: datetime):
        # Legacy list positions ⇒ no token_prices / wallet_balances.
        req = _request(positions_json=json.dumps([_position("x")]).encode())
        snap = StateServiceServicer._build_sqlite_snapshot("Strat:abc", ts, req)
        assert snap.token_prices == {}
        assert snap.wallet_balances == []

    def test_default_value_confidence_when_unset(self, ts: datetime):
        # Empty proto string ⇒ "HIGH" default per the orchestrator contract.
        req = _request(value_confidence="", positions_json=b"")
        snap = StateServiceServicer._build_sqlite_snapshot("Strat:abc", ts, req)
        assert snap.value_confidence == ValueConfidence.HIGH


# ──────────────────────────────────────────────────────────────────────────────
# _save_snapshot_postgres — fetchrow shape contract
# ──────────────────────────────────────────────────────────────────────────────


class TestSaveSnapshotPostgres:
    @pytest.fixture
    def pg_service(self) -> StateServiceServicer:
        svc = StateServiceServicer(_make_settings(database_url="postgres://x/y"))
        svc._initialized = True
        svc._snapshot_pool = MagicMock()
        svc._snapshot_pool_initialized = True
        return svc

    @pytest.mark.asyncio
    async def test_returns_id_from_fetchrow(self, pg_service: StateServiceServicer):
        pg_service._snapshot_fetchrow = AsyncMock(return_value={"id": 4242})
        ts = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
        now = datetime(2026, 5, 7, 12, 0, 1, tzinfo=UTC)
        req = _request(deployment_id="Strat:abc", cycle_id="cycle-7", execution_mode="live")
        snap_id = await pg_service._save_snapshot_postgres("Strat:abc", ts, now, req)
        assert snap_id == 4242

    @pytest.mark.asyncio
    async def test_returns_zero_when_fetchrow_none(self, pg_service: StateServiceServicer):
        # Defensive: ON CONFLICT path SHOULD return id, but a None response
        # must not raise — must fall to ``0`` per the original contract.
        pg_service._snapshot_fetchrow = AsyncMock(return_value=None)
        ts = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
        now = datetime(2026, 5, 7, 12, 0, 1, tzinfo=UTC)
        req = _request()
        snap_id = await pg_service._save_snapshot_postgres("Strat:abc", ts, now, req)
        assert snap_id == 0

    @pytest.mark.asyncio
    async def test_passes_identity_fields_into_query(self, pg_service: StateServiceServicer):
        pg_service._snapshot_fetchrow = AsyncMock(return_value={"id": 1})
        ts = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
        now = datetime(2026, 5, 7, 12, 0, 1, tzinfo=UTC)
        req = _request(deployment_id="Strat:abc", cycle_id="cycle-7", execution_mode="paper")
        await pg_service._save_snapshot_postgres("Strat:abc", ts, now, req)
        args = pg_service._snapshot_fetchrow.await_args.args
        # SQL is args[0], then 12 parameters; identity is the LAST 3.
        assert tuple(args[-3:]) == ("Strat:abc", "cycle-7", "paper")

    @pytest.mark.asyncio
    async def test_omitted_identity_collapses_to_empty_strings(self, pg_service: StateServiceServicer):
        # Wire-side proto3 default is ``""`` — helper passes that through
        # unchanged; the SQL's CASE clause is what preserves prior values.
        pg_service._snapshot_fetchrow = AsyncMock(return_value={"id": 1})
        ts = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
        now = datetime(2026, 5, 7, 12, 0, 1, tzinfo=UTC)
        req = _request()  # all identity fields = ""
        await pg_service._save_snapshot_postgres("Strat:abc", ts, now, req)
        args = pg_service._snapshot_fetchrow.await_args.args
        assert tuple(args[-3:]) == ("", "", "")

    @pytest.mark.asyncio
    async def test_default_value_confidence_passed_when_unset(self, pg_service: StateServiceServicer):
        pg_service._snapshot_fetchrow = AsyncMock(return_value={"id": 1})
        ts = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
        now = datetime(2026, 5, 7, 12, 0, 1, tzinfo=UTC)
        req = _request(value_confidence="")
        await pg_service._save_snapshot_postgres("Strat:abc", ts, now, req)
        args = pg_service._snapshot_fetchrow.await_args.args
        # value_confidence is the 6th param after SQL → args[6]
        # (agent_id, ts, iter, total, available, value_conf)
        assert args[6] == "HIGH"

    @pytest.mark.asyncio
    async def test_empty_positions_json_writes_empty_array_string(self, pg_service: StateServiceServicer):
        pg_service._snapshot_fetchrow = AsyncMock(return_value={"id": 1})
        ts = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
        now = datetime(2026, 5, 7, 12, 0, 1, tzinfo=UTC)
        req = _request(positions_json=b"")
        await pg_service._save_snapshot_postgres("Strat:abc", ts, now, req)
        args = pg_service._snapshot_fetchrow.await_args.args
        # positions_json is param 7 (after SQL)
        assert args[7] == "[]"
