"""VIB-4196 / T10 — `position_reference` JSON shape on `accounting_events`.

Tests the shape contract, the augment-chokepoint stamping rule, the SQLite
column wiring, and the migration backfill for legacy OPEN/CLOSE rows. Pinned
against the PRD §"The `position_reference` shape" canonical fields and the
Day-1 source-semantics clarification (every primitive lands as `"legacy"`
until its cutover ticket flips to `"receipt"` / `"registry"`).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.accounting.lp_accounting import LPAccountingEvent
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LPEventType,
)
from almanak.framework.accounting.position_reference import (
    POSITION_REFERENCE_SOURCES,
    PositionReference,
    build_legacy_position_reference,
)
from almanak.framework.accounting.writer import augment_accounting_payload
from almanak.framework.primitives.taxonomy import (
    TAXONOMY,
    record_for,
)
from almanak.framework.primitives.types import EventKind, Primitive
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


# ---------------------------------------------------------------------------
# D1.S1 — shape exists, frozen, fields complete
# ---------------------------------------------------------------------------
def test_position_reference_frozen_with_required_fields() -> None:
    ref = PositionReference(
        source="legacy",
        primitive="lp",
        accounting_category="lp",
        physical_identity_hash=None,
        semantic_grouping_key=None,
        registry_handle=None,
        grouping_policy_version=None,
        matching_policy_version=None,
    )
    import dataclasses as _dc

    with pytest.raises(_dc.FrozenInstanceError):
        ref.source = "receipt"  # type: ignore[misc]


def test_position_reference_sources_pinned() -> None:
    assert set(POSITION_REFERENCE_SOURCES) == {"receipt", "legacy", "registry"}


def test_position_reference_rejects_unknown_source() -> None:
    with pytest.raises(ValueError, match="source"):
        PositionReference(
            source="recipt",  # typo — must NOT be silently accepted
            primitive="lp",
            accounting_category="lp",
            physical_identity_hash=None,
            semantic_grouping_key=None,
            registry_handle=None,
            grouping_policy_version=None,
            matching_policy_version=None,
        )


def test_position_reference_rejects_empty_primitive() -> None:
    with pytest.raises(ValueError, match="primitive"):
        PositionReference(
            source="legacy",
            primitive="",
            accounting_category="lp",
            physical_identity_hash=None,
            semantic_grouping_key=None,
            registry_handle=None,
            grouping_policy_version=None,
            matching_policy_version=None,
        )


def test_position_reference_rejects_empty_string_hash() -> None:
    """Empty ≠ zero (CLAUDE.md). An empty hash is a parser bug, not a value."""
    with pytest.raises(ValueError, match="physical_identity_hash"):
        PositionReference(
            source="legacy",
            primitive="lp",
            accounting_category="lp",
            physical_identity_hash="",  # rejected
            semantic_grouping_key=None,
            registry_handle=None,
            grouping_policy_version=None,
            matching_policy_version=None,
        )


# ---------------------------------------------------------------------------
# D1.S2 — build_legacy_position_reference contract
# ---------------------------------------------------------------------------
def test_build_legacy_lp_open() -> None:
    ref = build_legacy_position_reference(record_for("LP_OPEN"))
    assert ref.source == "legacy"
    assert ref.primitive == "lp"
    assert ref.accounting_category == "lp"
    assert ref.physical_identity_hash is None
    assert ref.semantic_grouping_key is None
    assert ref.registry_handle is None
    assert ref.grouping_policy_version is None
    assert ref.matching_policy_version is None


def test_build_legacy_lp_close() -> None:
    ref = build_legacy_position_reference(record_for("LP_CLOSE"))
    assert ref.source == "legacy"
    assert ref.primitive == "lp"
    assert ref.accounting_category == "lp"


def test_build_legacy_perp_open() -> None:
    ref = build_legacy_position_reference(record_for("PERP_OPEN"))
    assert ref.primitive == "perp"
    assert ref.accounting_category == "perp"


def test_build_legacy_pendle_lp_carries_distinct_accounting_category() -> None:
    """PENDLE_LP_OPEN: primitive=lp, accounting_category=pendle_lp."""
    ref = build_legacy_position_reference(record_for("PENDLE_LP_OPEN"))
    assert ref.primitive == "lp"
    assert ref.accounting_category == "pendle_lp"


def test_build_legacy_rejects_swap() -> None:
    """SWAP has event_kind=NONE — no position lifecycle."""
    with pytest.raises(ValueError, match="not OPEN or CLOSE"):
        build_legacy_position_reference(record_for("SWAP"))


def test_build_legacy_rejects_collect() -> None:
    """LP_COLLECT_FEES has event_kind=COLLECT — no OPEN/CLOSE."""
    with pytest.raises(ValueError, match="not OPEN or CLOSE"):
        build_legacy_position_reference(record_for("LP_COLLECT_FEES"))


def test_build_legacy_rejects_adjust() -> None:
    """PERP_INCREASE has event_kind=ADJUST."""
    with pytest.raises(ValueError, match="not OPEN or CLOSE"):
        build_legacy_position_reference(record_for("PERP_INCREASE"))


# ---------------------------------------------------------------------------
# D1.S3 — augment chokepoint stamps on OPEN/CLOSE; omits otherwise
# ---------------------------------------------------------------------------
def test_augment_stamps_position_reference_on_lp_open() -> None:
    out = json.loads(
        augment_accounting_payload(json.dumps({"event_type": "LP_OPEN"}), is_live=True)
    )
    assert "position_reference" in out
    pr = out["position_reference"]
    assert pr["source"] == "legacy"
    assert pr["primitive"] == "lp"
    assert pr["accounting_category"] == "lp"
    assert pr["physical_identity_hash"] is None
    assert pr["semantic_grouping_key"] is None
    assert pr["registry_handle"] is None
    assert pr["grouping_policy_version"] is None
    assert pr["matching_policy_version"] is None


def test_augment_stamps_position_reference_on_lp_close() -> None:
    out = json.loads(
        augment_accounting_payload(json.dumps({"event_type": "LP_CLOSE"}), is_live=True)
    )
    assert out["position_reference"]["primitive"] == "lp"


def test_augment_stamps_position_reference_on_perp_open() -> None:
    out = json.loads(
        augment_accounting_payload(json.dumps({"event_type": "PERP_OPEN"}), is_live=True)
    )
    assert out["position_reference"]["primitive"] == "perp"


def test_augment_omits_position_reference_on_swap() -> None:
    out = json.loads(
        augment_accounting_payload(json.dumps({"event_type": "SWAP"}), is_live=True)
    )
    assert "position_reference" not in out


def test_augment_omits_position_reference_on_collect_fees() -> None:
    out = json.loads(
        augment_accounting_payload(
            json.dumps({"event_type": "LP_COLLECT_FEES"}), is_live=True
        )
    )
    assert "position_reference" not in out


def test_augment_omits_position_reference_on_perp_increase() -> None:
    out = json.loads(
        augment_accounting_payload(
            json.dumps({"event_type": "PERP_INCREASE"}), is_live=True
        )
    )
    assert "position_reference" not in out


# ---------------------------------------------------------------------------
# Anti-smuggling — caller-supplied `position_reference` MUST NOT survive
# the augment chokepoint, regardless of branch (CodeRabbit on PR #2211).
# Without this guard, a connector that fabricates `{"position_reference":
# {...}}` in `to_payload_json()` would have its smuggled value pulled into
# the SQLite `position_reference` column on non-OPEN/CLOSE event_kinds and
# unknown-event_type fallbacks — breaking the T10 invariant that the
# writer chokepoint is the ONLY construction site.
# ---------------------------------------------------------------------------
SMUGGLED_REFERENCE = {
    "source": "receipt",
    "primitive": "perp",
    "accounting_category": "perp",
    "physical_identity_hash": "0xdeadbeef",
    "semantic_grouping_key": None,
    "registry_handle": None,
    "grouping_policy_version": None,
    "matching_policy_version": None,
}


def test_augment_strips_smuggled_position_reference_on_swap() -> None:
    """SWAP has no position lifecycle — smuggled reference must be dropped."""
    out = json.loads(
        augment_accounting_payload(
            json.dumps({"event_type": "SWAP", "position_reference": SMUGGLED_REFERENCE}),
            is_live=True,
        )
    )
    assert "position_reference" not in out


def test_augment_strips_smuggled_position_reference_on_collect_fees() -> None:
    """LP_COLLECT_FEES is event_kind=ADJUST — smuggled reference must be dropped."""
    out = json.loads(
        augment_accounting_payload(
            json.dumps(
                {"event_type": "LP_COLLECT_FEES", "position_reference": SMUGGLED_REFERENCE}
            ),
            is_live=True,
        )
    )
    assert "position_reference" not in out


def test_augment_strips_smuggled_position_reference_on_unknown_event_type() -> None:
    """Unknown event_type fallback (paper mode) — smuggled reference must be dropped.

    The fallback path stamps version pairs against Primitive.UTILITY but
    leaves the column NULL. A smuggled key here would survive the augmenter
    and land in the SQLite position_reference column — bypassing the writer
    chokepoint exclusivity.
    """
    out = json.loads(
        augment_accounting_payload(
            json.dumps(
                {"event_type": "ZZ_NOT_A_REAL_EVENT", "position_reference": SMUGGLED_REFERENCE}
            ),
            is_live=False,
        )
    )
    assert "position_reference" not in out


def test_augment_overwrites_smuggled_position_reference_on_lp_open() -> None:
    """Even on the success path, a smuggled reference must be replaced (not merged) with the canonical legacy shape."""
    out = json.loads(
        augment_accounting_payload(
            json.dumps({"event_type": "LP_OPEN", "position_reference": SMUGGLED_REFERENCE}),
            is_live=True,
        )
    )
    pr = out["position_reference"]
    # Canonical legacy shape, NOT the smuggled "receipt" / "0xdeadbeef" values.
    assert pr["source"] == "legacy"
    assert pr["physical_identity_hash"] is None


# ---------------------------------------------------------------------------
# D1.S6 — SQLite end-to-end round trip on LP_OPEN
# ---------------------------------------------------------------------------
def _make_lp_event_for_round_trip(intent_type: str) -> LPAccountingEvent:
    """Build a minimal LPAccountingEvent suitable for save_accounting_event."""
    identity = AccountingIdentity(
        id=f"test-{intent_type}-id",
        deployment_id="TestStrat:abc123",
        strategy_id="TestStrat:abc123",
        cycle_id="cycle-1",
        execution_mode="paper",
        timestamp=datetime(2026, 5, 10, 0, 0, 0, tzinfo=UTC),
        chain="arbitrum",
        protocol="uniswap_v3",
        wallet_address="0x" + "1" * 40,
        ledger_entry_id="ledger-1",
        tx_hash="0xabc",
    )
    event_type = LPEventType(intent_type)
    return LPAccountingEvent(
        identity=identity,
        event_type=event_type,
        position_key=f"pos-{intent_type}",
        pool_address="0x" + "a" * 40,
        token0="USDC",
        token1="WETH",
        amount0=Decimal("100"),
        amount1=Decimal("0.05"),
        lp_token_amount=Decimal("1"),
        cost_basis_usd=Decimal("100"),
        realized_pnl_usd=None,
        fees0_collected=None,
        fees1_collected=None,
        confidence=AccountingConfidence.HIGH,
    )


@pytest.mark.asyncio
async def test_sqlite_round_trip_lp_open(tmp_path) -> None:
    """LP_OPEN writes to SQLite carry a non-NULL position_reference column."""
    db_path = str(tmp_path / "round.db")
    store = SQLiteStore(SQLiteConfig(db_path=db_path))
    await store.initialize()
    try:
        event = _make_lp_event_for_round_trip("LP_OPEN")
        ok = await store.save_accounting_event(event)
        assert ok

        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT position_reference, payload_json FROM accounting_events"
            ).fetchone()
            assert row is not None
            pr_text, payload_text = row
            assert pr_text is not None, "position_reference column was NULL on LP_OPEN row"
            pr = json.loads(pr_text)
            assert pr["source"] == "legacy"
            assert pr["primitive"] == "lp"
            assert pr["accounting_category"] == "lp"
            assert pr["physical_identity_hash"] is None
            # Same JSON is in the payload (denormalized copy).
            payload = json.loads(payload_text)
            assert payload["position_reference"]["source"] == "legacy"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_sqlite_round_trip_lp_collect_fees_leaves_column_null(tmp_path) -> None:
    db_path = str(tmp_path / "collect.db")
    store = SQLiteStore(SQLiteConfig(db_path=db_path))
    await store.initialize()
    try:
        event = _make_lp_event_for_round_trip("LP_COLLECT_FEES")
        await store.save_accounting_event(event)
        with sqlite3.connect(db_path) as conn:
            (pr_text,) = conn.execute(
                "SELECT position_reference FROM accounting_events"
            ).fetchone()
        assert pr_text is None, "LP_COLLECT_FEES row must NOT carry position_reference"
    finally:
        await store.close()


# ---------------------------------------------------------------------------
# D2.M1 — event-kind matrix (sweeps every TAXONOMY row)
# ---------------------------------------------------------------------------
def test_event_kind_matrix() -> None:
    """Every taxonomy row's augment output respects the OPEN/CLOSE-only rule."""
    bearing_kinds = {EventKind.OPEN, EventKind.CLOSE}
    bearing_event_types: list[str] = []
    non_bearing_event_types: list[str] = []
    for intent_type, record in TAXONOMY.items():
        if record.event_kind in bearing_kinds:
            bearing_event_types.append(intent_type)
        else:
            non_bearing_event_types.append(intent_type)

    assert bearing_event_types, "expected at least one OPEN/CLOSE row in TAXONOMY"
    assert non_bearing_event_types, "expected at least one non-OPEN/CLOSE row in TAXONOMY"

    for intent_type in bearing_event_types:
        out = json.loads(
            augment_accounting_payload(
                json.dumps({"event_type": intent_type}), is_live=True
            )
        )
        assert "position_reference" in out, (
            f"OPEN/CLOSE intent {intent_type!r} missing position_reference"
        )
        assert out["position_reference"]["source"] == "legacy"

    for intent_type in non_bearing_event_types:
        out = json.loads(
            augment_accounting_payload(
                json.dumps({"event_type": intent_type}), is_live=True
            )
        )
        assert "position_reference" not in out, (
            f"non-OPEN/CLOSE intent {intent_type!r} carried position_reference"
        )


# ---------------------------------------------------------------------------
# D2.M2 — per-primitive canonical strings
# ---------------------------------------------------------------------------
def test_per_primitive_canonical_strings() -> None:
    """Every primitive that has an OPEN row in TAXONOMY round-trips correctly."""
    seen: set[Primitive] = set()
    for _intent_type, record in TAXONOMY.items():
        if record.event_kind != EventKind.OPEN:
            continue
        ref = build_legacy_position_reference(record)
        assert ref.primitive == record.primitive.value
        assert ref.accounting_category == record.accounting_category.value
        seen.add(record.primitive)

    # Sanity: the canonical primitives we expect must be exercised.
    assert Primitive.LP in seen
    assert Primitive.PERP in seen


# ---------------------------------------------------------------------------
# D2.M3 — schema-rebase forbidden post-cutover (hash stability invariant)
# ---------------------------------------------------------------------------
def test_source_flip_preserves_hash() -> None:
    """The cutover (T12+) only flips `source`; identity fields stay byte-equal."""
    # Day-1: legacy reference for an LP_OPEN row.
    legacy = PositionReference(
        source="legacy",
        primitive="lp",
        accounting_category="lp",
        physical_identity_hash="0xabc123",  # what the cutover will start emitting
        semantic_grouping_key="arbitrum:0xpool",
        registry_handle=None,
        grouping_policy_version="univ3_lp@v1",
        matching_policy_version=3,
    )
    # Cutover: same identity, source flipped.
    receipt = PositionReference(
        source="receipt",
        primitive="lp",
        accounting_category="lp",
        physical_identity_hash="0xabc123",
        semantic_grouping_key="arbitrum:0xpool",
        registry_handle=None,
        grouping_policy_version="univ3_lp@v1",
        matching_policy_version=3,
    )
    legacy_d = legacy.to_dict()
    receipt_d = receipt.to_dict()
    assert legacy_d.pop("source") == "legacy"
    assert receipt_d.pop("source") == "receipt"
    # Every other field is byte-equal.
    assert legacy_d == receipt_d, "source flip MUST NOT mutate identity fields"


# ---------------------------------------------------------------------------
# D3.F2 — paper-mode unknown event_type leaves position_reference NULL
# ---------------------------------------------------------------------------
def test_paper_unknown_event_type_no_reference(caplog) -> None:
    """The augment fallback path (UTILITY) must NOT emit a position_reference."""
    out = json.loads(
        augment_accounting_payload(
            json.dumps({"event_type": "FROBNICATE"}), is_live=False
        )
    )
    assert "position_reference" not in out
    # Versions still stamped at UTILITY — the existing F5.4 contract is intact.
    assert "matching_policy_version" in out


def test_paper_missing_event_type_no_reference() -> None:
    out = json.loads(augment_accounting_payload(json.dumps({}), is_live=False))
    assert "position_reference" not in out


# ---------------------------------------------------------------------------
# D3.F5 — migration backfill populates pre-existing OPEN/CLOSE rows
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_migration_backfill(tmp_path) -> None:
    """Pre-T10 SQLite DBs migrate to populated position_reference on OPEN/CLOSE rows."""
    db_path = str(tmp_path / "legacy.db")
    # Construct a pre-T10-shaped accounting_events table — column-by-column.
    pre_t10_sql = """
    CREATE TABLE accounting_events (
        id TEXT PRIMARY KEY,
        deployment_id TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        cycle_id TEXT NOT NULL,
        execution_mode TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        chain TEXT NOT NULL,
        protocol TEXT NOT NULL,
        wallet_address TEXT NOT NULL,
        event_type TEXT NOT NULL,
        position_key TEXT NOT NULL,
        ledger_entry_id TEXT,
        tx_hash TEXT,
        confidence TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1
    );
    """
    with sqlite3.connect(db_path) as conn:
        conn.executescript(pre_t10_sql)
        # Seed three rows: LP_OPEN (bearer), LP_CLOSE (bearer), SWAP (non-bearer).
        for row_id, event_type in (
            ("row-lp-open", "LP_OPEN"),
            ("row-lp-close", "LP_CLOSE"),
            ("row-swap", "SWAP"),
            ("row-collect", "LP_COLLECT_FEES"),
            ("row-unknown", "FROBNICATE"),
        ):
            conn.execute(
                """
                INSERT INTO accounting_events
                (id, deployment_id, strategy_id, cycle_id, execution_mode,
                 timestamp, chain, protocol, wallet_address, event_type,
                 position_key, ledger_entry_id, tx_hash, confidence,
                 payload_json, schema_version)
                VALUES (?, 'd', 's', 'c', 'paper', '2026-01-01T00:00:00+00:00',
                        'arbitrum', 'p', '0x', ?, 'pos', 'l', '0x', 'HIGH', '{}', 1)
                """,
                (row_id, event_type),
            )
        conn.commit()

    # Run the SDK's migration path. SQLiteStore.initialize() applies the
    # migrations including the new position_reference column + backfill.
    store = SQLiteStore(SQLiteConfig(db_path=db_path))
    await store.initialize()
    try:
        with sqlite3.connect(db_path) as conn:
            rows = {
                row[0]: row[1]
                for row in conn.execute(
                    "SELECT id, position_reference FROM accounting_events"
                ).fetchall()
            }
    finally:
        await store.close()

    # OPEN/CLOSE rows have a populated legacy reference.
    for row_id, expected_primitive in (
        ("row-lp-open", "lp"),
        ("row-lp-close", "lp"),
    ):
        assert rows[row_id] is not None, f"{row_id} missing backfill"
        pr = json.loads(rows[row_id])
        assert pr["source"] == "legacy"
        assert pr["primitive"] == expected_primitive
        assert pr["physical_identity_hash"] is None

    # Non-OPEN/CLOSE + unknown rows stay NULL.
    assert rows["row-swap"] is None
    assert rows["row-collect"] is None
    assert rows["row-unknown"] is None


# ---------------------------------------------------------------------------
# D3.F6 — Empty ≠ zero: legacy hash is None, not ""
# ---------------------------------------------------------------------------
def test_legacy_hash_is_null_not_empty() -> None:
    """Day-1 legacy rows MUST have JSON-null hash, not empty string."""
    out = json.loads(
        augment_accounting_payload(json.dumps({"event_type": "LP_OPEN"}), is_live=True)
    )
    pr = out["position_reference"]
    assert pr["physical_identity_hash"] is None  # JSON null after parse
    # Roundtrip the JSON literal — confirm no "" leaked in.
    raw = json.dumps(pr)
    assert '"physical_identity_hash": null' in raw or '"physical_identity_hash":null' in raw, raw
