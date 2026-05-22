"""VIB-4278 — SQLiteStore.save_accounting_event wires position_registry lookup.

The state-backend chokepoint (per CLAUDE.md / VIB-3862) is
``SQLiteStore.save_accounting_event``. This test exercises the full chain:
seed a ``position_registry`` row → save a typed accounting event → read back
the ``accounting_events`` row → assert ``position_reference.source ==
'registry'`` with identity fields matching the registry row.

Closes Cell L5_22 of the Accountant Test for primitives that have wired the
registry-mode cutover (UniV3 LP via T12 / VIB-4198).

UAT card: ``docs/internal/uat-cards/VIB-4278.md``.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import tempfile
from datetime import UTC, datetime
from decimal import Decimal

import pytest
import pytest_asyncio

from almanak.framework.accounting.lp_accounting import LPAccountingEvent
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LPEventType,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _insert_registry_row(
    db_path: str,
    *,
    deployment_id: str = "TestStrat:abc123",
    chain: str = "arbitrum",
    primitive: str = "lp",
    accounting_category: str = "lp",
    physical_identity_hash: str = "0xnarrow-leg-hash-deadbeef0000000000000000",
    semantic_grouping_key: str = "arbitrum:0xpool-usdc-weth-500",
    grouping_policy_version: str = "univ3_lp@v1",
    handle: str | None = "leg_narrow",
    status: str = "open",
    opened_tx: str | None = "0xtxOpenNarrow",
    closed_tx: str | None = None,
    matching_policy_version: int = 3,
) -> None:
    """Insert a single ``position_registry`` row directly into the DB."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO position_registry
            (deployment_id, chain, primitive, accounting_category,
             physical_identity_hash, semantic_grouping_key, grouping_policy_version,
             handle, status, payload,
             opened_at_block, opened_tx, closed_at_block, closed_tx,
             last_reconciled_at_block, matching_policy_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                deployment_id,
                chain,
                primitive,
                accounting_category,
                physical_identity_hash,
                semantic_grouping_key,
                grouping_policy_version,
                handle,
                status,
                "{}",
                12345,
                opened_tx,
                23456 if closed_tx else None,
                closed_tx,
                None,
                matching_policy_version,
            ),
        )
        conn.commit()


def _make_lp_event(
    *,
    intent_type: str = "LP_OPEN",
    tx_hash: str = "0xtxOpenNarrow",
    deployment_id: str = "TestStrat:abc123",
    chain: str = "arbitrum",
    event_id: str = "event-narrow-open",
    position_key: str = "lp:arbitrum:0xpool:narrow",
    execution_mode: str = "paper",
) -> LPAccountingEvent:
    """Construct a minimal LPAccountingEvent for the round-trip tests."""
    identity = AccountingIdentity(
        id=event_id,
        deployment_id=deployment_id,
        cycle_id="cycle-vib-4278",
        execution_mode=execution_mode,
        timestamp=datetime(2026, 5, 11, 0, 0, 0, tzinfo=UTC),
        chain=chain,
        protocol="uniswap_v3",
        wallet_address="0x" + "1" * 40,
        ledger_entry_id="ledger-vib-4278",
        tx_hash=tx_hash,
    )
    return LPAccountingEvent(
        identity=identity,
        event_type=LPEventType(intent_type),
        position_key=position_key,
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def initialized_store():
    """Initialized SQLiteStore with empty DB. Yields (store, db_path)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "vib4278.db")
        store = SQLiteStore(SQLiteConfig(db_path=db_path))
        await store.initialize()
        try:
            yield store, db_path
        finally:
            await store.close()


# ---------------------------------------------------------------------------
# D1 — Correctness
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_lp_open_writes_source_registry_when_registry_row_exists(
    initialized_store,
) -> None:
    """F6 — full chain: registry seed → save_accounting_event → source='registry'."""
    store, db_path = initialized_store
    _insert_registry_row(db_path)
    ok = await store.save_accounting_event(_make_lp_event())
    assert ok

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT position_reference, payload_json FROM accounting_events"
        ).fetchone()
    pr_text, payload_text = row
    assert pr_text is not None
    pr = json.loads(pr_text)
    assert pr["source"] == "registry"
    assert pr["primitive"] == "lp"
    assert pr["accounting_category"] == "lp"
    assert pr["physical_identity_hash"] == "0xnarrow-leg-hash-deadbeef0000000000000000"
    assert pr["semantic_grouping_key"] == "arbitrum:0xpool-usdc-weth-500"
    assert pr["registry_handle"] == "leg_narrow"
    assert pr["grouping_policy_version"] == "univ3_lp@v1"
    assert pr["matching_policy_version"] == 3

    # Payload column carries the same JSON (denormalized copy).
    payload = json.loads(payload_text)
    assert payload["position_reference"]["source"] == "registry"


@pytest.mark.asyncio
async def test_lp_close_writes_source_registry_on_matching_closed_row(
    initialized_store,
) -> None:
    """CLOSE event matches closed_tx; same chain end-to-end."""
    store, db_path = initialized_store
    _insert_registry_row(
        db_path,
        physical_identity_hash="0xclosing-hash-aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        status="closed",
        opened_tx="0xtxOpenSomewhere",
        closed_tx="0xtxCloseNarrow",
    )
    event = _make_lp_event(
        intent_type="LP_CLOSE",
        tx_hash="0xtxCloseNarrow",
        event_id="event-narrow-close",
        position_key="lp:arbitrum:0xpool:narrow",
    )
    await store.save_accounting_event(event)

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    assert pr["source"] == "registry"
    assert pr["physical_identity_hash"] == "0xclosing-hash-aaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert pr["registry_handle"] == "leg_narrow"


@pytest.mark.asyncio
async def test_lp_open_without_registry_row_falls_back_to_legacy(
    initialized_store,
) -> None:
    """F1 — empty registry → event lands cleanly with source='legacy'."""
    store, db_path = initialized_store
    # No registry row seeded.
    await store.save_accounting_event(_make_lp_event())

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    assert pr["source"] == "legacy"
    assert pr["physical_identity_hash"] is None
    assert pr["registry_handle"] is None


@pytest.mark.asyncio
async def test_lp_open_with_non_matching_tx_falls_back_to_legacy(
    initialized_store,
) -> None:
    """F1 — registry has a row but the event's tx_hash doesn't match → legacy."""
    store, db_path = initialized_store
    _insert_registry_row(db_path, opened_tx="0xtxDifferent")
    await store.save_accounting_event(_make_lp_event(tx_hash="0xtxOpenNarrow"))

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    assert pr["source"] == "legacy"


@pytest.mark.asyncio
async def test_lp_open_with_empty_tx_hash_falls_back_to_legacy(
    initialized_store,
) -> None:
    """F1 — empty tx_hash skips the lookup (no possible match)."""
    store, db_path = initialized_store
    _insert_registry_row(db_path)
    await store.save_accounting_event(_make_lp_event(tx_hash=""))

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    assert pr["source"] == "legacy"


# ---------------------------------------------------------------------------
# D2 — Variance / scalability (lp_dual is the load-bearing case)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_lp_dual_two_legs_each_get_their_own_registry_reference(
    initialized_store,
) -> None:
    """Closes lp_dual L5_22 cell — two LP_OPEN events, two distinct registry rows."""
    store, db_path = initialized_store

    # Seed two registry rows (narrow + wide).
    _insert_registry_row(
        db_path,
        physical_identity_hash="0xnarrow-leg-hash" + "0" * 32,
        handle="leg_narrow",
        opened_tx="0xtxNarrow",
        semantic_grouping_key="arbitrum:0xpool-usdc-weth-500:narrow",
    )
    _insert_registry_row(
        db_path,
        physical_identity_hash="0xwide-leg-hash" + "0" * 34,
        handle="leg_wide",
        opened_tx="0xtxWide",
        semantic_grouping_key="arbitrum:0xpool-usdc-weth-500:wide",
    )

    narrow_event = _make_lp_event(
        intent_type="LP_OPEN",
        tx_hash="0xtxNarrow",
        event_id="event-narrow",
        position_key="lp:arbitrum:0xpool:narrow",
    )
    wide_event = _make_lp_event(
        intent_type="LP_OPEN",
        tx_hash="0xtxWide",
        event_id="event-wide",
        position_key="lp:arbitrum:0xpool:wide",
    )
    await store.save_accounting_event(narrow_event)
    await store.save_accounting_event(wide_event)

    with sqlite3.connect(db_path) as conn:
        rows = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT id, position_reference FROM accounting_events"
            ).fetchall()
        }
    assert len(rows) == 2

    narrow_pr = json.loads(rows["event-narrow"])
    wide_pr = json.loads(rows["event-wide"])

    assert narrow_pr["source"] == "registry"
    assert narrow_pr["registry_handle"] == "leg_narrow"
    assert narrow_pr["physical_identity_hash"].startswith("0xnarrow-leg-hash")

    assert wide_pr["source"] == "registry"
    assert wide_pr["registry_handle"] == "leg_wide"
    assert wide_pr["physical_identity_hash"].startswith("0xwide-leg-hash")

    # Crucial cross-check — each leg points to ITS own registry row, not
    # the other's. Without VIB-4278 both events carry source='legacy';
    # without the (tx_hash) match-key correctness, narrow could get
    # wide's hash. Both regressions caught here.
    assert narrow_pr["physical_identity_hash"] != wide_pr["physical_identity_hash"]
    assert narrow_pr["semantic_grouping_key"] != wide_pr["semantic_grouping_key"]


@pytest.mark.asyncio
async def test_registry_row_with_null_handle_emits_registry_with_null_handle(
    initialized_store,
) -> None:
    """Auto-mode registry rows write source='registry' with registry_handle=null."""
    store, db_path = initialized_store
    _insert_registry_row(db_path, handle=None)
    await store.save_accounting_event(_make_lp_event())

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    assert pr["source"] == "registry"
    assert pr["registry_handle"] is None
    # Identity fields still required.
    assert pr["physical_identity_hash"] is not None


# ---------------------------------------------------------------------------
# D3 — Robustness
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_chain_mismatch_falls_back_to_legacy(initialized_store) -> None:
    """Defense — a row on a different chain MUST NOT match."""
    store, db_path = initialized_store
    _insert_registry_row(db_path, chain="ethereum")  # registry row on ethereum
    await store.save_accounting_event(_make_lp_event(chain="arbitrum"))  # event on arbitrum

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    assert pr["source"] == "legacy"


@pytest.mark.asyncio
async def test_primitive_mismatch_falls_back_to_legacy(initialized_store) -> None:
    """A perp row in registry MUST NOT match an LP event with the same tx_hash."""
    store, db_path = initialized_store
    _insert_registry_row(db_path, primitive="perp", accounting_category="perp")
    await store.save_accounting_event(_make_lp_event())  # LP_OPEN, primitive='lp'

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    assert pr["source"] == "legacy"


@pytest.mark.asyncio
async def test_accounting_category_mismatch_falls_back_to_legacy(initialized_store) -> None:
    """A pendle_lp row MUST NOT match a UniV3 lp event with the same primitive+tx.

    Both share Primitive='lp' but have distinct AccountingCategory values
    ('lp' vs 'pendle_lp'). The lookup keys on accounting_category to
    prevent stamping a Pendle row's identity onto a UniV3 event when a
    single tx happens to touch both legs. CodeRabbit PR #2236 round 2.
    """
    store, db_path = initialized_store
    # Seed a Pendle LP row that shares primitive='lp' but differs on category.
    _insert_registry_row(
        db_path,
        primitive="lp",
        accounting_category="pendle_lp",
    )
    # Save a UniV3 LP event (accounting_category='lp').
    await store.save_accounting_event(_make_lp_event())

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    assert pr["source"] == "legacy", (
        "Cross-category lookup must not stamp source='registry'; "
        "AccountingCategory mismatch (lp vs pendle_lp) on the same Primitive='lp' "
        "would silently lose the L5_22 join key."
    )


@pytest.mark.asyncio
async def test_deployment_id_mismatch_falls_back_to_legacy(initialized_store) -> None:
    """Different deployment_id ⇒ no match (multi-tenant safety)."""
    store, db_path = initialized_store
    _insert_registry_row(db_path, deployment_id="OtherStrat:xyz")
    await store.save_accounting_event(_make_lp_event(deployment_id="TestStrat:abc123"))

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    assert pr["source"] == "legacy"


@pytest.mark.asyncio
async def test_collect_fees_row_position_reference_stays_null(
    initialized_store,
) -> None:
    """LP_COLLECT_FEES is event_kind=COLLECT — position_reference column stays NULL
    even if a registry row exists for the same tx_hash.
    """
    store, db_path = initialized_store
    _insert_registry_row(db_path, opened_tx="0xtxCollect")
    event = _make_lp_event(
        intent_type="LP_COLLECT_FEES",
        tx_hash="0xtxCollect",
        event_id="event-collect",
        position_key="lp:arbitrum:0xpool:narrow",
    )
    await store.save_accounting_event(event)

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    # Collect fees rows carry no position_reference per blueprint 27 §3.6.
    assert pr_text is None


@pytest.mark.asyncio
async def test_event_in_paper_mode_with_no_registry_lands_cleanly(
    initialized_store,
) -> None:
    """Smoke — legacy primitives (no registry write) keep working unchanged."""
    store, db_path = initialized_store
    await store.save_accounting_event(_make_lp_event(execution_mode="paper"))
    with sqlite3.connect(db_path) as conn:
        rows = list(
            conn.execute("SELECT id, position_reference FROM accounting_events")
        )
    assert len(rows) == 1
    assert rows[0][1] is not None  # position_reference column populated (legacy ref)
    pr = json.loads(rows[0][1])
    assert pr["source"] == "legacy"


@pytest.mark.asyncio
async def test_multiple_open_rows_same_tx_falls_back_to_legacy(
    initialized_store, caplog: pytest.LogCaptureFixture
) -> None:
    """F8 — when two rows share opened_tx (multi-position-in-one-tx), fall back to legacy.

    CodeRabbit PR #2236 round 2 reclassified the multi-row case from
    "pick first deterministically" to "fall back to legacy + WARN".
    Picking the first row would stamp ONE position's identity
    (``physical_identity_hash`` / ``handle``) onto BOTH accounting
    events, losing the L5_22 join key silently. The durable fix is to
    thread ``registry_handle`` into the lookup key so each leg joins
    to its own row.
    """
    import logging

    store, db_path = initialized_store
    # Two rows with same opened_tx but different physical_identity_hash —
    # simulates a batched LP_OPEN that mints two NFTs in one tx, before
    # the registry_handle-aware lookup lands.
    _insert_registry_row(
        db_path,
        physical_identity_hash="0xa-hash" + "0" * 41,
        handle="leg_a",
        opened_tx="0xtxShared",
    )
    _insert_registry_row(
        db_path,
        physical_identity_hash="0xb-hash" + "0" * 41,
        handle="leg_b",
        opened_tx="0xtxShared",
    )
    with caplog.at_level(logging.WARNING, logger="almanak.framework.state.backends.sqlite"):
        await store.save_accounting_event(_make_lp_event(tx_hash="0xtxShared"))

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    # Multi-row → legacy fallback, NOT a registry stamp on the wrong leg.
    assert pr["source"] == "legacy"
    assert pr["physical_identity_hash"] is None
    assert pr["registry_handle"] is None
    # WARN log surfaced so an operator can audit / file the
    # multi-position follow-up.
    assert any(
        "rows matched" in r.message and "falling back to legacy" in r.message
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# D2 — Case-insensitive tx_hash matching (CodeRabbit major / PR #2236)
#
# EVM tx hashes are 32-byte hex blobs but their *string* representation can
# arrive in either case depending on the producer (some RPCs lowercase,
# some checksum-uppercase the prefix-zero nibbles in addresses, some
# normalize on write). The registry side and the event side can disagree
# silently — a case-sensitive ``=`` would miss the row and stamp
# ``source="legacy"`` even though the data is present.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_uppercase_event_tx_matches_lowercase_registry_tx(
    initialized_store,
) -> None:
    """Event tx in UPPERCASE must still match a registry row stored lowercase."""
    store, db_path = initialized_store
    _insert_registry_row(db_path, opened_tx="0xtxopennarrow")  # registry lowercase
    await store.save_accounting_event(_make_lp_event(tx_hash="0xTXOPENNARROW"))

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    assert pr["source"] == "registry", (
        "Case-insensitive lookup regression — uppercase event tx fell back to legacy"
    )
    assert pr["registry_handle"] == "leg_narrow"


@pytest.mark.asyncio
async def test_lowercase_event_tx_matches_mixed_case_registry_tx(
    initialized_store,
) -> None:
    """Event tx lowercase must still match a registry row stored MixedCase.

    Mirror of the test above for the opposite write/read casing pair.
    """
    store, db_path = initialized_store
    _insert_registry_row(db_path, opened_tx="0xTxOpenNarrow")  # registry MixedCase
    await store.save_accounting_event(_make_lp_event(tx_hash="0xtxopennarrow"))

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    assert pr["source"] == "registry", (
        "Case-insensitive lookup regression — lowercase event tx fell back to legacy"
    )
    assert pr["registry_handle"] == "leg_narrow"


@pytest.mark.asyncio
async def test_close_event_uppercase_tx_matches_lowercase_closed_tx(
    initialized_store,
) -> None:
    """Case-insensitive lookup must apply to CLOSE→closed_tx too, not just OPEN."""
    store, db_path = initialized_store
    _insert_registry_row(
        db_path,
        physical_identity_hash="0xclosing-hash-aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        status="closed",
        opened_tx="0xtxopensomewhere",
        closed_tx="0xtxclosenarrow",  # registry lowercase
    )
    event = _make_lp_event(
        intent_type="LP_CLOSE",
        tx_hash="0xTXCLOSENARROW",  # event uppercase
        event_id="event-close-case",
    )
    await store.save_accounting_event(event)

    with sqlite3.connect(db_path) as conn:
        (pr_text,) = conn.execute(
            "SELECT position_reference FROM accounting_events"
        ).fetchone()
    pr = json.loads(pr_text)
    assert pr["source"] == "registry"
    assert pr["registry_handle"] == "leg_narrow"


# ---------------------------------------------------------------------------
# D3 — Silent-error backstop: confirm a future refactor cannot drop the wiring
# without a test failing.
# ---------------------------------------------------------------------------
def test_sqlite_store_exposes_build_registry_lookup_for_event() -> None:
    """If a future refactor renames / drops the helper this test catches it."""
    assert hasattr(SQLiteStore, "_build_registry_lookup_for_event")


@pytest.mark.asyncio
async def test_save_accounting_event_invokes_lookup_helper(
    initialized_store, monkeypatch
) -> None:
    """Confirms save_accounting_event actually calls _build_registry_lookup_for_event."""
    store, _db_path = initialized_store
    calls: list[dict] = []
    real_builder = store._build_registry_lookup_for_event  # type: ignore[attr-defined]

    def tracking_builder(**kwargs):
        calls.append(kwargs)
        return real_builder(**kwargs)

    monkeypatch.setattr(store, "_build_registry_lookup_for_event", tracking_builder)
    await store.save_accounting_event(_make_lp_event())
    assert len(calls) == 1
    assert calls[0]["deployment_id"] == "TestStrat:abc123"
    assert calls[0]["chain"] == "arbitrum"
    assert calls[0]["tx_hash"] == "0xtxOpenNarrow"
