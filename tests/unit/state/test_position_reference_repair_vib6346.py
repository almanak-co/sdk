"""VIB-6346 — deferred-identity ``position_reference`` join repair.

Blueprint 27 §10.6a claims the registry-lookup augmentation closes L5_22
because "registry rows and augmented accounting events now reconcile by
construction **at write time**". That holds only when the registry row exists
BEFORE the accounting event is written. On a two-phase (async-keeper) venue it
does not — GMX V2 does not surface the venue ``positionKey`` at order
submission, so the Phase-1 ``PERP_OPEN`` / ``PERP_CLOSE`` accounting rows land
with ``source="legacy"`` and the registry row only arrives when the keeper
settles (VIB-3872 §3 D2, ``perp_settlement_commit._complete_registry``).

The repair closes the join from the side that learned the identity LAST: it
runs inside :meth:`SQLiteStore.save_ledger_and_registry_atomic`'s transaction —
the single registry writer (blueprint 28 §4.1) — so it covers every registry
producer, including ones nobody has enumerated. That generality is deliberate:
VIB-6287's ground-truth section records that a producer census for this exact
row was wrong twice in the same direction, and asks for a check that holds when
an unenumerated producer appears rather than a per-producer patch.

The end-to-end proof against the audited mainnet bundle (L5_22 FAIL → PASS,
plus its permanent negative control) lives in
``tests/unit/accounting/test_accountant_cell_22.py``. This file owns the
transactional contract.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.accounting.writer import restamp_position_reference
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.ledger_registry_mode import LedgerRegistrySaveMode

_DEPLOYMENT = "deployment:926e7ed624d4"
_CHAIN = "arbitrum"
_OPEN_TX = "0x04aa84ba9382569b2372bce614e2a8d6dc5f769020f8fb243c7943bd0cecdeec"
_CLOSE_TX = "0x9eb5335b114844975e7382f68df5a08d48b9631ed8f1d92dae2ca15abd883647"
_PHID = "0x4aa383f59c6dd6ceacf3180ffe3b7574d852af5ce0d329e6bb2b1fc1d1f3a715"
_SGK = "arbitrum:gmx_v2:0xe92af3bc8204c3de65716e47150b9b50483d33497dbe27f6e260dbd83bbdcbe2"

_LEGACY_REFERENCE = {
    "accounting_category": "perp",
    "grouping_policy_version": None,
    "matching_policy_version": None,
    "physical_identity_hash": None,
    "primitive": "perp",
    "registry_handle": None,
    "semantic_grouping_key": None,
    "source": "legacy",
}


class _StubLedger:
    """Minimal ledger stand-in for ``mode='registry_reconciliation'``.

    That mode skips the ledger write entirely (blueprint 28 §4.5 rule 1) but the
    signature still requires the argument, so only ``id`` is ever read.
    """

    id = "ledger-stub"


async def _store(db_path: Path) -> SQLiteStore:
    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()
    return store


def _insert_phase1_event(
    conn: sqlite3.Connection,
    *,
    event_id: str,
    event_type: str,
    tx_hash: str,
    execution_mode: str = "live",
    chain: str = _CHAIN,
    position_key: str = "perp:gmx_v2:arbitrum:0xcea2bec0:eth/usd",
) -> None:
    """Insert a Phase-1 accounting row exactly as the submission lane leaves it.

    ``position_reference`` carries the legacy shape with a NULL
    ``physical_identity_hash`` — the state the mainnet bundle was measured in.
    """
    payload = {"event_type": event_type, "position_key": position_key}
    payload["position_reference"] = dict(_LEGACY_REFERENCE)
    conn.execute(
        """
        INSERT INTO accounting_events
        (id, deployment_id, cycle_id, execution_mode, timestamp, chain, protocol,
         wallet_address, event_type, position_key, ledger_entry_id, tx_hash,
         confidence, payload_json, schema_version, position_reference)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            _DEPLOYMENT,
            "cycle-1",
            execution_mode,
            "2026-08-05T02:23:01+00:00",
            chain,
            "gmx_v2",
            "0xcea2bec033628e2cbe3c51da056d0efbc9097021",
            event_type,
            position_key,
            "ledger-1",
            tx_hash,
            "ESTIMATED",
            json.dumps(payload, sort_keys=True),
            1,
            json.dumps(_LEGACY_REFERENCE, sort_keys=True),
        ),
    )
    conn.commit()


def _perp_registry_row(*, status: str = "closed", closed_tx: str | None = _CLOSE_TX) -> RegistryRow:
    """The row ``perp_settlement_commit._complete_registry`` builds at keeper settlement."""
    return RegistryRow(
        deployment_id=_DEPLOYMENT,
        chain=_CHAIN,
        primitive=Primitive.PERP,
        accounting_category=AccountingCategory.PERP,
        physical_identity_hash=_PHID,
        semantic_grouping_key=_SGK,
        grouping_policy_version="perp@v1",
        status=status,  # type: ignore[arg-type]
        payload={"protocol": "gmx_v2", "source": "settlement_reconciler"},
        matching_policy_version=1,
        opened_at_block=1,
        opened_tx=_OPEN_TX,
        closed_at_block=2 if status != "open" else None,
        closed_tx=closed_tx if status != "open" else None,
    )


def _reference(conn: sqlite3.Connection, event_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT position_reference FROM accounting_events WHERE id = ?", (event_id,)).fetchone()
    raw = row[0] if row else None
    return json.loads(raw) if raw else None


def _payload_reference(conn: sqlite3.Connection, event_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT payload_json FROM accounting_events WHERE id = ?", (event_id,)).fetchone()
    return json.loads(row[0]).get("position_reference") if row else None


# =============================================================================
# The repair fires, on both anchors
# =============================================================================


@pytest.mark.asyncio
async def test_repair_repoints_both_open_and_close_events(tmp_path: Path) -> None:
    """The settlement-time registry write repairs the Phase-1 rows it now identifies.

    Reproduces the exact production ordering: accounting events written FIRST
    (legacy, phid=NULL), registry row written SECOND.
    """
    db = tmp_path / "state.db"
    store = await _store(db)
    conn = store._conn
    assert conn is not None
    _insert_phase1_event(conn, event_id="ev-open", event_type="PERP_OPEN", tx_hash=_OPEN_TX)
    _insert_phase1_event(conn, event_id="ev-close", event_type="PERP_CLOSE", tx_hash=_CLOSE_TX)

    # Pre-condition: this is genuinely the broken state (liveness of the setup).
    assert _reference(conn, "ev-open")["physical_identity_hash"] is None
    assert _reference(conn, "ev-close")["physical_identity_hash"] is None

    await store.save_ledger_and_registry_atomic(
        _StubLedger(), _perp_registry_row(), None, mode=LedgerRegistrySaveMode.REGISTRY_RECONCILIATION
    )

    for event_id in ("ev-open", "ev-close"):
        ref = _reference(conn, event_id)
        assert ref is not None, event_id
        assert ref["source"] == "registry", event_id
        assert ref["physical_identity_hash"] == _PHID, event_id
        assert ref["semantic_grouping_key"] == _SGK, event_id
        assert ref["grouping_policy_version"] == "perp@v1", event_id
        assert ref["matching_policy_version"] == 1, event_id
        # payload_json is canonical; the column is a denormalized copy. Both
        # must move together or they manufacture the drift the column comment
        # forbids (sqlite.py accounting_events schema note, VIB-4196 / T10).
        assert _payload_reference(conn, event_id) == ref, event_id


@pytest.mark.asyncio
async def test_repair_does_not_touch_non_open_close_rows(tmp_path: Path) -> None:
    """PERP_SETTLEMENT has event_kind NONE — it carries no position pointer.

    The keeper-settlement row shares the position but not the lifecycle; stamping
    it would invent a CLOSE the taxonomy does not recognise.
    """
    db = tmp_path / "state.db"
    store = await _store(db)
    conn = store._conn
    assert conn is not None
    conn.execute(
        """
        INSERT INTO accounting_events
        (id, deployment_id, cycle_id, execution_mode, timestamp, chain, protocol,
         wallet_address, event_type, position_key, ledger_entry_id, tx_hash,
         confidence, payload_json, schema_version, position_reference)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            "ev-settle",
            _DEPLOYMENT,
            "cycle-1",
            "live",
            "2026-08-05T02:35:43+00:00",
            _CHAIN,
            "gmx_v2",
            "0xcea2bec0",
            "PERP_SETTLEMENT",
            "0xe92af3bc",
            "ledger-1",
            _CLOSE_TX,
            "HIGH",
            json.dumps({"event_type": "PERP_SETTLEMENT"}, sort_keys=True),
            1,
        ),
    )
    conn.commit()

    await store.save_ledger_and_registry_atomic(
        _StubLedger(), _perp_registry_row(), None, mode=LedgerRegistrySaveMode.REGISTRY_RECONCILIATION
    )

    assert _reference(conn, "ev-settle") is None


@pytest.mark.asyncio
async def test_repair_refuses_when_one_tx_has_two_same_category_events(tmp_path: Path) -> None:
    """Event-side ambiguity safeguard — the mirror of the registry-side one.

    Found by Codex in the panel review of PR #3609. One tx closing TWO positions
    of the same (primitive, accounting_category) writes its two registry rows via
    two separate calls to this writer. After the FIRST call only one row exists,
    so the registry-side safeguard in ``_build_registry_lookup_for_event`` sees
    exactly one match and reports no ambiguity — but there are two accounting
    events and nothing says which belongs to which position.

    Without the event-side guard the repair stamps BOTH with the first row's
    identity, and the no-downgrade rule then makes that guess permanent: when the
    second registry row lands, the registry-side safeguard returns None, so the
    wrong pointer is never revisited. A wrong hash is strictly worse than a null
    one (CLAUDE.md "Empty ≠ Zero").

    Negative control: delete the ``len(matching) > 1`` guard and this test fails
    with both events carrying ``_PHID``.
    """
    db = tmp_path / "state.db"
    store = await _store(db)
    conn = store._conn
    assert conn is not None
    # Two perp CLOSE events sharing ONE tx — two positions closed in one tx.
    _insert_phase1_event(
        conn,
        event_id="ev-close-a",
        event_type="PERP_CLOSE",
        tx_hash=_CLOSE_TX,
        position_key="perp:gmx_v2:arbitrum:0xcea2bec0:eth/usd",
    )
    _insert_phase1_event(
        conn,
        event_id="ev-close-b",
        event_type="PERP_CLOSE",
        tx_hash=_CLOSE_TX,
        position_key="perp:gmx_v2:arbitrum:0xcea2bec0:btc/usd",
    )

    # Only the FIRST position's registry row lands. The registry-side safeguard
    # cannot fire — there is genuinely only one row to find.
    await store.save_ledger_and_registry_atomic(
        _StubLedger(), _perp_registry_row(), None, mode=LedgerRegistrySaveMode.REGISTRY_RECONCILIATION
    )

    for event_id in ("ev-close-a", "ev-close-b"):
        ref = _reference(conn, event_id)
        assert ref is not None, event_id
        assert ref["physical_identity_hash"] is None, (
            f"{event_id} was stamped with a GUESSED identity — the repair cannot know "
            f"which of two same-category events belongs to this registry row"
        )
        assert ref["source"] == "legacy", event_id


@pytest.mark.asyncio
async def test_repair_ignores_events_of_a_different_primitive_on_the_same_tx(tmp_path: Path) -> None:
    """A perp registry row must not stamp an LP event that shares the tx hash."""
    db = tmp_path / "state.db"
    store = await _store(db)
    conn = store._conn
    assert conn is not None
    _insert_phase1_event(conn, event_id="ev-lp", event_type="LP_CLOSE", tx_hash=_CLOSE_TX)

    await store.save_ledger_and_registry_atomic(
        _StubLedger(), _perp_registry_row(), None, mode=LedgerRegistrySaveMode.REGISTRY_RECONCILIATION
    )

    assert _reference(conn, "ev-lp")["physical_identity_hash"] is None


# =============================================================================
# Idempotency
# =============================================================================


@pytest.mark.asyncio
async def test_repair_is_idempotent_and_issues_no_write_when_already_correct(tmp_path: Path) -> None:
    """A second registry write re-points nothing.

    The ``restamp_position_reference`` early-exit is what keeps the repair free
    for every primitive that already stamps ``source="registry"`` at Phase-1
    write time (LP, lending, Pendle) — they never reach an UPDATE.
    """
    db = tmp_path / "state.db"
    store = await _store(db)
    conn = store._conn
    assert conn is not None
    _insert_phase1_event(conn, event_id="ev-close", event_type="PERP_CLOSE", tx_hash=_CLOSE_TX)

    row = _perp_registry_row()
    await store.save_ledger_and_registry_atomic(
        _StubLedger(), row, None, mode=LedgerRegistrySaveMode.REGISTRY_RECONCILIATION
    )
    first = _reference(conn, "ev-close")

    repaired = store._repair_position_references_for_registry_row(
        conn,
        deployment_id=_DEPLOYMENT,
        chain=_CHAIN,
        primitive="perp",
        accounting_category="perp",
        physical_identity_hash=_PHID,
    )
    assert repaired == 0, "an already-correct pointer must not be rewritten"
    assert _reference(conn, "ev-close") == first


@pytest.mark.asyncio
async def test_close_side_registry_write_also_repairs_the_open_event(tmp_path: Path) -> None:
    """The anchors come from the PERSISTED row, not the caller's RegistryRow.

    Regression guard for a real gap in the first cut of this fix.
    ``perp_settlement_commit._complete_registry`` writes the anchors ONE AT A
    TIME — ``opened_tx`` only on the open fill, ``closed_tx`` only on the close
    fill — and the UPSERT ``COALESCE``-s them. Reading the caller's row at close
    settlement would repair the CLOSE event and leave the OPEN event
    ``source="legacy"`` forever, closing half the join. L5_22 would still PASS
    (it only scores CLOSE), so nothing else in the suite would catch it.
    """
    db = tmp_path / "state.db"
    store = await _store(db)
    conn = store._conn
    assert conn is not None
    _insert_phase1_event(conn, event_id="ev-open", event_type="PERP_OPEN", tx_hash=_OPEN_TX)
    _insert_phase1_event(conn, event_id="ev-close", event_type="PERP_CLOSE", tx_hash=_CLOSE_TX)

    # Open fill: status='open', opened_tx only. No CLOSE event to repair yet.
    open_row = _perp_registry_row(status="open")
    await store.save_ledger_and_registry_atomic(
        _StubLedger(), open_row, None, mode=LedgerRegistrySaveMode.REGISTRY_RECONCILIATION
    )
    assert _reference(conn, "ev-open")["physical_identity_hash"] == _PHID

    # Close fill: exactly what _complete_registry passes — closed_tx set,
    # opened_tx left None because the UPSERT preserves the open-side anchors.
    close_row = RegistryRow(
        deployment_id=_DEPLOYMENT,
        chain=_CHAIN,
        primitive=Primitive.PERP,
        accounting_category=AccountingCategory.PERP,
        physical_identity_hash=_PHID,
        semantic_grouping_key=_SGK,
        grouping_policy_version="perp@v1",
        status="closed",
        payload={"protocol": "gmx_v2", "source": "settlement_reconciler"},
        matching_policy_version=1,
        opened_at_block=None,
        opened_tx=None,
        closed_at_block=2,
        closed_tx=_CLOSE_TX,
    )
    await store.save_ledger_and_registry_atomic(
        _StubLedger(), close_row, None, mode=LedgerRegistrySaveMode.REGISTRY_RECONCILIATION
    )

    assert _reference(conn, "ev-close")["physical_identity_hash"] == _PHID
    assert _reference(conn, "ev-open")["physical_identity_hash"] == _PHID, (
        "the open-side event must stay joined after a close-side registry write"
    )


def test_restamp_returns_none_when_reference_unchanged() -> None:
    """Unit-level guard on the early-exit the idempotency contract rests on."""
    registry_row = {
        "physical_identity_hash": _PHID,
        "semantic_grouping_key": _SGK,
        "grouping_policy_version": "perp@v1",
        "handle": None,
        "matching_policy_version": 1,
    }

    def _lookup(_p: str, _k: str, _c: str) -> dict:
        return registry_row

    payload = json.dumps({"event_type": "PERP_CLOSE"}, sort_keys=True)
    first = restamp_position_reference(payload, is_live=True, registry_lookup=_lookup)
    assert first is not None
    assert json.loads(first)["position_reference"]["physical_identity_hash"] == _PHID
    assert restamp_position_reference(first, is_live=True, registry_lookup=_lookup) is None


def test_restamp_returns_none_for_unknown_and_non_position_bearing_event_types() -> None:
    """Empty ≠ Zero: an unprovable row stays unmeasured rather than being guessed."""

    def _lookup(_p: str, _k: str, _c: str) -> dict:
        raise AssertionError("lookup must not be reached for a non-repairable row")

    for payload in (
        json.dumps({"event_type": "NOT_A_REAL_EVENT"}),
        json.dumps({"event_type": "PERP_SETTLEMENT"}),
        json.dumps({}),
        "not json at all",
    ):
        assert restamp_position_reference(payload, is_live=True, registry_lookup=_lookup) is None


def test_restamp_never_downgrades_a_joined_reference_to_legacy() -> None:
    """A repair may only CLOSE a join, never open one.

    Reachable, not theoretical: two positions of the same
    ``(primitive, accounting_category)`` closed in ONE tx make
    ``_build_registry_lookup_for_event`` return ``None`` on its ambiguity
    safeguard. Without this guard, a row correctly stamped when the FIRST
    registry row landed would be un-joined when the SECOND one does — the
    repair actively destroying the join it exists to create.
    """

    def _ambiguous(_p: str, _k: str, _c: str) -> None:
        return None  # what the >1-row safeguard returns

    joined = json.dumps(
        {
            "event_type": "PERP_CLOSE",
            "position_reference": {
                **_LEGACY_REFERENCE,
                "source": "registry",
                "physical_identity_hash": _PHID,
                "semantic_grouping_key": _SGK,
                "grouping_policy_version": "perp@v1",
                "matching_policy_version": 1,
            },
        },
        sort_keys=True,
    )
    assert restamp_position_reference(joined, is_live=True, registry_lookup=_ambiguous) is None

    # And a still-legacy row is likewise left alone rather than rewritten.
    legacy = json.dumps({"event_type": "PERP_CLOSE", "position_reference": dict(_LEGACY_REFERENCE)}, sort_keys=True)
    assert restamp_position_reference(legacy, is_live=True, registry_lookup=_ambiguous) is None


def test_restamp_mutates_only_the_position_reference_key() -> None:
    """It must NOT re-stamp version fields.

    ``augment_accounting_payload`` would restamp ``schema_version`` /
    ``formula_version`` / ``matching_policy_version`` / ``primitive_version``
    from the CURRENT tables. The repair can legitimately fire after an SDK
    upgrade (a restart with a pending unsettled GMX order is a real sequence on
    the keeper lane), and silently advancing a Phase-1 row's version stamps is
    the quiet audit-trail mutation the version-stamp design exists to prevent.
    """

    def _lookup(_p: str, _k: str, _c: str) -> dict:
        return {
            "physical_identity_hash": _PHID,
            "semantic_grouping_key": _SGK,
            "grouping_policy_version": "perp@v1",
            "handle": None,
            "matching_policy_version": 1,
        }

    original = {
        "event_type": "PERP_CLOSE",
        "schema_version": 1,
        "formula_version": 1,
        "matching_policy_version": 99,
        "primitive_version": 42,
        "realized_pnl_usd": "1.25",
        "position_reference": dict(_LEGACY_REFERENCE),
    }
    out = restamp_position_reference(json.dumps(original, sort_keys=True), is_live=True, registry_lookup=_lookup)
    assert out is not None
    result = json.loads(out)
    for key, value in original.items():
        if key == "position_reference":
            continue
        assert result[key] == value, f"{key} must survive the repair untouched"
    assert result["position_reference"]["physical_identity_hash"] == _PHID


# =============================================================================
# Failure semantics — the repair must NEVER cost us the registry row
# =============================================================================


@pytest.mark.asyncio
async def test_repair_failure_does_not_roll_back_the_registry_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed bookkeeping repair must not recreate bug #2130.

    The registry row is the durable record of on-chain risk (blueprint 28 §1).
    Losing it to a failed *pointer* fix is strictly worse than leaving a legacy
    pointer, and the perp settlement lane already ratified this inversion for
    its own registry completion (VIB-3872 §3 D2: "best-effort, never block").
    Deliberate, named inversion of the VIB-3863 live-raises rule — which governs
    the accounting WRITE ITSELF, where a dropped event is unrecoverable.
    """
    db = tmp_path / "state.db"
    store = await _store(db)
    conn = store._conn
    assert conn is not None
    _insert_phase1_event(conn, event_id="ev-close", event_type="PERP_CLOSE", tx_hash=_CLOSE_TX)

    def _boom(*_args: Any, **_kwargs: Any) -> int:
        raise RuntimeError("simulated repair failure")

    monkeypatch.setattr(SQLiteStore, "_repair_position_references_for_registry_row", _boom)

    with caplog.at_level(logging.ERROR):
        await store.save_ledger_and_registry_atomic(
            _StubLedger(), _perp_registry_row(), None, mode=LedgerRegistrySaveMode.REGISTRY_RECONCILIATION
        )

    persisted = conn.execute(
        "SELECT status, physical_identity_hash FROM position_registry WHERE deployment_id = ?",
        (_DEPLOYMENT,),
    ).fetchone()
    assert persisted is not None, "the registry row must survive a repair failure"
    assert persisted["status"] == "closed"
    assert persisted["physical_identity_hash"] == _PHID

    # Loud, greppable, and it names the consequence.
    assert any(
        "position_reference repair FAILED" in r.message and "inverse orphan" in r.message
        for r in caplog.records
        if r.levelno >= logging.ERROR
    ), "a repair failure must log ERROR naming the L5_22 consequence"


@pytest.mark.asyncio
async def test_repair_runs_under_both_commit_and_reconciliation_modes(tmp_path: Path) -> None:
    """Every registry producer funnels through this writer; both modes repair."""
    for mode in (LedgerRegistrySaveMode.COMMIT, LedgerRegistrySaveMode.REGISTRY_RECONCILIATION):
        db = tmp_path / f"state-{mode.value if hasattr(mode, 'value') else mode}.db"
        store = await _store(db)
        conn = store._conn
        assert conn is not None
        _insert_phase1_event(conn, event_id="ev-close", event_type="PERP_CLOSE", tx_hash=_CLOSE_TX)

        entry = _StubLedger()
        if mode is LedgerRegistrySaveMode.COMMIT:
            pytest.importorskip("almanak.framework.observability.ledger")
            from almanak.framework.observability.ledger import LedgerEntry

            entry = LedgerEntry(
                deployment_id=_DEPLOYMENT,
                cycle_id="cycle-1",
                intent_type="PERP_CLOSE",
                tx_hash=_CLOSE_TX,
                chain=_CHAIN,
                protocol="gmx_v2",
                success=True,
            )

        await store.save_ledger_and_registry_atomic(entry, _perp_registry_row(), None, mode=mode)
        assert _reference(conn, "ev-close")["physical_identity_hash"] == _PHID, mode
