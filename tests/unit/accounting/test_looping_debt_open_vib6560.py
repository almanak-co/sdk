"""VIB-6560 — the lending corpus must be able to EXPRESS an open debt, and must
stay able to.

Two guards live here, and they answer different questions.

**Structural non-vacuity** (`TestStructuralNonVacuity`) asks *can this fixture
carry a position-sourced signal at all?* The plain ``looping`` fixture cannot:
measured 2026-08-05 it has 6 snapshots, 1 distinct ``available_cash_usd`` (= 0)
and 1 distinct ``positions_json`` (= ``[]``). No position ever appears, so any
cell folding NAV from positions or cash reads identically on every row no matter
what the events say. These tests fail if ``looping_debt_open`` is ever "enriched"
back into that blindness.

**Instrument liveness** (`TestInstrumentLiveness`) asks the harder question:
*does a real defect actually MOVE a named cell?* A non-empty fixture that still
cannot fail the instrument is the exact vacuity this ticket exists to remove, and
structural richness alone does not prove otherwise. So the liveness test scores
the same frozen DB twice, monkeypatching **only the equity projection** — never
the DB — once with ``_gross_equity`` and once with the canonical debt-netted
``_net_equity``, and asserts G6's ``gap_usd`` moves by at least the endpoint
``debt_mark``, in the direction the defect predicts.

**Both arms are named projections, neither is "whatever production does."** That
is deliberate: taking the gross arm from the live ``_snapshot_equity`` works only
while production is still defective, so the day VIB-5857 lands both arms would
compute the same thing and this file would fail — blocking the exact FAIL→PASS
improvement it exists to enable. Verified by simulating that fix (production
``_snapshot_equity`` replaced with the netted projection): all tests still pass.

Why G6 and not G8. The ticket this fixture serves originally targeted G8, and
that was wrong: ``_cell_g8_time_series`` fails only when equity is *unmeasured*
(both columns null). A gross-of-debt equity is still measured, so G8 passes
either way — it is a measured-ness cell, not a value cell. G6's ``gap_usd`` is a
value, it already carries a numeric ratchet floor, and its status is FAIL
(rank 0), so magnitude is the only thing that can move.

The defect under test is VIB-5857 — ``_snapshot_equity`` sums
``total_value_usd + available_cash_usd`` while ``total_value_usd`` is Σ POSITIVE
``value_usd`` (the debt leg dropped, VIB-3614), so mid-run NAV overstates
leveraged equity by the entire debt. Fixing it is explicitly NOT in scope here;
this file only proves the instrument can now see it.

These tests are themselves negative-controlled: `test_probe_is_dead_*` degrade a
COPY of the fixture in the two ways that would silently kill the probe (empty the
positions, zero the endpoint debt) and assert the movement collapses. Without
those, a probe that measured nothing would pass exactly like a probe that works.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from almanak.framework.accounting import accountant_test
from almanak.framework.accounting.accountant_test import Primitive, run_against_sqlite
from almanak.framework.valuation.net_debt import net_debt_from_positions_json, wallet_nav_usd

_FIXTURE_DIR = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "accounting" / "looping_debt_open"
_FIXTURE_DB = _FIXTURE_DIR / "expected_baseline.sqlite"
# Typed as the ScorecardProfile literal, not a bare str: the fixture DIRECTORY is
# ``looping_debt_open`` but it scores under the EXISTING ``looping`` profile, and
# a plain str would let a typo reach ``_profile_for`` as a runtime ValueError.
_PROFILE: Primitive = "looping"


# ── helpers ──────────────────────────────────────────────────────────────


def _snapshots(db: Path) -> list[dict[str, Any]]:
    """Every snapshot row, in the SAME order the Accountant Test reads them.

    ``_table_rows`` issues no ORDER BY, so G6's ``priced[0]`` / ``priced[-1]``
    endpoints are the DB's natural order. Reading them any other way here would
    measure a different pair of rows than the cell does.
    """
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM portfolio_snapshots")]
    finally:
        conn.close()


def _dec_or_none(raw: Any) -> Decimal | None:
    """Empty != Zero: ``None`` and ``""`` are unmeasured, never ``Decimal(0)``."""
    if raw is None or raw == "":
        return None
    return Decimal(str(raw))


def _gross_equity(s: dict[str, Any]) -> Decimal | None:
    """Today's projection — ``accountant_test._snapshot_equity``, restated."""
    deployed = _dec_or_none(s.get("total_value_usd"))
    cash = _dec_or_none(s.get("available_cash_usd"))
    if deployed is None and cash is None:
        return None
    return (deployed or Decimal("0")) + (cash or Decimal("0"))


def _net_equity(s: dict[str, Any]) -> Decimal | None:
    """The debt-netted projection, built from PRODUCTION helpers only.

    ``net_debt_from_positions_json`` is the canonical read path (blueprint 27
    §7.11) and ``wallet_nav_usd`` is the single shared NAV definition. Neither is
    re-implemented here — a hand-rolled netting formula in the test would let the
    test agree with itself rather than with production.

    The ``None`` condition is deliberately IDENTICAL to ``_snapshot_equity``'s
    (both columns unmeasured). G6 selects its endpoints with
    ``priced = [s for s in snapshots if _snapshot_equity(s) is not None]``, so a
    projection that returned ``None`` on a different set of rows would move the
    BRACKET as well as the value, and the gross-vs-net gap difference would no
    longer be attributable to the debt term alone. Same rows, different value.
    """
    deployed = _dec_or_none(s.get("total_value_usd"))
    cash = _dec_or_none(s.get("available_cash_usd"))
    if deployed is None and cash is None:
        return None
    _count, debt_mark, _debt_cost = net_debt_from_positions_json(s.get("positions_json"))
    return wallet_nav_usd(deployed or Decimal("0"), debt_mark, cash or Decimal("0"))


def _endpoint_debt_mark(db: Path) -> Decimal:
    """``debt_mark`` at the final endpoint G5/G6 bracket on."""
    rows = [s for s in _snapshots(db) if _gross_equity(s) is not None]
    _count, debt_mark, _debt_cost = net_debt_from_positions_json(rows[-1].get("positions_json"))
    return debt_mark


def _g6_gap(db: Path) -> Decimal:
    report = run_against_sqlite(db, primitive=_PROFILE, strict_lifecycle=True)
    g6 = next(c for c in report.cells if c.cell_id == "G6")
    return Decimal(g6.decomposition["gap_usd"])


def _g6_gap_under(db: Path, projection: Any, monkeypatch: pytest.MonkeyPatch) -> Decimal:
    """Score with ONLY the equity projection swapped. The DB is never touched.

    **Both arms are pinned explicitly, including the gross one.** An earlier
    revision took the gross arm from whatever ``accountant_test._snapshot_equity``
    happens to be — which is gross *today*, but becomes net the moment VIB-5857
    lands. At that point both arms would compute the same thing, ``net_gap <
    gross_gap`` would fail, and this file would block the exact FAIL→PASS
    improvement it was built to enable. A test that requires production to stay
    defective is worse than no test. ``_gross_equity`` restates today's formula
    so the comparison is between two named projections, not between a projection
    and the current state of the codebase.
    """
    monkeypatch.setattr(accountant_test, "_snapshot_equity", projection)
    return _g6_gap(db)


def _g6_gap_under_gross_equity(db: Path, monkeypatch: pytest.MonkeyPatch) -> Decimal:
    return _g6_gap_under(db, _gross_equity, monkeypatch)


def _g6_gap_under_net_equity(db: Path, monkeypatch: pytest.MonkeyPatch) -> Decimal:
    return _g6_gap_under(db, _net_equity, monkeypatch)


def _degraded_copy(src: Path, dest_dir: Path, mutate) -> Path:
    """A writable copy of the fixture with ``mutate(conn)`` applied.

    The fixture DB is committed and must never be mutated in place; every
    negative control operates on a tmp_path copy.
    """
    dest = dest_dir / "expected_baseline.sqlite"
    shutil.copy2(src, dest)
    conn = sqlite3.connect(str(dest))
    try:
        mutate(conn)
        conn.commit()
    finally:
        conn.close()
    return dest


# ── the structural gate ──────────────────────────────────────────────────


class TestStructuralNonVacuity:
    """Fails if the fixture is ever degraded back into position-blindness."""

    def test_debt_is_open_at_the_endpoint(self) -> None:
        """The one load-bearing property: a live signed-negative leg at the LAST
        snapshot. A run that fully unwinds ends flat and is inert for exactly the
        reason every completed run in the 193-DB corpus is."""
        assert _endpoint_debt_mark(_FIXTURE_DB) > 0

    def test_the_endpoint_is_the_same_row_in_row_order_and_time_order(self) -> None:
        """G6 reads snapshots WITHOUT an ORDER BY, so its endpoint is the DB's
        natural order; G5 time-orders. If those ever disagree the two cells
        bracket different intervals and this fixture's arithmetic stops meaning
        what the docstring says it means."""
        rows = _snapshots(_FIXTURE_DB)
        by_time = sorted(rows, key=lambda s: str(s["timestamp"]))
        assert rows[-1]["id"] == by_time[-1]["id"]
        assert rows[0]["id"] == by_time[0]["id"]

    def test_positions_json_has_at_least_two_distinct_shapes(self) -> None:
        """The plain ``looping`` fixture has exactly ONE (``[]``). One shape means
        a position-sourced fold is constant across the run and cannot carry a
        signal."""
        shapes = {str(s["positions_json"]) for s in _snapshots(_FIXTURE_DB)}
        assert len(shapes) >= 2

    def test_at_least_one_measured_non_zero_available_cash(self) -> None:
        """The plain ``looping`` fixture's cash column is 0 on every row. Empty !=
        Zero: an unmeasured column is not a measured zero, so the assertion is on
        MEASURED non-zero values only."""
        cash = [_dec_or_none(s["available_cash_usd"]) for s in _snapshots(_FIXTURE_DB)]
        assert any(c is not None and c != 0 for c in cash)

    def test_total_value_usd_is_coherent_with_the_legs(self) -> None:
        """``total_value_usd`` must be Σ POSITIVE ``value_usd`` (VIB-3614 — the
        debt leg is DROPPED here and must be re-subtracted by any NAV consumer).
        Without this the column is an independent decoration and the whole
        gross-vs-net demonstration would be arithmetic about nothing."""
        for s in _snapshots(_FIXTURE_DB):
            legs = json.loads(str(s["positions_json"]))["positions"]
            positive = sum(
                (Decimal(str(leg["value_usd"])) for leg in legs if Decimal(str(leg["value_usd"])) > 0),
                Decimal("0"),
            )
            assert Decimal(str(s["total_value_usd"])) == positive, f"snapshot {s['id']}"

    def test_legs_are_readable_by_the_production_net_debt_path(self) -> None:
        """Validate through ``net_debt_from_positions_json`` — the canonical read
        path — so the fixture cannot encode a private interpretation of the leg
        schema that only this test understands."""
        saw_debt = False
        for s in _snapshots(_FIXTURE_DB):
            count, debt_mark, debt_cost = net_debt_from_positions_json(s["positions_json"])
            legs = json.loads(str(s["positions_json"]))["positions"]
            # KNOWN LIMITATION (VIB-6570): the next two assertions cannot fail.
            # ``compute_net_debt_projection`` accumulates ``-value`` only on the
            # ``value < 0`` branch, so ``debt_mark`` is non-negative by
            # construction; and both sides of the count check unwrap the same
            # ``["positions"]`` key, so a schema-key change raises ``KeyError`` in
            # the ``json.loads`` above before either is reached. Left in place
            # rather than deleted mid-audit — deleting assertions is a change to
            # what this file pins, and this PR's own product is the claim that
            # nothing here is decorative. Removing them is VIB-6570's job, where
            # the deletion is the reviewed change rather than a drive-by.
            assert count == len(legs), f"snapshot {s['id']}: envelope not unwrapped"
            assert debt_mark >= 0
            if debt_mark > 0:
                saw_debt = True
                # Every debt leg carries a cost basis, so the projection is a
                # measured value and not a silently-skipped leg.
                assert debt_cost > 0, f"snapshot {s['id']}: debt_mark with no debt_cost"
        assert saw_debt, "no snapshot carries a debt leg — the fixture is inert"

    def test_collateral_and_debt_are_disjoint_reserves(self) -> None:
        """Same-reserve SUPPLY+BORROW is the VIB-5857 landmine and belongs in a
        later, explicit fixture. Freezing an invented same-reserve payload into
        the FIRST 'can fail' corpus would risk encoding the landmine as truth."""
        legs = json.loads(str(_snapshots(_FIXTURE_DB)[-1]["positions_json"]))["positions"]
        supply = {leg["details"]["asset"] for leg in legs if leg["position_type"] == "SUPPLY"}
        borrow = {leg["details"]["asset"] for leg in legs if leg["position_type"] == "BORROW"}
        assert supply and borrow
        assert supply.isdisjoint(borrow), f"same-reserve leverage: {supply & borrow}"


# ── the liveness test (the acceptance criterion) ─────────────────────────


class TestInstrumentLiveness:
    """Proves a named VALUE cell moves when the gross-equity defect is corrected."""

    def test_g6_gap_usd_moves_by_at_least_the_endpoint_debt_mark(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """THE acceptance criterion for VIB-6560.

        G6's ``gap_usd`` is ``|wallet_pnl − component_pnl|``. Swapping ONLY the
        equity projection changes ``wallet_pnl`` by exactly the endpoint
        ``debt_mark`` (the first snapshot pre-dates every transaction, so its
        debt_mark is zero), and the component method is untouched. Direction is
        asserted, not just magnitude: gross equity OVERSTATES, so netting can only
        SHRINK the gap.
        """
        debt_mark = _endpoint_debt_mark(_FIXTURE_DB)
        gross_gap = _g6_gap_under_gross_equity(_FIXTURE_DB, monkeypatch)
        net_gap = _g6_gap_under_net_equity(_FIXTURE_DB, monkeypatch)

        # DO NOT delete either of the next two assertions as "redundant".
        # Measured against both degradations below, `movement >= debt_mark` ON
        # ITS OWN is VACUOUS: degrade the fixture and `debt_mark` collapses to 0,
        # so that assertion reads `0 >= 0` and passes on a probe measuring
        # nothing -- precisely the failure shape this ticket exists to remove.
        # These two are what make the magnitude assertion discriminating.
        assert debt_mark > 0, "no debt at the endpoint -- the magnitude assertion would be vacuous"
        assert net_gap < gross_gap, "netting the debt did not shrink the gap — wrong direction"
        assert gross_gap - net_gap >= debt_mark, (
            f"G6 gap_usd moved by {gross_gap - net_gap}, less than the endpoint "
            f"debt_mark {debt_mark} — the instrument is not seeing the whole defect"
        )

    def test_the_gross_gap_IS_the_debt_mark_and_the_net_books_tie(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The identity re-derived independently, not read back from the manifest.

        Every flow in the fixture is NAV-conservative, interest is a measured
        zero and prices are flat, so gas is the only thing that moves true NAV —
        and the component method sums exactly that. Therefore the net books tie
        EXACTLY (gap 0) and the entire gross gap IS the dropped debt.

        The equalities alone are NOT self-guarding, despite being equalities: on
        a coherent full-unwind edit — drop the endpoint BORROW leg and its
        offsetting borrowed-token leg, recompute the totals — both sides go to
        zero and ``0 == 0`` passes on a dead probe. That is the same shape
        already carrying a `DO NOT delete` guard in
        ``test_g6_gap_usd_moves_by_at_least_the_endpoint_debt_mark``, recurring
        here, and it is the likeliest future
        perturbation (someone "repairing" this into an ordinary completed run).
        So the precondition is restated rather than assumed.
        """
        assert _endpoint_debt_mark(_FIXTURE_DB) > 0, (
            "the endpoint carries no debt — the equalities below would pass on 0 == 0"
        )
        assert _g6_gap_under_gross_equity(_FIXTURE_DB, monkeypatch) == _endpoint_debt_mark(_FIXTURE_DB)
        assert _g6_gap_under_net_equity(_FIXTURE_DB, monkeypatch) == 0

    def test_g6_status_is_an_honest_fail_that_netting_would_clear(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The frozen manifest says G6 FAIL. That must be the debt term and not
        some unrelated breakage: under the netted projection the SAME DB scores
        G6 PASS. A fixture whose G6 fails for a second reason would still show a
        moving ``gap_usd`` while never being able to reach green."""
        monkeypatch.setattr(accountant_test, "_snapshot_equity", _gross_equity)
        gross = run_against_sqlite(_FIXTURE_DB, primitive=_PROFILE, strict_lifecycle=True)
        assert next(c for c in gross.cells if c.cell_id == "G6").status == "FAIL"

        monkeypatch.setattr(accountant_test, "_snapshot_equity", _net_equity)
        netted = run_against_sqlite(_FIXTURE_DB, primitive=_PROFILE, strict_lifecycle=True)
        assert next(c for c in netted.cells if c.cell_id == "G6").status == "PASS"

    def test_the_gap_is_not_swallowed_by_the_tolerance(self) -> None:
        """A gap under ε would move the metric while never moving the status, and
        a VACUOUS ε (VIB-5826) would make G6 unfalsifiable for any input at all.
        Both would make the liveness result a coincidence."""
        report = run_against_sqlite(_FIXTURE_DB, primitive=_PROFILE, strict_lifecycle=True)
        decomp = next(c for c in report.cells if c.cell_id == "G6").decomposition
        assert decomp["ε_vacuous"] == "False"
        assert _endpoint_debt_mark(_FIXTURE_DB) > Decimal(decomp["ε_threshold_usd"])

    # ── negative controls: the probe must DIE when the fixture is degraded ──

    def test_probe_is_dead_when_positions_json_is_emptied(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Degrade the fixture into the plain ``looping`` fixture's blindness —
        every ``positions_json`` becomes ``[]``. ``debt_mark`` is then 0 on every
        row, gross and net equity coincide, and the gap cannot move. If this
        still reported movement, the measurement would be coming from somewhere
        other than the positions."""
        degraded = _degraded_copy(
            _FIXTURE_DB,
            tmp_path,
            lambda c: c.execute("UPDATE portfolio_snapshots SET positions_json = '[]'"),
        )
        assert _endpoint_debt_mark(degraded) == 0
        assert _g6_gap_under_gross_equity(degraded, monkeypatch) - _g6_gap_under_net_equity(degraded, monkeypatch) == 0

    def test_probe_is_dead_when_the_endpoint_debt_is_zeroed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Keep the fixture rich but drop ONLY the debt leg from the final
        snapshot — i.e. a run that unwound its debt before the endpoint, which is
        the shape of every completed run in the corpus. Positions are still
        varied and cash is still non-zero, so this isolates the debt-open
        property from mere structural richness."""

        def _drop_endpoint_debt(conn: sqlite3.Connection) -> None:
            row = conn.execute("SELECT id, positions_json FROM portfolio_snapshots ORDER BY id DESC LIMIT 1").fetchone()
            payload = json.loads(row[1])
            payload["positions"] = [leg for leg in payload["positions"] if Decimal(str(leg["value_usd"])) >= 0]
            conn.execute(
                "UPDATE portfolio_snapshots SET positions_json = ? WHERE id = ?",
                (json.dumps(payload), row[0]),
            )

        degraded = _degraded_copy(_FIXTURE_DB, tmp_path, _drop_endpoint_debt)
        assert _endpoint_debt_mark(degraded) == 0
        # Still structurally rich — this is NOT the emptied-positions control.
        assert len({str(s["positions_json"]) for s in _snapshots(degraded)}) >= 2
        assert _g6_gap_under_gross_equity(degraded, monkeypatch) - _g6_gap_under_net_equity(degraded, monkeypatch) == 0


# ── registration / determinism ───────────────────────────────────────────


def test_fixture_is_registered_in_the_ratchet() -> None:
    """An unregistered fixture is never swept, so nothing it proves is protected."""
    # Loader pattern (and the reason for it) copied from
    # ``tests/lint/test_accounting_gate.py::_load_module``: the gate module defines
    # dataclasses with ``InitVar`` fields, which the stdlib resolves via
    # ``sys.modules[cls.__module__]`` — so a ``sys.modules`` entry MUST exist during
    # ``exec_module`` or dataclass construction raises. The name is unique per load
    # to avoid a fixed global key colliding under xdist, and is popped afterwards.
    import importlib.util
    import sys
    import uuid

    unique_name = f"_check_accounting_ratchet_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(
        unique_name,
        Path(__file__).resolve().parents[3] / "scripts" / "ci" / "check_accounting_ratchet.py",
    )
    assert spec is not None and spec.loader is not None
    gate = importlib.util.module_from_spec(spec)
    sys.modules[unique_name] = gate
    try:
        spec.loader.exec_module(gate)
    finally:
        sys.modules.pop(unique_name, None)

    assert "looping_debt_open" in gate.PRIMITIVES
    assert gate._FIXTURE_SCORING_PROFILE["looping_debt_open"] == _PROFILE
    # The numeric floor is what makes a FAIL cell ratchetable at all: status is
    # already rank 0, so magnitude is the only thing left that can regress.
    manifest = json.loads((_FIXTURE_DIR / "expected_cells.json").read_text())
    assert Decimal(manifest["cell_metrics"]["G6"]["gap_usd"]) > 0


def _canonical_dump(db: Path) -> str:
    """Schema + every row, in SQLite's own deterministic dump order."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return "\n".join(conn.iterdump())
    finally:
        conn.close()


def test_fixture_regenerates_deterministically(tmp_path: Path) -> None:
    """The determinism contract of ``_generate_baselines.py``: fixed clock, fixed
    counter, no ``datetime.now()``, no ``uuid.uuid4()``. A fixture that does not
    reproduce cannot be audited against the code that claims to produce it.

    Compared as CONTENT, not bytes. **Known limitation (VIB-6563):** SQLite's
    on-disk byte layout is not portable across library versions — freelist and
    page-fill details differ — so a byte-equality assertion tests the local
    SQLite build as much as the generator. This one was written as
    ``assert regenerated.read_bytes() == committed.read_bytes()``; it passed on
    macOS (SQLite 3.49.1, where the committed blob was produced) and FAILED on
    CI's Linux runner with content that is identical row for row.

    A canonical dump is the stronger assertion, not a weaker one: it is what the
    docstring's contract actually claims, and it still fails loudly on the
    failure mode the contract exists to catch — an unseeded ``uuid4()`` or
    ``datetime.now()`` leaking into any column. Byte-equality would additionally
    have failed on a benign SQLite upgrade, which is a false positive.

    Byte layout explains VIB-6563 for ``lp`` **only** — measured: its dump is
    equal while its bytes differ. ``looping`` and ``perp`` differ in their
    DUMPS, carrying a legacy pre-``deployment_id`` identity column that this
    generator no longer emits (VIB-6563 names the column verbatim; it is not
    spelled here because ``check_deployment_id_proto_surface`` forbids the
    legacy token repo-wide, and allowlisting a file to quote it would erode the
    guard for a comment's convenience). Those two committed fixtures were
    produced by a materially different generator version — a stronger problem
    than encoding drift, recorded as such on VIB-6563. Do not read this comment
    as explaining them away.
    """
    from tests.fixtures.accounting._generate_baselines import generate_looping_debt_open_fixture

    regenerated = tmp_path / "expected_baseline.sqlite"
    generate_looping_debt_open_fixture(regenerated)
    assert _canonical_dump(regenerated) == _canonical_dump(_FIXTURE_DB)

    # Regenerating twice in the SAME environment must be byte-identical: that
    # isolates generator nondeterminism from cross-version encoding drift, so
    # this file still detects an unseeded value even if the dump above were
    # somehow insensitive to it.
    again = tmp_path / "again.sqlite"
    generate_looping_debt_open_fixture(again)
    assert again.read_bytes() == regenerated.read_bytes()
