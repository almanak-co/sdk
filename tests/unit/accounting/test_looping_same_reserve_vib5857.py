"""VIB-5857 — the same-reserve companion to ``looping_debt_open`` (VIB-6560).

``looping_debt_open`` proved the netted ``_snapshot_equity`` sees an open debt,
but it deliberately keeps collateral and debt on DISJOINT reserves
(``test_collateral_and_debt_are_disjoint_reserves``) because the same-reserve
SUPPLY-leg shape was undecided when it was frozen. VIB-5857 decided it: the
SUPPLY leg carries GROSS collateral value (``value_usd = supply_value_usd``);
debt is represented ONLY by the signed-negative BORROW leg; every NAV consumer
subtracts ``debt_mark`` exactly once. This file makes BOTH halves of that
decision measurable on the topology where they interact.

The load-bearing structure is a 2×2 — leg shape × equity projection::

                          gross projection       netted projection
    GROSS legs (fixture)  gap == debt_mark FAIL  gap == 0  PASS
    net legs (degraded)   gap == 0               gap == debt_mark

Exactly one diagonal ties under each consumer. The legacy net-SUPPLY shape
"worked" only because the legacy consumer was gross-of-debt; fixing either half
alone breaks the books (the pinned ``−24000`` double-subtract in
``test_netting_parity.py`` / ``test_portfoliovaluer_contract.py`` is the
bottom-right cell one layer down). That is why VIB-5857 ships the valuer shape
change and the ``_snapshot_equity`` netting in ONE commit, and why this file
pins all four cells rather than only the two that pass.

The matrix arms are NAMED projections; PRODUCTION additionally branches on the
schema marker (``accountant_test._legacy_net_supply_debt``) so a historical
net-shaped DB is read correctly rather than double-subtracted —
``test_production_reads_legacy_net_shaped_rows_correctly`` pins the branch
firing, and ``test_detection_bound_stripped_details_still_double_subtract``
pins the documented limit of what it can detect.

Projection helpers are imported from ``test_looping_debt_open_vib6560`` rather
than restated: both arms there are NAMED projections (not "whatever production
does"), which is exactly the property this file needs — the 2×2 is between two
named shapes and two named consumers, so it keeps meaning the same thing no
matter which half of the codebase moves next.

Like its sibling, the probe is negative-controlled: degrade a copy until the
measurement must die (empty positions, zero endpoint debt) and assert it does.

Position keys: SUPPLY and BORROW events share ONE key here because that is what
``lending_accounting._derive_position_key`` derives for a same-reserve loop
(``lending:{chain}:{protocol}:{wallet}:{asset}`` — no leg discriminator). The
fixture reproduces production's key shape rather than inventing a private
discriminator; the conflation itself is a separate pre-existing defect
(VIB-6697), and no cell pinned here depends on key uniqueness.
"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.accounting.accountant_test import run_against_sqlite
from tests.unit.accounting.test_looping_debt_open_vib6560 import (
    _PROFILE,
    _dec_or_none,
    _degraded_copy,
    _endpoint_debt_mark,
    _g6_gap_under_gross_equity,
    _g6_gap_under_net_equity,
    _snapshots,
)

_FIXTURE_DIR = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "accounting" / "looping_same_reserve"
_FIXTURE_DB = _FIXTURE_DIR / "expected_baseline.sqlite"


def _legs(s: dict) -> list[dict]:
    return json.loads(str(s["positions_json"]))["positions"]


def _renet_supply_legs(conn: sqlite3.Connection) -> None:
    """Degrade every snapshot into the LEGACY (pre-VIB-5857) leg shape.

    The old valuer wrote ``value_usd = net_value_usd`` on the SUPPLY leg while
    the BORROW leg still carried the signed negative — the double-count shape.
    ``total_value_usd`` is Σ positive ``value_usd`` (VIB-3614), so it is
    recomputed from the mutated legs; writing legacy legs under the NEW total
    would manufacture an incoherent row no writer ever produced.
    """
    rows = conn.execute("SELECT id, positions_json FROM portfolio_snapshots").fetchall()
    for row_id, raw in rows:
        payload = json.loads(raw)
        for leg in payload["positions"]:
            if leg["position_type"] == "SUPPLY":
                leg["value_usd"] = leg["details"]["net_value_usd"]
                leg["details"].pop("supply_leg_convention", None)
        total = sum(
            (Decimal(str(leg["value_usd"])) for leg in payload["positions"] if Decimal(str(leg["value_usd"])) > 0),
            Decimal("0"),
        )
        conn.execute(
            "UPDATE portfolio_snapshots SET positions_json = ?, total_value_usd = ? WHERE id = ?",
            (json.dumps(payload), str(total), row_id),
        )


# ── structural gates ─────────────────────────────────────────────────────


class TestStructuralNonVacuity:
    """Fails if the fixture stops being a same-reserve, gross-legged, debt-open
    stimulus — the three properties every assertion below leans on."""

    def test_debt_is_open_at_the_endpoint(self) -> None:
        assert _endpoint_debt_mark(_FIXTURE_DB) > 0

    def test_collateral_and_debt_share_one_reserve(self) -> None:
        """The inverse of the sibling fixture's disjointness gate — same-reserve
        IS the stimulus here. Both leg types must reference the same asset."""
        legs = _legs(_snapshots(_FIXTURE_DB)[-1])
        supply = {leg["details"]["asset"] for leg in legs if leg["position_type"] == "SUPPLY"}
        borrow = {leg["details"]["asset"] for leg in legs if leg["position_type"] == "BORROW"}
        assert supply and borrow
        assert supply == borrow, f"reserves diverged: supply={supply} borrow={borrow}"

    def test_supply_legs_are_gross_shaped(self) -> None:
        """The ratified shape, pinned leg by leg: ``value_usd`` equals the
        reserve's GROSS ``supply_value_usd``; whenever same-reserve debt is
        live, it differs from ``net_value_usd`` (on a debt-free row the two
        coincide by arithmetic — asserting inequality there would be false);
        and the VIB-5857 schema marker is present so a reader can distinguish
        this shape from historical net-shaped rows without re-deriving it."""
        saw_divergent = False
        for s in _snapshots(_FIXTURE_DB):
            for leg in _legs(s):
                if leg["position_type"] != "SUPPLY":
                    continue
                details = leg["details"]
                assert Decimal(str(leg["value_usd"])) == Decimal(str(details["supply_value_usd"]))
                assert details.get("supply_leg_convention") == "gross"
                if Decimal(str(details["debt_value_usd"])) > 0:
                    saw_divergent = True
                    assert Decimal(str(leg["value_usd"])) != Decimal(str(details["net_value_usd"]))
        assert saw_divergent, "no SUPPLY leg ever carries same-reserve debt — gross vs net is untestable here"

    def test_total_value_usd_is_coherent_with_the_legs(self) -> None:
        """Σ POSITIVE ``value_usd`` (VIB-3614) — with gross SUPPLY legs this is
        gross collateral + wallet lots, the quantity ``debt_mark`` re-nets."""
        for s in _snapshots(_FIXTURE_DB):
            positive = sum(
                (Decimal(str(leg["value_usd"])) for leg in _legs(s) if Decimal(str(leg["value_usd"])) > 0),
                Decimal("0"),
            )
            assert Decimal(str(s["total_value_usd"])) == positive, f"snapshot {s['id']}"

    def test_at_least_one_measured_non_zero_available_cash(self) -> None:
        cash = [_dec_or_none(s["available_cash_usd"]) for s in _snapshots(_FIXTURE_DB)]
        assert any(c is not None and c != 0 for c in cash)

    def test_the_endpoint_is_the_same_row_in_row_order_and_time_order(self) -> None:
        rows = _snapshots(_FIXTURE_DB)
        by_time = sorted(rows, key=lambda s: str(s["timestamp"]))
        assert rows[-1]["id"] == by_time[-1]["id"]
        assert rows[0]["id"] == by_time[0]["id"]


# ── the 2×2: leg shape × equity projection ───────────────────────────────


class TestShapeConsumerMatrix:
    """Each shape ties only under its matching consumer; the diagonal that
    fails does so by exactly the endpoint ``debt_mark``."""

    def test_gross_legs_tie_under_the_netted_projection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Top-right cell — the shipped configuration. Every flow is
        NAV-conservative, interest is a measured zero and marks are flat, so
        gas is the only true NAV mover and the component method sums exactly
        that: the books tie EXACTLY, not merely within ε."""
        assert _endpoint_debt_mark(_FIXTURE_DB) > 0, "no endpoint debt — the tie below would be vacuous"
        assert _g6_gap_under_net_equity(_FIXTURE_DB, monkeypatch) == 0

    def test_gross_legs_fail_under_the_gross_projection_by_the_debt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Top-left cell — what the pre-VIB-5857 consumer would report against
        the new shape: overstated by the whole endpoint debt."""
        assert _g6_gap_under_gross_equity(_FIXTURE_DB, monkeypatch) == _endpoint_debt_mark(_FIXTURE_DB)

    def test_net_legs_tie_under_the_gross_projection(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Bottom-left cell — why the legacy shape survived so long: gross
        consumer over net legs happens to cancel, so nothing looked wrong."""
        degraded = _degraded_copy(_FIXTURE_DB, tmp_path, _renet_supply_legs)
        assert _endpoint_debt_mark(degraded) > 0, "degradation dropped the debt leg — cancellation would be vacuous"
        assert _g6_gap_under_gross_equity(degraded, monkeypatch) == 0

    def test_net_legs_double_subtract_under_the_netted_projection(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Bottom-right cell — THE landmine (VIB-5201's ``−24000``, measured at
        fixture level): a net-shaped SUPPLY leg beside a signed BORROW leg has
        the debt subtracted twice by the RAW netted projection, understating
        NAV by the whole endpoint debt. The arm here is the NAMED raw
        projection (``_net_equity``, no legacy branch) on purpose: it pins the
        arithmetic hazard itself. PRODUCTION does not read legacy rows this
        way — ``_legacy_net_supply_debt`` branches on the marker's absence and
        excludes the double-counted debt; the two tests below pin that branch
        firing and its detection bound."""
        degraded = _degraded_copy(_FIXTURE_DB, tmp_path, _renet_supply_legs)
        assert _g6_gap_under_net_equity(degraded, monkeypatch) == _endpoint_debt_mark(degraded)

    def test_production_reads_legacy_net_shaped_rows_correctly(self, tmp_path: Path) -> None:
        """No monkeypatch: the LIVE ``_snapshot_equity`` on a LEGACY-shaped DB
        (net SUPPLY legs, marker stripped, exactly what the pre-VIB-5857
        enriched writer persisted) must NOT double-subtract —
        ``_legacy_net_supply_debt`` detects the shape via the enriched details
        (``value_usd == net_value_usd`` with ``debt_value_usd > 0`` and no
        marker) and excludes that debt, so the books tie and G6 scores PASS.
        This is the reader-side branch the schema marker exists for."""
        degraded = _degraded_copy(_FIXTURE_DB, tmp_path, _renet_supply_legs)
        assert _endpoint_debt_mark(degraded) > 0, "legacy copy lost its debt leg — the tie below would be vacuous"
        report = run_against_sqlite(degraded, primitive=_PROFILE, strict_lifecycle=True)
        g6 = next(c for c in report.cells if c.cell_id == "G6")
        assert g6.status == "PASS"
        assert Decimal(g6.decomposition["gap_usd"]) == 0

    def test_detection_bound_stripped_details_still_double_subtract(self, tmp_path: Path) -> None:
        """The documented BOUND of the legacy branch: a net-shaped leg whose
        enriched keys were stripped is indistinguishable from an
        account-state-protocol leg (whose values are genuinely gross), so the
        correction cannot fire and production double-subtracts. Without this
        control, the test above could pass via a correction that fires on
        EVERYTHING — which would un-net genuinely gross rows instead."""

        def _renet_and_strip(conn: sqlite3.Connection) -> None:
            _renet_supply_legs(conn)
            rows = conn.execute("SELECT id, positions_json FROM portfolio_snapshots").fetchall()
            for row_id, raw in rows:
                payload = json.loads(raw)
                for leg in payload["positions"]:
                    if leg["position_type"] == "SUPPLY":
                        leg["details"].pop("net_value_usd", None)
                        leg["details"].pop("debt_value_usd", None)
                        leg["details"].pop("supply_value_usd", None)
                conn.execute(
                    "UPDATE portfolio_snapshots SET positions_json = ? WHERE id = ?",
                    (json.dumps(payload), row_id),
                )

        degraded = _degraded_copy(_FIXTURE_DB, tmp_path, _renet_and_strip)
        assert _endpoint_debt_mark(degraded) > 0
        report = run_against_sqlite(degraded, primitive=_PROFILE, strict_lifecycle=True)
        g6 = next(c for c in report.cells if c.cell_id == "G6")
        assert g6.status == "FAIL"
        assert Decimal(g6.decomposition["gap_usd"]) == _endpoint_debt_mark(degraded)

    def test_production_scores_g6_pass_on_the_committed_fixture(self) -> None:
        """No monkeypatch: the LIVE ``_snapshot_equity`` must tie these books.
        The sibling file refuses to assert on production (it was written while
        production was defective, and a production-shaped arm would have
        blocked the fix); this file exists AFTER the fix, so pinning production
        is a pure regression guard — un-netting ``_snapshot_equity`` flips this
        test red."""
        report = run_against_sqlite(_FIXTURE_DB, primitive=_PROFILE, strict_lifecycle=True)
        g6 = next(c for c in report.cells if c.cell_id == "G6")
        assert g6.status == "PASS"
        assert Decimal(g6.decomposition["gap_usd"]) == 0

    # ── negative controls: the probe must DIE when the fixture is degraded ──

    def test_probe_is_dead_when_positions_json_is_emptied(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
        """Drop only the endpoint BORROW leg — the unwound-before-the-endpoint
        shape of every completed run. The SUPPLY leg's own details still record
        reserve debt, so this also pins that ``debt_mark`` reads the SIGNED
        LEGS, never the details blob: if the netting ever started consuming
        ``details.debt_value_usd`` the movement here would NOT collapse."""

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
        assert len({str(s["positions_json"]) for s in _snapshots(degraded)}) >= 2
        assert _g6_gap_under_gross_equity(degraded, monkeypatch) - _g6_gap_under_net_equity(degraded, monkeypatch) == 0


# ── registration / determinism ───────────────────────────────────────────


def test_fixture_is_registered_in_the_ratchet() -> None:
    """An unregistered fixture is never swept, so nothing it proves is protected.
    Loader pattern per ``test_looping_debt_open_vib6560.py`` (InitVar dataclasses
    need a live ``sys.modules`` entry during ``exec_module``)."""
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

    assert "looping_same_reserve" in gate.PRIMITIVES
    assert gate._FIXTURE_SCORING_PROFILE["looping_same_reserve"] == _PROFILE
    manifest = json.loads((_FIXTURE_DIR / "expected_cells.json").read_text())
    # Born tied: the floor is an exact zero, so ANY re-widening of the
    # reconciliation gap on this topology is a reportable regression.
    assert Decimal(manifest["cell_metrics"]["G6"]["gap_usd"]) == 0
    assert manifest["cells"]["G6"] == "PASS"


def _canonical_dump(db: Path) -> str:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return "\n".join(conn.iterdump())
    finally:
        conn.close()


def test_fixture_regenerates_deterministically(tmp_path: Path) -> None:
    """Same contract (and same content-not-bytes rationale, VIB-6563) as the
    sibling fixture's determinism test."""
    from tests.fixtures.accounting._generate_baselines import generate_looping_same_reserve_fixture

    regenerated = tmp_path / "expected_baseline.sqlite"
    generate_looping_same_reserve_fixture(regenerated)
    assert _canonical_dump(regenerated) == _canonical_dump(_FIXTURE_DB)

    again = tmp_path / "again.sqlite"
    generate_looping_same_reserve_fixture(again)
    assert again.read_bytes() == regenerated.read_bytes()
