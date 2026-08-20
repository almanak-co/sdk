"""VIB-5857 — direct unit truth table for the netted ``_snapshot_equity``.

The fixture-level files (``test_looping_debt_open_vib6560`` /
``test_looping_same_reserve_vib5857``) measure the projection through whole
Accountant runs; this file pins the projection's own edge semantics directly,
because two of them were review findings on the netting change itself:

1. **Empty ≠ Zero under the debt term** (panel finding): ``wallet_nav_usd``'s
   contract forbids the caller coercing an unmeasured column to ``0``. With
   the old gross projection, ``deployed or 0`` merely understated; with the
   debt term it would produce the wrong-signed ``0 − debt + cash``. A row
   with a measured debt leg but an unmeasured ``total_value_usd`` is
   therefore NOT an equity point (returns ``None``).
2. **Legacy net-shaped leg detection** (panel finding): the pre-VIB-5857
   enriched writer persisted ``value_usd = net_value_usd`` beside a signed
   BORROW leg; subtracting the full ``debt_mark`` from such a row
   double-subtracts. ``_legacy_net_supply_debt`` must fire on exactly that
   shape and on nothing else — account-state-protocol legs (no enriched
   keys) and marked gross legs must keep netting normally.
"""

from __future__ import annotations

import json
from decimal import Decimal

from almanak.framework.accounting.accountant_test import (
    _cell_g9_confidence,
    _legacy_net_supply_debt,
    _snapshot_debt_mark,
    _snapshot_equity,
)


def _leg(
    value: str,
    position_type: str = "SUPPLY",
    details: dict | None = None,
    asset: str = "WBNB",
    protocol: str = "aave_v3",
    chain: str = "bsc",
) -> dict:
    d = dict(details or {})
    d.setdefault("asset", asset)
    return {
        "position_type": position_type,
        "protocol": protocol,
        "chain": chain,
        "value_usd": value,
        "details": d,
    }


def _positions(*legs: dict) -> str:
    return json.dumps({"schema_version": 1, "positions": list(legs)})


def _row(total, cash, positions_json="[]", confidence="HIGH"):
    return {
        "total_value_usd": total,
        "available_cash_usd": cash,
        "positions_json": positions_json,
        "value_confidence": confidence,
    }


# ── Empty ≠ Zero under the debt term ─────────────────────────────────────


class TestEmptyNeZeroWithDebt:
    def test_unmeasured_deployed_with_debt_is_not_an_equity_point(self):
        """The wrong-signed shape the review caught: NULL deployed + measured
        cash + a live debt leg must NOT read as ``0 − debt + cash``."""
        pj = _positions(_leg("-300", "BORROW"))
        assert _snapshot_equity(_row(None, "100", pj)) is None
        assert _snapshot_equity(_row("", "100", pj)) is None

    def test_unmeasured_deployed_without_debt_stays_a_cash_point(self):
        """Bracket stability everywhere else: with no debt leg the projection
        keeps the pre-fix membership (pure-cash rows are valid equity)."""
        assert _snapshot_equity(_row(None, "100")) == Decimal("100")

    def test_both_columns_unmeasured_is_none(self):
        assert _snapshot_equity(_row(None, None)) is None

    def test_unavailable_confidence_is_none(self):
        assert _snapshot_equity(_row("50", "50", confidence="UNAVAILABLE")) is None

    def test_measured_row_nets_the_debt(self):
        pj = _positions(_leg("500"), _leg("-300", "BORROW"))
        assert _snapshot_equity(_row("500", "100", pj)) == Decimal("300")


# ── legacy net-shaped leg detection truth table ──────────────────────────

_LEGACY_NET = {"net_value_usd": "200", "debt_value_usd": "300", "supply_value_usd": "500"}
_GROSS_MARKED = {**_LEGACY_NET, "supply_leg_convention": "gross"}


class TestLegacyNetSupplyDetection:
    def test_fires_on_the_exact_legacy_shape(self):
        """Unmarked enriched leg, value == net, debt > 0 — the only shape the
        pre-VIB-5857 enriched writer produced for a same-reserve loop."""
        pj = _positions(_leg("200", details=dict(_LEGACY_NET)), _leg("-300", "BORROW"))
        assert _legacy_net_supply_debt(pj) == Decimal("300")
        # debt_mark is fully excluded → equity does not double-subtract.
        assert _snapshot_debt_mark(pj) == Decimal("0")
        assert _snapshot_equity(_row("200", "0", pj)) == Decimal("200")

    def test_does_not_fire_on_a_marked_gross_leg(self):
        """Same numbers plus the marker: value(500) == supply, netting applies
        normally (this is the shipped convention)."""
        pj = _positions(_leg("500", details=dict(_GROSS_MARKED)), _leg("-300", "BORROW"))
        assert _legacy_net_supply_debt(pj) == Decimal("0")
        assert _snapshot_debt_mark(pj) == Decimal("300")

    def test_does_not_fire_on_account_state_legs(self):
        """morpho_blue / benqi / compound_v3 legs carry no enriched keys and
        their strategy-reported values are gross — netting must proceed."""
        pj = _positions(
            _leg("500", details={"asset": "WETH", "market_id": "0xabc", "source": "account_state"}),
            _leg("-300", "BORROW", details={"asset": "WETH"}),
        )
        assert _legacy_net_supply_debt(pj) == Decimal("0")
        assert _snapshot_debt_mark(pj) == Decimal("300")

    def test_does_not_fire_on_separate_reserve_legacy_legs(self):
        """A legacy separate-reserve SUPPLY leg has debt_value_usd == 0 (its
        reserve carries no debt); gross == net there and the full debt_mark
        subtraction remains correct."""
        details = {"net_value_usd": "500", "debt_value_usd": "0", "supply_value_usd": "500"}
        pj = _positions(_leg("500", details=details), _leg("-300", "BORROW"))
        assert _legacy_net_supply_debt(pj) == Decimal("0")
        assert _snapshot_debt_mark(pj) == Decimal("300")

    def test_does_not_fire_on_negative_legs(self):
        """A legacy BORROW leg from the enriched writer can satisfy
        value == net on a debt-only reserve (net == −debt); the positive-side
        guard keeps it out of the correction."""
        details = {"net_value_usd": "-300", "debt_value_usd": "300", "supply_value_usd": "0"}
        pj = _positions(_leg("-300", "BORROW", details=details))
        assert _legacy_net_supply_debt(pj) == Decimal("0")

    def test_correction_is_clamped_at_zero(self):
        """A legacy net leg whose BORROW sibling is absent: total_value_usd is
        already net and debt_mark is 0 — the correction must never push
        debt_mark negative (which would INFLATE equity)."""
        pj = _positions(_leg("200", details=dict(_LEGACY_NET)))
        assert _snapshot_debt_mark(pj) == Decimal("0")
        assert _snapshot_equity(_row("200", "0", pj)) == Decimal("200")

    def test_sibling_less_legacy_leg_cannot_un_net_another_reserves_debt(self):
        """Delta-review finding 1 — the dangerous direction. Reserve A carries
        a legacy net-shaped SUPPLY leg with NO registered BORROW sibling (its
        reserve read shows debt, but nothing of that debt is in debt_mark);
        reserve B carries a normal gross pair. A GLOBAL correction would
        subtract A's details-debt (300) from B's real debt_mark (400) and
        overstate NAV by 300. Per-reserve pairing must leave B's debt intact."""
        pj = _positions(
            _leg("200", details=dict(_LEGACY_NET), asset="WETH"),
            _leg("500", details=dict(_GROSS_MARKED), asset="WBNB"),
            _leg("-400", "BORROW", asset="WBNB"),
        )
        assert _legacy_net_supply_debt(pj) == Decimal("0")
        assert _snapshot_debt_mark(pj) == Decimal("400")
        # equity = (200 net + 500 gross) − 400 = 300, never 600.
        assert _snapshot_equity(_row("700", "0", pj)) == Decimal("300")

    def test_pairing_caps_at_the_sibling_debt_actually_in_debt_mark(self):
        """Stale details vs live BORROW leg: the double-count is only what was
        subtracted twice — min(details debt 300, sibling 250) = 250."""
        pj = _positions(
            _leg("200", details=dict(_LEGACY_NET)),
            _leg("-250", "BORROW"),
        )
        assert _legacy_net_supply_debt(pj) == Decimal("250")
        assert _snapshot_debt_mark(pj) == Decimal("0")

    def test_pairing_is_per_reserve_not_by_asset_alone(self):
        """Same asset on a different protocol is a different reserve — its
        debt must not be cancelled by the legacy leg's correction."""
        pj = _positions(
            _leg("200", details=dict(_LEGACY_NET), protocol="aave_v3"),
            _leg("-300", "BORROW", protocol="spark"),
        )
        assert _legacy_net_supply_debt(pj) == Decimal("0")
        assert _snapshot_debt_mark(pj) == Decimal("300")

    def test_missing_asset_fails_closed_on_both_sides(self):
        """Re-extended delta finding A: a leg without ``details.asset`` has no
        reserve identity. Fail OPEN (empty-string key) would collide every
        asset-less leg on one protocol+chain into a single bucket and let a
        sibling-less legacy leg un-net another reserve's real debt. Fail
        CLOSED: the asset-less legacy leg gets no correction (stays
        double-subtracted — understated, safe) and the asset-less BORROW leg
        feeds no pool."""
        legacy_no_asset = dict(_LEGACY_NET)
        pj = _positions(
            # legacy leg with NO asset (remove the helper's default)
            {
                "position_type": "SUPPLY",
                "protocol": "aave_v3",
                "chain": "bsc",
                "value_usd": "200",
                "details": legacy_no_asset,
            },
            # real BORROW on the same protocol+chain, also asset-less
            {
                "position_type": "BORROW",
                "protocol": "aave_v3",
                "chain": "bsc",
                "value_usd": "-400",
                "details": {},
            },
        )
        assert _legacy_net_supply_debt(pj) == Decimal("0")
        assert _snapshot_debt_mark(pj) == Decimal("400")

    def test_two_legacy_legs_cannot_both_claim_one_sibling(self):
        """Re-extended delta finding B (budget): two legacy net-shaped SUPPLY
        rows on ONE reserve, one −300 sibling. An unbudgeted pool would grant
        each min(300, 300) = 300 → 600 total, and the global clamp would eat
        another reserve's debt with the surplus. The consumable pool caps the
        combined claim at the sibling's 300."""
        pj = _positions(
            _leg("200", details=dict(_LEGACY_NET)),
            _leg("120", details={"net_value_usd": "120", "debt_value_usd": "300", "supply_value_usd": "420"}),
            _leg("-300", "BORROW"),
            _leg("500", details=dict(_GROSS_MARKED), asset="WETH"),
            _leg("-400", "BORROW", asset="WETH"),
        )
        assert _legacy_net_supply_debt(pj) == Decimal("300")
        # raw 700 − 300 claimed = 400: reserve B's real debt fully intact.
        assert _snapshot_debt_mark(pj) == Decimal("400")

    def test_non_borrow_negative_legs_do_not_enlarge_the_pool(self):
        """Re-extended delta finding B (pool membership): an underwater PERP
        sharing the reserve key is not lending debt — it must not increase
        what a legacy correction may cancel."""
        pj = _positions(
            _leg("200", details=dict(_LEGACY_NET)),
            _leg("-250", "BORROW"),
            _leg("-500", "PERP"),
        )
        # claim capped at the BORROW sibling (250), never 250+500.
        assert _legacy_net_supply_debt(pj) == Decimal("250")
        assert _snapshot_debt_mark(pj) == Decimal("500")

    def test_nan_money_fields_disqualify_without_crashing(self):
        """A NaN in any of the correction's money fields must degrade to 'leg
        not corrected', never raise InvalidOperation and CRASH the cell.

        Scope: the CORRECTION's own comparisons (details fields + the pairing
        loop). A NaN in a leg's ``value_usd`` crashes the pre-existing
        canonical ``compute_net_debt_projection`` before this code runs —
        that class predates the delta and is out of its scope (delta-review
        finding 4 names it explicitly)."""
        bad = {"net_value_usd": "NaN", "debt_value_usd": "300", "supply_value_usd": "500"}
        bad_debt = {"net_value_usd": "200", "debt_value_usd": "NaN", "supply_value_usd": "500"}
        pj = _positions(
            _leg("200", details=bad),
            _leg("200", details=bad_debt, asset="WETH"),
            _leg("-300", "BORROW"),
        )
        assert _legacy_net_supply_debt(pj) == Decimal("0")
        assert _snapshot_debt_mark(pj) == Decimal("300")


# ── Empty≠Zero guard keys on the RAW mark (delta-review finding 3) ───────


class TestUnmeasuredDeployedGuardUsesRawMark:
    def test_legacy_row_with_unmeasured_deployed_is_still_refused(self):
        """On a legacy net-shaped row the CORRECTED mark is 0 — if the guard
        keyed on it, the row would slip back into the bracket as ``0 + cash``
        with the entire (unmeasured) net supply missing. The guard must key on
        the RAW mark and refuse."""
        pj = _positions(_leg("200", details=dict(_LEGACY_NET)), _leg("-300", "BORROW"))
        assert _snapshot_equity(_row(None, "100", pj)) is None


# ── unreadable-payload diagnostic (VIB-6703) ─────────────────────────────


class TestUnreadablePayloadDiagnostic:
    """A positive deployed column with no readable legs scores gross-of-debt
    with nothing to show for it; the count makes that shape LOUD in G4/G6."""

    def test_counts_positive_deployed_rows_with_no_legs(self):
        from almanak.framework.accounting.accountant_test import _unreadable_payload_rows

        rows = [
            {"total_value_usd": "500", "positions_json": "[]"},  # incoherent — counted
            {"total_value_usd": "500", "positions_json": "not json"},  # unparsable — counted
            {"total_value_usd": "500", "positions_json": None},  # absent — counted
            {"total_value_usd": "0", "positions_json": "[]"},  # coherent flat row — not counted
            {"total_value_usd": "", "positions_json": "[]"},  # unmeasured — not counted (G8's job)
            {  # coherent leveraged row — not counted
                "total_value_usd": "500",
                "positions_json": _positions(_leg("500", details=dict(_GROSS_MARKED)), _leg("-300", "BORROW")),
            },
        ]
        assert _unreadable_payload_rows(rows) == 3

    def test_g6_decomposition_carries_the_count(self, tmp_path):
        import shutil
        import sqlite3
        from pathlib import Path

        from almanak.framework.accounting.accountant_test import run_against_sqlite

        fixture = (
            Path(__file__).resolve().parents[3]
            / "tests"
            / "fixtures"
            / "accounting"
            / "looping_same_reserve"
            / "expected_baseline.sqlite"
        )
        pristine = run_against_sqlite(fixture, primitive="looping", strict_lifecycle=True)
        g6 = next(c for c in pristine.cells if c.cell_id == "G6")
        assert g6.decomposition["unreadable_payload_rows"] == "0"

        degraded = tmp_path / "expected_baseline.sqlite"
        shutil.copy2(fixture, degraded)
        conn = sqlite3.connect(str(degraded))
        conn.execute("UPDATE portfolio_snapshots SET positions_json = '[]'")
        conn.commit()
        conn.close()
        rep = run_against_sqlite(degraded, primitive="looping", strict_lifecycle=True)
        g6d = next(c for c in rep.cells if c.cell_id == "G6")
        # every priced row still claims a positive deployed value with no legs
        assert int(g6d.decomposition["unreadable_payload_rows"]) >= 6


# ── missing-debt-leg diagnostic (VIB-6699 loudness) ──────────────────────


class TestMissingDebtLegDiagnostic:
    """A marked-gross SUPPLY leg declaring reserve debt with no surviving
    BORROW sibling is the partial-discovery overstatement shape — it must be
    counted, not silently plausible."""

    def _rows(self, *legs):
        return [{"total_value_usd": "500", "positions_json": _positions(*legs)}]

    def test_counts_the_partial_loss_shape(self):
        from almanak.framework.accounting.accountant_test import _missing_debt_leg_rows

        rows = self._rows(_leg("500", details=dict(_GROSS_MARKED)))  # debt declared, no BORROW leg
        assert _missing_debt_leg_rows(rows) == 1

    def test_present_sibling_is_not_counted(self):
        from almanak.framework.accounting.accountant_test import _missing_debt_leg_rows

        rows = self._rows(_leg("500", details=dict(_GROSS_MARKED)), _leg("-300", "BORROW"))
        assert _missing_debt_leg_rows(rows) == 0

    def test_nan_valued_sibling_counts_as_missing(self):
        from almanak.framework.accounting.accountant_test import _missing_debt_leg_rows

        rows = self._rows(_leg("500", details=dict(_GROSS_MARKED)), _leg("NaN", "BORROW"))
        assert _missing_debt_leg_rows(rows) == 1

    def test_debt_free_marked_leg_is_not_counted(self):
        from almanak.framework.accounting.accountant_test import _missing_debt_leg_rows

        details = {
            "supply_leg_convention": "gross",
            "supply_value_usd": "500",
            "net_value_usd": "500",
            "debt_value_usd": "0",
        }
        rows = self._rows(_leg("500", details=details))
        assert _missing_debt_leg_rows(rows) == 0

    def test_unmarked_and_account_state_legs_are_out_of_detection_scope(self):
        from almanak.framework.accounting.accountant_test import _missing_debt_leg_rows

        rows = self._rows(
            _leg("200", details=dict(_LEGACY_NET)),  # legacy unmarked — bound documented
            _leg("500", details={"asset": "WETH", "market_id": "0xabc"}, asset="WETH"),  # account-state
        )
        assert _missing_debt_leg_rows(rows) == 0


# ── NaN canonical-field hardening (Empty≠Zero: NaN is not a measurement) ─


class TestNanCanonicalFields:
    def test_nan_leg_value_is_unmeasured_not_a_crash(self):
        from almanak.framework.valuation.net_debt import net_debt_from_positions_json

        pj = _positions(_leg("500", details=dict(_GROSS_MARKED)), _leg("NaN", "BORROW"))
        count, debt_mark, _dc = net_debt_from_positions_json(pj)
        assert debt_mark == Decimal("0")  # the NaN leg is skipped as unmeasured, never compared

    def test_nan_total_with_debt_is_refused(self):
        pj = _positions(_leg("500", details=dict(_GROSS_MARKED)), _leg("-300", "BORROW"))
        assert _snapshot_equity(_row("NaN", "100", pj)) is None

    def test_nan_cash_is_unmeasured_not_zero_coerced_crash(self):
        pj = _positions(_leg("500", details=dict(_GROSS_MARKED)), _leg("-300", "BORROW"))
        assert _snapshot_equity(_row("500", "NaN", pj)) == Decimal("200")


# ── G9 confidence guard: both directions pinned (delta-review finding 2) ─


class TestG9UsdBearing:
    def _g9(self, row):
        return _cell_g9_confidence([row], [])

    def test_zero_columns_with_live_debt_leg_still_needs_confidence(self):
        """The class the netted-equity form caught (equity == −debt ≠ 0) and a
        column-only rewrite would wave through: 0/0 columns, live BORROW leg,
        no stamp → flagged."""
        pj = _positions(_leg("-300", "BORROW"))
        row = {"id": 1, "total_value_usd": "0", "available_cash_usd": "0", "positions_json": pj}
        assert self._g9(row).status == "FAIL"

    def test_fully_drawn_netted_zero_still_needs_confidence(self):
        """The widening the rewrite was written for: gross columns whose
        NETTED equity is exactly 0 (deployed 300 − debt 300) previously
        skipped the stamp requirement — must be flagged."""
        pj = _positions(_leg("300", details=dict(_GROSS_MARKED)), _leg("-300", "BORROW"))
        row = {"id": 1, "total_value_usd": "300", "available_cash_usd": "0", "positions_json": pj}
        assert self._g9(row).status == "FAIL"

    def test_stamped_rows_pass(self):
        pj = _positions(_leg("300", details=dict(_GROSS_MARKED)), _leg("-300", "BORROW"))
        row = {
            "id": 1,
            "total_value_usd": "300",
            "available_cash_usd": "0",
            "positions_json": pj,
            "value_confidence": "HIGH",
        }
        assert self._g9(row).status == "PASS"

    def test_truly_empty_row_needs_no_stamp(self):
        row = {"id": 1, "total_value_usd": "0", "available_cash_usd": "0", "positions_json": "[]"}
        assert self._g9(row).status == "PASS"
