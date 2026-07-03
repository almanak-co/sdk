"""VIB-5540 / VIB-5612 — standing primitive-agnostic N-leg reconciliation
invariants.

Two invariants the accounting-matrix path asserts by construction across
primitives (Curve 3pool/4pool/tricrypto/metapool, Balancer, any future N-coin
venue — keyed only on the shared ``coin_symbols`` carrier):

  1. Seam A — the portfolio-snapshot equity universe must cover every coin any
     position touched. Catches "a returned coin fell out of equity" (the G6
     wallet-side gap).
  2. Seam B (LP5 companion) — a fungible LP close's ``principal_recovered_usd``
     must reconcile with the N-complete ``cost_basis_usd`` the typed accounting
     layer measured. Guards against regressing to the 2-coin
     ``position_events.value_usd`` that left principal at zero.

The synthetic cases prove the invariant LOGIC (a good round-trip passes; the
pre-fix defect is caught); the fixture cases assert the shipped Curve fixtures
satisfy it.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from almanak.framework.accounting.accountant_test import (
    check_lp5_principal_matches_cost_basis,
    check_snapshot_covers_position_coins,
)

_FIXTURE_BASE = Path(__file__).resolve().parents[2] / "fixtures" / "accounting"


def _snap(wallet_symbols: list[str], positions: list[dict] | None = None) -> dict:
    return {
        "wallet_balances_json": json.dumps([{"symbol": s, "balance": "1", "value_usd": "1"} for s in wallet_symbols]),
        "positions_json": json.dumps(positions or []),
    }


def _acct_close(
    coin_symbols: list[str] | None,
    cost_basis_usd: str | None = None,
    *,
    ledger_entry_id: str = "le-1",
) -> dict:
    payload: dict = {"event_type": "LP_CLOSE"}
    if coin_symbols is not None:
        payload["coin_symbols"] = coin_symbols
    if cost_basis_usd is not None:
        payload["cost_basis_usd"] = cost_basis_usd
    # ledger_entry_id is the correlation key both the accounting_event and the
    # position_event for the SAME close carry (see _lp_close_correlation_key).
    return {"event_type": "LP_CLOSE", "ledger_entry_id": ledger_entry_id, "payload_json": json.dumps(payload)}


class TestSnapshotCoversPositionCoins:
    def test_good_roundtrip_passes(self) -> None:
        # Close-time snapshot priced all three returned coins into the wallet.
        snapshots = [_snap(["USDC"]), _snap(["DAI", "USDC", "USDT", "ETH"])]
        acct = [_acct_close(["DAI", "USDC", "USDT"])]
        assert check_snapshot_covers_position_coins(snapshots, acct) == []

    def test_returned_coin_missing_from_equity_is_flagged(self) -> None:
        # The pre-fix defect: DAI/USDT returned by the close never entered any
        # snapshot's priced token universe.
        snapshots = [_snap(["USDC", "ETH"]), _snap(["USDC", "ETH"])]
        acct = [_acct_close(["DAI", "USDC", "USDT"])]
        violations = check_snapshot_covers_position_coins(snapshots, acct)
        assert len(violations) == 1
        assert "DAI" in violations[0] and "USDT" in violations[0]

    def test_consolidated_coin_still_passes(self) -> None:
        # Returned coin priced at close-time snapshot, then swapped to USDC by
        # teardown consolidation → gone from the final snapshot but still covered.
        snapshots = [
            _snap(["DAI", "USDC", "USDT", "ETH"]),  # close-time: all priced
            _snap(["USDC", "ETH"]),  # post-consolidation
        ]
        acct = [_acct_close(["DAI", "USDC", "USDT"])]
        assert check_snapshot_covers_position_coins(snapshots, acct) == []

    def test_no_coin_symbols_is_vacuously_true(self) -> None:
        assert check_snapshot_covers_position_coins([_snap(["USDC"])], [_acct_close(None)]) == []

    def test_open_position_coins_covered_by_positions_json(self) -> None:
        # An OPEN N-coin position: coins are deployed (not in wallet) but named
        # in the position row's details.coin_symbols → covered.
        snapshots = [
            _snap(["ETH"], positions=[{"details": {"coin_symbols": ["DAI", "USDC", "USDT"]}}]),
        ]
        acct = [_acct_close(["DAI", "USDC", "USDT"])]
        assert check_snapshot_covers_position_coins(snapshots, acct) == []


class TestLp5PrincipalMatchesCostBasis:
    def _pos_close(self, principal_recovered_usd: str | None, *, ledger_entry_id: str = "le-1") -> dict:
        attr = {"position_type": "LP"}
        if principal_recovered_usd is not None:
            attr["principal_recovered_usd"] = principal_recovered_usd
        # Same correlation key the matching accounting_event carries.
        return {
            "event_type": "CLOSE",
            "ledger_entry_id": ledger_entry_id,
            "attribution_json": json.dumps(attr),
        }

    def test_matching_principal_passes(self) -> None:
        acct = [_acct_close(["DAI", "USDC", "USDT"], cost_basis_usd="300.09")]
        pos = [self._pos_close("300.05")]
        assert check_lp5_principal_matches_cost_basis(pos, acct) == []

    def test_zero_principal_regression_flagged(self) -> None:
        # The exact Seam B bug: N-complete cost_basis measured $300 but the
        # 2-coin value_usd left principal_recovered at 0.
        acct = [_acct_close(["DAI", "USDC", "USDT"], cost_basis_usd="300.09")]
        pos = [self._pos_close("0")]
        violations = check_lp5_principal_matches_cost_basis(pos, acct)
        assert len(violations) == 1
        assert "principal_recovered_usd" in violations[0]

    def test_unmeasured_cost_basis_skipped_empty_ne_zero(self) -> None:
        # cost_basis unmeasured (None) → skip, never compare as zero.
        acct = [_acct_close(["DAI", "USDC", "USDT"], cost_basis_usd=None)]
        pos = [self._pos_close("0")]
        assert check_lp5_principal_matches_cost_basis(pos, acct) == []

    def test_two_coin_venue_not_applicable(self) -> None:
        acct = [_acct_close(None, cost_basis_usd="300")]
        pos = [self._pos_close("0")]
        assert check_lp5_principal_matches_cost_basis(pos, acct) == []

    def test_tightened_relative_bound_flags_half_percent_divergence(self) -> None:
        # On a $50k volatile close, a $300 divergence (0.6%) now FLAGS under the
        # 0.5% bound (the old 2% bound silently tolerated up to ~$1k).
        acct = [_acct_close(["USDT", "WBTC", "WETH"], cost_basis_usd="50000")]
        pos = [self._pos_close("50300")]  # 0.6% high
        violations = check_lp5_principal_matches_cost_basis(pos, acct)
        assert len(violations) == 1

    def test_within_half_percent_passes(self) -> None:
        acct = [_acct_close(["USDT", "WBTC", "WETH"], cost_basis_usd="50000")]
        pos = [self._pos_close("50200")]  # 0.4% high → within 0.5%
        assert check_lp5_principal_matches_cost_basis(pos, acct) == []

    def test_absolute_floor_tolerates_rounding_on_small_position(self) -> None:
        # A $150 position with a $0.80 rounding delta (>0.5% = $0.75) is NOT
        # flagged thanks to the $1.00 absolute floor.
        acct = [_acct_close(["DAI", "USDC", "USDT"], cost_basis_usd="150.00")]
        pos = [self._pos_close("150.80")]
        assert check_lp5_principal_matches_cost_basis(pos, acct) == []

    # ── Position-identity correlation (CodeRabbit Major) ──

    def test_multi_position_flags_only_the_regressed_one(self) -> None:
        # Two concurrent LP closes. Position A reconciles ($300 vs $300.05);
        # position B has a zeroed principal (the Seam-B regression) vs a measured
        # $500 cost_basis. The invariant must flag EXACTLY B — never cross-match
        # A's good principal to B's cost_basis (or vice-versa) and mask it.
        acct = [
            _acct_close(["DAI", "USDC", "USDT"], cost_basis_usd="300.09", ledger_entry_id="le-A"),
            _acct_close(["USDT", "WBTC", "WETH"], cost_basis_usd="500.00", ledger_entry_id="le-B"),
        ]
        pos = [
            self._pos_close("300.05", ledger_entry_id="le-A"),  # A: good
            self._pos_close("0", ledger_entry_id="le-B"),  # B: regressed
        ]
        violations = check_lp5_principal_matches_cost_basis(pos, acct)
        assert len(violations) == 1
        assert "le-B" in violations[0]
        assert "le-A" not in violations[0]

    def test_multi_position_good_pair_not_masked_by_other(self) -> None:
        # Symmetric: B is regressed, but A's healthy pair must still be reported
        # as clean — i.e. the presence of B's cost_basis (larger) must not be the
        # value A is compared against. Here A would FALSELY reconcile if compared
        # to B's $500 cost_basis (|300.05-500| huge) — correlation prevents that.
        acct = [
            _acct_close(["DAI", "USDC", "USDT"], cost_basis_usd="300.09", ledger_entry_id="le-A"),
            _acct_close(["USDT", "WBTC", "WETH"], cost_basis_usd="500.00", ledger_entry_id="le-B"),
        ]
        pos = [self._pos_close("300.05", ledger_entry_id="le-A")]  # only A closed so far
        # A reconciles against ITS OWN cost_basis (300.09), not B's (500) → clean.
        assert check_lp5_principal_matches_cost_basis(pos, acct) == []

    def test_cross_stream_key_mismatch_skips_not_crossmatches(self) -> None:
        # A close whose position_event carries a DIFFERENT ledger_entry_id than
        # any accounting cost_basis is skipped (no correlation), never matched to
        # an unrelated position's cost_basis.
        acct = [_acct_close(["DAI", "USDC", "USDT"], cost_basis_usd="300.09", ledger_entry_id="le-A")]
        pos = [self._pos_close("0", ledger_entry_id="le-ORPHAN")]
        assert check_lp5_principal_matches_cost_basis(pos, acct) == []

    def test_tx_hash_fallback_correlation(self) -> None:
        # When ledger_entry_id is absent, tx_hash is the fallback correlation key.
        acct = [
            {
                "event_type": "LP_CLOSE",
                "tx_hash": "0xabc",
                "payload_json": json.dumps(
                    {"event_type": "LP_CLOSE", "coin_symbols": ["DAI", "USDC", "USDT"], "cost_basis_usd": "300.00"}
                ),
            }
        ]
        pos = [
            {
                "event_type": "CLOSE",
                "tx_hash": "0xabc",
                "attribution_json": json.dumps({"position_type": "LP", "principal_recovered_usd": "0"}),
            }
        ]
        violations = check_lp5_principal_matches_cost_basis(pos, acct)
        assert len(violations) == 1
        assert "0xabc" in violations[0]


# ── Wiring: run_against_sqlite surfaces the invariants as a report diagnostic ──


def test_run_against_sqlite_populates_nleg_findings() -> None:
    """M1 — the invariants are called from the canonical evaluation path, not
    only the unit test. The frozen tricrypto fixture carries coin_symbols with
    pre-Seam-A snapshots, so run_against_sqlite must surface a non-empty
    diagnostic (without changing any cell status)."""
    from almanak.framework.accounting.accountant_test import run_against_sqlite
    from almanak.framework.primitives.types import Primitive

    db = _FIXTURE_BASE / "lp_curve_tricrypto" / "expected_baseline.sqlite"
    if not db.exists():
        pytest.skip(f"fixture DB missing: {db}")
    report = run_against_sqlite(db, primitive=Primitive.LP)
    # The diagnostic field exists and is populated on this violating fixture.
    assert report.nleg_invariant_findings
    assert any("equity universe" in f for f in report.nleg_invariant_findings)
    # It's a NON-failing diagnostic: it also lands in the JSON + markdown.
    assert report.to_json()["nleg_invariant_findings"] == report.nleg_invariant_findings
    assert "N-leg reconciliation diagnostics" in report.format_markdown()


def test_run_against_sqlite_no_findings_for_two_coin_fixture() -> None:
    """A 2-coin / no-coin_symbols fixture yields no findings (fail-safe)."""
    from almanak.framework.accounting.accountant_test import run_against_sqlite
    from almanak.framework.primitives.types import Primitive

    db = _FIXTURE_BASE / "lp" / "expected_baseline.sqlite"
    if not db.exists():
        pytest.skip(f"fixture DB missing: {db}")
    report = run_against_sqlite(db, primitive=Primitive.LP)
    assert report.nleg_invariant_findings == []


# ── Fixture-level assertions (green once the Curve fixtures carry coin_symbols) ──


def _load_rows(db_path: Path, table: str) -> list[dict]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute(f"SELECT * FROM {table}")]  # noqa: S608 — fixed table names
    finally:
        conn.close()


@pytest.mark.parametrize(
    "fixture",
    [
        "lp_curve",
        # The frozen tricrypto snapshots predate the Seam A fix (they carry
        # coin_symbols but their portfolio_snapshots were captured before the
        # N-coin token-universe union), so the invariant correctly FLAGS the
        # missing WBTC/USDT until the fixture is regenerated from a tricrypto
        # round-trip on the fixed valuer (VIB-5566). Non-strict xfail: when the
        # fixture is regenerated the check passes and this xpasses.
        pytest.param(
            "lp_curve_tricrypto",
            marks=pytest.mark.xfail(
                reason="VIB-5566: frozen tricrypto snapshots predate Seam A — regenerate to enforce",
                strict=False,
            ),
        ),
    ],
)
def test_curve_fixtures_satisfy_snapshot_coverage(fixture: str) -> None:
    db = _FIXTURE_BASE / fixture / "expected_baseline.sqlite"
    if not db.exists():
        pytest.skip(f"fixture DB missing: {db}")
    snapshots = _load_rows(db, "portfolio_snapshots")
    acct_events = _load_rows(db, "accounting_events")
    # Skip vacuously-true legacy captures (pre-VIB-5429, no coin_symbols); once
    # regenerated with the N-leg fix the assertion becomes meaningful.
    from almanak.framework.accounting.accountant_test import _acct_event_coin_symbols

    if not _acct_event_coin_symbols(acct_events):
        pytest.skip(f"{fixture} fixture predates coin_symbols stamping — regenerate to enforce")
    assert check_snapshot_covers_position_coins(snapshots, acct_events) == []
