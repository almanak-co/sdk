"""G6 must not PASS on a tolerance larger than the capital at risk (VIB-5826).

``eps = max(floor, eps_pct * scaling_base)``. The scaling base is a *notional*, and
a notional is derived from leg amounts — so a leg-scaling defect inflates ε. Once
ε meets or exceeds the capital at risk, ``gap <= eps`` holds for every
reconciliation error the run could physically produce: the cell reports PASS while
verifying nothing.

This is not hypothetical. The 2026-07-15 24-row matrix sweep
(``docs/internal/qa/g6-matrix-sweep-2026-07-15.md`` §6) found
``lp-uniswap_v3-ethereum`` scoring 21/22 — the best row in the matrix — with:

    gap      = $0.6897        (fails the $0.10 floor AND the legacy $0.50 floor)
    ε        = $5,160,574.46  = 0.0025 x notional_traded
    notional = $2,064,229,782 on $191,861 of capital

The $2.06bn notional came from token decimals applied by config *label* order
("WETH/USDC/500") instead of on-chain ``token0()``/``token1()`` — Ethereum's real
pool is USDC-first, so the WETH leg was scaled by 1e6 and priced at $1.00/unit,
yielding a $1.03bn phantom cost basis per leg. That row's baseline recorded
``G6: FAIL``, so an outcome-only ratchet read the corruption as ``FAIL -> PASS`` —
an *improvement*. Nothing else in the 21-cell matrix noticed: LP1/LP3/LP4 all
passed with $1.03bn on the books.

The guard is a LOGICAL bound, not a tuned threshold: below it the cell can still
discriminate; at or above it the cell is dead. Only the *scaled* term is bounded —
the floor is a deliberate rounding/oracle-noise ε and a dust-sized run must not
trip on it. Across that sweep the worst legitimate row sat at
``scaled/capital = 8.3e-6``, five orders of magnitude of headroom.

The core pair are identical CLEAN round-trips that differ ONLY in the swap's notional,
and they assert on the SAME G6 cell:

  * notional $2bn on $1k capital -> ε dwarfs capital -> G6 FAIL (``ε_vacuous``).
    Without the guard this case PASSes — that is the bug, and this test is the
    mutation check for it.
  * notional $1k on $1k capital  -> ε = $2.50 -> ordinary reconciliation -> G6 PASS.
    Proves the guard discriminates rather than blanket-failing anything large.

The remaining cases pin the guard's edges — zero capital (the MOST vacuous row, not an
exempt one), the emitted vacuity ratio, and the ``gap > ε`` case where the vacuity
branch must stand down in favour of the ordinary gap FAIL so the diagnostic stays
honest. All three were review findings on PR #3290, and each is a case where the
VERDICT is unchanged and only the DIAGNOSTIC (or a hole behind it) differs — which is
exactly the class no outcome-only ratchet can see.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from almanak.framework.accounting.accountant_test import run_against_sqlite

_DEP = "deployment:g65826"
_CHAIN = "ethereum"
_PROTO = "uniswap_v3"
_WALLET = "0x000000000000000000000000000000000000bbbb"
_CYCLE = "cycle-g65826-001"

# Equity is flat at $1000 across both endpoints, so wallet PnL = 0 and the ONLY
# thing separating the two cases is the notional the swap declares.
_CAPITAL = "1000.0"

# 0.0025 (lp eps_pct) x 2e9 = $5,000,000 >> $1000 capital -> vacuous.
_NOTIONAL_VACUOUS = "2000000000"
# 0.0025 x 1000 = $2.50 < $1000 capital -> a normal, discriminating tolerance.
_NOTIONAL_SANE = "1000"

_DDL = (
    """
    CREATE TABLE transaction_ledger (
        id TEXT PRIMARY KEY, cycle_id TEXT NOT NULL, deployment_id TEXT NOT NULL,
        execution_mode TEXT DEFAULT '', timestamp TEXT NOT NULL, intent_type TEXT NOT NULL,
        token_in TEXT, amount_in TEXT, token_out TEXT, amount_out TEXT,
        effective_price TEXT, slippage_bps REAL, gas_used INTEGER, gas_usd TEXT,
        tx_hash TEXT, chain TEXT, protocol TEXT, success BOOLEAN NOT NULL DEFAULT 1,
        error TEXT, extracted_data_json TEXT DEFAULT '', price_inputs_json TEXT DEFAULT '',
        pre_state_json TEXT DEFAULT '', post_state_json TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE accounting_events (
        id TEXT PRIMARY KEY, deployment_id TEXT NOT NULL, cycle_id TEXT NOT NULL,
        execution_mode TEXT NOT NULL, timestamp TEXT NOT NULL, chain TEXT NOT NULL,
        protocol TEXT NOT NULL, wallet_address TEXT NOT NULL, event_type TEXT NOT NULL,
        position_key TEXT NOT NULL, ledger_entry_id TEXT, tx_hash TEXT, confidence TEXT NOT NULL,
        payload_json TEXT NOT NULL, schema_version INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE TABLE portfolio_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT, deployment_id TEXT NOT NULL,
        cycle_id TEXT DEFAULT '', execution_mode TEXT DEFAULT '', timestamp TEXT NOT NULL,
        iteration_number INTEGER DEFAULT 0, total_value_usd TEXT NOT NULL,
        available_cash_usd TEXT NOT NULL, deployed_capital_usd TEXT DEFAULT '0',
        wallet_total_value_usd TEXT DEFAULT '0', value_confidence TEXT DEFAULT 'HIGH',
        positions_json TEXT NOT NULL, token_prices_json TEXT DEFAULT '{}',
        wallet_balances_json TEXT DEFAULT '[]', chain TEXT, created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE position_events (
        id TEXT PRIMARY KEY, deployment_id TEXT NOT NULL, cycle_id TEXT DEFAULT '',
        execution_mode TEXT DEFAULT '', position_id TEXT NOT NULL, position_type TEXT NOT NULL,
        event_type TEXT NOT NULL, timestamp TEXT NOT NULL, protocol TEXT, chain TEXT,
        token0 TEXT, token1 TEXT, amount0 TEXT, amount1 TEXT, value_usd TEXT,
        tick_lower INTEGER, tick_upper INTEGER, liquidity TEXT, in_range BOOLEAN,
        fees_token0 TEXT, fees_token1 TEXT, leverage TEXT, entry_price TEXT, mark_price TEXT,
        unrealized_pnl TEXT, is_long BOOLEAN, tx_hash TEXT, gas_usd TEXT, ledger_entry_id TEXT,
        protocol_fees_usd TEXT DEFAULT '', attribution_json TEXT DEFAULT '{}',
        attribution_version INTEGER DEFAULT 0
    )
    """,
    """
    CREATE TABLE portfolio_metrics (
        deployment_id TEXT PRIMARY KEY, initial_value_usd TEXT NOT NULL,
        initial_timestamp TEXT NOT NULL, deposits_usd TEXT DEFAULT '0',
        withdrawals_usd TEXT DEFAULT '0', gas_spent_usd TEXT DEFAULT '0',
        total_value_usd TEXT DEFAULT '0', positions_json TEXT DEFAULT '[]',
        cycle_id TEXT, execution_mode TEXT DEFAULT '', is_complete BOOLEAN DEFAULT 1,
        updated_at TEXT NOT NULL
    )
    """,
)

_TS_BASE = datetime(2026, 7, 15, 0, 0, 0, tzinfo=UTC)


def _ts(off: int) -> str:
    return (_TS_BASE + timedelta(seconds=off)).isoformat()


def _wallet_balances(capital_usd: str = _CAPITAL) -> str:
    # A flat USDC bag (stable, no revaluation) so the ambient inventory term is 0
    # and the tolerance is the only thing under test.
    return json.dumps(
        [
            {
                "symbol": "USDC",
                "balance": capital_usd,
                "value_usd": capital_usd,
                "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                "price_usd": "1",
            }
        ]
    )


def _swap_payload(notional_usd: str, realized_pnl_usd: str | None = None) -> str:
    """An acquiring SWAP (no prior FIFO lot) that declares ``notional_usd`` of volume.

    ``realized_pnl_usd=None`` with ``cost_basis_recorded`` is the legitimate
    no-prior-basis shape (VIB-4394) — it contributes ZERO to the component sum, so
    the gap stays 0 and the ONLY effect of ``notional_usd`` is on the ε scaling base.
    That isolates the guard: both cases below reconcile perfectly; they differ only
    in whether the tolerance is still capable of failing.
    """
    return json.dumps(
        {
            "event_type": "SWAP",
            "protocol": _PROTO,
            "token_in": "USDC",
            "token_out": "WETH",
            "amount_in": notional_usd,
            "amount_out": "0.4",
            "amount_in_usd": notional_usd,
            "amount_out_usd": notional_usd,
            "realized_pnl_usd": realized_pnl_usd,
            "realized_pnl_usd_matched": None,
            "cost_basis_recorded": True,
            "confidence": "HIGH",
            "swap_position_key": f"swap:{_CHAIN}:{_WALLET.lower()}",
        }
    )


def _build_db(
    path: Path,
    *,
    notional_usd: str,
    capital_usd: str = _CAPITAL,
    realized_pnl_usd: str | None = None,
    ledger_offset: int = 30,
    gas_usd: str = "0",
) -> None:
    """A net-flat round-trip whose only typed event is an acquiring SWAP.

    ``capital_usd`` sets equity at BOTH endpoints, so wallet PnL is 0 regardless and
    the row always reconciles — isolating the tolerance as the only variable.

    ``realized_pnl_usd`` overrides the SWAP's realized leg. Default ``None`` is the
    no-prior-basis shape that contributes ZERO to the component sum (gap 0). Setting
    it moves the COMPONENT side only, so the gap is exactly ``|realized_pnl_usd|``
    while equity stays flat. That is the only way to make the gap exceed a vacuous ε:
    ``capital = max(|initial|, |final|)`` (``accountant_test.py:1470``), so with
    non-negative equities an equity-derived gap can never outrun capital — and
    therefore can never outrun an ε that is itself >= capital.

    ``ledger_offset`` places the ledger/accounting row relative to the snapshot
    endpoints, which sit at 0s and 60s. The default 30 puts it INSIDE the wallet
    bracket (``covers=True``), which is why every test written before VIB-6434 was
    unable to reach the VIB-5854 window branch at all. A NEGATIVE offset puts it
    before ``priced[0]`` and makes the window uncovered.

    ``gas_usd`` is the gas on that row. It must be non-zero for a pre-window row to
    carry a measured, attributable spend — with the default "0" the window branch
    still fires but explains nothing.
    """
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(str(path))
    try:
        for ddl in _DDL:
            conn.execute(ddl)

        conn.execute(
            "INSERT INTO transaction_ledger (id, cycle_id, deployment_id, execution_mode,"
            " timestamp, intent_type, token_in, amount_in, token_out, amount_out,"
            " gas_usd, tx_hash, chain, protocol, success) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
            (
                "ldg-swap-1",
                _CYCLE,
                _DEP,
                "paper",
                _ts(ledger_offset),
                "SWAP",
                "USDC",
                notional_usd,
                "WETH",
                "0.4",
                gas_usd,
                "0xswap",
                _CHAIN,
                _PROTO,
            ),
        )
        conn.execute(
            "INSERT INTO accounting_events (id, deployment_id, cycle_id, execution_mode,"
            " timestamp, chain, protocol, wallet_address, event_type, position_key,"
            " ledger_entry_id, tx_hash, confidence, payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "ae-swap-1",
                _DEP,
                _CYCLE,
                "paper",
                _ts(ledger_offset),
                _CHAIN,
                _PROTO,
                _WALLET,
                "SWAP",
                "",
                "ldg-swap-1",
                "0xswap",
                "HIGH",
                _swap_payload(notional_usd, realized_pnl_usd),
            ),
        )

        # Two endpoint snapshots — equity flat => wallet PnL 0; any gap comes
        # from the component side via ``realized_pnl_usd``.
        for it, off, equity in ((1, 0, capital_usd), (2, 60, capital_usd)):
            conn.execute(
                "INSERT INTO portfolio_snapshots (deployment_id, cycle_id, execution_mode, timestamp,"
                " iteration_number, total_value_usd, available_cash_usd, deployed_capital_usd,"
                " wallet_total_value_usd, value_confidence, positions_json, token_prices_json,"
                " wallet_balances_json, chain, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    _DEP,
                    _CYCLE,
                    "paper",
                    _ts(off),
                    it,
                    "0",
                    equity,
                    "0",
                    equity,
                    "HIGH",
                    "[]",
                    "{}",
                    _wallet_balances(equity),
                    _CHAIN,
                    _ts(off),
                ),
            )

        conn.execute(
            "INSERT INTO portfolio_metrics (deployment_id, initial_value_usd, initial_timestamp,"
            " gas_spent_usd, total_value_usd, cycle_id, execution_mode, is_complete, updated_at)"
            " VALUES (?,?,?,?,?,?,?,1,?)",
            (_DEP, capital_usd, _ts(0), "0", capital_usd, _CYCLE, "paper", _ts(60)),
        )
        conn.commit()
    finally:
        conn.close()


def _g6(report) -> tuple[str, dict, str]:
    for c in report.cells:
        if c.cell_id == "G6":
            return c.status, c.decomposition, c.diagnostic
    raise AssertionError("G6 cell not found")


def test_vacuous_tolerance_does_not_pass_g6(tmp_path: Path) -> None:
    """ε >= capital cannot be a PASS — the cell would be unfalsifiable.

    MUTATION CHECK: delete the ``eps_vacuous`` guard in ``accountant_test.py`` and
    this test must FAIL (the row PASSes with a $5M tolerance on $1k of capital).
    A guard that still passes with the defect reintroduced is not a guard.
    """
    db = tmp_path / "g6_5826_vacuous.sqlite"
    _build_db(db, notional_usd=_NOTIONAL_VACUOUS)
    status, decomp, diagnostic = _g6(run_against_sqlite(db, primitive="lp"))

    # The books genuinely reconcile — this row is NOT failing on a real gap...
    assert Decimal(decomp["gap_usd"]) == Decimal("0")
    # ...but the tolerance has outgrown the capital at risk...
    assert decomp["ε_vacuous"] == "True"
    assert Decimal(decomp["ε_threshold_usd"]) >= Decimal(_CAPITAL)
    assert Decimal(decomp["ε_scaled_over_capital"]) > Decimal("1")
    # ...so the cell must NOT claim a pass it cannot have earned.
    assert status == "FAIL", decomp
    # The diagnostic must point at the SCALING BASE — the actual defect — not just
    # announce a failure. A reader who sees only "G6 FAIL" repeats this whole sweep.
    assert "vacuous" in diagnostic
    assert "scaling base" in diagnostic


def test_sane_tolerance_still_passes_g6(tmp_path: Path) -> None:
    """Control: an ordinary notional keeps the ordinary PASS (no blanket-failing)."""
    db = tmp_path / "g6_5826_sane.sqlite"
    _build_db(db, notional_usd=_NOTIONAL_SANE)
    status, decomp, _ = _g6(run_against_sqlite(db, primitive="lp"))

    assert decomp["ε_vacuous"] == "False"
    # 0.0025 x 1000 = $2.50 of scaled tolerance against $1000 of capital.
    assert Decimal(decomp["ε_scaled_over_capital"]) < Decimal("1")
    assert status == "PASS", decomp


def test_zero_capital_with_nonzero_notional_is_vacuous(tmp_path: Path) -> None:
    """Zero capital + a positive scaled ε is the MOST vacuous row, not an exempt one.

    Regression guard for a hole in the first cut of this guard (caught in review on
    PR #3290): the vacuity test was written as ``capital > 0 and eps_scaled >= capital``
    so that the ratio's divisor stayed safe. That predicate is False when capital is
    zero — suppressing the guard on precisely the row where nothing is at stake and
    the tolerance is still positive, leaving that false-green class open.

    The test and the ratio have different preconditions: only the RATIO needs a
    non-zero divisor. Here the ratio is undefined (Empty != Zero: emitted as "", not
    0) while the verdict is unambiguous.
    """
    db = tmp_path / "g6_5826_zero_capital.sqlite"
    _build_db(db, notional_usd=_NOTIONAL_SANE, capital_usd="0")
    status, decomp, diagnostic = _g6(run_against_sqlite(db, primitive="lp"))

    assert Decimal(decomp["capital_usd"]) == Decimal("0")
    # 0.0025 x 1000 = $2.50 of tolerance against $0 of capital.
    assert Decimal(decomp["ε_threshold_usd"]) > Decimal("0")
    assert decomp["ε_vacuous"] == "True"
    # Ratio is undefined, not zero — and must not crash the diagnostic.
    assert decomp["ε_scaled_over_capital"] == ""
    assert "capital is zero" in diagnostic
    assert status == "FAIL", decomp


def test_vacuous_eps_with_a_real_gap_reports_the_ordinary_gap_fail(tmp_path: Path) -> None:
    """A gap that exceeds even a vacuous ε is an ordinary gap FAIL — not a vacuity FAIL.

    Regression guard for a contradiction caught in review on PR #3290: the guard
    was written as a bare ``if eps_vacuous:`` placed above the ``gap <= eps`` PASS
    branch, while its own comment claimed *"a gap that exceeds even a vacuous ε is
    still reported as an ordinary gap FAIL below"*. It was not — the unconditional
    branch swallowed that case and mislabelled it.

    The verdict is FAIL either way, so no ratchet would ever have caught this. Only
    the DIAGNOSTIC differs — and the diagnostic is the whole point of the cell: the
    vacuity text tells the reader to *"root-cause the scaling base"*, which on this
    row is a wild goose chase. The books really are off by $1,500; the tolerance is
    incidental. Sending someone after a mis-scaled leg amount when the actual defect
    is a $1,500 reconciliation error is exactly the confident-wrong signal this epic
    exists to remove.

    Construction: ε_scaled = 0.0025 x $400,000 = $1,000 == capital -> vacuous. Equity
    stays flat (wallet PnL 0) and the swap books a $1,500 realized LOSS, so the gap is
    $1,500 > ε and the vacuity branch must NOT fire. The gap has to come from the
    component side: ``capital = max(|initial|, |final|)``, so an equity-derived gap can
    never outrun capital, hence never outrun an ε that is already >= capital.
    """
    db = tmp_path / "g6_5826_vacuous_with_gap.sqlite"
    _build_db(db, notional_usd="400000", capital_usd="1000.0", realized_pnl_usd="-1500")
    status, decomp, diagnostic = _g6(run_against_sqlite(db, primitive="lp"))

    # Preconditions: the tolerance IS vacuous...
    assert decomp["ε_vacuous"] == "True", decomp
    # ...and yet the gap outruns even it.
    gap = Decimal(decomp["gap_usd"])
    assert gap == Decimal("1500"), decomp
    assert gap > Decimal(decomp["ε_threshold_usd"]), decomp

    # FAIL either way — that is precisely why only the diagnostic can catch this.
    assert status == "FAIL", decomp
    # The honest diagnosis is the real gap, NOT the tolerance.
    assert "vacuous" not in diagnostic, diagnostic
    assert "scaling base" not in diagnostic, diagnostic


def test_vacuity_ratio_is_emitted_on_every_row(tmp_path: Path) -> None:
    """The diagnostic is always present, so drift is visible before it is fatal.

    An outcome-only record is what let the $2.06bn corruption read as an
    improvement; recording the ratio makes the same class a one-line diff.
    """
    db = tmp_path / "g6_5826_emit.sqlite"
    _build_db(db, notional_usd=_NOTIONAL_SANE)
    _, decomp, _ = _g6(run_against_sqlite(db, primitive="lp"))

    assert "ε_vacuous" in decomp
    assert "ε_scaled_over_capital" in decomp
    # Empty != Zero: the ratio is a measured number here, not an empty string.
    assert decomp["ε_scaled_over_capital"] != ""


def test_a_vacuous_eps_cannot_grant_the_window_xfail_waiver(tmp_path: Path) -> None:
    """VIB-6434: the vacuity guard must deny the WAIVER, not only the PASS.

    The guard fires on ``eps_vacuous and gap <= eps``. When ε is vacuous and the gap
    ALSO outruns it, control used to fall through to the VIB-5854 window branch,
    where ``window_residual <= eps`` is tested against that same unfalsifiable ε —
    and grant an **XFAIL**. Since XFAIL outranks FAIL on the status ratchet, a
    tolerance the cell itself calls meaningless was upgrading a failing row.

    Construction (the numbers matter, and none of them is free):

      ε_scaled = 0.0025 x $620,000 = $1,550 >= capital $1,000   -> vacuous
      component = R - G = -$1,500 - $100                        -> gap  = $1,600 > ε
      residual  = |signed - G| = |R|            = $1,500        -> residual <= ε

    ``gap > eps`` is what stands the VIB-5826 guard down; ``residual <= eps`` is what
    would have bought the waiver. Both must hold at once, which is a knife-edge: the
    waiver needs ε inside ``[gap - G, gap)``, an interval only as wide as the
    pre-baseline gas. That is why no committed fixture reaches it, and why this
    synthetic row exists at all.

    NOT hypothetical, though. Six real Hyperliquid DBs in the evidence corpus are
    vacuous with the window uncovered, measured gas, and residual <= ε — satisfying
    every condition here except ``gap > eps``. They are vacuous because HyperCore
    collateral is missing from snapshot equity ($20,439 notional against $0.74 of
    measured capital), not because of a corrupted notional.

    MUTATION CHECK: drop ``not eps_vacuous`` from ``window_explained`` in
    ``accountant_test.py`` and this test must fail with status XFAIL.
    """
    db = tmp_path / "g6_6434_vacuous_window_waiver.sqlite"
    _build_db(
        db,
        notional_usd="620000",
        capital_usd="1000.0",
        realized_pnl_usd="-1500",
        ledger_offset=-30,  # BEFORE priced[0] => the wallet window does not cover the run
        gas_usd="100",  # measured pre-window spend => window_gas is not None
    )
    status, decomp, diagnostic = _g6(run_against_sqlite(db, primitive="lp"))

    # Preconditions — assert the row really is the shape described above, so a
    # future harness change cannot quietly retarget this test at a different class.
    assert decomp["ε_vacuous"] == "True", decomp
    assert decomp["initial_endpoint_covers_run"] == "False", decomp
    assert Decimal(decomp["gap_usd"]) > Decimal(decomp["ε_threshold_usd"]), decomp
    assert Decimal(decomp["window_residual_usd"]) <= Decimal(decomp["ε_threshold_usd"]), decomp

    # The verdict: FAIL, never the XFAIL waiver.
    assert status == "FAIL", (status, diagnostic)

    # And it must say WHY it could not be decided, without asserting a false
    # inequality. The measured arm prints "residual > ε" — which is exactly
    # backwards here, with both numbers beside it.
    assert "cannot be tested" in diagnostic, diagnostic
    assert "compares nothing" in diagnostic, diagnostic
    assert f"residual=${decomp['window_residual_usd']} > " not in diagnostic, diagnostic
    # The actionable instruction is the scaling base, not the books.
    assert "Root-cause the ε scaling base" in diagnostic, diagnostic


def test_a_sound_eps_still_grants_the_window_xfail_waiver(tmp_path: Path) -> None:
    """The must-not-change half: VIB-6434's guard may only bite when ε is vacuous.

    Identical window shape to the test above — uncovered baseline, measured
    pre-window gas, residual within ε — but with a SOUND ε. This row must still
    reach the VIB-5854 waiver and report XFAIL. Without this, a change that simply
    deleted the window branch would pass the test above and look like a fix.
    """
    db = tmp_path / "g6_6434_sound_eps_window.sqlite"
    _build_db(
        db,
        notional_usd="620000",
        capital_usd="2000000",  # capital now dwarfs ε_scaled => NOT vacuous
        realized_pnl_usd=None,  # no component gap; the window gas is the whole story
        ledger_offset=-30,
        gas_usd="100",
    )
    status, decomp, diagnostic = _g6(run_against_sqlite(db, primitive="lp"))

    assert decomp["ε_vacuous"] == "False", decomp
    assert decomp["initial_endpoint_covers_run"] == "False", decomp
    assert status == "XFAIL", (status, diagnostic)
    assert "the endpoints measure different intervals" in diagnostic, diagnostic


def test_a_residue_that_beats_even_a_vacuous_eps_is_named_not_called_undecidable(tmp_path: Path) -> None:
    """A residue exceeding a vacuous ε is MORE conclusive, not less.

    Found by the PR panel (Codex P2 and Grok #1, independently). The vacuous arm
    was gated on ``eps_vacuous`` alone while its comment asserted "this branch is
    reached precisely when residual <= ε" — which control flow did not enforce.
    So a residue of $2,000 against ε=$1,550 was announced as "cannot be decided"
    and "compares nothing", about a comparison that had just produced a definite
    answer.

    The direction matters: ``eps_vacuous`` means ε is too WIDE to discriminate, so
    it can only ever excuse a SMALL residue. One that outruns even it has cleared a
    threshold at or above the whole capital at risk, which is exactly the
    "TWO defects" case — and steering the operator at the scaling base instead
    buries a real reconciliation failure.

    Constructed as the sibling of
    ``test_a_vacuous_eps_cannot_grant_the_window_xfail_waiver``: identical except
    the realized loss is $2,000 rather than $1,500, moving the residue from just
    under ε to just over it. The pair is what pins the boundary — either alone
    would be satisfied by a branch that always picks one arm.
    """
    db = tmp_path / "g6_6434_residue_beats_vacuous_eps.sqlite"
    _build_db(
        db,
        notional_usd="620000",
        capital_usd="1000",
        realized_pnl_usd="-2000",  # residue $2000 > vacuous ε $1550
        ledger_offset=-30,
        gas_usd="100",
    )
    status, decomp, diagnostic = _g6(run_against_sqlite(db, primitive="lp"))

    # Preconditions: vacuous ε, uncovered window, and the residue outruns ε.
    assert decomp["ε_vacuous"] == "True", decomp
    assert decomp["initial_endpoint_covers_run"] == "False", decomp
    assert Decimal(decomp["window_residual_usd"]) > Decimal(decomp["ε_threshold_usd"]), decomp

    assert status == "FAIL", (status, diagnostic)
    # It must NAME the residue, not disclaim the comparison.
    assert "TWO defects on one run." in diagnostic, diagnostic
    assert "compares nothing" not in diagnostic, diagnostic
    assert "cannot be decided" not in diagnostic, diagnostic
    # And it must not send the reader after the scaling base as the only action.
    assert "reconcile the residue as an ordinary gap" in diagnostic, diagnostic
