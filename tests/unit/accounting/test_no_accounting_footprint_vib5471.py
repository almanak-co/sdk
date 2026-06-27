"""VIB-5471 — NO_ACCOUNTING ledger token footprint (consolidation universe lane).

``no_accounting_ledger_token_footprint`` generalises the VIB-5416 measured-ledger
mechanism from the swap-back clamp to the teardown token-consolidation universe:
STAKE / WRAP_NATIVE / CDP-mint acquisitions write a ``transaction_ledger`` row but
ZERO ``accounting_events``, so the accounting-event footprint cannot see the held
token — without this it would never be a consolidation candidate and would strand.

These tests lock:

* every NO_ACCOUNTING category (STAKE / WRAP_NATIVE / MINT_STABLE) contributes
  its tokens — the lane is category-complete, not STAKE-only,
* both legs are returned (disposal + acquisition),
* accounted rows (SWAP) and failed / unknown rows are excluded,
* casing is preserved and ``None`` / ``[]`` yields an empty set.
"""

from __future__ import annotations

from almanak.framework.accounting.basis import no_accounting_ledger_token_footprint


def _row(intent_type, token_in, token_out, *, success=True, _id="r"):
    return {
        "intent_type": intent_type,
        "token_in": token_in,
        "token_out": token_out,
        "amount_in": "1",
        "amount_out": "1",
        "timestamp": "2026-06-25T00:00:00+00:00",
        "success": success,
        "id": _id,
        "chain": "ethereum",
    }


def test_empty_and_none_yield_empty_set():
    assert no_accounting_ledger_token_footprint(None) == set()
    assert no_accounting_ledger_token_footprint([]) == set()


def test_stake_acquisition_enters_footprint_both_legs():
    rows = [_row("STAKE", "ETH", "wstETH")]
    assert no_accounting_ledger_token_footprint(rows) == {"ETH", "wstETH"}


def test_category_complete_not_stake_only():
    """WRAP_NATIVE and CDP MINT_STABLE are NO_ACCOUNTING too — the lane is keyed
    on the accounting CATEGORY, not the STAKE intent (VIB-5471 generalisation)."""
    rows = [
        _row("WRAP_NATIVE", "ETH", "WETH", _id="wrap"),
        _row("MINT_STABLE", "WETH", "DAI", _id="mint"),
    ]
    assert no_accounting_ledger_token_footprint(rows) == {"ETH", "WETH", "DAI"}


def test_accounted_swap_row_excluded():
    """A SWAP row is ACCOUNTED — it already enters the universe via the
    accounting-event footprint; this lane must NOT double-source it."""
    rows = [_row("SWAP", "USDC", "WETH")]
    assert no_accounting_ledger_token_footprint(rows) == set()


def test_failed_and_unknown_rows_excluded():
    rows = [
        _row("STAKE", "ETH", "wstETH", success=False, _id="failed"),
        _row("NOT_A_REAL_INTENT", "ETH", "wstETH", _id="unknown"),
    ]
    assert no_accounting_ledger_token_footprint(rows) == set()


def test_casing_preserved():
    """Canonical mixed-case symbols (USDC.e) survive verbatim — downstream folds
    case only for the membership comparison."""
    rows = [_row("STAKE", "ETH", "USDC.e")]
    assert no_accounting_ledger_token_footprint(rows) == {"ETH", "USDC.e"}


def test_empty_legs_skipped():
    """Empty ≠ Zero: a row with no token symbols contributes nothing."""
    rows = [_row("STAKE", "", "")]
    assert no_accounting_ledger_token_footprint(rows) == set()
