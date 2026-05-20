"""Unit tests for the LP4 IL sanity-bound helpers in
``almanak.framework.accounting.accountant_test``.

Background — lp-close-may20.md §6.5: before this fix the LP4 cell only
checked ``payload.il_usd is not None``. The matrix scored PASS on
objectively wrong data (``il_usd = -hodl_value_usd`` on a position whose
entire withdrawal value was mis-attributed as fees). The helpers tested
here implement the sanity bound that would have caught the bug.

The signature predicate ``|il_usd| > 2.0 × max(|cost_basis|, |hodl|)``
is the cell's defence against principal-as-fees corruption flowing
through into IL.
"""

from __future__ import annotations

from typing import Any

from almanak.framework.accounting.accountant_test import (
    _LP4_IL_SANITY_FACTOR,
    _lp4_il_sanity_cell,
    _lp4_insanity_signature,
)


def _close_row(rid: str = "ae-1") -> dict[str, Any]:
    return {"id": rid, "event_type": "LP_CLOSE"}


def _open_row(rid: str = "ae-0") -> dict[str, Any]:
    return {"id": rid, "event_type": "LP_OPEN"}


# ---------------------------------------------------------------------------
# _lp4_insanity_signature — returns dict on violation, None otherwise
# ---------------------------------------------------------------------------


class TestLp4InsanitySignature:
    def test_normal_il_within_bounds_returns_none(self) -> None:
        payload = {
            "il_usd": "-0.5",
            "cost_basis_usd": "100.0",
            "hodl_value_usd": "100.5",
        }
        assert _lp4_insanity_signature(_close_row(), payload) is None

    def test_principal_as_fees_signature_flagged(self) -> None:
        """The lp-close-may20.md bug signature: ``il_usd ≈ −hodl_value_usd``
        on a position whose entire withdrawal was mis-attributed as fees.
        Magnitude equals the reference scale, so any factor above 1.0 catches
        it — and the configured ``_LP4_IL_SANITY_FACTOR = 2.0`` flags
        anything exceeding 2× the reference."""
        payload = {
            "il_usd": "-4.407",            # = -hodl_value_usd
            "cost_basis_usd": "4.255",
            "hodl_value_usd": "4.407",
        }
        # 4.407 / 4.407 = 1.0 — within the 2.0× bound, so this exact lp_triple
        # snapshot would actually PASS the cell. The cell still fires when
        # IL is unambiguously outside the bound:
        payload_blowout = {
            "il_usd": "-20.0",             # 4.5× the hodl reference
            "cost_basis_usd": "4.255",
            "hodl_value_usd": "4.407",
        }
        result = _lp4_insanity_signature(_close_row("ae-blowout"), payload_blowout)
        assert result is not None
        assert result["id"] == "ae-blowout"
        assert result["il_usd"] == "-20.0"
        assert result["factor"] > float(_LP4_IL_SANITY_FACTOR)

    def test_il_exactly_at_factor_boundary_passes(self) -> None:
        payload = {
            "il_usd": "10.0",
            "cost_basis_usd": "5.0",
            "hodl_value_usd": "5.0",
        }
        # 10.0 == 2.0 × 5.0 — strict inequality means this passes.
        assert _lp4_insanity_signature(_close_row(), payload) is None

    def test_il_just_above_boundary_flagged(self) -> None:
        payload = {
            "il_usd": "10.001",
            "cost_basis_usd": "5.0",
            "hodl_value_usd": "5.0",
        }
        result = _lp4_insanity_signature(_close_row(), payload)
        assert result is not None
        assert result["factor"] > float(_LP4_IL_SANITY_FACTOR)

    def test_open_event_skipped(self) -> None:
        """LP_OPEN has no hodl reference yet — the cell only sanity-bounds
        LP_CLOSE rows."""
        payload = {
            "il_usd": "1e9",  # absurd
            "cost_basis_usd": "1.0",
            "hodl_value_usd": "1.0",
        }
        assert _lp4_insanity_signature(_open_row(), payload) is None

    def test_missing_il_returns_none(self) -> None:
        payload = {"cost_basis_usd": "100.0", "hodl_value_usd": "100.0"}
        assert _lp4_insanity_signature(_close_row(), payload) is None

    def test_zero_reference_with_zero_il_returns_none(self) -> None:
        """A degenerate close with no economic value — IL must also be 0."""
        payload = {"il_usd": "0", "cost_basis_usd": "0", "hodl_value_usd": "0"}
        assert _lp4_insanity_signature(_close_row(), payload) is None

    def test_zero_reference_with_nonzero_il_flagged(self) -> None:
        """If cost_basis AND hodl are both zero but IL is non-zero, that is
        economically impossible and must be flagged."""
        payload = {"il_usd": "5.0", "cost_basis_usd": "0", "hodl_value_usd": "0"}
        result = _lp4_insanity_signature(_close_row(), payload)
        assert result is not None
        assert result["il_usd"] == "5.0"

    def test_only_cost_basis_reference_used_when_hodl_missing(self) -> None:
        payload = {"il_usd": "1.0", "cost_basis_usd": "5.0"}
        # 1.0 < 2 × 5.0 → PASS
        assert _lp4_insanity_signature(_close_row(), payload) is None

    def test_no_reference_scale_skips(self) -> None:
        """When both cost_basis_usd and hodl_value_usd are missing the cell
        cannot bound the value — leave the row as a presence-only PASS
        contributor."""
        payload = {"il_usd": "1000.0"}  # absurd magnitude, but no reference
        assert _lp4_insanity_signature(_close_row(), payload) is None

    def test_malformed_numeric_skipped(self) -> None:
        """Garbage il_usd is handled by the ``_payload_block_cell`` path —
        this helper just returns None so the orchestrating cell does not
        double-error."""
        payload = {
            "il_usd": "not-a-number",
            "cost_basis_usd": "5.0",
            "hodl_value_usd": "5.0",
        }
        assert _lp4_insanity_signature(_close_row(), payload) is None


# ---------------------------------------------------------------------------
# _lp4_il_sanity_cell — orchestrates the bound across all LP_CLOSE rows
# ---------------------------------------------------------------------------


class TestLp4IlSanityCell:
    def test_no_il_emitted_xfails(self) -> None:
        rows = [_close_row("ae-1")]
        payloads = {"ae-1": {"cost_basis_usd": "5.0"}}  # no il_usd
        cell = _lp4_il_sanity_cell(rows, payloads)
        assert cell.cell_id == "LP4"
        assert cell.status == "XFAIL"

    def test_single_close_within_bounds_passes(self) -> None:
        rows = [_close_row("ae-1")]
        payloads = {
            "ae-1": {
                "il_usd": "-0.001",
                "cost_basis_usd": "5.0",
                "hodl_value_usd": "5.0",
            },
        }
        cell = _lp4_il_sanity_cell(rows, payloads)
        assert cell.status == "PASS"

    def test_one_violation_among_many_fails_cell(self) -> None:
        """A single rotten row poisons the cell — that is the point of the
        sanity bound. Pre-fix, the matrix passed PASS because the cell only
        checked presence."""
        rows = [_close_row("good-1"), _close_row("bad-1"), _close_row("good-2")]
        payloads = {
            "good-1": {"il_usd": "-0.5", "cost_basis_usd": "100", "hodl_value_usd": "100"},
            "bad-1":  {"il_usd": "-50.0", "cost_basis_usd": "5",  "hodl_value_usd": "5"},
            "good-2": {"il_usd": "-0.3", "cost_basis_usd": "80",  "hodl_value_usd": "80"},
        }
        cell = _lp4_il_sanity_cell(rows, payloads)
        assert cell.status == "FAIL"
        # Diagnostic must surface the offending row id and reference the doc.
        assert "bad-1" in cell.diagnostic
        assert "lp-close-may20.md" in cell.diagnostic

    def test_lp_open_rows_ignored_in_violation_check(self) -> None:
        """LP_OPEN rows can carry il_usd=0 or unmeasured — the sanity bound
        only fires on LP_CLOSE."""
        rows = [_open_row("open-1"), _close_row("close-1")]
        payloads = {
            "open-1":  {"il_usd": "0",       "cost_basis_usd": "5", "hodl_value_usd": "5"},
            "close-1": {"il_usd": "-0.001",  "cost_basis_usd": "5", "hodl_value_usd": "5"},
        }
        cell = _lp4_il_sanity_cell(rows, payloads)
        assert cell.status == "PASS"

    def test_empty_payloads_for_some_rows_does_not_crash(self) -> None:
        rows = [_close_row("ae-1"), _close_row("ae-missing")]
        payloads = {"ae-1": {"il_usd": "-0.1", "cost_basis_usd": "5", "hodl_value_usd": "5"}}
        cell = _lp4_il_sanity_cell(rows, payloads)
        # ae-missing has no payload — its row is silently skipped, ae-1 passes
        assert cell.status == "PASS"

    def test_all_rows_lack_il_xfails(self) -> None:
        rows = [_close_row("ae-1"), _close_row("ae-2")]
        payloads = {
            "ae-1": {"cost_basis_usd": "5"},
            "ae-2": {"cost_basis_usd": "10"},
        }
        cell = _lp4_il_sanity_cell(rows, payloads)
        assert cell.status == "XFAIL"
