"""Unit tests for pre/post balance reconciliation (VIB-3158).

Covers the pure delta-math and expected-range functions. Runner integration
is covered by the existing runner test suite via the thin delegation stubs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from ...intents.vocabulary import HoldIntent, SwapIntent
from ..reconciliation import (
    BalanceSnapshot,
    DeltaMismatch,
    ExpectedRange,
    ReconciliationReport,
    build_reconciliation_report,
    compute_actual_deltas,
    compute_expected_swap_deltas,
)

# ---------------------------------------------------------------------------
# compute_actual_deltas
# ---------------------------------------------------------------------------


def _snap(balances: dict[str, str]) -> BalanceSnapshot:
    return BalanceSnapshot(
        timestamp=datetime.now(UTC),
        balances={k: Decimal(v) for k, v in balances.items()},
    )


class TestComputeActualDeltas:
    def test_simple_two_token_swap_delta(self) -> None:
        pre = _snap({"USDC": "1000", "WETH": "0"})
        post = _snap({"USDC": "500", "WETH": "0.25"})
        assert compute_actual_deltas(pre, post) == {
            "USDC": Decimal("-500"),
            "WETH": Decimal("0.25"),
        }

    def test_tokens_missing_from_pre_are_skipped(self) -> None:
        pre = _snap({"USDC": "1000"})
        post = _snap({"USDC": "500", "WETH": "0.25"})
        # WETH not in pre — can't compute delta safely, skipped.
        assert compute_actual_deltas(pre, post) == {"USDC": Decimal("-500")}

    def test_tokens_missing_from_post_are_skipped(self) -> None:
        pre = _snap({"USDC": "1000", "WETH": "1"})
        post = _snap({"USDC": "500"})
        assert compute_actual_deltas(pre, post) == {"USDC": Decimal("-500")}

    def test_zero_delta(self) -> None:
        pre = _snap({"USDC": "1000"})
        post = _snap({"USDC": "1000"})
        assert compute_actual_deltas(pre, post) == {"USDC": Decimal("0")}


# ---------------------------------------------------------------------------
# compute_expected_swap_deltas
# ---------------------------------------------------------------------------


def _mock_execution_result(amount_in: str, amount_out: str) -> MagicMock:
    """Minimal execution_result stand-in exposing swap_amounts.amount_in_decimal / amount_out_decimal."""
    sa = MagicMock()
    sa.amount_in_decimal = Decimal(amount_in)
    sa.amount_out_decimal = Decimal(amount_out)
    result = MagicMock()
    result.swap_amounts = sa
    return result


class TestExpectedSwapDeltas:
    def test_symmetric_slippage_bounds_around_enriched_amounts(self) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),  # 1%
        )
        result = _mock_execution_result("1000", "0.5")

        ranges = compute_expected_swap_deltas(intent, result)

        assert "USDC" in ranges and "WETH" in ranges

        # from-token: expect ~ -amount_in, within slippage band
        from_range = ranges["USDC"]
        assert from_range.min == -(Decimal("1000") + Decimal("1000") * Decimal("0.01"))
        assert from_range.max == -(Decimal("1000") - Decimal("1000") * Decimal("0.01"))

        # to-token: expect ~ +amount_out, within slippage band
        to_range = ranges["WETH"]
        assert to_range.min == Decimal("0.5") - Decimal("0.5") * Decimal("0.01")
        assert to_range.max == Decimal("0.5") + Decimal("0.5") * Decimal("0.01")

    def test_gas_payment_extends_from_token_lower_bound(self) -> None:
        intent = SwapIntent(
            from_token="ETH",
            to_token="USDC",
            amount_usd=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )
        result = _mock_execution_result("0.05", "100")
        gas = Decimal("0.002")

        ranges = compute_expected_swap_deltas(intent, result, gas_token="ETH", gas_cost_native=gas)

        # The allowable outflow of ETH is stretched by gas on the lower bound.
        from_range = ranges["ETH"]
        assert from_range.min == -(Decimal("0.05") + Decimal("0.05") * Decimal("0.005") + gas)

    def test_returns_empty_when_execution_result_missing(self) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),
        )
        assert compute_expected_swap_deltas(intent, None) == {}

    def test_returns_empty_when_swap_amounts_missing(self) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),
        )
        result = MagicMock()
        result.swap_amounts = None
        assert compute_expected_swap_deltas(intent, result) == {}

    def test_returns_empty_when_amounts_nonpositive(self) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),
        )
        # Non-positive amounts surface as legacy warnings elsewhere, not as an
        # expected-range check.
        result = _mock_execution_result("0", "0")
        assert compute_expected_swap_deltas(intent, result) == {}


# ---------------------------------------------------------------------------
# ExpectedRange.contains
# ---------------------------------------------------------------------------


class TestExpectedRange:
    def test_inclusive_bounds(self) -> None:
        r = ExpectedRange("USDC", Decimal("-1010"), Decimal("-990"))
        assert r.contains(Decimal("-1000"))
        assert r.contains(Decimal("-990"))
        assert r.contains(Decimal("-1010"))
        assert not r.contains(Decimal("-989"))
        assert not r.contains(Decimal("-1011"))


# ---------------------------------------------------------------------------
# build_reconciliation_report (SwapIntent enforcement)
# ---------------------------------------------------------------------------


class TestBuildReconciliationReport:
    def test_clean_swap_no_incident(self) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),
        )
        result = _mock_execution_result("1000", "0.5")

        pre = _snap({"USDC": "5000", "WETH": "0"})
        # actual deltas: USDC -1000 exactly, WETH +0.5 exactly
        post = _snap({"USDC": "4000", "WETH": "0.5"})

        report = build_reconciliation_report(pre, post, intent, result)

        assert report.enforced
        assert not report.incident
        assert report.mismatches == []
        assert report.actual_deltas["USDC"] == Decimal("-1000")
        assert report.actual_deltas["WETH"] == Decimal("0.5")
        assert "USDC" in report.expected_ranges
        assert "WETH" in report.expected_ranges

    def test_swap_with_underdelivery_raises_incident(self) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),  # 1%
        )
        result = _mock_execution_result("1000", "0.5")

        pre = _snap({"USDC": "5000", "WETH": "0"})
        # Only 0.4 WETH actually landed — 20% under. Far outside the 1% band.
        post = _snap({"USDC": "4000", "WETH": "0.4"})

        report = build_reconciliation_report(pre, post, intent, result)

        assert report.enforced
        assert report.incident
        assert len(report.mismatches) == 1
        mm = report.mismatches[0]
        assert mm.token == "WETH"
        assert mm.actual == Decimal("0.4")
        assert mm.expected_min == Decimal("0.5") - Decimal("0.5") * Decimal("0.01")

    def test_swap_with_overspend_from_token_raises_incident(self) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),
        )
        result = _mock_execution_result("1000", "0.5")

        pre = _snap({"USDC": "5000", "WETH": "0"})
        # 1100 USDC left the wallet — 10% over the enriched amount_in.
        # Clear accounting drift: wallet moved more than the intent declared.
        post = _snap({"USDC": "3900", "WETH": "0.5"})

        report = build_reconciliation_report(pre, post, intent, result)

        assert report.incident
        assert any(m.token == "USDC" for m in report.mismatches)

    def test_non_swap_intent_is_not_enforced(self) -> None:
        intent = HoldIntent(reason="waiting")
        pre = _snap({})
        post = _snap({})

        report = build_reconciliation_report(pre, post, intent, None)

        assert not report.enforced
        assert not report.incident
        assert report.expected_ranges == {}

    def test_missing_balance_produces_warning_not_incident(self) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),
        )
        result = _mock_execution_result("1000", "0.5")

        pre = _snap({"USDC": "5000"})  # WETH missing from pre
        post = _snap({"USDC": "4000", "WETH": "0.5"})

        report = build_reconciliation_report(pre, post, intent, result)

        # USDC delta is in range, WETH is uncheckable — we warn rather than
        # raise a false mismatch on the missing side.
        assert not report.incident
        assert any("WETH" in w for w in report.warnings)

    def test_successful_swap_with_missing_swap_amounts_passes_directional_check(self) -> None:
        """VIB-3292: a successful SwapIntent whose swap_amounts are missing
        must fall back to a directional-sanity check on actual pre/post
        balance deltas. When the deltas are directionally correct (from_token
        decreased, to_token increased) this is NOT an incident — the
        previous fail-closed behavior produced empty-mismatch false positives
        that flipped confirmed on-chain swaps to RECONCILIATION_FAILED
        (velodrome_swap_optimism and solana_swap both hit this)."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),
        )
        # execution_result with no parseable swap_amounts but success=True
        result = MagicMock()
        result.swap_amounts = None
        result.success = True

        pre = _snap({"USDC": "5000", "WETH": "0"})
        post = _snap({"USDC": "4000", "WETH": "0.5"})  # correct direction

        report = build_reconciliation_report(pre, post, intent, result)

        assert report.enforced, "fallback path still counts as enforced"
        assert not report.incident, (
            "directional deltas are correct — must not be an incident "
            "(the empty-mismatch false positive was the VIB-3292 bug)"
        )
        assert report.mismatches == []
        assert any("directional sanity check" in w for w in report.warnings)

    def test_missing_swap_amounts_wrong_direction_raises_structured_incident(self) -> None:
        """VIB-3292 safety net: when swap_amounts are missing AND actual
        deltas show the wrong direction (from_token gained, to_token lost
        or unchanged), the report MUST flag a structured incident whose
        ``mismatches`` list explains exactly which side moved the wrong way.
        Never an empty-list incident."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),
        )
        result = MagicMock()
        result.swap_amounts = None
        result.success = True

        # USDC gained (wrong sign) and WETH did not increase.
        pre = _snap({"USDC": "5000", "WETH": "0.5"})
        post = _snap({"USDC": "5100", "WETH": "0.5"})

        report = build_reconciliation_report(pre, post, intent, result)

        assert report.incident
        assert report.mismatches, "must surface structured mismatches, not empty list"
        mismatch_tokens = {m.token for m in report.mismatches}
        assert "USDC" in mismatch_tokens
        assert "WETH" in mismatch_tokens

    def test_missing_swap_amounts_native_gas_token_in_allows_gas_outflow(self) -> None:
        """VIB-3292: when swap_amounts are missing and from_token is the
        native gas token, the directional check must absorb gas outflow —
        otherwise a real native-gas swap whose from_token delta equals
        ``-(swap_in + gas)`` would erroneously pass only because gas is
        ignored. Here we assert the opposite end: a near-zero from-token
        delta (smaller in magnitude than gas) IS a mismatch because a real
        swap should have moved at least gas-worth of the native token."""
        intent = SwapIntent(
            from_token="ETH",
            to_token="USDC",
            amount_usd=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )
        result = MagicMock()
        result.swap_amounts = None
        result.success = True
        result.total_gas_cost_wei = 2 * 10**15  # 0.002 ETH

        # Pre has 1 ETH, post has 0.9999999 ETH — moved less than observed
        # gas. A real successful swap can't happen without burning at least
        # the gas.
        pre = _snap({"ETH": "1", "USDC": "0"})
        post = _snap({"ETH": "0.9999999", "USDC": "0"})

        # chain context is not wired through the MagicMock, so pass gas
        # explicitly via the keyword — the public helper accepts it.
        from ..reconciliation import build_reconciliation_report as _build

        report = _build(pre, post, intent, result, gas_token="ETH", gas_cost_native=Decimal("0.002"))

        assert report.incident
        assert any(m.token == "ETH" for m in report.mismatches)

    def test_missing_swap_amounts_into_native_gas_token_absorbs_gas_dip(self) -> None:
        """VIB-3292 (CodeRabbit follow-up): when swap_amounts are missing AND
        ``to_token`` IS the native gas token (e.g. USDC -> ETH on Arbitrum),
        a successful swap can still show ``to_delta`` slightly below zero
        because gas was paid from the same native balance. The directional
        fallback must absorb a dip of up to ``gas_cost_native`` on the
        to-side; below that we still flag a mismatch.
        """
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount_usd=Decimal("5"),
            max_slippage=Decimal("0.005"),
        )
        result = MagicMock()
        result.swap_amounts = None
        result.success = True

        # Pre 1 ETH, post 0.9985 ETH. Raw to-delta is -0.0015 ETH (wallet
        # ended slightly lighter) but gas cost was 0.002 ETH — the net
        # after accounting for gas is +0.0005, i.e. the swap DID deliver.
        pre = _snap({"USDC": "10", "ETH": "1"})
        post = _snap({"USDC": "5", "ETH": "0.9985"})

        report = build_reconciliation_report(
            pre,
            post,
            intent,
            result,
            gas_token="ETH",
            gas_cost_native=Decimal("0.002"),
        )

        assert report.enforced
        assert not report.incident, "to-delta within gas floor must not be an incident"

    def test_missing_swap_amounts_into_native_gas_token_at_floor_is_allowed(self) -> None:
        """Exact native-gas-floor equality is still tolerable on the to-side.

        If the net native balance change equals exactly ``-gas_cost_native``,
        the swap may have delivered value that was fully offset by gas spend.
        The fallback must only incident BELOW that floor, not at equality.
        """
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount_usd=Decimal("5"),
            max_slippage=Decimal("0.005"),
        )
        result = MagicMock()
        result.swap_amounts = None
        result.success = True

        pre = _snap({"USDC": "10", "ETH": "1"})
        post = _snap({"USDC": "5", "ETH": "0.998"})

        report = build_reconciliation_report(
            pre,
            post,
            intent,
            result,
            gas_token="ETH",
            gas_cost_native=Decimal("0.002"),
        )

        assert report.enforced
        assert not report.incident, "to-delta at the gas floor must not be an incident"

    def test_missing_swap_amounts_into_native_gas_token_below_floor_is_mismatch(self) -> None:
        """VIB-3292 (CodeRabbit follow-up): even with gas-aware tolerance
        on the to-side, a to-delta that dips BELOW -gas_cost_native (i.e.
        the wallet lost more native than gas explains) is still a real
        mismatch — the swap neither delivered nor merely ate gas, so
        something is wrong."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount_usd=Decimal("5"),
            max_slippage=Decimal("0.005"),
        )
        result = MagicMock()
        result.swap_amounts = None
        result.success = True

        # Post dips by 0.01 ETH but gas was only 0.002 ETH -> 0.008 ETH
        # is unaccounted for.
        pre = _snap({"USDC": "10", "ETH": "1"})
        post = _snap({"USDC": "5", "ETH": "0.99"})

        report = build_reconciliation_report(
            pre,
            post,
            intent,
            result,
            gas_token="ETH",
            gas_cost_native=Decimal("0.002"),
        )

        assert report.incident
        assert any(m.token == "ETH" for m in report.mismatches)

    def test_missing_swap_amounts_missing_balance_does_not_enforce(self) -> None:
        """VIB-3292 (CodeRabbit follow-up): if swap_amounts are missing AND
        the directional fallback cannot see one or both sides' pre/post
        deltas, the report MUST NOT claim enforcement — otherwise we could
        silently return ``enforced=True, mismatches=[]`` on a swap that
        was never actually verified. Downgrade to warnings-only and
        leave ``enforced=False``."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),
        )
        result = MagicMock()
        result.swap_amounts = None
        result.success = True

        # WETH absent from pre -> delta uncheckable on the to-side.
        pre = _snap({"USDC": "5000"})
        post = _snap({"USDC": "4000", "WETH": "0.5"})

        report = build_reconciliation_report(pre, post, intent, result)

        assert not report.enforced, "fallback must not claim enforcement with missing delta"
        assert not report.incident
        assert report.mismatches == []
        assert any("directional fallback skipped" in w for w in report.warnings)

    def test_failed_swap_with_missing_swap_amounts_does_not_incident(self) -> None:
        """A failed SwapIntent (execution_result.success=False) with missing
        swap_amounts should NOT trip the fail-closed path — the swap never
        happened, there is nothing to reconcile against."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),
        )
        result = MagicMock()
        result.swap_amounts = None
        result.success = False

        pre = _snap({"USDC": "5000", "WETH": "0"})
        post = _snap({"USDC": "5000", "WETH": "0"})

        report = build_reconciliation_report(pre, post, intent, result)

        assert not report.incident
        assert not report.enforced

    def test_serializes_to_json_safe_dict(self) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            max_slippage=Decimal("0.01"),
        )
        result = _mock_execution_result("1000", "0.5")
        pre = _snap({"USDC": "5000", "WETH": "0"})
        post = _snap({"USDC": "4000", "WETH": "0.5"})

        report = build_reconciliation_report(pre, post, intent, result)
        d = report.to_dict()

        # All Decimal values are strings — safe for JSON encoders.
        for value in d["pre_balances"].values():
            assert isinstance(value, str)
        for value in d["post_balances"].values():
            assert isinstance(value, str)
        for value in d["actual_deltas"].values():
            assert isinstance(value, str)
        assert isinstance(d["incident"], bool)
        assert isinstance(d["enforced"], bool)


# ---------------------------------------------------------------------------
# ReconciliationReport structure
# ---------------------------------------------------------------------------


class TestReconciliationReportShape:
    def test_required_fields_present_in_dict(self) -> None:
        report = ReconciliationReport(
            tokens_checked=["USDC"],
            pre_balances={"USDC": Decimal("1000")},
            post_balances={"USDC": Decimal("500")},
            actual_deltas={"USDC": Decimal("-500")},
            expected_ranges={},
            mismatches=[],
            warnings=[],
            incident=False,
            enforced=False,
        )
        d = report.to_dict()
        for key in (
            "tokens_checked",
            "pre_balances",
            "post_balances",
            "actual_deltas",
            "expected_ranges",
            "mismatches",
            "warnings",
            "incident",
            "enforced",
        ):
            assert key in d

    def test_delta_mismatch_serialization(self) -> None:
        report = ReconciliationReport(
            tokens_checked=["WETH"],
            pre_balances={"WETH": Decimal("0")},
            post_balances={"WETH": Decimal("0.4")},
            actual_deltas={"WETH": Decimal("0.4")},
            expected_ranges={"WETH": ExpectedRange("WETH", Decimal("0.495"), Decimal("0.505"))},
            mismatches=[
                DeltaMismatch(
                    token="WETH",
                    actual=Decimal("0.4"),
                    expected_min=Decimal("0.495"),
                    expected_max=Decimal("0.505"),
                )
            ],
            warnings=[],
            incident=True,
            enforced=True,
        )
        d = report.to_dict()
        assert d["mismatches"][0]["token"] == "WETH"
        assert d["mismatches"][0]["actual"] == "0.4"
        assert d["mismatches"][0]["expected_min"] == "0.495"
        assert d["mismatches"][0]["expected_max"] == "0.505"


# Avoid "pytest: no tests collected" when a future refactor empties a section.
def test_module_smoke() -> None:
    assert BalanceSnapshot.now({"USDC": Decimal("1")}).balances["USDC"] == Decimal("1")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
