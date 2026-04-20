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

    def test_successful_swap_with_missing_swap_amounts_fails_closed(self) -> None:
        """A successful SwapIntent whose swap_amounts are missing must not
        silently bypass reconciliation — the report must flag an incident."""
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
        post = _snap({"USDC": "4000", "WETH": "0.5"})

        report = build_reconciliation_report(pre, post, intent, result)

        assert report.incident, "must fail-closed when expectations are unavailable"
        assert report.enforced
        assert any("could not derive expected deltas" in w for w in report.warnings)

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
