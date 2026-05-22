"""Unit tests for VIB-3422: Pendle PT buy implied APR computation.

Tests:
  1. _parse_pt_maturity parses standard PT symbol formats
  2. compute_implied_apr_bps: pt_price=0.95 days=180 → ≈ 1067 bps
  3. compute_implied_apr_bps: days=0 → None (at maturity)
  4. compute_implied_apr_bps: days=1 → very large, capped at 500_000
  5. build_pendle_pt_buy_accounting_event: correct fields populated
  6. Non-Pendle SWAP → returns None
  7. PT sell (to_token is not PT) → returns None
  8. Missing swap_amounts → ESTIMATED confidence, amounts None
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock


class TestParsePtMaturity:

    def test_standard_format(self):
        from almanak.framework.accounting.pendle_pt_accounting import _parse_pt_maturity

        dt = _parse_pt_maturity("PT-wstETH-25JUN2026")
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 6
        assert dt.day == 25
        assert dt.tzinfo == UTC

    def test_uppercase_format(self):
        from almanak.framework.accounting.pendle_pt_accounting import _parse_pt_maturity

        dt = _parse_pt_maturity("PT-SUSDE-29MAY2025")
        assert dt is not None
        assert dt.year == 2025
        assert dt.month == 5
        assert dt.day == 29

    def test_compound_name_format(self):
        from almanak.framework.accounting.pendle_pt_accounting import _parse_pt_maturity

        dt = _parse_pt_maturity("PT-SUSDAI-15OCT2026")
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 10
        assert dt.day == 15

    def test_unknown_format_returns_none(self):
        from almanak.framework.accounting.pendle_pt_accounting import _parse_pt_maturity

        assert _parse_pt_maturity("WETH") is None
        assert _parse_pt_maturity("PT-wstETH") is None  # no date
        assert _parse_pt_maturity("") is None

    def test_invalid_date_returns_none(self):
        from almanak.framework.accounting.pendle_pt_accounting import _parse_pt_maturity

        # Day 32 is invalid
        assert _parse_pt_maturity("PT-WETH-32JAN2026") is None


class TestComputeImpliedAprBps:

    def test_standard_case(self):
        from almanak.framework.accounting.pendle_pt_accounting import compute_implied_apr_bps

        # pt_price=0.95, days=180
        # (1 - 0.95) / 0.95 * (365 / 180) * 10_000 ≈ 1067
        result = compute_implied_apr_bps(Decimal("0.95"), 180)
        assert result is not None
        assert 1050 <= result <= 1090, f"Expected ~1067, got {result}"

    def test_zero_days_returns_none(self):
        from almanak.framework.accounting.pendle_pt_accounting import compute_implied_apr_bps

        assert compute_implied_apr_bps(Decimal("0.95"), 0) is None

    def test_negative_days_returns_none(self):
        from almanak.framework.accounting.pendle_pt_accounting import compute_implied_apr_bps

        assert compute_implied_apr_bps(Decimal("0.95"), -5) is None

    def test_near_maturity_capped(self):
        from almanak.framework.accounting.pendle_pt_accounting import _APR_BPS_CAP, compute_implied_apr_bps

        # pt_price=0.50 with 1 day → (0.50/0.50)*365*10000 = 3_650_000 bps → capped
        result = compute_implied_apr_bps(Decimal("0.50"), 1)
        assert result is not None
        assert result == _APR_BPS_CAP

    def test_tiny_discount(self):
        from almanak.framework.accounting.pendle_pt_accounting import compute_implied_apr_bps

        # pt_price=0.999 (0.1% discount), 365 days → ≈ 10 bps
        result = compute_implied_apr_bps(Decimal("0.999"), 365)
        assert result is not None
        assert 8 <= result <= 12, f"Expected ~10, got {result}"


class TestBuildPendlePtBuyAccountingEvent:

    def _make_intent(self, to_token: str = "PT-wstETH-25JUN2026", protocol: str = "pendle"):
        intent = MagicMock()
        it = MagicMock()
        it.value = "SWAP"
        intent.intent_type = it
        intent.protocol = protocol
        intent.to_token = to_token
        intent.pool = "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b"
        return intent

    def _make_result(self, amount_in: int = 1_000_000_000_000_000_000, amount_out: int = 1_052_631_578_947_368_421):
        from almanak.framework.execution.extracted_data import SwapAmounts

        result = MagicMock()
        result.tx_hash = "0xdeadbeef12345678"
        swap_amounts = SwapAmounts(
            amount_in=amount_in,
            amount_out=amount_out,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("1.052631"),
            effective_price=Decimal("0.95"),
        )
        result.extracted_data = {"swap_amounts": swap_amounts}
        return result

    def _call(self, intent=None, result=None):
        from almanak.framework.accounting.pendle_pt_accounting import build_pendle_pt_buy_accounting_event

        if intent is None:
            intent = self._make_intent()
        if result is None:
            result = self._make_result()
        return build_pendle_pt_buy_accounting_event(
            intent=intent,
            result=result,
            deployment_id="dep-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain="arbitrum",
            wallet_address="0xwallet",
            ledger_entry_id="led-001",
        )

    def test_event_type_pt_buy(self):
        from almanak.framework.accounting.models import PendleEventType

        ev = self._call()
        assert ev is not None
        assert ev.event_type == PendleEventType.PT_BUY

    def test_pt_price_computed(self):
        # sy_in = 1e18, pt_out = 1.0526... e18 → pt_price ≈ 0.95
        ev = self._call()
        assert ev is not None
        assert ev.pt_price is not None
        assert Decimal("0.940") < ev.pt_price < Decimal("0.960")

    def test_implied_apr_populated(self):
        # Use 2030 maturity so the test remains valid long after 2026.
        intent = self._make_intent(to_token="PT-wstETH-25JUN2030")
        ev = self._call(intent=intent)
        assert ev is not None
        assert ev.implied_apr_bps is not None
        assert ev.implied_apr_bps > 0

    def test_maturity_parsed_from_symbol(self):
        ev = self._call()
        assert ev is not None
        assert ev.maturity_timestamp is not None
        assert ev.maturity_timestamp.year == 2026
        assert ev.maturity_timestamp.month == 6
        assert ev.maturity_timestamp.day == 25

    def test_non_pendle_returns_none(self):
        intent = self._make_intent(protocol="uniswap_v3")
        assert self._call(intent=intent) is None

    def test_pt_sell_returns_none(self):
        intent = self._make_intent(to_token="wstETH")  # selling PT → receiving wstETH
        assert self._call(intent=intent) is None

    def test_non_swap_intent_returns_none(self):
        from almanak.framework.accounting.pendle_pt_accounting import build_pendle_pt_buy_accounting_event

        intent = MagicMock()
        it = MagicMock()
        it.value = "LP_OPEN"
        intent.intent_type = it
        intent.protocol = "pendle"
        result = MagicMock()
        result.tx_hash = ""
        result.extracted_data = {}
        ev = build_pendle_pt_buy_accounting_event(
            intent=intent, result=result,
            deployment_id="s", cycle_id="c",
            execution_mode="paper", chain="arbitrum", wallet_address="0xw",
        )
        assert ev is None

    def test_missing_swap_amounts_estimated_confidence(self):
        from almanak.framework.accounting.models import AccountingConfidence

        result = MagicMock()
        result.tx_hash = ""
        result.extracted_data = {}  # no swap_amounts
        ev = self._call(result=result)
        assert ev is not None
        assert ev.sy_amount is None
        assert ev.pt_amount is None
        assert ev.pt_price is None
        assert ev.confidence == AccountingConfidence.ESTIMATED

    def test_identity_fields_populated(self):
        ev = self._call()
        assert ev is not None
        assert ev.identity.deployment_id == "dep-1"
        assert ev.identity.chain == "arbitrum"
        assert ev.identity.ledger_entry_id == "led-001"
        assert ev.pt_token == "PT-wstETH-25JUN2026"
