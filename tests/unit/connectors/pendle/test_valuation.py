"""Tests for Pendle position valuation (VIB-3487).

Tests:
  1. test_pendle_valuer_pt_pull_to_par
     PT position near maturity: value approaches face value as pt_to_asset_rate → 1.
  2. test_pendle_valuer_lp_component_decomposition
     LP position with pool reserves: correct weighted SY + PT sum.
  3. test_pt_valuation_no_discount_at_par
     pt_to_asset_rate=1.0 → value exactly = pt_amount × underlying_price.
  4. test_sy_valuation
     SY position: value = sy_amount × underlying_price.
  5. test_lp_fallback_without_pool_reserves
     lp_amount provided but no pool reserves → ESTIMATED confidence, fallback value.
  6. test_compute_pt_implied_apy_bps_standard
     pt_to_asset_rate=0.95, days=180 → ~1067 bps.
  7. test_compute_pt_implied_apy_bps_at_maturity
     days=0 → None.
  8. test_compute_pt_implied_apy_bps_capped
     Very near maturity → capped at _APR_BPS_CAP.
  9. test_no_position_data_returns_unavailable
     No lp/pt/sy amount → UNAVAILABLE.
  10. test_underlying_price_none_returns_unavailable
      underlying_price_usd=None → UNAVAILABLE.
  11. test_pt_pull_to_par_near_maturity
      pt_to_asset_rate=0.9999 → value ≈ face value within 0.02%.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from almanak.connectors.pendle.valuation import (
    _APR_BPS_CAP,
    compute_pt_implied_apy_bps,
    value_pendle_lp_from_components,
    value_pendle_position,
    value_pt_position,
    value_sy_position,
)

# ---------------------------------------------------------------------------
# Pure math unit tests
# ---------------------------------------------------------------------------


class TestComputePtImpliedApyBps:
    """Tests for the PT implied APY computation."""

    def test_standard_case(self):
        """pt_to_asset_rate=0.95, days=180 → ~1067 bps."""
        result = compute_pt_implied_apy_bps(Decimal("0.95"), 180)
        assert result is not None
        # (1 - 0.95) / 0.95 * (365 / 180) * 10_000 ≈ 1067
        assert 1050 <= result <= 1090, f"Expected ~1067, got {result}"

    def test_zero_days_returns_none(self):
        """days_to_maturity=0 (expired) → None."""
        assert compute_pt_implied_apy_bps(Decimal("0.95"), 0) is None

    def test_negative_days_returns_none(self):
        """Negative days (past maturity) → None."""
        assert compute_pt_implied_apy_bps(Decimal("0.95"), -5) is None

    def test_at_par_returns_zero(self):
        """pt_to_asset_rate=1.0 (at par) → 0 bps."""
        result = compute_pt_implied_apy_bps(Decimal("1.0"), 180)
        assert result is not None
        assert result == 0

    def test_near_maturity_capped(self):
        """Large discount near maturity → capped at _APR_BPS_CAP."""
        result = compute_pt_implied_apy_bps(Decimal("0.5"), 1)
        assert result is not None
        assert result == _APR_BPS_CAP

    def test_full_year_small_discount(self):
        """0.1% discount over 365 days → ~10 bps."""
        result = compute_pt_implied_apy_bps(Decimal("0.999"), 365)
        assert result is not None
        assert 8 <= result <= 12, f"Expected ~10, got {result}"


class TestValuePtPosition:
    """Tests for PT position valuation math."""

    def test_pt_at_par(self):
        """pt_to_asset_rate=1.0 → value = pt_amount × price."""
        val = value_pt_position(Decimal("2.0"), Decimal("3500"), Decimal("1.0"))
        assert val == Decimal("7000")

    def test_pt_with_discount(self):
        """pt_to_asset_rate=0.95 → value = pt_amount × price × 0.95."""
        val = value_pt_position(Decimal("1.0"), Decimal("1000"), Decimal("0.95"))
        assert val == Decimal("950")

    def test_pendle_valuer_pt_pull_to_par(self):
        """Near maturity: value approaches face value (within 0.01% of par)."""
        face_value = Decimal("3500")  # 1 PT × $3500 underlying at par
        # pt_to_asset_rate ≈ 1 (0.01% discount → near maturity)
        near_par_rate = Decimal("0.9999")
        val = value_pt_position(Decimal("1.0"), Decimal("3500"), near_par_rate)
        # val = 3500 × 0.9999 = 3499.65; within 0.01% of par
        diff_pct = abs(val - face_value) / face_value
        assert diff_pct < Decimal("0.0002")


class TestValueSyPosition:
    """Tests for SY position valuation math."""

    def test_sy_position(self):
        """SY value = sy_amount × underlying_price."""
        val = value_sy_position(Decimal("5.0"), Decimal("100"))
        assert val == Decimal("500")

    def test_zero_sy(self):
        """Zero SY → zero value."""
        val = value_sy_position(Decimal("0"), Decimal("3500"))
        assert val == Decimal("0")


class TestValuePendleLpFromComponents:
    """Tests for LP component decomposition math."""

    def test_pendle_valuer_lp_component_decomposition(self):
        """Correct weighted SY + PT sum with pool decomposition."""
        # Pool: 100 SY + 50 PT, price $1000, rate 0.95
        total, sy_val, pt_val = value_pendle_lp_from_components(
            sy_amount=Decimal("100"),
            pt_amount=Decimal("50"),
            underlying_price_usd=Decimal("1000"),
            pt_to_asset_rate=Decimal("0.95"),
        )
        assert sy_val == Decimal("100000")   # 100 × $1000
        assert pt_val == Decimal("47500")    # 50 × $1000 × 0.95
        assert total == Decimal("147500")

    def test_lp_at_par(self):
        """pt_to_asset_rate=1.0 → PT valued at full price."""
        total, sy_val, pt_val = value_pendle_lp_from_components(
            sy_amount=Decimal("10"),
            pt_amount=Decimal("10"),
            underlying_price_usd=Decimal("100"),
            pt_to_asset_rate=Decimal("1.0"),
        )
        assert sy_val == Decimal("1000")
        assert pt_val == Decimal("1000")
        assert total == Decimal("2000")


# ---------------------------------------------------------------------------
# Integration tests for value_pendle_position
# ---------------------------------------------------------------------------


class TestValuePendlePosition:
    """Tests for the high-level value_pendle_position function."""

    def test_pt_pull_to_par_near_maturity(self):
        """PT with rate=1.0 (no reader) → value exactly at par."""
        result = value_pendle_position(
            chain="arbitrum",
            market_address="0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
            pt_amount=Decimal("1.0"),
            underlying_price_usd=Decimal("3500"),
            on_chain_reader=None,  # No reader → rate defaults to 1.0
        )
        # With no reader, rate defaults to 1.0 (at-par)
        assert result.current_value_usd == Decimal("3500")

    def test_pt_valuation_with_on_chain_reader(self):
        """PT valuation uses pt_to_asset_rate from on-chain reader when available."""
        mock_reader = MagicMock()
        mock_reader.get_pt_to_asset_rate.return_value = Decimal("0.95")
        mock_reader.get_days_to_maturity.return_value = None  # expiry not needed for this test

        result = value_pendle_position(
            chain="arbitrum",
            market_address="0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
            pt_amount=Decimal("2.0"),
            underlying_price_usd=Decimal("1000"),
            on_chain_reader=mock_reader,
        )
        # 2.0 PT × $1000 × 0.95 = $1900
        assert result.current_value_usd == Decimal("1900")
        assert result.pt_to_asset_rate == Decimal("0.95")

    def test_sy_valuation(self):
        """SY position: value = sy_amount × underlying_price."""
        result = value_pendle_position(
            chain="arbitrum",
            market_address="0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
            sy_amount=Decimal("3.0"),
            underlying_price_usd=Decimal("1000"),
        )
        assert result.current_value_usd == Decimal("3000")
        assert result.sy_component_usd == Decimal("3000")
        assert result.pt_component_usd is None

    def test_pendle_valuer_lp_component_decomposition_via_position(self):
        """LP with pool reserves: correct SY + PT weighted sum."""
        result = value_pendle_position(
            chain="arbitrum",
            market_address="0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
            lp_amount=Decimal("10"),        # 10 LP tokens
            lp_pool_sy_amount=Decimal("1000"),   # 1000 SY in pool
            lp_pool_pt_amount=Decimal("500"),    # 500 PT in pool
            lp_total_supply=Decimal("100"),      # 100 total LP
            underlying_price_usd=Decimal("1000"),
            # No reader → rate defaults to 1.0
        )
        # 10/100 = 10% of pool → 100 SY + 50 PT
        # value = 100 × $1000 + 50 × $1000 × 1.0 = $150_000
        assert result.current_value_usd == Decimal("150000")
        assert result.sy_component_usd == Decimal("100000")
        assert result.pt_component_usd == Decimal("50000")

    def test_lp_fallback_without_pool_reserves(self):
        """LP without pool reserves uses fallback (lp_amount × sy_price)."""
        from almanak.framework.portfolio.models import ValueConfidence

        result = value_pendle_position(
            chain="arbitrum",
            market_address="0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
            lp_amount=Decimal("5.0"),
            underlying_price_usd=Decimal("2000"),
        )
        assert result.current_value_usd == Decimal("10000")  # 5.0 × $2000
        assert result.confidence == ValueConfidence.ESTIMATED
        assert "lp_pool_reserves not provided" in result.unavailable_reason

    def test_no_position_data_returns_unavailable(self):
        """No lp/pt/sy amount → UNAVAILABLE."""
        from almanak.framework.portfolio.models import ValueConfidence

        result = value_pendle_position(
            chain="arbitrum",
            market_address="0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
            underlying_price_usd=Decimal("1000"),
        )
        assert result.confidence == ValueConfidence.UNAVAILABLE
        assert result.current_value_usd == Decimal("0")

    def test_underlying_price_none_returns_unavailable(self):
        """underlying_price_usd=None → UNAVAILABLE."""
        from almanak.framework.portfolio.models import ValueConfidence

        result = value_pendle_position(
            chain="arbitrum",
            market_address="0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
            lp_amount=Decimal("1.0"),
            underlying_price_usd=None,
        )
        assert result.confidence == ValueConfidence.UNAVAILABLE

    def test_underlying_price_zero_returns_unavailable(self):
        """underlying_price_usd=0 → UNAVAILABLE."""
        from almanak.framework.portfolio.models import ValueConfidence

        result = value_pendle_position(
            chain="arbitrum",
            market_address="0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
            lp_amount=Decimal("1.0"),
            underlying_price_usd=Decimal("0"),
        )
        assert result.confidence == ValueConfidence.UNAVAILABLE

    def test_on_chain_reader_failure_graceful(self):
        """on_chain_reader failure → falls back to rate=1.0, ESTIMATED confidence."""
        from almanak.framework.portfolio.models import ValueConfidence

        mock_reader = MagicMock()
        mock_reader.get_pt_to_asset_rate.side_effect = Exception("network error")
        mock_reader.get_days_to_maturity.return_value = None

        result = value_pendle_position(
            chain="arbitrum",
            market_address="0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
            pt_amount=Decimal("1.0"),
            underlying_price_usd=Decimal("1000"),
            on_chain_reader=mock_reader,
        )
        # Should still return a value (using at-par default)
        assert result.current_value_usd == Decimal("1000")
        assert result.confidence == ValueConfidence.ESTIMATED
        assert "pt_to_asset_rate" in result.unavailable_reason

    def test_lp_decomposition_with_on_chain_rate(self):
        """LP decomposition correctly applies pt_to_asset_rate from reader."""
        mock_reader = MagicMock()
        mock_reader.get_pt_to_asset_rate.return_value = Decimal("0.95")
        mock_reader.get_days_to_maturity.return_value = None  # not needed for this test

        result = value_pendle_position(
            chain="arbitrum",
            market_address="0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
            lp_amount=Decimal("10"),
            lp_pool_sy_amount=Decimal("1000"),
            lp_pool_pt_amount=Decimal("500"),
            lp_total_supply=Decimal("100"),
            underlying_price_usd=Decimal("1000"),
            on_chain_reader=mock_reader,
        )
        # 10% of pool = 100 SY + 50 PT
        # SY value = 100 × $1000 = $100_000
        # PT value = 50 × $1000 × 0.95 = $47_500
        # Total = $147_500
        assert result.current_value_usd == Decimal("147500")
        assert result.sy_component_usd == Decimal("100000")
        assert result.pt_component_usd == Decimal("47500")

    def test_implied_apy_computed_when_reader_available(self):
        """implied_apy_bps is populated when pt_to_asset_rate and days_to_maturity available."""
        mock_reader = MagicMock()
        mock_reader.get_pt_to_asset_rate.return_value = Decimal("0.95")
        # Simulate 180 days to maturity via the public reader API
        mock_reader.get_days_to_maturity.return_value = 180

        result = value_pendle_position(
            chain="arbitrum",
            market_address="0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
            pt_amount=Decimal("1.0"),
            underlying_price_usd=Decimal("3500"),
            on_chain_reader=mock_reader,
        )
        assert result.implied_apy_bps is not None
        # With 5% discount and ~180 days: ~1067 bps
        assert 1000 <= result.implied_apy_bps <= 1200, f"Got {result.implied_apy_bps}"
        assert result.days_to_maturity == 180
