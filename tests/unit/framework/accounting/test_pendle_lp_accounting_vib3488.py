"""Unit tests for VIB-3488: Pendle LP accounting USD amount population.

Tests:
  1. test_pendle_lp_open_event_populates_usd_amounts
     Mock gateway decimals + price oracle → assert USD fields populated, confidence ESTIMATED
     (ESTIMATED because pt_price is an approximation of sy_price; HIGH requires on-chain pt_to_asset_rate)
  2. test_pendle_lp_open_event_graceful_on_price_failure
     Mock price oracle failure → assert confidence ESTIMATED, unavailable_reason non-empty
  3. test_pendle_lp_open_event_sy_price_from_underlying
     Assert sy_price is populated from underlying token (wstETH) via MARKET_TOKEN_MINT_SY
  4. test_pendle_lp_close_event_has_prices
     LP_CLOSE event also gets prices when oracle provided
  5. test_none_vs_zero_discipline
     When price oracle is missing an entry, sy_price is None (not 0)
  6. test_price_oracle_not_provided_leaves_prices_none
     price_oracle=None → sy_price is None, pt_price is None
  7. test_decimals_resolved_from_static_registry
     Known Pendle market (arbitrum wstETH) gets correct 18 decimals
  8. test_payload_roundtrip_preserves_sy_price
     to_payload_json / from_payload_json preserves sy_price correctly
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_intent(
    intent_type: str,
    protocol: str = "pendle",
    pool: str = "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",  # wstETH market on arbitrum
) -> MagicMock:
    intent = MagicMock()
    it = MagicMock()
    it.value = intent_type
    intent.intent_type = it
    intent.protocol = protocol
    intent.pool = pool
    return intent


def _make_result(intent_type: str) -> MagicMock:
    from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData

    result = MagicMock()
    result.tx_hash = "0xdeadbeef12345678"
    result.transaction_results = []

    if intent_type == "LP_OPEN":
        lp_open = LPOpenData(
            position_id=0,
            liquidity=1_000_000_000_000_000_000,   # 1e18 LP tokens
            amount0=500_000_000_000_000_000,        # 0.5 SY
            amount1=250_000_000_000_000_000,        # 0.25 PT
        )
        result.extracted_data = {"lp_open_data": lp_open}
    else:
        lp_close = LPCloseData(
            amount0_collected=520_000_000_000_000_000,   # 0.52 SY
            amount1_collected=260_000_000_000_000_000,   # 0.26 PT
            liquidity_removed=1_000_000_000_000_000_000,
        )
        result.extracted_data = {"lp_close_data": lp_close}

    return result


def _call_builder(
    intent_type: str = "LP_OPEN",
    protocol: str = "pendle",
    pool: str = "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
    price_oracle: dict | None = None,
) -> MagicMock:
    from almanak.framework.accounting.pendle_accounting import build_pendle_lp_accounting_event

    intent = _make_intent(intent_type, protocol, pool)
    result = _make_result(intent_type)
    return build_pendle_lp_accounting_event(
        intent=intent,
        result=result,
        deployment_id="dep-1",
        strategy_id="strat-1",
        cycle_id="cycle-001",
        execution_mode="paper",
        chain="arbitrum",
        wallet_address="0xwallet",
        ledger_entry_id="led-001",
        price_oracle=price_oracle,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPendleLpAccountingVib3488:
    """VIB-3488: USD amount population in Pendle LP accounting events."""

    def test_pendle_lp_open_event_populates_usd_amounts(self):
        """Mock gateway decimals + price oracle; assert USD fields populated."""
        from almanak.framework.accounting.models import AccountingConfidence

        # wstETH underlying price; wstETH is in MARKET_TOKEN_MINT_SY for arbitrum wstETH market
        price_oracle = {"wstETH": Decimal("3500.00"), "WSTETH": Decimal("3500.00")}

        # Patch token resolver to return wstETH symbol for the underlying address
        mock_resolved = MagicMock()
        mock_resolved.symbol = "wstETH"
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = mock_resolved

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            ev = _call_builder(price_oracle=price_oracle)

        assert ev is not None
        # sy_price should be populated with wstETH price
        assert ev.sy_price is not None
        assert ev.sy_price == Decimal("3500.00")
        # pt_price approximated as sy_price
        assert ev.pt_price is not None
        assert ev.pt_price == Decimal("3500.00")
        # Amounts should be scaled by 18 decimals
        assert ev.sy_amount == Decimal("0.5")   # 5e17 / 1e18
        assert ev.pt_amount == Decimal("0.25")  # 25e16 / 1e18
        # ESTIMATED because pt_price is only an approximation
        assert ev.confidence == AccountingConfidence.ESTIMATED

    def test_pendle_lp_open_event_graceful_on_price_failure(self):
        """Price oracle has no entry for the underlying → ESTIMATED with unavailable_reason."""
        from almanak.framework.accounting.models import AccountingConfidence

        # Oracle does NOT contain wstETH
        price_oracle = {"USDC": Decimal("1.00")}

        mock_resolved = MagicMock()
        mock_resolved.symbol = "wstETH"
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = mock_resolved

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            ev = _call_builder(price_oracle=price_oracle)

        assert ev is not None
        assert ev.sy_price is None
        assert ev.pt_price is None
        assert ev.confidence == AccountingConfidence.ESTIMATED
        assert ev.unavailable_reason != ""

    def test_none_vs_zero_discipline(self):
        """When price oracle missing, sy_price is None — not Decimal('0')."""
        ev = _call_builder(price_oracle={})  # empty dict, not None

        assert ev is not None
        # sy_price must be None (not zero) when price not found
        assert ev.sy_price is None
        assert ev.pt_price is None

    def test_price_oracle_not_provided_leaves_prices_none(self):
        """price_oracle=None → sy_price None, pt_price None."""
        ev = _call_builder(price_oracle=None)

        assert ev is not None
        assert ev.sy_price is None
        assert ev.pt_price is None
        assert "price_oracle not provided" in (ev.unavailable_reason or "")

    def test_pendle_lp_close_event_has_prices(self):
        """LP_CLOSE also gets prices when oracle provided."""
        price_oracle = {"wstETH": Decimal("3500.00")}

        mock_resolved = MagicMock()
        mock_resolved.symbol = "wstETH"
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = mock_resolved

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            ev = _call_builder(intent_type="LP_CLOSE", price_oracle=price_oracle)

        assert ev is not None
        assert ev.sy_price == Decimal("3500.00")
        assert ev.sy_amount == Decimal("0.52")   # 52e16 / 1e18
        assert ev.pt_amount == Decimal("0.26")   # 26e16 / 1e18

    def test_payload_roundtrip_preserves_sy_price(self):
        """to_payload_json / from_payload_json round-trips sy_price correctly."""
        from almanak.framework.accounting.models import AccountingConfidence, PendleAccountingEvent

        price_oracle = {"wstETH": Decimal("3500.00")}

        mock_resolved = MagicMock()
        mock_resolved.symbol = "wstETH"
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = mock_resolved

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            ev = _call_builder(price_oracle=price_oracle)

        assert ev is not None
        assert ev.sy_price is not None

        # Round-trip via JSON
        payload = ev.to_payload_json()
        recovered = PendleAccountingEvent.from_payload_json(ev.identity, payload)

        assert recovered.sy_price == ev.sy_price
        assert recovered.pt_price == ev.pt_price
        assert recovered.sy_amount == ev.sy_amount
        assert recovered.pt_amount == ev.pt_amount
        assert recovered.confidence == ev.confidence

    def test_sy_price_none_is_serialized_as_null(self):
        """sy_price=None serializes as JSON null (not '0' or 'None')."""
        import json

        ev = _call_builder(price_oracle=None)
        assert ev is not None

        payload = json.loads(ev.to_payload_json())
        assert payload["sy_price"] is None

    def test_resolver_failure_is_graceful(self):
        """If token resolver raises, sy_price is None and event still builds."""
        from almanak.framework.accounting.models import AccountingConfidence

        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = RuntimeError("resolver failure")

        price_oracle = {"wstETH": Decimal("3500.00")}

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            ev = _call_builder(price_oracle=price_oracle)

        assert ev is not None
        # With resolver failure, underlying symbol unknown → price lookup fails
        assert ev.sy_price is None
        assert ev.confidence == AccountingConfidence.ESTIMATED

    def test_amounts_not_affected_by_price_oracle(self):
        """Decimal scaling of amounts is independent of the price oracle."""
        ev_with_oracle = _call_builder(price_oracle={"wstETH": Decimal("1.0")})
        ev_without_oracle = _call_builder(price_oracle=None)

        assert ev_with_oracle is not None and ev_without_oracle is not None
        assert ev_with_oracle.sy_amount == ev_without_oracle.sy_amount
        assert ev_with_oracle.pt_amount == ev_without_oracle.pt_amount
