"""Unit tests for VIB-3492: Pendle PT pre-maturity sell accounting.

Tests:
  1. test_pt_sell_event_type  — builder emits PendleEventType.PT_SELL
  2. test_pt_sell_reduces_fifo_lot — sell half a lot; remaining_pt halved
  3. test_pt_sell_partial_then_redeem_only_remaining — sell half, redeem rest; FIFO correct
  4. test_pt_sell_no_lot_unmatched_confidence — no prior lot → ESTIMATED
  5. test_non_pt_swap_returns_none — to_token is not PT → builder returns None
  6. test_non_pendle_protocol_returns_none
  7. test_non_swap_intent_returns_none
  8. test_missing_swap_amounts_estimated_confidence
  9. test_identity_fields_populated
  10. test_pt_price_computed — sy_out / pt_in = pt_price
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock


# ─── Fixtures ────────────────────────────────────────────────────────────────

def _make_basis_store():
    from almanak.framework.accounting.basis import FIFOBasisStore
    return FIFOBasisStore()


def _make_sell_intent(
    from_token: str = "PT-wstETH-25JUN2026",
    protocol: str = "pendle",
    pool: str = "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b",
):
    intent = MagicMock()
    it = MagicMock()
    it.value = "SWAP"
    intent.intent_type = it
    intent.protocol = protocol
    intent.from_token = from_token
    intent.pool = pool
    return intent


def _make_sell_result(
    pt_amount_raw: int = 500_000_000_000_000_000,   # 0.5 PT (18-dec)
    sy_amount_raw: int = 490_000_000_000_000_000,   # 0.49 SY out
):
    from almanak.framework.execution.extracted_data import SwapAmounts

    result = MagicMock()
    result.tx_hash = "0xsell1234"
    swap_amounts = SwapAmounts(
        amount_in=pt_amount_raw,
        amount_out=sy_amount_raw,
        amount_in_decimal=Decimal(str(pt_amount_raw)) / Decimal(10**18),
        amount_out_decimal=Decimal(str(sy_amount_raw)) / Decimal(10**18),
        effective_price=Decimal(str(sy_amount_raw)) / Decimal(str(pt_amount_raw)),
    )
    result.extracted_data = {"swap_amounts": swap_amounts}
    return result


def _call_builder(
    basis_store=None,
    intent=None,
    result=None,
    deployment_id: str = "dep-1",
    chain: str = "arbitrum",
    wallet: str = "0xwallet",
):
    from almanak.framework.accounting.pendle_pt_sell_accounting import build_pendle_pt_sell_accounting_event

    if basis_store is None:
        basis_store = _make_basis_store()
    if intent is None:
        intent = _make_sell_intent()
    if result is None:
        result = _make_sell_result()
    return build_pendle_pt_sell_accounting_event(
        intent=intent,
        result=result,
        deployment_id=deployment_id,
        strategy_id="strat-1",
        cycle_id="cycle-001",
        execution_mode="paper",
        chain=chain,
        wallet_address=wallet,
        basis_store=basis_store,
        ledger_entry_id="led-001",
    )


# ─── Tests ────────────────────────────────────────────────────────────────────

class TestPtSellEventType:
    def test_event_type_is_pt_sell(self):
        from almanak.framework.accounting.models import PendleEventType

        ev = _call_builder()
        assert ev is not None
        assert ev.event_type == PendleEventType.PT_SELL

    def test_pt_token_propagated(self):
        ev = _call_builder()
        assert ev is not None
        assert ev.pt_token == "PT-wstETH-25JUN2026"

    def test_pt_price_computed(self):
        # pt_in = 0.5 * 1e18, sy_out = 0.49 * 1e18 → pt_price = 0.49/0.5 = 0.98
        ev = _call_builder()
        assert ev is not None
        assert ev.pt_price is not None
        assert abs(ev.pt_price - Decimal("0.98")) < Decimal("0.001")


class TestPtSellFifoReduction:
    """VIB-3492 core: PT_SELL reduces the FIFO lot so PT_REDEEM over-matches are prevented."""

    _DEPLOY_ID = "dep-sell"
    _MARKET = "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b"
    _PT_TOK = "PT-wstETH-25JUN2026"
    _CHAIN = "arbitrum"
    _WALLET = "0xwallet"

    def _position_key(self):
        return f"pendle_pt:{self._CHAIN}:{self._WALLET}:{self._MARKET}"

    def test_pt_sell_reduces_fifo_lot(self):
        """Sell half a lot; remaining_pt on the lot should halve."""
        from almanak.framework.accounting.basis import FIFOBasisStore

        bs = FIFOBasisStore()
        # Record a PT_BUY: 1.0 PT bought for 0.95 SY
        bs.record_pt_buy(
            deployment_id=self._DEPLOY_ID,
            position_key=self._position_key(),
            pt_token=self._PT_TOK,
            pt_amount=Decimal("1.0"),
            sy_cost=Decimal("0.95"),
        )

        # Sell 0.5 PT
        intent = _make_sell_intent(pool=self._MARKET)
        result = _make_sell_result(
            pt_amount_raw=500_000_000_000_000_000,   # 0.5 PT
            sy_amount_raw=490_000_000_000_000_000,   # 0.49 SY out
        )
        ev = _call_builder(
            basis_store=bs,
            intent=intent,
            result=result,
            deployment_id=self._DEPLOY_ID,
            chain=self._CHAIN,
            wallet=self._WALLET,
        )
        assert ev is not None

        # After PT_SELL(0.5), the lot should have 0.5 PT remaining
        lot_key = f"{self._DEPLOY_ID}:{self._position_key()}:{self._PT_TOK.lower()}"
        lots = bs._lots.get(lot_key, [])
        assert len(lots) == 1
        remaining = lots[0]["remaining_pt"]
        assert abs(remaining - Decimal("0.5")) < Decimal("0.0001"), f"Expected 0.5, got {remaining}"

    def test_pt_sell_reduces_fifo_lot_confidence_high_when_matched(self):
        """Fully matched PT sell → confidence HIGH."""
        from almanak.framework.accounting.basis import FIFOBasisStore
        from almanak.framework.accounting.models import AccountingConfidence

        bs = FIFOBasisStore()
        bs.record_pt_buy(
            deployment_id=self._DEPLOY_ID,
            position_key=self._position_key(),
            pt_token=self._PT_TOK,
            pt_amount=Decimal("1.0"),
            sy_cost=Decimal("0.95"),
        )
        # Sell exactly 1.0 PT (all of the lot)
        intent = _make_sell_intent(pool=self._MARKET)
        result = _make_sell_result(
            pt_amount_raw=1_000_000_000_000_000_000,
            sy_amount_raw=1_010_000_000_000_000_000,
        )
        ev = _call_builder(
            basis_store=bs,
            intent=intent,
            result=result,
            deployment_id=self._DEPLOY_ID,
            chain=self._CHAIN,
            wallet=self._WALLET,
        )
        assert ev is not None
        assert ev.confidence == AccountingConfidence.HIGH

    def test_pt_sell_partial_then_redeem_only_remaining(self):
        """Sell half → subsequent PT_REDEEM only matches remaining half.

        This is the core regression guard for VIB-3492: without PT_SELL wiring,
        PT_REDEEM would over-match and overstate realized yield.
        """
        from almanak.framework.accounting.basis import FIFOBasisStore

        bs = FIFOBasisStore()
        # Buy 1.0 PT for 0.95 SY
        bs.record_pt_buy(
            deployment_id=self._DEPLOY_ID,
            position_key=self._position_key(),
            pt_token=self._PT_TOK,
            pt_amount=Decimal("1.0"),
            sy_cost=Decimal("0.95"),
        )

        # Sell 0.5 PT pre-maturity
        intent = _make_sell_intent(pool=self._MARKET)
        result = _make_sell_result(
            pt_amount_raw=500_000_000_000_000_000,   # 0.5 PT
            sy_amount_raw=490_000_000_000_000_000,
        )
        _call_builder(
            basis_store=bs,
            intent=intent,
            result=result,
            deployment_id=self._DEPLOY_ID,
            chain=self._CHAIN,
            wallet=self._WALLET,
        )

        # Redeem remaining 0.5 PT at maturity (receive 0.5 SY, i.e. 1:1)
        # Original cost of 0.5 PT = 0.95 * 0.5 = 0.475 SY
        # Yield = 0.5 SY received - 0.475 SY cost = 0.025 SY
        redeem_result = bs.match_pt_redeem(
            deployment_id=self._DEPLOY_ID,
            position_key=self._position_key(),
            pt_token=self._PT_TOK,
            pt_redeemed=Decimal("0.5"),
            sy_received=Decimal("0.5"),
        )
        assert redeem_result.unmatched_amount == Decimal("0"), (
            "Expected full match for remaining 0.5 PT"
        )
        assert abs(redeem_result.repaid_principal - Decimal("0.475")) < Decimal("0.001"), (
            f"Expected cost basis 0.475, got {redeem_result.repaid_principal}"
        )
        assert abs(redeem_result.interest_or_yield - Decimal("0.025")) < Decimal("0.001"), (
            f"Expected yield 0.025, got {redeem_result.interest_or_yield}"
        )


class TestPtSellNoLot:
    def test_no_lot_yields_unmatched_estimated(self):
        """No prior PT_BUY lot → FIFO unmatched → confidence ESTIMATED."""
        from almanak.framework.accounting.models import AccountingConfidence

        bs = _make_basis_store()
        ev = _call_builder(basis_store=bs)
        assert ev is not None
        assert ev.confidence == AccountingConfidence.ESTIMATED
        assert "unmatched" in ev.unavailable_reason.lower() or "not matched" in ev.unavailable_reason.lower() or ev.confidence == AccountingConfidence.ESTIMATED


class TestPtSellGuards:
    def test_non_pt_from_token_returns_none(self):
        """SWAP where from_token = wstETH (buying PT) should return None."""
        intent = _make_sell_intent(from_token="wstETH")
        assert _call_builder(intent=intent) is None

    def test_non_pendle_protocol_returns_none(self):
        intent = _make_sell_intent(protocol="uniswap_v3")
        assert _call_builder(intent=intent) is None

    def test_non_swap_intent_returns_none(self):
        from almanak.framework.accounting.pendle_pt_sell_accounting import build_pendle_pt_sell_accounting_event

        intent = MagicMock()
        it = MagicMock()
        it.value = "WITHDRAW"
        intent.intent_type = it
        intent.protocol = "pendle"
        intent.from_token = "PT-wstETH-25JUN2026"
        intent.pool = "0xmarket"
        result = _make_sell_result()
        ev = build_pendle_pt_sell_accounting_event(
            intent=intent, result=result,
            deployment_id="d", strategy_id="s", cycle_id="c",
            execution_mode="paper", chain="arbitrum", wallet_address="0xw",
            basis_store=_make_basis_store(),
        )
        assert ev is None

    def test_missing_swap_amounts_estimated_confidence(self):
        from almanak.framework.accounting.models import AccountingConfidence

        result = MagicMock()
        result.tx_hash = ""
        result.extracted_data = {}  # no swap_amounts
        ev = _call_builder(result=result)
        assert ev is not None
        assert ev.pt_amount is None
        assert ev.sy_amount is None
        assert ev.pt_price is None
        assert ev.confidence == AccountingConfidence.ESTIMATED

    def test_identity_fields_populated(self):
        ev = _call_builder(deployment_id="dep-x", chain="arbitrum", wallet="0xww")
        assert ev is not None
        assert ev.identity.deployment_id == "dep-x"
        assert ev.identity.chain == "arbitrum"
        assert ev.identity.ledger_entry_id == "led-001"
        assert ev.identity.protocol == "pendle"
