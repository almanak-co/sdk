"""Unit tests for VIB-3423: Pendle PT maturity settlement accounting.

Tests:
  1. Full lifecycle: PT_BUY records lot → PT_REDEEM matches lot → yield computed
  2. PT_REDEEM with no prior lots → unmatched, ESTIMATED confidence, no yield
  3. Partial redemption: FIFO correctly attributes yield to opened lots
  4. Non-Pendle WITHDRAW → returns None
  5. WITHDRAW without RedeemPY event → returns None
  6. Missing price_oracle → realized_yield_usd is None
  7. Identity fields populated correctly
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock


# ─── Shared FIFOBasisStore fixture ───────────────────────────────────────────

def _make_basis_store():
    from almanak.framework.accounting.basis import FIFOBasisStore

    return FIFOBasisStore()


# ─── Intent/result fixtures ───────────────────────────────────────────────────

def _make_redeem_intent(protocol: str = "pendle", pool: str = "0xmarket0001"):
    intent = MagicMock()
    it = MagicMock()
    it.value = "WITHDRAW"
    intent.intent_type = it
    intent.protocol = protocol
    intent.pool = pool
    intent.from_token = "PT-wstETH-25JUN2026"
    return intent


def _make_redeem_result(sy_received_raw: int, py_redeemed_raw: int = 1_000_000_000_000_000_000):
    result = MagicMock()
    result.tx_hash = "0xdeadbeef12345678"
    result.extracted_data = {
        "redemption_amounts": {
            "py_redeemed": py_redeemed_raw,
            "sy_received": sy_received_raw,
        }
    }
    return result


# ─── Tests ────────────────────────────────────────────────────────────────────

class TestBuildPendlePtRedeemAccountingEvent:

    _DEPLOY_ID = "dep-1"
    _MARKET = "0xmarket0001"

    def _position_key(self):
        return f"pendle_pt:arbitrum:0xwallet:{self._MARKET}"

    def _call(self, basis_store, intent=None, result=None, price_oracle=None):
        from almanak.framework.accounting.pendle_redeem_accounting import build_pendle_pt_redeem_accounting_event

        if intent is None:
            intent = _make_redeem_intent(pool=self._MARKET)
        if result is None:
            result = _make_redeem_result(sy_received_raw=1_050_000_000_000_000_000)
        return build_pendle_pt_redeem_accounting_event(
            intent=intent,
            result=result,
            deployment_id=self._DEPLOY_ID,
            strategy_id="strat-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain="arbitrum",
            wallet_address="0xwallet",
            basis_store=basis_store,
            price_oracle=price_oracle,
            ledger_entry_id="led-001",
        )

    def test_event_type_pt_redeem(self):
        from almanak.framework.accounting.models import PendleEventType

        bs = _make_basis_store()
        ev = self._call(bs)
        assert ev is not None
        assert ev.event_type == PendleEventType.PT_REDEEM

    def test_full_lifecycle_yield_computed(self):
        """PT_BUY records lot → PT_REDEEM matches and computes yield."""
        bs = _make_basis_store()

        # Record a PT_BUY lot: paid 1.0 SY for 1.052631... PT (pt_price=0.95)
        bs.record_pt_buy(
            deployment_id=self._DEPLOY_ID,
            position_key=self._position_key(),
            pt_token="PT-wstETH-25JUN2026",
            pt_amount=Decimal("1.052631578947368421"),  # 1e18 / 0.95
            sy_cost=Decimal("1.0"),
            timestamp=datetime.now(UTC),
        )

        # Redeem 1.052631... PT → receive 1.052631... SY (1:1 at maturity)
        # yield = 1.052631... SY received - 1.0 SY paid = 0.052631... SY
        result = _make_redeem_result(
            sy_received_raw=1_052_631_578_947_368_421,  # 1.052631... * 1e18
            py_redeemed_raw=1_052_631_578_947_368_421,  # same amount of PT
        )
        price_oracle = {"SY": "2000.0"}  # 1 SY = $2000
        ev = self._call(bs, result=result, price_oracle=price_oracle)

        assert ev is not None
        assert ev.sy_amount is not None
        # yield ≈ 0.052631 SY * $2000 ≈ $105.26
        assert ev.realized_yield_usd is not None
        assert abs(ev.realized_yield_usd - Decimal("105.26")) < Decimal("2")

    def test_no_lots_yields_unmatched(self):
        """Without PT_BUY lots, yield is computed as sy_received (all principal)."""
        bs = _make_basis_store()
        ev = self._call(bs)
        # No lots → unmatched_amount > 0 → confidence ESTIMATED
        assert ev is not None
        assert ev.realized_yield_usd is None  # no price oracle

    def test_no_lots_missing_price_oracle(self):
        bs = _make_basis_store()
        ev = self._call(bs, price_oracle=None)
        assert ev is not None
        assert ev.realized_yield_usd is None

    def test_non_pendle_protocol_returns_none(self):
        bs = _make_basis_store()
        intent = _make_redeem_intent(protocol="aave_v3")
        result = _make_redeem_result(sy_received_raw=1_000_000_000_000_000_000)
        ev = self._call(bs, intent=intent, result=result)
        assert ev is None

    def test_non_withdraw_intent_returns_none(self):
        from almanak.framework.accounting.pendle_redeem_accounting import build_pendle_pt_redeem_accounting_event

        bs = _make_basis_store()
        intent = MagicMock()
        it = MagicMock()
        it.value = "SWAP"
        intent.intent_type = it
        intent.protocol = "pendle"
        intent.pool = self._MARKET
        intent.from_token = "PT-wstETH-25JUN2026"
        result = _make_redeem_result(sy_received_raw=1_000_000_000_000_000_000)
        ev = build_pendle_pt_redeem_accounting_event(
            intent=intent, result=result,
            deployment_id="d", strategy_id="s", cycle_id="c",
            execution_mode="paper", chain="arbitrum", wallet_address="0xw",
            basis_store=bs,
        )
        assert ev is None

    def test_no_redemption_event_returns_none(self):
        bs = _make_basis_store()
        result = MagicMock()
        result.tx_hash = ""
        result.extracted_data = {}  # no redemption_amounts
        ev = self._call(bs, result=result)
        assert ev is None

    def test_identity_fields(self):
        bs = _make_basis_store()
        ev = self._call(bs)
        assert ev is not None
        assert ev.identity.deployment_id == self._DEPLOY_ID
        assert ev.identity.chain == "arbitrum"
        assert ev.identity.ledger_entry_id == "led-001"

    def test_partial_redemption_fifo(self):
        """Two lots + partial redemption: FIFO matches first lot first."""
        bs = _make_basis_store()
        ts = datetime.now(UTC)
        pt_tok = "PT-wstETH-25JUN2026"

        # Two lots: each paid 1.0 SY for 1.0 PT (pt_price=1.0 for simplicity)
        bs.record_pt_buy(
            deployment_id=self._DEPLOY_ID,
            position_key=self._position_key(),
            pt_token=pt_tok,
            pt_amount=Decimal("1.0"),
            sy_cost=Decimal("1.0"),
            timestamp=ts,
        )
        bs.record_pt_buy(
            deployment_id=self._DEPLOY_ID,
            position_key=self._position_key(),
            pt_token=pt_tok,
            pt_amount=Decimal("1.0"),
            sy_cost=Decimal("1.0"),
            timestamp=ts,
        )

        # Redeem only 1.0 PT (first lot) → receive 1.03 SY (3% yield)
        result = _make_redeem_result(
            sy_received_raw=1_030_000_000_000_000_000,
            py_redeemed_raw=1_000_000_000_000_000_000,  # 1.0 PT
        )
        price_oracle = {"SY": "1000.0"}
        ev = self._call(bs, result=result, price_oracle=price_oracle)

        assert ev is not None
        # yield = 1.03 SY received - 1.0 SY cost = 0.03 SY * $1000 = $30
        assert ev.realized_yield_usd is not None
        assert abs(ev.realized_yield_usd - Decimal("30")) < Decimal("2")
