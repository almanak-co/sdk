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

import pytest

# ─── Fixtures ────────────────────────────────────────────────────────────────

# Stable, test-owned PT registry. Decoupled from production PT_TOKEN_INFO so
# unrelated connector data refreshes (new chains, renamed aliases, address
# rotations) cannot regress these unit tests. Two aliases on the same address
# exercise the dict-iteration-order ambiguity in _resolve_pt_token_sym.
_FAKE_PT_ADDR = "0xbE45F6F17b81571fC30253BDaE0A2A6f7b04D60F"
_FAKE_PT_TOKEN_INFO: dict[str, dict[str, tuple[str, int]]] = {
    "plasma": {
        "PT-FAKEUSD": (_FAKE_PT_ADDR, 6),
        "PT-fakeUSD": (_FAKE_PT_ADDR, 6),
    },
}


@pytest.fixture
def fake_pt_token_info(monkeypatch):
    """Patch PT_TOKEN_INFO on its source module with a stable test fixture.

    The SUT imports PT_TOKEN_INFO inside the helper functions
    (``from almanak.connectors.pendle.sdk import PT_TOKEN_INFO``),
    so patching the attribute on the source module is sufficient — no need to
    reach into the SUT module.
    """
    import almanak.connectors.pendle.sdk as _pendle_sdk
    monkeypatch.setattr(_pendle_sdk, "PT_TOKEN_INFO", _FAKE_PT_TOKEN_INFO)
    return _FAKE_PT_TOKEN_INFO


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
            deployment_id="s", cycle_id="c",
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


# ─── Helper detection / resolution (Phase 6b-2) ──────────────────────────────


class TestIsPtSell:
    """Direct coverage of the ``_is_pt_sell`` predicate.

    The end-to-end builder tests above only exercise the simple
    ``from_token.upper().startswith("PT-")`` branch. These tests cover the
    address-lookup + swap_amounts fallback paths.
    """

    def test_pt_dash_prefix_returns_true(self):
        from almanak.framework.accounting.pendle_pt_sell_accounting import _is_pt_sell

        intent = _make_sell_intent(from_token="PT-wstETH-25JUN2026")
        assert _is_pt_sell(intent, _make_sell_result()) is True

    def test_pt_dash_prefix_case_insensitive(self):
        from almanak.framework.accounting.pendle_pt_sell_accounting import _is_pt_sell

        intent = _make_sell_intent(from_token="pt-wstETH-25JUN2026")
        assert _is_pt_sell(intent, _make_sell_result()) is True

    def test_apt_optimism_prefix_does_not_match(self):
        """Guard rail: 'APT' or 'OPT' must not be classified as PT.

        The predicate requires the dash explicitly to avoid false positives.
        """
        from almanak.framework.accounting.pendle_pt_sell_accounting import _is_pt_sell

        for sym in ("APT", "OPT", "PTX", "PTABC"):
            intent = _make_sell_intent(from_token=sym)
            result = MagicMock()
            result.extracted_data = {}
            assert _is_pt_sell(intent, result) is False, sym

    def test_pt_address_match_with_chain(self, fake_pt_token_info):
        from almanak.framework.accounting.pendle_pt_sell_accounting import _is_pt_sell

        # Address comes from the test-owned fixture (decoupled from production registry).
        intent = _make_sell_intent(from_token=_FAKE_PT_ADDR)
        intent.chain = "plasma"
        result = MagicMock()
        result.extracted_data = {}
        assert _is_pt_sell(intent, result) is True

    def test_pt_address_match_case_insensitive(self, fake_pt_token_info):
        from almanak.framework.accounting.pendle_pt_sell_accounting import _is_pt_sell

        # Lower-cased address still matches.
        intent = _make_sell_intent(from_token=_FAKE_PT_ADDR.lower())
        intent.chain = "plasma"
        result = MagicMock()
        result.extracted_data = {}
        assert _is_pt_sell(intent, result) is True

    def test_pt_address_match_without_chain_searches_all(self, fake_pt_token_info):
        """When intent.chain is empty/missing, the predicate falls back to a
        cross-chain scan of PT_TOKEN_INFO.
        """
        from almanak.framework.accounting.pendle_pt_sell_accounting import _is_pt_sell

        intent = _make_sell_intent(from_token=_FAKE_PT_ADDR)
        intent.chain = ""  # force the all-chains branch
        result = MagicMock()
        result.extracted_data = {}
        assert _is_pt_sell(intent, result) is True

    def test_unknown_pt_address_returns_false(self, fake_pt_token_info):
        from almanak.framework.accounting.pendle_pt_sell_accounting import _is_pt_sell

        intent = _make_sell_intent(from_token="0xdeadbeef" + "0" * 32)
        intent.chain = "plasma"
        result = MagicMock()
        result.extracted_data = {}
        assert _is_pt_sell(intent, result) is False

    def test_unknown_pt_address_no_chain_returns_false(self, fake_pt_token_info):
        from almanak.framework.accounting.pendle_pt_sell_accounting import _is_pt_sell

        intent = _make_sell_intent(from_token="0x" + "1" * 40)
        intent.chain = ""
        result = MagicMock()
        result.extracted_data = {}
        assert _is_pt_sell(intent, result) is False

    def test_swap_amounts_token_in_pt_dash_fallback(self):
        """When intent.from_token is non-PT but the enriched receipt's
        swap_amounts.token_in is a PT- symbol, the predicate still detects it.
        """
        from almanak.framework.accounting.pendle_pt_sell_accounting import _is_pt_sell

        intent = _make_sell_intent(from_token="someothertoken")
        result = MagicMock()
        swap_amounts = MagicMock()
        swap_amounts.token_in = "PT-wstETH-25JUN2026"
        result.extracted_data = {"swap_amounts": swap_amounts}
        assert _is_pt_sell(intent, result) is True

    def test_pt_token_info_import_exception_swallowed(self, monkeypatch):
        """If PT_TOKEN_INFO import fails, the address branch must not raise."""
        import sys

        from almanak.framework.accounting.pendle_pt_sell_accounting import _is_pt_sell

        # Setting sys.modules[name] = None makes a subsequent
        # ``from name import attr`` raise ImportError without poisoning
        # builtins.__import__. Idiomatic pytest pattern.
        monkeypatch.setitem(sys.modules, "almanak.connectors.pendle.sdk", None)

        intent = _make_sell_intent(from_token="0x" + "a" * 40)
        intent.chain = "plasma"
        result = MagicMock()
        result.extracted_data = {}
        # Should not raise; falls through to swap_amounts check then returns False.
        assert _is_pt_sell(intent, result) is False

    def test_empty_from_token_with_no_swap_amounts_returns_false(self):
        from almanak.framework.accounting.pendle_pt_sell_accounting import _is_pt_sell

        intent = _make_sell_intent(from_token="")
        result = MagicMock()
        result.extracted_data = {}
        assert _is_pt_sell(intent, result) is False


class TestResolvePtTokenSym:
    """Direct coverage of the ``_resolve_pt_token_sym`` symbol resolver."""

    def test_pt_dash_prefix_returned_as_is(self):
        from almanak.framework.accounting.pendle_pt_sell_accounting import _resolve_pt_token_sym

        intent = _make_sell_intent(from_token="PT-wstETH-25JUN2026")
        assert _resolve_pt_token_sym(intent, _make_sell_result()) == "PT-wstETH-25JUN2026"

    def test_address_resolves_to_canonical_symbol_with_chain(self, fake_pt_token_info):
        from almanak.framework.accounting.pendle_pt_sell_accounting import _resolve_pt_token_sym

        intent = _make_sell_intent(from_token=_FAKE_PT_ADDR)
        intent.chain = "plasma"
        result = MagicMock()
        result.extracted_data = {}
        sym = _resolve_pt_token_sym(intent, result)
        # Either alias is acceptable: the test fixture registers both PT-FAKEUSD and
        # PT-fakeUSD on the same address; dict-iteration order picks one.
        assert sym in {"PT-FAKEUSD", "PT-fakeUSD"}

    def test_address_resolves_without_chain_searches_all(self, fake_pt_token_info):
        from almanak.framework.accounting.pendle_pt_sell_accounting import _resolve_pt_token_sym

        intent = _make_sell_intent(from_token=_FAKE_PT_ADDR)
        intent.chain = ""
        result = MagicMock()
        result.extracted_data = {}
        sym = _resolve_pt_token_sym(intent, result)
        assert sym in {"PT-FAKEUSD", "PT-fakeUSD"}

    def test_unknown_address_falls_through_to_from_token(self, fake_pt_token_info):
        from almanak.framework.accounting.pendle_pt_sell_accounting import _resolve_pt_token_sym

        addr = "0x" + "9" * 40
        intent = _make_sell_intent(from_token=addr)
        intent.chain = "plasma"
        result = MagicMock()
        result.extracted_data = {}
        # Falls through past the address lookup, no swap_amounts → returns from_token verbatim.
        assert _resolve_pt_token_sym(intent, result) == addr

    def test_swap_amounts_token_in_pt_dash_fallback(self):
        """Non-PT from_token + non-address path → use swap_amounts.token_in if PT-prefixed."""
        from almanak.framework.accounting.pendle_pt_sell_accounting import _resolve_pt_token_sym

        intent = _make_sell_intent(from_token="someotherstring")
        result = MagicMock()
        swap_amounts = MagicMock()
        swap_amounts.token_in = "PT-wstETH-25JUN2026"
        result.extracted_data = {"swap_amounts": swap_amounts}
        assert _resolve_pt_token_sym(intent, result) == "PT-wstETH-25JUN2026"

    def test_empty_from_token_and_no_swap_amounts_returns_pt(self):
        """Last-resort sentinel — never returns empty string."""
        from almanak.framework.accounting.pendle_pt_sell_accounting import _resolve_pt_token_sym

        intent = _make_sell_intent(from_token="")
        result = MagicMock()
        result.extracted_data = {}
        assert _resolve_pt_token_sym(intent, result) == "PT"

    def test_non_pt_from_token_with_no_swap_amounts_returns_from_token(self):
        from almanak.framework.accounting.pendle_pt_sell_accounting import _resolve_pt_token_sym

        intent = _make_sell_intent(from_token="WETH")
        result = MagicMock()
        result.extracted_data = {}
        assert _resolve_pt_token_sym(intent, result) == "WETH"

    def test_pt_token_info_import_exception_swallowed(self, monkeypatch):
        """If PT_TOKEN_INFO import fails, resolution falls through gracefully."""
        import sys

        from almanak.framework.accounting.pendle_pt_sell_accounting import _resolve_pt_token_sym

        # Idiomatic pytest pattern for simulating a missing module — does not
        # touch builtins.__import__.
        monkeypatch.setitem(sys.modules, "almanak.connectors.pendle.sdk", None)

        addr = "0x" + "a" * 40
        intent = _make_sell_intent(from_token=addr)
        intent.chain = "plasma"
        result = MagicMock()
        result.extracted_data = {}
        # Falls through past the address lookup, no swap_amounts → returns from_token.
        assert _resolve_pt_token_sym(intent, result) == addr
