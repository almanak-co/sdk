"""Unit tests for VIB-3421: Pendle LP accounting event builder.

Tests:
  1. LP_OPEN with Mint data → PendleAccountingEvent(LP_OPEN) built correctly
  2. LP_CLOSE with Burn data → PendleAccountingEvent(LP_CLOSE) built correctly
  3. Non-Pendle protocol → returns None
  4. Non-LP intent type → returns None
  5. Missing extracted data → event built with None amounts, confidence ESTIMATED
  6. extract_lp_open_data wires amounts into LPOpenData with correct fields
  7. extract_lp_close_data wires amounts into LPCloseData with correct fields
  8. position_key derivation is stable across calls with same market address
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock


# ─── Test fixtures ─────────────────────────────────────────────────────────────

def _make_intent(intent_type: str, protocol: str = "pendle", pool: str = "0xabcdef0123456789abcdef0123456789abcdef01"):
    intent = MagicMock()
    it = MagicMock()
    it.value = intent_type
    intent.intent_type = it
    intent.protocol = protocol
    intent.pool = pool
    return intent


def _make_result(intent_type: str, market_address: str = "0xabcdef0123456789abcdef0123456789abcdef01"):
    result = MagicMock()
    result.tx_hash = "0xdeadbeef12345678"

    from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData

    if intent_type == "LP_OPEN":
        lp_open = LPOpenData(
            position_id=0,  # Pendle: no NFT tokenId; position_id from extract_position_id
            liquidity=1_000_000_000_000_000_000,  # 1e18 LP tokens
            amount0=500_000_000_000_000_000,       # 0.5e18 SY
            amount1=250_000_000_000_000_000,       # 0.25e18 PT
        )
        result.extracted_data = {"lp_open_data": lp_open}
    else:
        lp_close = LPCloseData(
            amount0_collected=520_000_000_000_000_000,  # SY returned
            amount1_collected=260_000_000_000_000_000,  # PT returned
            liquidity_removed=1_000_000_000_000_000_000,
        )
        result.extracted_data = {"lp_close_data": lp_close}

    return result


# ─── Builder tests ─────────────────────────────────────────────────────────────

class TestBuildPendleLpAccountingEvent:

    def _call(self, intent_type: str = "LP_OPEN", protocol: str = "pendle"):
        from almanak.framework.accounting.pendle_accounting import build_pendle_lp_accounting_event

        intent = _make_intent(intent_type, protocol)
        result = _make_result(intent_type)
        return build_pendle_lp_accounting_event(
            intent=intent,
            result=result,
            deployment_id="dep-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain="arbitrum",
            wallet_address="0xwallet",
            ledger_entry_id="led-001",
        )

    def test_lp_open_event_type(self):
        from almanak.framework.accounting.models import PendleEventType

        ev = self._call("LP_OPEN")
        assert ev is not None
        assert ev.event_type == PendleEventType.PENDLE_LP_OPEN

    def test_lp_close_event_type(self):
        from almanak.framework.accounting.models import PendleEventType

        ev = self._call("LP_CLOSE")
        assert ev is not None
        assert ev.event_type == PendleEventType.PENDLE_LP_CLOSE

    def test_lp_open_amounts_populated(self):
        ev = self._call("LP_OPEN")
        assert ev.sy_amount == Decimal("0.5")    # 5e17 raw / 1e18
        assert ev.pt_amount == Decimal("0.25")   # 25e16 raw / 1e18

    def test_lp_close_amounts_populated(self):
        ev = self._call("LP_CLOSE")
        assert ev.sy_amount == Decimal("0.52")   # 52e16 raw / 1e18
        assert ev.pt_amount == Decimal("0.26")   # 26e16 raw / 1e18

    def test_non_pendle_protocol_returns_none(self):
        ev = self._call("LP_OPEN", protocol="uniswap_v3")
        assert ev is None

    def test_non_lp_intent_returns_none(self):
        from almanak.framework.accounting.pendle_accounting import build_pendle_lp_accounting_event

        intent = _make_intent("SWAP", "pendle")
        result = MagicMock()
        result.tx_hash = ""
        result.extracted_data = {}
        ev = build_pendle_lp_accounting_event(
            intent=intent, result=result,
            deployment_id="s", cycle_id="c",
            execution_mode="paper", chain="arbitrum", wallet_address="0xw",
        )
        assert ev is None

    def test_identity_fields_populated(self):
        ev = self._call("LP_OPEN")
        assert ev.identity.deployment_id == "dep-1"
        assert ev.identity.chain == "arbitrum"
        assert ev.identity.ledger_entry_id == "led-001"

    def test_position_key_stable(self):
        from almanak.framework.accounting.pendle_accounting import _derive_pendle_position_key

        market = "0xabcdef0123456789abcdef0123456789abcdef01"
        k1 = _derive_pendle_position_key("arbitrum", "0xWallet", market)
        k2 = _derive_pendle_position_key("arbitrum", "0xWallet", market)
        assert k1 == k2
        assert "pendle_lp" in k1
        assert "arbitrum" in k1

    def test_identity_id_is_deterministic(self):
        """Same inputs produce the same identity.id on repeated calls (uuid5, not uuid4)."""
        ev1 = self._call("LP_OPEN")
        ev2 = self._call("LP_OPEN")
        assert ev1 is not None and ev2 is not None
        assert ev1.identity.id == ev2.identity.id

    def test_identity_id_differs_by_intent_type(self):
        """LP_OPEN and LP_CLOSE produce different identity IDs even with the same tx_hash."""
        ev_open = self._call("LP_OPEN")
        ev_close = self._call("LP_CLOSE")
        assert ev_open is not None and ev_close is not None
        assert ev_open.identity.id != ev_close.identity.id

    def test_missing_extracted_data_yields_estimated(self):
        from almanak.framework.accounting.models import AccountingConfidence
        from almanak.framework.accounting.pendle_accounting import build_pendle_lp_accounting_event

        intent = _make_intent("LP_OPEN")
        result = MagicMock()
        result.tx_hash = ""
        result.extracted_data = {}
        ev = build_pendle_lp_accounting_event(
            intent=intent, result=result,
            deployment_id="s", cycle_id="c",
            execution_mode="paper", chain="arbitrum", wallet_address="0xw",
        )
        assert ev is not None
        assert ev.sy_amount is None
        assert ev.pt_amount is None
        assert ev.confidence == AccountingConfidence.ESTIMATED


# ─── Receipt parser extraction tests ─────────────────────────────────────────

class TestPendleReceiptParserLPExtraction:

    def _make_receipt_with_mint(self, market: str = "0xabcdef0123456789abcdef0123456789abcdef01"):
        """Minimal receipt dict with a Pendle Mint log."""
        from almanak.framework.connectors.pendle.receipt_parser import (
            EVENT_TOPICS,
            MintEventData,
            ParseResult,
            PendleReceiptParser,
        )

        parser = PendleReceiptParser(chain="arbitrum")
        mint = MintEventData(
            receiver="0xwallet",
            net_lp_minted=1_000_000_000_000_000_000,
            net_sy_used=500_000_000_000_000_000,
            net_pt_used=250_000_000_000_000_000,
            market_address=market,
        )
        parse_result = ParseResult(
            success=True,
            mint_events=[mint],
        )
        # Monkey-patch parse_receipt to return our fixture
        parser.parse_receipt = lambda r: parse_result
        return parser

    def test_extract_lp_open_data_fields(self):
        market = "0xabcdef0123456789abcdef0123456789abcdef01"
        parser = self._make_receipt_with_mint(market)
        lp_open = parser.extract_lp_open_data({})
        assert lp_open is not None
        assert lp_open.amount0 == 500_000_000_000_000_000  # SY
        assert lp_open.amount1 == 250_000_000_000_000_000  # PT
        assert lp_open.liquidity == 1_000_000_000_000_000_000
        # position_id=0: Pendle has no NFT tokenId; canonical id comes from extract_position_id
        assert lp_open.position_id == 0

    def test_extract_lp_open_data_no_mint_returns_none(self):
        from almanak.framework.connectors.pendle.receipt_parser import ParseResult, PendleReceiptParser

        parser = PendleReceiptParser(chain="arbitrum")
        parser.parse_receipt = lambda r: ParseResult(success=True)
        assert parser.extract_lp_open_data({}) is None

    def test_extract_lp_close_data_fields(self):
        from almanak.framework.connectors.pendle.receipt_parser import BurnEventData, ParseResult, PendleReceiptParser

        parser = PendleReceiptParser(chain="arbitrum")
        burn = BurnEventData(
            receiver_sy="0xwallet",
            receiver_pt="0xwallet",
            net_lp_burned=1_000_000_000_000_000_000,
            net_sy_out=520_000_000_000_000_000,
            net_pt_out=260_000_000_000_000_000,
            market_address="0xabcdef0123456789abcdef0123456789abcdef01",
        )
        parser.parse_receipt = lambda r: ParseResult(success=True, burn_events=[burn])
        lp_close = parser.extract_lp_close_data({})
        assert lp_close is not None
        assert lp_close.amount0_collected == 520_000_000_000_000_000
        assert lp_close.amount1_collected == 260_000_000_000_000_000
        assert lp_close.liquidity_removed == 1_000_000_000_000_000_000

    def test_extract_lp_close_data_no_burn_returns_none(self):
        from almanak.framework.connectors.pendle.receipt_parser import ParseResult, PendleReceiptParser

        parser = PendleReceiptParser(chain="arbitrum")
        parser.parse_receipt = lambda r: ParseResult(success=True)
        assert parser.extract_lp_close_data({}) is None

    def test_supported_extractions_declared(self):
        from almanak.framework.connectors.pendle.receipt_parser import PendleReceiptParser

        assert "lp_open_data" in PendleReceiptParser.SUPPORTED_EXTRACTIONS
        assert "lp_close_data" in PendleReceiptParser.SUPPORTED_EXTRACTIONS


# ─── Pool normalization tests ──────────────────────────────────────────────────

class TestGetMarketAddress:

    def test_bare_address(self):
        from almanak.framework.accounting.pendle_accounting import _get_market_address

        intent = MagicMock()
        market = "0xabcdef0123456789abcdef0123456789abcdef01"
        intent.pool = market
        assert _get_market_address(intent) == market.lower()

    def test_token_slash_address_format(self):
        from almanak.framework.accounting.pendle_accounting import _get_market_address

        intent = MagicMock()
        market = "0xabcdef0123456789abcdef0123456789abcdef01"
        intent.pool = f"USDC/{market}"
        assert _get_market_address(intent) == market.lower()

    def test_none_pool_returns_empty(self):
        from almanak.framework.accounting.pendle_accounting import _get_market_address

        intent = MagicMock()
        intent.pool = None
        assert _get_market_address(intent) == ""

    def test_pt_name_without_address_returns_empty(self):
        from almanak.framework.accounting.pendle_accounting import _get_market_address

        intent = MagicMock()
        intent.pool = "USDC/PT-sDAI-27JUN2025"
        assert _get_market_address(intent) == ""


# ─── Confidence always ESTIMATED tests ────────────────────────────────────────

class TestConfidenceAlwaysEstimated:

    def _build(self, extracted: dict):
        from almanak.framework.accounting.pendle_accounting import build_pendle_lp_accounting_event

        intent = _make_intent("LP_OPEN")
        result = MagicMock()
        result.tx_hash = ""
        result.transaction_results = []
        result.extracted_data = extracted
        return build_pendle_lp_accounting_event(
            intent=intent, result=result,
deployment_id="s", cycle_id="c",
            execution_mode="paper", chain="arbitrum", wallet_address="0xw",
        )

    def test_both_amounts_present_still_estimated(self):
        from almanak.framework.accounting.models import AccountingConfidence
        from almanak.framework.execution.extracted_data import LPOpenData

        ev = self._build({"lp_open_data": LPOpenData(
            position_id=0,
            liquidity=1_000_000_000_000_000_000,
            amount0=500_000_000_000_000_000,
            amount1=250_000_000_000_000_000,
        )})
        assert ev is not None
        assert ev.confidence == AccountingConfidence.ESTIMATED

    def test_one_amount_missing_is_estimated(self):
        from almanak.framework.accounting.models import AccountingConfidence
        from almanak.framework.execution.extracted_data import LPOpenData

        ev = self._build({"lp_open_data": LPOpenData(
            position_id=0,
            liquidity=1_000_000_000_000_000_000,
            amount0=500_000_000_000_000_000,
            amount1=None,
        )})
        assert ev is not None
        assert ev.confidence == AccountingConfidence.ESTIMATED

    def test_no_amounts_is_estimated(self):
        from almanak.framework.accounting.models import AccountingConfidence

        ev = self._build({})
        assert ev is not None
        assert ev.confidence == AccountingConfidence.ESTIMATED
