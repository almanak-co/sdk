"""Tests that ResultEnricher routes Polymarket BUY/SELL through the parser (VIB-3708).

VIB-3706 added an off-chain extraction path that read directly from
``result.prediction_fill``. VIB-3708 refactors that path to construct an
``OrderResponse``-shaped dict and call
:meth:`PolymarketReceiptParser.parse_order_response` so:

* Single source of truth for parse logic (parser, not enricher).
* Parser's previously-dead off-chain methods become live and testable.
* Edge cases (partial fills, explicit fees) handled in one place.

The fallback to the direct ``prediction_fill`` read is kept for parser
failures so VIB-3706's data-preservation guarantee survives a parser bug.

These tests cover:

a. PREDICTION_BUY routes through ``parse_order_response`` — the patched
   parser method is invoked exactly once with the constructed order dict.
b. The constructed order dict has the expected ``OrderResponse`` shape
   (``orderID``, ``status``, ``price``, ``size``, ``filledSize``,
   ``avgPrice``, ``side``, ``market``).
c. A successful parse populates ``extracted_data`` from the
   :class:`TradeResult` (proves the parser's output is the source, not a
   second-hand read of ``prediction_fill``).
d. A parser exception triggers the fallback — ``extracted_data`` is still
   populated from ``prediction_fill`` and a structured warning is emitted.
e. PREDICTION_SELL routes through the parser the same way as BUY but
   with sell semantics (``outcome_tokens_sold`` / ``proceeds``, ``side="SELL"``).
f. PREDICTION_REDEEM stays on the on-chain CTF parser path (regression
   guard — VIB-3708 must not change redemption enrichment).
g. Smoke import: the parser's three off-chain methods
   (``parse_order_response`` / ``parse_fill_notification`` /
   ``parse_order_status``) are importable from the same module the
   enricher imports them from. Catches accidental rename / removal that
   would silently break the enricher's lazy import at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import patch

from almanak.connectors.polymarket.receipt_parser import (
    PolymarketReceiptParser,
    TradeResult,
)
from almanak.framework.execution.extracted_data import PredictionFill
from almanak.framework.execution.result_enricher import ResultEnricher


# ---------------------------------------------------------------------------
# Minimal stubs — same shape as test_result_enricher_prediction_offchain.py
# ---------------------------------------------------------------------------


@dataclass
class _FakeReceipt:
    tx_hash: str = "0xabc"
    block_number: int = 100
    block_hash: str = "0xblock"
    gas_used: int = 200_000
    effective_gas_price: int = 1_000_000_000
    status: int = 1
    logs: list = field(default_factory=list)
    from_address: str | None = None
    to_address: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx_hash": self.tx_hash,
            "block_number": self.block_number,
            "block_hash": self.block_hash,
            "gas_used": self.gas_used,
            "effective_gas_price": str(self.effective_gas_price),
            "status": self.status,
            "logs": self.logs,
            "contract_address": None,
            "from_address": self.from_address,
            "to_address": self.to_address,
        }


@dataclass
class _FakeTxResult:
    success: bool = True
    tx_hash: str = "0xabc"
    receipt: _FakeReceipt | None = None
    gas_used: int = 200_000


@dataclass
class _FakeExecResult:
    success: bool = True
    transaction_results: list = field(default_factory=list)
    position_id: int | None = None
    swap_amounts: Any = None
    lp_close_data: Any = None
    bridge_data: Any = None
    prediction_fill: PredictionFill | None = None
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)


@dataclass
class _FakeContext:
    chain: str = "polygon"
    protocol: str | None = None


@dataclass
class _FakePredictionIntent:
    intent_type: str = "PREDICTION_BUY"
    protocol: str | None = "polymarket"
    market_id: str | None = (
        "0xbaed1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab"
    )


class _RedeemParserSpy:
    """Parser spy that proves PREDICTION_REDEEM still routes through extract_*."""

    SUPPORTED_EXTRACTIONS = frozenset(
        {"redemption_amount", "payout", "market_id"}
    )

    def __init__(self) -> None:
        self.calls: list[str] = []

    def extract_redemption_amount(self, receipt: dict) -> int | None:  # noqa: ARG002
        self.calls.append("redemption_amount")
        return 5_000_000

    def extract_payout(self, receipt: dict) -> int | None:  # noqa: ARG002
        self.calls.append("payout")
        return 5_000_000

    def extract_market_id(self, receipt: dict) -> str | None:  # noqa: ARG002
        self.calls.append("market_id")
        return "0xbaed-redeem"

    def parse_receipt(self, receipt: dict) -> dict:  # noqa: ARG002
        return {}


class _StubRegistry:
    def __init__(self, parser: object | None = None) -> None:
        self._parser = parser

    def get(self, protocol: str, **kwargs: object):  # noqa: ARG002
        if self._parser is None:
            raise ValueError(f"no parser for {protocol}")
        return self._parser


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MARKET_ID = "0xbaed1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab"

BUY_FILL = PredictionFill(
    filled_shares=Decimal("5.45"),
    requested_shares=Decimal("6.00"),
    avg_fill_price=Decimal("0.55"),
    order_id="clob-buy-1",
    status="matched",
)

SELL_FILL = PredictionFill(
    filled_shares=Decimal("3.0"),
    requested_shares=Decimal("3.0"),
    avg_fill_price=Decimal("0.60"),
    order_id="clob-sell-1",
    status="matched",
)


def _bundle_meta(market_id: str | None = MARKET_ID) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "intent_id": "intent-1",
        "side": "BUY",
        "protocol": "polymarket",
        "chain": "polygon",
    }
    if market_id is not None:
        meta["market_id"] = market_id
    return meta


# ===========================================================================
# (a) PREDICTION_BUY routes through parse_order_response
# ===========================================================================


class TestBuyRoutesThroughParser:
    def test_buy_invokes_parse_order_response_with_constructed_dict(self):
        """Patch ``parse_order_response`` and assert the enricher called it."""
        result = _FakeExecResult(
            transaction_results=[],
            prediction_fill=BUY_FILL,
            extracted_data={"order_id": "clob-buy-1"},
        )
        intent = _FakePredictionIntent(intent_type="PREDICTION_BUY", market_id=MARKET_ID)
        context = _FakeContext(chain="polygon", protocol="polymarket")

        # Spy returns a TradeResult that downstream mapping will use.
        spy_trade_result = TradeResult(
            success=True,
            order_id="clob-buy-1",
            status="MATCHED",
            filled_size=Decimal("5.45"),
            avg_price=Decimal("0.55"),
            side="BUY",
            token_id=MARKET_ID,
            timestamp=datetime(2025, 1, 15, 10, 30, tzinfo=UTC),
        )
        with patch.object(
            PolymarketReceiptParser,
            "parse_order_response",
            return_value=spy_trade_result,
        ) as mocked:
            enricher = ResultEnricher(parser_registry=_StubRegistry(parser=PolymarketReceiptParser()))
            enricher.enrich(
                result, intent, context, bundle_metadata=_bundle_meta()
            )

            assert mocked.call_count == 1, (
                "parse_order_response must be invoked exactly once for BUY"
            )
            (call_arg,) = mocked.call_args.args
            assert isinstance(call_arg, dict), "argument must be the OrderResponse dict"


# ===========================================================================
# (b) Constructed order dict shape
# ===========================================================================


class TestConstructedOrderDictShape:
    def test_buy_dict_has_documented_orderresponse_shape(self):
        """Verify every documented OrderResponse field is present and well-typed."""
        result = _FakeExecResult(
            transaction_results=[],
            prediction_fill=BUY_FILL,
            extracted_data={"order_id": "clob-buy-1"},
        )
        intent = _FakePredictionIntent(intent_type="PREDICTION_BUY", market_id=MARKET_ID)
        context = _FakeContext(chain="polygon", protocol="polymarket")

        captured: dict[str, Any] = {}

        def _capture(self, response):  # noqa: ARG001 - parser self
            captured.update(response)
            return TradeResult(
                success=True,
                order_id=response.get("orderID"),
                status=response.get("status"),
                filled_size=Decimal(str(response.get("filledSize", "0"))),
                avg_price=Decimal(str(response.get("avgPrice", "0"))),
            )

        with patch.object(PolymarketReceiptParser, "parse_order_response", _capture):
            enricher = ResultEnricher(parser_registry=_StubRegistry(parser=PolymarketReceiptParser()))
            enricher.enrich(
                result, intent, context, bundle_metadata=_bundle_meta()
            )

        # Fields documented on parse_order_response — every one must appear
        # because partial dicts mask edge cases (e.g. partial-fill detection
        # that reads ``size``).
        for key in (
            "orderID",
            "status",
            "side",
            "size",
            "filledSize",
            "avgPrice",
            "price",
            "market",
        ):
            assert key in captured, f"order dict missing documented field {key!r}"

        assert captured["orderID"] == "clob-buy-1"
        assert captured["status"] == "matched"
        assert captured["side"] == "BUY", "BUY intent must produce side=BUY"
        assert captured["size"] == "6.00", "size must be requested_shares as string"
        assert captured["filledSize"] == "5.45"
        assert captured["avgPrice"] == "0.55"
        # parse_order_response falls back to ``price`` when ``avgPrice`` is
        # absent; populate both so the fallback would still work if avgPrice
        # were stripped by a future change.
        assert captured["price"] == "0.55"
        assert captured["market"] == MARKET_ID

    def test_buy_dict_falls_back_to_prediction_fill_order_id_when_extracted_data_empty(self):
        """When the runner did not stamp order_id, the PredictionFill value wins."""
        result = _FakeExecResult(
            transaction_results=[],
            prediction_fill=BUY_FILL,
            # extracted_data starts empty — no ``order_id`` stamped yet.
        )
        intent = _FakePredictionIntent(intent_type="PREDICTION_BUY", market_id=MARKET_ID)
        context = _FakeContext(chain="polygon", protocol="polymarket")

        captured: dict[str, Any] = {}

        def _capture(self, response):  # noqa: ARG001
            captured.update(response)
            return TradeResult(
                success=True,
                filled_size=Decimal(str(response.get("filledSize", "0"))),
                avg_price=Decimal(str(response.get("avgPrice", "0"))),
            )

        with patch.object(PolymarketReceiptParser, "parse_order_response", _capture):
            enricher = ResultEnricher(parser_registry=_StubRegistry(parser=PolymarketReceiptParser()))
            enricher.enrich(
                result, intent, context, bundle_metadata=_bundle_meta()
            )

        assert captured["orderID"] == BUY_FILL.order_id


# ===========================================================================
# (c) Successful parse: extracted_data populated from TradeResult, not raw fill
# ===========================================================================


class TestExtractedDataFromTradeResult:
    def test_extracted_data_uses_trade_result_values(self):
        """Patched TradeResult returns deliberately-different numbers from
        ``prediction_fill``; the enricher must use the parser's values."""
        result = _FakeExecResult(
            transaction_results=[],
            prediction_fill=BUY_FILL,  # 5.45 @ 0.55
        )
        intent = _FakePredictionIntent(intent_type="PREDICTION_BUY", market_id=MARKET_ID)
        context = _FakeContext(chain="polygon", protocol="polymarket")

        # TradeResult intentionally carries DIFFERENT values from BUY_FILL —
        # if the enricher were still reading prediction_fill directly, the
        # assertions below would see 5.45 / 2.9975 instead of these.
        parser_filled = Decimal("4.20")
        parser_price = Decimal("0.50")
        spy_trade_result = TradeResult(
            success=True,
            order_id="clob-buy-1",
            status="MATCHED",
            filled_size=parser_filled,
            avg_price=parser_price,
        )
        with patch.object(
            PolymarketReceiptParser,
            "parse_order_response",
            return_value=spy_trade_result,
        ):
            enricher = ResultEnricher(parser_registry=_StubRegistry(parser=PolymarketReceiptParser()))
            enriched = enricher.enrich(
                result, intent, context, bundle_metadata=_bundle_meta()
            )

        assert enriched.extracted_data["outcome_tokens_received"] == parser_filled
        # cost_basis is derived from the parser's filled_size * avg_price
        # (4.20 * 0.50 = 2.10) — NOT from prediction_fill (which would be 2.9975).
        assert enriched.extracted_data["cost_basis"] == parser_filled * parser_price
        assert enriched.extracted_data["cost_basis"] == Decimal("2.10")
        assert enriched.extracted_data["market_id"] == MARKET_ID
        # The on-chain registry stub returns no parser, which legitimately
        # logs a "Parser not found" warning. That warning is unrelated to
        # the off-chain branch — assert no parse-error / fallback warnings.
        assert not any(
            "parse_order_response" in w or "TradeResult" in w
            for w in enriched.extraction_warnings
        ), f"Unexpected parser warning: {enriched.extraction_warnings}"


# ===========================================================================
# (d) Parser exception triggers fallback path
# ===========================================================================


class TestParserExceptionFallback:
    def test_parser_raises_extracted_data_still_populated_from_prediction_fill(self):
        """A parser bug must NOT silently lose fill data."""
        result = _FakeExecResult(
            transaction_results=[],
            prediction_fill=BUY_FILL,  # 5.45 @ 0.55 -> cost_basis 2.9975
        )
        intent = _FakePredictionIntent(intent_type="PREDICTION_BUY", market_id=MARKET_ID)
        context = _FakeContext(chain="polygon", protocol="polymarket")

        with patch.object(
            PolymarketReceiptParser,
            "parse_order_response",
            side_effect=RuntimeError("simulated parser bug"),
        ):
            enricher = ResultEnricher(parser_registry=_StubRegistry(parser=PolymarketReceiptParser()))
            enriched = enricher.enrich(
                result, intent, context, bundle_metadata=_bundle_meta()
            )

        # Fallback populated from the direct prediction_fill read.
        assert enriched.extracted_data["outcome_tokens_received"] == Decimal("5.45")
        assert enriched.extracted_data["cost_basis"] == Decimal("2.9975")
        assert enriched.extracted_data["market_id"] == MARKET_ID

        # Structured warning emitted so the parser bug is visible to ops.
        assert any(
            "parse_order_response" in w and "simulated parser bug" in w
            for w in enriched.extraction_warnings
        ), f"Expected parser-bug warning, got: {enriched.extraction_warnings}"

    def test_parser_returns_unsuccessful_trade_result_falls_back(self):
        """``TradeResult(success=False)`` must also drop to the fallback path."""
        result = _FakeExecResult(
            transaction_results=[],
            prediction_fill=BUY_FILL,
        )
        intent = _FakePredictionIntent(intent_type="PREDICTION_BUY", market_id=MARKET_ID)
        context = _FakeContext(chain="polygon", protocol="polymarket")

        with patch.object(
            PolymarketReceiptParser,
            "parse_order_response",
            return_value=TradeResult(success=False, error="unparseable response"),
        ):
            enricher = ResultEnricher(parser_registry=_StubRegistry(parser=PolymarketReceiptParser()))
            enriched = enricher.enrich(
                result, intent, context, bundle_metadata=_bundle_meta()
            )

        # Fallback path still populates from prediction_fill.
        assert enriched.extracted_data["outcome_tokens_received"] == Decimal("5.45")
        assert enriched.extracted_data["cost_basis"] == Decimal("2.9975")
        assert any(
            "unparseable response" in w
            for w in enriched.extraction_warnings
        ), f"Expected unsuccessful-TradeResult warning, got: {enriched.extraction_warnings}"


# ===========================================================================
# (e) PREDICTION_SELL routes through parser with sell semantics
# ===========================================================================


class TestSellRoutesThroughParser:
    def test_sell_invokes_parser_with_side_sell_and_maps_to_sold_proceeds(self):
        result = _FakeExecResult(
            transaction_results=[],
            prediction_fill=SELL_FILL,  # 3.0 @ 0.60
            extracted_data={"order_id": "clob-sell-1"},
        )
        intent = _FakePredictionIntent(intent_type="PREDICTION_SELL", market_id=MARKET_ID)
        context = _FakeContext(chain="polygon", protocol="polymarket")

        captured: dict[str, Any] = {}

        def _capture(self, response):  # noqa: ARG001
            captured.update(response)
            return TradeResult(
                success=True,
                order_id=response.get("orderID"),
                status=response.get("status"),
                filled_size=Decimal(str(response.get("filledSize", "0"))),
                avg_price=Decimal(str(response.get("avgPrice", "0"))),
                side="SELL",
            )

        with patch.object(PolymarketReceiptParser, "parse_order_response", _capture):
            enricher = ResultEnricher(parser_registry=_StubRegistry(parser=PolymarketReceiptParser()))
            enriched = enricher.enrich(
                result, intent, context, bundle_metadata=_bundle_meta()
            )

        # Parser was called with side=SELL.
        assert captured["side"] == "SELL", "SELL intent must produce side=SELL"
        # Sell-side spec keys populated from parser output.
        assert enriched.extracted_data["outcome_tokens_sold"] == Decimal("3.0")
        # 3.0 * 0.60 = 1.80
        assert enriched.extracted_data["proceeds"] == Decimal("1.80")
        assert enriched.extracted_data["market_id"] == MARKET_ID
        # Buy-side keys must NOT appear on a sell enrichment.
        assert "outcome_tokens_received" not in enriched.extracted_data
        assert "cost_basis" not in enriched.extracted_data


# ===========================================================================
# (f) PREDICTION_REDEEM keeps the on-chain CTF parser path (regression guard)
# ===========================================================================


class TestRedeemUnchanged:
    def test_redeem_does_not_invoke_parse_order_response(self):
        """Redemption is on-chain — parser CLOB methods must not be called."""
        receipt = _FakeReceipt(status=1, logs=[])
        result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)],
            prediction_fill=None,  # Redemption never carries a CLOB fill.
        )

        @dataclass
        class _RedeemIntent:
            intent_type: str = "PREDICTION_REDEEM"
            protocol: str | None = "polymarket"
            market_id: str | None = MARKET_ID

        intent = _RedeemIntent()
        context = _FakeContext(chain="polygon", protocol="polymarket")

        spy_parser = _RedeemParserSpy()

        with patch.object(
            PolymarketReceiptParser,
            "parse_order_response",
        ) as mocked_clob:
            enricher = ResultEnricher(
                parser_registry=_StubRegistry(parser=spy_parser),
                live_mode=False,
            )
            enricher.enrich(result, intent, context)

            # The CLOB off-chain path must NOT run for REDEEM.
            assert mocked_clob.call_count == 0
        # The on-chain CTF extract methods DID run (regression guard).
        assert "redemption_amount" in spy_parser.calls
        assert "payout" in spy_parser.calls


# ===========================================================================
# (g) Smoke: parser methods importable from the enricher's call path
# ===========================================================================


class TestParserMethodsImportable:
    """Catches accidental rename / removal that would break the lazy import."""

    def test_parse_order_response_importable(self):
        from almanak.connectors.polymarket.receipt_parser import (
            PolymarketReceiptParser as _ImportedParser,
        )

        assert hasattr(_ImportedParser, "parse_order_response")
        assert callable(_ImportedParser.parse_order_response)

    def test_parse_fill_notification_importable(self):
        from almanak.connectors.polymarket.receipt_parser import (
            PolymarketReceiptParser as _ImportedParser,
        )

        assert hasattr(_ImportedParser, "parse_fill_notification")
        assert callable(_ImportedParser.parse_fill_notification)

    def test_parse_order_status_importable(self):
        from almanak.connectors.polymarket.receipt_parser import (
            PolymarketReceiptParser as _ImportedParser,
        )

        assert hasattr(_ImportedParser, "parse_order_status")
        assert callable(_ImportedParser.parse_order_status)

    def test_trade_result_importable(self):
        """``TradeResult`` is the parser's return type — must stay importable."""
        from almanak.connectors.polymarket.receipt_parser import (
            TradeResult as _ImportedTradeResult,
        )

        # Constructable with the canonical success-shape used by the enricher.
        tr = _ImportedTradeResult(
            success=True,
            filled_size=Decimal("1"),
            avg_price=Decimal("0.5"),
        )
        assert tr.success is True
