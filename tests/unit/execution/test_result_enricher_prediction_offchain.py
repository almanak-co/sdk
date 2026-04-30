"""Tests for ResultEnricher off-chain Polymarket CLOB enrichment (VIB-3706).

Polymarket BUY / SELL orders submit off-chain via the CLOB API, so the
runner's CLOB branch attaches a :class:`PredictionFill` to ``result``
instead of producing on-chain receipts. Before this fix, the enricher
short-circuited on the empty-receipts path and the spec fields
(``outcome_tokens_received`` / ``cost_basis`` / ``market_id`` for BUY,
``outcome_tokens_sold`` / ``proceeds`` / ``market_id`` for SELL) were
silently dropped — leading to ledger rows with no fill data.

These tests cover:

a. PREDICTION_BUY off-chain extraction populates outcome_tokens_received,
   cost_basis, market_id from prediction_fill + bundle_metadata.
b. PREDICTION_SELL uses the outcome_tokens_sold / proceeds field labels.
c. PREDICTION_BUY with prediction_fill=None emits a structured warning
   instead of silently failing.
d. PREDICTION_BUY with filled_shares=0 (rejected / resting GTC) emits a
   warning and does NOT fabricate spurious fill values.
e. PREDICTION_REDEEM continues to take the on-chain CTF receipt path; the
   parser is invoked exactly as before — pinned as a regression guard.
f. Non-PREDICTION intents (SWAP) with no receipts keep the original skip
   behavior (no off-chain extraction attempted).
g. PREDICTION_BUY with BOTH prediction_fill and on-chain receipts: the
   off-chain values win (CLOB API is authoritative for fills); the
   on-chain pass does not overwrite them.
h. PREDICTION_BUY where bundle_metadata lacks market_id but the intent
   carries it — falls back to the intent and succeeds.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.execution.extracted_data import PredictionFill
from almanak.framework.execution.result_enricher import ResultEnricher


# ---------------------------------------------------------------------------
# Minimal stubs — mirror the existing result enricher test patterns
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
    """Mirrors the relevant attributes of PredictionBuy/SellIntent."""

    intent_type: str = "PREDICTION_BUY"
    protocol: str | None = "polymarket"
    market_id: str | None = "0xbaed1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab"


@dataclass
class _FakeSwapIntent:
    intent_type: str = "SWAP"
    protocol: str | None = None


# ---------------------------------------------------------------------------
# Stub registry to assert PREDICTION_REDEEM still routes through a parser
# (regression guard for the unchanged on-chain path).
# ---------------------------------------------------------------------------


class _RedeemParserSpy:
    """Records every extract_* invocation so the test can assert calls."""

    SUPPORTED_EXTRACTIONS = frozenset(
        {"redemption_amount", "payout", "market_id"}
    )

    def __init__(self) -> None:
        self.calls: list[str] = []

    def extract_redemption_amount(self, receipt: dict) -> int | None:  # noqa: ARG002
        self.calls.append("redemption_amount")
        return 5_000_000  # raw shares (legacy parser shape)

    def extract_payout(self, receipt: dict) -> int | None:  # noqa: ARG002
        self.calls.append("payout")
        return 5_000_000  # 5 USDC base units

    def extract_market_id(self, receipt: dict) -> str | None:  # noqa: ARG002
        self.calls.append("market_id")
        return "0xbaed-redeem"

    # parse_receipt is the cache key; stub it so the enricher's cache wrapper
    # can install cleanly without crashing.
    def parse_receipt(self, receipt: dict) -> dict:  # noqa: ARG002
        return {}


class _StubRegistry:
    def __init__(self, parser: object | None = None) -> None:
        self._parser = parser

    def get(self, protocol: str, chain: str):  # noqa: ARG002
        if self._parser is None:
            raise ValueError(f"no parser for {protocol}")
        return self._parser


# ---------------------------------------------------------------------------
# Constants used across the cases
# ---------------------------------------------------------------------------

MARKET_ID = "0xbaed1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab"

BUY_FILL = PredictionFill(
    filled_shares=Decimal("5.45"),
    requested_shares=Decimal("5.45"),
    avg_fill_price=Decimal("0.55"),
    order_id="clob-1",
    status="matched",
)

SELL_FILL = PredictionFill(
    filled_shares=Decimal("3.0"),
    requested_shares=Decimal("3.0"),
    avg_fill_price=Decimal("0.60"),
    order_id="clob-2",
    status="matched",
)

ZERO_FILL = PredictionFill(
    filled_shares=Decimal("0"),
    requested_shares=Decimal("5.0"),
    avg_fill_price=None,
    order_id="clob-3",
    status="unmatched",
)


def _bundle_meta(market_id: str | None = MARKET_ID) -> dict[str, Any]:
    """Mirror the polymarket adapter's ActionBundle.metadata shape."""
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
# (a) PREDICTION_BUY with prediction_fill + receipts=[] -> off-chain branch
# ===========================================================================


class TestPredictionBuyOffchain:
    def test_buy_off_chain_extracts_outcome_tokens_cost_basis_market_id(self):
        result = _FakeExecResult(
            transaction_results=[],  # CLOB orders -> no on-chain receipts
            prediction_fill=BUY_FILL,
        )
        intent = _FakePredictionIntent(
            intent_type="PREDICTION_BUY", market_id=MARKET_ID
        )
        context = _FakeContext(chain="polygon", protocol="polymarket")

        # Use the real registry — it resolves to the PolymarketReceiptParser
        # automatically. With receipts=[] the on-chain pass skips, leaving
        # only the off-chain branch populating spec fields. This mirrors the
        # production flow exactly.
        enricher = ResultEnricher()
        enriched = enricher.enrich(
            result, intent, context, bundle_metadata=_bundle_meta()
        )

        assert enriched.extracted_data["outcome_tokens_received"] == Decimal("5.45")
        # cost_basis = 5.45 * 0.55 = 2.9975 (Decimal arithmetic, no float drift)
        assert enriched.extracted_data["cost_basis"] == Decimal("2.9975")
        assert enriched.extracted_data["market_id"] == MARKET_ID
        # Sell-side keys should NOT appear on a BUY enrichment.
        assert "outcome_tokens_sold" not in enriched.extracted_data
        assert "proceeds" not in enriched.extracted_data
        # No spurious warnings on a clean fill.
        assert enriched.extraction_warnings == []


# ===========================================================================
# (b) PREDICTION_SELL with prediction_fill -> sold / proceeds keys
# ===========================================================================


class TestPredictionSellOffchain:
    def test_sell_off_chain_uses_sold_and_proceeds_keys(self):
        result = _FakeExecResult(
            transaction_results=[],
            prediction_fill=SELL_FILL,
        )
        intent = _FakePredictionIntent(
            intent_type="PREDICTION_SELL", market_id=MARKET_ID
        )
        context = _FakeContext(chain="polygon", protocol="polymarket")
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser=None))

        enriched = enricher.enrich(
            result, intent, context, bundle_metadata=_bundle_meta()
        )

        assert enriched.extracted_data["outcome_tokens_sold"] == Decimal("3.0")
        # proceeds = 3.0 * 0.60 = 1.80
        assert enriched.extracted_data["proceeds"] == Decimal("1.80")
        assert enriched.extracted_data["market_id"] == MARKET_ID
        # Buy-side keys must not appear on SELL enrichment.
        assert "outcome_tokens_received" not in enriched.extracted_data
        assert "cost_basis" not in enriched.extracted_data


# ===========================================================================
# (c) PREDICTION_BUY with prediction_fill=None -> warning, no spurious data
# ===========================================================================


class TestPredictionMissingFill:
    def test_buy_missing_prediction_fill_emits_warning(self):
        result = _FakeExecResult(
            transaction_results=[],
            prediction_fill=None,
        )
        intent = _FakePredictionIntent(
            intent_type="PREDICTION_BUY", market_id=MARKET_ID
        )
        context = _FakeContext(chain="polygon", protocol="polymarket")
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser=None))

        enriched = enricher.enrich(
            result, intent, context, bundle_metadata=_bundle_meta()
        )

        # No crash — warning surfaced through extraction_warnings.
        assert any(
            "prediction_fill" in w and "may have been rejected" in w
            for w in enriched.extraction_warnings
        )
        # No spurious shares / cost_basis fields fabricated from thin air.
        assert "outcome_tokens_received" not in enriched.extracted_data
        assert "cost_basis" not in enriched.extracted_data
        # market_id still attached so the strategy can identify the market.
        assert enriched.extracted_data["market_id"] == MARKET_ID

    # (d) PREDICTION_BUY with filled_shares == 0 (rejected / resting GTC)
    def test_buy_zero_fill_emits_warning_and_no_extraction(self):
        result = _FakeExecResult(
            transaction_results=[],
            prediction_fill=ZERO_FILL,
        )
        intent = _FakePredictionIntent(
            intent_type="PREDICTION_BUY", market_id=MARKET_ID
        )
        context = _FakeContext(chain="polygon", protocol="polymarket")
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser=None))

        enriched = enricher.enrich(
            result, intent, context, bundle_metadata=_bundle_meta()
        )

        assert any(
            "filled_shares=0" in w for w in enriched.extraction_warnings
        )
        assert "outcome_tokens_received" not in enriched.extracted_data
        assert "cost_basis" not in enriched.extracted_data
        # market_id still attached.
        assert enriched.extracted_data["market_id"] == MARKET_ID


# ===========================================================================
# (e) PREDICTION_REDEEM keeps the on-chain receipt path (regression guard)
# ===========================================================================


class TestPredictionRedeemUnchanged:
    def test_redeem_invokes_onchain_parser_extract_methods(self):
        receipt = _FakeReceipt(status=1, logs=[])
        result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)],
            # prediction_fill stays None for redemption — it's an on-chain merge.
            prediction_fill=None,
        )

        @dataclass
        class _RedeemIntent:
            intent_type: str = "PREDICTION_REDEEM"
            protocol: str | None = "polymarket"
            market_id: str | None = MARKET_ID

        intent = _RedeemIntent()
        context = _FakeContext(chain="polygon", protocol="polymarket")

        spy = _RedeemParserSpy()
        # live_mode=False so the legacy raw-int return values do not raise.
        enricher = ResultEnricher(
            parser_registry=_StubRegistry(parser=spy),
            live_mode=False,
        )
        enriched = enricher.enrich(result, intent, context)

        # The parser's extract_* methods must have been invoked — proves the
        # on-chain CTF receipt path still runs unchanged for redemptions.
        assert "redemption_amount" in spy.calls
        assert "payout" in spy.calls
        # Off-chain prediction extraction must NOT be triggered for REDEEM.
        # The PredictionFill was None, so if the off-chain branch had run we
        # would see a "prediction_fill" warning — assert we do not.
        assert not any("prediction_fill" in w for w in enriched.extraction_warnings)


# ===========================================================================
# (f) Non-PREDICTION intent (SWAP) with no receipts -> existing skip preserved
# ===========================================================================


class TestNonPredictionUnchanged:
    def test_swap_with_no_receipts_skips_without_offchain_branch(self):
        result = _FakeExecResult(transaction_results=[], prediction_fill=None)
        intent = _FakeSwapIntent(intent_type="SWAP", protocol="uniswap_v3")
        context = _FakeContext(chain="arbitrum", protocol="uniswap_v3")

        # Registry returns no parser to mirror "protocol resolved but parser
        # absent" — the enricher must NOT attempt off-chain prediction
        # extraction for a SWAP intent regardless.
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser=None))
        enriched = enricher.enrich(result, intent, context)

        # No prediction-shaped extraction fields appear on a non-prediction
        # intent. The "Parser not found" warning is the legitimate signal,
        # but no spurious cost_basis / outcome_tokens_received keys leak in.
        assert "outcome_tokens_received" not in enriched.extracted_data
        assert "outcome_tokens_sold" not in enriched.extracted_data
        assert "cost_basis" not in enriched.extracted_data
        assert "proceeds" not in enriched.extracted_data
        # Pre-existing behavior: no prediction-fill warnings on a SWAP path.
        assert not any("prediction_fill" in w for w in enriched.extraction_warnings)


# ===========================================================================
# (g) PREDICTION_BUY with BOTH prediction_fill AND on-chain receipts:
#     off-chain values take precedence; on-chain pass does not overwrite.
# ===========================================================================


class _PolymarketBuyParserSpy:
    """A parser that *would* fabricate different values if invoked.

    We use it to prove the on-chain pass does not overwrite the off-chain
    cost_basis / outcome_tokens_received fields when both data sources
    are present.
    """

    SUPPORTED_EXTRACTIONS = frozenset(
        {"outcome_tokens_received", "cost_basis", "market_id"}
    )

    def __init__(self) -> None:
        self.calls: list[str] = []

    def extract_outcome_tokens_received(self, receipt: dict) -> Decimal:  # noqa: ARG002
        self.calls.append("outcome_tokens_received")
        return Decimal("99.99")  # deliberately wrong on-chain value

    def extract_cost_basis(self, receipt: dict) -> Decimal:  # noqa: ARG002
        self.calls.append("cost_basis")
        return Decimal("999.99")

    def extract_market_id(self, receipt: dict) -> str:  # noqa: ARG002
        self.calls.append("market_id")
        return "0x-different-market"

    def parse_receipt(self, receipt: dict) -> dict:  # noqa: ARG002
        return {}


class TestOffchainTakesPrecedenceOverOnchain:
    def test_offchain_wins_when_both_available(self):
        receipt = _FakeReceipt(status=1, logs=[])
        result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)],
            prediction_fill=BUY_FILL,
        )
        intent = _FakePredictionIntent(
            intent_type="PREDICTION_BUY", market_id=MARKET_ID
        )
        context = _FakeContext(chain="polygon", protocol="polymarket")

        spy = _PolymarketBuyParserSpy()
        enricher = ResultEnricher(
            parser_registry=_StubRegistry(parser=spy),
            live_mode=False,
        )

        enriched = enricher.enrich(
            result, intent, context, bundle_metadata=_bundle_meta()
        )

        # Off-chain values from prediction_fill won — the on-chain spy's
        # bogus values must NOT appear.
        assert enriched.extracted_data["outcome_tokens_received"] == Decimal("5.45")
        assert enriched.extracted_data["cost_basis"] == Decimal("2.9975")
        assert enriched.extracted_data["market_id"] == MARKET_ID

        # Confirm the precedence rule: the spy's extract_* methods for the
        # off-chain-populated fields were NOT invoked, since the on-chain
        # spec was filtered to skip them.
        assert "outcome_tokens_received" not in spy.calls
        assert "cost_basis" not in spy.calls
        assert "market_id" not in spy.calls


# ===========================================================================
# (h) PREDICTION_BUY where bundle_metadata lacks market_id but intent has it
# ===========================================================================


class TestMarketIdFallback:
    def test_falls_back_to_intent_market_id_when_metadata_missing(self):
        result = _FakeExecResult(
            transaction_results=[],
            prediction_fill=BUY_FILL,
        )
        intent = _FakePredictionIntent(
            intent_type="PREDICTION_BUY", market_id=MARKET_ID
        )
        context = _FakeContext(chain="polygon", protocol="polymarket")
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser=None))

        # bundle_metadata explicitly omits market_id.
        enriched = enricher.enrich(
            result,
            intent,
            context,
            bundle_metadata=_bundle_meta(market_id=None),
        )

        assert enriched.extracted_data["market_id"] == MARKET_ID
        # The fallback path should not emit a "missing market_id" warning.
        assert not any("market_id" in w for w in enriched.extraction_warnings)

    def test_warns_when_market_id_missing_from_both_sources(self):
        result = _FakeExecResult(
            transaction_results=[],
            prediction_fill=BUY_FILL,
        )
        intent = _FakePredictionIntent(
            intent_type="PREDICTION_BUY", market_id=None
        )
        context = _FakeContext(chain="polygon", protocol="polymarket")
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser=None))

        enriched = enricher.enrich(
            result,
            intent,
            context,
            bundle_metadata=_bundle_meta(market_id=None),
        )

        # Fill data still extracted; market_id absent + warned.
        assert enriched.extracted_data["outcome_tokens_received"] == Decimal("5.45")
        assert enriched.extracted_data["cost_basis"] == Decimal("2.9975")
        assert "market_id" not in enriched.extracted_data
        assert any(
            "market_id" in w for w in enriched.extraction_warnings
        ), f"Expected market_id warning, got: {enriched.extraction_warnings}"
