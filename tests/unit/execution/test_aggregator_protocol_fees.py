"""VIB-3210 — Aggregator (Enso, LiFi) protocol-fee plumbing.

Aggregator parsers cannot reconstruct the integrator fee from on-chain
receipts: the fee is quote-time metadata. The fix:

1. Adapter captures ``quote.estimate.total_fee_usd`` (or equivalent) into
   ``ActionBundle.metadata["protocol_fee_usd"]`` at compile time.
2. ``ResultEnricher._build_extract_kwargs`` reads the metadata and threads
   it to ``extract_protocol_fees`` as the keyword-only ``protocol_fee_usd``.
3. The parser returns a measured ``ProtocolFees`` when the kwarg is
   present, and a typed ``unavailable_reason`` (per VIB-3495) when absent.

These tests pin the three layers:
* Kwargs builder threads ``protocol_fee_usd`` from metadata.
* LiFi parser returns measured ``ProtocolFees`` when threaded, typed
  unavailable otherwise.
* Enso parser falls back to typed unavailable (no adapter-side USD
  conversion yet).
* No parser ever returns raw ``None``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.connectors.enso.receipt_parser import EnsoReceiptParser
from almanak.framework.connectors.lifi.receipt_parser import LiFiReceiptParser
from almanak.framework.execution.extracted_data import ProtocolFees
from almanak.framework.execution.result_enricher import ResultEnricher


# ---------------------------------------------------------------------------
# Layer 1: _build_extract_kwargs threads protocol_fee_usd from metadata
# ---------------------------------------------------------------------------


class TestBuildExtractKwargsThreadsProtocolFeeUsd:
    def test_threads_decimal_from_string_metadata(self) -> None:
        kwargs = ResultEnricher._build_extract_kwargs(
            "protocol_fees",
            {"protocol_fee_usd": "1.23"},
        )
        assert kwargs["protocol_fee_usd"] == Decimal("1.23")

    def test_threads_alongside_existing_fee_tier(self) -> None:
        kwargs = ResultEnricher._build_extract_kwargs(
            "protocol_fees",
            {"selected_fee_tier": "30", "protocol_fee_usd": "0.42"},
        )
        assert kwargs["fee_tier_bps"] == 30
        assert kwargs["protocol_fee_usd"] == Decimal("0.42")

    def test_no_kwarg_when_metadata_missing(self) -> None:
        kwargs = ResultEnricher._build_extract_kwargs("protocol_fees", {})
        assert "protocol_fee_usd" not in kwargs

    def test_no_kwarg_when_metadata_empty_string(self) -> None:
        kwargs = ResultEnricher._build_extract_kwargs(
            "protocol_fees",
            {"protocol_fee_usd": ""},
        )
        assert "protocol_fee_usd" not in kwargs

    def test_negative_fee_is_threaded_through_for_parser_fail_fast(self) -> None:
        """End-to-end fail-fast: the kwargs builder threads negative values
        through so the parser's ValueError can surface upstream sign
        corruption (CodeRabbit pushback on PR #2256). Silent drop here would
        mask the bug."""
        kwargs = ResultEnricher._build_extract_kwargs(
            "protocol_fees",
            {"protocol_fee_usd": "-1"},
        )
        assert kwargs["protocol_fee_usd"] == Decimal("-1")

    def test_garbage_input_is_rejected_silently(self) -> None:
        kwargs = ResultEnricher._build_extract_kwargs(
            "protocol_fees",
            {"protocol_fee_usd": "not-a-number"},
        )
        assert "protocol_fee_usd" not in kwargs


# ---------------------------------------------------------------------------
# Layer 2: LiFi parser — measured when threaded, typed unavailable otherwise
# ---------------------------------------------------------------------------


class TestLiFiExtractProtocolFees:
    def test_returns_measured_when_kwarg_present(self) -> None:
        parser = LiFiReceiptParser()
        out = parser.extract_protocol_fees({}, protocol_fee_usd=Decimal("1.50"))
        assert isinstance(out, ProtocolFees)
        assert out.total_usd == Decimal("1.50")
        assert out.swap_fee_usd == Decimal("1.50")
        assert out.unavailable_reason is None

    def test_returns_zero_when_quote_reports_zero(self) -> None:
        """LiFi's free tier returns total_fee_usd=0; we must record it as a
        measured zero, not as 'unavailable'."""
        parser = LiFiReceiptParser()
        out = parser.extract_protocol_fees({}, protocol_fee_usd=Decimal("0"))
        assert out.total_usd == Decimal("0")
        assert out.swap_fee_usd == Decimal("0")
        assert out.unavailable_reason is None

    def test_returns_typed_unavailable_when_kwarg_missing(self) -> None:
        parser = LiFiReceiptParser()
        out = parser.extract_protocol_fees({})
        assert isinstance(out, ProtocolFees)
        assert out.total_usd is None
        assert out.unavailable_reason == "lifi_integrator_fee_quote_metadata_unavailable"

    def test_never_returns_raw_none(self) -> None:
        """The legacy `return None` was the bug — VIB-3210 forbids it.

        Assertion checks concrete typed-unavailable shape (CodeRabbit
        pushback on PR #2256): the regression we're guarding is "parser
        returns None instead of typed ProtocolFees", so asserting on
        specific fields catches a wider class of regressions than a bare
        ``is not None``.
        """
        parser = LiFiReceiptParser()
        out = parser.extract_protocol_fees({}, protocol_fee_usd=None)
        assert isinstance(out, ProtocolFees)
        assert out.total_usd is None
        assert out.swap_fee_usd is None
        assert out.unavailable_reason == "lifi_integrator_fee_quote_metadata_unavailable"

    def test_negative_fee_fails_fast(self) -> None:
        """CodeRabbit pushback on PR #2256: a negative integrator fee means
        upstream sign corruption. Silently downgrading to "unavailable"
        would mask the bug. Raise ValueError instead."""
        parser = LiFiReceiptParser()
        with pytest.raises(ValueError, match="non-negative"):
            parser.extract_protocol_fees({}, protocol_fee_usd=Decimal("-0.01"))


# ---------------------------------------------------------------------------
# Layer 3: Enso parser — typed unavailable (no adapter USD conversion yet)
# ---------------------------------------------------------------------------


class TestEnsoExtractProtocolFees:
    def test_returns_typed_unavailable_when_kwarg_missing(self) -> None:
        parser = EnsoReceiptParser()
        out = parser.extract_protocol_fees({})
        assert isinstance(out, ProtocolFees)
        assert out.total_usd is None
        assert (
            out.unavailable_reason
            == "enso_integrator_fee_quote_metadata_unavailable"
        )

    def test_threaded_kwarg_path_works_for_future_adapter_wiring(self) -> None:
        """Drop-in path: when adapter-side USD conversion ships, the parser
        signature already accepts ``protocol_fee_usd`` and returns a
        measured ``ProtocolFees``. No additional parser changes needed."""
        parser = EnsoReceiptParser()
        out = parser.extract_protocol_fees({}, protocol_fee_usd=Decimal("0.05"))
        assert out.total_usd == Decimal("0.05")
        assert out.swap_fee_usd == Decimal("0.05")
        assert out.unavailable_reason is None

    def test_never_returns_raw_none(self) -> None:
        """Concrete shape assertion — see LiFi equivalent for the rationale."""
        parser = EnsoReceiptParser()
        out = parser.extract_protocol_fees({}, protocol_fee_usd=None)
        assert isinstance(out, ProtocolFees)
        assert out.total_usd is None
        assert out.swap_fee_usd is None
        assert out.unavailable_reason == "enso_integrator_fee_quote_metadata_unavailable"

    def test_negative_fee_fails_fast(self) -> None:
        """Same fail-fast contract as LiFi (CodeRabbit pushback on PR #2256)."""
        parser = EnsoReceiptParser()
        with pytest.raises(ValueError, match="non-negative"):
            parser.extract_protocol_fees({}, protocol_fee_usd=Decimal("-0.01"))
