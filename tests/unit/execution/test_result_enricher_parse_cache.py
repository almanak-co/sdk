"""Tests for ResultEnricher parse_receipt cache (VIB-1419).

Verifies that the enricher installs a temporary parse_receipt cache on
the parser so that repeated extract_* calls don't redundantly re-parse
the same receipt.
"""

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch

import pytest

from almanak.framework.execution.result_enricher import ResultEnricher


# ---------------------------------------------------------------------------
# Minimal stubs
# ---------------------------------------------------------------------------


@dataclass
class _FakeReceipt:
    tx_hash: str = "0xabc123"
    logs: list = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx_hash": self.tx_hash,
            "logs": self.logs,
            "status": 1,
        }


@dataclass
class _FakeTxResult:
    success: bool = True
    receipt: _FakeReceipt | None = None


@dataclass
class _FakeExecResult:
    success: bool = True
    transaction_results: list = field(default_factory=list)
    position_id: int | None = None
    swap_amounts: Any = None
    lp_close_data: Any = None
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)


@dataclass
class _FakeContext:
    chain: str = "arbitrum"
    protocol: str | None = None


@dataclass
class _FakeIntent:
    intent_type: str = "PERP_OPEN"
    protocol: str = "gmx_v2"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestParseReceiptCache:
    """Verify parse_receipt is called at most once per receipt during enrichment."""

    def test_parse_receipt_called_once_for_perp_open(self) -> None:
        """PERP_OPEN extracts 5 fields; parse_receipt should be called once, not 5x."""
        parse_call_count = 0
        fake_parse_result = type("ParseResult", (), {
            "position_increases": [],
            "position_decreases": [],
            "swaps": [],
            "orders": [],
        })()

        class FakeParser:
            """Parser that counts parse_receipt calls."""

            SUPPORTED_EXTRACTIONS = frozenset({
                "position_id", "size_delta", "collateral", "entry_price", "leverage",
            })

            def parse_receipt(self, receipt: dict) -> Any:
                nonlocal parse_call_count
                parse_call_count += 1
                return fake_parse_result

            def extract_position_id(self, receipt: dict) -> str | None:
                self.parse_receipt(receipt)
                return "pos_123"

            def extract_size_delta(self, receipt: dict) -> Any:
                self.parse_receipt(receipt)
                return 1000

            def extract_collateral(self, receipt: dict) -> Any:
                self.parse_receipt(receipt)
                return 500

            def extract_entry_price(self, receipt: dict) -> Any:
                self.parse_receipt(receipt)
                return 2000

            def extract_leverage(self, receipt: dict) -> Any:
                self.parse_receipt(receipt)
                return 2



        parser = FakeParser()

        enricher = ResultEnricher()

        receipt = _FakeReceipt(tx_hash="0xtest123")
        tx_result = _FakeTxResult(receipt=receipt)
        exec_result = _FakeExecResult(transaction_results=[tx_result])
        intent = _FakeIntent()
        context = _FakeContext()

        # Mock registry.get() to return our fake parser
        with patch.object(enricher.parser_registry, "get", return_value=parser):
            enricher.enrich(exec_result, intent, context)

        # Without cache: 5 fields * 1 receipt = 5 parse_receipt calls
        # With cache: 1 parse_receipt call (cached for subsequent fields)
        assert parse_call_count == 1, (
            f"parse_receipt called {parse_call_count}x, expected 1 "
            f"(cache should prevent redundant parsing)"
        )

    def test_parse_receipt_called_once_per_distinct_receipt(self) -> None:
        """With 2 receipts and 1 extract field, parse_receipt should be called 2x (once per receipt)."""
        parse_call_count = 0

        class FakeParser:
            SUPPORTED_EXTRACTIONS = frozenset({"swap_amounts"})

            def parse_receipt(self, receipt: dict) -> Any:
                nonlocal parse_call_count
                parse_call_count += 1
                return None

            def extract_swap_amounts(self, receipt: dict) -> Any:
                self.parse_receipt(receipt)
                return None



        parser = FakeParser()
        enricher = ResultEnricher()

        receipt1 = _FakeReceipt(tx_hash="0xtx1")
        receipt2 = _FakeReceipt(tx_hash="0xtx2")
        exec_result = _FakeExecResult(
            transaction_results=[
                _FakeTxResult(receipt=receipt1),
                _FakeTxResult(receipt=receipt2),
            ]
        )
        intent = _FakeIntent(intent_type="SWAP", protocol="uniswap_v3")
        context = _FakeContext()

        with patch.object(enricher.parser_registry, "get", return_value=parser):
            enricher.enrich(exec_result, intent, context)

        # 1 field * 2 receipts = 2 calls (but each cached independently)
        assert parse_call_count == 2

    def test_cache_removed_after_enrichment(self) -> None:
        """The parse_receipt cache wrapper should be removed after enrich() completes."""
        class FakeParser:
            SUPPORTED_EXTRACTIONS = frozenset({"swap_amounts"})

            def parse_receipt(self, receipt: dict) -> Any:
                return None

            def extract_swap_amounts(self, receipt: dict) -> Any:
                return None



        parser = FakeParser()
        enricher = ResultEnricher()

        receipt = _FakeReceipt()
        exec_result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)]
        )
        intent = _FakeIntent(intent_type="SWAP", protocol="uniswap_v3")
        context = _FakeContext()

        with patch.object(enricher.parser_registry, "get", return_value=parser):
            enricher.enrich(exec_result, intent, context)

        # Cache should be removed - original method restored
        assert not getattr(parser.parse_receipt, "_is_cached_wrapper", False)

    def test_cache_removed_even_on_exception(self) -> None:
        """Cache should be cleaned up even if extraction raises."""
        class FakeParser:
            SUPPORTED_EXTRACTIONS = frozenset({"swap_amounts"})

            def parse_receipt(self, receipt: dict) -> Any:
                return None

            def extract_swap_amounts(self, receipt: dict) -> Any:
                raise RuntimeError("Intentional test error")



        parser = FakeParser()
        enricher = ResultEnricher()

        receipt = _FakeReceipt()
        exec_result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)]
        )
        intent = _FakeIntent(intent_type="SWAP", protocol="uniswap_v3")
        context = _FakeContext()

        # enrich() should not raise (errors are caught)
        with patch.object(enricher.parser_registry, "get", return_value=parser):
            enricher.enrich(exec_result, intent, context)

        # Cache should still be removed
        assert not getattr(parser.parse_receipt, "_is_cached_wrapper", False)

    def test_parser_without_parse_receipt_skipped(self) -> None:
        """Parsers without parse_receipt method should not cause errors."""
        class MinimalParser:
            SUPPORTED_EXTRACTIONS = frozenset({"swap_amounts"})

            def extract_swap_amounts(self, receipt: dict) -> Any:
                return None



        parser = MinimalParser()
        enricher = ResultEnricher()

        receipt = _FakeReceipt()
        exec_result = _FakeExecResult(
            transaction_results=[_FakeTxResult(receipt=receipt)]
        )
        intent = _FakeIntent(intent_type="SWAP", protocol="some_protocol")
        context = _FakeContext()

        # Should not raise
        with patch.object(enricher.parser_registry, "get", return_value=parser):
            enricher.enrich(exec_result, intent, context)

        # Verify parser still works and was not modified
        assert not hasattr(parser, "_is_cached_wrapper")


class TestInstallRemoveCache:
    """Unit tests for the cache install/remove helpers."""

    def test_install_wraps_parse_receipt(self) -> None:
        call_count = 0

        class P:
            def parse_receipt(self, receipt: dict) -> str:
                nonlocal call_count
                call_count += 1
                return "parsed"

        parser = P()
        ResultEnricher._install_parse_cache(parser)
        assert getattr(parser.parse_receipt, "_is_cached_wrapper", False)

        # Call twice with same receipt — original should be called once
        result1 = parser.parse_receipt({"transactionHash": "0x1"})
        result2 = parser.parse_receipt({"transactionHash": "0x1"})
        assert result1 == "parsed"
        assert result2 == "parsed"
        assert call_count == 1

    def test_remove_restores_original(self) -> None:
        class P:
            def parse_receipt(self, receipt: dict) -> str:
                return "original"

        parser = P()
        original = parser.parse_receipt
        ResultEnricher._install_parse_cache(parser)
        assert parser.parse_receipt is not original

        ResultEnricher._remove_parse_cache(parser)
        # After removal, the method should be the original (bound method — same func)
        assert not getattr(parser.parse_receipt, "_is_cached_wrapper", False)
        assert parser.parse_receipt.__func__ is original.__func__

    def test_double_install_is_noop(self) -> None:
        """Installing cache twice should not double-wrap."""
        class P:
            def parse_receipt(self, receipt: dict) -> str:
                return "parsed"

        parser = P()
        ResultEnricher._install_parse_cache(parser)
        wrapper1 = parser.parse_receipt
        ResultEnricher._install_parse_cache(parser)
        wrapper2 = parser.parse_receipt
        assert wrapper1 is wrapper2  # Same wrapper, not double-wrapped

    def test_cache_keys_by_tx_hash(self) -> None:
        """Different tx hashes should get different cache entries."""
        call_count = 0

        class P:
            def parse_receipt(self, receipt: dict) -> str:
                nonlocal call_count
                call_count += 1
                return f"parsed_{call_count}"

        parser = P()
        ResultEnricher._install_parse_cache(parser)

        r1 = parser.parse_receipt({"transactionHash": "0x1"})
        r2 = parser.parse_receipt({"transactionHash": "0x2"})
        r3 = parser.parse_receipt({"transactionHash": "0x1"})  # cached

        assert r1 == "parsed_1"
        assert r2 == "parsed_2"
        assert r3 == "parsed_1"  # Cached result
        assert call_count == 2  # Only 2 actual calls
