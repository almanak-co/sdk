"""VIB-3203 Phase B — realized slippage_bps for the 4 parsers Phase A left behind.

Phase A (PR #1601) shipped ``SwapAmounts.expected_out_decimal`` +
``ActionBundle.metadata["expected_output_human"]`` + the ResultEnricher
signature-introspection forwarding, wired for 5 parsers (uniswap_v3, v4,
aerodrome, pendle, sushiswap_v3).

Phase B wires the remaining 4 parsers — curve, pancakeswap_v3, traderjoe_v2,
gmx_v2 — plus the TraderJoe V2 compile-time quote so its compile path can
persist ``expected_output_human``.

These tests assert the parser signature accepts the kwarg and that realized
slippage_bps is computed correctly. They use hand-crafted receipts rather
than real fixtures because the goal is to verify the NEW kwarg path, not
rehash the existing parse_receipt machinery.
"""

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.execution.extracted_data import SwapAmounts


class TestCurveExtractSwapAmountsAcceptsKwarg:
    """Curve parser must accept the VIB-3203 Phase B ``expected_out`` kwarg."""

    def test_signature_declares_expected_out(self) -> None:
        """ResultEnricher._build_extract_kwargs uses signature introspection —
        the kwarg MUST appear by name for the forwarding path to engage."""
        import inspect

        from almanak.framework.connectors.curve.receipt_parser import CurveReceiptParser

        sig = inspect.signature(CurveReceiptParser.extract_swap_amounts)
        assert "expected_out" in sig.parameters
        param = sig.parameters["expected_out"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY
        assert param.default is None

    def test_computes_slippage_when_expected_out_supplied(self) -> None:
        """A realized amount 1% below the quote should produce ~100 bps."""
        from almanak.framework.connectors.curve.receipt_parser import CurveReceiptParser

        parser = CurveReceiptParser(chain="ethereum")

        # Fake a parse_receipt result with a single swap event — we stub the
        # Curve-specific parse + decimals plumbing so the test exercises
        # only the slippage math.
        from almanak.framework.connectors.curve.receipt_parser import ParseResult, SwapEventData

        swap = SwapEventData(
            pool_address="0x" + "0" * 40,
            buyer="0x" + "1" * 40,
            sold_id=0,
            bought_id=1,
            tokens_sold=1_000_000,  # 1.0 USDC (6 decimals)
            tokens_bought=980_000_000_000_000_000,  # 0.98 WETH (18 decimals) — realized
        )
        parse_result = ParseResult(success=True, swap_events=[swap])

        with (
            patch.object(parser, "parse_receipt", return_value=parse_result),
            patch.object(
                parser,
                "_find_swap_token_addresses",
                return_value=("0x" + "a" * 40, "0x" + "b" * 40),
            ),
            patch.object(parser, "_resolve_decimals", side_effect=[6, 18]),
        ):
            expected_out = Decimal("0.99")  # quoted 0.99 WETH
            result = parser.extract_swap_amounts(
                {"logs": [], "from": "0x" + "2" * 40},
                expected_out=expected_out,
            )

        assert isinstance(result, SwapAmounts)
        assert result.expected_out_decimal == expected_out
        # (0.99 - 0.98) / 0.99 * 10000 = 101.01... -> int() truncates to 101
        assert result.slippage_bps == 101

    def test_negative_bps_when_realized_beats_quote(self) -> None:
        """Realized > expected should produce a NEGATIVE slippage_bps.

        The strategy_runner slippage breaker compares
        ``actual_slippage > max_slippage`` so negatives never trigger a halt
        (correct). Still, a future refactor that wraps the bps in ``abs()``
        would silently flip favourable slippage into a halt — this test
        locks in the signed-negative invariant for Phase B parsers.
        """
        from almanak.framework.connectors.curve.receipt_parser import (
            CurveReceiptParser,
            ParseResult,
            SwapEventData,
        )

        parser = CurveReceiptParser(chain="ethereum")
        swap = SwapEventData(
            pool_address="0x" + "0" * 40,
            buyer="0x" + "1" * 40,
            sold_id=0,
            bought_id=1,
            tokens_sold=1_000_000,  # 1.0 USDC
            tokens_bought=1_010_000_000_000_000_000,  # 1.01 WETH — better than quote
        )
        parse_result = ParseResult(success=True, swap_events=[swap])

        with (
            patch.object(parser, "parse_receipt", return_value=parse_result),
            patch.object(
                parser,
                "_find_swap_token_addresses",
                return_value=("0x" + "a" * 40, "0x" + "b" * 40),
            ),
            patch.object(parser, "_resolve_decimals", side_effect=[6, 18]),
        ):
            result = parser.extract_swap_amounts(
                {"logs": [], "from": "0x" + "2" * 40},
                expected_out=Decimal("1.0"),  # quote was 1.0 WETH, realized 1.01
            )

        assert isinstance(result, SwapAmounts)
        # (1.0 - 1.01) / 1.0 * 10000 = -100
        assert result.slippage_bps is not None
        assert result.slippage_bps < 0
        assert result.slippage_bps == -100

    def test_slippage_none_without_expected_out(self) -> None:
        """Legacy call path (no kwarg) must keep slippage_bps at None."""
        from almanak.framework.connectors.curve.receipt_parser import (
            CurveReceiptParser,
            ParseResult,
            SwapEventData,
        )

        parser = CurveReceiptParser(chain="ethereum")
        swap = SwapEventData(
            pool_address="0x" + "0" * 40,
            buyer="0x" + "1" * 40,
            sold_id=0,
            bought_id=1,
            tokens_sold=1_000_000,
            tokens_bought=980_000_000_000_000_000,
        )
        parse_result = ParseResult(success=True, swap_events=[swap])

        with (
            patch.object(parser, "parse_receipt", return_value=parse_result),
            patch.object(
                parser,
                "_find_swap_token_addresses",
                return_value=("0x" + "a" * 40, "0x" + "b" * 40),
            ),
            patch.object(parser, "_resolve_decimals", side_effect=[6, 18]),
        ):
            result = parser.extract_swap_amounts({"logs": [], "from": "0x" + "2" * 40})

        assert isinstance(result, SwapAmounts)
        assert result.slippage_bps is None
        assert result.expected_out_decimal is None


class TestPancakeSwapV3ExtractSwapAmountsAcceptsKwarg:
    """PancakeSwap V3 parser parity with the Phase A uniswap_v3 template."""

    def test_signature_declares_expected_out(self) -> None:
        import inspect

        from almanak.framework.connectors.pancakeswap_v3.receipt_parser import PancakeSwapV3ReceiptParser

        sig = inspect.signature(PancakeSwapV3ReceiptParser.extract_swap_amounts)
        assert "expected_out" in sig.parameters
        param = sig.parameters["expected_out"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY


class TestTraderJoeV2ExtractSwapAmountsAcceptsKwarg:
    def test_signature_declares_expected_out(self) -> None:
        import inspect

        from almanak.framework.connectors.traderjoe_v2.receipt_parser import TraderJoeV2ReceiptParser

        sig = inspect.signature(TraderJoeV2ReceiptParser.extract_swap_amounts)
        assert "expected_out" in sig.parameters
        param = sig.parameters["expected_out"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY

    def test_slippage_suppressed_when_chain_unset(self) -> None:
        """If the parser was constructed without a chain, decimal resolution
        falls back to 18 — so realized slippage is computed against
        potentially-mis-scaled amounts. Suppress slippage_bps in this case."""
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import (
            ParsedSwapResult,
            ParseResult,
            TraderJoeV2ReceiptParser,
        )

        parser = TraderJoeV2ReceiptParser(chain=None)  # chain unset
        sr = ParsedSwapResult(
            success=True,
            amount_in=1_000_000,
            amount_out=980_000_000_000_000_000,
            token_in="0x" + "a" * 40,
            token_out="0x" + "b" * 40,
        )
        parse_result = ParseResult(
            success=True,
            transaction_hash="0xtest",
            block_number=1,
            gas_used=100_000,
            swap_result=sr,
        )

        with patch.object(parser, "parse_receipt", return_value=parse_result):
            result = parser.extract_swap_amounts(
                {"logs": []},
                expected_out=Decimal("0.99"),
            )

        assert isinstance(result, SwapAmounts)
        # amounts continue to surface for legacy paths (existing behaviour)
        assert result.amount_out > 0
        # but slippage MUST stay None because decimals couldn't be confirmed
        assert result.slippage_bps is None


class TestGmxV2ExtractSwapAmountsAcceptsKwargButIgnoresIt:
    """GMX V2 accepts the kwarg for interface parity but does NOT compute
    slippage — perp-order semantics differ from spot-swap slippage and are
    out of scope for VIB-3203."""

    def test_signature_declares_expected_out(self) -> None:
        import inspect

        from almanak.framework.connectors.gmx_v2.receipt_parser import GMXv2ReceiptParser

        sig = inspect.signature(GMXv2ReceiptParser.extract_swap_amounts)
        assert "expected_out" in sig.parameters
        param = sig.parameters["expected_out"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY

    def test_kwarg_is_accepted_but_ignored(self) -> None:
        """Passing expected_out to GMX V2 must not raise and must not alter
        slippage_bps — which for perp orders is always ``None``."""
        from almanak.framework.connectors.gmx_v2.receipt_parser import GMXv2ReceiptParser

        parser = GMXv2ReceiptParser(chain="arbitrum")
        # Empty receipt — no order events, returns None regardless of kwarg.
        assert parser.extract_swap_amounts({"logs": []}) is None
        assert parser.extract_swap_amounts({"logs": []}, expected_out=Decimal("100")) is None


class TestEnricherForwardsKwargToPhaseBParsers:
    """End-to-end check that ResultEnricher routes expected_output_human to
    the 4 Phase B parsers via the signature-introspection path #1601
    established. The per-parser signature check lives in the dedicated
    ``test_signature_declares_expected_out`` cases above; this class only
    asserts the enricher's static gate function."""

    def test_build_extract_kwargs_threads_expected_output_human(self) -> None:
        """``ResultEnricher._build_extract_kwargs`` is the gate that decides
        whether to forward ``expected_out``. When the bundle metadata carries
        ``expected_output_human``, the gate must produce a kwargs dict the
        parser can consume."""
        from almanak.framework.execution.result_enricher import ResultEnricher

        kwargs = ResultEnricher._build_extract_kwargs(
            field="swap_amounts",
            bundle_metadata={"expected_output_human": "100.5"},
        )
        assert kwargs == {"expected_out": Decimal("100.5")}

    def test_enricher_returns_empty_kwargs_without_metadata(self) -> None:
        from almanak.framework.execution.result_enricher import ResultEnricher

        assert ResultEnricher._build_extract_kwargs("swap_amounts", None) == {}
        assert ResultEnricher._build_extract_kwargs("swap_amounts", {}) == {}


class TestPhaseBLocalGuardsExpectedOut:
    """VIB-3203 Phase B parsers must mirror the Phase A guard pattern
    (``expected_out is not None and expected_out > 0 and amount_out_decimal > 0``)
    so a direct caller passing a bogus quote degrades to ``slippage_bps=None``
    instead of dropping the entire SwapAmounts via the outer ``except``.
    """

    @pytest.mark.parametrize("bad_quote", [Decimal("0"), Decimal("-1.5")])
    def test_curve_returns_swap_amounts_with_null_slippage_for_bad_quote(self, bad_quote: Decimal) -> None:
        from almanak.framework.connectors.curve.receipt_parser import (
            CurveReceiptParser,
            ParseResult,
            SwapEventData,
        )

        parser = CurveReceiptParser(chain="ethereum")
        swap = SwapEventData(
            pool_address="0x" + "0" * 40,
            buyer="0x" + "1" * 40,
            sold_id=0,
            bought_id=1,
            tokens_sold=1_000_000,
            tokens_bought=980_000_000_000_000_000,
        )
        with (
            patch.object(parser, "parse_receipt", return_value=ParseResult(success=True, swap_events=[swap])),
            patch.object(
                parser,
                "_find_swap_token_addresses",
                return_value=("0x" + "a" * 40, "0x" + "b" * 40),
            ),
            patch.object(parser, "_resolve_decimals", side_effect=[6, 18]),
        ):
            result = parser.extract_swap_amounts(
                {"logs": [], "from": "0x" + "2" * 40},
                expected_out=bad_quote,
            )

        assert isinstance(result, SwapAmounts)
        assert result.slippage_bps is None  # bad quote -> slippage suppressed
        assert result.amount_out > 0  # SwapAmounts itself preserved
