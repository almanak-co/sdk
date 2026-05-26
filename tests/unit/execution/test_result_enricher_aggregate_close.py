"""VIB-4310 — ResultEnricher aggregates LP_CLOSE data across receipts.

Aerodrome Slipstream's close is a two-transaction sequence:
``decreaseLiquidity`` (receipt #1, emits ``DecreaseLiquidity``) → ``collect``
(receipt #2, emits ``Collect``). The Collect amounts are the truth on
transfer (principal + accrued fees); the DecreaseLiquidity amounts are
principal-only.

Before this fix the enricher returned on first ExtractOk, so a bundle
ordering receipt #1 before receipt #2 silently dropped accrued fees from
the registry payload. The fix:

1. Tag each ``LPCloseData`` with its provenance (``source="collect"`` /
   ``source="decrease_liquidity"``) at parser-time.
2. Have the enricher scan all receipts for fields in ``_AGGREGATE_FIELDS``
   and pick the preferred-source candidate.

These tests pin the contract at both layers:
* ``_select_preferred_aggregate`` — the picker.
* ``ResultEnricher._extract_field`` end-to-end via a fake parser.
* ``AerodromeSlipstreamReceiptParser.extract_lp_close_data`` source tagging.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from almanak.framework.execution.extract_result import ExtractOk
from almanak.framework.execution.extracted_data import LPCloseData
from almanak.framework.execution.result_enricher import (
    _AGGREGATE_FIELDS,
    ResultEnricher,
)


# ---------------------------------------------------------------------------
# Layer 1: _select_preferred_aggregate (the picker)
# ---------------------------------------------------------------------------


class TestSelectPreferredAggregate:
    def test_prefers_collect_over_decrease_liquidity(self) -> None:
        decrease = LPCloseData(
            amount0_collected=100,
            amount1_collected=200,
            source="decrease_liquidity",
        )
        collect = LPCloseData(
            amount0_collected=150,
            amount1_collected=260,
            source="collect",
        )
        # Order #1: decrease first, collect second.
        chosen = ResultEnricher._select_preferred_aggregate([decrease, collect], "collect")
        assert chosen is collect

        # Order #2: collect first, decrease second.
        chosen = ResultEnricher._select_preferred_aggregate([collect, decrease], "collect")
        assert chosen is collect

    def test_falls_back_to_first_when_no_preferred_match(self) -> None:
        decrease_a = LPCloseData(
            amount0_collected=100,
            amount1_collected=200,
            source="decrease_liquidity",
        )
        decrease_b = LPCloseData(
            amount0_collected=110,
            amount1_collected=210,
            source="decrease_liquidity",
        )
        chosen = ResultEnricher._select_preferred_aggregate(
            [decrease_a, decrease_b], "collect"
        )
        assert chosen is decrease_a

    def test_handles_untagged_candidates(self) -> None:
        untagged_a = LPCloseData(amount0_collected=10, amount1_collected=20, source=None)
        untagged_b = LPCloseData(amount0_collected=30, amount1_collected=40, source=None)
        chosen = ResultEnricher._select_preferred_aggregate(
            [untagged_a, untagged_b], "collect"
        )
        assert chosen is untagged_a

    def test_lp_close_data_registered_as_aggregate_field(self) -> None:
        """The map ``_AGGREGATE_FIELDS`` is the public contract — if anyone
        renames ``lp_close_data`` it must move atomically."""
        assert _AGGREGATE_FIELDS["lp_close_data"] == "collect"

    def test_backfills_liquidity_removed_from_decrease_sibling(self) -> None:
        """Codex P2 pushback on PR #2256: picking the Collect-sourced
        candidate wholesale dropped ``liquidity_removed`` because that field
        only appears on the DecreaseLiquidity event. The picker must
        backfill ``None`` fields from siblings so the LP_CLOSE ledger row
        records the burned liquidity."""
        decrease = LPCloseData(
            amount0_collected=100,
            amount1_collected=200,
            liquidity_removed=1_000_000,
            source="decrease_liquidity",
        )
        collect = LPCloseData(
            amount0_collected=150,
            amount1_collected=260,
            source="collect",
        )
        chosen = ResultEnricher._select_preferred_aggregate([decrease, collect], "collect")
        # Collect-sourced amounts win (truth on transfer)…
        assert chosen.amount0_collected == 150
        assert chosen.amount1_collected == 260
        assert chosen.source == "collect"
        # …but liquidity_removed comes from the DecreaseLiquidity sibling.
        assert chosen.liquidity_removed == 1_000_000

    def test_malformed_lp_close_data_rejected_and_scan_continues(self) -> None:
        """CodeRabbit pushback on PR #2256: ``_attach_to_result`` must reject
        a non-LPCloseData value for the ``lp_close_data`` field. Aggregate
        attach success short-circuits the receipt scan for the field, so a
        broken parser that emits a dict / None / bare int would silently
        win over a legitimate sibling candidate."""

        class _BadAggregateParser:
            SUPPORTED_EXTRACTIONS = ("lp_close_data",)
            _calls = 0

            def extract_lp_close_data_result(self, _receipt: dict[str, Any]) -> ExtractOk:
                self._calls += 1
                if self._calls == 1:
                    # Malformed (dict instead of LPCloseData) — must be rejected.
                    return ExtractOk(value={"amount0_collected": 1, "amount1_collected": 2})
                return ExtractOk(
                    value=LPCloseData(
                        amount0_collected=150,
                        amount1_collected=260,
                        source="collect",
                    )
                )

        @dataclass
        class _Result:
            extracted_data: dict = field(default_factory=dict)
            lp_close_data: Any = None
            amount0_collected: Any = None
            amount1_collected: Any = None
            fees0: Any = None
            fees1: Any = None
            protocol_fees: Any = None
            extraction_warnings: list = field(default_factory=list)

        result = _Result()
        parser = _BadAggregateParser()
        enricher = ResultEnricher(live_mode=False)
        enricher._extract_field(
            result=result,  # type: ignore[arg-type]
            parser=parser,
            receipts=[{}, {}],
            field="lp_close_data",
            intent_type="LP_CLOSE",
            protocol="aerodrome_slipstream",
        )

        # The legitimate Collect-sourced candidate must win, not the dict.
        assert result.lp_close_data is not None
        assert isinstance(result.lp_close_data, LPCloseData)
        assert result.lp_close_data.amount0_collected == 150
        assert result.lp_close_data.source == "collect"

    def test_backfill_does_not_overwrite_populated_fields(self) -> None:
        """The chosen candidate's populated fields are authoritative — the
        sibling never overrides a value the preferred source already
        produced."""
        decrease = LPCloseData(
            amount0_collected=999,
            amount1_collected=999,
            liquidity_removed=1_000_000,
            source="decrease_liquidity",
        )
        collect = LPCloseData(
            amount0_collected=150,
            amount1_collected=260,
            source="collect",
        )
        chosen = ResultEnricher._select_preferred_aggregate([decrease, collect], "collect")
        # Collect's amounts (150/260) win, NOT decrease's bogus 999/999.
        assert chosen.amount0_collected == 150
        assert chosen.amount1_collected == 260


# ---------------------------------------------------------------------------
# lp-close-may20.md — aggregate-layer fee derivation
# ---------------------------------------------------------------------------


class TestAggregatorDerivesFeesFromSiblings:
    """The aggregator is the only layer that has both the ``collect`` and
    ``decrease_liquidity`` sibling receipts visible at the same time. For
    UniswapV3 / PancakeSwap V3 / SushiSwap V3 split-tx LP_CLOSE bundles, the
    parser emits ``fees0/fees1 = None`` on each receipt (Empty ≠ Zero) and
    relies on the aggregator to derive true fees as
    ``collect.amount - decrease.amount`` (clamped at zero).

    See ``docs/internal/lp-close-may20.md`` §6.3 — without this, a guard-only
    parser fix would silently drop real mainnet fees from LP_CLOSE rows.
    """

    def test_derives_fees_from_collect_and_decrease_siblings(self) -> None:
        """Split-tx LP_CLOSE: parser-side ``collect`` candidate has the
        whole-transfer-as-fees attribution (no Burn in its receipt to
        subtract). Aggregator overrides ``fees = collect - decrease``
        using the principal recovered from the decrease sibling."""
        principal0, principal1 = 1_000_000, 500_000_000_000_000_000
        fees0, fees1 = 1_234, 9_876_000_000_000
        decrease = LPCloseData(
            amount0_collected=principal0,
            amount1_collected=principal1,
            fees0=None,
            fees1=None,
            liquidity_removed=9_876_543_210,
            source="decrease_liquidity",
        )
        # Parser's collect-only attribution: fees = collect_amount.
        collect = LPCloseData(
            amount0_collected=principal0 + fees0,
            amount1_collected=principal1 + fees1,
            fees0=principal0 + fees0,
            fees1=principal1 + fees1,
            liquidity_removed=None,
            source="collect",
        )

        chosen = ResultEnricher._select_preferred_aggregate([decrease, collect], "collect")
        assert chosen.source == "collect"
        # amount*_collected stays as transfer truth (principal + fees).
        assert chosen.amount0_collected == principal0 + fees0
        assert chosen.amount1_collected == principal1 + fees1
        # Aggregator OVERRIDES parser's collect-only attribution.
        assert chosen.fees0 == fees0
        assert chosen.fees1 == fees1
        # liquidity_removed backfilled from the decrease sibling.
        assert chosen.liquidity_removed == 9_876_543_210

    def test_zero_volume_anvil_yields_measured_zero_fees(self) -> None:
        """On a forked Anvil with zero swap volume, real LP fees are 0.
        collect.amount == decrease.amount → aggregator derives
        fees = max(0, 0) = 0, overriding parser's whole-transfer-as-fees.
        This is the lp-close-may20.md repro path — pre-fix the dashboard
        showed Earn +$4.26 (entire principal as fees)."""
        principal0, principal1 = 953_559_913_649_337, 2_244_836
        decrease = LPCloseData(
            amount0_collected=principal0,
            amount1_collected=principal1,
            fees0=None,
            fees1=None,
            liquidity_removed=950_218_044_797,
            source="decrease_liquidity",
        )
        # Parser's collect-only attribution before aggregator overrides:
        # fees = full collect_amount (matches principal since fees=0).
        collect = LPCloseData(
            amount0_collected=principal0,
            amount1_collected=principal1,
            fees0=principal0,
            fees1=principal1,
            liquidity_removed=None,
            source="collect",
        )
        chosen = ResultEnricher._select_preferred_aggregate([decrease, collect], "collect")
        # Aggregator overrides: fees = collect - decrease = 0.
        assert chosen.fees0 == 0
        assert chosen.fees1 == 0
        # Principal still recoverable from amount*_collected.
        assert chosen.amount0_collected == principal0
        assert chosen.amount1_collected == principal1

    def test_clamps_negative_diff_to_zero(self) -> None:
        """Pre-existing ``tokensOwed`` dust can make decrease.amount >
        collect.amount transiently. Aggregator clamps at 0 — never reports
        negative fees."""
        decrease = LPCloseData(
            amount0_collected=1_000_000,
            amount1_collected=500_000_000_000_000_000,
            fees0=None,
            fees1=None,
            source="decrease_liquidity",
        )
        # Parser tagged: fees = full collect (the buggy attribution the
        # aggregator is responsible for correcting).
        collect = LPCloseData(
            amount0_collected=999_999,  # one wei less than decrease
            amount1_collected=499_999_999_999_999_999,
            fees0=999_999,
            fees1=499_999_999_999_999_999,
            source="collect",
        )
        chosen = ResultEnricher._select_preferred_aggregate([decrease, collect], "collect")
        assert chosen.fees0 == 0
        assert chosen.fees1 == 0

    def test_multicall_single_receipt_fees_preserved_no_sibling(self) -> None:
        """When the close lands as a single multicall receipt with BOTH
        Burn and Collect events, the parser computes fees correctly
        (``collect - burn``) and emits one ``"collect"``-tagged
        candidate. No decrease_liquidity sibling exists — aggregator
        leaves the parser-computed fees alone."""
        collect = LPCloseData(
            amount0_collected=1_500_000,
            amount1_collected=600_000_000_000_000_000,
            fees0=42,  # parser computed collect - burn in one receipt
            fees1=84,
            liquidity_removed=42_424_242,
            source="collect",
        )
        chosen = ResultEnricher._select_preferred_aggregate([collect], "collect")
        assert chosen.fees0 == 42
        assert chosen.fees1 == 84
        assert chosen.liquidity_removed == 42_424_242

    def test_lp_collect_fees_no_sibling_preserves_collect_amount(self) -> None:
        """LP_COLLECT_FEES intent / fee-only harvest: only ONE collect
        candidate exists (no decrease was ever called in the same flow).
        Parser's ``fees = collect_amount`` attribution is CORRECT —
        the whole transfer IS fees in this case. Aggregator must NOT
        override it (no decrease sibling to derive from).

        Also covers the no-liquidity-but-owed-tokens LP_CLOSE scenario
        where the compiler skipped the decrease step because liquidity
        was already 0.
        """
        collect = LPCloseData(
            amount0_collected=12_345,
            amount1_collected=6_789_000_000_000,
            fees0=12_345,           # parser: whole transfer = fees
            fees1=6_789_000_000_000,
            source="collect",
        )
        chosen = ResultEnricher._select_preferred_aggregate([collect], "collect")
        # Parser's attribution preserved — no decrease sibling means no
        # override.
        assert chosen.fees0 == 12_345
        assert chosen.fees1 == 6_789_000_000_000


# ---------------------------------------------------------------------------
# Layer 2: ResultEnricher._extract_field end-to-end via a fake parser
# ---------------------------------------------------------------------------


@dataclass
class _FakeResult:
    extracted_data: dict = field(default_factory=dict)
    lp_close_data: Any = None
    amount0_collected: Any = None
    amount1_collected: Any = None
    fees0: Any = None
    fees1: Any = None
    protocol_fees: Any = None
    extraction_warnings: list = field(default_factory=list)


class _FakeTwoReceiptSlipstreamParser:
    """Mimics Aerodrome's two-tx close: receipt #1 → decrease, #2 → collect."""

    SUPPORTED_EXTRACTIONS = ("lp_close_data",)

    def __init__(self, decrease_amounts: tuple[int, int], collect_amounts: tuple[int, int]) -> None:
        self._decrease = decrease_amounts
        self._collect = collect_amounts
        self._call_counter = 0

    def extract_lp_close_data_result(self, receipt: dict[str, Any]) -> ExtractOk:
        # Receipt-shape carries an inline marker so the test controls ordering.
        if receipt.get("event") == "collect":
            return ExtractOk(
                value=LPCloseData(
                    amount0_collected=self._collect[0],
                    amount1_collected=self._collect[1],
                    source="collect",
                )
            )
        return ExtractOk(
            value=LPCloseData(
                amount0_collected=self._decrease[0],
                amount1_collected=self._decrease[1],
                source="decrease_liquidity",
            )
        )


class TestExtractFieldAggregatesLpClose:
    def test_decrease_then_collect_picks_collect(self) -> None:
        """Bundle ordering: receipt #1 = DecreaseLiquidity, receipt #2 = Collect.

        Pre-fix: first-match returned the decrease-sourced LPCloseData and
        the registry payload reported principal-only. Post-fix: aggregation
        picks the Collect-sourced LPCloseData and the registry payload
        reports principal + accrued fees.
        """
        parser = _FakeTwoReceiptSlipstreamParser(
            decrease_amounts=(100, 200), collect_amounts=(150, 260)
        )
        result = _FakeResult()
        enricher = ResultEnricher(live_mode=False)
        receipts = [{"event": "decrease"}, {"event": "collect"}]

        enricher._extract_field(
            result=result,  # type: ignore[arg-type]
            parser=parser,
            receipts=receipts,
            field="lp_close_data",
            intent_type="LP_CLOSE",
            protocol="aerodrome_slipstream",
        )

        assert result.lp_close_data is not None
        assert result.lp_close_data.amount0_collected == 150
        assert result.lp_close_data.amount1_collected == 260
        assert result.lp_close_data.source == "collect"

    def test_collect_then_decrease_still_picks_collect(self) -> None:
        """Order-insensitive: even if Collect is receipt #1, the aggregator
        scans every receipt and prefers the Collect-sourced entry."""
        parser = _FakeTwoReceiptSlipstreamParser(
            decrease_amounts=(100, 200), collect_amounts=(150, 260)
        )
        result = _FakeResult()
        enricher = ResultEnricher(live_mode=False)
        receipts = [{"event": "collect"}, {"event": "decrease"}]

        enricher._extract_field(
            result=result,  # type: ignore[arg-type]
            parser=parser,
            receipts=receipts,
            field="lp_close_data",
            intent_type="LP_CLOSE",
            protocol="aerodrome_slipstream",
        )

        assert result.lp_close_data.amount0_collected == 150
        assert result.lp_close_data.source == "collect"

    def test_decrease_only_falls_back(self) -> None:
        """No Collect in the bundle → fall back to DecreaseLiquidity (legacy)."""

        class _DecreaseOnly:
            SUPPORTED_EXTRACTIONS = ("lp_close_data",)

            def extract_lp_close_data_result(self, _receipt: dict[str, Any]) -> ExtractOk:
                return ExtractOk(
                    value=LPCloseData(
                        amount0_collected=100,
                        amount1_collected=200,
                        source="decrease_liquidity",
                    )
                )

        result = _FakeResult()
        enricher = ResultEnricher(live_mode=False)

        enricher._extract_field(
            result=result,  # type: ignore[arg-type]
            parser=_DecreaseOnly(),
            receipts=[{}, {}],
            field="lp_close_data",
            intent_type="LP_CLOSE",
            protocol="aerodrome_slipstream",
        )

        assert result.lp_close_data.source == "decrease_liquidity"
        assert result.lp_close_data.amount0_collected == 100

    def test_single_receipt_untagged_legacy_parser_still_works(self) -> None:
        """A single-tx parser (UniV3) that doesn't tag source remains
        first-match compatible."""

        class _LegacyUntaggedParser:
            SUPPORTED_EXTRACTIONS = ("lp_close_data",)

            def extract_lp_close_data_result(self, _receipt: dict[str, Any]) -> ExtractOk:
                return ExtractOk(
                    value=LPCloseData(
                        amount0_collected=42,
                        amount1_collected=99,
                        source=None,
                    )
                )

        result = _FakeResult()
        enricher = ResultEnricher(live_mode=False)
        enricher._extract_field(
            result=result,  # type: ignore[arg-type]
            parser=_LegacyUntaggedParser(),
            receipts=[{}],
            field="lp_close_data",
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
        )

        assert result.lp_close_data.amount0_collected == 42
        assert result.lp_close_data.source is None


# ---------------------------------------------------------------------------
# Layer 3: Aerodrome parser stamps source correctly
# ---------------------------------------------------------------------------


class TestAerodromeSlipstreamSourceTagging:
    """Pin the parser-side contract — the enricher relies on these tags."""

    def _build_collect_receipt(self) -> dict[str, Any]:
        from almanak.connectors.aerodrome.receipt_parser import (
            _COLLECT_CL_TOPIC,
        )

        # Collect data layout: recipient(32B) + amount0(32B) + amount1(32B)
        recipient = "0x" + "00" * 12 + "11" * 20
        amount0 = "0x" + format(150, "064x")
        amount1 = "0x" + format(260, "064x")
        data = "0x" + recipient[2:] + amount0[2:] + amount1[2:]
        return {
            "logs": [
                {
                    "topics": [_COLLECT_CL_TOPIC, "0x" + format(42, "064x")],
                    "data": data,
                    "address": "0x" + "aa" * 20,
                }
            ]
        }

    def _build_decrease_receipt(self) -> dict[str, Any]:
        from almanak.connectors.aerodrome.receipt_parser import (
            _DECREASE_LIQUIDITY_TOPIC,
        )

        # DecreaseLiquidity data: liquidity(uint128) + amount0(uint256) + amount1(uint256)
        liquidity = "0x" + format(1_000_000, "064x")
        amount0 = "0x" + format(100, "064x")
        amount1 = "0x" + format(200, "064x")
        data = "0x" + liquidity[2:] + amount0[2:] + amount1[2:]
        return {
            "logs": [
                {
                    "topics": [_DECREASE_LIQUIDITY_TOPIC, "0x" + format(42, "064x")],
                    "data": data,
                    "address": "0x" + "bb" * 20,
                }
            ]
        }

    def test_collect_event_emits_source_collect(self) -> None:
        from almanak.connectors.aerodrome.receipt_parser import (
            AerodromeSlipstreamReceiptParser,
        )

        parser = AerodromeSlipstreamReceiptParser(chain="base")
        out = parser.extract_lp_close_data(self._build_collect_receipt())
        assert out is not None
        assert out.amount0_collected == 150
        assert out.amount1_collected == 260
        assert out.source == "collect"

    def test_decrease_only_emits_source_decrease_liquidity(self) -> None:
        from almanak.connectors.aerodrome.receipt_parser import (
            AerodromeSlipstreamReceiptParser,
        )

        parser = AerodromeSlipstreamReceiptParser(chain="base")
        out = parser.extract_lp_close_data(self._build_decrease_receipt())
        assert out is not None
        assert out.amount0_collected == 100
        assert out.amount1_collected == 200
        assert out.liquidity_removed == 1_000_000
        assert out.source == "decrease_liquidity"


class TestLPCloseDataSerialisesSource:
    """``to_dict`` must surface ``source`` so registry-payload JSON carries it."""

    def test_to_dict_includes_source(self) -> None:
        d = LPCloseData(
            amount0_collected=1, amount1_collected=2, source="collect"
        ).to_dict()
        assert d["source"] == "collect"

    def test_to_dict_source_none(self) -> None:
        d = LPCloseData(amount0_collected=1, amount1_collected=2).to_dict()
        assert d["source"] is None
