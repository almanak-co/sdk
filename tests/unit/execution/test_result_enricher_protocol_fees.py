"""Tests for ResultEnricher protocol_fees extraction pipeline (VIB-3204).

Covers:
- ProtocolFees values attached to result.extracted_data
- fee_tier_bps kwarg forwarded from bundle_metadata["selected_fee_tier"] to
  opt-in parsers via signature introspection
- Non-ProtocolFees return values rejected with a warning (defence-in-depth)
- Backward compatibility: parsers without the kwarg are called cleanly
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.execution.extracted_data import ProtocolFees
from almanak.framework.execution.result_enricher import ResultEnricher


# ---------------------------------------------------------------------------
# Minimal stubs (mirror those in test_result_enricher.py)
# ---------------------------------------------------------------------------
@dataclass
class _FakeReceipt:
    tx_hash: str = "0xabc123"
    block_number: int = 100
    block_hash: str = "0xblock"
    gas_used: int = 200000
    effective_gas_price: int = 1000000000
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
    tx_hash: str = "0xabc123"
    receipt: _FakeReceipt | None = None
    gas_used: int = 200000


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
    bundle_metadata: dict | None = None


@dataclass
class _FakeIntent:
    intent_type: str = "SWAP"
    protocol: str | None = None


class _StubRegistry:
    def __init__(self, parser: object) -> None:
        self._parser = parser

    def get(self, protocol: str, chain: str):  # noqa: ARG002
        return self._parser


# ---------------------------------------------------------------------------
# Parser stubs
# ---------------------------------------------------------------------------
class _FeeOptInParser:
    """Parser that opts into ``fee_tier_bps`` via signature introspection."""

    SUPPORTED_EXTRACTIONS = frozenset({"swap_amounts", "protocol_fees"})

    def __init__(self) -> None:
        self.last_call_kwargs: dict | None = None

    def extract_swap_amounts(self, receipt: dict) -> None:  # noqa: ARG002
        # Not under test here — return None so the enricher moves on.
        return None

    def extract_protocol_fees(
        self,
        receipt: dict,  # noqa: ARG002
        *,
        fee_tier_bps: int | None = None,
    ) -> ProtocolFees:
        self.last_call_kwargs = (
            {"fee_tier_bps": fee_tier_bps} if fee_tier_bps is not None else None
        )
        # VIB-3204 audit fix: ProtocolFees.__post_init__ now enforces
        # total_usd == sum of populated components, so we must pass a
        # total_usd that matches swap_fee_usd when populated.
        if fee_tier_bps is None:
            return ProtocolFees(total_usd=Decimal(0))
        return ProtocolFees(
            total_usd=Decimal("0.05"),
            swap_fee_usd=Decimal("0.05"),
        )


class _FeeLegacyParser:
    """Parser that does NOT accept ``fee_tier_bps``."""

    SUPPORTED_EXTRACTIONS = frozenset({"swap_amounts", "protocol_fees"})

    def __init__(self) -> None:
        self.call_count = 0

    def extract_swap_amounts(self, receipt: dict) -> None:  # noqa: ARG002
        return None

    def extract_protocol_fees(self, receipt: dict) -> ProtocolFees:  # noqa: ARG002
        self.call_count += 1
        return ProtocolFees(total_usd=Decimal(0))


class _FeeBadShapeParser:
    """Parser that returns a non-ProtocolFees value (should be rejected)."""

    SUPPORTED_EXTRACTIONS = frozenset({"swap_amounts", "protocol_fees"})

    def extract_swap_amounts(self, receipt: dict) -> None:  # noqa: ARG002
        return None

    def extract_protocol_fees(self, receipt: dict) -> dict:  # noqa: ARG002
        # Deliberate violation: parsers must return ProtocolFees, not a dict.
        return {"total_usd": "0"}


class _FeeMissingParser:
    """Parser that returns None (benign ExtractMissing)."""

    SUPPORTED_EXTRACTIONS = frozenset({"swap_amounts", "protocol_fees"})

    def extract_swap_amounts(self, receipt: dict) -> None:  # noqa: ARG002
        return None

    def extract_protocol_fees(self, receipt: dict) -> None:  # noqa: ARG002
        return None


def _fake_receipt() -> _FakeReceipt:
    return _FakeReceipt(status=1, logs=[{"address": "0x0"}], from_address="0x1")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestProtocolFeesAttachment:
    """ProtocolFees returned by the parser must land in result.extracted_data."""

    def test_protocol_fees_attached_to_extracted_data_on_swap(self):
        parser = _FeeOptInParser()
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser))
        result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=_fake_receipt())])
        intent = _FakeIntent(protocol="fee_protocol")
        context = _FakeContext(
            chain="ethereum",
            protocol="fee_protocol",
            bundle_metadata={"selected_fee_tier": 500},
        )

        enriched = enricher.enrich(result, intent, context, bundle_metadata=context.bundle_metadata)

        fees = enriched.extracted_data.get("protocol_fees")
        assert isinstance(fees, ProtocolFees)
        assert fees.swap_fee_usd == Decimal("0.05")

    def test_protocol_fees_missing_when_parser_returns_none(self):
        parser = _FeeMissingParser()
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser))
        result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=_fake_receipt())])
        intent = _FakeIntent(protocol="fee_protocol")
        context = _FakeContext(
            chain="ethereum",
            protocol="fee_protocol",
            bundle_metadata={"selected_fee_tier": 500},
        )

        enriched = enricher.enrich(result, intent, context, bundle_metadata=context.bundle_metadata)

        assert "protocol_fees" not in enriched.extracted_data


class TestFeeTierBpsKwargPlumbing:
    """fee_tier_bps forwarding must mirror the VIB-3203 expected_amount_out path."""

    def test_kwarg_forwarded_when_parser_opts_in(self):
        parser = _FeeOptInParser()
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser))
        result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=_fake_receipt())])
        intent = _FakeIntent(protocol="fee_protocol")
        context = _FakeContext(
            chain="ethereum",
            protocol="fee_protocol",
            bundle_metadata={"selected_fee_tier": 500},
        )

        enricher.enrich(result, intent, context, bundle_metadata=context.bundle_metadata)

        assert parser.last_call_kwargs == {"fee_tier_bps": 500}

    def test_kwarg_not_forwarded_when_missing_metadata(self):
        parser = _FeeOptInParser()
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser))
        result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=_fake_receipt())])
        intent = _FakeIntent(protocol="fee_protocol")
        context = _FakeContext(chain="ethereum", protocol="fee_protocol", bundle_metadata=None)

        enricher.enrich(result, intent, context, bundle_metadata=context.bundle_metadata)

        # Parser invoked via single-arg fallback — no kwarg recorded.
        assert parser.last_call_kwargs is None

    def test_kwarg_not_forwarded_when_parser_does_not_opt_in(self):
        """Legacy parser without the kwarg is still called cleanly (no TypeError)."""
        parser = _FeeLegacyParser()
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser))
        result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=_fake_receipt())])
        intent = _FakeIntent(protocol="fee_protocol")
        context = _FakeContext(
            chain="ethereum",
            protocol="fee_protocol",
            bundle_metadata={"selected_fee_tier": 500},
        )

        enriched = enricher.enrich(result, intent, context, bundle_metadata=context.bundle_metadata)

        assert parser.call_count == 1
        fees = enriched.extracted_data.get("protocol_fees")
        assert isinstance(fees, ProtocolFees)

    def test_non_integer_fee_tier_is_ignored(self):
        """Malformed fee-tier metadata must not crash enrichment."""
        parser = _FeeOptInParser()
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser))
        result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=_fake_receipt())])
        intent = _FakeIntent(protocol="fee_protocol")
        context = _FakeContext(
            chain="ethereum",
            protocol="fee_protocol",
            bundle_metadata={"selected_fee_tier": "not-a-number"},
        )

        enricher.enrich(result, intent, context, bundle_metadata=context.bundle_metadata)

        # Extractor still invoked without the kwarg (fallback path).
        assert parser.last_call_kwargs is None


class TestProtocolFeesRejectsBadShape:
    """Parsers must return ProtocolFees — other shapes are rejected to keep
    strategy authors from reaching into dicts by accident."""

    def test_non_protocol_fees_return_value_rejected(self):
        parser = _FeeBadShapeParser()
        enricher = ResultEnricher(parser_registry=_StubRegistry(parser))
        result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=_fake_receipt())])
        intent = _FakeIntent(protocol="fee_protocol")
        context = _FakeContext(chain="ethereum", protocol="fee_protocol", bundle_metadata=None)

        enriched = enricher.enrich(result, intent, context, bundle_metadata=context.bundle_metadata)

        assert "protocol_fees" not in enriched.extracted_data
