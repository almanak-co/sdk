"""VIB-4633 Finding A: Compound V3 SUPPLY enrichment must read
``SupplyCollateral`` events, not just the base-asset ``Supply`` event.

A Compound V3 ``SupplyIntent`` for a **collateral** asset (e.g. WETH on the
USDC Comet) routes through ``Comet.supplyCollateral(asset, amount)`` and emits
``SupplyCollateral`` on-chain — NOT the base-asset ``Supply`` event. The
generic ``EXTRACTION_SPECS["SUPPLY"]`` asks for ``supply_amount``, which the
Compound parser implements by reading the ``Supply`` event only — returning
``None`` for a collateral receipt. So the persisted
``LendingAccountingEvent.amount_token`` came back ``None`` even though the
supplied amount is known exactly on-chain.

The fix adds, mirroring the morpho_blue overlay:

    EXTRACTION_SPECS_BY_PROTOCOL["compound_v3"]["SUPPLY"] = ["supply_collateral_amount"]

plus the parser's ``extract_supply_collateral_amount`` (+ fail-closed
``_result`` variant). The lending handler's existing
``_COLLATERAL_FALLBACK_BY_INTENT["SUPPLY"]`` then scales it into
``amount_token``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from almanak.connectors.compound_v3.receipt_parser import (
    EVENT_TOPICS,
    CompoundV3ReceiptParser,
)
from almanak.framework.execution.result_enricher import ResultEnricher

# ─── Minimal stubs (mirrored from test_result_enricher_morpho_supply_collateral.py) ──


@dataclass
class _FakeReceipt:
    """Mimics TransactionReceipt.to_dict() shape consumed by the enricher."""

    tx_hash: str = "0xabc123"
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
    tx_hash: str = "0xabc123"
    receipt: _FakeReceipt | None = None
    gas_used: int = 200_000


@dataclass
class _FakeLendingExecResult:
    success: bool = True
    transaction_results: list = field(default_factory=list)
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)


@dataclass
class _FakeContext:
    chain: str = "ethereum"
    protocol: str | None = "compound_v3"


@dataclass
class _FakeIntent:
    intent_type: str = "SUPPLY"
    protocol: str | None = "compound_v3"


class _PinnedRegistry:
    """Registry stub that always returns one pinned parser."""

    def __init__(self, parser: Any) -> None:
        self._parser = parser

    def get(self, protocol, chain=None, **kwargs):  # noqa: ARG002
        return self._parser


# ─── Log builders ────────────────────────────────────────────────────────────


_WALLET = "0x1111111111111111111111111111111111111111"
_DST = "0x2222222222222222222222222222222222222222"
_WETH = "0xc02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
_COMET = "0xc3d688B66703497DAA19211EEdff47f25384cdc3"


def _addr_topic(addr: str) -> str:
    """ABI-pad an address into a 32-byte topic."""
    return "0x" + "00" * 12 + addr[2:].lower()


def _uint256_hex(value: int) -> str:
    return f"{value:064x}"


def _make_supply_collateral_log(amount: int) -> dict[str, Any]:
    """SupplyCollateral(from indexed, dst indexed, asset indexed, uint amount)."""
    return {
        "address": _COMET,
        "topics": [
            EVENT_TOPICS["SupplyCollateral"],
            _addr_topic(_WALLET),  # from
            _addr_topic(_DST),  # dst
            _addr_topic(_WETH),  # asset
        ],
        "data": "0x" + _uint256_hex(amount),
        "logIndex": 0,
    }


def _make_supply_log(amount: int) -> dict[str, Any]:
    """Supply(from indexed, dst indexed, uint amount) — base-asset leg."""
    return {
        "address": _COMET,
        "topics": [
            EVENT_TOPICS["Supply"],
            _addr_topic(_WALLET),  # from
            _addr_topic(_DST),  # dst
        ],
        "data": "0x" + _uint256_hex(amount),
        "logIndex": 0,
    }


# ─── Tests ───────────────────────────────────────────────────────────────────


class TestCompoundSupplyCollateralEnrichment:
    """VIB-4633 Finding A — Compound SUPPLY of a collateral asset emits
    SupplyCollateral, not Supply. Enricher must surface
    ``supply_collateral_amount``."""

    EXPECTED_AMOUNT = 1 * 10**18  # 1 WETH

    def _run_enricher_on_supply_collateral_only(self, protocol: str = "compound_v3") -> Any:
        log = _make_supply_collateral_log(self.EXPECTED_AMOUNT)
        parser = CompoundV3ReceiptParser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _FakeLendingExecResult(transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[log]))])
        intent = _FakeIntent(intent_type="SUPPLY", protocol=protocol)
        context = _FakeContext(chain="ethereum", protocol=protocol)
        return enricher.enrich(result, intent, context)

    def test_supply_collateral_amount_surfaces_in_extracted_data(self) -> None:
        """SUPPLY against a collateral asset (SupplyCollateral receipt) must expose
        the amount on ``extracted_data['supply_collateral_amount']`` for the
        downstream lending writer to book ``amount_token``."""
        enriched = self._run_enricher_on_supply_collateral_only()
        amount = enriched.extracted_data.get("supply_collateral_amount")
        assert amount == self.EXPECTED_AMOUNT, (
            "VIB-4633 Finding A: Compound SUPPLY of a collateral asset emits "
            "SupplyCollateral; the enricher must expose the amount via "
            "extracted_data['supply_collateral_amount']. "
            f"Got: {amount!r}. Full extracted_data: {enriched.extracted_data!r}."
        )

    def test_supply_amount_remains_none_for_pure_collateral_receipt(self) -> None:
        """Negative invariant: with only a SupplyCollateral event, ``supply_amount``
        (the base-asset leg) MUST stay None — never a fabricated 0 (Empty != Zero)."""
        enriched = self._run_enricher_on_supply_collateral_only()
        assert enriched.extracted_data.get("supply_amount") is None, (
            "supply_amount must remain None on a SupplyCollateral-only receipt. "
            f"Got: {enriched.extracted_data.get('supply_amount')!r}"
        )

    def test_base_asset_supply_event_still_extracts_supply_amount(self) -> None:
        """Base-asset ``Supply`` (Comet.supply of the base token) must continue to
        populate ``supply_amount`` — the overlay is additive, not a replacement."""
        log = _make_supply_log(amount=42_000_000)
        parser = CompoundV3ReceiptParser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _FakeLendingExecResult(transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[log]))])
        intent = _FakeIntent(intent_type="SUPPLY", protocol="compound_v3")
        context = _FakeContext(chain="ethereum", protocol="compound_v3")

        enriched = enricher.enrich(result, intent, context)

        assert enriched.extracted_data.get("supply_amount") == 42_000_000, (
            "Base-asset Supply enrichment must not regress when the overlay is added. "
            f"Got: {enriched.extracted_data.get('supply_amount')!r}"
        )

    def test_no_supply_collateral_warning_for_compound(self) -> None:
        """The new ``supply_collateral_amount`` field must not produce a
        SUPPORTED_EXTRACTIONS capability warning — the parser implements
        ``extract_supply_collateral_amount``."""
        enriched = self._run_enricher_on_supply_collateral_only()
        unwanted = "'supply_collateral_amount'"
        offenders = [w for w in enriched.extraction_warnings if unwanted in w]
        assert not offenders, f"Unexpected supply_collateral_amount warning for compound_v3: {offenders}"

    def test_compound_alias_normalizes_for_overlay(self) -> None:
        """``protocol='compound'`` must normalize to ``compound_v3`` so the
        overlay lookup applies (mirrors the morpho alias guard)."""
        enriched = self._run_enricher_on_supply_collateral_only(protocol="compound")
        assert enriched.extracted_data.get("supply_collateral_amount") == self.EXPECTED_AMOUNT, (
            "protocol='compound' must normalize to 'compound_v3' so the "
            "EXTRACTION_SPECS_BY_PROTOCOL overlay applies. "
            f"Got: {enriched.extracted_data.get('supply_collateral_amount')!r}."
        )
