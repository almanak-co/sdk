"""Morpho Blue F2 (MorphoMay15 §6.2): SUPPLY enrichment must read
``SupplyCollateral`` events, not just ``Supply`` events.

A Morpho ``SupplyIntent`` against an isolated lending market deposits the
**collateral** leg of the market and emits ``SupplyCollateral`` on-chain —
NOT ``Supply``. The latter is reserved for *loan-side* supply (depositing
the borrowable asset into the market for lenders).

Today the enricher's ``EXTRACTION_SPECS["SUPPLY"]`` asks for ``supply_amount``,
which Morpho's parser implements by reading the ``Supply`` event only —
returning ``None`` for ``SupplyCollateral`` receipts. The parser already
exposes ``extract_supply_collateral_amount`` for the collateral path, but
the enricher never requests it (no per-protocol overlay).

This block is RED until A2 adds:

    EXTRACTION_SPECS_BY_PROTOCOL["morpho_blue"]["SUPPLY"] = ["supply_collateral_amount"]

then ``extracted_data["supply_collateral_amount"]`` populates from the
SupplyCollateral event and downstream accounting can read the true amount.

The mirrored WITHDRAW path uses ``WithdrawCollateral`` and has the same
gap; covered as a future block. This file scopes to SUPPLY.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from almanak.connectors.morpho_blue.receipt_parser import (
    EVENT_TOPICS,
    MorphoBlueReceiptParser,
)
from almanak.framework.execution.result_enricher import ResultEnricher


# ─── Minimal stubs (mirrored from test_result_enricher.py) ───────────────────


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
    """Lending-shaped execution result. Mirrors the field set used by
    ``_attach_to_result`` so ``extracted_data`` round-trips correctly.
    """

    success: bool = True
    transaction_results: list = field(default_factory=list)
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)


@dataclass
class _FakeContext:
    chain: str = "ethereum"
    protocol: str | None = "morpho_blue"


@dataclass
class _FakeIntent:
    intent_type: str = "SUPPLY"
    protocol: str | None = "morpho_blue"


class _PinnedRegistry:
    """Registry stub that always returns one pinned parser, isolating us
    from the global ``_default_registry`` state across tests."""

    def __init__(self, parser: Any) -> None:
        self._parser = parser

    def get(self, protocol, chain=None, **kwargs):  # noqa: ARG002
        return self._parser


# ─── Log builders ────────────────────────────────────────────────────────────


_WALLET = "0x1111111111111111111111111111111111111111"
_MORPHO = "0xbBBBBbbBBbbBBbbBbbBbbbbbBBbBbbbbBbBbbBBbB"  # arbitrary; parser doesn't filter
_MARKET_ID = "0x" + "ab" * 32  # bytes32


def _addr_topic(addr: str) -> str:
    """ABI-pad an address into a 32-byte topic."""
    return "0x" + "00" * 12 + addr[2:].lower()


def _uint256_hex(value: int) -> str:
    return f"{value:064x}"


def _make_supply_collateral_log(assets: int) -> dict[str, Any]:
    """Build a SupplyCollateral(market_id, caller, onBehalfOf, assets) log."""
    return {
        "address": _MORPHO,
        "topics": [
            EVENT_TOPICS["SupplyCollateral"],
            _MARKET_ID,
            _addr_topic(_WALLET),  # caller
            _addr_topic(_WALLET),  # onBehalfOf
        ],
        "data": "0x" + _uint256_hex(assets),
        "logIndex": 0,
    }


def _make_supply_log(assets: int, shares: int) -> dict[str, Any]:
    """Build a Supply(market_id, caller, onBehalfOf, assets, shares) log
    for the loan-side supply path (regression guard).
    """
    return {
        "address": _MORPHO,
        "topics": [
            EVENT_TOPICS["Supply"],
            _MARKET_ID,
            _addr_topic(_WALLET),
            _addr_topic(_WALLET),
        ],
        "data": "0x" + _uint256_hex(assets) + _uint256_hex(shares),
        "logIndex": 0,
    }


# ─── Tests ───────────────────────────────────────────────────────────────────


class TestMorphoSupplyCollateralEnrichment:
    """F2 / MorphoMay15 §6.2 — Morpho SUPPLY intents emit SupplyCollateral,
    not Supply. Enricher must surface ``supply_collateral_amount``."""

    EXPECTED_ASSETS = 17_500_000_000_000_000  # 0.0175 wstETH (8 decimals fine)

    def _run_enricher_on_supply_collateral_only(self) -> Any:
        """Drive a single-log receipt (SupplyCollateral only) through the enricher."""
        log = _make_supply_collateral_log(self.EXPECTED_ASSETS)
        parser = MorphoBlueReceiptParser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _FakeLendingExecResult(
            transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[log]))]
        )
        intent = _FakeIntent(intent_type="SUPPLY", protocol="morpho_blue")
        context = _FakeContext(chain="ethereum", protocol="morpho_blue")
        return enricher.enrich(result, intent, context)

    # ─── 1. The bug-fix invariant (RED before A2, GREEN after) ───────────────

    def test_supply_collateral_amount_surfaces_in_extracted_data(self) -> None:
        """RED until A2 lands. After: SUPPLY intent against a market that
        emitted ``SupplyCollateral`` should expose the assets amount on
        ``extracted_data['supply_collateral_amount']`` so the downstream
        accounting writer can book the typed event with the correct amount.
        """
        enriched = self._run_enricher_on_supply_collateral_only()
        amount = enriched.extracted_data.get("supply_collateral_amount")
        assert amount == self.EXPECTED_ASSETS, (
            "F2 regression: Morpho SUPPLY intents emit SupplyCollateral on-chain, "
            "and the enricher must expose the assets via "
            "extracted_data['supply_collateral_amount']. "
            f"Got: {amount!r}. "
            f"Full extracted_data: {enriched.extracted_data!r}. "
            "Fix: add per-protocol overlay "
            "EXTRACTION_SPECS_BY_PROTOCOL['morpho_blue']['SUPPLY'] = "
            "['supply_collateral_amount']."
        )

    def test_supply_amount_remains_none_for_pure_collateral_receipt(self) -> None:
        """Negative invariant: when the only event is SupplyCollateral,
        ``supply_amount`` (the loan-side amount) MUST be None — not 0.

        ``None`` = unmeasured (no Supply event in this receipt). Substituting
        0 would mislabel collateral deposits as loan-side supplies of zero
        and pollute matching policies downstream (AGENTS.md §Accounting:
        Empty ≠ Zero).
        """
        enriched = self._run_enricher_on_supply_collateral_only()
        assert enriched.extracted_data.get("supply_amount") is None, (
            "supply_amount must remain None on a SupplyCollateral-only receipt. "
            f"Got: {enriched.extracted_data.get('supply_amount')!r}"
        )

    # ─── 2. Regression guards on existing Morpho extraction paths ────────────

    def test_loan_side_supply_event_still_extracts_supply_amount(self) -> None:
        """Loan-side ``Supply`` events (lender depositing borrowable asset)
        must continue to populate ``supply_amount`` after the overlay lands.
        This is the path that already works today; the overlay must be
        additive, not a replacement.
        """
        log = _make_supply_log(assets=42_000_000, shares=100_000_000)
        parser = MorphoBlueReceiptParser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _FakeLendingExecResult(
            transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[log]))]
        )
        intent = _FakeIntent(intent_type="SUPPLY", protocol="morpho_blue")
        context = _FakeContext(chain="ethereum", protocol="morpho_blue")

        enriched = enricher.enrich(result, intent, context)

        assert enriched.extracted_data.get("supply_amount") == 42_000_000, (
            "Loan-side Supply enrichment must not regress when overlay is added. "
            f"Got: {enriched.extracted_data.get('supply_amount')!r}"
        )

    def test_no_supply_collateral_warning_for_morpho(self) -> None:
        """The new ``supply_collateral_amount`` field must not produce a
        SUPPORTED_EXTRACTIONS capability warning for Morpho's parser — it
        implements ``extract_supply_collateral_amount``. (Compare to the
        VIB-4320 TJ-V2 overlay tests that assert no ``bin_ids`` warning.)
        """
        enriched = self._run_enricher_on_supply_collateral_only()
        unwanted = "'supply_collateral_amount'"
        offenders = [w for w in enriched.extraction_warnings if unwanted in w]
        assert not offenders, (
            f"Unexpected supply_collateral_amount warning for morpho_blue: {offenders}"
        )

    # ─── Codex P2 (PR #2322): morpho alias must hit the overlay ──────────────

    def test_morpho_alias_normalizes_to_morpho_blue_for_overlay(self) -> None:
        """The lending compiler and ReceiptParserRegistry both accept
        ``protocol="morpho"`` as an alias for the canonical ``morpho_blue``.
        Without alias normalization, ``_canonicalise_protocol`` would
        return ``"morpho"`` and the overlay lookup against
        ``EXTRACTION_SPECS_BY_PROTOCOL["morpho_blue"]`` would silently miss —
        the parser would still extract events but no overlay fields
        (including ``supply_collateral_amount``) would be requested.
        """
        log = _make_supply_collateral_log(self.EXPECTED_ASSETS)
        parser = MorphoBlueReceiptParser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _FakeLendingExecResult(
            transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[log]))]
        )
        # Intent + context both use the alias "morpho", not the canonical name.
        intent = _FakeIntent(intent_type="SUPPLY", protocol="morpho")
        context = _FakeContext(chain="ethereum", protocol="morpho")

        enriched = enricher.enrich(result, intent, context)

        assert enriched.extracted_data.get("supply_collateral_amount") == self.EXPECTED_ASSETS, (
            "Codex P2 regression: protocol='morpho' must normalize to "
            "'morpho_blue' so the EXTRACTION_SPECS_BY_PROTOCOL overlay applies. "
            f"Got: {enriched.extracted_data.get('supply_collateral_amount')!r}. "
            "Fix lives in protocol_aliases.py:_GLOBAL_ALIASES."
        )
