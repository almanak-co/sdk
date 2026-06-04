"""VIB-4635: Morpho Blue WITHDRAW enrichment must read ``WithdrawCollateral``
events, not just the loan-side ``Withdraw`` event.

A Morpho ``WithdrawIntent`` for the collateral leg of an isolated lending
market routes through ``withdrawCollateral(...)`` and emits
``WithdrawCollateral`` on-chain — NOT ``Withdraw`` (which is reserved for
loan-side withdrawals of the borrowable asset by a lender).

The generic ``EXTRACTION_SPECS["WITHDRAW"]`` asks for ``withdraw_amount``,
which Morpho's parser implements by reading the ``Withdraw`` event only —
returning ``None`` for ``WithdrawCollateral`` receipts. The morpho_blue
overlay now also requests ``withdraw_collateral_amount`` so
``extracted_data["withdraw_collateral_amount"]`` populates from the
WithdrawCollateral event and downstream accounting can read the true amount.

WITHDRAW-side mirror of test_result_enricher_morpho_supply_collateral.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from almanak.connectors.morpho_blue.receipt_parser import (
    EVENT_TOPICS,
    MorphoBlueReceiptParser,
)
from almanak.framework.execution.result_enricher import ResultEnricher

# ─── Minimal stubs (mirrored from the SUPPLY collateral test) ────────────────


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
    protocol: str | None = "morpho_blue"


@dataclass
class _FakeIntent:
    intent_type: str = "WITHDRAW"
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


def _addr_word(addr: str) -> str:
    """ABI-pad an address into a 32-byte data word (no 0x prefix)."""
    return "00" * 12 + addr[2:].lower()


def _uint256_hex(value: int) -> str:
    return f"{value:064x}"


def _make_withdraw_collateral_log(assets: int) -> dict[str, Any]:
    """Build a WithdrawCollateral(id, caller, onBehalfOf, receiver, assets) log.

    caller is the first data word; assets is the second.
    """
    return {
        "address": _MORPHO,
        "topics": [
            EVENT_TOPICS["WithdrawCollateral"],
            _MARKET_ID,
            _addr_topic(_WALLET),  # onBehalfOf
            _addr_topic(_WALLET),  # receiver
        ],
        "data": "0x" + _addr_word(_WALLET) + _uint256_hex(assets),
        "logIndex": 0,
    }


def _make_withdraw_log(assets: int, shares: int) -> dict[str, Any]:
    """Build a loan-side Withdraw(id, caller, onBehalfOf, receiver, assets,
    shares) log for the regression guard.
    """
    return {
        "address": _MORPHO,
        "topics": [
            EVENT_TOPICS["Withdraw"],
            _MARKET_ID,
            _addr_topic(_WALLET),  # onBehalfOf
            _addr_topic(_WALLET),  # receiver
        ],
        "data": "0x" + _addr_word(_WALLET) + _uint256_hex(assets) + _uint256_hex(shares),
        "logIndex": 0,
    }


# ─── Tests ───────────────────────────────────────────────────────────────────


class TestMorphoWithdrawCollateralEnrichment:
    """VIB-4635 — Morpho WITHDRAW intents emit WithdrawCollateral, not
    Withdraw. Enricher must surface ``withdraw_collateral_amount``."""

    EXPECTED_ASSETS = 200_000_000_000_000_000  # 0.2 wstETH

    def _run_enricher_on_withdraw_collateral_only(self) -> Any:
        log = _make_withdraw_collateral_log(self.EXPECTED_ASSETS)
        parser = MorphoBlueReceiptParser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _FakeLendingExecResult(transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[log]))])
        intent = _FakeIntent(intent_type="WITHDRAW", protocol="morpho_blue")
        context = _FakeContext(chain="ethereum", protocol="morpho_blue")
        return enricher.enrich(result, intent, context)

    def test_withdraw_collateral_amount_surfaces_in_extracted_data(self) -> None:
        enriched = self._run_enricher_on_withdraw_collateral_only()
        amount = enriched.extracted_data.get("withdraw_collateral_amount")
        assert amount == self.EXPECTED_ASSETS, (
            "VIB-4635: Morpho WITHDRAW intents emit WithdrawCollateral on-chain, "
            "and the enricher must expose the assets via "
            "extracted_data['withdraw_collateral_amount']. "
            f"Got: {amount!r}. Full extracted_data: {enriched.extracted_data!r}."
        )

    def test_withdraw_amount_remains_none_for_pure_collateral_receipt(self) -> None:
        """Empty ≠ Zero: a WithdrawCollateral-only receipt must leave the
        loan-side ``withdraw_amount`` None, never 0."""
        enriched = self._run_enricher_on_withdraw_collateral_only()
        assert enriched.extracted_data.get("withdraw_amount") is None, (
            "withdraw_amount must remain None on a WithdrawCollateral-only receipt. "
            f"Got: {enriched.extracted_data.get('withdraw_amount')!r}"
        )

    def test_loan_side_withdraw_event_still_extracts_withdraw_amount(self) -> None:
        """Loan-side ``Withdraw`` events must continue to populate
        ``withdraw_amount`` — the overlay is additive, not a replacement."""
        log = _make_withdraw_log(assets=42_000_000, shares=100_000_000)
        parser = MorphoBlueReceiptParser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _FakeLendingExecResult(transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[log]))])
        intent = _FakeIntent(intent_type="WITHDRAW", protocol="morpho_blue")
        context = _FakeContext(chain="ethereum", protocol="morpho_blue")

        enriched = enricher.enrich(result, intent, context)

        assert enriched.extracted_data.get("withdraw_amount") == 42_000_000, (
            "Loan-side Withdraw enrichment must not regress when overlay is added. "
            f"Got: {enriched.extracted_data.get('withdraw_amount')!r}"
        )

    def test_no_withdraw_collateral_warning_for_morpho(self) -> None:
        """The new ``withdraw_collateral_amount`` field must not produce a
        SUPPORTED_EXTRACTIONS capability warning — the parser implements
        ``extract_withdraw_collateral_amount``."""
        enriched = self._run_enricher_on_withdraw_collateral_only()
        unwanted = "'withdraw_collateral_amount'"
        offenders = [w for w in enriched.extraction_warnings if unwanted in w]
        assert not offenders, f"Unexpected withdraw_collateral_amount warning for morpho_blue: {offenders}"

    def test_morpho_alias_normalizes_to_morpho_blue_for_overlay(self) -> None:
        """``protocol='morpho'`` must normalize to ``morpho_blue`` so the
        overlay applies (mirror of the SUPPLY alias guard)."""
        log = _make_withdraw_collateral_log(self.EXPECTED_ASSETS)
        parser = MorphoBlueReceiptParser()
        enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
        result = _FakeLendingExecResult(transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[log]))])
        intent = _FakeIntent(intent_type="WITHDRAW", protocol="morpho")
        context = _FakeContext(chain="ethereum", protocol="morpho")

        enriched = enricher.enrich(result, intent, context)

        assert enriched.extracted_data.get("withdraw_collateral_amount") == self.EXPECTED_ASSETS, (
            "protocol='morpho' must normalize to 'morpho_blue' so the "
            "EXTRACTION_SPECS_BY_PROTOCOL overlay applies. "
            f"Got: {enriched.extracted_data.get('withdraw_collateral_amount')!r}."
        )
