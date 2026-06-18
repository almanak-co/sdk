"""VIB-5220 — Lido STAKE declares its money legs via the PrimitiveMoneyLeg contract.

Lido is the first connector migrated onto the typed money-leg contract (US-008 /
US-009). Its parser's ``extract_primitive_money_legs`` returns a
``PrimitiveMoneyLegs`` (INPUT=ETH staked, OUTPUT=stETH/wstETH minted); the
per-protocol overlay ``EXTRACTION_SPECS_BY_PROTOCOL["lido"]["STAKE"] =
["primitive_money_legs"]`` surfaces it under
``extracted_data["primitive_money_legs"]`` — the seam the ledger dispatcher
prefers over the legacy intent-attribute guesser.

This replaces the Lido-only STAKE output patch (#2897): instead of a
Lido-specific ``elif`` branch in ``observability/ledger.py``, the connector
declares its legs and the generic declared-legs path books the row.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from almanak.connectors.lido.receipt_parser import EVENT_TOPICS, LidoReceiptParser
from almanak.framework.execution.result_enricher import ResultEnricher

_STETH = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
_WSTETH = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
_USER = "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd"
_ZERO = "0x" + "0" * 64
_ONE_ETH = "0x" + "0000000000000000000000000000000000000000000000000de0b6b3a7640000" + "0" * 64
_ONE_VALUE = "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1.0
_085_VALUE = "0x0000000000000000000000000000000000000000000000000bcbce7f1b150000"  # 0.85


@dataclass
class _FakeReceipt:
    tx_hash: str = "0xstake"
    block_number: int = 100
    gas_used: int = 90_000
    status: int = 1
    logs: list = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx_hash": self.tx_hash,
            "block_number": self.block_number,
            "gas_used": self.gas_used,
            "status": self.status,
            "logs": self.logs,
            "contract_address": None,
        }


@dataclass
class _FakeTxResult:
    success: bool = True
    tx_hash: str = "0xstake"
    receipt: _FakeReceipt | None = None
    gas_used: int = 90_000


@dataclass
class _FakeExecResult:
    success: bool = True
    transaction_results: list = field(default_factory=list)
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)


@dataclass
class _FakeContext:
    chain: str = "ethereum"
    protocol: str | None = "lido"


@dataclass
class _FakeIntent:
    intent_type: str = "STAKE"
    protocol: str | None = "lido"
    token_in: str = "ETH"


class _PinnedRegistry:
    def __init__(self, parser: Any) -> None:
        self._parser = parser

    def get(self, protocol, chain=None, **kwargs):  # noqa: ARG002
        return self._parser


def _submitted() -> dict:
    return {"address": _STETH, "topics": [EVENT_TOPICS["Submitted"], _USER], "data": _ONE_ETH}


def _steth_mint() -> dict:
    return {"address": _STETH, "topics": [EVENT_TOPICS["Transfer"], _ZERO, _USER], "data": _ONE_VALUE}


def _wsteth_mint() -> dict:
    return {"address": _WSTETH, "topics": [EVENT_TOPICS["Transfer"], _ZERO, _USER], "data": _085_VALUE}


def _enrich(logs: list) -> Any:
    parser = LidoReceiptParser(chain="ethereum")
    enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
    result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=logs))])
    return enricher.enrich(result, _FakeIntent(), _FakeContext())


class TestLidoStakeDeclaredLegs:
    def test_overlay_surfaces_declared_legs(self) -> None:
        """The lido STAKE overlay surfaces a typed PrimitiveMoneyLegs under the
        seam key the dispatcher reads."""
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

        enriched = _enrich([_submitted(), _steth_mint()])
        legs = enriched.extracted_data.get("primitive_money_legs")
        assert isinstance(legs, PrimitiveMoneyLegs)
        assert [(leg.role.value, leg.token) for leg in legs.legs] == [
            ("input", "ETH"),
            ("output", "stETH"),
        ]

    def test_declared_legs_book_ledger_row(self) -> None:
        """End-to-end through the dispatcher: the declared legs book
        token_in=ETH/token_out=stETH + amounts — the #2897 booking via the
        typed contract, NOT the intent_fallback or a Lido-specific elif."""
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        enriched = _enrich([_submitted(), _steth_mint()])
        assert _extract_tokens_and_amounts(_FakeIntent(), enriched, chain="ethereum") == (
            "ETH",
            "stETH",
            "1",
            "1",
            "",
            None,
        )

    def test_no_fallback_warn_for_lido_stake(self, caplog) -> None:
        """The declared-legs path drives the row, so the intent_fallback WARN /
        metric must NOT fire for a Lido STAKE."""
        import logging

        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        enriched = _enrich([_submitted(), _steth_mint()])
        with caplog.at_level(logging.WARNING):
            _extract_tokens_and_amounts(_FakeIntent(), enriched, chain="ethereum")
        assert "intent_fallback" not in caplog.text

    def test_no_capability_warning_for_primitive_money_legs(self) -> None:
        """The parser implements ``extract_primitive_money_legs`` so the overlay
        field must not emit a SUPPORTED_EXTRACTIONS capability warning."""
        enriched = _enrich([_submitted(), _steth_mint()])
        offenders = [w for w in enriched.extraction_warnings if "primitive_money_legs" in w]
        assert not offenders, f"Unexpected capability warning: {offenders}"

    def test_wrapped_stake_books_wsteth(self) -> None:
        """receive_wrapped path: the OUTPUT leg is the measured wstETH wrap-mint."""
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        enriched = _enrich([_submitted(), _steth_mint(), _wsteth_mint()])
        token_in, token_out, amount_in, amount_out, *_ = _extract_tokens_and_amounts(
            _FakeIntent(), enriched, chain="ethereum"
        )
        assert (token_in, token_out, amount_in, amount_out) == ("ETH", "wstETH", "1", "0.85")
