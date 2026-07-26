"""ResultEnricher stamps the effective trading wallet onto receipts (VIB-6043).

The enricher is the only component that knows both the receipt and the
execution identity: ``ExecutionContext.wallet_address`` is
``runtime_config.execution_address`` — the **Safe** under Safe / Zodiac
execution, the EOA otherwise. It stamps that address onto every receipt it
hands a parser so no parser has to infer the trading wallet from
``receipt["from"]`` (the agent EOA that merely signs
``execTransactionWithRole``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from almanak.connectors._strategy_base.base.receipt_wallet import (
    TRADING_WALLET_KEY,
    resolve_trading_wallet,
)
from almanak.framework.execution.result_enricher import ResultEnricher

AGENT_EOA = "0x42657d7c6Fe1bC3FB0af01a702b88cC31A93661b"
SAFE = "0x4c373c8D5c486F601874EF02A2Cc19b5F4E9e837"


@dataclass
class _FakeExecResult:
    success: bool = True
    transaction_results: list = field(default_factory=list)
    position_id: int | None = None
    swap_amounts: Any = None
    lp_close_data: Any = None
    bridge_data: Any = None
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)


@dataclass
class _FakeTxResult:
    success: bool = True
    tx_hash: str = "0xabc"
    receipt: dict | None = None
    gas_used: int = 200_000


@dataclass
class _FakeContext:
    chain: str = "arbitrum"
    protocol: str | None = "curve"
    wallet_address: str = ""


@dataclass
class _FakeSwapIntent:
    intent_type: str = "SWAP"
    protocol: str | None = "curve"


class _WalletSpyParser:
    """Records the receipts the enricher passes to the extraction methods."""

    SUPPORTED_EXTRACTIONS = frozenset({"swap_amounts"})

    def __init__(self) -> None:
        self.seen: list[dict] = []

    def extract_swap_amounts(self, receipt: dict, **_kwargs: Any) -> None:
        self.seen.append(receipt)
        return None

    def parse_receipt(self, receipt: dict) -> dict:  # noqa: ARG002
        return {}


class _StubRegistry:
    def __init__(self, parser: object) -> None:
        self._parser = parser

    def get(self, protocol: str, **kwargs: object):  # noqa: ARG002
        return self._parser


def _run(wallet_address: str, receipt: dict) -> _WalletSpyParser:
    parser = _WalletSpyParser()
    enricher = ResultEnricher(parser_registry=_StubRegistry(parser), live_mode=False)
    result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=receipt)])
    enricher.enrich(result, _FakeSwapIntent(), _FakeContext(wallet_address=wallet_address))
    return parser


def _receipt() -> dict:
    return {"from": AGENT_EOA, "status": 1, "transactionHash": "0x" + "cd" * 32, "logs": []}


def test_safe_execution_context_reaches_the_parser():
    parser = _run(SAFE, _receipt())
    assert parser.seen, "parser was never called"
    assert parser.seen[0][TRADING_WALLET_KEY] == SAFE.lower()
    assert resolve_trading_wallet(parser.seen[0]) == SAFE.lower()


def test_eoa_execution_context_stamps_the_eoa():
    parser = _run(AGENT_EOA, _receipt())
    # Assert the STAMP explicitly — resolve_trading_wallet alone would also pass
    # if stamping were skipped, because receipt["from"] is already the EOA.
    assert parser.seen[0][TRADING_WALLET_KEY] == AGENT_EOA.lower()
    assert resolve_trading_wallet(parser.seen[0]) == AGENT_EOA.lower()


def test_empty_context_wallet_leaves_legacy_from_behaviour():
    parser = _run("", _receipt())
    assert TRADING_WALLET_KEY not in parser.seen[0]
    assert resolve_trading_wallet(parser.seen[0]) == AGENT_EOA.lower()


def test_stamping_does_not_mutate_the_persisted_receipt():
    """The ledger holds the same receipt object — the stamp must not leak into it."""
    receipt = _receipt()
    _run(SAFE, receipt)
    assert TRADING_WALLET_KEY not in receipt


def test_additional_receipts_are_stamped_too():
    """Keeper / post-submission receipts go through the same parsers."""
    parser = _WalletSpyParser()
    enricher = ResultEnricher(parser_registry=_StubRegistry(parser), live_mode=False)
    result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=_receipt())])
    extra = {"from": AGENT_EOA, "status": 1, "transactionHash": "0x" + "ef" * 32, "logs": []}
    enricher.enrich(
        result,
        _FakeSwapIntent(),
        _FakeContext(wallet_address=SAFE),
        additional_receipts=(extra,),
    )
    assert len(parser.seen) >= 2
    assert all(seen.get(TRADING_WALLET_KEY) == SAFE.lower() for seen in parser.seen)
    # The DISTINCT extra receipt must be the one that reached the parser...
    assert any(seen["transactionHash"] == extra["transactionHash"] for seen in parser.seen)
    # ...and the caller's copy must be untouched.
    assert TRADING_WALLET_KEY not in extra
