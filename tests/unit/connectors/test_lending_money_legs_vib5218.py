"""VIB-5218 — euler_v2 / spark / morpho_blue declare lending money legs.

Lending intents (SUPPLY / WITHDRAW / BORROW / REPAY) used to write a NULL
``token_in`` on the transaction ledger: their row fell through to the legacy
intent-attribute guesser, which left ``token_in`` empty → the lending accounting
handler resolved ``asset = "UNKNOWN"`` → no FIFO supply lot →
``deployed_capital_usd = 0`` and no cost basis (the books didn't tie).

The fix is the shared ``extract_primitive_money_legs`` seam (the Lido / Curve /
Pendle pattern): each connector DECLARES one typed PRINCIPAL leg, and the US-009
ledger dispatcher projects it onto ``token_in`` / ``amount_in``. These tests prove
the declared leg carries a real token + measured amount (not UNKNOWN / None) and
that it drives the ledger row end-to-end through the dispatcher.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from almanak.connectors._strategy_base.primitive_money_leg import MoneyLegRole, PrimitiveMoneyLegs
from almanak.framework.execution.result_enricher import ResultEnricher
from almanak.framework.observability.ledger import _extract_tokens_and_amounts

# Real mainnet addresses so the static token resolver yields (symbol, decimals).
_USDC_ETH = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
_USER = "0x1111111111111111111111111111111111111111"


def _word(value: int) -> str:
    return f"{int(value):064x}"


def _addr_topic(addr: str) -> str:
    return "0x" + "0" * 24 + addr.lower().replace("0x", "")


# ──────────────────────────────────────────────────────────────────────────────
# Receipt builders (one SUPPLY receipt per connector, 1.5 / 2 / 2.5 USDC raw)
# ──────────────────────────────────────────────────────────────────────────────


def _spark_supply_receipt(amount_raw: int) -> dict[str, Any]:
    from almanak.connectors.spark.receipt_parser import EVENT_TOPICS, SPARK_POOL_ADDRESSES

    pool = next(iter(SPARK_POOL_ADDRESSES))
    data = "0x" + _word(int(_USER, 16)) + _word(amount_raw) + _word(0)  # user, amount, referral
    log = {
        "address": pool,
        "topics": [EVENT_TOPICS["Supply"], _addr_topic(_USDC_ETH), _addr_topic(_USER)],
        "data": data,
    }
    return {"transactionHash": "0xspark", "blockNumber": 1, "logs": [log]}


def _euler_deposit_receipt(amount_raw: int, vault: str) -> dict[str, Any]:
    from almanak.connectors.euler_v2.receipt_parser import DEPOSIT_TOPIC

    log = {
        "address": vault,
        "topics": [DEPOSIT_TOPIC, _addr_topic(_USER), _addr_topic(_USER)],
        "data": "0x" + _word(amount_raw) + _word(amount_raw - 1000),  # assets, shares
    }
    return {"transactionHash": "0xeuler", "blockNumber": 1, "logs": [log]}


def _euler_borrow_with_collateral_receipt(
    *, collateral_vault: str, collateral_raw: int, borrow_vault: str, borrow_raw: int
) -> dict[str, Any]:
    """A combined euler BORROW EVC-batch receipt: a collateral Deposit emitted
    BEFORE the loan Borrow (the on-chain ordering #3057's borrow path produces).

    The principal of a BORROW is the LOAN token, so the extractor must book the
    Borrow vault's underlying — NOT the collateral Deposit emitted first.
    """
    from almanak.connectors.euler_v2.receipt_parser import BORROW_TOPIC, DEPOSIT_TOPIC

    return {
        "transactionHash": "0xeuler_borrow",
        "blockNumber": 1,
        "logs": [
            {
                # Collateral Deposit FIRST (the trap: first lending log != principal).
                "address": collateral_vault,
                "topics": [DEPOSIT_TOPIC, _addr_topic(_USER), _addr_topic(_USER)],
                "data": "0x" + _word(collateral_raw) + _word(collateral_raw - 1000),
            },
            {
                # Loan Borrow SECOND — this vault's underlying is the principal.
                "address": borrow_vault,
                "topics": [BORROW_TOPIC, _addr_topic(_USER)],
                "data": "0x" + _word(borrow_raw),
            },
        ],
    }


def _morpho_supply_receipt(amount_raw: int, *, with_transfer: bool = True) -> dict[str, Any]:
    from almanak.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

    morpho_ctr = "0xBBBBBBBBBB1111111111111111111111111111BB"
    mkt_id = "0x" + "ab" * 32
    logs = [
        {
            "address": morpho_ctr,
            "topics": [EVENT_TOPICS["Supply"], mkt_id, _addr_topic(_USER), _addr_topic(_USER)],
            "data": "0x" + _word(amount_raw) + _word(amount_raw * 100),  # assets, shares
        }
    ]
    if with_transfer:
        logs.append(
            {
                "address": _USDC_ETH,
                "topics": [EVENT_TOPICS["Transfer"], _addr_topic(_USER), _addr_topic(morpho_ctr)],
                "data": "0x" + _word(amount_raw),
            }
        )
    return {"transactionHash": "0xmorpho", "blockNumber": 1, "logs": logs}


def _morpho_loan_receipt(event_name: str, amount_raw: int) -> dict[str, Any]:
    """A Morpho loan-side receipt for any of Supply / Withdraw / Borrow / Repay.

    Morpho carries ``assets`` at DIFFERENT data offsets per action: Withdraw /
    Borrow prefix a non-indexed ``caller`` word (assets at word 1) while Supply /
    Repay put ``assets`` at word 0. The builder mirrors those exact layouts so the
    test exercises the real per-event decode, plus a matching loan-token Transfer.
    """
    from almanak.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

    morpho_ctr = "0xBBBBBBBBBB1111111111111111111111111111BB"
    mkt_id = "0x" + "ab" * 32
    if event_name in ("Supply", "Repay"):
        # topics: id, caller, onBehalfOf ; data: assets, shares
        topics = [EVENT_TOPICS[event_name], mkt_id, _addr_topic(_USER), _addr_topic(_USER)]
        data = "0x" + _word(amount_raw) + _word(amount_raw * 100)
    else:  # Withdraw / Borrow — topics: id, onBehalfOf, receiver ; data: caller, assets, shares
        topics = [EVENT_TOPICS[event_name], mkt_id, _addr_topic(_USER), _addr_topic(_USER)]
        data = "0x" + _word(int(_USER, 16)) + _word(amount_raw) + _word(amount_raw * 100)
    logs = [
        {"address": morpho_ctr, "topics": topics, "data": data},
        {
            # Loan-token Transfer (direction-agnostic for the matcher).
            "address": _USDC_ETH,
            "topics": [EVENT_TOPICS["Transfer"], _addr_topic(morpho_ctr), _addr_topic(_USER)],
            "data": "0x" + _word(amount_raw),
        },
    ]
    return {"transactionHash": "0xmorpho_" + event_name.lower(), "blockNumber": 1, "logs": logs}


def _euler_vault() -> str:
    # eUSDC-2 ethereum (underlying USDC, 6 decimals) — present in the static map.
    return "0x797DD80692c3b2dAdabCe8e30C07fDE5307D48a9"


def _euler_vault_underlying_map() -> dict[str, tuple[str, int]]:
    from almanak.connectors.euler_v2.receipt_parser import _vault_underlying_map

    return _vault_underlying_map()


# ──────────────────────────────────────────────────────────────────────────────
# Direct parser-level assertions: a SUPPLY receipt declares a real PRINCIPAL leg
# ──────────────────────────────────────────────────────────────────────────────


def _spark_parser() -> Any:
    from almanak.connectors.spark.receipt_parser import SparkReceiptParser

    return SparkReceiptParser(chain="ethereum")


def _euler_parser() -> Any:
    from almanak.connectors.euler_v2.receipt_parser import EulerV2ReceiptParser

    return EulerV2ReceiptParser(chain="ethereum")


def _morpho_parser() -> Any:
    from almanak.connectors.morpho_blue.receipt_parser import MorphoBlueReceiptParser

    return MorphoBlueReceiptParser(chain="ethereum")


@pytest.mark.parametrize(
    ("parser_factory", "receipt", "expected_amount"),
    [
        (_spark_parser, _spark_supply_receipt(1_500_000), "1.5"),
        (_euler_parser, _euler_deposit_receipt(2_000_000, _euler_vault()), "2"),
        (_morpho_parser, _morpho_supply_receipt(2_500_000), "2.5"),
    ],
)
def test_supply_declares_real_principal_leg(parser_factory, receipt, expected_amount) -> None:
    parser = parser_factory()
    legs = parser.extract_primitive_money_legs(receipt)
    assert isinstance(legs, PrimitiveMoneyLegs)
    principal = legs.principal_legs
    assert len(principal) == 1, "lending action declares exactly one PRINCIPAL leg"
    leg = principal[0]
    assert leg.role is MoneyLegRole.PRINCIPAL
    assert leg.token == "USDC", "token is the resolved chain-truth symbol, not UNKNOWN"
    assert leg.amount.is_measured, "amount is measured (Empty != Zero), not None"
    assert str(leg.amount.value) == expected_amount


@pytest.mark.parametrize(
    ("event_name", "amount_raw", "expected_amount"),
    [
        ("Withdraw", 3_000_000, "3"),
        ("Borrow", 3_500_000, "3.5"),
        ("Repay", 4_000_000, "4"),
    ],
)
def test_morpho_all_loan_actions_declare_real_principal_leg(
    event_name: str, amount_raw: int, expected_amount: str
) -> None:
    """The seam is action-agnostic: WITHDRAW / BORROW / REPAY each declare a real
    PRINCIPAL leg with the right token + measured amount, despite Morpho's
    differing per-event ``assets`` offsets (the SUPPLY-only test would miss a
    Withdraw/Borrow offset regression)."""
    parser = _morpho_parser()
    legs = parser.extract_primitive_money_legs(_morpho_loan_receipt(event_name, amount_raw))
    assert isinstance(legs, PrimitiveMoneyLegs)
    principal = legs.principal_legs
    assert len(principal) == 1, "lending action declares exactly one PRINCIPAL leg"
    leg = principal[0]
    assert leg.role is MoneyLegRole.PRINCIPAL
    assert leg.token == "USDC", "token is the resolved chain-truth symbol, not UNKNOWN"
    assert leg.amount.is_measured, "amount is measured (Empty != Zero), not None"
    assert str(leg.amount.value) == expected_amount


def test_euler_borrow_books_loan_token_not_collateral() -> None:
    """A combined euler BORROW (WETH collateral Deposit + USDC Borrow in one tx)
    must declare the LOAN token (USDC) as the principal, NOT the collateral (WETH)
    emitted first. Regression guard for the VIB-5218 'first lending log wins' bug
    that booked the collateral and broke euler borrow/repay accounting on
    base/arbitrum/ethereum (asset==WETH instead of USDC)."""
    vault_map = _euler_vault_underlying_map()
    weth_vault = next(addr for addr, (sym, _dec) in vault_map.items() if sym == "WETH")
    usdc_vault = next(addr for addr, (sym, _dec) in vault_map.items() if sym == "USDC")

    parser = _euler_parser()
    receipt = _euler_borrow_with_collateral_receipt(
        collateral_vault=weth_vault,
        collateral_raw=500_000_000_000_000_000,  # 0.5 WETH
        borrow_vault=usdc_vault,
        borrow_raw=3_000_000,  # 3 USDC
    )
    legs = parser.extract_primitive_money_legs(receipt)
    assert isinstance(legs, PrimitiveMoneyLegs)
    principal = legs.principal_legs
    assert len(principal) == 1
    leg = principal[0]
    assert leg.token == "USDC", "BORROW principal is the loan token, not the WETH collateral"
    assert leg.amount.is_measured
    assert str(leg.amount.value) == "3"


def test_euler_unknown_vault_degrades_to_none() -> None:
    """A vault absent from the static registry yields None (legacy fallback),
    never a token-less leg that would re-create the UNKNOWN bug."""
    parser = _euler_parser()
    receipt = _euler_deposit_receipt(1_000_000, "0xdeadDEADdeadDEADdeadDEADdeadDEADdeadDEAD")
    assert parser.extract_primitive_money_legs(receipt) is None


def test_morpho_unmatched_transfer_degrades_to_none() -> None:
    """No matching loan-token Transfer → None (Empty != Zero), not a guessed leg."""
    parser = _morpho_parser()
    receipt = _morpho_supply_receipt(1_000_000, with_transfer=False)
    assert parser.extract_primitive_money_legs(receipt) is None


def test_non_lending_receipt_degrades_to_none() -> None:
    """A receipt with no lending event yields None for every connector."""
    empty = {"transactionHash": "0x0", "blockNumber": 1, "logs": []}
    assert _spark_parser().extract_primitive_money_legs(empty) is None
    assert _euler_parser().extract_primitive_money_legs(empty) is None
    assert _morpho_parser().extract_primitive_money_legs(empty) is None


# ──────────────────────────────────────────────────────────────────────────────
# End-to-end: enricher surfaces the legs and the ledger dispatcher books token_in
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class _FakeReceipt:
    logs: list
    tx_hash: str = "0xtx"
    block_number: int = 1
    gas_used: int = 1
    status: int = 1

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
    receipt: Any
    success: bool = True
    tx_hash: str = "0xtx"
    gas_used: int = 1


@dataclass
class _FakeExecResult:
    transaction_results: list
    success: bool = True
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)


@dataclass
class _FakeContext:
    chain: str = "ethereum"
    protocol: str | None = "spark"


@dataclass
class _FakeIntent:
    intent_type: str = "SUPPLY"
    protocol: str | None = "spark"


class _PinnedRegistry:
    def __init__(self, parser: Any) -> None:
        self._parser = parser

    def get(self, protocol, chain=None, **kwargs):  # noqa: ARG002
        return self._parser


@pytest.mark.parametrize(
    ("parser_factory", "protocol", "receipt", "expected_in"),
    [
        (_spark_parser, "spark", _spark_supply_receipt(1_500_000), ("USDC", "1.5")),
        (_euler_parser, "euler_v2", _euler_deposit_receipt(2_000_000, _euler_vault()), ("USDC", "2")),
        (_morpho_parser, "morpho_blue", _morpho_supply_receipt(2_500_000), ("USDC", "2.5")),
    ],
)
def test_declared_legs_book_ledger_token_in(parser_factory, protocol, receipt, expected_in) -> None:
    """End-to-end through the enricher + dispatcher: the declared PRINCIPAL leg
    drives ``token_in`` / ``amount_in`` (was empty → ``UNKNOWN`` → cap=0 before)."""
    parser = parser_factory()
    enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
    intent = _FakeIntent(protocol=protocol)
    context = _FakeContext(protocol=protocol)
    result = _FakeExecResult(transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=receipt["logs"]))])
    result = enricher.enrich(result, intent, context)

    legs = result.extracted_data.get("primitive_money_legs")
    assert isinstance(legs, PrimitiveMoneyLegs)
    assert not [w for w in result.extraction_warnings if "primitive_money_legs" in w]

    token_in, token_out, amount_in, amount_out, _, _ = _extract_tokens_and_amounts(intent, result, chain="ethereum")
    assert (token_in, amount_in) == expected_in
    # A one-sided lending principal: out slot stays empty (no fabricated zero).
    assert (token_out, amount_out) == ("", "")
