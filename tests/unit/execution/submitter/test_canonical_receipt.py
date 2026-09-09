"""Canonical inclusion and exact receipt-cost regression evidence."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult
from almanak.framework.execution.interfaces import SubmissionError, TransactionRevertedError
from almanak.framework.execution.submitter.canonical_receipt import wait_for_canonical_receipt
from almanak.framework.execution.submitter.public import PublicMempoolSubmitter
from almanak.framework.observability.ledger import _build_sub_transactions

TX = "0x" + "12" * 32
BLOCK = "0x" + "34" * 32


def raw(**updates):
    return dict(
        transactionHash=TX,
        blockHash=BLOCK,
        blockNumber=1,
        gasUsed=21000,
        effectiveGasPrice=6,
        l1Fee=7,
        status=1,
        logs=[],
        **updates,
    )


def client(receipt=None):
    receipt = receipt or raw()
    return SimpleNamespace(
        eth=SimpleNamespace(
            wait_for_transaction_receipt=AsyncMock(return_value=receipt),
            get_transaction_receipt=AsyncMock(return_value=receipt),
            get_block=AsyncMock(
                return_value={"hash": receipt["blockHash"], "transactions": [receipt["transactionHash"]]}
            ),
        )
    )


def test_refetched_fee_is_authoritative_and_persisted(caplog):
    web3 = client()
    web3.eth.get_transaction_receipt.return_value = {**raw(), "l1Fee": 0, "effectiveGasPrice": 5}
    receipt = asyncio.run(wait_for_canonical_receipt(web3, TX, 1))
    assert receipt.gas_cost_wei == 105000
    assert "receipt_cost_changed" in caplog.text
    evidence = _build_sub_transactions([SimpleNamespace(receipt=receipt, tx_hash=TX, gas_used=21000)])[0]
    assert evidence["receipt_evidence"]["l1_fee_wei"] == "0"
    assert evidence["receipt_evidence"]["effective_gas_price"] == "5"
    assert evidence["receipt_evidence"]["block_hash"] == BLOCK


@pytest.mark.parametrize(
    "field,value",
    [("gasUsed", -1), ("effectiveGasPrice", None), ("l1Fee", -1), ("transactionHash", "0x" + "99" * 32), ("status", 2)],
)
def test_incomplete_or_wrong_receipt_rejected(field, value):
    web3 = client()
    web3.eth.get_transaction_receipt.return_value = {**raw(), field: value}
    with pytest.raises(ValueError):
        asyncio.run(wait_for_canonical_receipt(web3, TX, 1))


@pytest.mark.parametrize(
    "blocks",
    [
        [{"hash": "0x" + "56" * 32, "transactions": [TX]}],
        [{"hash": BLOCK, "transactions": []}],
        [{"hash": BLOCK, "transactions": [TX]}, {"hash": "0x" + "56" * 32, "transactions": [TX]}],
    ],
)
def test_reorg_or_missing_membership_never_returns_receipt(blocks):
    web3 = client()
    web3.eth.get_block.side_effect = blocks + [blocks[-1]] * 10
    with pytest.raises(TimeoutError):
        asyncio.run(wait_for_canonical_receipt(web3, TX, 0.02))


def test_rpc_failure_retains_known_transaction_hash():
    submitter = PublicMempoolSubmitter(rpc_url="https://unused.invalid")
    submitter._web3 = client()
    submitter._web3.eth.get_block.side_effect = RuntimeError("RPC unavailable")
    with pytest.raises(SubmissionError) as raised:
        asyncio.run(submitter.get_receipt(TX, 1))
    assert raised.value.tx_hash == TX


def test_reverted_canonical_receipt_retains_cost_and_identity():
    submitter = PublicMempoolSubmitter(rpc_url="https://unused.invalid")
    submitter._web3 = client({**raw(), "status": 0})
    submitter._extract_revert_reason = AsyncMock(return_value="reverted")
    with pytest.raises(TransactionRevertedError) as raised:
        asyncio.run(submitter.get_receipt(TX, 1))
    assert raised.value.tx_hash == TX
    assert raised.value.receipt.gas_cost_wei == 126007


def test_missing_l1_evidence_is_not_measured_zero():
    receipt = raw()
    del receipt["l1Fee"]
    parsed = asyncio.run(wait_for_canonical_receipt(client(receipt), TX, 1))
    evidence = _build_sub_transactions([SimpleNamespace(receipt=parsed)])[0]["receipt_evidence"]
    assert evidence["l1_fee_wei"] is None


def test_nine_mainnet_receipts_exact_wallet_gas_replay():
    receipts = json.loads((Path(__file__).parent / "fixtures/base_v4_lp_receipts.json").read_text())
    parsed = [asyncio.run(wait_for_canonical_receipt(client(r), r["transactionHash"], 1)) for r in receipts]
    assert len(parsed) == 9
    assert sum(r.gas_cost_wei for r in parsed) == 5362027412005
    gateway_result = GatewayExecutionResult(
        success=True,
        tx_hashes=[r.tx_hash for r in parsed],
        total_gas_used=sum(r.gas_used for r in parsed),
        receipts=[r.to_dict() for r in parsed],
        execution_id="canonical-receipt-replay",
    )
    assert gateway_result.total_gas_cost_wei == 5362027412005
    assert sum(r.receipt.gas_cost_wei for r in gateway_result.transaction_results) == 5362027412005
    evidence = _build_sub_transactions(
        [SimpleNamespace(receipt=r, tx_hash=r.tx_hash, gas_used=r.gas_used) for r in parsed]
    )
    assert sum(int(r["receipt_evidence"]["gas_cost_wei"]) for r in evidence) == 5362027412005


def test_reorg_candidate_retries_under_same_deadline():
    web3 = client()
    web3.eth.get_block.side_effect = [
        {"hash": "0x" + "56" * 32, "transactions": [TX]},
        {"hash": BLOCK, "transactions": [TX]},
        {"hash": BLOCK, "transactions": [TX]},
    ]
    receipt = asyncio.run(wait_for_canonical_receipt(web3, TX, 1))
    assert receipt.block_hash == BLOCK
    assert web3.eth.wait_for_transaction_receipt.await_count == 2
