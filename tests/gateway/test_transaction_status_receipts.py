"""Canonical receipt evidence across the read-only gateway boundary."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import grpc
import pytest
from aiohttp import ClientConnectionError
from hexbytes import HexBytes

from almanak.gateway.data.transaction_status import observe_evm_transaction
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import ExecutionServiceServicer

TX = "0x" + "12" * 32
BLOCK = "0x" + "34" * 32


def client(status=1):
    receipt = {
        "transactionHash": HexBytes(TX),
        "blockHash": HexBytes(BLOCK),
        "blockNumber": 7,
        "status": status,
        "gasUsed": 21_000,
        "effectiveGasPrice": 6,
        "logs": [{"address": "0x" + "56" * 20, "topics": [HexBytes(TX)], "data": HexBytes("0x00")}],
    }
    eth = MagicMock()
    type(eth).chain_id = PropertyMock(side_effect=lambda: AsyncMock(return_value=56)())
    type(eth).block_number = PropertyMock(side_effect=lambda: AsyncMock(return_value=7)())
    eth.wait_for_transaction_receipt = AsyncMock(return_value=receipt)
    eth.get_transaction_receipt = AsyncMock(return_value=receipt)
    eth.get_block = AsyncMock(return_value={"hash": HexBytes(BLOCK), "transactions": [HexBytes(TX)]})
    return SimpleNamespace(eth=eth)


@pytest.mark.asyncio
@pytest.mark.parametrize("status,expected", [(1, "confirmed"), (0, "reverted")])
async def test_canonical_evidence_retains_log_identity_costs_and_reverts(status, expected):
    web3 = client(status)
    result = await observe_evm_transaction(web3, TX, chain_id=56)
    receipt = json.loads(result.canonical_receipt)
    assert result.status == expected
    assert result.confirmations == 0
    assert receipt["status"] == status
    assert receipt["tx_hash"].removeprefix("0x") == TX[2:]
    assert receipt["block_hash"].removeprefix("0x") == BLOCK[2:]
    assert receipt["gas_used"] == 21_000
    assert receipt["effective_gas_price"] == "6"
    assert receipt["l1_fee_wei"] is None
    assert receipt["logs"][0]["topics"] == [TX]
    web3.eth.get_block.assert_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("status", 2), ("gasUsed", -1), ("transactionHash", "0x" + "99" * 32)])
async def test_invalid_evidence_cannot_claim_confirmed_or_reverted(field, value):
    web3 = client()
    web3.eth.get_transaction_receipt.return_value = {
        **web3.eth.get_transaction_receipt.return_value,
        field: value,
    }
    result = await observe_evm_transaction(web3, TX, chain_id=56)
    assert result.status == "unknown"
    assert not result.canonical_receipt


@pytest.mark.asyncio
async def test_chain_mismatch_refuses_receipt_lookup():
    web3 = client()
    result = await observe_evm_transaction(web3, TX, chain_id=1)
    assert result.status == "unknown"
    assert not result.canonical_receipt
    web3.eth.wait_for_transaction_receipt.assert_not_awaited()


@pytest.mark.asyncio
async def test_visibility_timeout_remains_pending_without_receipt(caplog):
    web3 = client()
    web3.eth.get_block.side_effect = ClientConnectionError("https://provider.invalid/secret")
    result = await observe_evm_transaction(web3, TX, chain_id=56, timeout=0.02)
    assert result.status == "pending"
    assert not result.canonical_receipt
    assert "secret" not in caplog.text


@pytest.mark.asyncio
async def test_reorg_does_not_return_the_orphaned_receipt():
    web3 = client()
    web3.eth.get_block.side_effect = [
        {"hash": BLOCK, "transactions": [TX]},
        {"hash": "0x" + "99" * 32, "transactions": [TX]},
    ]
    result = await observe_evm_transaction(web3, TX, chain_id=56, timeout=0.02)
    assert result.status == "pending"
    assert not result.canonical_receipt


@pytest.mark.asyncio
async def test_gateway_routes_validated_chain_and_disconnects_owned_transport():
    service = ExecutionServiceServicer(MagicMock(network="anvil"))
    context = MagicMock()
    provider = MagicMock(disconnect=AsyncMock())
    with (
        patch("web3.AsyncHTTPProvider", return_value=provider),
        patch("web3.AsyncWeb3", return_value=client()),
        patch("almanak.gateway.utils.get_rpc_url", return_value="http://localhost:8545") as resolve,
    ):
        response = await service.GetTransactionStatus(gateway_pb2.TxStatusRequest(tx_hash=TX, chain="bsc"), context)
    assert response.status == "confirmed"
    assert response.canonical_receipt
    resolve.assert_called_once_with("bsc", network="anvil")
    provider.disconnect.assert_awaited_once()
    context.set_code.assert_not_called()


@pytest.mark.asyncio
async def test_cancelled_observation_propagates():
    web3 = client()
    web3.eth.get_block.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await observe_evm_transaction(web3, TX, chain_id=56)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["rpc_url", "provider", "web3", "disconnect"])
async def test_gateway_status_service_failure_is_sanitized_and_unmeasured(failure_stage, caplog):
    service = ExecutionServiceServicer(MagicMock(network="anvil"))
    context = MagicMock()
    provider = MagicMock(disconnect=AsyncMock())
    failure = RuntimeError("https://provider.invalid/secret")
    with (
        patch("almanak.gateway.utils.get_rpc_url", return_value="http://localhost:8545") as resolve,
        patch("web3.AsyncHTTPProvider", return_value=provider) as make_provider,
        patch("web3.AsyncWeb3", return_value=client()) as make_web3,
    ):
        {"rpc_url": resolve, "provider": make_provider, "web3": make_web3, "disconnect": provider.disconnect}[
            failure_stage
        ].side_effect = failure
        response = await service.GetTransactionStatus(gateway_pb2.TxStatusRequest(tx_hash=TX, chain="bsc"), context)
    assert response.status == "unknown"
    assert not response.canonical_receipt
    assert response.error == "canonical_receipt_service_unavailable"
    context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
    context.set_details.assert_called_once_with(response.error)
    assert "secret" not in caplog.text
    if failure_stage in {"web3", "disconnect"}:
        provider.disconnect.assert_awaited_once()
    else:
        provider.disconnect.assert_not_awaited()


@pytest.mark.asyncio
async def test_gateway_cancelled_observation_disconnects_and_propagates():
    service = ExecutionServiceServicer(MagicMock(network="anvil"))
    context = MagicMock()
    provider = MagicMock(disconnect=AsyncMock())
    with (
        patch("almanak.gateway.utils.get_rpc_url", return_value="http://localhost:8545"),
        patch("web3.AsyncHTTPProvider", return_value=provider),
        patch("web3.AsyncWeb3", return_value=client()),
        patch("almanak.gateway.data.transaction_status.observe_evm_transaction", side_effect=asyncio.CancelledError),
        pytest.raises(asyncio.CancelledError),
    ):
        await service.GetTransactionStatus(gateway_pb2.TxStatusRequest(tx_hash=TX, chain="bsc"), context)
    provider.disconnect.assert_awaited_once()
    context.set_code.assert_not_called()
