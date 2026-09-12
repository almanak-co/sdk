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
from almanak.gateway.utils.async_web3_cleanup import drain_failed_client_cleanup

TX = "0x" + "12" * 32
BLOCK = "0x" + "34" * 32


def client(status=1, provider=None):
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
    return SimpleNamespace(eth=eth, middleware_onion=MagicMock(), provider=provider)


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
        patch("web3.AsyncWeb3", return_value=client(provider=provider)),
        patch("almanak.gateway.utils.get_rpc_url", return_value="http://localhost:8545") as resolve,
    ):
        response = await service.GetTransactionStatus(gateway_pb2.TxStatusRequest(tx_hash=TX, chain="bsc"), context)
    assert response.status == "confirmed"
    assert response.canonical_receipt
    resolve.assert_called_once_with("bsc", network="anvil")
    await drain_failed_client_cleanup()
    provider.disconnect.assert_awaited_once()
    context.set_code.assert_not_called()


@pytest.mark.asyncio
async def test_cancelled_observation_propagates():
    web3 = client()
    web3.eth.get_block.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await observe_evm_transaction(web3, TX, chain_id=56)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["rpc_url", "provider", "web3", "middleware", "disconnect"])
async def test_gateway_status_service_failure_is_sanitized_and_unmeasured(failure_stage, caplog):
    service = ExecutionServiceServicer(MagicMock(network="anvil"))
    context = MagicMock()
    provider = MagicMock(disconnect=AsyncMock())
    failure = RuntimeError("https://provider.invalid/secret")
    with (
        patch("almanak.gateway.utils.get_rpc_url", return_value="http://localhost:8545") as resolve,
        patch("web3.AsyncHTTPProvider", return_value=provider) as make_provider,
        patch("web3.AsyncWeb3", return_value=client(provider=provider)) as make_web3,
    ):
        {
            "rpc_url": resolve,
            "provider": make_provider,
            "web3": make_web3,
            "middleware": make_web3.return_value.middleware_onion.inject,
            "disconnect": provider.disconnect,
        }[failure_stage].side_effect = failure
        response = await service.GetTransactionStatus(gateway_pb2.TxStatusRequest(tx_hash=TX, chain="bsc"), context)
    assert response.status == "unknown"
    assert not response.canonical_receipt
    assert response.error == "canonical_receipt_service_unavailable"
    context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
    context.set_details.assert_called_once_with(response.error)
    assert "secret" not in caplog.text
    if failure_stage in {"web3", "middleware", "disconnect"}:
        await drain_failed_client_cleanup()
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
        patch("web3.AsyncWeb3", return_value=client(provider=provider)),
        patch("almanak.gateway.data.transaction_status.observe_evm_transaction", side_effect=asyncio.CancelledError),
        pytest.raises(asyncio.CancelledError),
    ):
        await service.GetTransactionStatus(gateway_pb2.TxStatusRequest(tx_hash=TX, chain="bsc"), context)
    await drain_failed_client_cleanup()
    provider.disconnect.assert_awaited_once()
    context.set_code.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("chain,chain_id,expected", [("bsc", 56, "confirmed"), ("ethereum", 1, "unknown")])
async def test_status_service_formats_actual_web3_poa_blocks_only_for_declared_chains(chain, chain_id, expected):
    from web3 import AsyncHTTPProvider

    provider = AsyncHTTPProvider("https://unused.invalid")
    raw_receipt = {
        "transactionHash": TX,
        "blockHash": BLOCK,
        "blockNumber": "0x7",
        "transactionIndex": "0x0",
        "status": "0x1",
        "gasUsed": "0x5208",
        "effectiveGasPrice": "0x6",
        "logs": [],
    }
    raw_block = {"hash": BLOCK, "number": "0x7", "transactions": [TX], "extraData": "0x" + "ab" * 97}

    async def request(method, params):
        responses = {
            "eth_chainId": hex(chain_id),
            "eth_blockNumber": "0x7",
            "eth_getTransactionReceipt": raw_receipt,
            "eth_getBlockByNumber": raw_block,
        }
        return {"jsonrpc": "2.0", "id": 1, "result": responses[method]}

    service = ExecutionServiceServicer(MagicMock(network="mainnet"))
    with (
        patch.object(provider, "make_request", side_effect=request),
        patch.object(provider, "disconnect", new=AsyncMock()) as disconnect,
        patch("web3.AsyncHTTPProvider", return_value=provider),
        patch("almanak.gateway.utils.get_rpc_url", return_value="https://unused.invalid"),
    ):
        response = await service.GetTransactionStatus(gateway_pb2.TxStatusRequest(tx_hash=TX, chain=chain), MagicMock())
    assert response.status == expected
    if expected == "confirmed":
        receipt = json.loads(response.canonical_receipt)
        assert receipt["tx_hash"].removeprefix("0x") == TX[2:]
        assert receipt["block_hash"].removeprefix("0x") == BLOCK[2:]
    else:
        assert not response.canonical_receipt
    await drain_failed_client_cleanup()
    disconnect.assert_awaited_once()
