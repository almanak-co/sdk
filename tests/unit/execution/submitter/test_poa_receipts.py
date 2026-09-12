"""Exercise canonical inclusion with Web3's real block-formatting middleware."""

from unittest.mock import AsyncMock, patch

import pytest
from web3 import AsyncHTTPProvider

from almanak.framework.execution.interfaces import SubmissionError
from almanak.framework.execution.submitter.public import PublicMempoolSubmitter
from almanak.gateway.utils.async_web3_cleanup import drain_failed_client_cleanup

TX = "0x" + "12" * 32
BLOCK = "0x" + "34" * 32


def landed_provider(chain_id: int, extra_data_bytes: int):
    provider = AsyncHTTPProvider("https://unused.invalid")
    receipt = {
        "transactionHash": TX,
        "blockHash": BLOCK,
        "blockNumber": "0x7",
        "transactionIndex": "0x0",
        "status": "0x1",
        "gasUsed": "0x5208",
        "effectiveGasPrice": "0x6",
        "logs": [],
    }
    block = {"hash": BLOCK, "number": "0x7", "transactions": [TX], "extraData": "0x" + "ab" * extra_data_bytes}

    async def request(method, params):
        return {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "eth_chainId": hex(chain_id),
                "eth_getTransactionReceipt": receipt,
                "eth_getBlockByNumber": block,
            }[method],
        }

    provider.make_request = AsyncMock(side_effect=request)
    return provider


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chain,chain_id,extra_data_bytes",
    [
        ("bsc", 56, 97),
        ("bnb", 56, 97),
        ("polygon", 137, 97),
        ("avalanche", 43114, 97),
        ("optimism", 10, 97),
        ("ethereum", 1, 32),
        ("base", 8453, 32),
    ],
)
async def test_landed_receipt_confirmed_through_actual_submitter(chain, chain_id, extra_data_bytes):
    provider = landed_provider(chain_id, extra_data_bytes)
    with patch("web3.AsyncHTTPProvider", return_value=provider):
        submitter = PublicMempoolSubmitter("https://unused.invalid", chain=chain)
        receipt = await submitter.get_receipt(TX, timeout=1)
    assert receipt.status == 1
    assert receipt.tx_hash.removeprefix("0x") == TX[2:]
    assert receipt.block_hash.removeprefix("0x") == BLOCK[2:]
    assert receipt.gas_used == 21_000
    assert receipt.gas_cost_wei == 126_000
    methods = [call.args[0] for call in provider.make_request.call_args_list]
    assert methods == [
        "eth_getTransactionReceipt",
        "eth_getBlockByNumber",
        "eth_getTransactionReceipt",
        "eth_getBlockByNumber",
    ]


@pytest.mark.asyncio
async def test_missing_injection_reproduces_landed_transaction_reported_retryable():
    provider = landed_provider(56, 97)
    with (
        patch("web3.AsyncHTTPProvider", return_value=provider),
        patch("almanak.gateway.utils.rpc_provider.inject_poa_middleware"),
    ):
        submitter = PublicMempoolSubmitter("https://unused.invalid", chain="bsc")
        with pytest.raises(SubmissionError, match="extraData") as caught:
            await submitter.get_receipt(TX, timeout=1)
    assert caught.value.recoverable is True
    assert caught.value.tx_hash == TX


@pytest.mark.asyncio
async def test_legacy_submitter_resolves_chain_before_receipt_observation():
    provider = landed_provider(56, 97)
    with patch("web3.AsyncHTTPProvider", return_value=provider):
        submitter = PublicMempoolSubmitter("https://unused.invalid")
        receipt = await submitter.get_receipt(TX, timeout=1)
    assert receipt.status == 1
    assert provider.make_request.call_args_list[0].args[0] == "eth_chainId"


@pytest.mark.asyncio
async def test_chain_discovery_transport_failure_preserves_submitted_hash():
    provider = landed_provider(56, 97)
    provider.make_request = AsyncMock(side_effect=OSError("endpoint unavailable"))
    with (
        patch("web3.AsyncHTTPProvider", return_value=provider),
        patch.object(provider, "disconnect", new=AsyncMock()) as disconnect,
    ):
        submitter = PublicMempoolSubmitter("https://unused.invalid")
        with pytest.raises(SubmissionError, match="endpoint unavailable") as caught:
            await submitter.get_receipt(TX, timeout=1)
    assert caught.value.recoverable is True
    assert caught.value.tx_hash == TX
    assert submitter._web3 is None
    await drain_failed_client_cleanup()
    disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_chain_discovery_timeout_cancels_request_and_closes_owned_transport():
    import asyncio

    provider = landed_provider(56, 97)
    cancelled = asyncio.Event()

    async def request(method, params):
        assert method == "eth_chainId"
        try:
            await asyncio.sleep(1)
            raise AssertionError("Chain discovery exceeded the receipt deadline")
        except asyncio.CancelledError:
            cancelled.set()
            raise

    provider.make_request = AsyncMock(side_effect=request)
    with (
        patch("web3.AsyncHTTPProvider", return_value=provider),
        patch.object(provider, "disconnect", new=AsyncMock()) as disconnect,
    ):
        submitter = PublicMempoolSubmitter("https://unused.invalid")
        with pytest.raises(SubmissionError, match="Timeout waiting") as caught:
            await submitter.get_receipt(TX, timeout=0.1)
    assert cancelled.is_set()
    assert caught.value.recoverable is True
    assert caught.value.tx_hash == TX
    assert submitter._web3 is None
    await drain_failed_client_cleanup()
    disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_chain_discovery_and_canonical_proof_share_one_receipt_deadline():
    import asyncio

    provider = landed_provider(56, 97)
    original_request = provider.make_request.side_effect
    chain_resolved = asyncio.Event()
    receipt_requested = asyncio.Event()

    async def request(method, params):
        if method == "eth_chainId":
            await asyncio.sleep(0.1)
            chain_resolved.set()
        elif method == "eth_getTransactionReceipt" and not receipt_requested.is_set():
            receipt_requested.set()
            await asyncio.sleep(0.2)
        return await original_request(method, params)

    provider.make_request = AsyncMock(side_effect=request)
    with patch("web3.AsyncHTTPProvider", return_value=provider):
        submitter = PublicMempoolSubmitter("https://unused.invalid")
        with pytest.raises(SubmissionError, match="Timeout waiting") as caught:
            await submitter.get_receipt(TX, timeout=0.25)
    assert chain_resolved.is_set()
    assert receipt_requested.is_set()
    assert caught.value.recoverable is True
    assert caught.value.tx_hash == TX


@pytest.mark.asyncio
async def test_concurrent_legacy_receipt_reads_share_one_initialized_transport():
    import asyncio

    provider = landed_provider(56, 97)
    original_request = provider.make_request.side_effect

    async def request(method, params):
        if method == "eth_chainId":
            await asyncio.sleep(0.01)
        return await original_request(method, params)

    provider.make_request = AsyncMock(side_effect=request)
    with patch("web3.AsyncHTTPProvider", return_value=provider) as create_provider:
        submitter = PublicMempoolSubmitter("https://unused.invalid")
        receipts = await submitter.get_receipts([TX, TX], timeout=1)
    assert len(receipts) == 2
    assert all(receipt.status == 1 for receipt in receipts)
    create_provider.assert_called_once()
    assert sum(call.args[0] == "eth_chainId" for call in provider.make_request.call_args_list) == 1
