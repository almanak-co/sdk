"""Opt-in, read-only canonical receipt acceptance against BSC mainnet."""

import os
from unittest.mock import patch

import pytest

from almanak.framework.execution.interfaces import SubmissionError
from almanak.framework.execution.submitter.public import PublicMempoolSubmitter
from almanak.gateway.utils.rpc_provider import get_rpc_url

TX = "0xaeef324215ec2cabdcd9c17aac7c6632d95ff181758b8b9dfd590b0d179a8a08"


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get("RUN_MAINNET_RECEIPT_TEST") != "1", reason="Opt in to read-only BSC mainnet RPC")
async def test_canonical_mainnet_receipt_with_poa_negative_control():
    rpc_url = get_rpc_url("bsc", network="mainnet")
    broken = PublicMempoolSubmitter(rpc_url, chain="bsc")
    try:
        with patch("almanak.gateway.utils.rpc_provider.inject_poa_middleware"):
            with pytest.raises(SubmissionError, match="extraData") as caught:
                await broken.get_receipt(TX, timeout=30)
        assert caught.value.recoverable is True
        assert caught.value.tx_hash == TX
    finally:
        if broken._web3 is not None:
            await broken._web3.provider.disconnect()

    fixed = PublicMempoolSubmitter(rpc_url, chain="bsc")
    try:
        receipt = await fixed.get_receipt(TX, timeout=30)
        assert receipt.status == 1
        assert receipt.tx_hash.removeprefix("0x") == TX[2:]
        assert receipt.block_number == 120868826
        assert receipt.gas_used == 270478
        assert receipt.gas_cost_wei > 0
        print(
            f"canonical_receipt tx={TX} block={receipt.block_number} block_hash={receipt.block_hash} "
            f"status={receipt.status} gas_used={receipt.gas_used} gas_cost_wei={receipt.gas_cost_wei}"
        )
    finally:
        if fixed._web3 is not None:
            await fixed._web3.provider.disconnect()
