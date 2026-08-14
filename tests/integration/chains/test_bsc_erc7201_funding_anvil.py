"""Real-fork coverage for managed Anvil funding of OZ v5 ERC-7201 tokens.

Run explicitly with:
    BSC_RPC_URL=<archive-or-latest-capable-rpc> uv run pytest \
        tests/integration/chains/test_bsc_erc7201_funding_anvil.py \
        -n 0 -v -s --import-mode=importlib
"""

from __future__ import annotations

import os
import shutil
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
import pytest_asyncio

import almanak.framework.anvil.fork_manager as fork_manager_module
from almanak.core.chains.bsc import DESCRIPTOR as BSC_DESCRIPTOR
from almanak.framework.anvil.fork_manager import RollingForkManager

SOURCE_WALLET = "0x1111111111111111111111111111111111111111"
RECIPIENT_WALLET = "0x2222222222222222222222222222222222222222"
ONE_TOKEN = 10**18
TRANSFER_AMOUNT = ONE_TOKEN // 10

ERC7201_TOKENS = {
    "CRCLB": "0x80f3d493ebce97e343c53d29a137942416b4ffc0",
    "TSMB": "0xab78b89b5bb00236be0b4b20704cbfa04efc711c",
    "AMZNB": "0x1a4b499833a79a09ad7cf1d42d7dacf71e92eb00",
}

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(shutil.which("anvil") is None, reason="Anvil is not installed"),
    pytest.mark.timeout(240),
    pytest.mark.xdist_group("bsc_erc7201_funding"),
]


@pytest_asyncio.fixture(scope="module")
async def bsc_fork(unused_tcp_port_factory) -> RollingForkManager:
    """Start one latest-block BSC fork and always terminate it after the test."""
    rpc_url = os.environ.get("BSC_RPC_URL", BSC_DESCRIPTOR.rpc.public_rpc)
    manager = RollingForkManager(
        rpc_url=rpc_url,
        chain="bsc",
        anvil_port=unused_tcp_port_factory(),
        startup_timeout_seconds=90,
        rpc_timeout_seconds=120,
        cache_path=None,
    )

    try:
        if not await manager.start():
            pytest.skip("Anvil could not start a BSC fork; check BSC_RPC_URL and network access")
        yield manager
    finally:
        await manager.stop()


def _encode_transfer(recipient: str, amount: int) -> str:
    """Encode ERC-20 transfer(address,uint256) without introducing an ABI dependency."""
    recipient_word = recipient.removeprefix("0x").lower().zfill(64)
    amount_word = amount.to_bytes(32, "big").hex()
    return f"0xa9059cbb{recipient_word}{amount_word}"


async def _transfer_and_get_receipt(
    manager: RollingForkManager,
    token_address: str,
    amount: int,
) -> dict[str, Any]:
    sent, tx_hash = await manager._rpc_call_raw(
        "eth_sendTransaction",
        [
            {
                "from": SOURCE_WALLET,
                "to": token_address,
                "data": _encode_transfer(RECIPIENT_WALLET, amount),
                "gas": hex(500_000),
            }
        ],
    )
    assert sent and isinstance(tx_hash, str), f"transfer submission failed for {token_address}"

    mined, _ = await manager._rpc_call_raw("evm_mine", [])
    assert mined, f"failed to mine transfer for {token_address}"

    receipt_ok, receipt = await manager._rpc_call_raw("eth_getTransactionReceipt", [tx_hash])
    assert receipt_ok and isinstance(receipt, dict), f"missing transfer receipt for {token_address}"
    return receipt


@pytest.mark.asyncio
async def test_managed_funding_and_transfer_for_bsc_erc7201_tokens(bsc_fork: RollingForkManager):
    """All reported tokens fund through the public path and remain transferable."""
    assert await bsc_fork.fund_wallet(SOURCE_WALLET, Decimal("1"))

    original_rpc_call_raw = bsc_fork._rpc_call_raw

    async def rpc_call_raw_without_deal(
        method: str,
        params: list[Any],
        timeout_override: float | None = None,
    ) -> tuple[bool, Any]:
        if method == "anvil_dealERC20":
            return False, None
        return await original_rpc_call_raw(method, params, timeout_override=timeout_override)

    with patch.object(bsc_fork, "_rpc_call_raw", side_effect=rpc_call_raw_without_deal) as rpc_mock:
        failed = await bsc_fork.fund_tokens_report(
            SOURCE_WALLET,
            {token_address: Decimal("1") for token_address in ERC7201_TOKENS.values()},
        )

    storage_key = bsc_fork._calculate_mapping_slot(
        SOURCE_WALLET,
        fork_manager_module._OPENZEPPELIN_ERC20_STORAGE_LOCATION,
    )
    storage_calls = [call for call in rpc_mock.await_args_list if call.args[0] == "anvil_setStorageAt"]
    for symbol, token_address in ERC7201_TOKENS.items():
        assert any(call.args[1][:2] == [token_address, storage_key] for call in storage_calls), (
            f"{symbol} was not funded through the ERC-7201 storage key"
        )
    assert failed == []

    for symbol, token_address in ERC7201_TOKENS.items():
        storage_ok, stored_balance = await bsc_fork._rpc_call_raw(
            "eth_getStorageAt",
            [token_address, storage_key, "latest"],
        )
        assert storage_ok and int(stored_balance, 16) == ONE_TOKEN, f"{symbol} ERC-7201 storage mismatch"
        assert await bsc_fork._get_token_balance(token_address, SOURCE_WALLET) == ONE_TOKEN

        receipt = await _transfer_and_get_receipt(bsc_fork, token_address, TRANSFER_AMOUNT)
        assert int(receipt["status"], 16) == 1, f"{symbol} transfer reverted"
        assert await bsc_fork._get_token_balance(token_address, RECIPIENT_WALLET) == TRANSFER_AMOUNT
        assert await bsc_fork._get_token_balance(token_address, SOURCE_WALLET) == ONE_TOKEN - TRANSFER_AMOUNT
