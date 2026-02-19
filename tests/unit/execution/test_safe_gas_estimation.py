"""Unit tests for SafeSigner._estimate_wrapper_gas method."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from almanak.framework.execution.interfaces import SigningError
from almanak.framework.execution.signer.safe.base import SafeSigner


@pytest.fixture
def mock_web3():
    """Create a mock AsyncWeb3 instance."""
    web3 = AsyncMock()
    web3.eth = AsyncMock()
    return web3


@pytest.fixture
def safe_signer():
    """Create a SafeSigner instance with mocked config and account."""
    with patch("almanak.framework.execution.signer.safe.base.Account") as mock_account_cls:
        mock_account = MagicMock()
        mock_account.address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
        mock_account_cls.from_key.return_value = mock_account

        config = MagicMock()
        config.mode = "direct"
        config.private_key = "0x" + "ab" * 32
        config.gas_buffer_multiplier = 2.0
        config.wallet_config.safe_address = "0x98aE9CE2606e2773eE948178C3a163fdB8194c04"
        config.wallet_config.eoa_address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

        # SafeSigner is abstract; create a minimal concrete subclass
        class ConcreteSafeSigner(SafeSigner):
            async def sign_with_web3(self, tx, web3, eoa_nonce, pos_in_bundle=0):
                raise NotImplementedError

            async def sign_bundle_with_web3(self, txs, web3, eoa_nonce, chain):
                raise NotImplementedError

        return ConcreteSafeSigner(config)


WRAPPER_TX = {
    "from": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
    "to": "0x98aE9CE2606e2773eE948178C3a163fdB8194c04",
    "data": "0xabcdef",
    "value": 0,
}


@pytest.mark.asyncio
async def test_estimate_gas_happy_path(safe_signer, mock_web3):
    """eth_estimateGas succeeds: returns estimated * 1.3."""
    mock_web3.eth.estimate_gas.return_value = 400_000
    result = await safe_signer._estimate_wrapper_gas(mock_web3, WRAPPER_TX, 200_000)
    assert result == int(400_000 * 1.3)
    mock_web3.eth.estimate_gas.assert_awaited_once()


@pytest.mark.asyncio
async def test_fallback_on_transient_error(safe_signer, mock_web3):
    """RPC timeout / connectivity error: falls back to static buffer."""
    mock_web3.eth.estimate_gas.side_effect = ConnectionError("RPC unreachable")
    result = await safe_signer._estimate_wrapper_gas(mock_web3, WRAPPER_TX, 200_000)
    # Fallback = base_gas * (1 + gas_buffer_multiplier) = 200_000 * 3 = 600_000
    assert result == safe_signer.calculate_gas_with_buffer(200_000)


@pytest.mark.asyncio
async def test_raises_on_revert_error(safe_signer, mock_web3):
    """Transaction would revert: raises SigningError instead of falling back."""
    mock_web3.eth.estimate_gas.side_effect = Exception("execution reverted: GS013")
    with pytest.raises(SigningError, match="would revert"):
        await safe_signer._estimate_wrapper_gas(mock_web3, WRAPPER_TX, 200_000)


@pytest.mark.asyncio
async def test_raises_on_revert_keyword(safe_signer, mock_web3):
    """Any error containing 'revert' is treated as a transaction revert."""
    mock_web3.eth.estimate_gas.side_effect = Exception("VM revert: insufficient balance")
    with pytest.raises(SigningError, match="would revert"):
        await safe_signer._estimate_wrapper_gas(mock_web3, WRAPPER_TX, 200_000)


@pytest.mark.asyncio
async def test_fallback_on_non_revert_error(safe_signer, mock_web3):
    """Non-revert error (e.g., timeout): uses buffer fallback."""
    mock_web3.eth.estimate_gas.side_effect = TimeoutError("request timed out")
    result = await safe_signer._estimate_wrapper_gas(mock_web3, WRAPPER_TX, 300_000)
    assert result == safe_signer.calculate_gas_with_buffer(300_000)


@pytest.mark.asyncio
async def test_estimate_zero_returns_buffered_zero(safe_signer, mock_web3):
    """Edge case: estimateGas returns 0 (unlikely but shouldn't crash)."""
    mock_web3.eth.estimate_gas.return_value = 0
    result = await safe_signer._estimate_wrapper_gas(mock_web3, WRAPPER_TX, 100_000)
    assert result == 0  # int(0 * 1.3) = 0
