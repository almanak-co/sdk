"""Tests for ExecutionOrchestrator._check_token_balance_before_submit.

Covers all branches: intent type filtering, metadata validation, native token
skip, malformed amount, missing RPC URL, RPC errors (fail-open), sufficient
balance, and insufficient balance (InsufficientFundsError).
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.core.models.action_bundle import ActionBundle
from almanak.framework.execution.interfaces import InsufficientFundsError
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator

WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
TOKEN_ADDR = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


def _make_orchestrator(rpc_url: str = "http://localhost:8545") -> ExecutionOrchestrator:
    """Create a minimal orchestrator for testing."""
    orch = ExecutionOrchestrator.__new__(ExecutionOrchestrator)
    orch.rpc_url = rpc_url
    orch.chain = "arbitrum"
    orch._web3 = None
    return orch


def _make_context() -> ExecutionContext:
    return ExecutionContext(wallet_address=WALLET)


def _make_bundle(
    intent_type: str = "SWAP",
    from_token: dict | None = None,
    amount_in: str | None = None,
) -> ActionBundle:
    bundle = MagicMock(spec=ActionBundle)
    bundle.intent_type = intent_type
    metadata = {}
    if from_token is not None:
        metadata["from_token"] = from_token
    if amount_in is not None:
        metadata["amount_in"] = amount_in
    bundle.metadata = metadata
    return bundle


def _balance_bytes(amount: int) -> bytes:
    return amount.to_bytes(32, "big")


class TestCheckTokenBalanceBeforeSubmit:
    """Unit tests for _check_token_balance_before_submit."""

    @pytest.mark.asyncio
    async def test_non_swap_intent_returns_immediately(self):
        orch = _make_orchestrator()
        bundle = _make_bundle(intent_type="HOLD")
        # Should return without any RPC call
        await orch._check_token_balance_before_submit(bundle, _make_context())

    @pytest.mark.asyncio
    async def test_lowercase_swap_intent_is_recognized(self):
        orch = _make_orchestrator()
        bundle = _make_bundle(
            intent_type="swap",
            from_token={"address": TOKEN_ADDR, "symbol": "USDC"},
            amount_in="1000000",
        )
        mock_web3 = AsyncMock()
        mock_web3.eth.call = AsyncMock(return_value=_balance_bytes(2000000))
        mock_web3.to_checksum_address = lambda addr: addr
        with patch.object(orch, "_get_web3", return_value=mock_web3):
            await orch._check_token_balance_before_submit(bundle, _make_context())

    @pytest.mark.asyncio
    async def test_missing_from_token_returns(self):
        orch = _make_orchestrator()
        bundle = _make_bundle(intent_type="SWAP", amount_in="1000")
        await orch._check_token_balance_before_submit(bundle, _make_context())

    @pytest.mark.asyncio
    async def test_missing_amount_in_returns(self):
        orch = _make_orchestrator()
        bundle = _make_bundle(
            intent_type="SWAP",
            from_token={"address": TOKEN_ADDR, "symbol": "USDC"},
        )
        await orch._check_token_balance_before_submit(bundle, _make_context())

    @pytest.mark.asyncio
    async def test_native_token_skips_check(self):
        orch = _make_orchestrator()
        bundle = _make_bundle(
            intent_type="SWAP",
            from_token={"address": TOKEN_ADDR, "is_native": True},
            amount_in="1000",
        )
        await orch._check_token_balance_before_submit(bundle, _make_context())

    @pytest.mark.asyncio
    async def test_malformed_amount_in_returns(self):
        orch = _make_orchestrator()
        bundle = _make_bundle(
            intent_type="SWAP",
            from_token={"address": TOKEN_ADDR, "symbol": "USDC"},
            amount_in="not_a_number",
        )
        await orch._check_token_balance_before_submit(bundle, _make_context())

    @pytest.mark.asyncio
    async def test_no_rpc_url_returns(self):
        orch = _make_orchestrator(rpc_url="")
        bundle = _make_bundle(
            intent_type="SWAP",
            from_token={"address": TOKEN_ADDR, "symbol": "USDC"},
            amount_in="1000000",
        )
        await orch._check_token_balance_before_submit(bundle, _make_context())

    @pytest.mark.asyncio
    async def test_rpc_timeout_fail_open(self):
        orch = _make_orchestrator()
        bundle = _make_bundle(
            intent_type="SWAP",
            from_token={"address": TOKEN_ADDR, "symbol": "USDC"},
            amount_in="1000000",
        )
        mock_web3 = AsyncMock()
        mock_web3.eth.call = AsyncMock(side_effect=asyncio.TimeoutError())
        mock_web3.to_checksum_address = lambda addr: addr
        with patch.object(orch, "_get_web3", return_value=mock_web3):
            # Should NOT raise - fail open
            await orch._check_token_balance_before_submit(bundle, _make_context())

    @pytest.mark.asyncio
    async def test_rpc_error_fail_open(self):
        orch = _make_orchestrator()
        bundle = _make_bundle(
            intent_type="SWAP",
            from_token={"address": TOKEN_ADDR, "symbol": "USDC"},
            amount_in="1000000",
        )
        mock_web3 = AsyncMock()
        mock_web3.eth.call = AsyncMock(side_effect=ConnectionError("RPC down"))
        mock_web3.to_checksum_address = lambda addr: addr
        with patch.object(orch, "_get_web3", return_value=mock_web3):
            await orch._check_token_balance_before_submit(bundle, _make_context())

    @pytest.mark.asyncio
    async def test_sufficient_balance_passes(self):
        orch = _make_orchestrator()
        bundle = _make_bundle(
            intent_type="SWAP",
            from_token={"address": TOKEN_ADDR, "symbol": "USDC"},
            amount_in="1000000",
        )
        mock_web3 = AsyncMock()
        mock_web3.eth.call = AsyncMock(return_value=_balance_bytes(2000000))
        mock_web3.to_checksum_address = lambda addr: addr
        with patch.object(orch, "_get_web3", return_value=mock_web3):
            await orch._check_token_balance_before_submit(bundle, _make_context())

    @pytest.mark.asyncio
    async def test_insufficient_balance_raises(self):
        orch = _make_orchestrator()
        bundle = _make_bundle(
            intent_type="SWAP",
            from_token={"address": TOKEN_ADDR, "symbol": "USDC"},
            amount_in="1000000",
        )
        mock_web3 = AsyncMock()
        mock_web3.eth.call = AsyncMock(return_value=_balance_bytes(500))
        mock_web3.to_checksum_address = lambda addr: addr
        with patch.object(orch, "_get_web3", return_value=mock_web3):
            with pytest.raises(InsufficientFundsError) as exc_info:
                await orch._check_token_balance_before_submit(bundle, _make_context())
            assert exc_info.value.required == 1000000
            assert exc_info.value.available == 500
            assert exc_info.value.token == "USDC"

    @pytest.mark.asyncio
    async def test_insufficient_balance_default_symbol(self):
        orch = _make_orchestrator()
        bundle = _make_bundle(
            intent_type="SWAP",
            from_token={"address": TOKEN_ADDR},  # no symbol
            amount_in="1000000",
        )
        mock_web3 = AsyncMock()
        mock_web3.eth.call = AsyncMock(return_value=_balance_bytes(0))
        mock_web3.to_checksum_address = lambda addr: addr
        with patch.object(orch, "_get_web3", return_value=mock_web3):
            with pytest.raises(InsufficientFundsError) as exc_info:
                await orch._check_token_balance_before_submit(bundle, _make_context())
            assert exc_info.value.token == "ERC20"
