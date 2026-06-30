"""VIB-5419: live EIP-1559 priority-fee floor.

Both live gas builders (``orchestrator.get_gas_price`` and
``chain_executor.get_gas_params``) used to floor the miner tip to 1 gwei
**only when the RPC raised**, never when the node returned a legitimate
``0`` (common on Ethereum L1). The tx then shipped with ``tip≈0`` and
``maxFee = 2·base_fee`` — which stalls / drops when the base fee rises.

These tests prove the shared :func:`build_eip1559_fees` helper floors the
tip to the per-chain descriptor value (``min_priority_fee_gwei``) on a
returned ``0`` *and* on an RPC exception, that a high RPC suggestion is left
untouched, that L2s (floor 0) are behaviour-preserving, and that **both**
production call sites delegate to the shared helper.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.gas.fees import (
    build_eip1559_fees,
    priority_fee_floor_wei,
)

GWEI = 10**9


# =============================================================================
# priority_fee_floor_wei — per-chain descriptor floor
# =============================================================================


class TestPriorityFeeFloorWei:
    def test_ethereum_l1_has_a_real_floor(self):
        # ethereum.py declares min_priority_fee_gwei=2.0
        assert priority_fee_floor_wei("ethereum") == 2 * GWEI

    def test_ethereum_alias_resolves(self):
        assert priority_fee_floor_wei("mainnet") == 2 * GWEI

    def test_avalanche_floor(self):
        assert priority_fee_floor_wei("avalanche") == 1 * GWEI

    def test_polygon_validator_floor(self):
        # Polygon PoS enforces ~30 gwei min priority fee at the validator layer.
        assert priority_fee_floor_wei("polygon") == 30 * GWEI

    @pytest.mark.parametrize("l2", ["base", "arbitrum", "optimism", "bsc"])
    def test_l2s_have_no_floor(self, l2):
        # L2s declare no live floor (None) → 0; behaviour-preserving.
        assert priority_fee_floor_wei(l2) == 0

    def test_unknown_chain_is_zero(self):
        assert priority_fee_floor_wei("not-a-real-chain") == 0


# =============================================================================
# build_eip1559_fees — the floor + max-fee math
# =============================================================================


class TestBuildEip1559Fees:
    def test_rpc_zero_is_floored_on_l1(self):
        """The core bug: node returns 0 → tip floored to the descriptor value."""
        fees = build_eip1559_fees(
            base_fee_wei=30 * GWEI,
            rpc_priority_fee_wei=0,
            chain="ethereum",
        )
        assert fees["max_priority_fee_per_gas"] == 2 * GWEI
        # max_fee = 2*base + tip = 60 + 2 = 62 gwei (the old bug gave 60).
        assert fees["max_fee_per_gas"] == 62 * GWEI
        assert fees["base_fee_per_gas"] == 30 * GWEI

    def test_rpc_none_is_floored_on_l1(self):
        """RPC raised (None) is treated as 'no suggestion' → floored, not 1 gwei."""
        fees = build_eip1559_fees(
            base_fee_wei=10 * GWEI,
            rpc_priority_fee_wei=None,
            chain="ethereum",
        )
        assert fees["max_priority_fee_per_gas"] == 2 * GWEI

    def test_high_rpc_suggestion_untouched_on_l1(self):
        """A node suggestion above the floor passes through unchanged."""
        fees = build_eip1559_fees(
            base_fee_wei=10 * GWEI,
            rpc_priority_fee_wei=5 * GWEI,
            chain="ethereum",
        )
        assert fees["max_priority_fee_per_gas"] == 5 * GWEI
        assert fees["max_fee_per_gas"] == 25 * GWEI

    def test_l2_zero_stays_zero(self):
        """L2 floor is 0 → a returned 0 stays 0 (behaviour-preserving)."""
        fees = build_eip1559_fees(
            base_fee_wei=1 * GWEI,
            rpc_priority_fee_wei=0,
            chain="base",
        )
        assert fees["max_priority_fee_per_gas"] == 0
        assert fees["max_fee_per_gas"] == 2 * GWEI

    def test_l2_none_stays_zero(self):
        """On an L2, an RPC exception floors to 0, not the legacy 1 gwei."""
        fees = build_eip1559_fees(
            base_fee_wei=1 * GWEI,
            rpc_priority_fee_wei=None,
            chain="arbitrum",
        )
        assert fees["max_priority_fee_per_gas"] == 0

    def test_zero_base_fee(self):
        fees = build_eip1559_fees(
            base_fee_wei=0,
            rpc_priority_fee_wei=0,
            chain="ethereum",
        )
        assert fees["base_fee_per_gas"] == 0
        assert fees["max_fee_per_gas"] == 2 * GWEI
        assert fees["max_priority_fee_per_gas"] == 2 * GWEI


# =============================================================================
# Both production sites delegate to the shared helper
# =============================================================================


def _make_mock_web3(*, base_fee_wei: int, rpc_priority_fee_wei: int):
    web3 = MagicMock()
    web3.eth.get_block = AsyncMock(return_value={"baseFeePerGas": base_fee_wei})
    # `await web3.eth.max_priority_fee` — the property returns an awaitable.
    web3.eth.max_priority_fee = AsyncMock(return_value=rpc_priority_fee_wei)()
    return web3


class TestOrchestratorUsesSharedHelper:
    def _orchestrator(self, chain: str):
        from almanak.framework.execution.orchestrator import ExecutionOrchestrator

        signer = MagicMock()
        signer.address = "0x1234567890abcdef1234567890abcdef12345678"
        return ExecutionOrchestrator(
            signer=signer,
            submitter=MagicMock(),
            simulator=MagicMock(),
            chain=chain,
        )

    @pytest.mark.asyncio
    async def test_l1_zero_priority_is_floored(self):
        orch = self._orchestrator("ethereum")
        orch._web3 = _make_mock_web3(base_fee_wei=30 * GWEI, rpc_priority_fee_wei=0)

        gas = await orch.get_gas_price()

        assert gas["max_priority_fee_per_gas"] == 2 * GWEI
        assert gas["max_fee_per_gas"] == 62 * GWEI

    @pytest.mark.asyncio
    async def test_l2_zero_priority_preserved(self):
        orch = self._orchestrator("base")
        orch._web3 = _make_mock_web3(base_fee_wei=1 * GWEI, rpc_priority_fee_wei=0)

        gas = await orch.get_gas_price()

        assert gas["max_priority_fee_per_gas"] == 0

    @pytest.mark.asyncio
    async def test_delegates_to_helper(self):
        orch = self._orchestrator("ethereum")
        orch._web3 = _make_mock_web3(base_fee_wei=10 * GWEI, rpc_priority_fee_wei=0)

        with patch(
            "almanak.framework.execution.orchestrator.build_eip1559_fees",
            wraps=build_eip1559_fees,
        ) as spy:
            await orch.get_gas_price()

        spy.assert_called_once()
        assert spy.call_args.kwargs["chain"] == "ethereum"
        assert spy.call_args.kwargs["rpc_priority_fee_wei"] == 0


class TestChainExecutorUsesSharedHelper:
    def _executor(self, chain: str, max_gas_price_gwei: int = 1000):
        from almanak.framework.execution.chain_executor import ChainExecutor

        return ChainExecutor(
            chain=chain,
            rpc_url="https://example.com",
            private_key="0x" + "ab" * 32,
            max_gas_price_gwei=max_gas_price_gwei,
        )

    @pytest.mark.asyncio
    async def test_l1_zero_priority_is_floored(self):
        executor = self._executor("ethereum")
        web3 = _make_mock_web3(base_fee_wei=30 * GWEI, rpc_priority_fee_wei=0)

        async def fake_get_web3():
            return web3

        with patch.object(executor, "_get_web3", side_effect=fake_get_web3):
            gas = await executor.get_gas_params()

        assert gas["max_priority_fee_per_gas"] == 2 * GWEI
        assert gas["max_fee_per_gas"] == 62 * GWEI

    @pytest.mark.asyncio
    async def test_l2_zero_priority_preserved(self):
        executor = self._executor("base")
        web3 = _make_mock_web3(base_fee_wei=1 * GWEI, rpc_priority_fee_wei=0)

        async def fake_get_web3():
            return web3

        with patch.object(executor, "_get_web3", side_effect=fake_get_web3):
            gas = await executor.get_gas_params()

        assert gas["max_priority_fee_per_gas"] == 0

    @pytest.mark.asyncio
    async def test_floor_then_cap_keeps_eip1559_invariant(self):
        """Floored tip is still clamped to a low cap (priority <= max_fee)."""
        executor = self._executor("ethereum", max_gas_price_gwei=1)
        web3 = _make_mock_web3(base_fee_wei=30 * GWEI, rpc_priority_fee_wei=0)

        async def fake_get_web3():
            return web3

        with patch.object(executor, "_get_web3", side_effect=fake_get_web3):
            gas = await executor.get_gas_params()

        assert gas["max_fee_per_gas"] == 1 * GWEI  # capped
        assert gas["max_priority_fee_per_gas"] <= gas["max_fee_per_gas"]

    @pytest.mark.asyncio
    async def test_delegates_to_helper(self):
        executor = self._executor("ethereum")
        web3 = _make_mock_web3(base_fee_wei=10 * GWEI, rpc_priority_fee_wei=0)

        async def fake_get_web3():
            return web3

        with (
            patch.object(executor, "_get_web3", side_effect=fake_get_web3),
            patch(
                "almanak.framework.execution.chain_executor.build_eip1559_fees",
                wraps=build_eip1559_fees,
            ) as spy,
        ):
            await executor.get_gas_params()

        spy.assert_called_once()
        assert spy.call_args.kwargs["chain"] == "ethereum"
