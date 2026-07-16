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

VIB-5673 update — read before "fixing" a failing assertion here:
    VIB-5673 retuned the L1 / avalanche floors (2.0 / 1.0 gwei → 0.02 gwei)
    and made the floor congestion-relative
    (``max(absolute, 0.05 * base_fee)``). The absolute gwei values below were
    *calibration*, not the VIB-5419 contract, and they moved.

    **The VIB-5419 invariant is "tip > 0 or the tx stalls" — NOT "tip == 2
    gwei".** That invariant is unchanged and is now asserted directly and
    calibration-independently in :class:`TestVib5419InvariantTipIsNeverZero`,
    so it can no longer rot when the floor is re-tuned. Every
    behaviour-preserving property VIB-5419 pinned (L2 zero stays zero, a
    healthy RPC suggestion passes through untouched, ``priority <= max_fee``
    survives capping, both call sites delegate) is likewise unchanged.
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
        # ethereum.py declares min_priority_fee_gwei=0.02 (VIB-5673 retune of
        # the original 2.0). Still a real, non-zero floor.
        assert priority_fee_floor_wei("ethereum") == 0.02 * GWEI

    def test_ethereum_alias_resolves(self):
        assert priority_fee_floor_wei("mainnet") == 0.02 * GWEI

    def test_avalanche_floor(self):
        assert priority_fee_floor_wei("avalanche") == 0.02 * GWEI

    def test_polygon_validator_floor(self):
        # Polygon PoS enforces ~30 gwei min priority fee at the validator layer.
        # VIB-5673 deliberately left this ABSOLUTE: it is a protocol-enforced
        # minimum, not a soft heuristic.
        assert priority_fee_floor_wei("polygon") == 30 * GWEI

    @pytest.mark.parametrize("l2", ["base", "arbitrum", "optimism", "bsc"])
    def test_l2s_have_no_floor(self, l2):
        # L2s declare no live floor (None) → 0; behaviour-preserving.
        assert priority_fee_floor_wei(l2) == 0

    @pytest.mark.parametrize("l2", ["base", "arbitrum", "optimism", "bsc"])
    def test_l2s_get_no_relative_term_either(self, l2):
        """VIB-5673: a chain with no floor policy stays at exactly 0.

        The relative term must NOT leak onto L2s — declaring no floor means
        no floor, so a returned 0 still ships as 0.
        """
        assert priority_fee_floor_wei(l2, base_fee_wei=50 * GWEI) == 0

    def test_unknown_chain_is_zero(self):
        assert priority_fee_floor_wei("not-a-real-chain") == 0


# =============================================================================
# build_eip1559_fees — the floor + max-fee math
# =============================================================================


class TestBuildEip1559Fees:
    def test_rpc_zero_is_floored_on_l1(self):
        """The core bug: node returns 0 → tip floored to a non-zero value.

        VIB-5673: at a congested 30 gwei base the relative term dominates —
        floor = max(0.02, 0.05 * 30) = 1.5 gwei. The tip scales WITH
        congestion, which is exactly when a tip is needed to land.
        """
        fees = build_eip1559_fees(
            base_fee_wei=30 * GWEI,
            rpc_priority_fee_wei=0,
            chain="ethereum",
        )
        assert fees["max_priority_fee_per_gas"] == 1.5 * GWEI
        # max_fee = 2*base + tip = 60 + 1.5 = 61.5 gwei (the old bug gave 60).
        assert fees["max_fee_per_gas"] == 61.5 * GWEI
        assert fees["base_fee_per_gas"] == 30 * GWEI

    def test_rpc_none_is_floored_on_l1(self):
        """RPC raised (None) is treated as 'no suggestion' → floored, not 1 gwei."""
        fees = build_eip1559_fees(
            base_fee_wei=10 * GWEI,
            rpc_priority_fee_wei=None,
            chain="ethereum",
        )
        # floor = max(0.02, 0.05 * 10) = 0.5 gwei
        assert fees["max_priority_fee_per_gas"] == 0.5 * GWEI

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
        # Relative term is 0 at zero base fee → the absolute component holds
        # the line, keeping the tip > 0 (the VIB-5419 invariant).
        assert fees["base_fee_per_gas"] == 0
        assert fees["max_fee_per_gas"] == 0.02 * GWEI
        assert fees["max_priority_fee_per_gas"] == 0.02 * GWEI


# =============================================================================
# The VIB-5419 invariant itself, stated independently of calibration
# =============================================================================


class TestVib5419InvariantTipIsNeverZero:
    """VIB-5419's actual contract: **tip > 0, or the tx stalls**.

    Stated without reference to any particular gwei value, so re-tuning the
    floor (as VIB-5673 did) cannot silently delete the protection. The
    original tests asserted ``tip == 2 gwei``, which conflated the invariant
    with a calibration constant that later became the VIB-5673 overpay bug.

    There is no pending-tx replacement / speed-up path anywhere in
    ``almanak/framework/execution/`` (VIB-69), so the floor carries the whole
    anti-stall burden: a tip of exactly 0 has no recovery path.
    """

    @pytest.mark.parametrize(
        "base_fee_gwei",
        [0, 0.001, 0.09, 0.16, 1, 10, 30, 100, 500],
    )
    @pytest.mark.parametrize("chain", ["ethereum", "avalanche", "polygon"])
    @pytest.mark.parametrize("rpc_suggestion", [0, None])
    def test_tip_is_never_zero_when_node_gives_no_suggestion(self, chain, base_fee_gwei, rpc_suggestion):
        """A node returning 0/None must never yield a tip≈0 tx, at any base fee."""
        fees = build_eip1559_fees(
            base_fee_wei=int(base_fee_gwei * GWEI),
            rpc_priority_fee_wei=rpc_suggestion,
            chain=chain,
        )
        assert fees["max_priority_fee_per_gas"] > 0, (
            f"{chain} at base={base_fee_gwei} gwei with rpc={rpc_suggestion} "
            f"produced tip=0 — VIB-5419 anti-stall protection is GONE."
        )

    @pytest.mark.parametrize("chain", ["ethereum", "avalanche", "polygon"])
    def test_floor_scales_with_congestion(self, chain):
        """VIB-5673: the floor must track base fee, not sit at a constant.

        This is what stops the floor from rotting into a 12.5x overpay the
        next time L1 economics shift.
        """
        quiet = priority_fee_floor_wei(chain, base_fee_wei=int(0.16 * GWEI))
        busy = priority_fee_floor_wei(chain, base_fee_wei=1000 * GWEI)
        assert busy > quiet, f"{chain} floor did not scale with base fee"

    @pytest.mark.parametrize(
        ("chain", "base_fee_gwei"),
        [("ethereum", 0.16), ("avalanche", 0.01), ("polygon", 283.95)],
    )
    def test_floor_never_dominates_max_fee_at_typical_base(self, chain, base_fee_gwei):
        """VIB-5673 regression: the tip must not become most of max_fee.

        At the 2.0 gwei L1 floor the tip was 86% of max_fee. The tip is money
        that is ALWAYS paid, so a tip-dominated max_fee is a direct ~10x
        overpay on every transaction.

        The `negligible` escape hatch matters: on a chain whose base fee is
        ~0 (avalanche, 0.01 gwei) the share test and VIB-5419's "tip > 0"
        invariant are in direct tension — ANY non-zero tip dominates a
        near-zero max_fee. A 0.05 gwei tip costs ~$0.02 on a 400k-gas tx, so
        it cannot be the ~10x overpay this guards. The share rule applies
        where base fee is large enough for the ratio to mean anything.
        """
        fees = build_eip1559_fees(
            base_fee_wei=int(base_fee_gwei * GWEI),
            rpc_priority_fee_wei=0,
            chain=chain,
        )
        tip = fees["max_priority_fee_per_gas"]
        tip_share = tip / fees["max_fee_per_gas"]
        negligible = tip <= 0.05 * GWEI
        assert negligible or tip_share <= 0.25, (
            f"{chain}: tip is {tip_share:.0%} of max_fee ({tip / GWEI} gwei) "
            f"at a typical {base_fee_gwei} gwei base fee — the floor has "
            f"drifted above base fee again (VIB-5673)."
        )


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

        # VIB-5673: floor = max(0.02, 0.05 * 30) = 1.5 gwei.
        assert gas["max_priority_fee_per_gas"] == 1.5 * GWEI
        assert gas["max_fee_per_gas"] == 61.5 * GWEI

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

        # VIB-5673: floor = max(0.02, 0.05 * 30) = 1.5 gwei.
        assert gas["max_priority_fee_per_gas"] == 1.5 * GWEI
        assert gas["max_fee_per_gas"] == 61.5 * GWEI

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
