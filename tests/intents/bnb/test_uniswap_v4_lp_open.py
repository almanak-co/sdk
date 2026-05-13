"""4-layer intent tests for Uniswap V4 LP_OPEN on BNB Chain Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
opening concentrated liquidity positions via V4 PositionManager on BNB:
1. Create LPOpenIntent with pool, amounts, and price range
2. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV4ReceiptParser (position_id + liquidity)
5. Verify balances changed correctly (tokens deposited into pool)

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

Pool choice: NATIVE_BNB/USDT fee=500, tick_spacing=10. Probed against BNB
mainnet on 2026-05-14:
sqrtPriceX96=2.055e30 (~673 USDT per BNB), liquidity=2.10e23. The fee=500
tier carries ~38x the liquidity of fee=3000 on the native-keyed BNB/USDT
pool, so use it. The adapter resolves the "BNB" symbol via
``_resolve_token(..., for_v4_pool=True)`` to ``address(0)`` (NATIVE_CURRENCY),
so the pool key uses the native sentinel. The PositionManager.modifyLiquidities
TX carries the LP value as ``msg.value`` and SETTLE_PAIR refunds excess to
the wallet.

BSC USDT (and USDC) are 18-decimal Binance-Peg tokens — unlike most chains
where USDT/USDC are 6-decimal. The adapter resolves decimals via the token
resolver so the wei math works without any test-side override.

To run:
    uv run pytest tests/intents/bnb/test_uniswap_v4_lp_open.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType, LPOpenIntent
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

pytestmark = pytest.mark.no_zodiac(
    reason="VIB-4343: uniswap_v4 not yet in synthetic_intents matrix"
)

# =============================================================================
# Test Configuration
# =============================================================================

# Framework canonical chain name for BNB Chain (matches the per-chain conftest
# and the per-chain price-oracle fixture). The intent's ``chain=`` param is
# normalised to "bsc" by ``resolve_chain_name`` regardless of whether the
# caller writes "bnb" or "bsc".
CHAIN_NAME = "bsc"

# BNB/USDT pool with 0.05% fee tier (500), tick spacing 10.
# This is the native-keyed V4 pool — pool key currency0=address(0). Probed
# against BSC mainnet on 2026-05-14: sqrtPriceX96≈2.055e30 (mid-price ~673
# USDT per BNB) and liquidity≈2.10e23 — by far the deepest native-keyed BNB
# pool (fee=500 is the dominant tier on BSC). Picking BNB over WBNB forces
# the adapter into ``for_v4_pool=True`` -> NATIVE_CURRENCY substitution,
# matching the native-key path used by the avalanche sibling (VIB-4367) and
# avoiding the VIB-4413 ERC20<>ERC20 revert.
LP_POOL = "BNB/USDT/500"

# Modest amounts sized so the BNB-side deposit is big enough that the
# gas-headroom-subtracted lower bound below is meaningfully positive even
# when gas spend is at the BSC upper end (~5 mBNB for ~5 TXs). funded_wallet
# is seeded by the bnb conftest with 100 native BNB and 100,000 USDT, so
# this is well inside the funding envelope (~0.1% of the seed).
LP_AMOUNT_BNB = Decimal("0.1")    # ~$67 of BNB at fork prices (~$670/BNB)
LP_AMOUNT_USDT = Decimal("100")   # ~$100 of USDT (18 decimals on BSC)

# Wide price range centred around the ~673 USDT/BNB mid-price. Roughly 7x
# below and ~3x above ensures the position is unambiguously in-range and
# both tokens must be deposited (no-op guard). Units are token1/token0 =
# USDT-per-BNB (BNB is currency0 as address(0) < any ERC20 address).
LP_RANGE_LOWER = Decimal("100")   # 100 USDT per BNB
LP_RANGE_UPPER = Decimal("2000")  # 2000 USDT per BNB


# =============================================================================
# LPOpenIntent Tests -- Uniswap V4 on BNB Chain
# =============================================================================


@pytest.mark.bsc
@pytest.mark.lp
class TestUniswapV4LPOpenIntent:
    """Test Uniswap V4 LP_OPEN using LPOpenIntent on BNB Chain.

    These tests verify the full Intent flow:
    - LPOpenIntent creation with protocol="uniswap_v4"
    - IntentCompiler routes to UniswapV4Adapter.compile_lp_open_intent()
    - Transactions execute successfully on-chain via PositionManager
    - UniswapV4ReceiptParser correctly extracts position_id and liquidity
    - Balance changes match expected deposits

    Pair choice: BNB/USDT routes through the NATIVE/USDT V4 pool. The
    adapter remaps "BNB" to address(0) for the pool key via
    ``_resolve_token(..., for_v4_pool=True)``, and the SDK threads
    ``amount0_max`` as ``msg.value`` on the modifyLiquidities call.
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_bnb_usdt(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test opening a BNB/USDT LP position via Uniswap V4.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> position_id extracted, liquidity > 0
        4. Balance Deltas: native BNB and USDT deposited into pool (bilateral)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]

        usdt_decimals = get_token_decimals(web3, usdt_addr)
        bnb_decimals = 18  # Native BNB is 18 decimals

        print(f"\n{'='*80}")
        print("Test: LP_OPEN BNB/USDT via Uniswap V4 on BNB Chain")
        print(f"{'='*80}")
        print(f"BNB amount:  {LP_AMOUNT_BNB}")
        print(f"USDT amount: {LP_AMOUNT_USDT}")
        print(f"Price range: {LP_RANGE_LOWER} - {LP_RANGE_UPPER} USDT/BNB")

        # Record balances before. BNB is native -- track via web3.eth.get_balance.
        bnb_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)

        # Fail-fast on underfunded fixtures: catch both the zero-balance and
        # the under-seeded cases so the failure is actionable instead of
        # surfacing later as a confusing on-chain revert.
        required_bnb = int(LP_AMOUNT_BNB * (Decimal(10) ** bnb_decimals))
        required_usdt = int(LP_AMOUNT_USDT * (Decimal(10) ** usdt_decimals))
        assert bnb_before >= required_bnb, (
            f"funded_wallet={funded_wallet} must hold >= {required_bnb} native BNB "
            f"(wei); have={bnb_before}. Check the bnb conftest seeding fixture."
        )
        assert usdt_before >= required_usdt, (
            f"funded_wallet={funded_wallet} must hold >= {required_usdt} USDT "
            f"({usdt_addr}); have={usdt_before}. "
            "Check the bnb conftest seeding fixture."
        )

        print(f"BNB before:  {format_token_amount(bnb_before, bnb_decimals)}")
        print(f"USDT before: {format_token_amount(usdt_before, usdt_decimals)}")

        # Layer 1: Compilation
        intent = LPOpenIntent(
            pool=LP_POOL,
            amount0=LP_AMOUNT_BNB,
            amount1=LP_AMOUNT_USDT,
            range_lower=LP_RANGE_LOWER,
            range_upper=LP_RANGE_UPPER,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated LPOpenIntent: pool={intent.pool}, protocol={intent.protocol}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        print("Compiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        bundle = compilation_result.action_bundle
        print(f"ActionBundle created with {len(bundle.transactions)} transactions")
        print(f"Metadata: liquidity={bundle.metadata.get('liquidity')}, "
              f"tick_lower={bundle.metadata.get('tick_lower')}, "
              f"tick_upper={bundle.metadata.get('tick_upper')}")

        # Layer 2: Execution
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Receipt Parsing
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        position_id = None
        liquidity = None
        saw_modify_liquidity_event = False
        saw_transfer_event = False

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()

                # Exercise parse_receipt() entrypoint -- this is the surface
                # ResultEnricher consumes in production via extract_swap_amounts
                # / extract_lp_amounts, so the intent-test contract requires
                # calling it here (.claude/rules/intent-tests.md Layer 3).
                parse_result = parser.parse_receipt(receipt_dict)
                if parse_result.modify_liquidity_events:
                    saw_modify_liquidity_event = True
                if parse_result.transfer_events:
                    saw_transfer_event = True

                # Extract position_id from ERC-721 Transfer (mint) event
                extracted_id = parser.extract_position_id(receipt_dict)
                if extracted_id is not None:
                    position_id = extracted_id
                    print(f"  Position ID (NFT tokenId): {position_id}")

                # Extract liquidity from ModifyLiquidity event
                extracted_liq = parser.extract_liquidity(receipt_dict)
                if extracted_liq is not None:
                    liquidity = extracted_liq
                    print(f"  Liquidity delta: {liquidity}")

        assert position_id is not None, "Must extract position_id from LP mint receipt"
        assert position_id > 0, f"Position ID must be positive, got {position_id}"
        assert liquidity is not None, "Must extract liquidity from ModifyLiquidity event"
        assert liquidity > 0, f"Liquidity must be positive, got {liquidity}"
        assert saw_modify_liquidity_event, (
            "parse_receipt() must surface the ModifyLiquidity event for an LP_OPEN"
        )
        assert saw_transfer_event, (
            "parse_receipt() must surface the ERC-721 mint Transfer for an LP_OPEN"
        )

        # Layer 4: Balance Deltas
        bnb_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)

        bnb_spent = bnb_before - bnb_after
        usdt_spent = usdt_before - usdt_after
        max_bnb_spend = int(LP_AMOUNT_BNB * (Decimal(10) ** bnb_decimals))
        max_usdt_spend = int(LP_AMOUNT_USDT * (Decimal(10) ** usdt_decimals))

        print("\n--- Balance Deltas ---")
        print(f"BNB spent (incl. gas): {format_token_amount(bnb_spent, bnb_decimals)}")
        print(f"USDT spent:            {format_token_amount(usdt_spent, usdt_decimals)}")

        # MANDATORY bilateral delta (see .claude/rules/intent-tests.md and #1691):
        # the position is opened with `range_lower=100`, `range_upper=2000`
        # USDT/BNB and BNB/USDT at ~673 USDT/BNB at fork time — unambiguously
        # in-range. Both currencies MUST have been deposited. Permitting `or`
        # here would let a V4 no-op silently pass.
        #
        # BNB is native: ``bnb_spent`` is the wallet's native delta, which
        # equals ``actual_bnb_deposited + gas_used*gas_price``. SETTLE_PAIR
        # refunds the slippage cushion via the PoolManager, so the deposit
        # itself is bounded above by ``amount0_max`` (~1.30× amount0 under
        # the LP minimum 30% slippage when on-chain sqrtPrice falls back to
        # the oracle estimate; the cushion drops to 5% when the StateView
        # query succeeds). We assert bnb_spent is strictly positive AND
        # bounded above by ``amount0_max + 0.01 BNB`` gas headroom — anything
        # outside that window is either a no-op (lower bound) or a regression
        # that overspent (upper bound).
        # Gas headroom for the native-BNB bound (defined here so the lower
        # and upper bounds can both reference it without duplication). At BSC
        # gas prices an LP_OPEN bundle (2× ERC20 approve, 2× Permit2.approve,
        # 1× mint = 5 TXs) totals well under 0.005 BNB, so 0.01 BNB is a
        # safe ceiling. Sized to 10% of LP_AMOUNT_BNB (mirrors the avalanche
        # sibling VIB-4367 which uses 0.1/1.0 = 10%) so the lower-bound
        # assertion still has plenty of room above gas-only.
        gas_headroom = int(Decimal("0.01") * (Decimal(10) ** bnb_decimals))

        # Lower bound for the BNB deposit: subtract the gas-headroom ceiling
        # so we still prove the position actually consumed native BNB. If a
        # tick/pool-key regression makes this position mint as one-sided
        # USDT liquidity, ``bnb_spent`` would be ~= gas alone (well below
        # gas_headroom), and this assertion catches it; a plain ``> 0`` check
        # would mask that case as long as any gas was paid.
        actual_bnb_deposit = bnb_spent - gas_headroom
        assert actual_bnb_deposit > 0, (
            "In-range LP_OPEN must deposit native BNB above the gas-only "
            "floor (no-op guard). "
            f"bnb_spent={bnb_spent}, gas_headroom={gas_headroom}, "
            f"deposit_estimate={actual_bnb_deposit}"
        )
        assert usdt_spent > 0, (
            f"In-range LP_OPEN must deposit USDT (no-op guard). usdt_spent={usdt_spent}"
        )

        # Upper-bound checks. For USDT the bound is the requested max1
        # (modifyLiquidities will refund the slippage cushion via SETTLE_PAIR,
        # so the actual spend cannot exceed the requested amount1_max which
        # the adapter caps at amount1 * (1 + slippage)). We bound on the
        # unbuffered amount1 to catch any off-by-decimals overspend.
        assert usdt_spent <= max_usdt_spend, (
            f"USDT spend exceeded requested amount1: spent={usdt_spent}, max={max_usdt_spend}"
        )
        # For native BNB include a gas headroom: a modifyLiquidities TX plus
        # 4 approval TXs (2× ERC-20 approve, 2× Permit2.approve, 1× mint) at
        # BSC gas prices ought to stay well below 0.01 BNB. The adapter caps
        # amount0_max at amount0 * (1 + lp_default_slippage) where
        # lp_default_slippage is 0.30 when sqrtPrice came from oracle prices
        # or 0.05 when on-chain. Take the looser 30% cap + the gas headroom
        # defined above to keep this robust regardless of which path the
        # compiler took.
        max_native_spend = int(
            Decimal(max_bnb_spend) * Decimal("1.30")
        ) + gas_headroom
        assert bnb_spent <= max_native_spend, (
            f"Native BNB spend exceeded amount0_max + gas headroom: "
            f"spent={bnb_spent}, max={max_native_spend}"
        )

        print(f"\nPosition ID: {position_id}")
        print(f"Liquidity:   {liquidity}")
        print("\nALL 4 LAYERS PASSED")

    @pytest.mark.intent(IntentType.LP_OPEN)  # noqa: layers
    @pytest.mark.asyncio
    async def test_lp_open_with_invalid_pool_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
    ):
        """Test that LP_OPEN with an invalid pool fails at compilation.

        Verifies compilation produces a clear error for invalid pool specs.
        """
        print(f"\n{'='*80}")
        print("Test: LP_OPEN with invalid pool (should fail)")
        print(f"{'='*80}")

        # Snapshot balances BEFORE compile so we can assert the failure-path
        # conservation invariant (no on-chain tx, no balance movement).
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]
        bnb_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)

        intent = LPOpenIntent(
            pool="INVALID/TOKENS/3000",
            amount0=Decimal("1"),
            amount1=Decimal("1"),
            range_lower=Decimal("1000"),
            range_upper=Decimal("2000"),
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "FAILED", (
            "Compilation should fail for invalid token symbols"
        )
        assert compilation_result.error is not None
        assert compilation_result.action_bundle is None, (
            "Compiler must not return an ActionBundle on FAILED compilation"
        )

        # Failure-path bilateral conservation: compile-only test, no tx fired,
        # both balances MUST be unchanged.
        bnb_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        assert bnb_after == bnb_before, (
            f"Native BNB balance must remain unchanged on compile failure. "
            f"before={bnb_before}, after={bnb_after}"
        )
        assert usdt_after == usdt_before, (
            f"USDT balance must remain unchanged on compile failure. "
            f"before={usdt_before}, after={usdt_after}"
        )

        print(f"Compilation failed as expected: {compilation_result.error}")
        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
