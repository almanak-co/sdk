"""4-layer intent tests for Uniswap V4 LP_OPEN on Polygon Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
opening concentrated liquidity positions via V4 PositionManager on Polygon:
1. Create LPOpenIntent with pool, amounts, and price range
2. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV4ReceiptParser (position_id + liquidity)
5. Verify balances changed correctly (tokens deposited into pool)

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

To run:
    uv run pytest tests/intents/polygon/test_uniswap_v4_lp_open.py -v -s
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

CHAIN_NAME = "polygon"

# WETH/USDC pool with 0.3% fee tier (3000) — same shape as the arbitrum /
# ethereum golden tests. Verified 2026-05-14 via StateView.getSlot0(bytes32)
# against PoolManager 0x67366782... that this pool is initialized on Polygon
# with sqrtPriceX96 corresponding to tick=199085 (~$2260 USDC/WETH) and
# non-zero liquidity (~2.9e14). USDC here is the native (Circle) USDC token
# 0x3c499c..., which sorts before WETH 0x7ceB23.. so it is currency0.
LP_POOL = "WETH/USDC/3000"

# Small amounts to minimize capital requirements
LP_AMOUNT_WETH = Decimal("0.01")  # ~$25 of WETH
LP_AMOUNT_USDC = Decimal("25")    # $25 of USDC

# Wide price range to ensure position is in range
# Roughly 50% below and 200% above current price
LP_RANGE_LOWER = Decimal("1000")   # 1000 USDC per WETH
LP_RANGE_UPPER = Decimal("10000")  # 10000 USDC per WETH


# =============================================================================
# LPOpenIntent Tests -- Uniswap V4 on Polygon
# =============================================================================


@pytest.mark.polygon
@pytest.mark.lp
class TestUniswapV4LPOpenIntent:
    """Test Uniswap V4 LP_OPEN using LPOpenIntent on Polygon.

    These tests verify the full Intent flow:
    - LPOpenIntent creation with protocol="uniswap_v4"
    - IntentCompiler routes to UniswapV4Adapter.compile_lp_open_intent()
    - Transactions execute successfully on-chain via PositionManager
    - UniswapV4ReceiptParser correctly extracts position_id and liquidity
    - Balance changes match expected deposits
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_weth_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test opening a WETH/USDC LP position via Uniswap V4.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> position_id extracted, liquidity > 0
        4. Balance Deltas: WETH and USDC deposited into pool
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]

        weth_decimals = get_token_decimals(web3, weth_addr)
        usdc_decimals = get_token_decimals(web3, usdc_addr)

        print(f"\n{'='*80}")
        print("Test: LP_OPEN WETH/USDC via Uniswap V4 on Polygon")
        print(f"{'='*80}")
        print(f"WETH amount: {LP_AMOUNT_WETH}")
        print(f"USDC amount: {LP_AMOUNT_USDC}")
        print(f"Price range: {LP_RANGE_LOWER} - {LP_RANGE_UPPER}")

        # Record balances before
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        # Fail-fast on underfunded fixtures: catch both the zero-balance and
        # the under-seeded cases so the failure is actionable instead of
        # surfacing later as a confusing on-chain revert.
        required_weth = int(LP_AMOUNT_WETH * (Decimal(10) ** weth_decimals))
        required_usdc = int(LP_AMOUNT_USDC * (Decimal(10) ** usdc_decimals))
        assert weth_before >= required_weth, (
            f"funded_wallet={funded_wallet} must hold >= {required_weth} WETH "
            f"({weth_addr}); have={weth_before}. "
            "Check the polygon conftest seeding fixture."
        )
        assert usdc_before >= required_usdc, (
            f"funded_wallet={funded_wallet} must hold >= {required_usdc} USDC "
            f"({usdc_addr}); have={usdc_before}. "
            "Check the polygon conftest seeding fixture."
        )

        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # Layer 1: Compilation
        intent = LPOpenIntent(
            pool=LP_POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_USDC,
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

                # Exercise parse_receipt() entrypoint — this is the surface
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
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_spent = usdc_before - usdc_after
        max_weth_spend = int(LP_AMOUNT_WETH * (Decimal(10) ** weth_decimals))
        max_usdc_spend = int(LP_AMOUNT_USDC * (Decimal(10) ** usdc_decimals))

        print("\n--- Balance Deltas ---")
        print(f"WETH spent: {format_token_amount(weth_spent, weth_decimals)}")
        print(f"USDC spent: {format_token_amount(usdc_spent, usdc_decimals)}")

        # MANDATORY bilateral delta (see .claude/rules/intent-tests.md and #1691):
        # the position is opened with `range_lower=1000`, `range_upper=10000` and
        # WETH/USDC at ~2260 at fork time — unambiguously in-range. Both tokens
        # MUST have been deposited. Permitting `or` here would let a V4 no-op
        # silently pass.
        assert weth_spent > 0 and usdc_spent > 0, (
            f"In-range LP_OPEN must deposit BOTH tokens (no-op guard). "
            f"weth_spent={weth_spent}, usdc_spent={usdc_spent}"
        )
        # Upper-bound the spend at the requested amount0 / amount1 so a regression
        # that overspends (e.g. an off-by-decimals or fee-on-transfer surprise)
        # surfaces as a test failure rather than silently moving more capital
        # than the intent asked for.
        assert weth_spent <= max_weth_spend, (
            f"WETH spend exceeded requested max: spent={weth_spent}, max={max_weth_spend}"
        )
        assert usdc_spent <= max_usdc_spend, (
            f"USDC spend exceeded requested max: spent={usdc_spent}, max={max_usdc_spend}"
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
        print(f"Compilation failed as expected: {compilation_result.error}")
        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
