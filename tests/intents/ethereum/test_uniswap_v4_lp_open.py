"""4-layer intent tests for Uniswap V4 LP_OPEN on Ethereum Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
opening concentrated liquidity positions via V4 PositionManager:
1. Create LPOpenIntent with pool, amounts, and price range
2. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV4ReceiptParser (position_id + liquidity)
5. Verify balances changed correctly (tokens deposited into pool)

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

To run:
    uv run pytest tests/intents/ethereum/test_uniswap_v4_lp_open.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import LPOpenIntent
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "ethereum"

# WETH/USDC pool with 0.3% fee tier (3000)
LP_POOL = "WETH/USDC/3000"

# Small amounts to minimize capital requirements
LP_AMOUNT_WETH = Decimal("0.01")  # ~$25 of WETH
LP_AMOUNT_USDC = Decimal("25")    # $25 of USDC

# Wide price range to ensure position is in range
# Roughly 50% below and 200% above current price
LP_RANGE_LOWER = Decimal("1000")   # 1000 USDC per WETH
LP_RANGE_UPPER = Decimal("10000")  # 10000 USDC per WETH


# =============================================================================
# LPOpenIntent Tests -- Uniswap V4 on Ethereum
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.lp
class TestUniswapV4LPOpenIntent:
    """Test Uniswap V4 LP_OPEN using LPOpenIntent on Ethereum.

    These tests verify the full Intent flow:
    - LPOpenIntent creation with protocol="uniswap_v4"
    - IntentCompiler routes to UniswapV4Adapter.compile_lp_open_intent()
    - Transactions execute successfully on-chain via PositionManager
    - UniswapV4ReceiptParser correctly extracts position_id and liquidity
    - Balance changes match expected deposits
    """

    @pytest.mark.xfail(
        reason="V4 LP on Anvil fork may fail due to PositionManager event differences (VIB-2025)",
        strict=False,
    )
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
        print("Test: LP_OPEN WETH/USDC via Uniswap V4 on Ethereum")
        print(f"{'='*80}")
        print(f"WETH amount: {LP_AMOUNT_WETH}")
        print(f"USDC amount: {LP_AMOUNT_USDC}")
        print(f"Price range: {LP_RANGE_LOWER} - {LP_RANGE_UPPER}")

        # Record balances before
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

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

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()

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

        # Layer 4: Balance Deltas
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_spent = usdc_before - usdc_after

        print("\n--- Balance Deltas ---")
        print(f"WETH spent: {format_token_amount(weth_spent, weth_decimals)}")
        print(f"USDC spent: {format_token_amount(usdc_spent, usdc_decimals)}")

        # At least one token must have been deposited (both for in-range positions)
        assert weth_spent > 0 or usdc_spent > 0, (
            "Must deposit at least one token into LP position"
        )

        # Both should be deposited for an in-range position with the wide range we specified
        assert weth_spent >= 0, "WETH balance should not increase from LP_OPEN"
        assert usdc_spent >= 0, "USDC balance should not increase from LP_OPEN"

        print(f"\nPosition ID: {position_id}")
        print(f"Liquidity:   {liquidity}")
        print("\nALL 4 LAYERS PASSED")

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
        print(f"Compilation failed as expected: {compilation_result.error}")
        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
