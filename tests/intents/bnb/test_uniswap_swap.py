"""Production-grade SwapIntent tests for Uniswap V3 on BNB Chain.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV3ReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

Note: USDC/WBNB tests are not included because the Uniswap V3 WBNB/USDC pool
does not exist on BNB chain with meaningful liquidity. PancakeSwap V3 is the
dominant DEX for USDC/WBNB swaps on BNB chain. USDT/WBNB pools do exist on
Uniswap V3 BNB with good liquidity.

To run:
    uv run pytest tests/intents/bnb/test_uniswap_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    SWAP_MAX_SLIPPAGE,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_v3_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "bnb"


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.bsc
@pytest.mark.swap
class TestUniswapV3SwapIntent:
    """Test Uniswap V3 swaps using SwapIntent on BNB Chain.

    These tests verify the full Intent flow:
    - SwapIntent creation with proper parameters
    - IntentCompiler generates correct Uniswap V3 transactions
    - Transactions execute successfully on-chain
    - UniswapV3ReceiptParser correctly interprets results
    - Balance changes match expected amounts

    Note: Only USDT/WBNB pairs are tested because Uniswap V3 on BNB chain
    does not have USDC/WBNB pools with liquidity. For USDC swaps on BNB,
    use PancakeSwap V3 instead.
    """

    @pytest.mark.asyncio
    async def test_swap_usdt_to_wbnb_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDT -> WBNB swap using SwapIntent with Uniswap V3."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDT"]
        token_out = tokens["WBNB"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, 500)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDT

        print(f"\n{'=' * 80}")
        print("Test: USDT -> WBNB Swap via SwapIntent (Uniswap V3)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDT")

        usdt_before = get_token_balance(web3, token_in, funded_wallet)
        wbnb_before = get_token_balance(web3, token_out, funded_wallet)

        # Create intent
        intent = SwapIntent(
            from_token="USDT",
            to_token="WBNB",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        # Compile with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=orchestrator.rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        # Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success

        # Parse receipts
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                from almanak.framework.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser

                parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.swap_result:
                    assert parse_result.swap_result.amount_in_decimal > 0
                    assert parse_result.swap_result.amount_out_decimal > 0

        # Verify balance changes
        usdt_after = get_token_balance(web3, token_in, funded_wallet)
        wbnb_after = get_token_balance(web3, token_out, funded_wallet)

        usdt_spent = usdt_before - usdt_after
        wbnb_received = wbnb_after - wbnb_before

        expected_usdt_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdt_spent == expected_usdt_spent, (
            f"USDT spent must EXACTLY equal swap amount. Expected: {expected_usdt_spent}, Got: {usdt_spent}"
        )
        assert wbnb_received > 0, "Must receive positive WBNB"

        print(f"USDT spent:    {format_token_amount(usdt_spent, in_decimals)}")
        print(f"WBNB received: {format_token_amount(wbnb_received, out_decimals)}")
        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_swap_wbnb_to_usdt_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WBNB -> USDT swap using SwapIntent with Uniswap V3 (reverse direction)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WBNB"]
        token_out = tokens["USDT"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, 500)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("0.1")  # 0.1 WBNB

        print(f"\n{'=' * 80}")
        print("Test: WBNB -> USDT Swap via SwapIntent (Uniswap V3)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} WBNB")

        wbnb_before = get_token_balance(web3, token_in, funded_wallet)
        usdt_before = get_token_balance(web3, token_out, funded_wallet)

        # Create intent
        intent = SwapIntent(
            from_token="WBNB",
            to_token="USDT",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        # Compile with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=orchestrator.rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        # Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success

        # Parse receipts
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                from almanak.framework.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser

                parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.swap_result:
                    assert parse_result.swap_result.amount_in_decimal > 0
                    assert parse_result.swap_result.amount_out_decimal > 0

        # Verify balance changes
        wbnb_after = get_token_balance(web3, token_in, funded_wallet)
        usdt_after = get_token_balance(web3, token_out, funded_wallet)

        wbnb_spent = wbnb_before - wbnb_after
        usdt_received = usdt_after - usdt_before

        expected_wbnb_spent = int(swap_amount * Decimal(10**in_decimals))
        assert wbnb_spent == expected_wbnb_spent, (
            f"WBNB spent must EXACTLY equal swap amount. Expected: {expected_wbnb_spent}, Got: {wbnb_spent}"
        )
        assert usdt_received > 0, "Must receive positive USDT"

        print(f"WBNB spent:    {format_token_amount(wbnb_spent, in_decimals)}")
        print(f"USDT received: {format_token_amount(usdt_received, out_decimals)}")
        print("\nALL CHECKS PASSED")

    @pytest.mark.xfail(reason="flaky: needs more investigation", strict=False)
    @pytest.mark.asyncio
    async def test_swap_intent_with_insufficient_balance_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test that SwapIntent with insufficient balance fails gracefully."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDT"]

        # Get current balance
        usdt_balance = get_token_balance(web3, token_in, funded_wallet)
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdt_balance) / Decimal(10**in_decimals)

        # Try to swap more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'='*80}")
        print("Test: SwapIntent with Insufficient Balance (Uniswap V3)")
        print(f"{'='*80}")
        print(f"Balance:   {balance_decimal} USDT")
        print(f"Trying:    {excessive_amount} USDT")

        intent = SwapIntent(
            from_token="USDT",
            to_token="WBNB",
            amount=excessive_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        # Try to execute - should fail
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        # Verify balance unchanged (conservation check)
        usdt_after = get_token_balance(web3, token_in, funded_wallet)
        assert usdt_after == usdt_balance, "Balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
