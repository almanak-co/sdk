"""Production-grade borrow/repay intent tests for BENQI on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for BENQI borrowing:
1. Create BorrowIntent / RepayIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using BenqiReceiptParser
5. Verify balance changes and debt accounting are correct

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

IMPORTANT: All borrow amounts target ~30% LTV to handle CoinGecko price fluctuations.

To run:
    uv run pytest tests/intents/avalanche/test_benqi_borrow.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.benqi.receipt_parser import BenqiReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import BorrowIntent, RepayIntent
from almanak.framework.intents.compiler import IntentCompiler
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "avalanche"

# BENQI qiToken addresses for receipt filtering
BENQI_QI_USDC = "0xB715808a78F6041E46d61Cb123C9B4A27056AE9C"
BENQI_QI_WAVAX = "0x5C0401e81Bc07Ca70fAD469b451682c0d747Ef1c"


# =============================================================================
# Borrow/Repay Tests
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.borrow
@pytest.mark.lending
class TestBenqiBorrowIntent:
    """Test BENQI borrow/repay operations using BorrowIntent and RepayIntent.

    These tests verify the full Intent flow:
    - BorrowIntent creation with collateral and borrow parameters
    - IntentCompiler generates correct BENQI transactions (supply collateral + enterMarkets + borrow)
    - Transactions execute successfully on-chain
    - BenqiReceiptParser correctly interprets Mint/Borrow/RepayBorrow events
    - Balance changes match expected amounts
    """

    @pytest.mark.asyncio
    async def test_borrow_usdc_with_wavax_collateral_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test borrowing USDC with WAVAX collateral using BorrowIntent.

        Flow:
        1. Create BorrowIntent with WAVAX as collateral, borrowing USDC
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Parse receipt for Mint (supply) and Borrow events
        5. Verify WAVAX decreased (collateral) and USDC increased (borrowed)

        LTV calculation: ~10 WAVAX at ~$20 = $200 collateral.
        Borrow $50 USDC = ~25% LTV (safe under 30% cap).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]

        usdc_decimals = get_token_decimals(web3, usdc)

        # Supply 10 AVAX as collateral (~$200), borrow 50 USDC (~25% LTV)
        collateral_amount = Decimal("10")
        borrow_amount = Decimal("50")

        print(f"\n{'='*80}")
        print(f"Test: Borrow {borrow_amount} USDC with {collateral_amount} AVAX collateral using BorrowIntent")
        print(f"{'='*80}")

        # Record balances BEFORE
        # BENQI qiAVAX uses native AVAX (not WAVAX ERC-20), so check native balance
        avax_before = web3.eth.get_balance(funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        print(f"Native AVAX before: {format_token_amount(avax_before, 18)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # Create BorrowIntent
        intent = BorrowIntent(
            protocol="benqi",
            collateral_token="AVAX",
            collateral_amount=collateral_amount,
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            chain=CHAIN_NAME,
        )

        print("\nCreated BorrowIntent:")
        print(f"  Collateral: {intent.collateral_amount} {intent.collateral_token}")
        print(f"  Borrow: {intent.borrow_amount} {intent.borrow_token}")

        # Compile intent
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        print("\nCompiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Execute via ExecutionOrchestrator
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts - track that we found expected events
        found_borrow_event = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                # Use appropriate decimals: 18 for AVAX collateral supply, usdc_decimals for borrow
                # The borrow tx (last) uses USDC decimals; supply tx uses AVAX (18) decimals
                tx_decimals = usdc_decimals if i == len(execution_result.transaction_results) - 1 else 18
                parser = BenqiReceiptParser(underlying_decimals=tx_decimals)
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success:
                    if parse_result.supply_amount > 0:
                        print(f"  Supply (collateral): {parse_result.supply_amount}")
                    if parse_result.borrow_amount > 0:
                        print(f"  Borrow amount: {parse_result.borrow_amount}")
                        assert parse_result.borrow_amount > 0, "Borrow amount must be positive"
                        found_borrow_event = True

        assert found_borrow_event, "Receipt parser must find at least one Borrow event"

        # Verify balance changes
        # BENQI qiAVAX uses native AVAX, so native balance decreases by collateral + gas
        avax_after = web3.eth.get_balance(funded_wallet)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)

        avax_spent = avax_before - avax_after
        usdc_received = usdc_after - usdc_before

        # Calculate total gas cost across all transactions
        total_gas_cost = sum(
            tx.gas_used * tx.receipt.effective_gas_price
            for tx in execution_result.transaction_results
            if tx.receipt
        )

        print("\n--- Results ---")
        print(f"Native AVAX spent (total): {format_token_amount(avax_spent, 18)}")
        print(f"Gas cost: {format_token_amount(total_gas_cost, 18)}")
        print(f"USDC received (borrowed): {format_token_amount(usdc_received, usdc_decimals)}")

        # Verify native AVAX spent = collateral + gas (not exact due to gas, check collateral component)
        expected_collateral_wei = int(collateral_amount * Decimal(10**18))
        collateral_spent = avax_spent - total_gas_cost
        assert collateral_spent == expected_collateral_wei, (
            f"Native AVAX collateral spent must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {collateral_spent} "
            f"(total spent: {avax_spent}, gas: {total_gas_cost})"
        )

        # Verify USDC was received (exact for BENQI)
        expected_usdc_received = int(borrow_amount * Decimal(10**usdc_decimals))
        assert usdc_received == expected_usdc_received, (
            f"USDC received must EXACTLY equal borrow amount. "
            f"Expected: {expected_usdc_received}, Got: {usdc_received}"
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_repay_usdc_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test repaying USDC debt using RepayIntent.

        Flow:
        1. Setup: Borrow USDC with WAVAX collateral first
        2. Create RepayIntent to repay partial debt
        3. Compile and execute
        4. Parse receipt for RepayBorrow event
        5. Verify USDC balance decreased by exact repay amount
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        # First borrow USDC with WAVAX collateral
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        borrow_intent = BorrowIntent(
            protocol="benqi",
            collateral_token="AVAX",
            collateral_amount=Decimal("10"),
            borrow_token="USDC",
            borrow_amount=Decimal("50"),
            chain=CHAIN_NAME,
        )

        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle)
        assert borrow_exec.success, f"Initial borrow failed: {borrow_exec.error}"

        # Now repay partial debt
        repay_amount = Decimal("25")

        print(f"\n{'='*80}")
        print(f"Test: Repay {repay_amount} USDC debt using RepayIntent")
        print(f"{'='*80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before repay: {format_token_amount(usdc_before, usdc_decimals)}")

        # Create RepayIntent
        intent = RepayIntent(
            protocol="benqi",
            token="USDC",
            amount=repay_amount,
            chain=CHAIN_NAME,
        )

        print(f"\nCreated RepayIntent: token={intent.token}, amount={intent.amount}")

        # Compile and execute
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Parse receipts
        found_repay_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = BenqiReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    qi_token_address=BENQI_QI_USDC,
                )

                if parse_result.success and parse_result.repay_amount > 0:
                    print(f"  Repay amount: {parse_result.repay_amount}")
                    assert parse_result.repay_amount > 0, "Repay amount must be positive"
                    found_repay_event = True

        assert found_repay_event, "Receipt parser must find at least one RepayBorrow event"

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print(f"\nUSDC spent (repaid): {format_token_amount(usdc_spent, usdc_decimals)}")

        expected_usdc_spent = int(repay_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal repay amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.xfail(
        reason="BENQI Comptroller on Anvil fork does not reliably enforce borrow limits. "
        "The oracle price feed on the fork may return values that allow excessive borrows.",
        strict=False,
    )
    @pytest.mark.asyncio
    async def test_borrow_excessive_amount_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test that borrowing far more than collateral supports fails gracefully.

        Supply minimal collateral (1 AVAX ~ $20) but try to borrow $100,000 USDC.
        This exceeds any collateral factor and must revert on-chain.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]

        print(f"\n{'='*80}")
        print("Test: BorrowIntent with Excessive Amount (BENQI - should fail)")
        print(f"{'='*80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        # Supply tiny collateral but borrow massive amount
        intent = BorrowIntent(
            protocol="benqi",
            collateral_token="AVAX",
            collateral_amount=Decimal("1"),  # ~$20 collateral
            borrow_token="USDC",
            borrow_amount=Decimal("100000"),  # $100k borrow >> collateral
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert not execution_result.success, "Execution should fail with excessive borrow amount"
        print(f"Execution failed as expected: {execution_result.error}")

        # Verify no USDC received
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed borrow"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
