"""Production-grade borrow/repay intent tests for Joe Lend (Banker Joe) on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for Joe Lend borrowing:
1. Create BorrowIntent / RepayIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using JoeLendReceiptParser
5. Verify balance changes and debt accounting are correct

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

IMPORTANT: All borrow amounts target ~30% LTV to handle CoinGecko price fluctuations.

To run:
    uv run pytest tests/intents/avalanche/test_joelend_borrow.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.joelend.receipt_parser import JoeLendReceiptParser
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

# Joe Lend jToken addresses for receipt filtering
JOELEND_J_USDC_E = "0xEd6AaF91a2B084bd594DBd1245be3691F9f637aC"
JOELEND_J_AVAX = "0xC22F01ddc8010Ee05574028528614634684EC29e"


# =============================================================================
# Borrow/Repay Tests
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.borrow
@pytest.mark.lending
class TestJoeLendBorrowIntent:
    """Test Joe Lend borrow/repay operations using BorrowIntent and RepayIntent.

    These tests verify the full Intent flow:
    - BorrowIntent creation with collateral and borrow parameters
    - IntentCompiler generates correct Joe Lend transactions (supply collateral + enterMarkets + borrow)
    - Transactions execute successfully on-chain
    - JoeLendReceiptParser correctly interprets Mint/Borrow/RepayBorrow events
    - Balance changes match expected amounts
    """

    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-3960: JoeLend protocol wound down on-chain (Avalanche). "
        "Reverts with `Error: wind down` for every supply/borrow/repay/withdraw call. "
        "strict=True so an XPASS will alert us if JoeLend resurrects.",
        strict=True,
    )
    async def test_borrow_usdc_with_avax_collateral_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test borrowing USDC.e with AVAX collateral using BorrowIntent.

        Flow:
        1. Create BorrowIntent with AVAX as collateral, borrowing USDC
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Parse receipt for Mint (supply) and Borrow events
        5. Verify AVAX decreased (collateral) and USDC increased (borrowed)

        LTV calculation: ~10 AVAX at ~$20 = $200 collateral.
        Borrow $50 USDC = ~25% LTV (safe under 30% cap).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC.e"]

        usdc_decimals = get_token_decimals(web3, usdc)

        # Supply 10 AVAX as collateral (~$200), borrow 50 USDC (~25% LTV)
        collateral_amount = Decimal("10")
        borrow_amount = Decimal("50")

        print(f"\n{'='*80}")
        print(f"Test: Borrow {borrow_amount} USDC.e with {collateral_amount} AVAX collateral using BorrowIntent")
        print(f"{'='*80}")

        # Record balances BEFORE
        avax_before = web3.eth.get_balance(funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        print(f"Native AVAX before: {format_token_amount(avax_before, 18)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # Create BorrowIntent
        intent = BorrowIntent(
            protocol="joelend",
            collateral_token="AVAX",
            collateral_amount=collateral_amount,
            borrow_token="USDC.e",
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
                tx_decimals = usdc_decimals if i == len(execution_result.transaction_results) - 1 else 18
                parser = JoeLendReceiptParser(underlying_decimals=tx_decimals)
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
        avax_after = web3.eth.get_balance(funded_wallet)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)

        avax_spent = avax_before - avax_after
        usdc_received = usdc_after - usdc_before

        total_gas_cost = sum(
            tx.gas_used * tx.receipt.effective_gas_price
            for tx in execution_result.transaction_results
            if tx.receipt
        )

        print("\n--- Results ---")
        print(f"Native AVAX spent (total): {format_token_amount(avax_spent, 18)}")
        print(f"Gas cost: {format_token_amount(total_gas_cost, 18)}")
        print(f"USDC received (borrowed): {format_token_amount(usdc_received, usdc_decimals)}")

        expected_collateral_wei = int(collateral_amount * Decimal(10**18))
        collateral_spent = avax_spent - total_gas_cost
        assert collateral_spent == expected_collateral_wei, (
            f"Native AVAX collateral spent must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {collateral_spent} "
            f"(total spent: {avax_spent}, gas: {total_gas_cost})"
        )

        expected_usdc_received = int(borrow_amount * Decimal(10**usdc_decimals))
        assert usdc_received == expected_usdc_received, (
            f"USDC received must EXACTLY equal borrow amount. "
            f"Expected: {expected_usdc_received}, Got: {usdc_received}"
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-3960: JoeLend protocol wound down on-chain (Avalanche). "
        "Reverts with `Error: wind down` for every supply/borrow/repay/withdraw call. "
        "strict=True so an XPASS will alert us if JoeLend resurrects.",
        strict=True,
    )
    async def test_repay_usdc_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test repaying USDC.e debt using RepayIntent.

        Flow:
        1. Setup: Borrow USDC.e with AVAX collateral first
        2. Create RepayIntent to repay partial debt
        3. Compile and execute
        4. Parse receipt for RepayBorrow event
        5. Verify USDC balance decreased by exact repay amount
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC.e"]
        usdc_decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # First borrow USDC.e with AVAX collateral
        borrow_intent = BorrowIntent(
            protocol="joelend",
            collateral_token="AVAX",
            collateral_amount=Decimal("10"),
            borrow_token="USDC.e",
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
        print(f"Test: Repay {repay_amount} USDC.e debt using RepayIntent")
        print(f"{'='*80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before repay: {format_token_amount(usdc_before, usdc_decimals)}")

        # Create RepayIntent
        intent = RepayIntent(
            protocol="joelend",
            token="USDC.e",
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
                parser = JoeLendReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    j_token_address=JOELEND_J_USDC_E,
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

    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-3960: JoeLend protocol wound down on-chain (Avalanche). "
        "Reverts with `Error: wind down` for every supply/borrow/repay/withdraw call. "
        "strict=True so an XPASS will alert us if JoeLend resurrects.",
        strict=True,
    )
    async def test_borrow_only_usdc_against_existing_collateral(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test borrow-only path: BorrowIntent with collateral_amount=0.

        Flow:
        1. Setup: Supply AVAX + enterMarkets via BorrowIntent with collateral
        2. Record balances
        3. Create BorrowIntent with collateral_amount=0
        4. Compile (verify only BORROW tx, no SUPPLY/enterMarkets)
        5. Execute
        6. Parse receipt for Borrow event
        7. Verify USDC increased, AVAX unchanged (no collateral spent)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC.e"]
        usdc_decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Setup: supply collateral via a regular BorrowIntent
        setup_intent = BorrowIntent(
            protocol="joelend",
            collateral_token="AVAX",
            collateral_amount=Decimal("10"),
            borrow_token="USDC.e",
            borrow_amount=Decimal("10"),
            chain=CHAIN_NAME,
        )

        setup_result = compiler.compile(setup_intent)
        assert setup_result.status.value == "SUCCESS"
        assert setup_result.action_bundle is not None
        setup_exec = await orchestrator.execute(setup_result.action_bundle)
        assert setup_exec.success, f"Setup execution failed: {setup_exec.error}"
        print("Setup complete: 10 AVAX supplied, markets entered, 10 USDC borrowed")

        # Record balances BEFORE borrow-only
        avax_before = web3.eth.get_balance(funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        borrow_amount = Decimal("20")

        print(f"\n{'='*80}")
        print(f"Test: Borrow-only {borrow_amount} USDC.e (collateral_amount=0) using BorrowIntent")
        print(f"{'='*80}")
        print(f"Native AVAX before: {format_token_amount(avax_before, 18)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # Create BorrowIntent with collateral_amount=0
        intent = BorrowIntent(
            protocol="joelend",
            collateral_token="AVAX",
            collateral_amount=Decimal("0"),
            borrow_token="USDC.e",
            borrow_amount=borrow_amount,
            chain=CHAIN_NAME,
        )

        # Layer 1: Compilation
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        bundle = compilation_result.action_bundle
        num_txs = len(bundle.transactions)
        assert num_txs == 1, (
            f"Borrow-only path (collateral_amount=0) should produce exactly 1 transaction "
            f"(just the borrow), but got {num_txs}."
        )

        if compilation_result.warnings:
            assert any("existing collateral" in w.lower() for w in compilation_result.warnings)

        # Layer 2: Execution
        execution_result = await orchestrator.execute(bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt Parsing
        found_borrow_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = JoeLendReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.borrow_amount > 0:
                    assert parse_result.borrow_amount > 0
                    found_borrow_event = True

                assert parse_result.supply_amount == 0, "Borrow-only path must NOT produce supply events"

        assert found_borrow_event, "Receipt parser must find at least one Borrow event"

        # Layer 4: Balance Deltas
        avax_after = web3.eth.get_balance(funded_wallet)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)

        total_gas_cost = sum(
            tx.gas_used * tx.receipt.effective_gas_price
            for tx in execution_result.transaction_results
            if tx.receipt
        )

        usdc_received = usdc_after - usdc_before
        avax_spent_excluding_gas = (avax_before - avax_after) - total_gas_cost

        expected_usdc = int(borrow_amount * Decimal(10**usdc_decimals))
        assert usdc_received == expected_usdc, (
            f"USDC received must EXACTLY equal borrow amount. "
            f"Expected: {expected_usdc}, Got: {usdc_received}"
        )

        assert avax_spent_excluding_gas == 0, (
            f"Borrow-only path must NOT spend native AVAX as collateral. "
            f"AVAX spent (excluding gas): {avax_spent_excluding_gas}"
        )

        print("\nALL CHECKS PASSED — borrow-only path verified")

    @pytest.mark.xfail(
        reason="Joe Lend Joetroller on Anvil fork does not reliably enforce borrow limits. "
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
        """Test that borrowing far more than collateral supports fails gracefully."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC.e"]

        print(f"\n{'='*80}")
        print("Test: BorrowIntent with Excessive Amount (Joe Lend - should fail)")
        print(f"{'='*80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        intent = BorrowIntent(
            protocol="joelend",
            collateral_token="AVAX",
            collateral_amount=Decimal("1"),
            borrow_token="USDC.e",
            borrow_amount=Decimal("100000"),
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

        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed borrow"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
