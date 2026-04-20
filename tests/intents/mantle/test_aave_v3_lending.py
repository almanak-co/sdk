"""Production-grade lending intent tests for Aave V3 on Mantle.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for lending operations:
1. Create lending intents (SupplyIntent, BorrowIntent, RepayIntent)
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using AaveV3ReceiptParser
5. Verify balance changes and account data are correct

Aave V3 Pool on Mantle: 0x458F293454fE0d67EC0655f3672301301DD51422

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/mantle/test_aave_v3_lending.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.aave_v3.adapter import (
    AAVE_V3_POOL_ADDRESSES,
)
from almanak.framework.connectors.aave_v3.receipt_parser import AaveV3ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
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

CHAIN_NAME = "mantle"

# Aave V3 Pool ABI (minimal - just getUserAccountData)
AAVE_POOL_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
        "name": "getUserAccountData",
        "outputs": [
            {"internalType": "uint256", "name": "totalCollateralBase", "type": "uint256"},
            {"internalType": "uint256", "name": "totalDebtBase", "type": "uint256"},
            {"internalType": "uint256", "name": "availableBorrowsBase", "type": "uint256"},
            {"internalType": "uint256", "name": "currentLiquidationThreshold", "type": "uint256"},
            {"internalType": "uint256", "name": "ltv", "type": "uint256"},
            {"internalType": "uint256", "name": "healthFactor", "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]


# =============================================================================
# Helper Functions
# =============================================================================


def get_user_account_data(web3: Web3, user: str) -> dict:
    """Get user account data from Aave V3 Pool contract on Mantle."""
    pool_address = AAVE_V3_POOL_ADDRESSES[CHAIN_NAME]
    pool_contract = web3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=AAVE_POOL_ABI)

    result = pool_contract.functions.getUserAccountData(Web3.to_checksum_address(user)).call()

    return {
        "totalCollateralBase": result[0],
        "totalDebtBase": result[1],
        "availableBorrowsBase": result[2],
        "currentLiquidationThreshold": result[3],
        "ltv": result[4],
        "healthFactor": result[5],
    }


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    """Create ExecutionContext with simulation enabled for accurate gas estimation."""
    return ExecutionContext(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        simulation_enabled=True,
    )


# =============================================================================
# Supply Tests
# =============================================================================


@pytest.mark.mantle
@pytest.mark.supply
@pytest.mark.lending
class TestAaveV3SupplyIntent:
    """Test Aave V3 supply operations using SupplyIntent on Mantle.

    Verifies:
    - SupplyIntent creation and compilation
    - On-chain execution
    - Receipt parsing for Supply events
    - Balance changes and account data verification
    """

    @pytest.mark.asyncio
    async def test_supply_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test WETH supply to Aave V3 on Mantle.

        4-Layer Verification:
        1. Compilation: SupplyIntent -> ActionBundle (SUCCESS)
        2. Execution: on-chain transactions succeed
        3. Receipt Parsing: Supply event parsed with correct amount
        4. Balance Deltas: WETH decreased, totalCollateralBase increased
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth = tokens["WETH"]
        decimals = get_token_decimals(web3, weth)

        supply_amount = Decimal("0.05")  # 0.05 WETH (~$175)

        print(f"\n{'=' * 80}")
        print(f"Test: Supply {supply_amount} WETH to Aave V3 on Mantle")
        print(f"{'=' * 80}")

        # Layer 4a: Record balances BEFORE
        weth_before = get_token_balance(web3, weth, funded_wallet)
        print(f"WETH before: {format_token_amount(weth_before, decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Collateral before: {account_data_before['totalCollateralBase']}")

        # Layer 1: Create and compile SupplyIntent
        intent = SupplyIntent(
            protocol="aave_v3",
            token="WETH",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        print("\nCompiling SupplyIntent...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: Execute
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Parse receipts
        supply_parsed = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = AaveV3ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.supplies:
                    supply_parsed = True
                    for supply_event in parse_result.supplies:
                        assert supply_event.amount > 0, "Supply amount must be > 0"
                        print(f"  Supply amount: {supply_event.amount}")
                        print(f"  Reserve: {supply_event.reserve}")

        assert supply_parsed, "Must find at least one Supply event in receipts"

        # Layer 4b: Verify balance changes
        weth_after = get_token_balance(web3, weth, funded_wallet)
        weth_spent = weth_before - weth_after

        print("\n--- Results ---")
        print(f"WETH spent: {format_token_amount(weth_spent, decimals)}")

        expected_weth_spent = int(supply_amount * Decimal(10**decimals))
        assert weth_spent == expected_weth_spent, (
            f"WETH spent must EXACTLY equal supply amount. Expected: {expected_weth_spent}, Got: {weth_spent}"
        )

        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Collateral after: {account_data_after['totalCollateralBase']}")

        assert (
            account_data_after["totalCollateralBase"] > account_data_before["totalCollateralBase"]
        ), "Collateral must increase after supply"

        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_withdraw_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test WETH withdraw using WithdrawIntent (after supplying).

        4-Layer Verification:
        1. Compilation: WithdrawIntent -> ActionBundle (SUCCESS)
        2. Execution: on-chain transactions succeed
        3. Receipt Parsing: Withdraw event parsed
        4. Balance Deltas: WETH increased, totalCollateralBase decreased
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth = tokens["WETH"]
        decimals = get_token_decimals(web3, weth)

        # First supply 0.1 WETH
        supply_amount = Decimal("0.1")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        supply_intent = SupplyIntent(
            protocol="aave_v3",
            token="WETH",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )

        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec_result = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_exec_result.success, f"Initial supply failed: {supply_exec_result.error}"

        # Now withdraw 0.05 WETH
        withdraw_amount = Decimal("0.05")

        print(f"\n{'=' * 80}")
        print(f"Test: Withdraw {withdraw_amount} WETH from Aave V3 on Mantle")
        print(f"{'=' * 80}")

        weth_before = get_token_balance(web3, weth, funded_wallet)
        print(f"WETH before withdraw: {format_token_amount(weth_before, decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Collateral before: {account_data_before['totalCollateralBase']}")

        # Layer 1: Create and compile WithdrawIntent
        intent = WithdrawIntent(
            protocol="aave_v3",
            token="WETH",
            amount=withdraw_amount,
            chain=CHAIN_NAME,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Parse receipts
        withdraw_parsed = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            if tx_result.receipt:
                parser = AaveV3ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                if parse_result.success and parse_result.withdrawals:
                    withdraw_parsed = True
                    for withdraw_event in parse_result.withdrawals:
                        assert withdraw_event.amount > 0, "Withdraw amount must be > 0"
                        print(f"  Withdraw amount: {withdraw_event.amount}")

        assert withdraw_parsed, "Must find at least one Withdraw event in receipts"

        # Layer 4b: Verify balance changes
        weth_after = get_token_balance(web3, weth, funded_wallet)
        weth_received = weth_after - weth_before

        print(f"\nWETH received: {format_token_amount(weth_received, decimals)}")

        expected_weth_received = int(withdraw_amount * Decimal(10**decimals))
        assert weth_received == expected_weth_received, (
            f"WETH received must EXACTLY equal withdraw amount. "
            f"Expected: {expected_weth_received}, Got: {weth_received}"
        )

        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Collateral after: {account_data_after['totalCollateralBase']}")

        assert (
            account_data_after["totalCollateralBase"] < account_data_before["totalCollateralBase"]
        ), "Collateral must decrease after withdraw"

        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_supply_intent_with_insufficient_balance_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test that SupplyIntent with insufficient balance fails gracefully.

        3-Layer Verification (failure mode):
        1. Compilation: succeeds (doesn't check balance)
        2. Execution: should fail on-chain
        3. Balance Conservation: WETH balance unchanged
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth = tokens["WETH"]
        decimals = get_token_decimals(web3, weth)

        # Get current balance
        weth_balance = get_token_balance(web3, weth, funded_wallet)
        balance_decimal = Decimal(weth_balance) / Decimal(10**decimals)

        # Try to supply more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'=' * 80}")
        print("Test: SupplyIntent with Insufficient Balance")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} WETH")
        print(f"Trying:    {excessive_amount} WETH")

        intent = SupplyIntent(
            protocol="aave_v3",
            token="WETH",
            amount=excessive_amount,
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
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)

        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        # Verify balance unchanged
        weth_after = get_token_balance(web3, weth, funded_wallet)
        assert weth_after == weth_balance, "Balance must be unchanged after failed supply"

        print("\nALL CHECKS PASSED")


# =============================================================================
# Borrow Tests
# =============================================================================


@pytest.mark.mantle
@pytest.mark.borrow
@pytest.mark.lending
class TestAaveV3BorrowIntent:
    """Test Aave V3 borrow operations on Mantle.

    Verifies supply WETH as collateral, then borrow USDC at ~30% LTV.
    """

    @pytest.mark.asyncio
    async def test_borrow_usdc_after_supply_weth(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC borrow after supplying WETH as collateral.

        4-Layer Verification:
        1. Compilation: BorrowIntent -> ActionBundle (SUCCESS)
        2. Execution: on-chain transactions succeed
        3. Receipt Parsing: Borrow event parsed
        4. Balance Deltas: USDC increased, totalDebtBase increased,
           healthFactor > 1e18
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth = tokens["WETH"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        collateral_amount = Decimal("0.1")  # 0.1 WETH as collateral
        borrow_amount = Decimal("50")  # ~28% LTV

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        print(f"\n{'=' * 80}")
        print("Test: Borrow USDC with WETH collateral on Aave V3 (Mantle)")
        print(f"{'=' * 80}")

        # Layer 4a: Record balances BEFORE borrow
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"USDC before borrow: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"Debt before: {account_data_before['totalDebtBase']}")

        # Layer 1: Compile BorrowIntent (includes collateral supply)
        borrow_intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=collateral_amount,
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )

        compilation_result = compiler.compile(borrow_intent)
        assert compilation_result.status.value == "SUCCESS", f"Borrow compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        print(f"ActionBundle: {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Borrow execution failed: {execution_result.error}"
        print(f"Borrow successful! {len(execution_result.transaction_results)} transactions")

        # Layer 3: Parse receipts
        borrow_parsed = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            if tx_result.receipt:
                parser = AaveV3ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                if parse_result.success and parse_result.borrows:
                    borrow_parsed = True
                    for borrow_event in parse_result.borrows:
                        assert borrow_event.amount > 0, "Borrow amount must be > 0"
                        print(f"  Borrow amount: {borrow_event.amount}")
                        print(f"  Reserve: {borrow_event.reserve}")

        assert borrow_parsed, "Must find at least one Borrow event in receipts"

        # Layer 4b: Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before

        print("\n--- Results ---")
        print(f"USDC received: {format_token_amount(usdc_received, usdc_decimals)}")

        expected_usdc = int(borrow_amount * Decimal(10**usdc_decimals))
        assert usdc_received == expected_usdc, (
            f"USDC received must EXACTLY equal borrow amount. Expected: {expected_usdc}, Got: {usdc_received}"
        )

        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Debt after: {account_data_after['totalDebtBase']}")
        print(f"Health factor: {account_data_after['healthFactor']}")

        assert account_data_after["totalDebtBase"] > account_data_before["totalDebtBase"], (
            "Debt must increase after borrow"
        )
        assert account_data_after["healthFactor"] > 10**18, (
            f"Health factor must be > 1.0 (got {account_data_after['healthFactor'] / 10**18:.4f})"
        )

        print("\nALL CHECKS PASSED")


# =============================================================================
# Repay Tests
# =============================================================================


@pytest.mark.mantle
@pytest.mark.repay
@pytest.mark.lending
class TestAaveV3RepayIntent:
    """Test Aave V3 repay operations on Mantle.

    Verifies supply -> borrow -> repay flow.
    """

    @pytest.mark.asyncio
    async def test_repay_usdc_after_borrow(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC repay after supply + borrow on Mantle.

        4-Layer Verification:
        1. Compilation: RepayIntent -> ActionBundle (SUCCESS)
        2. Execution: on-chain transactions succeed
        3. Receipt Parsing: Repay event parsed
        4. Balance Deltas: USDC decreased, totalDebtBase decreased,
           healthFactor improved
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        print(f"\n{'=' * 80}")
        print("Test: Repay USDC after supply + borrow on Aave V3 (Mantle)")
        print(f"{'=' * 80}")

        # Step 1: Supply 0.1 WETH + Borrow 30 USDC via BorrowIntent
        print("\nStep 1: Supplying WETH and borrowing 30 USDC...")

        borrow_intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("0.1"),
            borrow_token="USDC",
            borrow_amount=Decimal("30"),
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )
        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle, execution_context)
        assert borrow_exec.success, f"Borrow failed: {borrow_exec.error}"
        print("Supply + Borrow successful!")

        # Step 3: Repay 30 USDC
        repay_amount = Decimal("30")
        print(f"\nStep 3: Repaying {repay_amount} USDC...")

        # Layer 4a: Record state BEFORE repay
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"USDC before repay: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"Debt before repay: {account_data_before['totalDebtBase']}")
        print(f"Health factor before: {account_data_before['healthFactor']}")

        # Layer 1: Compile RepayIntent
        repay_intent = RepayIntent(
            protocol="aave_v3",
            token="USDC",
            amount=repay_amount,
            chain=CHAIN_NAME,
        )

        compilation_result = compiler.compile(repay_intent)
        assert compilation_result.status.value == "SUCCESS", f"Repay compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        print(f"ActionBundle: {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Repay execution failed: {execution_result.error}"
        print(f"Repay successful! {len(execution_result.transaction_results)} transactions")

        # Layer 3: Parse receipts
        repay_parsed = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            if tx_result.receipt:
                parser = AaveV3ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                if parse_result.success and parse_result.repays:
                    repay_parsed = True
                    for repay_event in parse_result.repays:
                        assert repay_event.amount > 0, "Repay amount must be > 0"
                        print(f"  Repay amount: {repay_event.amount}")
                        print(f"  Reserve: {repay_event.reserve}")

        assert repay_parsed, "Must find at least one Repay event in receipts"

        # Layer 4b: Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print("\n--- Results ---")
        print(f"USDC spent on repay: {format_token_amount(usdc_spent, usdc_decimals)}")

        expected_usdc_spent = int(repay_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal repay amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Debt after repay: {account_data_after['totalDebtBase']}")
        print(f"Health factor after: {account_data_after['healthFactor']}")

        assert account_data_after["totalDebtBase"] < account_data_before["totalDebtBase"], (
            "Debt must decrease after repay"
        )

        # Health factor should improve (increase) after repay
        if account_data_before["healthFactor"] < 2**256 - 1:  # Not max uint (no debt)
            assert account_data_after["healthFactor"] >= account_data_before["healthFactor"], (
                "Health factor must improve (or stay same) after repay"
            )

        print("\nALL CHECKS PASSED")


# =============================================================================
# Failure Mode Tests
# =============================================================================


@pytest.mark.mantle
@pytest.mark.lending
class TestAaveV3FailureModes:
    """Test Aave V3 failure modes on Mantle — balance conservation."""

    @pytest.mark.asyncio
    async def test_borrow_without_collateral_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test that borrowing without collateral fails and conserves balances.

        3-Layer Verification (failure mode):
        1. Compilation: may succeed (compilation doesn't check collateral)
        2. Execution: should fail or revert
        3. Balance Conservation: USDC balance unchanged
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]

        print(f"\n{'=' * 80}")
        print("Test: Borrow without collateral (failure mode)")
        print(f"{'=' * 80}")

        # Record USDC balance BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        # Try to borrow 1000 USDC with zero collateral
        # Even if wallet has residual collateral from prior tests,
        # 1000 USDC borrow should exceed any available borrowing power
        intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("1000"),
            interest_rate_mode="variable",
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

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert not execution_result.success, "Execution should fail without sufficient collateral"
        print(f"Execution failed as expected: {execution_result.error}")

        # Balance conservation: USDC must not change
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_before, (
            f"USDC balance must be conserved on failure. Before: {usdc_before}, After: {usdc_after}"
        )

        print("\nBALANCE CONSERVATION VERIFIED")
