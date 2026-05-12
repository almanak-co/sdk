"""Production-grade lending intent tests for Aave V3.6 on X-Layer.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for lending operations:
1. Create lending intents (SupplyIntent, WithdrawIntent, BorrowIntent, RepayIntent)
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using AaveV3ReceiptParser
5. Verify balance changes and account data are correct

X-Layer Aave V3.6 reserves (verified on-chain via getReservesList):
  - USDT0 (USD₮0): LTV=70%, borrowingEnabled=true — used as collateral in tests
  - xETH: LTV=70%, borrowingEnabled=true (limited liquidity)
  - xBTC: LTV=70%, borrowingEnabled=true
  - WOKB: LTV=0 (cannot be used as collateral)
  - GHO: borrow-only reserve
  - USDG (Gravity USD): borrow reserve — used as borrow token in tests

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/xlayer/test_aave_v3_lending.py -v -s
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
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "xlayer"

# Aave V3 Pool ABI (minimal - just what we need for getUserAccountData)
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
    """Get user account data from Aave V3 Pool contract."""
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
# Supply/Withdraw Tests
# =============================================================================


@pytest.mark.xlayer
@pytest.mark.supply
@pytest.mark.lending
class TestAaveV3SupplyIntent:
    """Test Aave V3.6 supply/withdraw operations on X-Layer using SupplyIntent and WithdrawIntent.

    Uses USDT0 (USD₮0) as the supply token — it has LTV=70% on X-Layer Aave V3.6.
    Note: USDC and WETH are NOT Aave reserves on X-Layer.
    """

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_usdt0_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDT0 supply using SupplyIntent.

        Flow:
        1. Create SupplyIntent for USDT0
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify balances and account data changed correctly
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt0 = tokens["USDT0"]
        decimals = get_token_decimals(web3, usdt0)

        supply_amount = Decimal("1000")  # 1000 USDT0

        print(f"\n{'='*80}")
        print(f"Test: Supply {supply_amount} USDT0 to Aave V3.6 using SupplyIntent (X-Layer)")
        print(f"{'='*80}")

        # Record balances BEFORE
        usdt0_before = get_token_balance(web3, usdt0, funded_wallet)
        print(f"USDT0 before: {format_token_amount(usdt0_before, decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Collateral before: {account_data_before['totalCollateralBase']}")

        # Create SupplyIntent
        intent = SupplyIntent(
            protocol="aave_v3",
            token="USDT0",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SupplyIntent: protocol={intent.protocol}, token={intent.token}, amount={intent.amount}")

        # Compile intent
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        print("Compiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Execute via ExecutionOrchestrator (with simulation enabled for accurate gas estimation)
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts (Layer 3)
        supply_parsed = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = AaveV3ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.supplies:
                    for supply_event in parse_result.supplies:
                        assert supply_event.amount > 0, "Supply event amount must be > 0"
                        print(f"  Supply amount:  {supply_event.amount}")
                        print(f"  Reserve: {supply_event.reserve}")
                        supply_parsed = True

        assert supply_parsed, "At least one supply event must be parsed from receipts"

        # Verify balance changes (Layer 4)
        usdt0_after = get_token_balance(web3, usdt0, funded_wallet)
        usdt0_spent = usdt0_before - usdt0_after

        print("\n--- Results ---")
        print(f"USDT0 spent: {format_token_amount(usdt0_spent, decimals)}")

        # Verify USDT0 was spent
        expected_usdt0_spent = int(supply_amount * Decimal(10**decimals))
        assert usdt0_spent == expected_usdt0_spent, (
            f"USDT0 spent must EXACTLY equal supply amount. "
            f"Expected: {expected_usdt0_spent}, Got: {usdt0_spent}"
        )

        # Verify account data changed
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Collateral after: {account_data_after['totalCollateralBase']}")

        assert (
            account_data_after["totalCollateralBase"] > account_data_before["totalCollateralBase"]
        ), "Collateral must increase after supply"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_usdt0_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDT0 withdraw using WithdrawIntent (after supplying).

        Flow:
        1. Supply USDT0 first
        2. Create WithdrawIntent to withdraw portion
        3. Compile and execute
        4. Verify balances changed correctly
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt0 = tokens["USDT0"]
        decimals = get_token_decimals(web3, usdt0)

        # First supply 2000 USDT0
        supply_amount = Decimal("2000")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        supply_intent = SupplyIntent(
            protocol="aave_v3",
            token="USDT0",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )

        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec_result = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_exec_result.success, f"Initial supply failed: {supply_exec_result.error}"

        # Now withdraw 1000 USDT0
        withdraw_amount = Decimal("1000")

        print(f"\n{'='*80}")
        print(f"Test: Withdraw {withdraw_amount} USDT0 from Aave V3.6 using WithdrawIntent (X-Layer)")
        print(f"{'='*80}")

        usdt0_before = get_token_balance(web3, usdt0, funded_wallet)
        print(f"USDT0 before withdraw: {format_token_amount(usdt0_before, decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Collateral before: {account_data_before['totalCollateralBase']}")

        # Create WithdrawIntent
        intent = WithdrawIntent(
            protocol="aave_v3",
            token="USDT0",
            amount=withdraw_amount,
            chain=CHAIN_NAME,
        )

        print(f"\nCreated WithdrawIntent: protocol={intent.protocol}, token={intent.token}, amount={intent.amount}")

        # Compile and execute
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Parse receipts (Layer 3)
        withdraw_parsed = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = AaveV3ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                if parse_result.success and parse_result.withdraws:
                    for withdraw_event in parse_result.withdraws:
                        assert withdraw_event.amount > 0, "Withdraw event amount must be > 0"
                        withdraw_parsed = True

        assert withdraw_parsed, "At least one withdraw event must be parsed from receipts"

        # Verify balance changes
        usdt0_after = get_token_balance(web3, usdt0, funded_wallet)
        usdt0_received = usdt0_after - usdt0_before

        print(f"\nUSDT0 received: {format_token_amount(usdt0_received, decimals)}")

        expected_usdt0_received = int(withdraw_amount * Decimal(10**decimals))
        assert usdt0_received == expected_usdt0_received, (
            f"USDT0 received must EXACTLY equal withdraw amount. "
            f"Expected: {expected_usdt0_received}, Got: {usdt0_received}"
        )

        # Verify account data changed
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Collateral after: {account_data_after['totalCollateralBase']}")

        assert (
            account_data_after["totalCollateralBase"] < account_data_before["totalCollateralBase"]
        ), "Collateral must decrease after withdraw"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_intent_with_insufficient_balance_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test that SupplyIntent with insufficient balance fails gracefully."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt0 = tokens["USDT0"]
        decimals = get_token_decimals(web3, usdt0)

        # Get current balance
        usdt0_balance = get_token_balance(web3, usdt0, funded_wallet)
        balance_decimal = Decimal(usdt0_balance) / Decimal(10**decimals)

        # Try to supply more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'='*80}")
        print("Test: SupplyIntent with Insufficient Balance")
        print(f"{'='*80}")
        print(f"Balance:   {balance_decimal} USDT0")
        print(f"Trying:    {excessive_amount} USDT0")

        # Create SupplyIntent with excessive amount
        intent = SupplyIntent(
            protocol="aave_v3",
            token="USDT0",
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
        usdt0_after = get_token_balance(web3, usdt0, funded_wallet)
        assert usdt0_after == usdt0_balance, "Balance must be unchanged after failed supply"

        print("\nALL CHECKS PASSED")


# =============================================================================
# Borrow/Repay Tests
# =============================================================================


@pytest.mark.xlayer
@pytest.mark.borrow
@pytest.mark.lending
class TestAaveV3BorrowIntent:
    """Test Aave V3.6 borrow/repay operations on X-Layer using BorrowIntent and RepayIntent.

    Uses USDT0 as collateral (LTV=70%) and borrows USDG (Gravity USD).
    Note: USDC and WETH are NOT Aave reserves on X-Layer.
    """

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_usdg_with_usdt0_collateral_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test borrowing USDG with USDT0 collateral using BorrowIntent.

        Flow:
        1. Create BorrowIntent with USDT0 as collateral, borrowing USDG
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify USDG balance increased and debt was created

        USDT0 LTV=70%, so 1000 USDT0 collateral can borrow up to 700 USDG.
        We borrow 300 USDG (~30% LTV) for safety margin.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt0 = tokens["USDT0"]

        usdt0_decimals = get_token_decimals(web3, usdt0)

        # USDG address (Aave V3.6 borrow reserve on X-Layer)
        usdg_address = "0x4ae46a509F6b1D9056937BA4500cb143933D2dc8"
        usdg_decimals = 6  # USDG is 6 decimals

        # Supply 1000 USDT0 as collateral, borrow 300 USDG (~30% LTV)
        collateral_amount = Decimal("1000")
        borrow_amount = Decimal("300")

        print(f"\n{'='*80}")
        print(f"Test: Borrow {borrow_amount} USDG with {collateral_amount} USDT0 collateral (X-Layer)")
        print(f"{'='*80}")

        # Record balances BEFORE
        usdt0_before = get_token_balance(web3, usdt0, funded_wallet)
        usdg_before = get_token_balance(web3, usdg_address, funded_wallet)

        print(f"USDT0 before: {format_token_amount(usdt0_before, usdt0_decimals)}")
        print(f"USDG before: {format_token_amount(usdg_before, usdg_decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Debt before: {account_data_before['totalDebtBase']}")

        # Create BorrowIntent
        intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="USDT0",
            collateral_amount=collateral_amount,
            borrow_token="USDG",
            borrow_amount=borrow_amount,
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )

        print("\nCreated BorrowIntent:")
        print(f"  Collateral: {intent.collateral_amount} {intent.collateral_token}")
        print(f"  Borrow: {intent.borrow_amount} {intent.borrow_token}")
        print(f"  Interest rate mode: {intent.interest_rate_mode}")

        # Compile intent
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        print("\nCompiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Execute via ExecutionOrchestrator
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts (Layer 3)
        borrow_parsed = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = AaveV3ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success:
                    if parse_result.supplies:
                        for supply_event in parse_result.supplies:
                            assert supply_event.amount > 0, "Supply event amount must be > 0"
                            print(f"  Supply (collateral): {supply_event.amount}")

                    if parse_result.borrows:
                        for borrow_event in parse_result.borrows:
                            assert borrow_event.amount > 0, "Borrow event amount must be > 0"
                            print(f"  Borrow amount: {borrow_event.amount}")
                            print(f"  Interest rate mode: {borrow_event.interest_rate_mode}")
                            borrow_parsed = True

        assert borrow_parsed, "At least one borrow event must be parsed from receipts"

        # Verify balance changes (Layer 4)
        usdt0_after = get_token_balance(web3, usdt0, funded_wallet)
        usdg_after = get_token_balance(web3, usdg_address, funded_wallet)

        usdt0_spent = usdt0_before - usdt0_after
        usdg_received = usdg_after - usdg_before

        print("\n--- Results ---")
        print(f"USDT0 spent (collateral): {format_token_amount(usdt0_spent, usdt0_decimals)}")
        print(f"USDG received (borrowed): {format_token_amount(usdg_received, usdg_decimals)}")

        # Verify USDT0 was spent as collateral
        expected_usdt0_spent = int(collateral_amount * Decimal(10**usdt0_decimals))
        assert usdt0_spent == expected_usdt0_spent, (
            f"USDT0 spent must EXACTLY equal collateral amount. "
            f"Expected: {expected_usdt0_spent}, Got: {usdt0_spent}"
        )

        # Verify USDG was received (allow small tolerance for Aave origination fees/rounding)
        expected_usdg_received = int(borrow_amount * Decimal(10**usdg_decimals))
        min_expected = int(expected_usdg_received * Decimal("0.99"))
        assert usdg_received >= min_expected, (
            f"USDG received must be at least 99% of borrow amount. "
            f"Expected min: {min_expected}, Got: {usdg_received}"
        )
        assert usdg_received <= expected_usdg_received, (
            f"USDG received should not exceed borrow amount. "
            f"Expected max: {expected_usdg_received}, Got: {usdg_received}"
        )

        # Verify account data changed
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Debt after: {account_data_after['totalDebtBase']}")
        print(f"Health factor: {account_data_after['healthFactor']}")

        assert account_data_after["totalDebtBase"] > 0, "Debt must be created"
        assert account_data_after["healthFactor"] > 1e18, "Health factor must be > 1.0"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_usdg_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test repaying USDG debt using RepayIntent.

        Flow:
        1. Setup: Borrow USDG with USDT0 collateral first
        2. Create RepayIntent to repay partial debt
        3. Compile and execute
        4. Verify USDG balance decreased and debt was reduced
        """
        # USDG address (Aave V3.6 borrow reserve on X-Layer)
        usdg_address = "0x4ae46a509F6b1D9056937BA4500cb143933D2dc8"
        usdg_decimals = 6

        # First borrow USDG
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        borrow_intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="USDT0",
            collateral_amount=Decimal("1000"),
            borrow_token="USDG",
            borrow_amount=Decimal("300"),
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )

        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle, execution_context)
        assert borrow_exec.success, f"Setup borrow failed: {borrow_exec.error}"

        # Now repay partial debt
        repay_amount = Decimal("100")

        print(f"\n{'='*80}")
        print(f"Test: Repay {repay_amount} USDG debt using RepayIntent (X-Layer)")
        print(f"{'='*80}")

        usdg_before = get_token_balance(web3, usdg_address, funded_wallet)
        print(f"USDG before repay: {format_token_amount(usdg_before, usdg_decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Debt before: {account_data_before['totalDebtBase']}")
        print(f"Health factor before: {account_data_before['healthFactor']}")

        # Create RepayIntent
        intent = RepayIntent(
            protocol="aave_v3",
            token="USDG",
            amount=repay_amount,
            chain=CHAIN_NAME,
        )

        print(f"\nCreated RepayIntent: token={intent.token}, amount={intent.amount}")

        # Compile and execute
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Parse receipts (Layer 3)
        repay_parsed = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = AaveV3ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                if parse_result.success and parse_result.repays:
                    for repay_event in parse_result.repays:
                        assert repay_event.amount > 0, "Repay event amount must be > 0"
                        repay_parsed = True

        assert repay_parsed, "At least one repay event must be parsed from receipts"

        # Verify balance changes
        usdg_after = get_token_balance(web3, usdg_address, funded_wallet)
        usdg_spent = usdg_before - usdg_after

        print(f"\nUSDG spent (repaid): {format_token_amount(usdg_spent, usdg_decimals)}")

        expected_usdg_spent = int(repay_amount * Decimal(10**usdg_decimals))
        assert usdg_spent == expected_usdg_spent, (
            f"USDG spent must EXACTLY equal repay amount. "
            f"Expected: {expected_usdg_spent}, Got: {usdg_spent}"
        )

        # Verify account data changed
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Debt after: {account_data_after['totalDebtBase']}")
        print(f"Health factor after: {account_data_after['healthFactor']}")

        assert account_data_after["totalDebtBase"] < account_data_before["totalDebtBase"], "Debt must decrease"
        assert (
            account_data_after["healthFactor"] > account_data_before["healthFactor"]
        ), "Health factor must improve"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_without_collateral_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test that borrowing far beyond available collateral fails gracefully.

        Uses a borrow amount (10M USDG) that exceeds any possible collateral
        from earlier tests, making this test order-independent.

        Note: in production the compile-time borrow-capacity pre-flight
        (PR #2129) catches this earlier via gateway eth_call. The intent
        test harness instantiates IntentCompiler without a gateway client,
        so the pre-flight is bypassed here and the on-chain Pool's
        rejection is what surfaces. Unit-test coverage for the pre-flight
        lives in tests/unit/intents/test_compiler_borrow_pre_flight_capacity.py.
        """
        # USDG address (Aave V3.6 borrow reserve on X-Layer)
        usdg_address = "0x4ae46a509F6b1D9056937BA4500cb143933D2dc8"

        print(f"\n{'='*80}")
        print("Test: BorrowIntent exceeding collateral (should fail)")
        print(f"{'='*80}")

        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt0 = tokens["USDT0"]
        usdt0_before = get_token_balance(web3, usdt0, funded_wallet)
        usdg_before = get_token_balance(web3, usdg_address, funded_wallet)

        # Borrow far more than any residual collateral can cover (order-independent)
        intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="USDT0",
            collateral_amount=Decimal("0"),  # No new collateral
            borrow_token="USDG",
            borrow_amount=Decimal("10000000"),  # 10M USDG — exceeds any prior collateral
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

        assert not execution_result.success, "Execution should fail without collateral"
        print(f"Execution failed as expected: {execution_result.error}")

        # Verify bilateral conservation (both tokens unchanged)
        usdt0_after = get_token_balance(web3, usdt0, funded_wallet)
        usdg_after = get_token_balance(web3, usdg_address, funded_wallet)
        assert usdt0_after == usdt0_before, "USDT0 balance must be unchanged after failed borrow"
        assert usdg_after == usdg_before, "USDG balance must be unchanged after failed borrow"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
