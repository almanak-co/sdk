"""Production-grade lending intent tests for Radiant V2 on Ethereum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for lending operations:
1. Create lending intents (SupplyIntent, WithdrawIntent, BorrowIntent, RepayIntent)
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using RadiantV2ReceiptParser
5. Verify balance changes and account data are correct

Radiant V2 is an Aave V2 fork. The framework routes radiant_v2 through the
shared Aave V2 lending-pool path (see ``AAVE_V2_FORKS`` and
``AAVE_V2_DEPOSIT_SELECTOR`` in ``compiler_constants``). The on-chain
``getUserAccountData`` ABI is identical to Aave V3 (same 6-tuple), so the
minimal pool ABI from the Aave V3 reference is reused below.

Issue #1900 originally targeted Arbitrum. The Radiant V2 Arbitrum pool was
frozen following the October 2024 Radiant Capital hack, so the framework's
``SUPPORTED_PROTOCOLS["radiant_v2"]`` is now ``{"ethereum"}`` only. These
tests run against the Ethereum LendingPool at
``0xA950974f64aA33f27F6C5e017eEE93BF7588ED07``.

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/ethereum/test_radiant_v2_lending.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.radiant_v2.receipt_parser import RadiantV2ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.compiler_constants import LENDING_POOL_ADDRESSES
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

CHAIN_NAME = "ethereum"

# Radiant V2 LendingPool ABI (minimal — just ``getUserAccountData``).
# Radiant V2 is an Aave V2 fork; its ``getUserAccountData`` returns the same
# 6-tuple as Aave V3, so the ABI shape is identical (renamed for clarity).
RADIANT_V2_POOL_ABI = [
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
    """Get user account data from the Radiant V2 LendingPool contract."""
    pool_address = LENDING_POOL_ADDRESSES[CHAIN_NAME]["radiant_v2"]
    pool_contract = web3.eth.contract(
        address=Web3.to_checksum_address(pool_address),
        abi=RADIANT_V2_POOL_ABI,
    )

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
        simulation_enabled=True,  # Enable simulation to use LocalSimulator's gas estimates
    )


# =============================================================================
# Supply/Withdraw Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.supply
@pytest.mark.lending
class TestRadiantV2SupplyIntent:
    """Test Radiant V2 supply/withdraw operations using SupplyIntent and WithdrawIntent.

    These tests verify the full Intent flow:
    - SupplyIntent / WithdrawIntent creation with token symbols and amounts
    - IntentCompiler generates correct Radiant V2 (Aave V2 fork) transactions
    - Transactions execute successfully on-chain
    - RadiantV2ReceiptParser correctly interprets results (parses Aave V2's
      ``Deposit`` event into ``parse_result.supplies``)
    - Balance changes and account data match expected amounts
    """

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC supply using SupplyIntent.

        Flow:
        1. Create SupplyIntent for USDC
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify balances and account data changed correctly
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("1000")  # 1000 USDC

        print(f"\n{'='*80}")
        print(f"Test: Supply {supply_amount} USDC to Radiant V2 using SupplyIntent")
        print(f"{'='*80}")

        # Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before: {format_token_amount(usdc_before, decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Collateral before: {account_data_before['totalCollateralBase']}")

        # Create SupplyIntent
        intent = SupplyIntent(
            protocol="radiant_v2",
            token="USDC",
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

        # Layer 3: Parse receipts and verify Deposit (V2) event was emitted.
        # NOTE: Radiant V2 emits ``Deposit`` (Aave V2 ABI), not Aave V3's
        # ``Supply`` — but the parser exposes deposits via ``parse_result.supplies``
        # (the dataclass is ``DepositEventData``).
        deposit_seen = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = RadiantV2ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.supplies:
                    deposit_seen = True
                    for supply_event in parse_result.supplies:
                        print(f"  Deposit amount: {supply_event.amount}")
                        print(f"  Reserve: {supply_event.reserve}")
                        assert supply_event.amount > 0, "Parsed Deposit amount must be > 0"
                        assert (
                            supply_event.reserve.lower() == usdc.lower()
                        ), f"Deposit reserve must be USDC. Expected {usdc.lower()}, got {supply_event.reserve.lower()}"

        assert deposit_seen, "RadiantV2ReceiptParser must surface at least one Deposit event"

        # Layer 4: Verify exact balance delta
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print("\n--- Results ---")
        print(f"USDC spent: {format_token_amount(usdc_spent, decimals)}")

        expected_usdc_spent = int(supply_amount * Decimal(10**decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal supply amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify account data changed (Radiant Ethereum has positive LTV on USDC,
        # so totalCollateralBase strictly increases — unlike Spark where USDC has
        # zero LTV and only totalDebtBase moves).
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Collateral after: {account_data_after['totalCollateralBase']}")

        assert (
            account_data_after["totalCollateralBase"] > account_data_before["totalCollateralBase"]
        ), "Collateral must increase after supply"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC withdraw using WithdrawIntent (after supplying).

        Flow:
        1. Supply USDC first
        2. Create WithdrawIntent to withdraw portion
        3. Compile and execute
        4. Verify balances changed correctly
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        # First supply 2000 USDC
        supply_amount = Decimal("2000")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        supply_intent = SupplyIntent(
            protocol="radiant_v2",
            token="USDC",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )

        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec_result = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_exec_result.success, f"Initial supply failed: {supply_exec_result.error}"

        # Now withdraw 1000 USDC
        withdraw_amount = Decimal("1000")

        print(f"\n{'='*80}")
        print(f"Test: Withdraw {withdraw_amount} USDC from Radiant V2 using WithdrawIntent")
        print(f"{'='*80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before withdraw: {format_token_amount(usdc_before, decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Collateral before: {account_data_before['totalCollateralBase']}")

        # Create WithdrawIntent
        intent = WithdrawIntent(
            protocol="radiant_v2",
            token="USDC",
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

        # Layer 3: Parse receipts and verify Withdraw event was emitted
        withdraw_seen = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = RadiantV2ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.withdraws:
                    withdraw_seen = True
                    for withdraw_event in parse_result.withdraws:
                        print(f"  Withdraw amount: {withdraw_event.amount}")
                        print(f"  Reserve: {withdraw_event.reserve}")
                        assert withdraw_event.amount > 0, "Parsed Withdraw amount must be > 0"
                        assert (
                            withdraw_event.reserve.lower() == usdc.lower()
                        ), f"Withdraw reserve must be USDC. Got {withdraw_event.reserve.lower()}"

        assert withdraw_seen, "RadiantV2ReceiptParser must surface at least one Withdraw event"

        # Layer 4: Verify exact balance delta
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before

        print(f"\nUSDC received: {format_token_amount(usdc_received, decimals)}")

        expected_usdc_received = int(withdraw_amount * Decimal(10**decimals))
        assert usdc_received == expected_usdc_received, (
            f"USDC received must EXACTLY equal withdraw amount. "
            f"Expected: {expected_usdc_received}, Got: {usdc_received}"
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
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        # Get current balance
        usdc_balance = get_token_balance(web3, usdc, funded_wallet)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**decimals)

        # Try to supply more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'='*80}")
        print("Test: SupplyIntent with Insufficient Balance")
        print(f"{'='*80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        # Create SupplyIntent with excessive amount
        intent = SupplyIntent(
            protocol="radiant_v2",
            token="USDC",
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

        # Verify balance unchanged (balance conservation)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_balance, "Balance must be unchanged after failed supply"

        print("\nALL CHECKS PASSED")


# =============================================================================
# Borrow/Repay Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.borrow
@pytest.mark.lending
class TestRadiantV2BorrowIntent:
    """Test Radiant V2 borrow/repay operations using BorrowIntent and RepayIntent.

    Radiant V2 Ethereum is live and unfrozen — these tests run end-to-end
    (no xfail). The Arbitrum pool was frozen post the October 2024 hack;
    framework support is therefore Ethereum-only.
    """

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_usdc_with_weth_collateral_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test borrowing USDC with WETH collateral using BorrowIntent.

        Flow:
        1. Create BorrowIntent with WETH as collateral, borrowing USDC
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify USDC balance increased and debt was created

        Sized at 1 WETH collateral / 500 USDC borrow (~17-20% LTV at current
        ETH price, well under the 30% cap mandated by the test plan).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth = tokens["WETH"]
        usdc = tokens["USDC"]

        weth_decimals = get_token_decimals(web3, weth)
        usdc_decimals = get_token_decimals(web3, usdc)

        # Supply 1 WETH as collateral, borrow 500 USDC (~17-20% LTV)
        collateral_amount = Decimal("1")
        borrow_amount = Decimal("500")

        print(f"\n{'='*80}")
        print(f"Test: Borrow {borrow_amount} USDC with {collateral_amount} WETH collateral using BorrowIntent")
        print(f"{'='*80}")

        # Record balances BEFORE
        weth_before = get_token_balance(web3, weth, funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Debt before: {account_data_before['totalDebtBase']}")

        # Create BorrowIntent (variable rate is the only supported mode for V2)
        intent = BorrowIntent(
            protocol="radiant_v2",
            collateral_token="WETH",
            collateral_amount=collateral_amount,
            borrow_token="USDC",
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

        # Layer 3: Parse receipts and verify both Deposit and Borrow events emitted.
        # Aggregate parsed events across all transaction receipts so we can index
        # them by reserve (rather than blindly trusting events[0]).
        all_supplies = []
        all_borrows = []
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = RadiantV2ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success:
                    all_supplies.extend(parse_result.supplies)
                    all_borrows.extend(parse_result.borrows)

        # Index by reserve — must surface a Deposit event for the WETH collateral.
        collateral_supply = next(
            (s for s in all_supplies if s.reserve.lower() == weth.lower()),
            None,
        )
        assert collateral_supply is not None, (
            f"Expected a Deposit event for WETH collateral. "
            f"Got reserves: {[s.reserve for s in all_supplies]}"
        )
        assert collateral_supply.amount > 0, "WETH collateral deposit amount must be > 0"

        # Index by reserve — must surface a Borrow event for USDC.
        usdc_borrow = next(
            (b for b in all_borrows if b.reserve.lower() == usdc.lower()),
            None,
        )
        assert usdc_borrow is not None, (
            f"Expected a Borrow event for USDC. "
            f"Got reserves: {[b.reserve for b in all_borrows]}"
        )
        assert usdc_borrow.amount > 0, "USDC borrow amount must be > 0"
        assert usdc_borrow.interest_rate_mode == 2, "Borrow must be variable rate"

        # Layer 4: Verify balance changes
        weth_after = get_token_balance(web3, weth, funded_wallet)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_received = usdc_after - usdc_before

        print("\n--- Results ---")
        print(f"WETH spent (collateral): {format_token_amount(weth_spent, weth_decimals)}")
        print(f"USDC received (borrowed): {format_token_amount(usdc_received, usdc_decimals)}")

        # Verify WETH was spent as collateral (exact)
        expected_weth_spent = int(collateral_amount * Decimal(10**weth_decimals))
        assert weth_spent == expected_weth_spent, (
            f"WETH spent must EXACTLY equal collateral amount. "
            f"Expected: {expected_weth_spent}, Got: {weth_spent}"
        )

        # Compare wallet delta to the parsed Borrow event amount EXACTLY — Radiant's
        # on-chain borrow amount is the source of truth, not our test's expected value.
        assert usdc_received == int(usdc_borrow.amount), (
            f"USDC received from borrow must equal the parsed Borrow event amount. "
            f"Wallet delta: {usdc_received}, parsed: {int(usdc_borrow.amount)}"
        )
        # And the borrow event amount itself must be >= 99% of the requested borrow
        # (Radiant may round down by a few wei but should not materially short us).
        expected_borrow = int(borrow_amount * Decimal(10**usdc_decimals))
        assert int(usdc_borrow.amount) >= expected_borrow * 99 // 100, (
            f"Borrow amount materially shorter than requested. "
            f"Requested: {expected_borrow}, got: {int(usdc_borrow.amount)}"
        )

        # Verify account data changed
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Debt after: {account_data_after['totalDebtBase']}")
        print(f"Health factor: {account_data_after['healthFactor']}")

        assert (
            account_data_after["totalDebtBase"] > account_data_before["totalDebtBase"]
        ), "Debt must be created after borrow"
        # Use integer 10**18 (uint256-safe) instead of float 1e18 to avoid
        # 64-bit float precision loss on healthFactor comparisons.
        assert account_data_after["healthFactor"] > 10**18, "Health factor must be > 1.0"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test repaying USDC debt using RepayIntent.

        Flow:
        1. Setup: Borrow USDC with WETH collateral first (1 WETH / 500 USDC)
        2. Create RepayIntent to repay partial debt (200 USDC)
        3. Compile and execute
        4. Verify USDC balance decreased and debt was reduced
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        # First borrow USDC against WETH collateral
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        borrow_intent = BorrowIntent(
            protocol="radiant_v2",
            collateral_token="WETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )

        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_exec_result = await orchestrator.execute(borrow_result.action_bundle, execution_context)
        assert borrow_exec_result.success, f"Initial borrow failed: {borrow_exec_result.error}"

        # Now repay partial debt
        repay_amount = Decimal("200")

        print(f"\n{'='*80}")
        print(f"Test: Repay {repay_amount} USDC debt using RepayIntent")
        print(f"{'='*80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before repay: {format_token_amount(usdc_before, usdc_decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Debt before: {account_data_before['totalDebtBase']}")
        print(f"Health factor before: {account_data_before['healthFactor']}")

        # Create RepayIntent
        intent = RepayIntent(
            protocol="radiant_v2",
            token="USDC",
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

        # Layer 3: Parse receipts and verify Repay event was emitted with the
        # funded wallet as the repayer. Aggregate across all receipts and index
        # by reserve.
        all_repays = []
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = RadiantV2ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success:
                    all_repays.extend(parse_result.repays)

        usdc_repay = next(
            (r for r in all_repays if r.reserve.lower() == usdc.lower()),
            None,
        )
        assert usdc_repay is not None, (
            f"Expected a Repay event for USDC. "
            f"Got reserves: {[r.reserve for r in all_repays]}"
        )
        assert usdc_repay.amount > 0, "USDC repay amount must be > 0"
        assert usdc_repay.repayer.lower() == funded_wallet.lower(), (
            "Repayer must be the funded wallet"
        )

        # Layer 4: Verify exact balance delta
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print(f"\nUSDC spent (repaid): {format_token_amount(usdc_spent, usdc_decimals)}")

        expected_usdc_spent = int(repay_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal repay amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify account data changed
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Debt after: {account_data_after['totalDebtBase']}")
        print(f"Health factor after: {account_data_after['healthFactor']}")

        assert (
            account_data_after["totalDebtBase"] < account_data_before["totalDebtBase"]
        ), "Debt must decrease after repay"
        assert (
            account_data_after["healthFactor"] > account_data_before["healthFactor"]
        ), "Health factor must improve after repay"

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
        """Test that borrowing without collateral fails gracefully."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        weth = tokens["WETH"]

        print(f"\n{'='*80}")
        print("Test: BorrowIntent without Collateral (should fail)")
        print(f"{'='*80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)

        # Try to borrow without supplying collateral (collateral_amount = 0)
        intent = BorrowIntent(
            protocol="radiant_v2",
            collateral_token="WETH",
            collateral_amount=Decimal("0"),  # No collateral
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

        assert not execution_result.success, "Execution should fail without collateral"
        print(f"Execution failed as expected: {execution_result.error}")

        # Verify no USDC received (balance conservation)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed borrow"

        # Verify WETH conservation as well — neither relevant token may move
        # on a failed borrow.
        weth_after = get_token_balance(web3, weth, funded_wallet)
        assert weth_after == weth_before, (
            f"WETH balance must be unchanged after failed borrow. "
            f"Before: {weth_before}, After: {weth_after}"
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
