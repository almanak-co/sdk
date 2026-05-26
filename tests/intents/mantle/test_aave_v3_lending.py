"""Production-grade lending intent tests for Aave V3 on Mantle.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for lending operations:
1. Create lending intents (SupplyIntent, BorrowIntent, RepayIntent)
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using AaveV3ReceiptParser
5. Verify balance changes and account data are correct

Aave V3 Pool on Mantle: 0x458F293454fE0d67EC0655f3672301301DD51422

Mantle Aave V3 reserve configuration (verified on-chain via getReserveConfigurationData):

    Token   active  frozen  ltv   borrowable  collateral
    WETH    true    TRUE    0     true        false       <- frozen, supply reverts (#2102)
    WMNT    true    false   4000  false       true        <- only collateral with LTV>0
    USDC    true    false   0     true        false       <- borrow-only (LTV 0)
    USDT0   true    false   0     true        false       <- borrow-only
    USDe    true    false   0     true        false
    GHO     true    false   0     true        false

Tests use:
- Supply/withdraw: USDC (active, non-frozen).
- Borrow/repay: WMNT collateral (only reserve with non-zero LTV) + USDC borrow asset.

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/mantle/test_aave_v3_lending.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.aave_v3.adapter import (
    AAVE_V3_POOL_ADDRESSES,
)
from almanak.connectors.aave_v3.receipt_parser import AaveV3ReceiptParser
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

CHAIN_NAME = "mantle"

# Aave V3 Pool ABI (minimal - getUserAccountData + getReserveData for aToken
# address lookup, used by the Layer-4b aToken-balance receiver-side check).
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
    {
        "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
        "name": "getReserveData",
        "outputs": [
            {
                "components": [
                    {"internalType": "uint256", "name": "data", "type": "uint256"},
                ],
                "internalType": "struct DataTypes.ReserveConfigurationMap",
                "name": "configuration",
                "type": "tuple",
            },
            {"internalType": "uint128", "name": "liquidityIndex", "type": "uint128"},
            {"internalType": "uint128", "name": "currentLiquidityRate", "type": "uint128"},
            {"internalType": "uint128", "name": "variableBorrowIndex", "type": "uint128"},
            {"internalType": "uint128", "name": "currentVariableBorrowRate", "type": "uint128"},
            {"internalType": "uint128", "name": "currentStableBorrowRate", "type": "uint128"},
            {"internalType": "uint40", "name": "lastUpdateTimestamp", "type": "uint40"},
            {"internalType": "uint16", "name": "id", "type": "uint16"},
            {"internalType": "address", "name": "aTokenAddress", "type": "address"},
            {"internalType": "address", "name": "stableDebtTokenAddress", "type": "address"},
            {"internalType": "address", "name": "variableDebtTokenAddress", "type": "address"},
            {"internalType": "address", "name": "interestRateStrategyAddress", "type": "address"},
            {"internalType": "uint128", "name": "accruedToTreasury", "type": "uint128"},
            {"internalType": "uint128", "name": "unbacked", "type": "uint128"},
            {"internalType": "uint128", "name": "isolationModeTotalDebt", "type": "uint128"},
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


def get_atoken_address(web3: Web3, asset: str) -> str:
    """Look up the aToken address for an Aave V3 reserve on Mantle.

    USDC has LTV=0 on Mantle, so ``totalCollateralBase`` from
    ``getUserAccountData`` doesn't move on supply/withdraw — using the
    aToken's ERC-20 ``balanceOf`` instead gives the test a real Layer-4b
    receiver-side check that doesn't rely on collateral accounting.
    """
    pool_address = AAVE_V3_POOL_ADDRESSES[CHAIN_NAME]
    pool_contract = web3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=AAVE_POOL_ABI)
    reserve_data = pool_contract.functions.getReserveData(Web3.to_checksum_address(asset)).call()
    # aTokenAddress is the 9th field (index 8) in ReserveData.
    return reserve_data[8]


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

    Uses USDC (active, non-frozen) as the supply token. WETH is intentionally
    avoided because the WETH reserve is frozen on Mantle Aave V3 (#2102).
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
        """Test USDC supply to Aave V3 on Mantle.

        4-Layer Verification:
        1. Compilation: SupplyIntent -> ActionBundle (SUCCESS)
        2. Execution: on-chain transactions succeed
        3. Receipt Parsing: Supply event parsed with correct amount
        4. Balance Deltas: USDC decreased AND aUSDC (the Aave receipt token)
           increased by the supply amount. USDC has LTV=0 on Mantle so
           ``totalCollateralBase`` doesn't move — the aToken-balance check
           is the real receiver-side signal.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)
        ausdc = get_atoken_address(web3, usdc)

        supply_amount = Decimal("100")  # 100 USDC

        print(f"\n{'=' * 80}")
        print(f"Test: Supply {supply_amount} USDC to Aave V3 on Mantle")
        print(f"{'=' * 80}")

        # Layer 4a: Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        ausdc_before = get_token_balance(web3, ausdc, funded_wallet)
        print(f"USDC before: {format_token_amount(usdc_before, decimals)}")
        print(f"aUSDC before: {format_token_amount(ausdc_before, decimals)}")

        # Layer 1: Create and compile SupplyIntent
        # use_as_collateral=False is required: USDC has LTV=0 on Mantle Aave
        # V3, so setUserUseReserveAsCollateral(USDC, true) reverts with
        # UnderlyingCannotBeUsedAsCollateral. The supply itself succeeds; only
        # the auto-toggle fails.
        intent = SupplyIntent(
            protocol="aave_v3",
            token="USDC",
            amount=supply_amount,
            use_as_collateral=False,
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

        # Layer 4b: Verify balance changes — bilateral check.
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        ausdc_after = get_token_balance(web3, ausdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        ausdc_received = ausdc_after - ausdc_before

        print("\n--- Results ---")
        print(f"USDC spent: {format_token_amount(usdc_spent, decimals)}")
        print(f"aUSDC received: {format_token_amount(ausdc_received, decimals)}")

        expected_usdc_spent = int(supply_amount * Decimal(10**decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal supply amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        # aToken balance is index-scaled on each accrual; we assert the
        # delta is at least supply_amount minus a 1-wei rounding tolerance.
        assert ausdc_received >= expected_usdc_spent - 1, (
            f"aUSDC received must be ≥ supply amount (modulo 1-wei index "
            f"rounding). Expected ≥ {expected_usdc_spent - 1}, Got: {ausdc_received}"
        )

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

        4-Layer Verification:
        1. Compilation: WithdrawIntent -> ActionBundle (SUCCESS)
        2. Execution: on-chain transactions succeed
        3. Receipt Parsing: Withdraw event parsed
        4. Balance Deltas: USDC increased AND aUSDC decreased by ~the
           withdraw amount. USDC has LTV=0 on Mantle so
           ``totalCollateralBase`` doesn't move — the aToken-balance check
           is the real receiver-side signal.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)
        ausdc = get_atoken_address(web3, usdc)

        # First supply 200 USDC
        supply_amount = Decimal("200")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        # use_as_collateral=False is required: USDC has LTV=0 on Mantle Aave
        # V3 — see test_supply_usdc_using_intent for full rationale.
        supply_intent = SupplyIntent(
            protocol="aave_v3",
            token="USDC",
            amount=supply_amount,
            use_as_collateral=False,
            chain=CHAIN_NAME,
        )

        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec_result = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_exec_result.success, f"Initial supply failed: {supply_exec_result.error}"

        # Now withdraw 100 USDC
        withdraw_amount = Decimal("100")

        print(f"\n{'=' * 80}")
        print(f"Test: Withdraw {withdraw_amount} USDC from Aave V3 on Mantle")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        ausdc_before = get_token_balance(web3, ausdc, funded_wallet)
        print(f"USDC before withdraw: {format_token_amount(usdc_before, decimals)}")
        print(f"aUSDC before withdraw: {format_token_amount(ausdc_before, decimals)}")

        # Layer 1: Create and compile WithdrawIntent
        intent = WithdrawIntent(
            protocol="aave_v3",
            token="USDC",
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
                if parse_result.success and parse_result.withdraws:
                    withdraw_parsed = True
                    for withdraw_event in parse_result.withdraws:
                        assert withdraw_event.amount > 0, "Withdraw amount must be > 0"
                        print(f"  Withdraw amount: {withdraw_event.amount}")

        assert withdraw_parsed, "Must find at least one Withdraw event in receipts"

        # Layer 4b: Verify balance changes — bilateral check.
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        ausdc_after = get_token_balance(web3, ausdc, funded_wallet)
        usdc_received = usdc_after - usdc_before
        ausdc_burned = ausdc_before - ausdc_after

        print(f"\nUSDC received: {format_token_amount(usdc_received, decimals)}")
        print(f"aUSDC burned: {format_token_amount(ausdc_burned, decimals)}")

        expected_usdc_received = int(withdraw_amount * Decimal(10**decimals))
        assert usdc_received == expected_usdc_received, (
            f"USDC received must EXACTLY equal withdraw amount. "
            f"Expected: {expected_usdc_received}, Got: {usdc_received}"
        )
        # aUSDC burned should be at least the withdraw amount (modulo the
        # 1-wei rounding from index-scaled aToken accounting).
        assert ausdc_burned >= expected_usdc_received - 1, (
            f"aUSDC burned must be ≥ withdraw amount (modulo 1-wei index "
            f"rounding). Expected ≥ {expected_usdc_received - 1}, "
            f"Got: {ausdc_burned}"
        )

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
        """Test that SupplyIntent with insufficient balance fails gracefully.

        3-Layer Verification (failure mode):
        1. Compilation: succeeds (doesn't check balance)
        2. Execution: should fail on-chain
        3. Balance Conservation: USDC balance unchanged
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        # Get current balance — if 0, surface as a fixture/funding regression
        # rather than silently exercising zero-amount behaviour.
        usdc_balance = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_balance > 0, (
            "funded_wallet must have USDC seeded; zero balance indicates a "
            "fixture / Safe-funding regression rather than a real test scenario"
        )
        balance_decimal = Decimal(usdc_balance) / Decimal(10**decimals)

        # Try to supply more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'=' * 80}")
        print("Test: SupplyIntent with Insufficient Balance")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        # use_as_collateral=False — USDC has LTV=0 on Mantle Aave V3.
        intent = SupplyIntent(
            protocol="aave_v3",
            token="USDC",
            amount=excessive_amount,
            use_as_collateral=False,
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
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_balance, "Balance must be unchanged after failed supply"

        print("\nALL CHECKS PASSED")


# =============================================================================
# Borrow Tests
# =============================================================================


@pytest.mark.mantle
@pytest.mark.borrow
@pytest.mark.lending
class TestAaveV3BorrowIntent:
    """Test Aave V3 borrow operations on Mantle.

    Uses WMNT as collateral (LTV=40%, the only Mantle Aave reserve with
    non-zero LTV) and borrows USDC. WETH is frozen on Mantle Aave V3 (#2102),
    USDC has LTV=0 so it cannot be used as collateral, leaving WMNT as the
    sole viable collateral asset.
    """

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_usdc_with_wmnt_collateral(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC borrow after supplying WMNT as collateral.

        Done in two intents because Aave V3's BorrowIntent compiler emits
        ``approve + supply + borrow`` but does NOT emit
        ``setUserUseReserveAsCollateral`` — and on Mantle's Aave V3
        deployment, supplying WMNT does not auto-enable it as collateral
        (likely because of isolation-mode / debt-ceiling configuration), so
        the subsequent borrow reverts. The fix is to issue a SupplyIntent
        first with ``use_as_collateral=True`` (which DOES emit the explicit
        toggle) and then a borrow-only BorrowIntent with
        ``collateral_amount=0``.

        WMNT has LTV=40% on Mantle. With CoinGecko WMNT ≈ $0.65 (CI run
        showed $0.648), 200 WMNT collateral ≈ $130 supports up to ~$52 USDC
        borrow at 40% LTV. We borrow 20 USDC (~38% utilization of LTV cap,
        ~15% effective LTV — well under the 30% intent-tests cap).

        4-Layer Verification:
        1. Compilation: SupplyIntent + BorrowIntent each → ActionBundle (SUCCESS)
        2. Execution: both on-chain bundles succeed
        3. Receipt Parsing: Supply (WMNT) and Borrow (USDC) events parsed
        4. Balance Deltas: WMNT decreased by collateral, USDC increased by
           borrow, totalDebtBase increased, healthFactor > 1e18
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wmnt = tokens["WMNT"]
        usdc = tokens["USDC"]
        wmnt_decimals = get_token_decimals(web3, wmnt)
        usdc_decimals = get_token_decimals(web3, usdc)

        collateral_amount = Decimal("200")  # 200 WMNT (~$130 at $0.65/WMNT)
        borrow_amount = Decimal("20")  # 20 USDC (~15% effective LTV)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        print(f"\n{'=' * 80}")
        print("Test: Borrow USDC with WMNT collateral on Aave V3 (Mantle)")
        print(f"{'=' * 80}")

        # Layer 4a: Record balances BEFORE
        wmnt_before = get_token_balance(web3, wmnt, funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"WMNT before: {format_token_amount(wmnt_before, wmnt_decimals)}")
        print(f"USDC before borrow: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"Debt before: {account_data_before['totalDebtBase']}")

        # Step 1: SupplyIntent with use_as_collateral=True — emits the
        # explicit setUserUseReserveAsCollateral(WMNT, true) call required
        # to make WMNT count as collateral on Mantle Aave V3.
        supply_intent = SupplyIntent(
            protocol="aave_v3",
            token="WMNT",
            amount=collateral_amount,
            use_as_collateral=True,
            chain=CHAIN_NAME,
        )
        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS", f"Supply compilation failed: {supply_result.error}"
        assert supply_result.action_bundle is not None
        print(f"\nStep 1 — Supply ActionBundle: {len(supply_result.action_bundle.transactions)} transactions")
        supply_exec = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_exec.success, f"Supply execution failed: {supply_exec.error}"

        # Step 2: BorrowIntent with collateral_amount=0 — borrows USDC
        # against existing collateral (the WMNT just supplied).
        borrow_intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="WMNT",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )

        compilation_result = compiler.compile(borrow_intent)
        assert compilation_result.status.value == "SUCCESS", f"Borrow compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        print(f"Step 2 — Borrow ActionBundle: {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: Execute borrow
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Borrow execution failed: {execution_result.error}"
        print(f"Borrow successful! {len(execution_result.transaction_results)} transactions")

        # Layer 3: Parse receipts across BOTH bundles (supply + borrow).
        supply_parsed = False
        for tx_result in supply_exec.transaction_results:
            if tx_result.receipt:
                parser = AaveV3ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                if parse_result.success and parse_result.supplies:
                    supply_parsed = True
                    for supply_event in parse_result.supplies:
                        assert supply_event.amount > 0, "Supply amount must be > 0"
                        print(f"  Supply amount: {supply_event.amount}")
        assert supply_parsed, "Must find at least one Supply event in supply bundle"

        borrow_parsed = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = AaveV3ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                if parse_result.success and parse_result.borrows:
                    borrow_parsed = True
                    for borrow_event in parse_result.borrows:
                        assert borrow_event.amount > 0, "Borrow amount must be > 0"
                        print(f"  Borrow amount: {borrow_event.amount}")
                        print(f"  Reserve: {borrow_event.reserve}")

        assert borrow_parsed, "Must find at least one Borrow event in borrow bundle"

        # Layer 4b: Verify balance changes (across both bundles)
        wmnt_after = get_token_balance(web3, wmnt, funded_wallet)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        wmnt_spent = wmnt_before - wmnt_after
        usdc_received = usdc_after - usdc_before

        print("\n--- Results ---")
        print(f"WMNT spent (collateral): {format_token_amount(wmnt_spent, wmnt_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, usdc_decimals)}")

        expected_wmnt_spent = int(collateral_amount * Decimal(10**wmnt_decimals))
        assert wmnt_spent == expected_wmnt_spent, (
            f"WMNT spent must EXACTLY equal collateral amount. "
            f"Expected: {expected_wmnt_spent}, Got: {wmnt_spent}"
        )

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

    Verifies WMNT collateral supply -> USDC borrow -> USDC repay flow.
    """

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_usdc_after_borrow(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC repay after WMNT-collateralised borrow on Mantle.

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
        print("Test: Repay USDC after WMNT collateral + USDC borrow on Aave V3 (Mantle)")
        print(f"{'=' * 80}")

        # Step 1a: SupplyIntent with use_as_collateral=True so that
        # setUserUseReserveAsCollateral(WMNT, true) actually fires (Mantle's
        # Aave V3 does NOT auto-enable WMNT as collateral on supply — see
        # test_borrow_usdc_with_wmnt_collateral for the rationale).
        print("\nStep 1a: Supplying 200 WMNT as collateral...")
        supply_intent = SupplyIntent(
            protocol="aave_v3",
            token="WMNT",
            amount=Decimal("200"),
            use_as_collateral=True,
            chain=CHAIN_NAME,
        )
        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_exec.success, f"Supply failed: {supply_exec.error}"

        # Step 1b: BorrowIntent against the just-supplied WMNT collateral.
        print("Step 1b: Borrowing 10 USDC...")
        borrow_intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="WMNT",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("10"),
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )
        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle, execution_context)
        assert borrow_exec.success, f"Borrow failed: {borrow_exec.error}"
        print("Supply + Borrow successful!")

        # Step 2: Repay 10 USDC
        repay_amount = Decimal("10")
        print(f"\nStep 2: Repaying {repay_amount} USDC...")

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

        # Try to borrow 10000 USDC with zero collateral. Even if a prior test
        # left residual WMNT collateral, 10k USDC borrow should massively
        # exceed any available borrowing power (4k+ WMNT @ 40% LTV needed).
        intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="WMNT",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("10000"),
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
