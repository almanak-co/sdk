"""Production-grade lending intent tests for Aave V3 on Linea.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for lending
operations:
1. Create lending intents (SupplyIntent, WithdrawIntent, BorrowIntent, RepayIntent)
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline, default-on Zodiac)
4. Parse receipts using AaveV3ReceiptParser
5. Verify balance changes and account data are correct

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

Chain-specific direction (VIB-5916):
    Unlike the ethereum / arbitrum / base / avalanche suites (which supply an
    ETH-correlated asset and borrow USDC), Linea's Aave V3 market has its USDC
    **and** USDT borrow caps EXCEEDED at head — any new USDC/USDT borrow reverts
    with ``BORROW_CAP_EXCEEDED``. WETH still has borrow headroom (~1.24 WETH at
    the fork block). The direction is therefore inverted here: collateral = USDC
    (active, unfrozen, 75% LTV, collateral-enabled), borrow = WETH, kept small
    (<=0.1 WETH) so the tests stay well inside the WETH borrow headroom. This
    mirrors how the base suite substitutes wstETH collateral for the frozen WETH
    reserve — the *categories* (supply / withdraw / borrow / repay + two failure
    paths) are identical to the sibling Aave chains; only the on-chain-forced
    token roles differ.

To run:
    uv run pytest tests/intents/linea/test_aave_v3_lending.py -v -s
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

CHAIN_NAME = "linea"

# Maximum LTV any borrow test may target. Kept well below Aave's 75% USDC
# liquidation LTV so live-price drift can't push the position toward
# liquidation mid-test (see .claude/rules/intent-tests.md §"Borrow Amount LTV
# Cap").
MAX_TEST_LTV = Decimal("0.30")
TARGET_BORROW_LTV = Decimal("0.18")
# Hard cap on the borrowed WETH regardless of price — Linea's WETH reserve has
# only ~1.24 WETH of borrow headroom at the fork block, so every test stays far
# under it.
MAX_BORROW_WETH = Decimal("0.1")

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


def compute_weth_borrow_amount(
    price_oracle: dict[str, Decimal],
    collateral_usdc: Decimal,
) -> Decimal:
    """Derive a WETH borrow amount targeting ~18% LTV from LIVE oracle prices.

    Never assumes a fixed WETH price — the price_oracle fixture is session-scoped
    live CoinGecko data, so a hard-coded amount would drift into a higher LTV (or
    the WETH borrow-cap ceiling) as prices move. Result is capped at
    ``MAX_BORROW_WETH`` and asserted to stay under ``MAX_TEST_LTV``.
    """
    weth_price = price_oracle["WETH"]
    usdc_price = price_oracle.get("USDC", Decimal("1"))
    assert weth_price > 0, f"Oracle returned non-positive WETH price: {weth_price}"

    collateral_usd = collateral_usdc * usdc_price
    raw_borrow_weth = (collateral_usd * TARGET_BORROW_LTV) / weth_price
    borrow_weth = min(raw_borrow_weth, MAX_BORROW_WETH).quantize(Decimal("0.0001"))
    assert borrow_weth > 0, "Computed WETH borrow amount must be positive"

    realized_ltv = (borrow_weth * weth_price) / collateral_usd
    assert realized_ltv <= MAX_TEST_LTV, (
        f"Computed borrow LTV {realized_ltv:.4f} exceeds cap {MAX_TEST_LTV}. "
        f"weth_price={weth_price}, collateral_usd={collateral_usd}"
    )
    return borrow_weth


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


@pytest.mark.linea
@pytest.mark.supply
@pytest.mark.lending
class TestAaveV3SupplyIntent:
    """Test Aave V3 supply/withdraw operations using SupplyIntent and WithdrawIntent.

    These tests verify the full Intent flow:
    - SupplyIntent creation with token symbols and amounts
    - IntentCompiler generates correct Aave V3 transactions
    - Transactions execute successfully on-chain
    - AaveV3ReceiptParser correctly interprets results
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
        """Test USDC supply using SupplyIntent (exact accounting).

        Flow:
        1. Create SupplyIntent for USDC
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify balances and account data changed correctly
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("500")  # 500 USDC

        print(f"\n{'='*80}")
        print(f"Test: Supply {supply_amount} USDC to Aave V3 using SupplyIntent")
        print(f"{'='*80}")

        # Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before: {format_token_amount(usdc_before, decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Collateral before: {account_data_before['totalCollateralBase']}")

        # Create SupplyIntent
        intent = SupplyIntent(
            protocol="aave_v3",
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

        # Execute via ExecutionOrchestrator
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: parse receipts and assert decoded Supply amount > 0
        parser = AaveV3ReceiptParser(chain=CHAIN_NAME)
        decoded_supply_amount = Decimal("0")
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                assert parse_result.success, (
                    f"Receipt parser failed on tx {tx_result.tx_hash}: {parse_result.error}"
                )
                for supply_event in parse_result.supplies:
                    print(f"  Supply amount:  {supply_event.amount}")
                    print(f"  Reserve: {supply_event.reserve}")
                    if supply_event.amount > decoded_supply_amount:
                        decoded_supply_amount = supply_event.amount

        assert decoded_supply_amount > 0, (
            "Layer 3: AaveV3ReceiptParser must decode at least one Supply event "
            "with amount > 0 from the supply transaction receipts"
        )

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print("\n--- Results ---")
        print(f"USDC spent: {format_token_amount(usdc_spent, decimals)}")

        # Verify USDC was spent EXACTLY
        expected_usdc_spent = int(supply_amount * Decimal(10**decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal supply amount. " f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify account data changed (collateral increased)
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Collateral after: {account_data_after['totalCollateralBase']}")

        assert (
            account_data_after["totalCollateralBase"] > account_data_before["totalCollateralBase"]
        ), "Collateral must increase after supply"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_all_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test withdrawing ALL supplied USDC using WithdrawIntent(withdraw_all=True).

        Flow:
        1. Supply USDC first
        2. Create WithdrawIntent with withdraw_all=True (Aave MAX_UINT256 path)
        3. Compile and execute
        4. Verify aToken/collateral back to zero and USDC principal recovered
           (principal + any accrued interest, within rounding)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        # First supply 500 USDC
        supply_amount = Decimal("500")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        supply_intent = SupplyIntent(
            protocol="aave_v3",
            token="USDC",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )

        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_execution_result = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_execution_result.success, f"Supply execution failed: {supply_execution_result.error}"

        print(f"\n{'='*80}")
        print("Test: Withdraw ALL USDC from Aave V3 using WithdrawIntent(withdraw_all=True)")
        print(f"{'='*80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before withdraw: {format_token_amount(usdc_before, decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Collateral before: {account_data_before['totalCollateralBase']}")
        assert account_data_before["totalCollateralBase"] > 0, "Precondition: must hold collateral before withdraw"

        # Create WithdrawIntent for the FULL balance
        intent = WithdrawIntent(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("0"),  # Ignored when withdraw_all=True
            withdraw_all=True,
            chain=CHAIN_NAME,
        )

        print(f"\nCreated WithdrawIntent: protocol={intent.protocol}, token={intent.token}, withdraw_all=True")

        # Compile and execute
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: parse receipts and assert decoded Withdraw amount > 0
        parser = AaveV3ReceiptParser(chain=CHAIN_NAME)
        decoded_withdraw_amount = Decimal("0")
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                assert parse_result.success, (
                    f"Receipt parser failed on tx {tx_result.tx_hash}: {parse_result.error}"
                )
                for w in parse_result.withdraws:
                    if w.amount > decoded_withdraw_amount:
                        decoded_withdraw_amount = w.amount

        assert decoded_withdraw_amount > 0, (
            "Layer 3: AaveV3ReceiptParser must decode at least one Withdraw event "
            "with amount > 0 from the withdraw transaction receipts"
        )

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before

        print(f"\nUSDC received: {format_token_amount(usdc_received, decimals)}")

        # Principal must be recovered; withdraw_all returns principal + accrued
        # interest, so >= principal and within a tiny tolerance (no MEV on Anvil,
        # only ~1 block of interest can accrue).
        expected_principal = int(supply_amount * Decimal(10**decimals))
        assert usdc_received >= expected_principal, (
            f"USDC received must recover at least the principal. "
            f"Expected >= {expected_principal}, Got: {usdc_received}"
        )
        max_expected = int(expected_principal * Decimal("1.001"))
        assert usdc_received <= max_expected, (
            f"USDC received unexpectedly exceeds principal + tolerance. "
            f"Expected <= {max_expected}, Got: {usdc_received}"
        )

        # Verify collateral / aToken fully drained to zero
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Collateral after: {account_data_after['totalCollateralBase']}")

        assert account_data_after["totalCollateralBase"] == 0, (
            "Collateral (aToken) must be zero after withdraw_all. "
            f"Got: {account_data_after['totalCollateralBase']}"
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
            protocol="aave_v3",
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

        # Verify balance conservation (unchanged after failed supply)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_balance, "Balance must be unchanged after failed supply"

        print("\nALL CHECKS PASSED")


# =============================================================================
# Borrow/Repay Tests
# =============================================================================


@pytest.mark.linea
@pytest.mark.borrow
@pytest.mark.lending
class TestAaveV3BorrowIntent:
    """Test Aave V3 borrow/repay operations using BorrowIntent and RepayIntent.

    These tests verify the full Intent flow:
    - BorrowIntent creation with collateral and borrow parameters
    - IntentCompiler generates correct Aave V3 transactions
    - Transactions execute successfully on-chain
    - AaveV3ReceiptParser correctly interprets results
    - Balance changes and account data match expected amounts
    """

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_weth_with_usdc_collateral_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test borrowing WETH against USDC collateral via the two-intent form.

        Uses the accounting-correct SUPPLY -> standalone BORROW split (#2827):
        a bundled ``BorrowIntent(collateral_amount > 0)`` is fail-closed at the
        intent validator because accounting writes one event per intent — the
        supply leg would collapse into the single BORROW event, and (post
        VIB-5218 merged-receipt money legs) the BORROW row would carry the
        SUPPLY leg's asset/amount instead of the borrowed reserve.

        Uses USDC as collateral and WETH as the borrow asset because Linea's
        Aave V3 market has its USDC/USDT borrow caps EXCEEDED (new stablecoin
        borrows revert with BORROW_CAP_EXCEEDED), while WETH retains borrow
        headroom. See the module docstring / VIB-5916.

        Flow:
        1. Setup: supply USDC collateral via SupplyIntent (use_as_collateral
           defaults True); verify the Supply receipt event
        2. Create a standalone BorrowIntent (collateral_amount=0) for a small
           WETH amount computed from the live oracle price at ~18% LTV
        3. Compile + execute via the production pipeline
        4. Verify WETH balance increased and debt was created (health factor > 1.0)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        weth = tokens["WETH"]

        usdc_decimals = get_token_decimals(web3, usdc)
        weth_decimals = get_token_decimals(web3, weth)

        # Supply 500 USDC as collateral, borrow small WETH (~18% LTV from live price)
        collateral_amount = Decimal("500")
        borrow_amount = compute_weth_borrow_amount(price_oracle, collateral_amount)

        print(f"\n{'='*80}")
        print(f"Test: Borrow {borrow_amount} WETH with {collateral_amount} USDC collateral (two-intent form)")
        print(f"  (WETH price ${price_oracle['WETH']}, target LTV {TARGET_BORROW_LTV})")
        print(f"{'='*80}")

        # Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Debt before: {account_data_before['totalDebtBase']}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        # Step 1 — supply the USDC collateral (the two-intent form is the
        # only shape the public API allows — see #2827).
        supply_intent = SupplyIntent(
            protocol="aave_v3",
            token="USDC",
            amount=collateral_amount,
            chain=CHAIN_NAME,
        )
        supply_compile = compiler.compile(supply_intent)
        assert supply_compile.status.value == "SUCCESS", f"Supply compilation failed: {supply_compile.error}"
        assert supply_compile.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_compile.action_bundle, execution_context)
        assert supply_exec.success, f"Collateral supply failed: {supply_exec.error}"

        # Layer 3 (supply leg): the Supply event must land on the USDC reserve.
        supply_event_seen = False
        for tx_result in supply_exec.transaction_results:
            if tx_result.receipt:
                supply_parse = AaveV3ReceiptParser(chain=CHAIN_NAME).parse_receipt(tx_result.receipt.to_dict())
                if supply_parse.success and supply_parse.supplies:
                    collateral_supply = next(
                        (s for s in supply_parse.supplies if s.reserve.lower() == usdc.lower()),
                        None,
                    )
                    assert collateral_supply is not None, (
                        f"Expected a Supply event for USDC collateral. Got reserves: "
                        f"{[s.reserve for s in supply_parse.supplies]}"
                    )
                    assert collateral_supply.amount > 0, "USDC collateral supply amount must be > 0"
                    supply_event_seen = True
        assert supply_event_seen, "Expected at least one Supply event for the USDC collateral leg"

        # Step 2 — standalone borrow (the only shape the public API allows).
        intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="USDC",
            collateral_amount=Decimal("0"),
            borrow_token="WETH",
            borrow_amount=borrow_amount,
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )

        print("\nCreated standalone BorrowIntent:")
        print(f"  Borrow: {intent.borrow_amount} {intent.borrow_token}")
        print(f"  Interest rate mode: {intent.interest_rate_mode}")

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

        # Layer 3: parse receipts and assert decoded Borrow amount > 0
        parser = AaveV3ReceiptParser(chain=CHAIN_NAME)
        decoded_borrow_amount = Decimal("0")
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                assert parse_result.success, (
                    f"Receipt parser failed on tx {tx_result.tx_hash}: {parse_result.error}"
                )
                assert not parse_result.supplies, (
                    f"Standalone borrow must not emit Supply events. Got reserves: "
                    f"{[s.reserve for s in parse_result.supplies]}"
                )
                for borrow_event in parse_result.borrows:
                    print(f"  Borrow amount: {borrow_event.amount}")
                    print(f"  Interest rate mode: {borrow_event.interest_rate_mode}")
                    if borrow_event.amount > decoded_borrow_amount:
                        decoded_borrow_amount = borrow_event.amount

        assert decoded_borrow_amount > 0, (
            "Layer 3: AaveV3ReceiptParser must decode at least one Borrow event "
            "with amount > 0 from the borrow transaction receipts"
        )

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        weth_after = get_token_balance(web3, weth, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before

        print("\n--- Results ---")
        print(f"USDC spent (collateral): {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"WETH received (borrowed): {format_token_amount(weth_received, weth_decimals)}")

        # Verify USDC was spent as collateral EXACTLY
        expected_usdc_spent = int(collateral_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal collateral amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify WETH was received EXACTLY (Aave V3 charges no borrow origination fee)
        expected_weth_received = int(borrow_amount * Decimal(10**weth_decimals))
        assert weth_received == expected_weth_received, (
            f"WETH received must EXACTLY equal borrow amount. "
            f"Expected: {expected_weth_received}, Got: {weth_received}"
        )

        # Verify account data changed (debt created, healthy position)
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Debt after: {account_data_after['totalDebtBase']}")
        print(f"Health factor: {account_data_after['healthFactor']}")

        assert account_data_after["totalDebtBase"] > 0, "Debt must be created"
        assert account_data_after["healthFactor"] > 1e18, "Health factor must be > 1.0"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_full_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Test repaying the FULL WETH debt using RepayIntent(repay_full=True).

        Flow:
        1. Setup: Borrow WETH with USDC collateral first.
        2. Create RepayIntent with repay_full=True (Aave MAX_UINT256 path).
        3. Compile and execute.
        4. Verify WETH spend is bounded and positive and debt returns to zero
           (within protocol rounding).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth = tokens["WETH"]
        weth_decimals = get_token_decimals(web3, weth)

        # First borrow WETH against USDC collateral
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        collateral_amount = Decimal("500")
        borrow_amount = compute_weth_borrow_amount(price_oracle, collateral_amount)

        supply_intent = SupplyIntent(
            protocol="aave_v3",
            token="USDC",
            amount=collateral_amount,
            chain=CHAIN_NAME,
        )
        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_exec.success, f"Collateral supply setup failed: {supply_exec.error}"

        borrow_intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="USDC",
            collateral_amount=Decimal("0"),
            borrow_token="WETH",
            borrow_amount=borrow_amount,
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )

        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_execution_result = await orchestrator.execute(borrow_result.action_bundle, execution_context)
        assert borrow_execution_result.success, f"Borrow execution failed: {borrow_execution_result.error}"

        print(f"\n{'='*80}")
        print("Test: Repay FULL WETH debt using RepayIntent(repay_full=True)")
        print(f"{'='*80}")

        weth_before = get_token_balance(web3, weth, funded_wallet)
        print(f"WETH before repay: {format_token_amount(weth_before, weth_decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Debt before: {account_data_before['totalDebtBase']}")
        print(f"Health factor before: {account_data_before['healthFactor']}")
        assert account_data_before["totalDebtBase"] > 0, "Precondition: must hold debt before repay"

        # Create RepayIntent for the FULL debt
        intent = RepayIntent(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("0"),  # Ignored when repay_full=True
            repay_full=True,
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated RepayIntent: token={intent.token}, repay_full=True")

        # Compile and execute
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: parse receipts and assert decoded Repay amount > 0
        parser = AaveV3ReceiptParser(chain=CHAIN_NAME)
        decoded_repay_amount = Decimal("0")
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                assert parse_result.success, (
                    f"Receipt parser failed on tx {tx_result.tx_hash}: {parse_result.error}"
                )
                for r in parse_result.repays:
                    if r.amount > decoded_repay_amount:
                        decoded_repay_amount = r.amount

        assert decoded_repay_amount > 0, (
            "Layer 3: AaveV3ReceiptParser must decode at least one Repay event "
            "with amount > 0 from the repay transaction receipts"
        )

        # Verify balance changes: WETH spend bounded and positive
        weth_after = get_token_balance(web3, weth, funded_wallet)
        weth_spent = weth_before - weth_after

        print(f"\nWETH spent (repaid): {format_token_amount(weth_spent, weth_decimals)}")

        borrow_wei = int(borrow_amount * Decimal(10**weth_decimals))
        # Repaid amount = principal + accrued interest, so >= borrow principal and
        # bounded by a small tolerance (only ~1 block of interest on Anvil).
        assert weth_spent >= borrow_wei, (
            f"WETH repaid must be at least the borrowed principal. "
            f"Expected >= {borrow_wei}, Got: {weth_spent}"
        )
        max_repay = int(borrow_wei * Decimal("1.001"))
        assert weth_spent <= max_repay, (
            f"WETH repaid unexpectedly exceeds principal + tolerance. "
            f"Expected <= {max_repay}, Got: {weth_spent}"
        )

        # Verify account data changed: debt fully cleared
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Debt after: {account_data_after['totalDebtBase']}")
        print(f"Health factor after: {account_data_after['healthFactor']}")

        assert account_data_after["totalDebtBase"] == 0, (
            "Debt must be zero after repay_full. "
            f"Got: {account_data_after['totalDebtBase']}"
        )
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
        weth = tokens["WETH"]

        print(f"\n{'='*80}")
        print("Test: BorrowIntent without Collateral (should fail)")
        print(f"{'='*80}")

        weth_before = get_token_balance(web3, weth, funded_wallet)

        # Try to borrow WETH without supplying collateral (collateral_amount = 0)
        intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="USDC",
            collateral_amount=Decimal("0"),  # No collateral
            borrow_token="WETH",
            borrow_amount=Decimal("0.05"),
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

        # Verify conservation: no WETH received, debt unchanged
        weth_after = get_token_balance(web3, weth, funded_wallet)
        assert weth_after == weth_before, "WETH balance must be unchanged after failed borrow"

        account_data_after = get_user_account_data(web3, funded_wallet)
        assert account_data_after["totalDebtBase"] == 0, "No debt must be created by a failed borrow"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
