"""Production-grade lending intent tests for Aave V3 on Mantle.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for lending operations:
1. Create lending intents (SupplyIntent, BorrowIntent, RepayIntent)
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using AaveV3ReceiptParser
5. Verify balance changes and account data are correct

Aave V3 Pool on Mantle: 0x458F293454fE0d67EC0655f3672301301DD51422

Mantle Aave V3 reserve configuration (verified on-chain via getReserveConfigurationData).

CURRENT (since block 98303344, ~2026-07-22 — VIB-6111): Aave governance set
``ltv=0`` on ALL 10 reserves of this market. No asset can be enabled as
collateral, so NO borrow is possible for any asset here. Only the LTV field
moved; liquidationThreshold, borrowingEnabled, isActive, isFrozen and the
supply/borrow caps are byte-identical either side of that block.

    Token   active  frozen  ltv           borrowable  collateral
    WETH    true    TRUE    0 (was 8050)  true        false       <- frozen, supply reverts (#2102)
    WMNT    true    false   0 (was 4000)  false       false       <- was the only LTV>0 reserve
    USDC    true    false   0             true        false       <- borrow-only (LTV 0)
    USDT0   true    false   0             true        false       <- borrow-only
    USDe    true    false   0             true        false
    GHO     true    false   0             true        false

Tests use:
- Supply/withdraw: USDC (active, non-frozen) — unaffected, these pass
  ``use_as_collateral=False`` and remain full round trips.
- Borrow/repay: quarantined under VIB-6111. Rather than a bare ``xfail`` that
  would mute unrelated regressions too, those tests now assert the specific
  market state that blocks them (LTV=0 on-chain, failure isolated to the
  ``setUserUseReserveAsCollateral`` leg, no debt created). They fail loudly if
  Aave restores a non-zero LTV.

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
from tests.intents._permission_onchain_harness import AuthorizationFailed
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

# Aave aTokens store scaled balances: mint divides by the liquidity index and
# balanceOf multiplies by the current index. The composed rounded operations
# have produced a two-wei difference in pinned-fork CI at Mantle's USDC scale.
ATOKEN_BALANCE_ROUNDING_TOLERANCE_WEI = 2

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


def assert_atoken_delta_within_rounding(actual: int, expected: int, operation: str) -> None:
    """Assert an aToken delta matches its underlying amount within index-rounding tolerance."""
    absolute_error = abs(actual - expected)
    assert absolute_error <= ATOKEN_BALANCE_ROUNDING_TOLERANCE_WEI, (
        f"aUSDC {operation} must match the underlying amount within "
        f"{ATOKEN_BALANCE_ROUNDING_TOLERANCE_WEI} wei of index rounding. "
        f"Expected: {expected}, Got: {actual}, Absolute error: {absolute_error}"
    )


def get_reserve_ltv(web3: Web3, asset: str) -> int:
    """Read an Aave V3 reserve's loan-to-value, in basis points (VIB-6111).

    LTV is packed into bits 0-15 of ``ReserveConfigurationMap.data``, the first
    field of ``ReserveData``. An LTV of 0 means the asset cannot be enabled as
    collateral at all — ``setUserUseReserveAsCollateral`` reverts
    ``UserHasAssetWithZeroLtv()`` (``0x21e5c4ae``) for it.

    This is the discriminating on-chain read behind the borrow/repay
    quarantine: it distinguishes "the market zeroed LTV" from any other reason
    a lending test might fail, so unrelated regressions stay real failures.
    """
    pool_address = AAVE_V3_POOL_ADDRESSES[CHAIN_NAME]
    pool_contract = web3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=AAVE_POOL_ABI)
    reserve_data = pool_contract.functions.getReserveData(Web3.to_checksum_address(asset)).call()
    configuration = reserve_data[0]
    # web3 decodes the single-member struct as a tuple/list; unwrap defensively.
    packed = configuration[0] if isinstance(configuration, list | tuple) else configuration
    return packed & 0xFFFF


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
        # UserHasAssetWithZeroLtv() (0x21e5c4ae). The supply itself succeeds;
        # only the auto-toggle fails.
        #
        # VIB-6111: this previously named UnderlyingCannotBeUsedAsCollateral,
        # which is a different error — and the selector documented for it
        # elsewhere in the tree (0x0cafc072) was keccak-wrong besides. The
        # correct one is UserHasAssetWithZeroLtv (0x21e5c4ae).
        #
        # NOTE: THIS test does not observe that revert — it passes
        # use_as_collateral=False and therefore never sends
        # setUserUseReserveAsCollateral(..., true). The revert is EXPECTED
        # behaviour, measured separately; see
        # ``docs/internal/reports/vib-6111-mantle-zero-ltv-measurements.md``.
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
        assert_atoken_delta_within_rounding(ausdc_received, expected_usdc_spent, "received")

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
        assert_atoken_delta_within_rounding(ausdc_burned, expected_usdc_received, "burned")

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

    HISTORICAL (pre-2026-07-22): used WMNT as collateral (LTV=40%, then the
    only Mantle Aave reserve with non-zero LTV) and borrowed USDC.

    CURRENT: every reserve has base LTV=0 (VIB-6111) — but that does NOT make
    borrowing impossible here, and an earlier version of this docstring said it
    did. Aave V3 Mantle runs v3.2 "liquid eModes": the collateral power moved
    into six eMode categories, measured live at block 98645311 with LTV 40-93%
    ('WMNT__Stablecoins' 40%, 'sUSDe Stablecoins' 90%, 'wrsETH Correlated'
    93%, ...). The market carries ~$78M of borrows at ~33% utilisation.

    What is actually blocked is OUR path: no compiler calls
    ``adapter.set_user_emode()``, so we never enrol in a category and a
    compiled borrow sees base LTV 0. Tracked in ALM-3075, which also owns
    replacing the base-LTV monitor below — it watches a parameter Aave has
    already moved away from, so it can no longer fire.
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
        """VIB-6111: borrowing via BASE LTV is blocked on Aave V3 Mantle.

        HISTORICAL: this asserted a full WMNT-collateral → USDC-borrow round
        trip. Aave governance set ``ltv=0`` on all 10 Mantle reserves at block
        98303344 (~2026-07-22) — WETH 8050→0, WMNT 4000→0, every other reserve
        config field byte-identical.

        SCOPE CORRECTION (ALM-3075): that is a fact about *base* LTV only. It
        was previously written up here as "no borrow is possible for any asset
        on this market", which is wrong — Aave moved the collateral power into
        six v3.2 eMode categories (LTV 40-93%), and the market is actively
        borrowed against. What this test actually pins is the base-LTV path,
        which is the only path our connector can compile today because nothing
        calls ``adapter.set_user_emode()``.

        Deliberately NOT a bare ``xfail``: muting the whole body would also
        swallow a compiler break, an RPC error or a receipt regression, and
        would report the same green tick for all of them. Instead this asserts
        the *specific* market state that blocks the base-LTV path, so every
        other failure mode stays a real failure:

        1. Compilation: SupplyIntent AND BorrowIntent both still compile to
           ActionBundles (SUCCESS) — a compiler regression fails here.
        2. Execution: the supply bundle fails, and fails specifically at its
           final ``setUserUseReserveAsCollateral`` leg — ``approve`` and
           ``supply`` still land on-chain. An earlier-leg failure fails here.
        3. Market state: WMNT *base* LTV reads 0 on-chain.
           NOTE (ALM-3075): this was written as a live monitor — "if Aave
           restores a non-zero LTV this fails loudly" — but it can no longer
           fire. Aave did not park the parameter, they relocated it: base LTV
           stays 0 and borrowing power now lives in eMode categories. Watching
           base LTV therefore watches a value that will not change, so this
           layer asserts a true fact while providing no regression cover.
           ALM-3075 owns replacing it with a check on eMode category config.
        4. Conservation: the supplied WMNT converts to aWMNT, no debt is
           created, and USDC does not move.

        EXECUTION SHAPE — asserted shape-agnostically on purpose (VIB-6111).

        These tests run on the ``ZodiacOrchestrator`` **shim**
        (``tests/intents/_permission_onchain_harness.py``), which the per-chain
        conftests substitute for the production ``ExecutionOrchestrator`` —
        default-on since Phase G, no marker required (``.claude/rules/
        intent-tests.md``). The shim sends **one ``execTransactionWithRole``
        per bundle tx, in order**, so there is no MultiSend and no atomicity:
        ``approve`` and ``supply`` commit, and only the toggle reverts. Capital
        lands as aWMNT with zero collateral value.

        Production Safe execution goes through ``ExecutionOrchestrator``, whose
        ``_sign_safe_batch`` bundles a multi-tx ActionBundle into an atomic
        MultiSend (via ``SafeSigner.sign_bundle_with_web3``) — a different
        shape, and one this test does not exercise. Do not read this test's
        behaviour as evidence about the production lane.

        That lane has since been MEASURED directly, on a fork at head, rather
        than inferred: the production Safe/Zodiac path reverts the whole
        MultiSend atomically and moves NO funds, while the EOA lane and this
        shim both park capital. (The report documents that measurement with
        WETH/aWETH deltas — cited here for the LANE behaviour, which is
        token-independent, not for a WMNT-specific figure.) Evidence and
        receipts:
        ``docs/internal/reports/vib-6111-mantle-zero-ltv-measurements.md``.

        The conservation block below asserts only what holds either way. Note
        this does NOT extend to the Layer 3 receipt assertion further down
        (``assert supply_parsed, "Must find at least one Supply event..."``):
        that assertion depends on the supply leg having actually committed,
        which is a property of the non-atomic shim, not of both shapes — an
        atomic-MultiSend lane would revert the whole bundle and emit no
        ``Supply`` event. If the harness or lane changes, that assertion (not
        just the conservation block) needs revisiting.
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
        print("Test: Borrow USDC with WMNT collateral on Aave V3 (Mantle) — VIB-6111 quarantine")
        print(f"{'=' * 80}")

        # Layer 3 (market state): WMNT LTV must be zero. This is the fact that
        # makes borrowing impossible; asserting it is what keeps this
        # quarantine narrow. If Aave restores a non-zero LTV, this fails and
        # the historical round-trip assertions should be restored.
        wmnt_ltv = get_reserve_ltv(web3, wmnt)
        assert wmnt_ltv == 0, (
            f"VIB-6111 quarantine is STALE: WMNT LTV on Aave V3 Mantle is now {wmnt_ltv} bps "
            f"(expected 0). Aave restored a non-zero LTV — restore the full "
            f"WMNT-collateral → USDC-borrow round-trip assertions and delete this quarantine."
        )
        print(f"WMNT LTV on-chain: {wmnt_ltv} bps (zero — collateral cannot be enabled)")

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

        # Layer 2: the bundle must fail, and must fail at the collateral
        # toggle specifically — approve and supply still land on-chain.
        supply_exec = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert not supply_exec.success, (
            "Supply-with-collateral unexpectedly SUCCEEDED. Every Aave V3 Mantle reserve "
            "has LTV=0 (VIB-6111), so setUserUseReserveAsCollateral must revert "
            "UserHasAssetWithZeroLtv(). A success here means the market changed — restore "
            "the full borrow round-trip assertions."
        )
        tx_results = supply_exec.transaction_results
        assert len(tx_results) >= 1, f"expected at least one transaction result: {supply_exec.error}"
        # Only the LAST leg may fail, whichever shape the executor produced
        # (see the execution-shape note in the docstring). With an atomic
        # MultiSend there is a single result and this slice is empty.
        assert all(r.success for r in tx_results[:-1]), (
            "ONLY the final setUserUseReserveAsCollateral leg may revert. An earlier leg "
            "failed, which means something other than the zero-LTV block broke: "
            + "; ".join(f"tx[{i}] success={r.success} error={r.error}" for i, r in enumerate(tx_results))
        )
        assert not tx_results[-1].success, "the failing leg must be the last one"
        print(f"Supply bundle failed at the collateral toggle as expected: {supply_exec.error}")

        # Step 2: BorrowIntent must still COMPILE — this keeps the BORROW
        # compiler path under test even though it can never execute here.
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

        print(f"Step 2 — Borrow ActionBundle compiles: {len(compilation_result.action_bundle.transactions)} txs")

        # Layer 4b: conservation. Deliberately execution-shape agnostic — on
        # the ZodiacOrchestrator shim the supply leg commits (non-atomic), on
        # an atomic-MultiSend lane it would roll back, so asserting either
        # outcome would pin this test to a harness. What must hold either way:
        # WMNT is conserved (it either stays put or converts 1:1 into aWMNT),
        # no debt is created, and USDC never moves because borrow is
        # unreachable.
        wmnt_after = get_token_balance(web3, wmnt, funded_wallet)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        account_data_after = get_user_account_data(web3, funded_wallet)

        expected_wmnt_spent = int(collateral_amount * Decimal(10**wmnt_decimals))
        wmnt_spent = wmnt_before - wmnt_after
        assert wmnt_spent in (0, expected_wmnt_spent), (
            f"WMNT must either be untouched (bundle rolled back) or spent exactly "
            f"{expected_wmnt_spent} (supply leg committed); got {wmnt_spent}"
        )
        if wmnt_spent:
            atoken = get_atoken_address(web3, wmnt)
            assert get_token_balance(web3, atoken, funded_wallet) >= wmnt_spent, (
                "WMNT left the wallet, so the matching aWMNT must have been received"
            )
        assert usdc_after == usdc_before, (
            f"USDC must NOT move — the borrow is unreachable. before={usdc_before} after={usdc_after}"
        )
        assert account_data_after["totalDebtBase"] == account_data_before["totalDebtBase"], (
            "no debt may be created while no reserve can be enabled as collateral"
        )
        print("Conservation verified: WMNT→aWMNT supplied, zero debt, USDC untouched")

        # Layer 3: Parse receipts from the supply legs that DID commit. The
        # borrow bundle never executes, so there is no Borrow event to parse —
        # asserting one would be asserting a state that cannot exist.
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
        assert supply_parsed, "Must find at least one Supply event in the committed supply legs"

        print("\n--- Results ---")
        print(f"WMNT spent (supplied): {format_token_amount(wmnt_before - wmnt_after, wmnt_decimals)}")
        print(f"USDC delta: {format_token_amount(usdc_after - usdc_before, usdc_decimals)} (must be 0)")
        print(f"Debt after: {account_data_after['totalDebtBase']} (must be unchanged)")
        print("\nVIB-6111 QUARANTINE ASSERTIONS PASSED — borrow remains blocked by LTV=0")


# =============================================================================
# Repay Tests
# =============================================================================


@pytest.mark.mantle
@pytest.mark.repay
@pytest.mark.lending
class TestAaveV3RepayIntent:
    """Test Aave V3 repay operations on Mantle.

    HISTORICAL (pre-2026-07-22): verified a WMNT collateral supply -> USDC
    borrow -> USDC repay flow.

    CURRENT: blocked by the same market change as TestAaveV3BorrowIntent —
    every reserve has LTV=0, so no borrow can occur and there is no debt to
    repay. See VIB-6111.
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
        """VIB-6111: repay is unreachable on Mantle — the borrow it needs cannot happen.

        HISTORICAL: this asserted a WMNT-collateral → USDC-borrow → USDC-repay
        round trip. Aave governance set ``ltv=0`` on all 10 Mantle reserves at
        block 98303344 (~2026-07-22), so the collateral toggle reverts, the
        borrow never happens, and there is no debt to repay.

        Same discipline as ``test_borrow_usdc_with_wmnt_collateral``: rather
        than muting the whole body with a bare ``xfail`` — which would also
        swallow compiler, RPC and receipt regressions — this asserts the
        specific reachable state, so every other failure mode stays real:

        1. Compilation: SupplyIntent, BorrowIntent AND RepayIntent all still
           compile (SUCCESS) — the REPAY compiler path stays under test.
        2. Execution: the supply bundle fails, and only its final
           ``setUserUseReserveAsCollateral`` leg may be the one that reverts.
           These tests run on the ``ZodiacOrchestrator`` shim (one
           ``execTransactionWithRole`` per bundle tx, non-atomic) rather than
           the production ``ExecutionOrchestrator``, so no leg count is
           asserted and the shape is not evidence about production.
        3. Market state: WMNT LTV reads 0 on-chain — the monitor. A non-zero
           LTV fails here and means the round trip should be restored.
        4. Conservation: no debt is created and USDC does not move.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        wmnt = tokens["WMNT"]
        usdc_decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        print(f"\n{'=' * 80}")
        print("Test: Repay USDC on Aave V3 (Mantle) — VIB-6111 quarantine")
        print(f"{'=' * 80}")

        # Layer 3 (market state): the fact that makes the whole chain impossible.
        wmnt_ltv = get_reserve_ltv(web3, wmnt)
        assert wmnt_ltv == 0, (
            f"VIB-6111 quarantine is STALE: WMNT LTV on Aave V3 Mantle is now {wmnt_ltv} bps "
            f"(expected 0). Aave restored a non-zero LTV — restore the full "
            f"supply → borrow → repay round-trip assertions and delete this quarantine."
        )
        print(f"WMNT LTV on-chain: {wmnt_ltv} bps (zero — collateral cannot be enabled)")

        # Layer 4a: Record state BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"Debt before: {account_data_before['totalDebtBase']}")

        # Step 1a: SupplyIntent with use_as_collateral=True — the leg that the
        # zero-LTV market change breaks.
        print("\nStep 1a: Supplying 200 WMNT as collateral (expected to fail at the toggle)...")
        supply_intent = SupplyIntent(
            protocol="aave_v3",
            token="WMNT",
            amount=Decimal("200"),
            use_as_collateral=True,
            chain=CHAIN_NAME,
        )
        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS", f"Supply compilation failed: {supply_result.error}"
        assert supply_result.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert not supply_exec.success, (
            "Supply-with-collateral unexpectedly SUCCEEDED. Every Aave V3 Mantle reserve has "
            "LTV=0 (VIB-6111), so setUserUseReserveAsCollateral must revert. A success here "
            "means the market changed — restore the full repay round-trip assertions."
        )
        tx_results = supply_exec.transaction_results
        assert len(tx_results) >= 1, f"expected at least one transaction result: {supply_exec.error}"
        # Only the LAST leg may fail. No leg count is asserted: the
        # ZodiacOrchestrator shim yields one result per bundle tx, while a
        # production atomic-MultiSend lane would yield a single result (the
        # slice below is then empty). Both shapes satisfy this.
        assert all(r.success for r in tx_results[:-1]), (
            "ONLY the final setUserUseReserveAsCollateral leg may revert. An earlier leg "
            "failed, which means something other than the zero-LTV block broke: "
            + "; ".join(f"tx[{i}] success={r.success} error={r.error}" for i, r in enumerate(tx_results))
        )
        assert not tx_results[-1].success, "the failing leg must be the last one"
        assert tx_results[-1].receipt is not None, "the failing leg must still produce a receipt"
        assert tx_results[-1].receipt.status == 0, (
            f"the failing leg must revert on-chain (receipt status 0), got status={tx_results[-1].receipt.status}"
        )
        print(f"Supply bundle failed at the collateral toggle as expected: {supply_exec.error}")

        # Step 1b: BorrowIntent must still COMPILE (it can never execute here).
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
        assert borrow_result.status.value == "SUCCESS", f"Borrow compilation failed: {borrow_result.error}"
        assert borrow_result.action_bundle is not None
        print(f"Step 1b — Borrow ActionBundle compiles: {len(borrow_result.action_bundle.transactions)} txs")

        # Step 2: RepayIntent must still COMPILE — keeps the REPAY compiler
        # path under test even though there is no debt to repay.
        repay_amount = Decimal("10")
        repay_intent = RepayIntent(
            protocol="aave_v3",
            token="USDC",
            amount=repay_amount,
            chain=CHAIN_NAME,
        )

        compilation_result = compiler.compile(repay_intent)
        assert compilation_result.status.value == "SUCCESS", f"Repay compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None
        print(f"Step 2 — Repay ActionBundle compiles: {len(compilation_result.action_bundle.transactions)} txs")

        # Layer 3: Parse receipts from the supply legs that DID commit. There
        # is no Repay event to parse — no debt was ever created.
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
        assert supply_parsed, "Must find at least one Supply event in the committed supply legs"

        # Layer 4b: conservation — no borrow, therefore no debt and no USDC movement.
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        account_data_after = get_user_account_data(web3, funded_wallet)

        print("\n--- Results ---")
        print(f"USDC delta: {format_token_amount(usdc_after - usdc_before, usdc_decimals)} (must be 0)")
        print(f"Debt after: {account_data_after['totalDebtBase']} (must be unchanged)")

        assert usdc_after == usdc_before, (
            f"USDC must NOT move — neither borrow nor repay is reachable. before={usdc_before} after={usdc_after}"
        )
        assert account_data_after["totalDebtBase"] == account_data_before["totalDebtBase"], (
            "no debt may be created or repaid while no reserve can be enabled as collateral"
        )

        print("\nVIB-6111 QUARANTINE ASSERTIONS PASSED — repay remains unreachable")


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
        2. Execution: must be REFUSED, by either path below
        3. Balance Conservation: USDC balance unchanged

        Two refusal paths are accepted, because which one fires depends on the
        permission manifest rather than on anything this test controls:

        a) ``AuthorizationFailed`` — aave_v3 does not declare BORROW on mantle
           (ALM-3075: the connector cannot enrol an eMode, so the base-LTV-0
           path is all it could compile), so the derived Zodiac manifest holds
           no ``borrow`` permission and Roles blocks
           ``execTransactionWithRole`` before the Pool is reached. Current path.
        b) ``ExecutionResult(success=False)`` — the Pool itself reverts for
           want of collateral. The path while BORROW was declared here, and the
           path again once ALM-3075 lands eMode enrolment and mantle returns to
           the BORROW list.

        Accepting both keeps this honest across that transition without an
        edit, while still FAILING if the borrow ever SUCCEEDS — the property
        that actually matters. It deliberately does not assert *which* path
        fired: pinning path (a) would turn the ALM-3075 fix into a red test,
        and pinning it as "the market cannot be borrowed against" is the exact
        misreading ALM-3075 was filed to correct.
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

        try:
            execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        except AuthorizationFailed as exc:
            # Path (a): Roles refused it — the borrow never reached the Pool,
            # so no debt was created and no USDC was received.
            print(f"Borrow refused at the authorisation layer as expected: {exc}")
        else:
            # Path (b): authz passed, so the Pool is what rejected it.
            assert not execution_result.success, "Execution should fail without sufficient collateral"
            print(f"Execution failed as expected: {execution_result.error}")

        # Balance conservation: USDC must not change
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_before, (
            f"USDC balance must be conserved on failure. Before: {usdc_before}, After: {usdc_after}"
        )

        print("\nBALANCE CONSERVATION VERIFIED")
