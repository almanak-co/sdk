"""Production-grade borrow/repay intent tests for Euler V2 on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for Euler V2 borrow operations:
1. Create BorrowIntent / RepayIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using EulerV2ReceiptParser
5. Verify balance changes are correct

Euler V2 borrow flow:
- Supply collateral (USDC) via SupplyIntent into eUSDC-19 vault
- Borrow via BorrowIntent using EVC batch: enableCollateral + enableController + borrow
- Repay via RepayIntent (approve + repay on the borrow vault)

INFRASTRUCTURE NOTE (2026-04-10):
The eWAVAX-2 vault (original collateral target) has maxDeposit=0 (supply cap reached).
The eUSDC-19 vault's valid collateral vaults (per LTVList) use BTC.b, sAVAX, WETH.e,
or ggAVAX — none of which are currently funded in the test wallet or registered in the
Euler V2 adapter. These tests use USDC collateral + USDC borrow via eUSDC-2 to test
the compilation path, but the EVC batch will revert because eUSDC-19 is not a valid
collateral vault for eUSDC-2 borrowing. The borrow/repay tests are marked xfail until
a valid collateral vault is added to the adapter.

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/avalanche/test_euler_v2_borrow.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.euler_v2.receipt_parser import EulerV2ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent
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

# Euler V2 vault addresses for receipt filtering
EULER_V2_USDC_VAULT = "0x37ca03aD51B8ff79aAD35FadaCBA4CEDF0C3e74e"  # eUSDC-19 (collateral vault)
EULER_V2_WAVAX_VAULT = "0x6c718a70239fA548c0bD268fE88F37EBE8b6E2ea"  # eWAVAX-2 (CLOSED)

# Conservative amounts: 1000 USDC collateral, 100 USDC borrow (~10% LTV)
COLLATERAL_AMOUNT = Decimal("1000")
BORROW_AMOUNT = Decimal("100")
REPAY_AMOUNT = Decimal("50")


# =============================================================================
# Borrow/Repay Tests
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.borrow
@pytest.mark.lending
class TestEulerV2BorrowIntent:
    """Test Euler V2 borrow/repay operations using BorrowIntent and RepayIntent.

    These tests verify the full Intent flow:
    - BorrowIntent with collateral supply + borrow via EVC batch
    - RepayIntent with approve + repay on the borrow vault
    - Receipt parsing for Borrow/Repay events
    - Balance changes match expected amounts

    NOTE: Borrow/repay tests are marked xfail because the eWAVAX-2 vault (the only
    non-stablecoin collateral vault in the adapter) has maxDeposit=0. The compilation
    path is fully tested but execution reverts due to supply cap. When a new collateral
    vault is added to the adapter (e.g., eBTC.b or eWETH.e), remove the xfail markers.
    """

    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="eWAVAX-2 vault has maxDeposit=0 (supply cap reached). "
        "Euler V2 borrow requires a valid collateral vault not yet in adapter. "
        "See VIB-2643 for adding eBTC.b or eWETH.e collateral vaults.",
        strict=True,
    )
    async def test_borrow_usdc_with_wavax_collateral(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC borrow with WAVAX collateral using SupplyIntent + BorrowIntent.

        Flow:
        1. Supply WAVAX as collateral via SupplyIntent to eWAVAX-2
        2. Create BorrowIntent with zero additional collateral (already supplied)
        3. Compile and execute borrow via EVC batch
        4. Parse receipt for Borrow event
        5. Verify USDC balance increased by exact borrow amount
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        wavax = tokens["WAVAX"]
        usdc_decimals = get_token_decimals(web3, usdc)
        wavax_decimals = get_token_decimals(web3, wavax)

        collateral_amount = Decimal("5")  # 5 WAVAX (~$125)
        borrow_amount = Decimal("10")  # 10 USDC (~8% LTV)

        print(f"\n{'='*80}")
        print(f"Test: Borrow {borrow_amount} USDC with {collateral_amount} WAVAX collateral on Euler V2")
        print(f"{'='*80}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Step 1: Supply WAVAX as collateral
        supply_intent = SupplyIntent(
            protocol="euler_v2",
            token="WAVAX",
            amount=collateral_amount,
            chain=CHAIN_NAME,
        )

        wavax_before = get_token_balance(web3, wavax, funded_wallet)
        expected_wavax_wei = int(collateral_amount * Decimal(10**wavax_decimals))
        assert wavax_before >= expected_wavax_wei, (
            f"Funded wallet lacks required WAVAX. Need {expected_wavax_wei}, have {wavax_before}"
        )
        print(f"WAVAX before supply: {format_token_amount(wavax_before, wavax_decimals)}")

        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS", f"Supply compilation failed: {supply_result.error}"
        assert supply_result.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_result.action_bundle)
        assert supply_exec.success, f"Collateral supply failed: {supply_exec.error}"
        print(f"Collateral supply succeeded: {collateral_amount} WAVAX deposited")

        # Step 2: Borrow USDC against supplied WAVAX collateral
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before borrow: {format_token_amount(usdc_before, usdc_decimals)}")

        intent = BorrowIntent(
            protocol="euler_v2",
            collateral_token="WAVAX",
            collateral_amount=Decimal("0"),  # Already supplied above
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            chain=CHAIN_NAME,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Parse receipts for Borrow event
        found_borrow_event = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            if tx_result.receipt:
                parser = EulerV2ReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
                )
                if parse_result.success and parse_result.borrow_amount > 0:
                    assert parse_result.borrow_amount > 0, "Borrow amount must be positive"
                    found_borrow_event = True

        assert found_borrow_event, "Receipt parser must find at least one Borrow event"

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before
        expected_usdc_received = int(borrow_amount * Decimal(10**usdc_decimals))
        assert usdc_received == expected_usdc_received, (
            f"USDC received must EXACTLY equal borrow amount. "
            f"Expected: {expected_usdc_received}, Got: {usdc_received}"
        )

        wavax_after = get_token_balance(web3, wavax, funded_wallet)
        wavax_spent = wavax_before - wavax_after
        expected_wavax_spent = int(collateral_amount * Decimal(10**wavax_decimals))
        assert wavax_spent == expected_wavax_spent, (
            f"WAVAX spent must EXACTLY equal collateral amount. "
            f"Expected: {expected_wavax_spent}, Got: {wavax_spent}"
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="eWAVAX-2 vault has maxDeposit=0 (supply cap reached). "
        "Euler V2 borrow requires a valid collateral vault not yet in adapter. "
        "See VIB-2643 for adding eBTC.b or eWETH.e collateral vaults.",
        strict=True,
    )
    async def test_repay_usdc_after_borrow(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC repay using RepayIntent (after borrowing).

        Flow:
        1. Supply WAVAX collateral + borrow USDC (setup)
        2. Create RepayIntent to repay portion of debt
        3. Compile and execute
        4. Verify USDC balance decreased by exact repay amount
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        wavax = tokens["WAVAX"]
        usdc_decimals = get_token_decimals(web3, usdc)

        collateral_amount = Decimal("5")
        borrow_amount = Decimal("10")
        repay_amount = Decimal("5")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Precondition: verify wallet has enough WAVAX
        wavax_before = get_token_balance(web3, tokens["WAVAX"], funded_wallet)
        expected_wavax_wei = int(collateral_amount * Decimal(10**get_token_decimals(web3, tokens["WAVAX"])))
        assert wavax_before >= expected_wavax_wei, (
            f"Funded wallet lacks required WAVAX. Need {expected_wavax_wei}, have {wavax_before}"
        )

        # Setup step 1: Supply WAVAX collateral
        supply_intent = SupplyIntent(
            protocol="euler_v2",
            token="WAVAX",
            amount=collateral_amount,
            chain=CHAIN_NAME,
        )
        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_result.action_bundle)
        assert supply_exec.success, f"Collateral supply failed: {supply_exec.error}"

        # Setup step 2: Borrow USDC against supplied collateral
        borrow_intent = BorrowIntent(
            protocol="euler_v2",
            collateral_token="WAVAX",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            chain=CHAIN_NAME,
        )
        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle)
        assert borrow_exec.success, f"Initial borrow failed: {borrow_exec.error}"

        # Now repay
        print(f"\n{'='*80}")
        print(f"Test: Repay {repay_amount} USDC to Euler V2 using RepayIntent")
        print(f"{'='*80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before repay: {format_token_amount(usdc_before, usdc_decimals)}")

        intent = RepayIntent(
            protocol="euler_v2",
            token="USDC",
            amount=repay_amount,
            chain=CHAIN_NAME,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Parse receipts for Repay event
        found_repay_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = EulerV2ReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
                )
                if parse_result.success and parse_result.repay_amount > 0:
                    assert parse_result.repay_amount > 0, "Repay amount must be positive"
                    found_repay_event = True

        assert found_repay_event, "Receipt parser must find at least one Repay event"

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        expected_usdc_spent = int(repay_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal repay amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_borrow_without_collateral_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test that BorrowIntent without collateral fails gracefully.

        Creates a BorrowIntent with zero collateral and no prior supply.
        The borrow should fail (either at compilation or execution) because
        there's no collateral backing the loan. Verifies balance conservation.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        wavax = tokens["WAVAX"]

        print(f"\n{'='*80}")
        print("Test: BorrowIntent without Collateral (Euler V2)")
        print(f"{'='*80}")

        # Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        wavax_before = get_token_balance(web3, wavax, funded_wallet)

        # Create BorrowIntent with zero collateral
        intent = BorrowIntent(
            protocol="euler_v2",
            collateral_token="WAVAX",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=BORROW_AMOUNT,
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        compilation_result = compiler.compile(intent)

        # Compilation must succeed — the compiler builds the EVC batch regardless
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"
        print(f"Compilation succeeded with {len(compilation_result.action_bundle.transactions)} transactions")

        # Execution should fail on-chain due to insufficient collateral
        print("Executing -- expecting on-chain failure due to no collateral...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert not execution_result.success, "Execution should fail without collateral"
        print(f"Execution failed as expected: {execution_result.error}")

        # Verify balances unchanged (conservation check)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        wavax_after = get_token_balance(web3, wavax, funded_wallet)

        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed borrow"
        assert wavax_after == wavax_before, "WAVAX balance must be unchanged after failed borrow"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
