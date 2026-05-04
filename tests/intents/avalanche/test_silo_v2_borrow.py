"""Production-grade borrow/repay intent tests for Silo V2 on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for Silo V2 borrow operations:
1. Create BorrowIntent / RepayIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using SiloV2ReceiptParser
5. Verify balance changes are correct

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

NOTE: The borrow/repay happy-path tests are currently skipped because the WAVAX/USDC
Silo V2 market on Avalanche has zero USDC deposits at the mainnet fork block. Silo V2's
isolated architecture means there is no USDC to borrow until other users deposit USDC
into the silo. The tests are correctly structured and will pass once the market has
USDC liquidity.

To run:
    uv run pytest tests/intents/avalanche/test_silo_v2_borrow.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.silo_v2.receipt_parser import SiloV2ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent
from almanak.framework.intents.compiler import IntentCompiler
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

pytestmark = pytest.mark.no_zodiac(reason="silo_v2 connector not in manifest matrix")

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "avalanche"

# Silo V2 WAVAX/USDC market silo addresses
SILO_V2_WAVAX_SILO = "0xDa4b05e351696296060e6a1245C55e32DF8bFC84"  # WAVAX vault (silo0)
SILO_V2_USDC_SILO = "0xfA5f7d5BcD70dC2F031eE906fc692a9e19584CB0"  # USDC vault (silo1)

# Skip reason for borrow/repay tests that require USDC liquidity
SKIP_NO_LIQUIDITY = (
    "Silo V2 WAVAX/USDC market has insufficient borrowable USDC at fork block — "
    "borrow tests require USDC liquidity from other depositors. "
    "Ticket: VIB-2643"
)


def _silo_borrowable_liquidity(web3: Web3, silo_address: str) -> int:
    """Return the borrowable underlying available on a Silo V2 vault.

    totalAssets() is misleading here — it sums Collateral-type (borrowable)
    AND Protected-type (non-borrowable, guaranteed-withdrawal) deposits, so a
    silo with only Protected deposits still reports totalAssets > 0 but
    reverts on borrow. Silo V2's getLiquidity() returns just the amount
    currently available to borrow, which is what the skip guard needs.
    """
    selector = Web3.keccak(text="getLiquidity()")[:4]
    raw = web3.eth.call({"to": silo_address, "data": "0x" + selector.hex()})
    return int.from_bytes(raw, "big") if raw else 0


# =============================================================================
# Borrow/Repay Tests
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.borrow
@pytest.mark.lending
class TestSiloV2BorrowIntent:
    """Test Silo V2 borrow/repay operations using BorrowIntent and RepayIntent.

    These tests verify the full Intent flow:
    - SupplyIntent to deposit collateral (WAVAX into WAVAX/USDC market)
    - BorrowIntent to borrow USDC from the paired silo
    - RepayIntent to repay USDC debt
    - SiloV2ReceiptParser correctly interprets Borrow/Repay events
    - Balance changes match expected amounts

    Silo V2 is isolated lending — depositing WAVAX into the WAVAX silo
    enables borrowing USDC from the paired USDC silo.
    """

    @pytest.mark.asyncio
    async def test_borrow_usdc_with_wavax_collateral(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC borrow with WAVAX collateral using BorrowIntent.

        Flow:
        1. Supply WAVAX as collateral to the WAVAX/USDC market
        2. Create BorrowIntent for USDC on Silo V2
        3. Compile to ActionBundle using IntentCompiler
        4. Execute via ExecutionOrchestrator
        5. Parse receipt for Borrow event
        6. Verify USDC balance increased by exact borrow amount
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("5")  # 5 WAVAX as collateral
        borrow_amount = Decimal("1")  # 1 USDC (very low LTV, well under 30%)

        # Skip if the silo doesn't have enough borrowable USDC for this test —
        # getLiquidity() excludes Protected (non-borrowable) deposits, unlike
        # totalAssets().
        borrow_amount_wei = int(borrow_amount * Decimal(10**usdc_decimals))
        if _silo_borrowable_liquidity(web3, SILO_V2_USDC_SILO) < borrow_amount_wei:
            pytest.skip(SKIP_NO_LIQUIDITY)

        print(f"\n{'='*80}")
        print(f"Test: Supply {supply_amount} WAVAX, then Borrow {borrow_amount} USDC from Silo V2")
        print(f"{'='*80}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Step 1: Supply WAVAX as collateral
        supply_intent = SupplyIntent(
            protocol="silo_v2",
            token="WAVAX",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )

        print(f"\nStep 1: Supplying {supply_amount} WAVAX as collateral...")
        supply_compilation = compiler.compile(supply_intent)
        assert supply_compilation.status.value == "SUCCESS", f"Supply compilation failed: {supply_compilation.error}"
        assert supply_compilation.action_bundle is not None

        supply_exec = await orchestrator.execute(supply_compilation.action_bundle)
        assert supply_exec.success, f"Supply execution failed: {supply_exec.error}"
        print(f"Supply successful! {len(supply_exec.transaction_results)} transactions confirmed")

        # Step 2: Record USDC balance BEFORE borrow
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"\nUSDC before borrow: {format_token_amount(usdc_before, usdc_decimals)}")

        # Step 3: Create BorrowIntent (Silo V2 borrow requires collateral_token/borrow_token fields)
        intent = BorrowIntent(
            protocol="silo_v2",
            collateral_token="WAVAX",
            collateral_amount=Decimal("0"),  # Already supplied above
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            chain=CHAIN_NAME,
        )

        print(f"\nCreated BorrowIntent: protocol={intent.protocol}, borrow_token={intent.borrow_token}, borrow_amount={intent.borrow_amount}")

        # Layer 1: Compilation
        print("Compiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: Execution
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Receipt Parsing
        found_borrow_event = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = SiloV2ReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    silo_address=SILO_V2_USDC_SILO,
                )

                if parse_result.success and parse_result.borrow_amount > 0:
                    print(f"  Borrow amount: {parse_result.borrow_amount}")
                    print(f"  Debt shares: {parse_result.borrow_shares}")
                    assert parse_result.borrow_amount > 0, "Borrow amount must be positive"
                    assert parse_result.borrow_shares > 0, "Debt shares must be positive"
                    found_borrow_event = True

        assert found_borrow_event, "Receipt parser must find at least one Borrow event"

        # Layer 4: Balance Deltas
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before

        print("\n--- Results ---")
        print(f"USDC received: {format_token_amount(usdc_received, usdc_decimals)}")

        expected_usdc_received = int(borrow_amount * Decimal(10**usdc_decimals))
        assert usdc_received == expected_usdc_received, (
            f"USDC received must EXACTLY equal borrow amount. "
            f"Expected: {expected_usdc_received}, Got: {usdc_received}"
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_repay_usdc_after_borrow(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC repay after borrowing using RepayIntent.

        Flow:
        1. Supply WAVAX + borrow USDC (setup)
        2. Create RepayIntent to repay 0.5 USDC
        3. Compile and execute
        4. Parse receipt for Repay event
        5. Verify USDC balance decreased by exact repay amount
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        # Skip if the silo doesn't have enough borrowable USDC for the 1 USDC
        # setup-borrow required by this repay test.
        setup_borrow_wei = int(Decimal("1") * Decimal(10**usdc_decimals))
        if _silo_borrowable_liquidity(web3, SILO_V2_USDC_SILO) < setup_borrow_wei:
            pytest.skip(SKIP_NO_LIQUIDITY)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Setup: Supply 5 WAVAX as collateral
        supply_intent = SupplyIntent(
            protocol="silo_v2",
            token="WAVAX",
            amount=Decimal("5"),
            chain=CHAIN_NAME,
        )
        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_result.action_bundle)
        assert supply_exec.success, f"Initial supply failed: {supply_exec.error}"

        # Setup: Borrow 1 USDC (very low LTV)
        borrow_intent = BorrowIntent(
            protocol="silo_v2",
            collateral_token="WAVAX",
            collateral_amount=Decimal("0"),  # Already supplied
            borrow_token="USDC",
            borrow_amount=Decimal("1"),
            chain=CHAIN_NAME,
        )
        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle)
        assert borrow_exec.success, f"Initial borrow failed: {borrow_exec.error}"

        # Now repay 0.5 USDC (half of borrowed amount)
        repay_amount = Decimal("0.5")

        print(f"\n{'='*80}")
        print(f"Test: Repay {repay_amount} USDC to Silo V2 using RepayIntent")
        print(f"{'='*80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before repay: {format_token_amount(usdc_before, usdc_decimals)}")

        # Create RepayIntent
        intent = RepayIntent(
            protocol="silo_v2",
            token="USDC",
            amount=repay_amount,
            chain=CHAIN_NAME,
        )

        print(f"\nCreated RepayIntent: protocol={intent.protocol}, token={intent.token}, amount={intent.amount}")

        # Layer 1: Compilation
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        # Layer 2: Execution
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt Parsing
        found_repay_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = SiloV2ReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    silo_address=SILO_V2_USDC_SILO,
                )

                if parse_result.success and parse_result.repay_amount > 0:
                    print(f"  Repay amount: {parse_result.repay_amount}")
                    print(f"  Debt shares burned: {parse_result.repay_shares}")
                    assert parse_result.repay_amount > 0, "Repay amount must be positive"
                    assert parse_result.repay_shares > 0, "Debt shares burned must be positive"
                    found_repay_event = True

        assert found_repay_event, "Receipt parser must find at least one Repay event"

        # Layer 4: Balance Deltas
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print(f"\nUSDC spent on repay: {format_token_amount(usdc_spent, usdc_decimals)}")

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

        Attempting to borrow USDC without supplying collateral first should fail.
        Balance must be unchanged after the failed attempt.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        print(f"\n{'='*80}")
        print("Test: BorrowIntent without Collateral (Silo V2)")
        print(f"{'='*80}")

        # Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # Create BorrowIntent without any collateral supplied
        intent = BorrowIntent(
            protocol="silo_v2",
            collateral_token="WAVAX",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("100"),
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

        # Execute — should fail due to no collateral
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert not execution_result.success, "Execution should fail without collateral"
        print(f"Execution failed as expected: {execution_result.error}")

        # Balance conservation check
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed borrow"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
