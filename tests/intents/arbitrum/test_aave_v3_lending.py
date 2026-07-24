"""Production-grade lending intent tests for Aave V3 on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for lending operations:
1. Create lending intents (SupplyIntent, WithdrawIntent, BorrowIntent, RepayIntent)
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using AaveV3ReceiptParser
5. Verify balance changes and account data are correct
6. Layer 5 — persist the real ExecutionResult through the real accounting
   pipeline (ledger -> outbox -> AccountingProcessor.drain_one) into a
   throwaway SQLite and assert the typed LendingAccountingEvent is correct.

Layer 5 (epic VIB-4591 / ticket VIB-4593): the borrow-then-repay happy path
asserts the exact ``principal_delta_usd`` / ``interest_delta_usd`` FIFO split;
the standalone-repay path asserts the degradation contract
(``interest_delta_usd is None``, never a fabricated 0). Aave V3 has a
pre/post-state reader, so the Anvil ``eth_call`` adapter populates
before/after collateral / debt / health-factor at ``confidence=HIGH``. The
failure path asserts zero ``accounting_events`` rows.

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/arbitrum/test_aave_v3_lending.py -v -s
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.framework.accounting.lending_accounting import (
    capture_lending_post_state,
    capture_lending_pre_state,
    lending_state_to_dict,
)
from almanak.connectors.aave_v3.adapter import (
    AAVE_V3_POOL_ADDRESSES,
)
from almanak.connectors.aave_v3.receipt_parser import AaveV3ReceiptParser
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionResult,
)
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_accounting_persisted,
    assert_no_accounting_on_failure,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"
PROTOCOL = "aave_v3"

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
# Layer 5 helpers (shared)
# =============================================================================
#
# These mirror the runner's accounting wiring: ``enrich_result`` so the
# ledger entry carries extracted_data; ``capture_lending_pre_state`` /
# ``capture_lending_post_state`` via the test-scoped Anvil ``eth_call``
# adapter so the lending category handler reads real collateral/debt/HF and
# emits ``confidence=HIGH``. The conftest Layer-5 helper threads the
# serialized state dicts into ``build_ledger_entry``.


def _execution_context(wallet: str) -> ExecutionContext:
    return ExecutionContext(
        deployment_id="layer5-aave-v3-lending",
        chain=CHAIN_NAME,
        wallet_address=wallet,
        protocol=PROTOCOL,
        simulation_enabled=True,
    )


def _enrich_for_accounting(
    execution_result: ExecutionResult,
    intent: Any,
    wallet: str,
    bundle_metadata: dict | None = None,
) -> ExecutionResult:
    return enrich_result(
        execution_result,
        intent,
        _execution_context(wallet),
        live_mode=False,
        bundle_metadata=bundle_metadata,
    )


def _capture_lending_state(
    intent: Any,
    wallet: str,
    reader: Any,
    price_oracle: dict[str, Decimal],
    *,
    post: bool,
) -> dict | None:
    """Capture and serialize Aave V3 pre/post state via the Anvil eth_call adapter.

    Returns the runner-shaped state dict (``lending_state_to_dict`` output) or
    ``None`` when the read genuinely yields nothing — never a fabricated zero.
    """
    capture = capture_lending_post_state if post else capture_lending_pre_state
    state = capture(
        intent=intent,
        chain=CHAIN_NAME,
        wallet_address=wallet,
        gateway_client=reader,
        price_oracle=price_oracle,
    )
    return lending_state_to_dict(state, protocol=PROTOCOL)


def _payload(row: dict) -> dict:
    return json.loads(row["payload_json"])


def _assert_identity(row: dict, *, event_type: str, wallet: str) -> None:
    """Identity sextuple per epic VIB-4591 decision #5 (no agent_id)."""
    assert row["deployment_id"] == "layer5-intent-test"
    assert row["cycle_id"] == "layer5-cycle"
    assert row["execution_mode"] == "paper"
    assert row["event_type"] == event_type
    assert row["tx_hash"], "accounting row must link to an on-chain tx_hash"
    assert row["ledger_entry_id"], "accounting row must link to transaction_ledger"
    assert row["wallet_address"].lower() == wallet.lower()


def _assert_no_lot_id(row: dict, payload: dict) -> None:
    """Epic decision #6: no lot_id on the persisted lending event."""
    assert "lot_id" not in row
    assert "lot_id" not in payload


def _assert_high_confidence_state(payload: dict) -> None:
    """Aave V3 has a pre/post-state reader → confidence=HIGH with state populated."""
    assert payload["confidence"] == "HIGH", (
        f"Aave V3 lending must persist confidence=HIGH (reader + Anvil eth_call adapter), "
        f"got {payload['confidence']!r} (unavailable_reason={payload.get('unavailable_reason')!r})"
    )
    assert payload["collateral_value_before_usd"] is not None, "before-collateral must be populated"
    assert payload["collateral_value_after_usd"] is not None, "after-collateral must be populated"
    assert payload["debt_value_before_usd"] is not None, "before-debt must be populated"
    assert payload["debt_value_after_usd"] is not None, "after-debt must be populated"
    assert payload["health_factor_before"] is not None, "before-health-factor must be populated"
    assert payload["health_factor_after"] is not None, "after-health-factor must be populated"


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


@pytest.mark.arbitrum
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
    - Layer 5: the real accounting pipeline persists a correct
      LendingAccountingEvent (confidence=HIGH, identity sextuple)
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
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test USDC supply using SupplyIntent.

        Flow:
        1. Create SupplyIntent for USDC
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify balances and account data changed correctly
        5. Layer 5: assert persisted SUPPLY accounting event
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("1000")  # 1000 USDC

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

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        # Execute via ExecutionOrchestrator (with simulation enabled for accurate gas estimation)
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = AaveV3ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.supplies:
                    for supply_event in parse_result.supplies:
                        print(f"  Supply amount:  {supply_event.amount}")
                        print(f"  Reserve: {supply_event.reserve}")

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print("\n--- Results ---")
        print(f"USDC spent: {format_token_amount(usdc_spent, decimals)}")

        # Verify USDC was spent
        expected_usdc_spent = int(supply_amount * Decimal(10**decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal supply amount. " f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify account data changed
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Collateral after: {account_data_after['totalCollateralBase']}")

        assert (
            account_data_after["totalCollateralBase"] > account_data_before["totalCollateralBase"]
        ), "Collateral must increase after supply"

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True
        )

        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="SUPPLY",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="SUPPLY", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        _assert_high_confidence_state(payload)
        assert payload["asset"] == "USDC"
        assert payload["amount_token"] is not None
        assert Decimal(payload["amount_token"]) == supply_amount
        # SUPPLY drains wallet inventory: principal_delta_usd is measured (the
        # supplied principal in USD); interest is not applicable on SUPPLY.
        assert payload["principal_delta_usd"] is not None, "SUPPLY must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "SUPPLY has no interest leg — must be None, not 0"
        # Supply increases collateral on-chain.
        assert Decimal(payload["collateral_value_after_usd"]) > Decimal(payload["collateral_value_before_usd"])

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
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test USDC withdraw using WithdrawIntent (after supplying).

        Flow:
        1. Supply USDC first
        2. Create WithdrawIntent to withdraw portion
        3. Compile and execute
        4. Verify balances changed correctly
        5. Layer 5: assert persisted WITHDRAW accounting event
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
            protocol="aave_v3",
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
        print(f"Test: Withdraw {withdraw_amount} USDC from Aave V3 using WithdrawIntent")
        print(f"{'='*80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before withdraw: {format_token_amount(usdc_before, decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Collateral before: {account_data_before['totalCollateralBase']}")

        # Create WithdrawIntent
        intent = WithdrawIntent(
            protocol="aave_v3",
            token="USDC",
            amount=withdraw_amount,
            chain=CHAIN_NAME,
        )

        print(f"\nCreated WithdrawIntent: protocol={intent.protocol}, token={intent.token}, amount={intent.amount}")

        # Compile and execute
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

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

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        # The SUPPLY above was NOT persisted through the Layer-5 harness, so
        # the FIFO supply pool is empty: WITHDRAW degrades — principal falls
        # back to the total and interest_delta_usd stays None (never a
        # fabricated 0). This is the degradation contract for an unmatched
        # withdraw (epic decision #6, mirrors standalone repay).
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True
        )

        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="WITHDRAW",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="WITHDRAW", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        _assert_high_confidence_state(payload)
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == withdraw_amount
        assert payload["principal_delta_usd"] is not None, "WITHDRAW must measure a principal leg"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, (
            "Unmatched WITHDRAW (no Layer-5 SUPPLY lot) must degrade interest to "
            "None — never a fabricated 0"
        )
        # Withdraw reduces collateral on-chain.
        assert Decimal(payload["collateral_value_after_usd"]) < Decimal(payload["collateral_value_before_usd"])

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
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test that SupplyIntent with insufficient balance fails gracefully.

        Layer 5 failure contract: a failed execution must write ZERO
        accounting_events rows (books-side mirror of "balances unchanged").
        """
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

        # Verify balance unchanged
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_balance, "Balance must be unchanged after failed supply"

        # ── Layer 5: failure-path accounting contract ────────────────────────
        failed_result = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        await assert_no_accounting_on_failure(
            layer5_accounting_harness,
            intent=intent,
            result=failed_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )

        print("\nALL CHECKS PASSED")


# =============================================================================
# Borrow/Repay Tests
# =============================================================================


@pytest.mark.arbitrum
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
    - Layer 5: real accounting pipeline persists a correct
      LendingAccountingEvent with the exact FIFO principal/interest split
    """

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_usdc_with_wsteth_collateral_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test borrowing USDC against wstETH collateral via the two-intent form.

        Uses the accounting-correct SUPPLY -> standalone BORROW split (#2827):
        a bundled ``BorrowIntent(collateral_amount > 0)`` is fail-closed at the
        intent validator because accounting writes one event per intent — the
        supply leg would collapse into the single BORROW event, and (post
        VIB-5218 merged-receipt money legs) the BORROW row would carry the
        SUPPLY leg's asset/amount instead of the borrowed reserve.

        Uses wstETH as the collateral asset because Aave governance has frozen
        the Arbitrum WETH reserve (no new supplies allowed); wstETH still
        carries an active LTV (~75%) and is unfrozen, so it's the canonical
        ETH-correlated collateral that works across ethereum / arbitrum / base.
        See #1696 for the freeze details.

        Flow:
        1. Setup: supply wstETH collateral via SupplyIntent (use_as_collateral
           defaults True); verify the Supply receipt event
        2. Create a standalone BorrowIntent (collateral_amount=0) for USDC
        3. Compile + execute via the production pipeline
        4. Verify USDC balance increased and debt was created
        5. Layer 5: assert persisted BORROW accounting event — principal
           measured, interest_delta_usd None (no repay leg yet)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wsteth = tokens["wstETH"]
        usdc = tokens["USDC"]

        wsteth_decimals = get_token_decimals(web3, wsteth)
        usdc_decimals = get_token_decimals(web3, usdc)

        # Supply 1 wstETH as collateral (~$3500), borrow 500 USDC (~14% LTV)
        collateral_amount = Decimal("1")
        borrow_amount = Decimal("500")

        print(f"\n{'='*80}")
        print(f"Test: Borrow {borrow_amount} USDC with {collateral_amount} wstETH collateral (two-intent form)")
        print(f"{'='*80}")

        # Record balances BEFORE
        wsteth_before = get_token_balance(web3, wsteth, funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        print(f"wstETH before: {format_token_amount(wsteth_before, wsteth_decimals)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Debt before: {account_data_before['totalDebtBase']}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        # Step 1 — supply the wstETH collateral (setup; deliberately not
        # persisted through Layer 5: this test asserts the BORROW row, and the
        # FIFO borrow lot does not depend on the supply lot).
        supply_intent = SupplyIntent(
            protocol="aave_v3",
            token="wstETH",
            amount=collateral_amount,
            chain=CHAIN_NAME,
        )
        supply_compile = compiler.compile(supply_intent)
        assert supply_compile.status.value == "SUCCESS", f"Supply compilation failed: {supply_compile.error}"
        assert supply_compile.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_compile.action_bundle, execution_context)
        assert supply_exec.success, f"Collateral supply failed: {supply_exec.error}"

        # Layer 3 (supply leg): the Supply event must land on the wstETH reserve.
        supply_event_seen = False
        for tx_result in supply_exec.transaction_results:
            if tx_result.receipt:
                parse_result = AaveV3ReceiptParser().parse_receipt(tx_result.receipt.to_dict())
                if parse_result.success and parse_result.supplies:
                    collateral_supply = next(
                        (s for s in parse_result.supplies if s.reserve.lower() == wsteth.lower()),
                        None,
                    )
                    assert collateral_supply is not None, (
                        f"Expected a Supply event for wstETH collateral. Got reserves: "
                        f"{[s.reserve for s in parse_result.supplies]}"
                    )
                    assert collateral_supply.amount > 0, "wstETH collateral supply amount must be > 0"
                    supply_event_seen = True
        assert supply_event_seen, "Expected at least one Supply event for the wstETH collateral leg"

        # Step 2 — standalone borrow (the only shape the public API allows).
        intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="wstETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
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

        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        # Execute via ExecutionOrchestrator
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts (Layer 3) — the standalone borrow must emit a Borrow
        # event on the USDC reserve and NO Supply event (collateral moved in
        # step 1; a Supply here would mean the bundled shape leaked back).
        borrow_event_seen = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = AaveV3ReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success:
                    assert not parse_result.supplies, (
                        f"Standalone borrow must not emit Supply events. Got reserves: "
                        f"{[s.reserve for s in parse_result.supplies]}"
                    )
                    if parse_result.borrows:
                        usdc_borrow = next(
                            (b for b in parse_result.borrows if b.reserve.lower() == usdc.lower()),
                            None,
                        )
                        assert usdc_borrow is not None, (
                            f"Expected a Borrow event for USDC. Got reserves: "
                            f"{[b.reserve for b in parse_result.borrows]}"
                        )
                        assert usdc_borrow.amount > 0, "USDC borrow amount must be > 0"
                        borrow_event_seen = True
                        for borrow_event in parse_result.borrows:
                            print(f"  Borrow amount: {borrow_event.amount}")
                            print(f"  Interest rate mode: {borrow_event.interest_rate_mode}")

        assert borrow_event_seen, "Expected at least one Borrow event for the USDC borrow leg"

        # Verify balance changes
        wsteth_after = get_token_balance(web3, wsteth, funded_wallet)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)

        wsteth_spent = wsteth_before - wsteth_after
        usdc_received = usdc_after - usdc_before

        print("\n--- Results ---")
        print(f"wstETH spent (collateral): {format_token_amount(wsteth_spent, wsteth_decimals)}")
        print(f"USDC received (borrowed): {format_token_amount(usdc_received, usdc_decimals)}")

        # Verify wstETH was spent as collateral
        expected_wsteth_spent = int(collateral_amount * Decimal(10**wsteth_decimals))
        assert wsteth_spent == expected_wsteth_spent, (
            f"wstETH spent must EXACTLY equal collateral amount. "
            f"Expected: {expected_wsteth_spent}, Got: {wsteth_spent}"
        )

        # Verify USDC was received (allow small tolerance for Aave origination fees/rounding)
        expected_usdc_received = int(borrow_amount * Decimal(10**usdc_decimals))
        # Allow up to 1% tolerance for protocol fees and rounding
        min_expected = int(expected_usdc_received * Decimal("0.99"))
        assert usdc_received >= min_expected, (
            f"USDC received must be at least 99% of borrow amount. "
            f"Expected min: {min_expected}, Got: {usdc_received}"
        )
        assert usdc_received <= expected_usdc_received, (
            f"USDC received should not exceed borrow amount. "
            f"Expected max: {expected_usdc_received}, Got: {usdc_received}"
        )

        # Verify account data changed
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Debt after: {account_data_after['totalDebtBase']}")
        print(f"Health factor: {account_data_after['healthFactor']}")

        assert account_data_after["totalDebtBase"] > 0, "Debt must be created"
        assert account_data_after["healthFactor"] > 1e18, "Health factor must be > 1.0"

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True
        )

        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="BORROW",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="BORROW", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        _assert_high_confidence_state(payload)
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == borrow_amount
        # BORROW records the FIFO principal lot: principal measured, interest
        # has no leg yet (a repay would match it) — must be None, not 0.
        assert payload["principal_delta_usd"] is not None, "BORROW must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "BORROW has no interest leg yet — must be None, not 0"
        # Borrow creates debt on-chain.
        assert Decimal(payload["debt_value_after_usd"]) > Decimal(payload["debt_value_before_usd"])

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
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test repaying USDC debt using RepayIntent.

        Flow:
        1. Setup: supply wstETH collateral, then a standalone borrow — the
           accounting-correct two-intent form (#2827). (wstETH chosen for
           collateral because Aave froze the WETH reserve on Arbitrum — see
           #1696.)
        2. Create RepayIntent to repay partial debt
        3. Compile and execute
        4. Verify USDC balance decreased and debt was reduced
        5. Layer 5: persist BOTH the BORROW and the REPAY through the same
           harness so the FIFO basis pool matches — assert the EXACT
           principal_delta_usd / interest_delta_usd split (epic decision #6).
           The supply setup is not persisted: the split matches the borrow
           lot only.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        # Setup: supply collateral, then a standalone borrow (two-intent form).
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        supply_intent = SupplyIntent(
            protocol="aave_v3",
            token="wstETH",
            amount=Decimal("1"),
            chain=CHAIN_NAME,
        )
        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_exec.success, f"Collateral supply setup failed: {supply_exec.error}"

        borrow_amount = Decimal("500")
        borrow_intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="wstETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )

        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_pre_state = _capture_lending_state(
            borrow_intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )
        borrow_exec_result = await orchestrator.execute(borrow_result.action_bundle, execution_context)
        assert borrow_exec_result.success, f"Borrow setup failed: {borrow_exec_result.error}"

        # Layer 5: persist the BORROW so the FIFO basis pool holds the lot the
        # REPAY will match against (this is what makes the split exact).
        borrow_enriched = _enrich_for_accounting(
            borrow_exec_result, borrow_intent, funded_wallet, borrow_result.action_bundle.metadata
        )
        borrow_post_state = _capture_lending_state(
            borrow_intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True
        )
        borrow_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=borrow_intent,
            result=borrow_enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="BORROW",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=borrow_pre_state,
            post_state=borrow_post_state,
        )
        borrow_payload = _payload(borrow_row)
        borrowed_principal_usd = Decimal(borrow_payload["principal_delta_usd"])

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
            protocol="aave_v3",
            token="USDC",
            amount=repay_amount,
            chain=CHAIN_NAME,
        )

        print(f"\nCreated RepayIntent: token={intent.token}, amount={intent.amount}")

        # Compile and execute
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

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

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print(f"\nUSDC spent (repaid): {format_token_amount(usdc_spent, usdc_decimals)}")

        expected_usdc_spent = int(repay_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal repay amount. " f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify account data changed
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Debt after: {account_data_after['totalDebtBase']}")
        print(f"Health factor after: {account_data_after['healthFactor']}")

        assert account_data_after["totalDebtBase"] < account_data_before["totalDebtBase"], "Debt must decrease"
        assert (
            account_data_after["healthFactor"] > account_data_before["healthFactor"]
        ), "Health factor must improve"

        # ── Layer 5: borrow-then-repay FIFO split ────────────────────────────
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True
        )

        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="REPAY",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="REPAY", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        _assert_high_confidence_state(payload)
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == repay_amount

        # Exact FIFO split: the REPAY matched the prior BORROW lot in the same
        # harness. Repaying 200 of a 500 borrow within the same Anvil block
        # accrues no interest, so the entire 200 is matched principal and the
        # interest leg is a measured zero (NOT None — the match succeeded).
        # principal + interest must reconcile to the repaid cash flow.
        assert payload["principal_delta_usd"] is not None, "matched REPAY must measure principal"
        assert payload["interest_delta_usd"] is not None, (
            "matched REPAY (BORROW lot present in harness) must produce a "
            "measured interest leg — not None"
        )
        principal_usd = Decimal(payload["principal_delta_usd"])
        interest_usd = Decimal(payload["interest_delta_usd"])
        # Matched principal in USD = repaid fraction of the borrowed principal.
        # Both legs use the session price oracle, so this is exact (no MEV on
        # Anvil): repay_amount / borrow_amount of borrowed_principal_usd.
        repaid_usd = repay_amount * (borrowed_principal_usd / borrow_amount)
        assert principal_usd == repaid_usd, (
            f"FIFO principal_delta_usd must equal the matched principal "
            f"({repaid_usd}); got {principal_usd}"
        )
        assert interest_usd == Decimal("0"), (
            f"same-block partial repay accrues no interest — interest_delta_usd "
            f"must be a measured 0, got {interest_usd}"
        )
        assert principal_usd + interest_usd == repaid_usd, "principal + interest must tie to repaid cash flow"
        # Repay reduces debt on-chain.
        assert Decimal(payload["debt_value_after_usd"]) < Decimal(payload["debt_value_before_usd"])

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_standalone_repay_degrades_interest_to_none(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Standalone repay degradation contract (epic VIB-4591 decision #6).

        A REPAY whose matching BORROW lot is NOT in the Layer-5 FIFO basis
        pool (here: the on-chain borrow is executed but deliberately not
        persisted through the harness) must degrade ``interest_delta_usd`` to
        ``None`` — never a fabricated 0. ``match_repay`` consumes no lots, so
        ``repaid_principal == 0`` and ``principal_delta_usd`` is the *measured*
        attributable zero (``_amount_to_usd(0)`` — a real Decimal('0'), not
        None, and not the full repaid cash flow: the REPAY handler does not
        fall back to total the way WITHDRAW does). This is the books-side
        proof that linkage's only durable form is the split: an unmatched
        repay attributes no principal and never invents an interest figure.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        # On-chain borrow setup (two-intent form, #2827) — intentionally NOT
        # persisted through Layer 5, so the FIFO basis pool has no matching
        # BORROW lot.
        supply_intent = SupplyIntent(
            protocol="aave_v3",
            token="wstETH",
            amount=Decimal("1"),
            chain=CHAIN_NAME,
        )
        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_exec.success, f"Collateral supply setup failed: {supply_exec.error}"

        borrow_intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="wstETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )
        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle, execution_context)
        assert borrow_exec.success, f"Borrow setup failed: {borrow_exec.error}"

        repay_amount = Decimal("200")
        print(f"\n{'='*80}")
        print(f"Test: Standalone Repay {repay_amount} USDC — degradation contract")
        print(f"{'='*80}")

        intent = RepayIntent(
            protocol="aave_v3",
            token="USDC",
            amount=repay_amount,
            chain=CHAIN_NAME,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: parser must still decode the repay
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
        assert decoded_repay_amount > 0, "Layer 3: parser must decode the Repay event"

        # Layer 4: exact balance delta
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        expected_usdc_spent = int(repay_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal repay amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # ── Layer 5: degradation contract ────────────────────────────────────
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True
        )

        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="REPAY",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="REPAY", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        _assert_high_confidence_state(payload)
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == repay_amount
        # No matching BORROW lot in the harness. match_repay consumes nothing
        # → repaid_principal == 0 → principal_delta_usd is the *measured*
        # attributable zero (a real Decimal('0'), NOT None and NOT the full
        # repaid amount — the REPAY handler does not fall back to total).
        # interest_delta_usd degrades to None (never a fabricated 0). This is
        # the epic's standalone-repay degradation contract.
        assert payload["principal_delta_usd"] is not None, (
            "unmatched REPAY must report a measured principal (Decimal('0'), not None)"
        )
        assert Decimal(payload["principal_delta_usd"]) == 0, (
            "unmatched REPAY attributes zero principal (FIFO pool empty) — a "
            "measured 0, not the full repaid cash flow"
        )
        assert payload["interest_delta_usd"] is None, (
            "standalone repay with no Layer-5 BORROW lot must degrade "
            "interest_delta_usd to None — never a fabricated 0"
        )

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
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test that borrowing without collateral fails gracefully.

        Layer 5 failure contract: zero accounting_events rows.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]

        print(f"\n{'='*80}")
        print("Test: BorrowIntent without Collateral (should fail)")
        print(f"{'='*80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        # Try to borrow without supplying collateral (collateral_amount = 0)
        intent = BorrowIntent(
            protocol="aave_v3",
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

        # Verify no USDC received
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed borrow"

        # ── Layer 5: failure-path accounting contract ────────────────────────
        failed_result = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        await assert_no_accounting_on_failure(
            layer5_accounting_harness,
            intent=intent,
            result=failed_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
