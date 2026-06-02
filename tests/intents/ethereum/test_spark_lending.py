"""Production-grade lending intent tests for Spark on Ethereum.

Spark is an Aave V3 fork deployed on Ethereum mainnet, so this file mirrors
``tests/intents/arbitrum/test_aave_v3_lending.py`` 1:1, swapping in the
Spark adapter, Spark receipt parser, and Spark pool address.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for lending operations:
1. Create lending intents (SupplyIntent, WithdrawIntent, BorrowIntent, RepayIntent)
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using SparkReceiptParser
5. Verify balance changes and account data are correct
6. Layer 5 — persist the real ExecutionResult through the real accounting
   pipeline (ledger -> outbox -> AccountingProcessor.drain_one) into a
   throwaway SQLite and assert the typed LendingAccountingEvent is correct.

Layer 5 (epic VIB-4591 / ticket VIB-4608): mirrors the merged Compound V3 and
Aave V3 goldens. The lending category handler is protocol-agnostic — it keys on
``intent_type`` and the FIFO basis store, not on the protocol — so the FIFO
principal / interest split assertions are identical to the Aave V3 golden.

SPARK CHAIN-STATE (VIB-4929 PR-3c — closes VIB-4963): Spark is an Aave V3 fork
with an identical ``getUserAccountData`` / ``getUserEMode`` ABI (USD-denominated
on-chain), so it routes through the shared ``AAVE_FORK_ACCOUNT_STATE_READ`` spec.
Adding ``spark`` to ``_GENERIC_PRE_STATE_PROTOCOLS`` in
``almanak/framework/accounting/lending_accounting.py`` enables a HIGH-confidence
read: ``capture_lending_pre_state`` / ``capture_lending_post_state`` populate
before/after collateral / debt / health-factor from real eth_calls against the
Spark pool. So Spark Layer 5 now asserts the HIGH-confidence contract
(``_assert_high_confidence_state``) like the Aave / Compound goldens. (Earlier
this file asserted ESTIMATED degradation while the read was gated out pending
fork verification — VIB-4963; that gate is now lifted.)

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/ethereum/test_spark_lending.py -v -s
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.spark.adapter import SPARK_POOL_ADDRESSES
from almanak.connectors.spark.receipt_parser import SparkReceiptParser
from almanak.framework.accounting.lending_accounting import (
    capture_lending_post_state,
    capture_lending_pre_state,
    lending_state_to_dict,
)
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

CHAIN_NAME = "ethereum"
PROTOCOL = "spark"

# Spark Pool ABI (minimal — Spark is an Aave V3 fork, getUserAccountData is identical)
SPARK_POOL_ABI = [
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
    """Get user account data from the Spark Pool contract.

    Spark is an Aave V3 fork, so the ``getUserAccountData`` ABI is identical.
    """
    pool_address = SPARK_POOL_ADDRESSES[CHAIN_NAME]
    pool_contract = web3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=SPARK_POOL_ABI)

    result = pool_contract.functions.getUserAccountData(Web3.to_checksum_address(user)).call()

    return {
        "totalCollateralBase": result[0],
        "totalDebtBase": result[1],
        "availableBorrowsBase": result[2],
        "currentLiquidationThreshold": result[3],
        "ltv": result[4],
        "healthFactor": result[5],
    }


def _safe_usdc_borrow_amount(price_oracle: dict[str, Decimal], weth_amount: Decimal) -> Decimal:
    """Return a USDC borrow amount targeting ~25% LTV against ``weth_amount`` of WETH.

    The 4-layer mandate caps lending borrow tests at 30% LTV; the price oracle
    is session-scoped and reads live CoinGecko prices, so a hardcoded
    ``borrow_amount`` silently breaches the cap whenever WETH drops below the
    ratio that made it ~30% at write time. Computing from the fixture keeps
    headroom durable across normal market drift. Targets 25% LTV (5% headroom
    under the 30% cap); quantizes to 2 decimals so USDC rounds to whole cents.
    """
    weth_price_usd = price_oracle["WETH"]
    return (weth_amount * weth_price_usd * Decimal("0.25")).quantize(Decimal("0.01"))


# =============================================================================
# Layer 5 helpers (shared)
# =============================================================================
#
# Mirror the merged Aave V3 / Compound V3 goldens. ``enrich_result`` makes the
# ledger entry carry extracted_data; ``capture_lending_pre_state`` /
# ``capture_lending_post_state`` dispatch on ``intent.protocol``. Spark has NO
# entry in ``_PROTOCOL_PRE_STATE_READERS`` (VIB-4963), so both captures return
# ``None`` and ``lending_state_to_dict`` serializes ``None`` — the persisted
# event therefore degrades to ``confidence=ESTIMATED`` with no before/after
# chain state. The conftest Layer-5 helper threads the serialized state dicts
# (here ``None``) into ``build_ledger_entry``.


def _execution_context(wallet: str) -> ExecutionContext:
    # NOTE: this deployment_id flows only into ``enrich_result`` (it labels the
    # ExecutionContext for enrichment). It is deliberately NOT what lands in
    # the persisted accounting row: the conftest ``assert_accounting_persisted``
    # helper stamps the row's deployment_id from its own ``deployment_id=
    # "layer5-intent-test"`` default, which is what ``_assert_identity``
    # checks. This split (descriptive enrichment id vs canonical persisted
    # identity) mirrors the merged Aave V3 / Compound V3 goldens.
    return ExecutionContext(
        deployment_id="layer5-spark-lending",
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
    """Capture and serialize Spark pre/post state via the Anvil eth_call adapter.

    Returns the runner-shaped state dict (``lending_state_to_dict`` output) or
    ``None`` — never a fabricated zero. For Spark this currently ALWAYS returns
    ``None`` because Spark has no pre/post-state reader (VIB-4963); the call is
    kept to mirror the runner's wiring exactly so a future reader fix lights up
    the HIGH-confidence path with no test change.
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
    """Spark HIGH-confidence chain-state contract (VIB-4929 PR-3c — closes VIB-4963).

    Spark now routes through the shared ``AAVE_FORK_ACCOUNT_STATE_READ`` spec: it is an
    Aave V3 fork with an identical ``getUserAccountData`` / ``getUserEMode`` ABI and is
    USD-denominated on-chain, so adding ``spark`` to ``_GENERIC_PRE_STATE_PROTOCOLS``
    enables a HIGH-confidence read with before/after collateral / debt / health-factor
    populated from real eth_calls against the Spark pool. Mirrors the Aave V3 golden's
    ``_assert_high_confidence_state``. Empty≠Zero≠None: a measured ``0`` is a real value,
    distinct from ``None``.
    """
    assert payload["confidence"] == "HIGH", (
        f"Spark lending must persist confidence=HIGH (shared Aave V3 reader + Anvil "
        f"eth_call adapter), got {payload['confidence']!r} "
        f"(unavailable_reason={payload.get('unavailable_reason')!r})"
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


@pytest.mark.ethereum
@pytest.mark.supply
@pytest.mark.lending
class TestSparkSupplyIntent:
    """Test Spark supply/withdraw operations using SupplyIntent and WithdrawIntent.

    These tests verify the full Intent flow:
    - SupplyIntent creation with token symbols and amounts
    - IntentCompiler generates correct Spark transactions
    - Transactions execute successfully on-chain
    - SparkReceiptParser correctly interprets results
    - Balance changes and account data match expected amounts
    - Layer 5: the real accounting pipeline persists a correct
      LendingAccountingEvent (degraded chain state per VIB-4963, identity
      sextuple, FIFO principal split)
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

        print(f"\n{'=' * 80}")
        print(f"Test: Supply {supply_amount} USDC to Spark using SupplyIntent")
        print(f"{'=' * 80}")

        # Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before: {format_token_amount(usdc_before, decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Collateral before: {account_data_before['totalCollateralBase']}")

        # Create SupplyIntent
        intent = SupplyIntent(
            protocol="spark",
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
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        # Execute via ExecutionOrchestrator (with simulation enabled for accurate gas estimation)
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts (Layer 3) — at least one Supply event with non-zero amount on the USDC reserve
        supply_event_seen = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = SparkReceiptParser()  # Spark parser takes no chain= kwarg
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.supplies:
                    assert len(parse_result.supplies) >= 1, "Expected at least one Supply event"
                    assert parse_result.supplies[0].amount > 0, "Supply event amount must be > 0"
                    assert parse_result.supplies[0].reserve.lower() == usdc.lower(), "Supply event reserve must be USDC"
                    supply_event_seen = True
                    for supply_event in parse_result.supplies:
                        print(f"  Supply amount:  {supply_event.amount}")
                        print(f"  Reserve: {supply_event.reserve}")

        assert supply_event_seen, "Expected at least one Supply event across all transactions"

        # Verify balance changes (Layer 4)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print("\n--- Results ---")
        print(f"USDC spent: {format_token_amount(usdc_spent, decimals)}")

        # Verify USDC was spent EXACTLY equal to the supply amount
        expected_usdc_spent = int(supply_amount * Decimal(10**decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal supply amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify account data changed
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Collateral after: {account_data_after['totalCollateralBase']}")

        # USDC has ltv=0 on Spark Ethereum (supply earns yield but isn't collateral-eligible).
        # totalCollateralBase MUST be unchanged — strict equality is the right invariant.
        assert account_data_after["totalCollateralBase"] == account_data_before["totalCollateralBase"], (
            f"USDC supply on Spark Ethereum must NOT change totalCollateralBase (ltv=0). "
            f"Before: {account_data_before['totalCollateralBase']}, "
            f"After: {account_data_after['totalCollateralBase']}"
        )

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True)

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
        # VIB-4929 PR-3c: Spark routes through the shared Aave V3 reader → HIGH
        # confidence with before/after chain state populated (closes VIB-4963).
        _assert_high_confidence_state(payload)
        # A plain USDC supply on Spark is NOT auto-enabled as collateral (USDC reserve
        # LTV config) → totalCollateralBase stays a measured 0 (verified on-fork); the
        # HIGH read reports it faithfully — confidence=HIGH proves the read ran, and
        # Empty≠Zero (a measured 0, not None). Collateral deltas are asserted on the
        # WETH-collateralised borrow/repay tests, where the asset IS collateral.
        assert payload["asset"] == "USDC"
        assert payload["amount_token"] is not None
        assert Decimal(payload["amount_token"]) == supply_amount
        # SUPPLY drains wallet inventory: principal_delta_usd is measured (the
        # supplied principal in USD); interest is not applicable on SUPPLY. The
        # FIFO basis store is unaffected by the missing chain-state reader.
        assert payload["principal_delta_usd"] is not None, "SUPPLY must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "SUPPLY has no interest leg — must be None, not 0"

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
            protocol="spark",
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

        print(f"\n{'=' * 80}")
        print(f"Test: Withdraw {withdraw_amount} USDC from Spark using WithdrawIntent")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before withdraw: {format_token_amount(usdc_before, decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Collateral before: {account_data_before['totalCollateralBase']}")

        # Create WithdrawIntent
        intent = WithdrawIntent(
            protocol="spark",
            token="USDC",
            amount=withdraw_amount,
            chain=CHAIN_NAME,
        )

        print(f"\nCreated WithdrawIntent: protocol={intent.protocol}, token={intent.token}, amount={intent.amount}")

        # Compile and execute
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Parse receipts (Layer 3) — at least one Withdraw event with non-zero amount on the USDC reserve
        withdraw_event_seen = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = SparkReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.withdraws:
                    assert len(parse_result.withdraws) >= 1, "Expected at least one Withdraw event"
                    assert parse_result.withdraws[0].amount > 0, "Withdraw event amount must be > 0"
                    assert parse_result.withdraws[0].reserve.lower() == usdc.lower(), (
                        "Withdraw event reserve must be USDC"
                    )
                    withdraw_event_seen = True

        assert withdraw_event_seen, "Expected at least one Withdraw event across all transactions"

        # Verify balance changes (Layer 4)
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

        # USDC has ltv=0 on Spark Ethereum (supply earns yield but isn't collateral-eligible).
        # totalCollateralBase MUST be unchanged — strict equality is the right invariant.
        assert account_data_after["totalCollateralBase"] == account_data_before["totalCollateralBase"], (
            f"USDC supply/withdraw on Spark Ethereum must NOT change totalCollateralBase (ltv=0). "
            f"Before: {account_data_before['totalCollateralBase']}, "
            f"After: {account_data_after['totalCollateralBase']}"
        )

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        # The SUPPLY above was NOT persisted through the Layer-5 harness, so the
        # FIFO supply pool is empty: WITHDRAW degrades — principal falls back to
        # the total and interest_delta_usd stays None (never a fabricated 0).
        # This is the degradation contract for an unmatched withdraw (epic
        # decision #6, mirrors standalone repay), identical to the Aave V3
        # golden. The chain-state read is ALSO degraded (Spark has no reader,
        # VIB-4963) — distinct from the unmatched-FIFO degradation.
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True)

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
        # USDC is not collateral on this Spark market (see the SUPPLY test) → collateral
        # stays a measured 0 across the withdraw; the HIGH read reports it faithfully.
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == withdraw_amount
        assert payload["principal_delta_usd"] is not None, "WITHDRAW must measure a principal leg"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, (
            "Unmatched WITHDRAW (no Layer-5 SUPPLY lot) must degrade interest to None — never a fabricated 0"
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
        # Guard against funding-fixture regression: if the wallet has 0 USDC, this test
        # becomes vacuous (excessive_amount = 0 * 100 = 0, which doesn't exercise the
        # insufficient-balance path). Fail loudly so the regression is caught.
        assert usdc_balance > 0, (
            "Funded wallet has 0 USDC -- funding fixture regressed. "
            "Expected >=1 USDC to compute a meaningfully excessive amount."
        )
        balance_decimal = Decimal(usdc_balance) / Decimal(10**decimals)

        # Try to supply more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'=' * 80}")
        print("Test: SupplyIntent with Insufficient Balance")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        # Create SupplyIntent with excessive amount
        intent = SupplyIntent(
            protocol="spark",
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

        # Try to execute — should fail
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)

        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        # Verify balance unchanged (balance conservation)
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


@pytest.mark.ethereum
@pytest.mark.borrow
@pytest.mark.lending
class TestSparkBorrowIntent:
    """Test Spark borrow/repay operations using BorrowIntent and RepayIntent.

    These tests verify the full Intent flow:
    - BorrowIntent creation with collateral and borrow parameters
    - IntentCompiler generates correct Spark transactions
    - Transactions execute successfully on-chain
    - SparkReceiptParser correctly interprets results
    - Balance changes and account data match expected amounts
    - Layer 5: the real accounting pipeline persists a correct
      LendingAccountingEvent (degraded chain state per VIB-4963, FIFO split)
    """

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=False,
        reason="VIB-4590: USDC borrow reverts under execTransactionWithRole (selector 0xd27b44a9) at current ethereum fork pin — likely oracle/LTV/cap drift, not authz (as of 2026-05-18)",
    )
    async def test_borrow_usdc_with_weth_collateral_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test borrowing USDC with WETH collateral using BorrowIntent.

        Flow:
        1. Create BorrowIntent with WETH as collateral, borrowing USDC
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify USDC balance increased and debt was created
        5. Layer 5: assert persisted BORROW accounting event
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth = tokens["WETH"]
        usdc = tokens["USDC"]

        weth_decimals = get_token_decimals(web3, weth)
        usdc_decimals = get_token_decimals(web3, usdc)

        # Borrow amount derived from the live price oracle to target ~25% LTV
        # (5% headroom under the 30% cap from .claude/rules/intent-tests.md).
        collateral_amount = Decimal("1")
        borrow_amount = _safe_usdc_borrow_amount(price_oracle, collateral_amount)

        print(f"\n{'=' * 80}")
        print(f"Test: Borrow {borrow_amount} USDC with {collateral_amount} WETH collateral using BorrowIntent")
        print(f"{'=' * 80}")

        # Record balances BEFORE
        weth_before = get_token_balance(web3, weth, funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Debt before: {account_data_before['totalDebtBase']}")

        # Create BorrowIntent
        intent = BorrowIntent(
            protocol="spark",
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

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        # Execute via ExecutionOrchestrator
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts (Layer 3) — expect at least one Supply (collateral) and one Borrow event
        supply_event_seen = False
        borrow_event_seen = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = SparkReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success:
                    if parse_result.supplies:
                        assert len(parse_result.supplies) >= 1, "Expected at least one Supply event"
                        # Assert the supply event corresponds to WETH (collateral)
                        collateral_supply = next(
                            (s for s in parse_result.supplies if s.reserve.lower() == weth.lower()),
                            None,
                        )
                        assert collateral_supply is not None, (
                            f"Expected a Supply event for WETH collateral. Got reserves: "
                            f"{[s.reserve for s in parse_result.supplies]}"
                        )
                        assert collateral_supply.amount > 0, "WETH collateral supply amount must be > 0"
                        supply_event_seen = True
                        for supply_event in parse_result.supplies:
                            print(f"  Supply (collateral): {supply_event.amount}")
                            print(f"  Reserve: {supply_event.reserve}")

                    if parse_result.borrows:
                        assert len(parse_result.borrows) >= 1, "Expected at least one Borrow event"
                        # Assert the borrow event corresponds to USDC (debt)
                        usdc_borrow = next(
                            (b for b in parse_result.borrows if b.reserve.lower() == usdc.lower()),
                            None,
                        )
                        assert usdc_borrow is not None, (
                            f"Expected a Borrow event for USDC. Got reserves: "
                            f"{[b.reserve for b in parse_result.borrows]}"
                        )
                        assert usdc_borrow.amount > 0, "USDC borrow amount must be > 0"
                        assert usdc_borrow.interest_rate_mode == 2, "Spark adapter only supports variable rate (mode=2)"
                        borrow_event_seen = True
                        for borrow_event in parse_result.borrows:
                            print(f"  Borrow amount: {borrow_event.amount}")
                            print(f"  Reserve: {borrow_event.reserve}")
                            print(f"  Interest rate mode: {borrow_event.interest_rate_mode}")

        assert supply_event_seen, "Expected at least one Supply event for the WETH collateral leg"
        assert borrow_event_seen, "Expected at least one Borrow event for the USDC borrow leg"

        # Verify balance changes (Layer 4)
        weth_after = get_token_balance(web3, weth, funded_wallet)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_received = usdc_after - usdc_before

        print("\n--- Results ---")
        print(f"WETH spent (collateral): {format_token_amount(weth_spent, weth_decimals)}")
        print(f"USDC received (borrowed): {format_token_amount(usdc_received, usdc_decimals)}")

        # Verify WETH was spent EXACTLY equal to the collateral amount
        expected_weth_spent = int(collateral_amount * Decimal(10**weth_decimals))
        assert weth_spent == expected_weth_spent, (
            f"WETH spent must EXACTLY equal collateral amount. Expected: {expected_weth_spent}, Got: {weth_spent}"
        )

        # Verify USDC was received (allow up to 1% tolerance for protocol fees / rounding)
        expected_usdc_received = int(borrow_amount * Decimal(10**usdc_decimals))
        min_expected = int(expected_usdc_received * Decimal("0.99"))
        assert usdc_received >= min_expected, (
            f"USDC received must be at least 99% of borrow amount. Expected min: {min_expected}, Got: {usdc_received}"
        )
        assert usdc_received <= expected_usdc_received, (
            f"USDC received should not exceed borrow amount. "
            f"Expected max: {expected_usdc_received}, Got: {usdc_received}"
        )

        # Verify account data changed
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Collateral after: {account_data_after['totalCollateralBase']}")
        print(f"Debt after: {account_data_after['totalDebtBase']}")
        print(f"Health factor: {account_data_after['healthFactor']}")

        # WETH has positive LTV (8300) on Spark Ethereum, so supplying WETH as collateral
        # MUST strictly increase totalCollateralBase. >= would be too permissive.
        assert account_data_after["totalCollateralBase"] > account_data_before["totalCollateralBase"], (
            "WETH collateral supply must increase totalCollateralBase (WETH ltv=8300 on Spark Ethereum)"
        )
        assert account_data_after["totalDebtBase"] > account_data_before["totalDebtBase"], (
            "Debt must increase after borrow"
        )
        assert account_data_after["healthFactor"] > 1e18, "Health factor must be > 1.0"

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True)

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
        # BORROW increases debt on the Spark pool.
        assert Decimal(payload["debt_value_after_usd"]) > Decimal(payload["debt_value_before_usd"])
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == borrow_amount
        # BORROW records the FIFO principal lot: principal measured, interest
        # has no leg yet (a repay would match it) — must be None, not 0.
        assert payload["principal_delta_usd"] is not None, "BORROW must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "BORROW has no interest leg yet — must be None, not 0"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=False,
        reason="VIB-4590: setup borrow reverts under execTransactionWithRole (selector 0xd27b44a9) at current ethereum fork pin, blocking repay path (as of 2026-05-18)",
    )
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
        1. Setup: Borrow USDC with WETH collateral first (persisted through the
           Layer-5 harness so the FIFO basis pool holds the matching lot)
        2. Create RepayIntent to repay partial debt
        3. Compile and execute
        4. Verify USDC balance decreased and debt was reduced
        5. Layer 5: assert the EXACT principal/interest FIFO split
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        # Setup: open the borrow position. Reuse the same oracle-derived sizing
        # as the dedicated borrow test so the repay flow stays under the 30% LTV
        # cap as WETH price moves.
        setup_collateral = Decimal("1")
        setup_borrow = _safe_usdc_borrow_amount(price_oracle, setup_collateral)
        borrow_intent = BorrowIntent(
            protocol="spark",
            collateral_token="WETH",
            collateral_amount=setup_collateral,
            borrow_token="USDC",
            borrow_amount=setup_borrow,
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
        assert borrow_exec_result.success, f"Initial borrow failed: {borrow_exec_result.error}"

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

        print(f"\n{'=' * 80}")
        print(f"Test: Repay {repay_amount} USDC debt using RepayIntent")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before repay: {format_token_amount(usdc_before, usdc_decimals)}")

        account_data_before = get_user_account_data(web3, funded_wallet)
        print(f"Debt before: {account_data_before['totalDebtBase']}")
        print(f"Health factor before: {account_data_before['healthFactor']}")

        # Create RepayIntent
        intent = RepayIntent(
            protocol="spark",
            token="USDC",
            amount=repay_amount,
            chain=CHAIN_NAME,
        )

        print(f"\nCreated RepayIntent: token={intent.token}, amount={intent.amount}")

        # Compile and execute
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Parse receipts (Layer 3) — expect a Repay event from the funded wallet on the USDC reserve
        repay_event_seen = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = SparkReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.repays:
                    assert len(parse_result.repays) >= 1, "Expected at least one Repay event"
                    assert parse_result.repays[0].amount > 0, "Repay event amount must be > 0"
                    assert parse_result.repays[0].reserve.lower() == usdc.lower(), "Repay event reserve must be USDC"
                    assert parse_result.repays[0].repayer.lower() == funded_wallet.lower(), (
                        "Repayer must equal the funded wallet"
                    )
                    repay_event_seen = True

        assert repay_event_seen, "Expected at least one Repay event across all transactions"

        # Verify balance changes (Layer 4)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print(f"\nUSDC spent (repaid): {format_token_amount(usdc_spent, usdc_decimals)}")

        expected_usdc_spent = int(repay_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal repay amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify account data changed
        account_data_after = get_user_account_data(web3, funded_wallet)
        print(f"Debt after: {account_data_after['totalDebtBase']}")
        print(f"Health factor after: {account_data_after['healthFactor']}")

        assert account_data_after["totalDebtBase"] < account_data_before["totalDebtBase"], (
            "Debt must decrease after repay"
        )
        assert account_data_after["healthFactor"] > account_data_before["healthFactor"], "Health factor must improve"

        # ── Layer 5: borrow-then-repay FIFO split ────────────────────────────
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True)

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
        # REPAY reduces debt on the Spark pool.
        assert Decimal(payload["debt_value_after_usd"]) < Decimal(payload["debt_value_before_usd"])
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == repay_amount

        # Exact FIFO split: independent of the chain-state read (it derives from
        # the basis store, not from post_state_json). The REPAY matched the
        # prior BORROW lot in the same harness; repaying repay_amount of a
        # setup_borrow position within the same Anvil block accrues no interest,
        # so the entire repaid amount is matched principal and the interest leg
        # is a measured zero (NOT None — the match succeeded). principal +
        # interest must reconcile to the repaid cash flow.
        assert payload["principal_delta_usd"] is not None, "matched REPAY must measure principal"
        assert payload["interest_delta_usd"] is not None, (
            "matched REPAY (BORROW lot present in harness) must produce a measured interest leg — not None"
        )
        principal_usd = Decimal(payload["principal_delta_usd"])
        interest_usd = Decimal(payload["interest_delta_usd"])
        # Matched principal in USD = repaid fraction of the borrowed principal.
        # Both legs use the session price oracle, so this is exact (no MEV on
        # Anvil): repay_amount / setup_borrow of borrowed_principal_usd.
        repaid_usd = repay_amount * (borrowed_principal_usd / setup_borrow)
        assert principal_usd == repaid_usd, (
            f"FIFO principal_delta_usd must equal the matched principal ({repaid_usd}); got {principal_usd}"
        )
        assert interest_usd == Decimal("0"), (
            f"same-block partial repay accrues no interest — interest_delta_usd must be a measured 0, got {interest_usd}"
        )
        assert principal_usd + interest_usd == repaid_usd, "principal + interest must tie to repaid cash flow"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.REPAY)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=False,
        reason="VIB-4590: setup borrow reverts under execTransactionWithRole (selector 0xd27b44a9) at current ethereum fork pin, blocking the standalone repay setup (as of 2026-05-18)",
    )
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

        A REPAY whose matching BORROW lot is NOT in the Layer-5 FIFO basis pool
        (here: the on-chain borrow is executed but deliberately not persisted
        through the harness) must degrade ``interest_delta_usd`` to ``None`` —
        never a fabricated 0. ``match_repay`` consumes no lots, so
        ``repaid_principal == 0`` and ``principal_delta_usd`` is the *measured*
        attributable zero (a real Decimal('0'), not None, and not the full
        repaid cash flow: the REPAY handler does not fall back to total the way
        WITHDRAW does). Mirrors the Aave V3 / Compound V3 goldens exactly — the
        lending handler is protocol-agnostic.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        # On-chain borrow setup — intentionally NOT persisted through Layer 5,
        # so the FIFO basis pool has no matching BORROW lot. Oracle-sized to
        # ~25% LTV so it stays under the 30% cap as WETH price moves.
        setup_collateral = Decimal("1")
        setup_borrow = _safe_usdc_borrow_amount(price_oracle, setup_collateral)
        borrow_intent = BorrowIntent(
            protocol="spark",
            collateral_token="WETH",
            collateral_amount=setup_collateral,
            borrow_token="USDC",
            borrow_amount=setup_borrow,
            interest_rate_mode="variable",
            chain=CHAIN_NAME,
        )
        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle, execution_context)
        assert borrow_exec.success, f"Borrow setup failed: {borrow_exec.error}"

        repay_amount = Decimal("200")
        print(f"\n{'=' * 80}")
        print(f"Test: Standalone Repay {repay_amount} USDC — degradation contract")
        print(f"{'=' * 80}")

        intent = RepayIntent(
            protocol="spark",
            token="USDC",
            amount=repay_amount,
            chain=CHAIN_NAME,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: parser must observe the Repay event on the USDC reserve
        repay_event_seen = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = SparkReceiptParser()
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                if parse_result.success and parse_result.repays:
                    assert parse_result.repays[0].amount > 0, "Repay event amount must be > 0"
                    assert parse_result.repays[0].reserve.lower() == usdc.lower(), "Repay event reserve must be USDC"
                    repay_event_seen = True
        assert repay_event_seen, "Layer 3: parser must observe the Repay event on the USDC reserve"

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
        post_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True)

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
        # REPAY reduces debt on the Spark pool.
        assert Decimal(payload["debt_value_after_usd"]) < Decimal(payload["debt_value_before_usd"])
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == repay_amount
        # FIFO basis-store contract — independent of the chain-state read. No
        # matching BORROW lot in the harness. match_repay consumes nothing →
        # repaid_principal == 0 → principal_delta_usd is the *measured*
        # attributable zero (a real Decimal('0'), NOT None and NOT the full
        # repaid amount — the REPAY handler does not fall back to total).
        # interest_delta_usd degrades to None (never a fabricated 0).
        assert payload["principal_delta_usd"] is not None, (
            "unmatched REPAY must report a measured principal (Decimal('0'), not None)"
        )
        assert Decimal(payload["principal_delta_usd"]) == 0, (
            "unmatched REPAY attributes zero principal (FIFO pool empty) — a measured 0, not the full repaid cash flow"
        )
        assert payload["interest_delta_usd"] is None, (
            "standalone repay with no Layer-5 BORROW lot must degrade interest_delta_usd to None — never a fabricated 0"
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
        weth = tokens["WETH"]

        print(f"\n{'=' * 80}")
        print("Test: BorrowIntent without Collateral (should fail)")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)

        # Try to borrow without supplying collateral (collateral_amount = 0)
        intent = BorrowIntent(
            protocol="spark",
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
        weth_after = get_token_balance(web3, weth, funded_wallet)
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed borrow"
        assert weth_after == weth_before, "WETH balance must be unchanged after failed borrow (collateral_token)"

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
