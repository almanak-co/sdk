"""Production-grade supply/withdraw intent tests for Silo V2 on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for Silo V2 supply operations:
1. Create SupplyIntent / WithdrawIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using SiloV2ReceiptParser
5. Verify balance changes are correct
6. Layer 5 — persist the real ExecutionResult through the real accounting
   pipeline (ledger -> outbox -> AccountingProcessor.drain_one) into a
   throwaway SQLite and assert the typed LendingAccountingEvent is correct.

Layer 5 (epic VIB-4591 / ticket VIB-4606): mirrors the merged Spark lending
gold (``tests/intents/ethereum/test_spark_lending.py``). The lending category
handler is protocol-agnostic — it keys on ``intent_type`` and the FIFO basis
store, not on the protocol — so the FIFO principal / interest split assertions
are identical to the Aave V3 / Spark goldens.

THE SILO V2 DIVERGENCE (genuine production gap, tracked by VIB-4965):
Silo V2 has NO pre/post-state reader. ``_PROTOCOL_PRE_STATE_READERS`` in
``almanak/framework/accounting/lending_accounting.py`` has entries for
``aave_v3`` / ``aave`` / ``morpho_blue`` / ``compound_v3`` but NOT ``silo_v2``,
so ``capture_lending_pre_state`` / ``capture_lending_post_state`` return
``None`` for a Silo V2 intent and ``lending_state_to_dict`` serializes ``None``.
With no ``post_state_json`` the lending handler sets ``confidence=ESTIMATED``
and leaves every before/after collateral / debt / health-factor field ``None``
with a populated ``unavailable_reason`` (Empty≠Zero≠None — nothing is
fabricated). So Silo V2 Layer 5 asserts the DEGRADATION contract for chain
state (``_assert_state_degraded_no_reader``), NOT the HIGH-confidence contract
the Aave / Compound goldens use. The FIFO principal / interest split is derived
from the basis store and is unaffected by the missing reader, so those
assertions match the goldens exactly. The HIGH-confidence + before/after
fidelity is the gap tracked by VIB-4965 (add a Silo V2 pre/post-state
reader — Silo V2's isolated ERC-4626 silos expose ``maxWithdraw`` /
``getDebtAssets`` / ``collateralBalanceOfUnderlying`` per silo, distinct from
Aave's single ``getUserAccountData``, so it needs a bespoke reader).

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/avalanche/test_silo_v2_supply.py -v -s
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.silo_v2.receipt_parser import SiloV2ReceiptParser
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
from almanak.framework.intents import SupplyIntent, WithdrawIntent
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

pytestmark = pytest.mark.no_zodiac(reason="silo_v2 connector not in manifest matrix")

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "avalanche"
PROTOCOL = "silo_v2"

# Silo V2 WAVAX/USDC market silo addresses
SILO_V2_USDC_SILO = "0xfA5f7d5BcD70dC2F031eE906fc692a9e19584CB0"  # USDC vault (silo1)


# =============================================================================
# Layer 5 helpers (shared)
# =============================================================================
#
# Mirror the merged Spark / Aave V3 goldens. ``enrich_result`` makes the ledger
# entry carry extracted_data; ``capture_lending_pre_state`` /
# ``capture_lending_post_state`` dispatch on ``intent.protocol``. Silo V2 has
# NO entry in ``_PROTOCOL_PRE_STATE_READERS`` (VIB-4965), so both
# captures return ``None`` and ``lending_state_to_dict`` serializes ``None`` —
# the persisted event therefore degrades to ``confidence=ESTIMATED`` with no
# before/after chain state. The conftest Layer-5 helper threads the serialized
# state dicts (here ``None``) into ``build_ledger_entry``.


def _execution_context(wallet: str) -> ExecutionContext:
    # NOTE: this deployment_id flows only into ``enrich_result`` (it labels the
    # ExecutionContext for enrichment). It is deliberately NOT what lands in
    # the persisted accounting row: the conftest ``assert_accounting_persisted``
    # helper stamps the row's deployment_id from its own ``deployment_id=
    # "layer5-intent-test"`` default, which is what ``_assert_identity``
    # checks. This split (descriptive enrichment id vs canonical persisted
    # identity) mirrors the merged Spark / Aave V3 goldens.
    return ExecutionContext(
        deployment_id="layer5-silo-v2-lending",
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
    """Capture and serialize Silo V2 pre/post state via the Anvil eth_call adapter.

    Returns the runner-shaped state dict (``lending_state_to_dict`` output) or
    ``None`` — never a fabricated zero. For Silo V2 this currently ALWAYS
    returns ``None`` because Silo V2 has no pre/post-state reader
    (VIB-4965); the call is kept to mirror the runner's wiring exactly so
    a future reader fix lights up the HIGH-confidence path with no test change.
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


def _assert_state_degraded_no_reader(payload: dict) -> None:
    """Silo V2 genuine production degradation contract (VIB-4965).

    Silo V2 is absent from ``_PROTOCOL_PRE_STATE_READERS`` in
    ``almanak/framework/accounting/lending_accounting.py`` (only ``aave_v3`` /
    ``aave`` / ``morpho_blue`` / ``compound_v3`` are wired), so
    ``capture_lending_pre_state`` / ``capture_lending_post_state`` return
    ``None`` for a Silo V2 intent. With no ``post_state_json`` the lending
    handler sets ``confidence=ESTIMATED`` and leaves every before/after
    collateral / debt / health-factor field ``None`` with a populated
    ``unavailable_reason``. This is the TRUE current production behavior
    (deterministic across the avalanche Anvil-fork CI), NOT a flake. We assert
    the genuine degradation contract here rather than HIGH; the HIGH-confidence
    expectation (and before/after collateral / debt / HF fidelity) is the gap
    tracked by VIB-4965 — wire Silo V2 into the reader registry. Unlike
    Spark (an Aave V3 fork that can reuse ``read_aave_account_state``), Silo
    V2's isolated ERC-4626 silos have no ``getUserAccountData`` ABI, so the
    reader is bespoke per-silo. Empty≠Zero≠None: ``unavailable_reason`` is set,
    nothing is fabricated.
    """
    assert payload["confidence"] == "ESTIMATED", (
        f"Silo V2 lending genuinely degrades to confidence=ESTIMATED today "
        f"(VIB-4965: no silo_v2 entry in _PROTOCOL_PRE_STATE_READERS); "
        f"got {payload['confidence']!r}"
    )
    assert payload.get("unavailable_reason"), (
        "degraded Silo V2 lending must carry a non-empty unavailable_reason (never fabricated)"
    )
    # Degradation must not fabricate before/after chain state.
    assert payload["collateral_value_before_usd"] is None, (
        "VIB-4965: degraded Silo V2 must not fabricate before-collateral"
    )
    assert payload["collateral_value_after_usd"] is None, (
        "VIB-4965: degraded Silo V2 must not fabricate after-collateral"
    )
    assert payload["debt_value_before_usd"] is None, "VIB-4965: degraded Silo V2 must not fabricate before-debt"
    assert payload["debt_value_after_usd"] is None, "VIB-4965: degraded Silo V2 must not fabricate after-debt"
    assert payload["health_factor_before"] is None, (
        "VIB-4965: degraded Silo V2 must not fabricate before-health-factor"
    )
    assert payload["health_factor_after"] is None, (
        "VIB-4965: degraded Silo V2 must not fabricate after-health-factor"
    )


# =============================================================================
# Supply/Withdraw Tests
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.supply
@pytest.mark.lending
class TestSiloV2SupplyIntent:
    """Test Silo V2 supply/withdraw operations using SupplyIntent and WithdrawIntent.

    These tests verify the full Intent flow:
    - SupplyIntent creation with token symbols and amounts
    - IntentCompiler generates correct Silo V2 transactions (ERC-4626 deposit)
    - Transactions execute successfully on-chain
    - SiloV2ReceiptParser correctly interprets Deposit/Withdraw events
    - Balance changes match expected amounts
    - Layer 5: the real accounting pipeline persists a correct
      LendingAccountingEvent (degraded chain state per VIB-4965, identity
      sextuple, FIFO principal split)
    """

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_usdc_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test USDC supply using SupplyIntent.

        Flow:
        1. Create SupplyIntent for USDC on Silo V2
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Parse receipt for Deposit event
        5. Verify USDC balance decreased by exact supply amount
        6. Layer 5: assert persisted SUPPLY accounting event
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("1000")  # 1000 USDC

        print(f"\n{'=' * 80}")
        print(f"Test: Supply {supply_amount} USDC to Silo V2 using SupplyIntent")
        print(f"{'=' * 80}")

        # Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before: {format_token_amount(usdc_before, decimals)}")

        # Create SupplyIntent
        intent = SupplyIntent(
            protocol="silo_v2",
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
            rpc_url=anvil_rpc_url,
        )

        print("Compiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        # Execute via ExecutionOrchestrator
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts - track that we found expected Deposit event
        found_supply_event = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = SiloV2ReceiptParser(underlying_decimals=decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    silo_address=SILO_V2_USDC_SILO,
                )

                if parse_result.success and parse_result.deposit_amount > 0:
                    print(f"  Deposit amount:  {parse_result.deposit_amount}")
                    print(f"  Shares minted: {parse_result.deposit_shares}")
                    assert parse_result.deposit_amount > 0, "Deposit amount must be positive"
                    assert parse_result.deposit_shares > 0, "Shares minted must be positive"
                    found_supply_event = True

        assert found_supply_event, "Receipt parser must find at least one Deposit (supply) event"

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print("\n--- Results ---")
        print(f"USDC spent: {format_token_amount(usdc_spent, decimals)}")

        # Verify USDC was spent
        expected_usdc_spent = int(supply_amount * Decimal(10**decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal supply amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
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
        # VIB-4965: Silo V2 has no pre/post-state reader → confidence=ESTIMATED,
        # before/after chain state degraded to None (not fabricated).
        _assert_state_degraded_no_reader(payload)
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
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test USDC withdraw using WithdrawIntent (after supplying).

        Flow:
        1. Supply USDC first
        2. Create WithdrawIntent to withdraw portion
        3. Compile and execute
        4. Verify USDC balance increased by exact withdraw amount
        5. Layer 5: assert persisted WITHDRAW accounting event
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # First supply 2000 USDC
        supply_amount = Decimal("2000")
        supply_intent = SupplyIntent(
            protocol="silo_v2",
            token="USDC",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )

        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_result.action_bundle)
        assert supply_exec.success, f"Initial supply failed: {supply_exec.error}"

        # Now withdraw 1000 USDC
        withdraw_amount = Decimal("1000")

        print(f"\n{'=' * 80}")
        print(f"Test: Withdraw {withdraw_amount} USDC from Silo V2 using WithdrawIntent")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before withdraw: {format_token_amount(usdc_before, decimals)}")

        # Create WithdrawIntent
        intent = WithdrawIntent(
            protocol="silo_v2",
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

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Parse receipts - track that we found expected Withdraw event
        found_withdraw_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = SiloV2ReceiptParser(underlying_decimals=decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    silo_address=SILO_V2_USDC_SILO,
                )

                if parse_result.success and parse_result.withdraw_amount > 0:
                    print(f"  Withdraw amount: {parse_result.withdraw_amount}")
                    print(f"  Shares redeemed: {parse_result.withdraw_shares}")
                    assert parse_result.withdraw_amount > 0, "Withdraw amount must be positive"
                    assert parse_result.withdraw_shares > 0, "Shares redeemed must be positive"
                    found_withdraw_event = True

        assert found_withdraw_event, "Receipt parser must find at least one Withdraw event"

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before

        print(f"\nUSDC received: {format_token_amount(usdc_received, decimals)}")

        expected_usdc_received = int(withdraw_amount * Decimal(10**decimals))
        assert usdc_received == expected_usdc_received, (
            f"USDC received must EXACTLY equal withdraw amount. "
            f"Expected: {expected_usdc_received}, Got: {usdc_received}"
        )

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        # The SUPPLY above was NOT persisted through the Layer-5 harness, so the
        # FIFO supply pool is empty: WITHDRAW degrades — principal falls back to
        # the total and interest_delta_usd stays None (never a fabricated 0).
        # This is the degradation contract for an unmatched withdraw (epic
        # decision #6, mirrors standalone repay), identical to the Spark golden.
        # The chain-state read is ALSO degraded (Silo V2 has no reader,
        # VIB-4965) — distinct from the unmatched-FIFO degradation.
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
        _assert_state_degraded_no_reader(payload)
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
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
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

        # Get current balance and guard against zero
        usdc_balance = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_balance > 0, "Funded wallet must have positive USDC balance for this test"
        balance_decimal = Decimal(usdc_balance) / Decimal(10**decimals)

        # Try to supply more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'=' * 80}")
        print("Test: SupplyIntent with Insufficient Balance (Silo V2)")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SupplyIntent(
            protocol="silo_v2",
            token="USDC",
            amount=excessive_amount,
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

        # Try to execute - should fail
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
