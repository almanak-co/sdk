"""Production-grade supply/withdraw intent tests for Euler V2 on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for Euler V2 supply operations:
1. Create SupplyIntent / WithdrawIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using EulerV2ReceiptParser
5. Verify balance changes are correct

Euler V2 uses ERC-4626 vaults — deposit/withdraw are standard ERC-4626 operations.
Target vault: eUSDC-19 (0x37ca03aD51B8ff79aAD35FadaCBA4CEDF0C3e74e) — 3.1M USDC TVL, 86% utilization.

Layer 5 (epic VIB-4591 / ticket VIB-4605): mirrors the merged Spark / Aave V3 /
Compound V3 lending goldens and is kept identical to the Ethereum Euler V2 file
(intent-test rule #7: no per-chain variance for the same protocol). The
success-path SUPPLY/WITHDRAW tests persist the real ``ExecutionResult`` through
the production accounting pipeline and assert the typed
``LendingAccountingEvent``; the failure-path test asserts zero
``accounting_events`` rows.

THE EULER V2 DIVERGENCE (genuine production gap, tracked by VIB-4966):
Euler V2 has NO pre/post-state reader — ``_PROTOCOL_PRE_STATE_READERS`` in
``almanak/framework/accounting/lending_accounting.py`` has no ``euler_v2``
entry, so ``capture_lending_pre_state`` / ``capture_lending_post_state`` return
``None`` and the lending handler degrades to ``confidence=ESTIMATED`` with every
before/after collateral / debt / health-factor field ``None`` and a populated
``unavailable_reason`` (Empty≠Zero≠None — nothing fabricated). Layer 5 asserts
this DEGRADATION contract (``_assert_state_degraded_no_reader_vib4605``). The
FIFO principal / interest split is derived from the basis store and is
unaffected by the missing reader.

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/avalanche/test_euler_v2_supply.py -v -s
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.euler_v2.receipt_parser import EulerV2ReceiptParser
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

pytestmark = pytest.mark.no_zodiac(reason="euler_v2 connector not in manifest matrix")

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "avalanche"

# Euler V2 vault addresses for receipt filtering
EULER_V2_USDC_VAULT = "0x37ca03aD51B8ff79aAD35FadaCBA4CEDF0C3e74e"  # eUSDC-19

PROTOCOL = "euler_v2"


# =============================================================================
# Layer 5 helpers (shared) — kept identical to the Ethereum Euler V2 file
# (intent-test rule #7: no per-chain variance for the same protocol). Euler V2
# has NO entry in ``_PROTOCOL_PRE_STATE_READERS`` (VIB-4966), so both
# captures return ``None`` and the persisted event degrades to
# ``confidence=ESTIMATED`` with no before/after chain state.
# =============================================================================


def _execution_context(wallet: str) -> ExecutionContext:
    # deployment_id here labels the ExecutionContext for enrichment only; the
    # persisted row's deployment_id comes from the conftest helper's
    # ``deployment_id="layer5-intent-test"`` default (see _assert_identity).
    return ExecutionContext(
        deployment_id="layer5-euler-v2-lending",
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
    block: int | str | None = None,
) -> dict | None:
    """Capture and serialize Euler V2 pre/post state via the Anvil eth_call adapter.

    Returns the runner-shaped state dict or ``None`` — never a fabricated zero.
    For Euler V2 this currently ALWAYS returns ``None`` (no reader,
    VIB-4966); the call mirrors the runner's wiring so a future reader
    fix lights up the HIGH-confidence path with no test change.

    ``block`` (VIB-4589 / F7) pins the read: pre-state passes ``None`` (→
    ``"latest"``, safe because the read precedes submission); post-state passes
    the confirmed receipt's ``block_number`` so a future reader cannot race the
    upstream RPC's receipt indexer. Threaded now so the wiring is byte-for-byte
    the runner's the moment an Euler V2 reader lands.
    """
    capture = capture_lending_post_state if post else capture_lending_pre_state
    state = capture(
        intent=intent,
        chain=CHAIN_NAME,
        wallet_address=wallet,
        gateway_client=reader,
        price_oracle=price_oracle,
        block=block,
    )
    return lending_state_to_dict(state, protocol=PROTOCOL)


def _receipt_block(execution_result: ExecutionResult) -> int | None:
    """Block number of the last confirmed receipt (for post-state pinning)."""
    results = getattr(execution_result, "transaction_results", None) or []
    for tx_result in reversed(results):
        receipt = getattr(tx_result, "receipt", None)
        block_number = getattr(receipt, "block_number", None) if receipt else None
        if block_number is not None:
            return block_number
    return None


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


def _assert_state_degraded_no_reader_vib4605(payload: dict) -> None:
    """Euler V2 genuine production degradation contract (VIB-4966).

    Euler V2 is absent from ``_PROTOCOL_PRE_STATE_READERS`` in
    ``almanak/framework/accounting/lending_accounting.py`` so both captures
    return ``None``; the lending handler sets ``confidence=ESTIMATED`` and
    leaves every before/after field ``None`` with a populated
    ``unavailable_reason``. This is the TRUE current production behavior
    (deterministic across the Anvil-fork CI), NOT a flake. The HIGH-confidence
    expectation is the gap tracked by VIB-4966 (add a Euler V2
    pre/post-state reader: vault ``convertToAssets`` + EVC controller /
    collateral reads). Empty≠Zero≠None: ``unavailable_reason`` is set, nothing
    is fabricated.
    """
    assert payload["confidence"] == "ESTIMATED", (
        f"Euler V2 lending genuinely degrades to confidence=ESTIMATED today "
        f"(VIB-4966: no euler_v2 entry in _PROTOCOL_PRE_STATE_READERS); "
        f"got {payload['confidence']!r}"
    )
    assert payload.get("unavailable_reason"), (
        "degraded Euler V2 lending must carry a non-empty unavailable_reason (never fabricated)"
    )
    assert payload["collateral_value_before_usd"] is None, (
        "VIB-4966: degraded Euler V2 must not fabricate before-collateral"
    )
    assert payload["collateral_value_after_usd"] is None, (
        "VIB-4966: degraded Euler V2 must not fabricate after-collateral"
    )
    assert payload["debt_value_before_usd"] is None, "VIB-4966: degraded Euler V2 must not fabricate before-debt"
    assert payload["debt_value_after_usd"] is None, "VIB-4966: degraded Euler V2 must not fabricate after-debt"
    assert payload["health_factor_before"] is None, (
        "VIB-4966: degraded Euler V2 must not fabricate before-health-factor"
    )
    assert payload["health_factor_after"] is None, "VIB-4966: degraded Euler V2 must not fabricate after-health-factor"


# =============================================================================
# Supply/Withdraw Tests
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.supply
@pytest.mark.lending
class TestEulerV2SupplyIntent:
    """Test Euler V2 supply/withdraw operations using SupplyIntent and WithdrawIntent.

    These tests verify the full Intent flow:
    - SupplyIntent creation with token symbols and amounts
    - IntentCompiler generates correct Euler V2 transactions (ERC-4626 deposit)
    - Transactions execute successfully on-chain
    - EulerV2ReceiptParser correctly interprets Deposit/Withdraw events
    - Balance changes match expected amounts
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
        1. Create SupplyIntent for USDC on Euler V2
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
        print(f"Test: Supply {supply_amount} USDC to Euler V2 using SupplyIntent")
        print(f"{'=' * 80}")

        # Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before: {format_token_amount(usdc_before, decimals)}")

        # Create SupplyIntent
        intent = SupplyIntent(
            protocol="euler_v2",
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
                parser = EulerV2ReceiptParser(underlying_decimals=decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
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
        post_state = _capture_lending_state(
            intent,
            funded_wallet,
            anvil_eth_call_adapter,
            price_oracle,
            post=True,
            block=_receipt_block(execution_result),
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
        # VIB-4966: Euler V2 has no pre/post-state reader → confidence=ESTIMATED,
        # before/after chain state degraded to None (not fabricated).
        _assert_state_degraded_no_reader_vib4605(payload)
        assert payload["asset"] == "USDC"
        assert payload["amount_token"] is not None
        assert Decimal(payload["amount_token"]) == supply_amount
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
            protocol="euler_v2",
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
        print(f"Test: Withdraw {withdraw_amount} USDC from Euler V2 using WithdrawIntent")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before withdraw: {format_token_amount(usdc_before, decimals)}")

        # Create WithdrawIntent
        intent = WithdrawIntent(
            protocol="euler_v2",
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
                parser = EulerV2ReceiptParser(underlying_decimals=decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
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
        # The chain-state read is ALSO degraded (Euler V2 has no reader,
        # VIB-4966) — distinct from the unmatched-FIFO degradation.
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent,
            funded_wallet,
            anvil_eth_call_adapter,
            price_oracle,
            post=True,
            block=_receipt_block(execution_result),
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
        _assert_state_degraded_no_reader_vib4605(payload)
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
        print("Test: SupplyIntent with Insufficient Balance (Euler V2)")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SupplyIntent(
            protocol="euler_v2",
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
