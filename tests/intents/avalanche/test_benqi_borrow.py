"""Production-grade borrow/repay intent tests for BENQI on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for BENQI borrowing:
1. Create BorrowIntent / RepayIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using BenqiReceiptParser
5. Verify balance changes and debt accounting are correct
6. Layer 5 — persist the real ExecutionResult through the real accounting
   pipeline (ledger -> outbox -> AccountingProcessor.drain_one) into a
   throwaway SQLite and assert the typed LendingAccountingEvent is correct.

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

IMPORTANT: All borrow amounts target ~30% LTV to handle CoinGecko price fluctuations.

Layer 5 (epic VIB-4591 / ticket VIB-4607): mirrors the merged Spark and Aave V3
goldens. The lending category handler is protocol-agnostic — it keys on
``intent_type`` and the FIFO basis store, not on the protocol — so the FIFO
principal / interest split assertions are identical to the Aave V3 / Spark
goldens.

THE BENQI FIDELITY FIX (VIB-4967 landed — both prior gaps closed):
BENQI is a Compound-V2-style market (qiTokens). VIB-4967 shipped the two
production pieces that previously degraded BENQI lending accounting, so this
Layer-5 suite now asserts the SAME HIGH-confidence contract the Aave / Compound /
Silo / Euler goldens use:

  1. Pre/post-state reader — ``almanak/connectors/benqi/lending_read.py`` (enabled
     in ``_GENERIC_PRE_STATE_PROTOCOLS``). BENQI is pooled cross-asset, so it is read
     WHOLE-ACCOUNT: ``getAccountSnapshot`` on every listed qiToken (supply + debt) +
     the Comptroller's ``markets(qiToken).collateralFactorMantissa`` for a TRUE
     liquidation-aware HF (``Σ(supply_usd × CF) / Σ debt_usd`` — the on-chain
     liquidation parameter, NOT a bare collateral/debt proxy). Being whole-account it
     also resolves a bare REPAY (``token`` only) cleanly — no per-pair collateral
     ambiguity. ``capture_lending_*`` now serialize populated before/after collateral
     / debt / HF → ``confidence=HIGH``.
  2. Lending amount extractors on ``BenqiReceiptParser`` (``SUPPORTED_EXTRACTIONS`` +
     ``extract_borrow_amount`` / ``extract_repay_amount`` / ``extract_supply_amount``
     / ``extract_withdraw_amount`` returning RAW token wei, mirroring Spark). The
     enricher populates ``extracted_data`` and the handler measures ``amount_token``
     + records the FIFO principal lot → the borrow→repay split is EXACT (matched
     principal + measured interest), like the Spark/Aave goldens.

To run:
    uv run pytest tests/intents/avalanche/test_benqi_borrow.py -v -s
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.benqi.receipt_parser import BenqiReceiptParser
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
from almanak.framework.intents import BorrowIntent, RepayIntent
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

pytestmark = pytest.mark.no_zodiac(reason="benqi connector not in manifest matrix")

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "avalanche"
PROTOCOL = "benqi"

# BENQI qiToken addresses for receipt filtering
BENQI_QI_USDC = "0xB715808a78F6041E46d61Cb123C9B4A27056AE9C"
BENQI_QI_WAVAX = "0x5C0401e81Bc07Ca70fAD469b451682c0d747Ef1c"


# =============================================================================
# Layer 5 helpers (shared)
# =============================================================================
#
# Mirror the merged Spark / Aave V3 goldens. ``enrich_result`` makes the ledger
# entry carry extracted_data; ``capture_lending_pre_state`` /
# ``capture_lending_post_state`` dispatch on ``intent.protocol``. BENQI has NO
# entry in ``_PROTOCOL_PRE_STATE_READERS`` (VIB-4967), so both captures
# return ``None`` and ``lending_state_to_dict`` serializes ``None`` — the
# persisted event therefore degrades to ``confidence=ESTIMATED`` with no
# before/after chain state.


def _execution_context(wallet: str) -> ExecutionContext:
    """Build the ExecutionContext used to enrich a BENQI lending result for Layer 5."""
    # The deployment_id here labels enrichment only — the persisted accounting
    # row's deployment_id is stamped by the conftest helper default
    # ("layer5-intent-test"), which ``_assert_identity`` checks. Mirrors the
    # merged Spark / Aave V3 goldens.
    return ExecutionContext(
        deployment_id="layer5-benqi-lending",
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
    """Enrich the raw execution result for accounting (paper mode, no live writes)."""
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
    """Capture and serialize BENQI pre/post state via the Anvil eth_call adapter.

    Returns the runner-shaped state dict or ``None`` — never a fabricated zero.
    VIB-4967 landed the bespoke Compound-V2 qiToken whole-account reader
    (``almanak/connectors/benqi/lending_read.py``, enabled in
    ``_GENERIC_PRE_STATE_PROTOCOLS``), so this now returns populated whole-account
    before/after collateral / debt / HF via the Anvil eth_call adapter, lighting up
    the HIGH-confidence path.

    The read is left at ``block=None`` (→ ``"latest"``), matching the Silo/Euler
    Layer-5 wiring: the conftest ``AnvilEthCallAdapter.eth_call`` signature does not
    accept a ``block`` kwarg, and ``_gateway_eth_call`` intentionally refuses to fall
    back to the 3-arg form for a *pinned* block (VIB-4589). Pre-state precedes
    submission and post-state runs against the per-test Anvil snapshot, so ``latest``
    is correct here.
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
    """Deserialize an accounting_events row's ``payload_json`` to a dict."""
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
    """BENQI HIGH-confidence chain-state contract (VIB-4967 reader landed).

    BENQI now has a BESPOKE Compound-V2 qiToken WHOLE-ACCOUNT pre/post-state reader
    (``almanak/connectors/benqi/lending_read.py``, enabled in
    ``_GENERIC_PRE_STATE_PROTOCOLS``): every listed qiToken is read via
    ``getAccountSnapshot`` (supply + debt) + the Comptroller's
    ``markets(qiToken).collateralFactorMantissa`` for a TRUE liquidation-aware HF
    (``Σ(supply_usd × CF) / Σ debt_usd`` — the on-chain liquidation parameter, NOT a
    bare collateral/debt proxy), valued from the injected price/decimals seam. Being
    whole-account, it resolves a bare REPAY (``token`` only) cleanly — no per-pair
    collateral ambiguity. So ``capture_lending_*`` return populated before/after
    collateral / debt / HF through the Anvil eth_call adapter and the handler emits
    ``confidence=HIGH`` (Empty ≠ Zero — a measured zero is ``"0"``, never ``None``).
    This is the inverted contract VIB-4967 ships, replacing the prior
    ``_assert_state_degraded_no_reader`` degradation contract.
    """
    assert payload["confidence"] == "HIGH", (
        f"BENQI lending must persist confidence=HIGH (bespoke Compound-V2 reader + Anvil eth_call adapter), "
        f"got {payload['confidence']!r} (unavailable_reason={payload.get('unavailable_reason')!r})"
    )
    assert payload["collateral_value_before_usd"] is not None, "before-collateral must be populated"
    assert payload["collateral_value_after_usd"] is not None, "after-collateral must be populated"
    assert payload["debt_value_before_usd"] is not None, "before-debt must be populated"
    assert payload["debt_value_after_usd"] is not None, "after-debt must be populated"
    assert payload["health_factor_before"] is not None, "before-health-factor must be populated"
    assert payload["health_factor_after"] is not None, "after-health-factor must be populated"


# =============================================================================
# Borrow/Repay Tests
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.borrow
@pytest.mark.lending
class TestBenqiBorrowIntent:
    """Test BENQI borrow/repay operations using BorrowIntent and RepayIntent.

    These tests verify the full Intent flow:
    - BorrowIntent creation with collateral and borrow parameters
    - IntentCompiler generates correct BENQI transactions (supply collateral + enterMarkets + borrow)
    - Transactions execute successfully on-chain
    - BenqiReceiptParser correctly interprets Mint/Borrow/RepayBorrow events
    - Balance changes match expected amounts
    - Layer 5: the real accounting pipeline persists a correct
      LendingAccountingEvent (degraded chain state per VIB-4967, FIFO split)
    """

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_usdc_with_wavax_collateral_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test borrowing USDC with WAVAX collateral using BorrowIntent.

        Flow:
        1. Create BorrowIntent with WAVAX as collateral, borrowing USDC
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Parse receipt for Mint (supply) and Borrow events
        5. Verify WAVAX decreased (collateral) and USDC increased (borrowed)
        6. Layer 5: assert persisted BORROW accounting event

        LTV calculation: ~10 WAVAX at ~$20 = $200 collateral.
        Borrow $50 USDC = ~25% LTV (safe under 30% cap).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]

        usdc_decimals = get_token_decimals(web3, usdc)

        # Supply 10 AVAX as collateral (~$200), borrow 50 USDC (~25% LTV)
        collateral_amount = Decimal("10")
        borrow_amount = Decimal("50")

        print(f"\n{'=' * 80}")
        print(f"Test: Borrow {borrow_amount} USDC with {collateral_amount} AVAX collateral using BorrowIntent")
        print(f"{'=' * 80}")

        # Record balances BEFORE
        # BENQI qiAVAX uses native AVAX (not WAVAX ERC-20), so check native balance
        avax_before = web3.eth.get_balance(funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        print(f"Native AVAX before: {format_token_amount(avax_before, 18)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # Create BorrowIntent
        intent = BorrowIntent(
            protocol="benqi",
            collateral_token="AVAX",
            collateral_amount=collateral_amount,
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            chain=CHAIN_NAME,
        )

        print("\nCreated BorrowIntent:")
        print(f"  Collateral: {intent.collateral_amount} {intent.collateral_token}")
        print(f"  Borrow: {intent.borrow_amount} {intent.borrow_token}")

        # Compile intent
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
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
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts - track that we found expected events
        found_borrow_event = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                # Use appropriate decimals: 18 for AVAX collateral supply, usdc_decimals for borrow
                # The borrow tx (last) uses USDC decimals; supply tx uses AVAX (18) decimals
                tx_decimals = usdc_decimals if i == len(execution_result.transaction_results) - 1 else 18
                parser = BenqiReceiptParser(underlying_decimals=tx_decimals)
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success:
                    if parse_result.supply_amount > 0:
                        print(f"  Supply (collateral): {parse_result.supply_amount}")
                    if parse_result.borrow_amount > 0:
                        print(f"  Borrow amount: {parse_result.borrow_amount}")
                        assert parse_result.borrow_amount > 0, "Borrow amount must be positive"
                        found_borrow_event = True

        assert found_borrow_event, "Receipt parser must find at least one Borrow event"

        # Verify balance changes
        # BENQI qiAVAX uses native AVAX, so native balance decreases by collateral + gas
        avax_after = web3.eth.get_balance(funded_wallet)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)

        avax_spent = avax_before - avax_after
        usdc_received = usdc_after - usdc_before

        # Calculate total gas cost across all transactions
        total_gas_cost = sum(
            tx.gas_used * tx.receipt.effective_gas_price for tx in execution_result.transaction_results if tx.receipt
        )

        print("\n--- Results ---")
        print(f"Native AVAX spent (total): {format_token_amount(avax_spent, 18)}")
        print(f"Gas cost: {format_token_amount(total_gas_cost, 18)}")
        print(f"USDC received (borrowed): {format_token_amount(usdc_received, usdc_decimals)}")

        # Verify native AVAX spent = collateral + gas (not exact due to gas, check collateral component)
        expected_collateral_wei = int(collateral_amount * Decimal(10**18))
        collateral_spent = avax_spent - total_gas_cost
        assert collateral_spent == expected_collateral_wei, (
            f"Native AVAX collateral spent must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {collateral_spent} "
            f"(total spent: {avax_spent}, gas: {total_gas_cost})"
        )

        # Verify USDC was received (exact for BENQI)
        expected_usdc_received = int(borrow_amount * Decimal(10**usdc_decimals))
        assert usdc_received == expected_usdc_received, (
            f"USDC received must EXACTLY equal borrow amount. Expected: {expected_usdc_received}, Got: {usdc_received}"
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
        # VIB-4967: bespoke Compound-V2 whole-account reader → confidence=HIGH with
        # populated before/after chain state. Borrow increases on-chain debt.
        _assert_high_confidence_state(payload)
        assert Decimal(payload["debt_value_after_usd"]) > Decimal(payload["debt_value_before_usd"]), (
            "BORROW must increase on-chain debt value"
        )
        assert payload["asset"] == "USDC"
        # VIB-4967: extractors landed → amount_token + the FIFO principal lot are
        # MEASURED. BORROW records the principal lot (a later REPAY matches it); the
        # interest leg has no match yet (must be None, never a fabricated 0).
        assert Decimal(payload["amount_token"]) == borrow_amount
        assert payload["principal_delta_usd"] is not None, "BORROW must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "BORROW has no interest leg yet — must be None, not 0"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_usdc_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test repaying USDC debt using RepayIntent.

        Flow:
        1. Setup: Borrow USDC with WAVAX collateral first (persisted through the
           Layer-5 harness so the FIFO basis pool holds the matching lot)
        2. Create RepayIntent to repay partial debt
        3. Compile and execute
        4. Parse receipt for RepayBorrow event
        5. Verify USDC balance decreased by exact repay amount
        6. Layer 5: assert the EXACT principal/interest FIFO split
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        # First borrow USDC with WAVAX collateral
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        setup_borrow = Decimal("50")
        borrow_intent = BorrowIntent(
            protocol="benqi",
            collateral_token="AVAX",
            collateral_amount=Decimal("10"),
            borrow_token="USDC",
            borrow_amount=setup_borrow,
            chain=CHAIN_NAME,
        )

        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_pre_state = _capture_lending_state(
            borrow_intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle)
        assert borrow_exec.success, f"Initial borrow failed: {borrow_exec.error}"

        # Layer 5: persist the BORROW. In the Spark/Aave goldens this seeds the
        # FIFO basis pool with the matching lot so the REPAY split is exact. For
        # BENQI it does NOT — the receipt parser exposes no lending amount
        # extractors, so amount_human is None and record_borrow is never called
        # (VIB-4967). We still persist + assert the BORROW degradation
        # contract so the row's existence and identity are verified, and so this
        # block lights up the exact-split path automatically once the extractors
        # land.
        borrow_enriched = _enrich_for_accounting(
            borrow_exec, borrow_intent, funded_wallet, borrow_result.action_bundle.metadata
        )
        borrow_post_state = _capture_lending_state(
            borrow_intent,
            funded_wallet,
            anvil_eth_call_adapter,
            price_oracle,
            post=True,
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
        # VIB-4967: the setup BORROW now reaches HIGH confidence AND records the FIFO
        # principal lot (extractors landed) — which is what makes the REPAY split exact
        # below. Mirrors the Spark/Aave/Silo goldens.
        _assert_high_confidence_state(borrow_payload)
        assert Decimal(borrow_payload["amount_token"]) == setup_borrow
        assert borrow_payload["principal_delta_usd"] is not None, "setup BORROW must record the FIFO principal lot"
        assert Decimal(borrow_payload["principal_delta_usd"]) > 0
        assert borrow_payload["interest_delta_usd"] is None, "BORROW has no interest leg yet — must be None, not 0"

        # Now repay partial debt
        repay_amount = Decimal("25")

        print(f"\n{'=' * 80}")
        print(f"Test: Repay {repay_amount} USDC debt using RepayIntent")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before repay: {format_token_amount(usdc_before, usdc_decimals)}")

        # Create RepayIntent
        intent = RepayIntent(
            protocol="benqi",
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

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Parse receipts
        found_repay_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = BenqiReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    qi_token_address=BENQI_QI_USDC,
                )

                if parse_result.success and parse_result.repay_amount > 0:
                    print(f"  Repay amount: {parse_result.repay_amount}")
                    assert parse_result.repay_amount > 0, "Repay amount must be positive"
                    found_repay_event = True

        assert found_repay_event, "Receipt parser must find at least one RepayBorrow event"

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print(f"\nUSDC spent (repaid): {format_token_amount(usdc_spent, usdc_decimals)}")

        expected_usdc_spent = int(repay_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal repay amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # ── Layer 5: borrow-then-repay (degraded — no exact FIFO split) ──────
        # In the Spark/Aave goldens the prior BORROW lot makes this REPAY split
        # exact (matched principal + measured-zero interest). For BENQI the
        # BORROW recorded NO lot (amount_human=None, VIB-4967), so the
        # REPAY likewise cannot derive amount_token or match a lot: amount_token
        # + principal + interest all degrade to None. The typed REPAY event is
        # still persisted with correct identity + event_type.
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent,
            funded_wallet,
            anvil_eth_call_adapter,
            price_oracle,
            post=True,
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
        # VIB-4967: whole-account reader resolves a bare REPAY (token only) cleanly →
        # confidence=HIGH. Repay decreases on-chain debt (borrowed 50, repaid 25).
        _assert_high_confidence_state(payload)
        assert Decimal(payload["debt_value_after_usd"]) < Decimal(payload["debt_value_before_usd"]), (
            "REPAY must decrease on-chain debt value"
        )
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == repay_amount
        # Exact FIFO split: independent of the chain-state read (it derives from the
        # basis store). The REPAY matched the prior BORROW lot in the same harness;
        # repaying within the same Anvil block accrues ~no interest, so the repaid
        # amount is matched principal and the interest leg is a MEASURED value (NOT
        # None — the match succeeded). Empty ≠ Zero ≠ None.
        assert payload["principal_delta_usd"] is not None, "matched REPAY must measure principal"
        assert payload["interest_delta_usd"] is not None, (
            "matched REPAY (BORROW lot present in harness) must produce a measured interest leg — not None"
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_only_usdc_against_existing_collateral(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test borrow-only path: BorrowIntent with collateral_amount=0.

        This exercises the compiler codepath where BORROW is generated WITHOUT
        a preceding SUPPLY action in the same ActionBundle. The wallet must
        already have collateral supplied and entered as collateral.

        Flow:
        1. Setup: Supply AVAX + enterMarkets via BorrowIntent with collateral
        2. Record balances
        3. Create BorrowIntent with collateral_amount=0
        4. Compile (verify only BORROW tx, no SUPPLY/enterMarkets)
        5. Execute
        6. Parse receipt for Borrow event
        7. Verify USDC increased, AVAX unchanged (no collateral spent)
        8. Layer 5: assert persisted BORROW accounting event

        VIB-1381: This is the untested borrow-only codepath found in iter 88.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        # ── Step 1: Setup — supply collateral via a regular BorrowIntent ──
        # This supplies AVAX, enters markets, and borrows a small amount.
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        setup_intent = BorrowIntent(
            protocol="benqi",
            collateral_token="AVAX",
            collateral_amount=Decimal("10"),  # ~$200 collateral
            borrow_token="USDC",
            borrow_amount=Decimal("10"),  # Small initial borrow
            chain=CHAIN_NAME,
        )

        setup_result = compiler.compile(setup_intent)
        assert setup_result.status.value == "SUCCESS", f"Setup compilation failed: {setup_result.error}"
        assert setup_result.action_bundle is not None
        setup_exec = await orchestrator.execute(setup_result.action_bundle)
        assert setup_exec.success, f"Setup execution failed: {setup_exec.error}"
        print("Setup complete: 10 AVAX supplied, markets entered, 10 USDC borrowed")

        # ── Step 2: Record balances BEFORE borrow-only ──
        avax_before = web3.eth.get_balance(funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        borrow_amount = Decimal("20")  # Borrow more against existing collateral

        print(f"\n{'=' * 80}")
        print(f"Test: Borrow-only {borrow_amount} USDC (collateral_amount=0) using BorrowIntent")
        print(f"{'=' * 80}")
        print(f"Native AVAX before: {format_token_amount(avax_before, 18)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # ── Step 3: Create BorrowIntent with collateral_amount=0 ──
        intent = BorrowIntent(
            protocol="benqi",
            collateral_token="AVAX",
            collateral_amount=Decimal("0"),  # No new collateral — borrow-only
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            chain=CHAIN_NAME,
        )

        print("\nCreated BorrowIntent (borrow-only):")
        print(f"  Collateral: {intent.collateral_amount} {intent.collateral_token} (none — existing)")
        print(f"  Borrow: {intent.borrow_amount} {intent.borrow_token}")

        # ── Layer 1: Compilation ──
        print("\nCompiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        # Verify the borrow-only path: should have only 1 tx (borrow), no supply or enterMarkets
        bundle = compilation_result.action_bundle
        num_txs = len(bundle.transactions)
        print(f"ActionBundle created with {num_txs} transaction(s)")
        assert num_txs == 1, (
            f"Borrow-only path (collateral_amount=0) should produce exactly 1 transaction "
            f"(just the borrow), but got {num_txs}. The compiler may be generating "
            f"unnecessary supply/enterMarkets transactions."
        )

        # Verify the warning about existing collateral
        if compilation_result.warnings:
            print(f"Warnings: {compilation_result.warnings}")
            assert any("existing collateral" in w.lower() for w in compilation_result.warnings), (
                "Compiler should warn about borrowing against existing collateral"
            )

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        # ── Layer 2: Execution ──
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transaction(s) confirmed")

        # ── Layer 3: Receipt Parsing ──
        found_borrow_event = False
        for tx_result in execution_result.transaction_results:
            print("\nTransaction:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = BenqiReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.borrow_amount > 0:
                    print(f"  Borrow amount: {parse_result.borrow_amount}")
                    assert parse_result.borrow_amount > 0, "Borrow amount must be positive"
                    found_borrow_event = True

                # Verify NO supply event (borrow-only path must not supply)
                assert parse_result.supply_amount == 0, (
                    f"Borrow-only path must NOT produce supply events, "
                    f"but found supply_amount={parse_result.supply_amount}"
                )

        assert found_borrow_event, "Receipt parser must find at least one Borrow event"

        # ── Layer 4: Balance Deltas ──
        avax_after = web3.eth.get_balance(funded_wallet)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)

        # Calculate gas cost
        total_gas_cost = sum(
            tx.gas_used * tx.receipt.effective_gas_price for tx in execution_result.transaction_results if tx.receipt
        )

        usdc_received = usdc_after - usdc_before
        avax_spent_excluding_gas = (avax_before - avax_after) - total_gas_cost

        print("\n--- Results ---")
        print(f"USDC received (borrowed): {format_token_amount(usdc_received, usdc_decimals)}")
        print(f"Native AVAX spent (excluding gas): {format_token_amount(avax_spent_excluding_gas, 18)}")
        print(f"Gas cost: {format_token_amount(total_gas_cost, 18)}")

        # Verify USDC received matches borrow amount exactly
        expected_usdc = int(borrow_amount * Decimal(10**usdc_decimals))
        assert usdc_received == expected_usdc, (
            f"USDC received must EXACTLY equal borrow amount. Expected: {expected_usdc}, Got: {usdc_received}"
        )

        # Verify NO native AVAX was spent as collateral (only gas)
        assert avax_spent_excluding_gas == 0, (
            f"Borrow-only path must NOT spend native AVAX as collateral. "
            f"AVAX spent (excluding gas): {avax_spent_excluding_gas}"
        )

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        # VIB-4967: the BENQI receipt parser exposes no lending amount
        # extractors, so this borrow-only BORROW degrades identically to the
        # main borrow test — amount_token + principal/interest legs all None
        # (no lot recorded). The typed BORROW event still persists with correct
        # identity + event_type.
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent,
            funded_wallet,
            anvil_eth_call_adapter,
            price_oracle,
            post=True,
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
        # VIB-4967: bespoke whole-account reader → confidence=HIGH. The borrow-only
        # BORROW increases on-chain debt against the existing AVAX collateral, and the
        # extractors measure amount_token + the FIFO principal lot (mirrors the main
        # borrow test).
        _assert_high_confidence_state(payload)
        assert Decimal(payload["debt_value_after_usd"]) > Decimal(payload["debt_value_before_usd"]), (
            "borrow-only BORROW must increase on-chain debt value"
        )
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == borrow_amount
        assert payload["principal_delta_usd"] is not None, "BORROW must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "BORROW has no interest leg yet — must be None, not 0"

        print("\nALL CHECKS PASSED — borrow-only path verified")

    # The compile-time borrow-capacity pre-flight added in PR #2129 prevents
    # this scenario in production (gateway-connected) — see
    # tests/unit/intents/test_compiler_borrow_pre_flight_capacity.py for
    # the unit-test coverage. The pre-flight is gated on
    # ``compiler._gateway_client.is_connected`` and the intent-test harness
    # instantiates IntentCompiler without a gateway client, so the pre-flight
    # does not fire here. The on-chain Comptroller's enforcement is also
    # unreliable on Anvil forks (oracle drift), which is the original xfail
    # reason. End-to-end mainnet validation is tracked at GitHub issue #2128.
    @pytest.mark.intent(IntentType.BORROW)
    # xfail-grandfathered: #1694 (pre-dates xfail-hygiene rule)
    @pytest.mark.xfail(
        reason="BENQI Comptroller on Anvil fork does not reliably enforce borrow limits. "
        "The oracle price feed on the fork may return values that allow excessive borrows. "
        "Compile-time pre-flight (PR #2129) covers this in production via gateway eth_call; "
        "intent tests run without a gateway client so the pre-flight is bypassed here.",
        strict=False,
    )
    @pytest.mark.asyncio
    async def test_borrow_excessive_amount_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test that borrowing far more than collateral supports fails gracefully.

        Supply minimal collateral (1 AVAX ~ $20) but try to borrow $100,000 USDC.
        This exceeds any collateral factor and must revert on-chain.

        Layer 5 failure contract: a failed execution must write ZERO
        accounting_events rows.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]

        print(f"\n{'=' * 80}")
        print("Test: BorrowIntent with Excessive Amount (BENQI - should fail)")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        # Supply tiny collateral but borrow massive amount
        intent = BorrowIntent(
            protocol="benqi",
            collateral_token="AVAX",
            collateral_amount=Decimal("1"),  # ~$20 collateral
            borrow_token="USDC",
            borrow_amount=Decimal("100000"),  # $100k borrow >> collateral
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

        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert not execution_result.success, "Execution should fail with excessive borrow amount"
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
