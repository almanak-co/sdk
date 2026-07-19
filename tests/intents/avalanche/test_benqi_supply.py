"""Production-grade supply/withdraw intent tests for BENQI on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for BENQI supply operations:
1. Create SupplyIntent / WithdrawIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using BenqiReceiptParser
5. Verify balance changes are correct
6. Layer 5 — persist the real ExecutionResult through the real accounting
   pipeline (ledger -> outbox -> AccountingProcessor.drain_one) into a
   throwaway SQLite and assert the typed LendingAccountingEvent is correct.

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
     in ``_GENERIC_PRE_STATE_PROTOCOLS``). BENQI is pooled cross-asset, so it is
     read WHOLE-ACCOUNT: ``getAccountSnapshot`` on every listed qiToken (supply +
     debt) + the Comptroller's ``markets(qiToken).collateralFactorMantissa`` for a
     TRUE liquidation-aware HF (``Σ(supply_usd × CF) / Σ debt_usd`` — the on-chain
     liquidation parameter, NOT a bare collateral/debt proxy). ``capture_lending_*``
     now serialize populated before/after collateral / debt / HF → ``confidence=HIGH``.
  2. Lending amount extractors on ``BenqiReceiptParser``
     (``SUPPORTED_EXTRACTIONS`` + ``extract_supply_amount`` /
     ``extract_withdraw_amount`` / ``extract_borrow_amount`` / ``extract_repay_amount``
     returning RAW token wei, mirroring Spark). ``ResultEnricher`` now populates
     ``extracted_data`` and the handler's ``_extract_amount_human`` returns the
     measured amount → ``amount_token`` + the FIFO principal/interest split are
     measured. Empty ≠ Zero ≠ None still holds: an unmatched WITHDRAW (no Layer-5
     SUPPLY lot) degrades the interest leg to ``None``, never a fabricated 0.

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/avalanche/test_benqi_supply.py -v -s
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

pytestmark = pytest.mark.no_zodiac(reason="benqi connector not in manifest matrix")

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "avalanche"
PROTOCOL = "benqi"

# BENQI qiToken addresses for receipt filtering
BENQI_QI_USDC = "0xB715808a78F6041E46d61Cb123C9B4A27056AE9C"


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
# before/after chain state. The conftest Layer-5 helper threads the serialized
# state dicts (here ``None``) into ``build_ledger_entry``.


def _execution_context(wallet: str) -> ExecutionContext:
    """Build the ExecutionContext used to enrich a BENQI lending result for Layer 5."""
    # NOTE: this deployment_id flows only into ``enrich_result`` (it labels the
    # ExecutionContext for enrichment). It is deliberately NOT what lands in
    # the persisted accounting row: the conftest ``assert_accounting_persisted``
    # helper stamps the row's deployment_id from its own ``deployment_id=
    # "layer5-intent-test"`` default, which is what ``_assert_identity``
    # checks. This split (descriptive enrichment id vs canonical persisted
    # identity) mirrors the merged Spark / Aave V3 goldens.
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

    Returns the runner-shaped state dict (``lending_state_to_dict`` output) or
    ``None`` — never a fabricated zero. VIB-4967 landed the bespoke Compound-V2
    qiToken pre/post-state reader (``almanak/connectors/benqi/lending_read.py``,
    enabled in ``_GENERIC_PRE_STATE_PROTOCOLS``), so this now returns populated
    whole-account before/after collateral / debt / HF via the Anvil eth_call
    adapter, lighting up the HIGH-confidence path.

    The read is left at ``block=None`` (→ ``"latest"``), matching the Silo/Euler
    Layer-5 wiring: the conftest ``AnvilEthCallAdapter.eth_call`` signature does not
    accept a ``block`` kwarg, and ``_gateway_eth_call`` intentionally refuses to
    fall back to the 3-arg form for a *pinned* block (VIB-4589). Pre-state precedes
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

    BENQI now has a BESPOKE Compound-V2 qiToken pre/post-state reader
    (``almanak/connectors/benqi/lending_read.py``, enabled in
    ``_GENERIC_PRE_STATE_PROTOCOLS``): unlike the Aave family's single
    ``getUserAccountData``, BENQI is read WHOLE-ACCOUNT via ``getAccountSnapshot``
    on every listed qiToken (supply + debt) + the Comptroller's
    ``markets(qiToken).collateralFactorMantissa`` for a TRUE liquidation-aware HF
    (``Σ(supply_usd × CF) / Σ debt_usd`` — the on-chain liquidation parameter, NOT a
    bare collateral/debt proxy), valued from the injected price/decimals seam (BENQI
    is not USD-native). So ``capture_lending_pre_state`` /
    ``capture_lending_post_state`` return populated whole-account state through the
    Anvil eth_call adapter and the lending handler emits ``confidence=HIGH`` with
    every before/after collateral / debt / health-factor field populated (Empty ≠
    Zero — a measured zero is ``"0"``, never ``None``). This is the inverted contract
    VIB-4967 ships, replacing the prior ``_assert_state_degraded_no_reader``.
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
# Supply/Withdraw Tests
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.supply
@pytest.mark.lending
class TestBenqiSupplyIntent:
    """Test BENQI supply/withdraw operations using SupplyIntent and WithdrawIntent.

    These tests verify the full Intent flow:
    - SupplyIntent creation with token symbols and amounts
    - IntentCompiler generates correct BENQI transactions (Compound V2 mint)
    - Transactions execute successfully on-chain
    - BenqiReceiptParser correctly interprets Mint/Redeem events
    - Balance changes match expected amounts
    - Layer 5: the real accounting pipeline persists a correct
      LendingAccountingEvent (degraded chain state per VIB-4967, identity
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
        1. Create SupplyIntent for USDC on BENQI
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Parse receipt for Mint event
        5. Verify USDC balance decreased by exact supply amount
        6. Layer 5: assert persisted SUPPLY accounting event
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("1000")  # 1000 USDC

        print(f"\n{'=' * 80}")
        print(f"Test: Supply {supply_amount} USDC to BENQI using SupplyIntent")
        print(f"{'=' * 80}")

        # Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before: {format_token_amount(usdc_before, decimals)}")

        # Create SupplyIntent
        intent = SupplyIntent(
            protocol="benqi",
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

        # Parse receipts - track that we found expected Mint event
        found_supply_event = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = BenqiReceiptParser(underlying_decimals=decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    qi_token_address=BENQI_QI_USDC,
                )

                if parse_result.success and parse_result.supply_amount > 0:
                    print(f"  Supply amount:  {parse_result.supply_amount}")
                    print(f"  qiTokens minted: {parse_result.qi_tokens_minted}")
                    assert parse_result.supply_amount > 0, "Supply amount must be positive"
                    assert parse_result.qi_tokens_minted > 0, "qiTokens minted must be positive"
                    found_supply_event = True

        assert found_supply_event, "Receipt parser must find at least one Mint (supply) event"

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
        # VIB-4967: BENQI now has a bespoke Compound-V2 qiToken reader →
        # confidence=HIGH with populated whole-account before/after chain state.
        # Supply increases the user's collateral.
        _assert_high_confidence_state(payload)
        assert Decimal(payload["collateral_value_after_usd"]) > Decimal(payload["collateral_value_before_usd"]), (
            "SUPPLY must increase on-chain collateral value"
        )
        assert payload["asset"] == "USDC"
        # VIB-4967: BENQI receipt parser now exposes the lending amount extractors,
        # so the enricher populates extracted_data and amount_token + the FIFO
        # principal leg are MEASURED (no longer degraded to None). SUPPLY drains
        # wallet inventory: principal_delta_usd is the supplied principal in USD;
        # interest is not applicable on SUPPLY (must be None, never a fabricated 0).
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
            protocol="benqi",
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
        print(f"Test: Withdraw {withdraw_amount} USDC from BENQI using WithdrawIntent")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before withdraw: {format_token_amount(usdc_before, decimals)}")

        # Create WithdrawIntent
        intent = WithdrawIntent(
            protocol="benqi",
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

        # Parse receipts - track that we found expected Redeem event
        found_withdraw_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = BenqiReceiptParser(underlying_decimals=decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    qi_token_address=BENQI_QI_USDC,
                )

                if parse_result.success and parse_result.withdraw_amount > 0:
                    print(f"  Withdraw amount: {parse_result.withdraw_amount}")
                    print(f"  qiTokens redeemed: {parse_result.qi_tokens_redeemed}")
                    assert parse_result.withdraw_amount > 0, "Withdraw amount must be positive"
                    assert parse_result.qi_tokens_redeemed > 0, "qiTokens redeemed must be positive"
                    found_withdraw_event = True

        assert found_withdraw_event, "Receipt parser must find at least one Redeem (withdraw) event"

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
        # Two independent degradations stack here (VIB-4967):
        #   1. Chain state — BENQI has no pre/post-state reader, so
        #      confidence=ESTIMATED + before/after fields None.
        #   2. Amount/FIFO — the BENQI receipt parser exposes no lending amount
        #      extractors, so amount_token + principal/interest legs are None.
        # The typed WITHDRAW event still persists with correct identity + type.
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
            expected_event_type="WITHDRAW",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="WITHDRAW", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        # VIB-4967: bespoke Compound-V2 reader → confidence=HIGH. Withdraw decreases
        # collateral (we supplied 2000 then withdrew 1000, so after < before).
        _assert_high_confidence_state(payload)
        assert Decimal(payload["collateral_value_after_usd"]) < Decimal(payload["collateral_value_before_usd"]), (
            "WITHDRAW must decrease on-chain collateral value"
        )
        assert payload["asset"] == "USDC"
        # VIB-4967: extractors landed → amount_token + principal leg MEASURED.
        assert Decimal(payload["amount_token"]) == withdraw_amount
        assert payload["principal_delta_usd"] is not None, "WITHDRAW must measure a principal leg"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, (
            "Unmatched WITHDRAW (no Layer-5 SUPPLY lot) must degrade interest to None — never a fabricated 0"
        )

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_all_full_close_redeems_entire_qitoken_balance(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """withdraw_all=True compiles redeem(<full qiToken balance>) and leaves ZERO shares (VIB-5404).

        Pre-fix, withdraw_all without a redeem_amount FAILED to compile, forcing
        strategies into a redeemUnderlying(tracked-amount) workaround that
        stranded every wei of accrued interest — measured on a real fork as a
        26.79B-wei residual by the VIB-5795 TD-14 post-close verifier. This
        test pins the fixed contract at all four layers: the compile-time
        gateway balance read → redeem-by-share calldata → on-chain execution →
        the wallet's qiToken balance is EXACTLY zero afterwards (the full-close
        invariant the teardown verifier asserts).
        """
        from almanak.connectors.benqi.adapter import BENQI_REDEEM_SELECTOR

        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Supply first so there is a live qiToken position to fully close.
        supply_amount = Decimal("2000")
        supply_result = compiler.compile(
            SupplyIntent(protocol="benqi", token="USDC", amount=supply_amount, chain=CHAIN_NAME)
        )
        assert supply_result.status.value == "SUCCESS"
        supply_exec = await orchestrator.execute(supply_result.action_bundle)
        assert supply_exec.success, f"Initial supply failed: {supply_exec.error}"

        qi_before = get_token_balance(web3, BENQI_QI_USDC, funded_wallet)
        assert qi_before > 0, "Supply must have minted qiUSDC"
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        # Layer 1 — compile the full close: withdraw_all with NO amount (the
        # exact shape that failed pre-VIB-5404).
        intent = WithdrawIntent(
            protocol="benqi",
            token="USDC",
            amount=Decimal("0"),
            withdraw_all=True,
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"withdraw_all must compile without a redeem_amount (VIB-5404): {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None
        assert compilation_result.action_bundle.metadata["withdraw_all"] is True
        withdraw_txs = [
            tx
            for tx in compilation_result.action_bundle.transactions
            if str(tx.get("data", "")).lower().startswith(BENQI_REDEEM_SELECTOR)
        ]
        assert len(withdraw_txs) == 1, (
            "withdraw_all must compile the redeem-by-share path (redeem(uint256), "
            f"selector {BENQI_REDEEM_SELECTOR}), never redeemUnderlying — that path strands interest"
        )
        # The redeem argument is the compile-time-read FULL qiToken balance.
        compiled_redeem_shares = int(str(withdraw_txs[0]["data"])[10:74], 16)
        assert compiled_redeem_shares == qi_before, (
            f"redeem amount must be the full qiToken balance: {compiled_redeem_shares} != {qi_before}"
        )

        # Layer 5 pre-state (mirrors the runner), then Layer 2 — execute.
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3 — receipt parse: one Redeem event burning the FULL share balance.
        found_withdraw_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = BenqiReceiptParser(underlying_decimals=decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    qi_token_address=BENQI_QI_USDC,
                )
                if parse_result.success and parse_result.withdraw_amount > 0:
                    # Parser reports qiTokens scaled by the qiToken's 8 decimals;
                    # qi_before is raw units.
                    assert int(parse_result.qi_tokens_redeemed * 10**8) == qi_before, (
                        "Redeem event must burn the entire qiToken balance"
                    )
                    found_withdraw_event = True
        assert found_withdraw_event, "Receipt parser must find the Redeem (withdraw) event"

        # Layer 4 — bilateral balance deltas: ALL shares spent, underlying received.
        qi_after = get_token_balance(web3, BENQI_QI_USDC, funded_wallet)
        assert qi_after == 0, (
            f"Full close must leave EXACTLY zero qiTokens (the VIB-5795 TD-14 invariant); got {qi_after}"
        )
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before
        expected_floor = int(supply_amount * Decimal(10**decimals)) - 2  # mint/redeem floor rounding
        assert usdc_received >= expected_floor, (
            f"Full close must return the whole position (principal + accrued interest): "
            f"received {usdc_received} < floor {expected_floor}"
        )

        # Layer 5 — real accounting pipeline: typed WITHDRAW event persists and
        # collateral goes to (measured) zero.
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
        assert payload["asset"] == "USDC"
        assert Decimal(payload["collateral_value_after_usd"]) < Decimal(payload["collateral_value_before_usd"]), (
            "Full-close WITHDRAW must decrease on-chain collateral value"
        )
        assert Decimal(payload["collateral_value_after_usd"]) == 0, (
            "Full close must read MEASURED zero collateral afterwards"
        )
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
        print("Test: SupplyIntent with Insufficient Balance (BENQI)")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SupplyIntent(
            protocol="benqi",
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
