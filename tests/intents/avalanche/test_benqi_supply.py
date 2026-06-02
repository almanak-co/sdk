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

THE BENQI DEGRADATION (genuine production gap, tracked by VIB-4967):
BENQI is a Compound-V2-style market (qiTokens) and has NO pre/post-state reader.
``_PROTOCOL_PRE_STATE_READERS`` in
``almanak/framework/accounting/lending_accounting.py`` has entries for
``aave_v3`` / ``aave`` / ``morpho_blue`` / ``compound_v3`` but NOT ``benqi``,
so ``capture_lending_pre_state`` / ``capture_lending_post_state`` return
``None`` for a BENQI intent and ``lending_state_to_dict`` serializes ``None``.
With no ``post_state_json`` the lending handler sets ``confidence=ESTIMATED``
and leaves every before/after collateral / debt / health-factor field
``None`` with a populated ``unavailable_reason`` (Empty≠Zero≠None — nothing is
fabricated). So BENQI Layer 5 asserts the DEGRADATION contract for chain state
(``_assert_state_degraded_no_reader``), NOT the HIGH-confidence contract the
Aave / Compound goldens use. The FIFO principal / interest split is derived
from the basis store and is unaffected by the missing reader, so those
assertions match the goldens exactly.

TWO STACKED PRODUCTION GAPS (both VIB-4967, both encoded as degradation):
  1. No pre/post-state reader — confidence=ESTIMATED, before/after fields None.
  2. No lending amount extractors on ``BenqiReceiptParser`` (no
     ``SUPPORTED_EXTRACTIONS`` / ``extract_supply_amount`` etc., unlike Spark),
     so ``ResultEnricher`` cannot populate ``extracted_data`` and the handler's
     ``_extract_amount_human`` returns ``None`` → ``amount_token``,
     ``principal_delta_usd`` and ``interest_delta_usd`` all degrade to ``None``
     (measured-unavailable, never fabricated). The typed event still persists
     with correct identity + event_type. Closing both gaps lights up the
     Spark/Aave HIGH contract with no test rewrite.

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
    block: int | str | None = None,
) -> dict | None:
    """Capture and serialize BENQI pre/post state via the Anvil eth_call adapter.

    Returns the runner-shaped state dict (``lending_state_to_dict`` output) or
    ``None`` — never a fabricated zero. For BENQI this currently ALWAYS returns
    ``None`` because BENQI has no pre/post-state reader (VIB-4967); the
    call is kept to mirror the runner's wiring exactly so a future reader fix
    lights up the HIGH-confidence path with no test change.

    ``block`` (VIB-4589 / F7) pins the read: pre-state passes ``None`` (→
    ``"latest"``, safe because the read precedes submission); post-state passes
    the confirmed receipt's ``block_number`` so a future reader cannot race the
    upstream RPC's receipt indexer. Threaded now so the wiring is byte-for-byte
    the runner's the moment a BENQI reader lands.
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


def _assert_state_degraded_no_reader(payload: dict) -> None:
    """BENQI genuine production degradation contract (VIB-4967).

    BENQI is absent from ``_PROTOCOL_PRE_STATE_READERS`` in
    ``almanak/framework/accounting/lending_accounting.py`` (only ``aave_v3`` /
    ``aave`` / ``morpho_blue`` / ``compound_v3`` are wired), so
    ``capture_lending_pre_state`` / ``capture_lending_post_state`` return
    ``None`` for a BENQI intent. With no ``post_state_json`` the lending
    handler sets ``confidence=ESTIMATED`` and leaves every before/after
    collateral / debt / health-factor field ``None`` with a populated
    ``unavailable_reason``. This is the TRUE current production behavior
    (deterministic across the avalanche Anvil-fork CI), NOT a flake. We assert
    the genuine degradation contract here rather than HIGH; the
    HIGH-confidence expectation (and before/after collateral / debt / HF
    fidelity) is the gap tracked by VIB-4967 — wire a BENQI
    Compound-V2-style qiToken pre/post-state reader into the registry.
    Empty≠Zero≠None: ``unavailable_reason`` is set, nothing is fabricated.
    """
    assert payload["confidence"] == "ESTIMATED", (
        f"BENQI lending genuinely degrades to confidence=ESTIMATED today "
        f"(VIB-4967: no BENQI entry in _PROTOCOL_PRE_STATE_READERS); "
        f"got {payload['confidence']!r}"
    )
    assert payload.get("unavailable_reason"), (
        "degraded BENQI lending must carry a non-empty unavailable_reason (never fabricated)"
    )
    # Degradation must not fabricate before/after chain state.
    assert payload["collateral_value_before_usd"] is None, (
        "VIB-4967: degraded BENQI must not fabricate before-collateral"
    )
    assert payload["collateral_value_after_usd"] is None, (
        "VIB-4967: degraded BENQI must not fabricate after-collateral"
    )
    assert payload["debt_value_before_usd"] is None, "VIB-4967: degraded BENQI must not fabricate before-debt"
    assert payload["debt_value_after_usd"] is None, "VIB-4967: degraded BENQI must not fabricate after-debt"
    assert payload["health_factor_before"] is None, (
        "VIB-4967: degraded BENQI must not fabricate before-health-factor"
    )
    assert payload["health_factor_after"] is None, (
        "VIB-4967: degraded BENQI must not fabricate after-health-factor"
    )


def _assert_amount_and_fifo_degraded_no_extractor(payload: dict) -> None:
    """BENQI amount/FIFO degradation contract (VIB-4967).

    DISTINCT from the missing chain-state reader above. The
    ``BenqiReceiptParser`` (a strategy-side ``ReceiptParserConnector``) exposes
    NO standalone lending amount extractors — there is no ``SUPPORTED_EXTRACTIONS``
    set and no ``extract_supply_amount`` / ``extract_borrow_amount`` /
    ``extract_repay_amount`` / ``extract_withdraw_amount`` methods (contrast
    ``almanak/connectors/spark/receipt_parser.py``, which has them and returns a
    raw ``int``). So ``ResultEnricher`` cannot populate
    ``extracted_data["supply_amount" | "borrow_amount" | ...]`` for a BENQI
    intent, and ``lending_handler._extract_amount_human`` returns ``None``.

    With ``amount_human is None`` the handler emits ``amount_token=None`` AND
    skips the FIFO basis-store block entirely (it is gated on
    ``amount_human is not None``), so ``principal_delta_usd`` and
    ``interest_delta_usd`` both stay ``None`` — measured-as-unavailable, never
    fabricated (Empty≠Zero≠None). The typed event row IS still persisted with
    the correct ``event_type`` and identity; only the value legs degrade.

    This is the TRUE current production behavior (deterministic on the avalanche
    Anvil-fork CI), NOT a flake. Closing it (a BENQI lending extraction spec on
    the receipt parser, mirroring Spark/Aave) is the gap tracked by
    VIB-4967; the moment those extractors land, ``amount_token`` and the
    FIFO legs populate and these asserts must be tightened to the Spark/Aave
    HIGH contract.
    """
    assert payload["amount_token"] is None, (
        "VIB-4967: BENQI receipt parser exposes no lending amount extractors, "
        "so the enricher cannot populate extracted_data and amount_token degrades to None "
        f"(never fabricated); got {payload['amount_token']!r}"
    )
    assert payload["principal_delta_usd"] is None, (
        "VIB-4967: with amount_human=None the FIFO block is skipped, "
        "so principal_delta_usd degrades to None (measured-unavailable, never a fabricated 0)"
    )
    assert payload["interest_delta_usd"] is None, (
        "VIB-4967: with amount_human=None the FIFO block is skipped, "
        "so interest_delta_usd degrades to None (measured-unavailable, never a fabricated 0)"
    )


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
        # VIB-4967: BENQI has no pre/post-state reader → confidence=ESTIMATED,
        # before/after chain state degraded to None (not fabricated).
        _assert_state_degraded_no_reader(payload)
        assert payload["asset"] == "USDC"
        # VIB-4967: BENQI receipt parser exposes no lending amount
        # extractors, so amount_token + the FIFO principal/interest legs all
        # degrade to None (measured-unavailable, never fabricated). The typed
        # SUPPLY event still persists with correct identity + event_type.
        _assert_amount_and_fifo_degraded_no_extractor(payload)

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
        _assert_state_degraded_no_reader(payload)
        assert payload["asset"] == "USDC"
        _assert_amount_and_fifo_degraded_no_extractor(payload)

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
