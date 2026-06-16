"""Production-grade Morpho Blue lending intent tests for Base.

Mirrors the coverage shape of the Ethereum Morpho Blue tests:
- Exact token balance deltas
- Receipt parser integration
- On-chain position sanity checks
- Failure case with conservation
- Layer 5 — persist the real ExecutionResult through the real accounting
  pipeline (ledger -> outbox -> AccountingProcessor.drain_one) into a
  throwaway SQLite and assert the typed LendingAccountingEvent is correct.

Layer 5 (epic VIB-4591 / ticket VIB-4604): Morpho Blue's lending pre/post
-state reader has full parity with Aave V3 (derives both market legs from
``intent.market_id`` for every lending intent type), so the Anvil ``eth_call``
adapter populates before/after collateral / debt / health-factor at
``confidence=HIGH``. The borrow-then-repay happy path asserts the exact
``principal_delta_usd`` / ``interest_delta_usd`` FIFO split; an unmatched
withdraw asserts the degradation contract (``interest_delta_usd is None``).
Collateral WITHDRAW ``amount_token`` is now measured (VIB-4635 FIXED) — the
amount is asserted exactly, alongside the HIGH-confidence + before/after
fidelity hard asserts. The failure path asserts zero ``accounting_events`` rows.

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/base/test_morpho_blue_lending.py -v -s
"""

from __future__ import annotations

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
from almanak.connectors.morpho_blue.adapter import MORPHO_MARKETS
from almanak.connectors.morpho_blue.receipt_parser import (
    MorphoBlueEvent,
    MorphoBlueEventType,
    MorphoBlueReceiptParser,
)
from almanak.connectors.morpho_blue.sdk import MorphoBlueSDK
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

CHAIN_NAME = "base"
PROTOCOL = "morpho_blue"
MORPHO_MARKET_NAME = "wstETH/USDC"


def _select_market_id(chain: str, market_name: str) -> str:
    markets = MORPHO_MARKETS.get(chain, {})
    for market_id, info in markets.items():
        if info.get("name") == market_name:
            return market_id
    raise AssertionError(f"Expected Morpho market '{market_name}' to exist for chain='{chain}'")


MORPHO_MARKET_ID = _select_market_id(CHAIN_NAME, MORPHO_MARKET_NAME)
MORPHO_MARKET_INFO = MORPHO_MARKETS[CHAIN_NAME][MORPHO_MARKET_ID]


def _collect_morpho_events(execution_result) -> list[MorphoBlueEvent]:
    parser = MorphoBlueReceiptParser()
    events: list[MorphoBlueEvent] = []

    for tx_result in execution_result.transaction_results:
        receipt = tx_result.receipt
        assert receipt is not None, "Expected receipt for executed transaction"

        parse_result = parser.parse_receipt(receipt.to_dict())
        assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
        events.extend(parse_result.events)

    return events


def _first_event(events: list[MorphoBlueEvent], event_type: MorphoBlueEventType) -> MorphoBlueEvent | None:
    for event in events:
        if event.event_type == event_type:
            return event
    return None


def _assets_wei(event: MorphoBlueEvent) -> int:
    assets = event.data.get("assets")
    assert assets is not None, f"Expected 'assets' in event data for {event.event_type}"
    return int(Decimal(str(assets)))


# =============================================================================
# Layer 5 helpers (shared) — epic VIB-4591 / ticket VIB-4604
# =============================================================================
#
# Identical shape to the merged Aave V3 golden
# (``tests/intents/arbitrum/test_aave_v3_lending.py``) and the Arbitrum Morpho
# Blue Layer-5 rollout: ``enrich_result`` so the ledger entry carries
# extracted_data; ``capture_lending_pre_state`` / ``capture_lending_post_state``
# via the test-scoped Anvil ``eth_call`` adapter so the lending category
# handler reads real collateral/debt/HF and emits ``confidence=HIGH``.


def _execution_context(wallet: str) -> ExecutionContext:
    return ExecutionContext(
        deployment_id="layer5-morpho-blue-lending",
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
    """Capture and serialize Morpho Blue pre/post state via the Anvil eth_call adapter.

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
    # Epic decision #5: the identity is a sextuple — there is NO agent_id.
    # Enforce the contract, don't just document it: a persisted lending row
    # must never carry a populated agent_id (absent, or present-but-empty).
    assert not row.get("agent_id"), (
        f"Layer-5 lending row must not carry an agent_id (epic decision #5); "
        f"got {row.get('agent_id')!r}"
    )


def _assert_no_lot_id(row: dict, payload: dict) -> None:
    """Epic decision #6: no lot_id on the persisted lending event."""
    assert "lot_id" not in row
    assert "lot_id" not in payload


def _assert_high_confidence_state(payload: dict) -> None:
    """Morpho Blue has a full pre/post-state reader → confidence=HIGH.

    Morpho Blue's ``_capture_morpho_blue_pre_state`` resolves both market legs
    from ``intent.market_id`` via ``MORPHO_MARKETS`` for every lending intent
    type (it does NOT require ``intent.collateral_token`` the way Compound V3's
    REPAY arm does — VIB-4633), so the Anvil eth_call adapter yields a live
    before+after read at ``confidence=HIGH`` with collateral/debt/HF populated.
    """
    assert payload["confidence"] == "HIGH", (
        f"Morpho Blue lending must persist confidence=HIGH (full reader + Anvil "
        f"eth_call adapter), got {payload['confidence']!r} "
        f"(unavailable_reason={payload.get('unavailable_reason')!r})"
    )
    assert payload["collateral_value_before_usd"] is not None, "before-collateral must be populated"
    assert payload["collateral_value_after_usd"] is not None, "after-collateral must be populated"
    assert payload["debt_value_before_usd"] is not None, "before-debt must be populated"
    assert payload["debt_value_after_usd"] is not None, "after-debt must be populated"
    assert payload["health_factor_before"] is not None, "before-health-factor must be populated"
    assert payload["health_factor_after"] is not None, "after-health-factor must be populated"


def _assert_asset(payload: dict, expected: str) -> None:
    """Asset-symbol assertion (case-insensitive).

    The lending category handler upper-cases the asset symbol
    (lending_handler.py: ``asset = (...).upper()``), so compare
    case-insensitively — the symbol identity, not its casing, is the contract.
    """
    assert payload["asset"].upper() == expected.upper(), (
        f"persisted asset {payload['asset']!r} must match {expected!r} (case-insensitive)"
    )


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    return ExecutionContext(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        simulation_enabled=True,
    )


@pytest.mark.base
@pytest.mark.borrow
@pytest.mark.lending
class TestMorphoBlueBorrowIntent:
    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_usdc_with_wsteth_collateral_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]

        wsteth_address = tokens["wstETH"]
        usdc_address = tokens["USDC"]
        assert wsteth_address.lower() == MORPHO_MARKET_INFO["collateral_token_address"].lower()
        assert usdc_address.lower() == MORPHO_MARKET_INFO["loan_token_address"].lower()

        wsteth_decimals = get_token_decimals(web3, wsteth_address)
        usdc_decimals = get_token_decimals(web3, usdc_address)

        collateral_amount = Decimal("0.1")
        borrow_amount = Decimal("100")

        print(f"\n{'='*80}")
        print(f"Test: Morpho Blue borrow {borrow_amount} USDC with {collateral_amount} wstETH collateral")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")

        wsteth_before = get_token_balance(web3, wsteth_address, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)

        print(f"wstETH before: {format_token_amount(wsteth_before, wsteth_decimals)}")
        print(f"USDC before:   {format_token_amount(usdc_before, usdc_decimals)}")

        intent = BorrowIntent.model_construct(
            protocol="morpho_blue",
            collateral_token="wstETH",
            collateral_amount=collateral_amount,
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            market_id=MORPHO_MARKET_ID,
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"
        assert len(compilation_result.action_bundle.transactions) == 3, (
            "Expected 3 transactions: approve(wstETH) + supplyCollateral + borrow"
        )

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        events = _collect_morpho_events(execution_result)

        supply_collateral_event = _first_event(events, MorphoBlueEventType.SUPPLY_COLLATERAL)
        assert supply_collateral_event is not None, "Expected SupplyCollateral event in Morpho Blue receipts"
        assert supply_collateral_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()

        borrow_event = _first_event(events, MorphoBlueEventType.BORROW)
        assert borrow_event is not None, "Expected Borrow event in Morpho Blue receipts"
        assert borrow_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()

        expected_collateral_wei = int(collateral_amount * Decimal(10**wsteth_decimals))
        expected_borrow_wei = int(borrow_amount * Decimal(10**usdc_decimals))

        supplied_collateral_wei = _assets_wei(supply_collateral_event)
        borrowed_assets_wei = _assets_wei(borrow_event)

        assert supplied_collateral_wei == expected_collateral_wei, (
            "SupplyCollateral assets must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {supplied_collateral_wei}"
        )
        assert borrowed_assets_wei == expected_borrow_wei, (
            "Borrow assets must EXACTLY equal borrow amount. "
            f"Expected: {expected_borrow_wei}, Got: {borrowed_assets_wei}"
        )

        wsteth_after = get_token_balance(web3, wsteth_address, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)

        wsteth_spent = wsteth_before - wsteth_after
        usdc_received = usdc_after - usdc_before

        print("\n--- Results ---")
        print(f"wstETH spent:  {format_token_amount(wsteth_spent, wsteth_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, usdc_decimals)}")

        assert wsteth_spent == expected_collateral_wei, (
            "wstETH spent must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {wsteth_spent}"
        )
        assert usdc_received == expected_borrow_wei, (
            "USDC received must EXACTLY equal borrow amount. "
            f"Expected: {expected_borrow_wei}, Got: {usdc_received}"
        )

        assert wsteth_spent == supplied_collateral_wei, (
            "wstETH spent must EXACTLY equal SupplyCollateral event assets. "
            f"Expected: {supplied_collateral_wei}, Got: {wsteth_spent}"
        )
        assert usdc_received == borrowed_assets_wei, (
            "USDC received must EXACTLY equal Borrow event assets. "
            f"Expected: {borrowed_assets_wei}, Got: {usdc_received}"
        )

        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.collateral > 0, "Expected collateral to be present after borrow"
        assert position.borrow_shares > 0, "Expected debt (borrow_shares) to be present after borrow"

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
        _assert_asset(payload, "USDC")
        assert Decimal(payload["amount_token"]) == borrow_amount
        assert payload["principal_delta_usd"] is not None, "BORROW must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "BORROW has no interest leg yet — must be None, not 0"
        assert Decimal(payload["debt_value_after_usd"]) > Decimal(payload["debt_value_before_usd"])

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_without_collateral_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Layer 5 failure contract: zero accounting_events rows."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_address = tokens["USDC"]

        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)

        intent = BorrowIntent(
            protocol="morpho_blue",
            collateral_token="wstETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("100"),
            market_id=MORPHO_MARKET_ID,
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

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert not execution_result.success, "Execution should fail without collateral"

        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)
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


async def _setup_borrow(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    execution_context: ExecutionContext,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
    collateral_amount: Decimal,
    borrow_amount: Decimal,
) -> tuple[BorrowIntent, ExecutionResult, dict | None]:
    """Helper: supply wstETH collateral and borrow USDC. Asserts success.

    Returns ``(borrow_intent, exec_result, bundle_metadata)`` so callers that
    need the FIFO BORROW lot in the Layer-5 basis pool (the exact
    borrow-then-repay split) can persist it through the harness. Callers that
    only need the on-chain side effect ignore the return value.
    """
    intent = BorrowIntent.model_construct(
        protocol="morpho_blue",
        collateral_token="wstETH",
        collateral_amount=collateral_amount,
        borrow_token="USDC",
        borrow_amount=borrow_amount,
        market_id=MORPHO_MARKET_ID,
        chain=CHAIN_NAME,
    )
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    result = compiler.compile(intent)
    assert result.status.value == "SUCCESS", f"Borrow setup compile failed: {result.error}"
    assert result.action_bundle is not None, "Borrow setup missing action_bundle"
    exec_result = await orchestrator.execute(result.action_bundle, execution_context)
    assert exec_result.success, f"Borrow setup execution failed: {exec_result.error}"
    return intent, exec_result, result.action_bundle.metadata


@pytest.mark.base
@pytest.mark.repay
@pytest.mark.lending
class TestMorphoBlueRepayIntent:
    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_usdc_full_after_borrow(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Repay full USDC debt with repay_full=True after borrowing against wstETH.

        Verifies the VIB-587 fix: repay_full=True correctly queries borrow_shares via
        Anvil fork RPC (not Alchemy mainnet), so all shares including residuals are repaid.

        Layer 5: persist BOTH the BORROW and the REPAY through the same harness
        so the FIFO basis pool matches — assert the exact principal_delta_usd /
        interest_delta_usd split (epic decision #6).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wsteth_address = tokens["wstETH"]
        usdc_address = tokens["USDC"]

        wsteth_decimals = get_token_decimals(web3, wsteth_address)
        usdc_decimals = get_token_decimals(web3, usdc_address)

        collateral_amount = Decimal("0.1")
        borrow_amount = Decimal("100")

        print(f"\n{'='*80}")
        print(f"Test: Morpho Blue repay_full=True after borrowing {borrow_amount} USDC")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")

        # Setup: borrow first
        borrow_intent, borrow_exec_result, borrow_bundle_meta = await _setup_borrow(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            anvil_rpc_url=anvil_rpc_url,
            collateral_amount=collateral_amount,
            borrow_amount=borrow_amount,
        )

        # Layer 5: persist the BORROW so the FIFO basis pool holds the lot the
        # REPAY will match against (this is what makes the split exact).
        borrow_pre_state = _capture_lending_state(
            borrow_intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )
        borrow_enriched = _enrich_for_accounting(
            borrow_exec_result, borrow_intent, funded_wallet, borrow_bundle_meta
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

        # Record balances before repay
        wsteth_before = get_token_balance(web3, wsteth_address, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)

        print(f"wstETH before repay: {format_token_amount(wsteth_before, wsteth_decimals)}")
        print(f"USDC before repay:   {format_token_amount(usdc_before, usdc_decimals)}")

        # Layer 1: Compile RepayIntent with repay_full=True
        intent = RepayIntent(
            protocol="morpho_blue",
            token="USDC",
            amount=borrow_amount,  # fallback; ignored by repay_full
            repay_full=True,
            market_id=MORPHO_MARKET_ID,
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parsing -- expect Repay event
        parser = MorphoBlueReceiptParser()
        all_events: list[MorphoBlueEvent] = []
        for tx_result in execution_result.transaction_results:
            assert tx_result.receipt is not None, "Expected receipt for executed transaction"
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
            all_events.extend(parse_result.events)

        repay_event = _first_event(all_events, MorphoBlueEventType.REPAY)
        assert repay_event is not None, "Expected Repay event in Morpho Blue receipts"
        assert repay_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()

        repaid_assets_wei = _assets_wei(repay_event)
        assert repaid_assets_wei > 0, "Repay event must report positive assets repaid"

        # Layer 4: Balance deltas -- USDC decreases, wstETH unchanged
        wsteth_after = get_token_balance(web3, wsteth_address, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        wsteth_delta = abs(wsteth_before - wsteth_after)

        print("\n--- Results ---")
        print(f"USDC spent (repaid):  {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"wstETH change:        {format_token_amount(wsteth_delta, wsteth_decimals)} (expect 0)")

        expected_usdc_wei = int(borrow_amount * Decimal(10**usdc_decimals))
        assert usdc_spent >= expected_usdc_wei, (
            "USDC spent must be at least the borrowed amount (includes tiny interest). "
            f"Expected >= {expected_usdc_wei}, Got: {usdc_spent}"
        )
        assert usdc_spent == repaid_assets_wei, (
            "USDC spent must EXACTLY equal Repay event assets. "
            f"Expected: {repaid_assets_wei}, Got: {usdc_spent}"
        )
        assert wsteth_delta == 0, (
            "wstETH balance must not change during repay (collateral stays locked). "
            f"Got wstETH delta: {wsteth_delta}"
        )

        # On-chain sanity: borrow_shares must be 0 after full repay
        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.borrow_shares == 0, (
            f"Expected borrow_shares=0 after repay_full=True, got {position.borrow_shares}"
        )
        assert position.collateral > 0, "Collateral must still be present after repay (not withdrawn yet)"

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
        _assert_asset(payload, "USDC")

        # Exact FIFO split: the REPAY matched the prior BORROW lot persisted in
        # the same harness. ``repay_full=True`` closes the entire borrow, so
        # Morpho Blue's per-block ``accrueInterest`` adds a tiny interest charge
        # on top of principal (verified on a real Anvil fork). The FIFO matcher
        # produces a measured principal leg (== the full borrowed principal,
        # the lot is fully consumed) AND a measured interest leg (a small
        # positive value — NOT None, NOT a fabricated 0). principal + interest
        # must reconcile to the actual repaid cash flow in USD.
        assert payload["principal_delta_usd"] is not None, "matched REPAY must measure principal"
        assert payload["interest_delta_usd"] is not None, (
            "matched REPAY (BORROW lot present in harness) must produce a "
            "measured interest leg — not None"
        )
        principal_usd = Decimal(payload["principal_delta_usd"])
        interest_usd = Decimal(payload["interest_delta_usd"])
        assert principal_usd == borrowed_principal_usd, (
            f"FIFO principal_delta_usd must equal the fully-matched borrowed "
            f"principal ({borrowed_principal_usd}); got {principal_usd}"
        )
        assert interest_usd > Decimal("0"), (
            f"Morpho repay_full accrues per-block interest — interest_delta_usd "
            f"must be a measured positive value, got {interest_usd}"
        )
        usdc_price = price_oracle["USDC"]
        repaid_usd = (Decimal(usdc_spent) / Decimal(10**usdc_decimals)) * usdc_price
        assert principal_usd + interest_usd == repaid_usd, (
            f"principal ({principal_usd}) + interest ({interest_usd}) must tie "
            f"to the repaid cash flow ({repaid_usd})"
        )
        assert Decimal(payload["debt_value_after_usd"]) < Decimal(payload["debt_value_before_usd"])

        print("\nALL CHECKS PASSED")


@pytest.mark.base
@pytest.mark.withdraw
@pytest.mark.lending
class TestMorphoBlueWithdrawCollateralIntent:
    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_wsteth_collateral_after_repay(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Withdraw wstETH collateral after a full borrow-repay cycle.

        Verifies amount-based withdraw (withdraw_all=False) recovers exact collateral.
        This is the final step of the iter-146 lifecycle.

        Layer 5: the collateral was supplied via the BorrowIntent setup (NOT
        persisted through the Layer-5 harness as a SUPPLY), so the FIFO supply
        pool is empty: this WITHDRAW degrades — interest_delta_usd stays None.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wsteth_address = tokens["wstETH"]

        wsteth_decimals = get_token_decimals(web3, wsteth_address)

        collateral_amount = Decimal("0.1")
        borrow_amount = Decimal("100")

        print(f"\n{'='*80}")
        print(f"Test: Morpho Blue withdraw {collateral_amount} wstETH collateral after borrow-repay")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")

        # Setup step 1: borrow
        await _setup_borrow(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            anvil_rpc_url=anvil_rpc_url,
            collateral_amount=collateral_amount,
            borrow_amount=borrow_amount,
        )

        # Setup step 2: repay full debt so withdrawCollateral is unblocked
        repay_intent = RepayIntent(
            protocol="morpho_blue",
            token="USDC",
            amount=borrow_amount,
            repay_full=True,
            market_id=MORPHO_MARKET_ID,
            chain=CHAIN_NAME,
        )
        repay_compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        repay_result = repay_compiler.compile(repay_intent)
        assert repay_result.status.value == "SUCCESS", f"Repay setup compile failed: {repay_result.error}"
        repay_exec = await orchestrator.execute(repay_result.action_bundle, execution_context)
        assert repay_exec.success, f"Repay setup execution failed: {repay_exec.error}"

        # Record balances before withdraw
        wsteth_before = get_token_balance(web3, wsteth_address, funded_wallet)
        print(f"wstETH before withdraw: {format_token_amount(wsteth_before, wsteth_decimals)}")

        # Layer 1: Compile WithdrawIntent (amount-based, withdraw_all=False)
        intent = WithdrawIntent(
            protocol="morpho_blue",
            token="wstETH",
            amount=collateral_amount,
            withdraw_all=False,
            market_id=MORPHO_MARKET_ID,
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parsing -- expect WithdrawCollateral event
        parser = MorphoBlueReceiptParser()
        all_events: list[MorphoBlueEvent] = []
        for tx_result in execution_result.transaction_results:
            assert tx_result.receipt is not None, "Expected receipt for executed transaction"
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
            all_events.extend(parse_result.events)

        withdraw_event = _first_event(all_events, MorphoBlueEventType.WITHDRAW_COLLATERAL)
        assert withdraw_event is not None, "Expected WithdrawCollateral event in Morpho Blue receipts"
        assert withdraw_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()

        withdrawn_assets_wei = _assets_wei(withdraw_event)
        expected_collateral_wei = int(collateral_amount * Decimal(10**wsteth_decimals))
        assert withdrawn_assets_wei == expected_collateral_wei, (
            "WithdrawCollateral event assets must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {withdrawn_assets_wei}"
        )

        # Layer 4: Balance delta -- wstETH must return to wallet
        wsteth_after = get_token_balance(web3, wsteth_address, funded_wallet)
        wsteth_received = wsteth_after - wsteth_before

        print("\n--- Results ---")
        print(f"wstETH received: {format_token_amount(wsteth_received, wsteth_decimals)}")

        assert wsteth_received == expected_collateral_wei, (
            "wstETH received must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {wsteth_received}"
        )
        assert wsteth_received == withdrawn_assets_wei, (
            "wstETH received must EXACTLY equal WithdrawCollateral event assets. "
            f"Expected: {withdrawn_assets_wei}, Got: {wsteth_received}"
        )

        # On-chain sanity: position should be empty after full unwind
        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.collateral == 0, (
            f"Expected collateral=0 after withdrawal, got {position.collateral}"
        )
        assert position.borrow_shares == 0, (
            f"Expected borrow_shares=0 after full repay+withdraw, got {position.borrow_shares}"
        )

        # ── Layer 5: unmatched-withdraw degradation + VIB-4635 amount_token ───
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
        # Morpho Blue collateral WITHDRAW DOES reach confidence=HIGH with full
        # before/after collateral/debt/HF (verified on a real Anvil fork). This
        # part is correct and stays a hard assert.
        _assert_high_confidence_state(payload)
        _assert_asset(payload, "wstETH")
        assert payload["interest_delta_usd"] is None, (
            "Unmatched WITHDRAW (no Layer-5 SUPPLY lot) must degrade interest to "
            "None — never a fabricated 0"
        )
        assert Decimal(payload["collateral_value_after_usd"]) < Decimal(payload["collateral_value_before_usd"])
        # VIB-4635 (FIXED): Morpho Blue collateral WITHDRAW now populates
        # amount_token. Collateral withdrawals route through
        # withdrawCollateral(...) and emit WithdrawCollateral (not the
        # loan-side Withdraw), so the generic withdraw_amount key is absent.
        # The Morpho parser's extract_withdraw_collateral_amount surfaces the
        # amount as withdraw_collateral_amount via the morpho_blue enricher
        # overlay, and the lending handler's _extract_amount_human falls back
        # to it (mirror of the SUPPLY collateral path). The on-chain
        # withdrawal is verified correct above (exact balance delta + event
        # assets agree); the books now record the exact measured amount
        # (Empty≠Zero≠None). WITHDRAW-side mirror of VIB-4633's Compound V3
        # Finding A.
        assert Decimal(payload["amount_token"]) == collateral_amount, (
            "VIB-4635: Morpho Blue collateral WITHDRAW must record the exact "
            f"on-chain amount. Got amount_token={payload['amount_token']!r}, "
            f"expected {collateral_amount!r}."
        )
        # Unmatched WITHDRAW (no Layer-5 SUPPLY lot) degrades principal to the
        # total measured withdrawal — still a measured, positive leg (never a
        # fabricated 0). interest stays None (asserted above).
        assert payload["principal_delta_usd"] is not None, "WITHDRAW must measure a principal leg"
        assert Decimal(payload["principal_delta_usd"]) > 0

        print("\nALL CHECKS PASSED")


@pytest.mark.base
@pytest.mark.supply
@pytest.mark.lending
class TestMorphoBlueSupplyIntent:
    """SUPPLY USDC as loan token into the wstETH/USDC market (VIB-4307).

    Uses ``use_as_collateral=False`` so we deposit USDC as loan capital
    (earning interest from borrowers) rather than as collateral. This is
    the standard "lending" side of the market — no collateral checks
    needed, just an approve + supply pair.
    """

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_usdc_as_loan_token(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Supply USDC as loan token (not collateral) into wstETH/USDC market.

        Layer 5: loan-side supply emits a Morpho ``Supply`` event, so the
        lending handler resolves the amount via the primary ``supply_amount``
        key → ``amount_token`` populated and ``confidence=HIGH`` (this is the
        loan-side path, NOT the collateral path that hits VIB-4635).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_address = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc_address)
        assert usdc_address.lower() == MORPHO_MARKET_INFO["loan_token_address"].lower(), (
            "Expected market loan token to be USDC"
        )

        supply_amount = Decimal("1000")  # 1000 USDC

        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)
        expected_wei = int(supply_amount * Decimal(10**usdc_decimals))
        assert usdc_before >= expected_wei, (
            f"funded_wallet has only {usdc_before} USDC wei, need >= {expected_wei}"
        )

        print(f"\n{'='*80}")
        print(f"Morpho Blue SUPPLY: {supply_amount} USDC (loan token) on Base")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # Layer 1: Compile
        intent = SupplyIntent(
            protocol="morpho_blue",
            token="USDC",
            amount=supply_amount,
            use_as_collateral=False,
            market_id=MORPHO_MARKET_ID,
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        # Layer 2: Execute
        execution_result = await orchestrator.execute(
            compilation_result.action_bundle, execution_context
        )
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parse — Supply (loan-token) event
        events = _collect_morpho_events(execution_result)
        supply_event = _first_event(events, MorphoBlueEventType.SUPPLY)
        assert supply_event is not None, (
            "Expected Supply event in Morpho Blue receipts (loan-token supply)"
        )
        assert supply_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()
        supplied_wei = _assets_wei(supply_event)
        assert supplied_wei == expected_wei, (
            f"Supply event assets must EXACTLY equal supply amount. "
            f"Expected: {expected_wei}, Got: {supplied_wei}"
        )

        # Layer 4: Balance delta — exact USDC spent
        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        assert usdc_spent == expected_wei, (
            f"USDC spent must EXACTLY equal supply amount. "
            f"Expected: {expected_wei}, Got: {usdc_spent}"
        )
        assert usdc_spent == supplied_wei

        # On-chain sanity
        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.supply_shares > 0, (
            f"Expected supply_shares > 0 after loan-token supply, got {position.supply_shares}"
        )

        # ── Layer 5: loan-side SUPPLY accounting ─────────────────────────────
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
        _assert_asset(payload, "USDC")
        # Loan-side Supply event → handler resolves the amount via the primary
        # supply_amount key (NOT the collateral path), so amount_token is
        # populated. SUPPLY drains wallet inventory: principal measured,
        # interest not applicable (must be None, not 0).
        assert payload["amount_token"] is not None, (
            "loan-side SUPPLY must populate amount_token (Supply event → "
            "supply_amount key; not the VIB-4635 collateral path)"
        )
        assert Decimal(payload["amount_token"]) == supply_amount
        assert payload["principal_delta_usd"] is not None, "SUPPLY must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "SUPPLY has no interest leg — must be None, not 0"

        print(f"\nUSDC spent: {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"Supply shares: {position.supply_shares}")
        print("\nALL CHECKS PASSED")
