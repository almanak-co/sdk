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

Layer 5 (epic VIB-4591 / ticket VIB-4605): mirrors the merged Spark / Aave V3 /
Compound V3 lending goldens and is kept identical to the Ethereum Euler V2 file
(intent-test rule #7: no per-chain variance for the same protocol). Euler V2 now has
a BESPOKE vault/EVC pre/post-state reader (VIB-4966, enabled in
``_GENERIC_PRE_STATE_PROTOCOLS``): its independent ERC-4626 vaults are read via
``maxWithdraw`` on the deposit vault + ``debtOf`` on the borrow/controller vault, so
the persisted typed event reaches ``confidence=HIGH`` with populated before/after
collateral / debt / HF (``_assert_high_confidence_state`` — INVERTS the prior
degradation contract). The FIFO principal / interest split derives from the basis
store and is unaffected by the reader. The xfail-marked borrow/repay bodies carry the
full Layer-5 success block so they light up automatically when a valid collateral
vault is registered; the non-xfail zero-collateral test asserts the Layer-5 failure
contract (zero ``accounting_events`` rows).

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

import json
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.euler_v2.compiler import EulerV2Compiler
from almanak.connectors.euler_v2.receipt_parser import EulerV2ReceiptParser
from almanak.framework.accounting.lending_accounting import (
    capture_lending_post_state,
    capture_lending_pre_state,
    lending_state_to_dict,
)
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent
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
EULER_V2_USDC_VAULT = "0x37ca03aD51B8ff79aAD35FadaCBA4CEDF0C3e74e"  # eUSDC-19 (collateral vault)
EULER_V2_WAVAX_VAULT = "0x6c718a70239fA548c0bD268fE88F37EBE8b6E2ea"  # eWAVAX-2 (CLOSED)

# Conservative amounts: 1000 USDC collateral, 100 USDC borrow (~10% LTV)
COLLATERAL_AMOUNT = Decimal("1000")
BORROW_AMOUNT = Decimal("100")
REPAY_AMOUNT = Decimal("50")

PROTOCOL = "euler_v2"


# =============================================================================
# Layer 5 helpers (shared) — kept identical to the Ethereum Euler V2 file
# (intent-test rule #7: no per-chain variance for the same protocol). Euler V2 now
# has a BESPOKE vault/EVC pre/post-state reader (VIB-4966), so both captures return
# populated state and the persisted event reaches ``confidence=HIGH`` with before/
# after collateral / debt / HF.
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

    Returns the runner-shaped state dict or ``None`` — never a fabricated zero. With
    the bespoke vault/EVC reader (VIB-4966) this returns populated before/after
    collateral / debt / HF via the Anvil eth_call adapter (a measured-zero leg is
    ``"0"``, never ``None``), lighting up the HIGH-confidence path.

    ``block`` (VIB-4589 / F7) pins the read: pre-state passes ``None`` (→
    ``"latest"``, safe because the read precedes submission); post-state passes
    the confirmed receipt's ``block_number`` so the reader cannot race the
    upstream RPC's receipt indexer.
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
    """Euler V2 HIGH-confidence chain-state contract (VIB-4966 reader landed).

    Euler V2 now has a BESPOKE vault/EVC pre/post-state reader
    (``almanak/connectors/euler_v2/lending_read.py``, enabled in
    ``_GENERIC_PRE_STATE_PROTOCOLS``): unlike Aave's single ``getUserAccountData``,
    its independent ERC-4626 vaults are read via ``maxWithdraw`` on the deposit vault +
    ``debtOf`` on the borrow/controller vault, valued from the framework-injected
    price/decimals seam (Euler is not USD-native). So ``capture_lending_pre_state`` /
    ``capture_lending_post_state`` return populated state through the Anvil eth_call
    adapter and the lending handler emits ``confidence=HIGH`` with every before/after
    collateral / debt / health-factor field populated (Empty ≠ Zero — a measured zero
    is ``"0"``, never ``None``). This is the inverted contract VIB-4966 ships,
    replacing the prior ``_assert_state_degraded_no_reader_vib4605`` degradation
    contract.
    """
    assert payload["confidence"] == "HIGH", (
        f"Euler V2 lending must persist confidence=HIGH (bespoke reader + Anvil eth_call adapter), "
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

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-2643: eWAVAX-2 vault has maxDeposit=0 (supply cap reached). "
        "Euler V2 borrow requires a valid collateral vault not yet in adapter "
        "(eBTC.b or eWETH.e — VIB-2643 tracks adding them). Formally re-pointed "
        "to open VIB-2643 in the VIB-5964 sweep (as of 2026-07-24).",
        strict=True,
    )
    async def test_borrow_usdc_with_wavax_collateral(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test USDC borrow with WAVAX collateral using SupplyIntent + BorrowIntent.

        Flow:
        1. Supply WAVAX as collateral via SupplyIntent to eWAVAX-2
        2. Create BorrowIntent with zero additional collateral (already supplied)
        3. Compile and execute borrow via EVC batch
        4. Parse receipt for Borrow event
        5. Verify USDC balance increased by exact borrow amount
        6. Layer 5: assert persisted BORROW accounting event
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        wavax = tokens["WAVAX"]
        usdc_decimals = get_token_decimals(web3, usdc)
        wavax_decimals = get_token_decimals(web3, wavax)

        collateral_amount = Decimal("5")  # 5 WAVAX (~$125)
        borrow_amount = Decimal("10")  # 10 USDC (~8% LTV)

        print(f"\n{'=' * 80}")
        print(f"Test: Borrow {borrow_amount} USDC with {collateral_amount} WAVAX collateral on Euler V2")
        print(f"{'=' * 80}")

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

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Parse receipts for Borrow event
        found_borrow_event = False
        for tx_result in execution_result.transaction_results:
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
            f"USDC received must EXACTLY equal borrow amount. Expected: {expected_usdc_received}, Got: {usdc_received}"
        )

        wavax_after = get_token_balance(web3, wavax, funded_wallet)
        wavax_spent = wavax_before - wavax_after
        expected_wavax_spent = int(collateral_amount * Decimal(10**wavax_decimals))
        assert wavax_spent == expected_wavax_spent, (
            f"WAVAX spent must EXACTLY equal collateral amount. Expected: {expected_wavax_spent}, Got: {wavax_spent}"
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
        # VIB-4966: bespoke reader → confidence=HIGH. Borrow increases on-chain debt.
        _assert_high_confidence_state(payload)
        assert Decimal(payload["debt_value_after_usd"]) > Decimal(payload["debt_value_before_usd"]), (
            "BORROW must increase on-chain debt value"
        )
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == borrow_amount
        # BORROW records the FIFO principal lot: principal measured, interest
        # has no leg yet (a repay would match it) — must be None, not 0.
        assert payload["principal_delta_usd"] is not None, "BORROW must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "BORROW has no interest leg yet — must be None, not 0"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-2643: eWAVAX-2 vault has maxDeposit=0 (supply cap reached). "
        "Euler V2 borrow requires a valid collateral vault not yet in adapter "
        "(eBTC.b or eWETH.e — VIB-2643 tracks adding them). Formally re-pointed "
        "to open VIB-2643 in the VIB-5964 sweep (as of 2026-07-24).",
        strict=True,
    )
    async def test_repay_usdc_after_borrow(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test USDC repay using RepayIntent (after borrowing).

        Flow:
        1. Supply WAVAX collateral + borrow USDC (setup; the BORROW is persisted
           through the Layer-5 harness so the FIFO basis pool holds the lot)
        2. Create RepayIntent to repay portion of debt
        3. Compile and execute
        4. Verify USDC balance decreased by exact repay amount
        5. Layer 5: assert the EXACT principal/interest FIFO split
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
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
        expected_wavax_wei = int(collateral_amount * Decimal(10 ** get_token_decimals(web3, tokens["WAVAX"])))
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
        borrow_pre_state = _capture_lending_state(
            borrow_intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle)
        assert borrow_exec.success, f"Initial borrow failed: {borrow_exec.error}"

        # Layer 5: persist the BORROW so the FIFO basis pool holds the lot the
        # REPAY will match against (this is what makes the split exact).
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
        borrowed_principal_usd = Decimal(borrow_payload["principal_delta_usd"])

        # Now repay
        print(f"\n{'=' * 80}")
        print(f"Test: Repay {repay_amount} USDC to Euler V2 using RepayIntent")
        print(f"{'=' * 80}")

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

        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

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
            f"USDC spent must EXACTLY equal repay amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # ── Layer 5: borrow-then-repay FIFO split ────────────────────────────
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
        # VIB-4966: bespoke reader → confidence=HIGH. Repay decreases on-chain debt
        # (we borrowed borrow_amount then repaid repay_amount, so after < before).
        _assert_high_confidence_state(payload)
        assert Decimal(payload["debt_value_after_usd"]) < Decimal(payload["debt_value_before_usd"]), (
            "REPAY must decrease on-chain debt value"
        )
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == repay_amount

        # Exact FIFO split: independent of the chain-state read (it derives from
        # the basis store, not from post_state_json). The REPAY matched the prior
        # BORROW lot in the same harness; repaying repay_amount of a borrow_amount
        # position within the same Anvil block accrues no interest, so the entire
        # repaid amount is matched principal and the interest leg is a measured
        # zero (NOT None — the match succeeded).
        assert payload["principal_delta_usd"] is not None, "matched REPAY must measure principal"
        assert payload["interest_delta_usd"] is not None, (
            "matched REPAY (BORROW lot present in harness) must produce a measured interest leg — not None"
        )
        principal_usd = Decimal(payload["principal_delta_usd"])
        interest_usd = Decimal(payload["interest_delta_usd"])
        repaid_usd = repay_amount * (borrowed_principal_usd / borrow_amount)
        assert principal_usd == repaid_usd, (
            f"FIFO principal_delta_usd must equal the matched principal ({repaid_usd}); got {principal_usd}"
        )
        assert interest_usd == Decimal("0"), (
            f"same-block partial repay accrues no interest — interest_delta_usd must be a measured 0, got {interest_usd}"
        )
        assert principal_usd + interest_usd == repaid_usd, "principal + interest must tie to repaid cash flow"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_without_collateral_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test that BorrowIntent without collateral fails gracefully.

        Creates a BorrowIntent with zero collateral and no prior supply.
        The borrow should fail (either at compilation or execution) because
        there's no collateral backing the loan. Verifies balance conservation.

        Layer 5 failure contract: zero accounting_events rows.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        wavax = tokens["WAVAX"]

        print(f"\n{'=' * 80}")
        print("Test: BorrowIntent without Collateral (Euler V2)")
        print(f"{'=' * 80}")

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

        # The borrow is structurally doomed (no collateral / WAVAX not enabled as
        # collateral for the eUSDC borrow vault — LTVBorrow == 0 on-chain), so it
        # must fail at compilation OR execution. The VIB-5374 Euler BORROW preflight
        # now catches this at compile time (FAILED + EULER_BORROW_INFEASIBLE) before
        # any gas is burned; on a fork where the preflight read is unavailable it
        # fails open and the EVC borrow reverts at execution. Both are valid; the
        # invariants below (balance conservation + zero accounting rows) hold for
        # both and are the real contract this test guards.
        if compilation_result.status.value != "SUCCESS":
            # Preflight rejected the doomed borrow before building the EVC batch.
            assert compilation_result.action_bundle is None, "rejected compilation must not produce a bundle"
            assert EulerV2Compiler.BORROW_INFEASIBLE_ERROR_PREFIX in str(compilation_result.error), (
                f"compile-time rejection must carry the structured infeasibility prefix; got: {compilation_result.error}"
            )
            print(f"Compilation rejected the doomed borrow as expected: {compilation_result.error}")

            # No on-chain action was taken -> balances are trivially conserved.
            usdc_after = get_token_balance(web3, usdc, funded_wallet)
            wavax_after = get_token_balance(web3, wavax, funded_wallet)
            assert usdc_after == usdc_before, "USDC balance must be unchanged after rejected borrow"
            assert wavax_after == wavax_before, "WAVAX balance must be unchanged after rejected borrow"

            # ── Layer 5: failure-path accounting contract (compile-reject branch) ──
            # A compile-rejected intent never executed, so it must produce zero typed
            # accounting_events rows — the SAME contract the execution-failure branch
            # below asserts. Drive a synthesized failed result (no transactions) through
            # the harness so the invariant is enforced for BOTH valid failure modes.
            rejected_result = ExecutionResult(
                success=False,
                phase=ExecutionPhase.VALIDATION,  # rejected pre-execution; never reached the chain
                error=str(compilation_result.error),
            )
            await assert_no_accounting_on_failure(
                layer5_accounting_harness,
                intent=intent,
                result=rejected_result,
                chain=CHAIN_NAME,
                wallet_address=funded_wallet,
                price_oracle=price_oracle,
                eth_call_reader=anvil_eth_call_adapter,
            )
            print("\nALL CHECKS PASSED")
            return

        # Fail-open path: preflight read unavailable -> compile builds the EVC batch
        # and the borrow reverts on-chain.
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
