"""Production-grade borrow/repay intent tests for Silo V2 on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for Silo V2 borrow operations:
1. Create BorrowIntent / RepayIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using SiloV2ReceiptParser
5. Verify balance changes are correct
6. Layer 5 — persist the real ExecutionResult through the real accounting
   pipeline (ledger -> outbox -> AccountingProcessor.drain_one) into a
   throwaway SQLite and assert the typed LendingAccountingEvent is correct.

NOTE: The borrow/repay happy-path tests are currently skipped when the
WAVAX/USDC Silo V2 market on Avalanche has zero borrowable USDC at the mainnet
fork block. Silo V2's isolated architecture means there is no USDC to borrow
until other users deposit USDC into the silo. The tests are correctly
structured (including the Layer-5 assertions, which run only when liquidity is
present) and will pass once the market has USDC liquidity.

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
reader — Silo V2's isolated ERC-4626 silos have no Aave-style
``getUserAccountData``, so the reader is bespoke per-silo).

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/avalanche/test_silo_v2_borrow.py -v -s
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

pytestmark = pytest.mark.no_zodiac(reason="silo_v2 connector not in manifest matrix")

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "avalanche"
PROTOCOL = "silo_v2"

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
    # See the supply-file twin for the deployment_id split rationale: this id
    # labels enrichment only; the persisted row's identity is stamped from the
    # conftest helper's ``deployment_id="layer5-intent-test"`` default.
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
    ``unavailable_reason``. This is the TRUE current production behavior, NOT a
    flake; the HIGH-confidence expectation is the gap tracked by
    VIB-4965. Empty≠Zero≠None: ``unavailable_reason`` is set, nothing is
    fabricated.
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
    - Layer 5: the real accounting pipeline persists a correct
      LendingAccountingEvent (degraded chain state per VIB-4965, FIFO split)

    Silo V2 is isolated lending — depositing WAVAX into the WAVAX silo
    enables borrowing USDC from the paired USDC silo.
    """

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW)
    @pytest.mark.asyncio
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
        """Test USDC borrow with WAVAX collateral using BorrowIntent.

        Flow:
        1. Supply WAVAX as collateral to the WAVAX/USDC market
        2. Create BorrowIntent for USDC on Silo V2
        3. Compile to ActionBundle using IntentCompiler
        4. Execute via ExecutionOrchestrator
        5. Parse receipt for Borrow event
        6. Verify USDC balance increased by exact borrow amount
        7. Layer 5: assert persisted BORROW accounting event
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

        print(f"\n{'=' * 80}")
        print(f"Test: Supply {supply_amount} WAVAX, then Borrow {borrow_amount} USDC from Silo V2")
        print(f"{'=' * 80}")

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

        print(
            f"\nCreated BorrowIntent: protocol={intent.protocol}, borrow_token={intent.borrow_token}, borrow_amount={intent.borrow_amount}"
        )

        # Layer 1: Compilation
        print("Compiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        # Layer 2: Execution
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Receipt Parsing
        found_borrow_event = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
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
            f"USDC received must EXACTLY equal borrow amount. Expected: {expected_usdc_received}, Got: {usdc_received}"
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
            expected_event_type="BORROW",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="BORROW", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        _assert_state_degraded_no_reader(payload)
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
        """Test USDC repay after borrowing using RepayIntent.

        Flow:
        1. Supply WAVAX + borrow USDC (setup; the BORROW is persisted through
           the Layer-5 harness so the FIFO basis pool holds the matching lot)
        2. Create RepayIntent to repay 0.5 USDC
        3. Compile and execute
        4. Parse receipt for Repay event
        5. Verify USDC balance decreased by exact repay amount
        6. Layer 5: assert the EXACT principal/interest FIFO split
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        # Skip if the silo doesn't have enough borrowable USDC for the 1 USDC
        # setup-borrow required by this repay test.
        setup_borrow = Decimal("1")
        setup_borrow_wei = int(setup_borrow * Decimal(10**usdc_decimals))
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

        # Layer 5: persist the BORROW so the FIFO basis pool holds the lot the
        # REPAY will match against (this is what makes the split exact).
        borrow_enriched = _enrich_for_accounting(
            borrow_exec, borrow_intent, funded_wallet, borrow_result.action_bundle.metadata
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

        # Now repay 0.5 USDC (half of borrowed amount)
        repay_amount = Decimal("0.5")

        print(f"\n{'=' * 80}")
        print(f"Test: Repay {repay_amount} USDC to Silo V2 using RepayIntent")
        print(f"{'=' * 80}")

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

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

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
            f"USDC spent must EXACTLY equal repay amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

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
        _assert_state_degraded_no_reader(payload)
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

        Attempting to borrow USDC without supplying collateral first should fail.
        Balance must be unchanged after the failed attempt.

        Layer 5 failure contract: zero accounting_events rows.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        print(f"\n{'=' * 80}")
        print("Test: BorrowIntent without Collateral (Silo V2)")
        print(f"{'=' * 80}")

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
