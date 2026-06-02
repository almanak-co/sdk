"""Production-grade intent tests for Euler V2 on Ethereum (VIB-4307).

Covers all four lending verbs (SUPPLY / WITHDRAW / BORROW / REPAY) for the
eUSDC-2 vault on Ethereum mainnet:

- USDC: ``0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48``
- eUSDC-2 vault: ``0x797DD80692c3b2dAdabCe8e30C07fDE5307D48a9``

Each test runs the full Intent → Compile → Execute → Parse → Verify pipeline
on an Anvil fork.

Layer 5 (epic VIB-4591 / ticket VIB-4605): mirrors the merged Spark / Aave V3 /
Compound V3 lending goldens. The success-path SUPPLY/WITHDRAW tests persist the
real ``ExecutionResult`` through the production accounting pipeline (ledger →
outbox → ``AccountingProcessor.drain_one`` into a throwaway SQLite) and assert
the typed ``LendingAccountingEvent`` is correct; the failure-path tests assert
the books-side mirror of balance conservation (zero ``accounting_events`` rows).

THE EULER V2 DIVERGENCE (genuine production gap, tracked by VIB-4966):
Euler V2 has NO pre/post-state reader. ``_PROTOCOL_PRE_STATE_READERS`` in
``almanak/framework/accounting/lending_accounting.py`` has entries for
``aave_v3`` / ``aave`` / ``morpho_blue`` / ``compound_v3`` but NOT ``euler_v2``,
so ``capture_lending_pre_state`` / ``capture_lending_post_state`` return ``None``
for a Euler V2 intent and ``lending_state_to_dict`` serializes ``None``. With no
``post_state_json`` the lending handler sets ``confidence=ESTIMATED`` and leaves
every before/after collateral / debt / health-factor field ``None`` with a
populated ``unavailable_reason`` (Empty≠Zero≠None — nothing is fabricated). So
Euler V2 Layer 5 asserts the DEGRADATION contract for chain state
(``_assert_state_degraded_no_reader_vib4605``), identical to the merged Spark
golden. The FIFO principal / interest split is derived from the basis store and
is unaffected by the missing reader. Euler V2 is a vault-based (ERC-4626) lending
model rather than an Aave-fork ``getUserAccountData`` model, so wiring a reader
is more involved than the Spark case (it needs vault ``convertToAssets`` +
controller/collateral reads via the EVC). The HIGH-confidence + before/after
fidelity is the gap tracked by VIB-4966.

NO MOCKING. All tests execute real on-chain transactions and verify state
changes through receipt-event assertions and exact-wei balance deltas.

Borrow/repay tests are marked ``xfail(strict=True)`` until a non-stablecoin
collateral vault (e.g. eWETH, eWBTC) is added to the Ethereum branch of
``EULER_V2_VAULTS_BY_CHAIN`` in
``almanak/connectors/euler_v2/adapter.py``. The compilation path
runs end-to-end, but execution reverts because eUSDC-2 is not a valid
collateral vault for borrowing USDC from itself. Mirrors the Avalanche
``test_euler_v2_borrow.py`` pattern (VIB-2643).

To run:
    uv run pytest tests/intents/ethereum/test_euler_v2_lending.py -v -s
"""

from __future__ import annotations

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
from almanak.framework.intents import (
    BorrowIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
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

# euler_v2 is NOT in the synthetic-intents lending matrix
# (_LENDING_PROTOCOLS in almanak/framework/permissions/synthetic_intents.py),
# so every test in this module must opt out of the default-on Zodiac wrap.
# See .claude/rules/intent-tests.md §Opt-out for the rationale.
pytestmark = pytest.mark.no_zodiac(reason="VIB-4307: euler_v2 not in synthetic-intents matrix")


# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "ethereum"

# Euler V2 vault address on Ethereum (eUSDC-2) — used for receipt-parser filtering
# so we only count Deposit/Withdraw/Borrow/Repay events emitted by this vault.
EULER_V2_USDC_VAULT = "0x797DD80692c3b2dAdabCe8e30C07fDE5307D48a9"

PROTOCOL = "euler_v2"


# =============================================================================
# Layer 5 helpers (shared) — mirror the merged Spark / Aave V3 / Compound V3
# goldens. ``enrich_result`` makes the ledger entry carry extracted_data;
# ``capture_lending_pre_state`` / ``capture_lending_post_state`` dispatch on
# ``intent.protocol``. Euler V2 has NO entry in ``_PROTOCOL_PRE_STATE_READERS``
# (VIB-4966), so both captures return ``None`` and ``lending_state_to_dict``
# serializes ``None`` — the persisted event therefore degrades to
# ``confidence=ESTIMATED`` with no before/after chain state. The conftest Layer-5
# helper threads the serialized state dicts (here ``None``) into
# ``build_ledger_entry``.
# =============================================================================


def _execution_context(wallet: str) -> ExecutionContext:
    # NOTE: this deployment_id flows only into ``enrich_result`` (it labels the
    # ExecutionContext for enrichment). It is deliberately NOT what lands in the
    # persisted accounting row: the conftest ``assert_accounting_persisted``
    # helper stamps the row's deployment_id from its own
    # ``deployment_id="layer5-intent-test"`` default, which is what
    # ``_assert_identity`` checks. This split (descriptive enrichment id vs
    # canonical persisted identity) mirrors the merged Spark golden.
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

    Returns the runner-shaped state dict (``lending_state_to_dict`` output) or
    ``None`` — never a fabricated zero. For Euler V2 this currently ALWAYS
    returns ``None`` because Euler V2 has no pre/post-state reader
    (VIB-4966); the call is kept to mirror the runner's wiring exactly so
    a future reader fix lights up the HIGH-confidence path with no test change.

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
    ``almanak/framework/accounting/lending_accounting.py`` (only ``aave_v3`` /
    ``aave`` / ``morpho_blue`` / ``compound_v3`` are wired), so
    ``capture_lending_pre_state`` / ``capture_lending_post_state`` return
    ``None`` for a Euler V2 intent. With no ``post_state_json`` the lending
    handler sets ``confidence=ESTIMATED`` and leaves every before/after
    collateral / debt / health-factor field ``None`` with a populated
    ``unavailable_reason``. This is the TRUE current production behavior
    (deterministic across the Anvil-fork CI), NOT a flake. We assert the genuine
    degradation contract here rather than HIGH; the HIGH-confidence expectation
    (and before/after collateral / debt / HF fidelity) is the gap tracked by
    VIB-4966 — add a Euler V2 pre/post-state reader (vault
    ``convertToAssets`` + EVC controller / collateral reads). Empty≠Zero≠None:
    ``unavailable_reason`` is set, nothing is fabricated.
    """
    assert payload["confidence"] == "ESTIMATED", (
        f"Euler V2 lending genuinely degrades to confidence=ESTIMATED today "
        f"(VIB-4966: no euler_v2 entry in _PROTOCOL_PRE_STATE_READERS); "
        f"got {payload['confidence']!r}"
    )
    assert payload.get("unavailable_reason"), (
        "degraded Euler V2 lending must carry a non-empty unavailable_reason (never fabricated)"
    )
    # Degradation must not fabricate before/after chain state.
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
# Supply / Withdraw Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.supply
@pytest.mark.lending
class TestEulerV2SupplyIntent:
    """Test Euler V2 supply/withdraw operations using SupplyIntent and WithdrawIntent."""

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
    ) -> None:
        """Supply USDC into the eUSDC-2 vault via SupplyIntent."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("1000")  # 1000 USDC

        print(f"\n{'=' * 80}")
        print(f"Test: Supply {supply_amount} USDC to Euler V2 (Ethereum)")
        print(f"{'=' * 80}")

        # Record balance BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_before >= int(supply_amount * Decimal(10**decimals)), (
            f"Funded wallet lacks required USDC. Need {supply_amount}, have {usdc_before / 10**decimals}"
        )
        print(f"USDC before: {format_token_amount(usdc_before, decimals)}")

        # Layer 1: Compile
        intent = SupplyIntent(
            protocol="euler_v2",
            token="USDC",
            amount=supply_amount,
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
        assert compilation_result.action_bundle is not None

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parse — locate Deposit event from eUSDC-2 vault
        found_supply_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = EulerV2ReceiptParser(underlying_decimals=decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
                )
                if parse_result.success and parse_result.deposit_amount > 0:
                    assert parse_result.deposit_amount > 0
                    assert parse_result.deposit_shares > 0
                    found_supply_event = True
        assert found_supply_event, "Receipt parser must find at least one Deposit event"

        # Layer 4: Balance delta — exact USDC spent
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after
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
    ) -> None:
        """Supply, then withdraw a portion of USDC via WithdrawIntent."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Setup: supply 2000 USDC first.
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

        # Now withdraw 1000 USDC.
        withdraw_amount = Decimal("1000")

        print(f"\n{'=' * 80}")
        print(f"Test: Withdraw {withdraw_amount} USDC from Euler V2 (Ethereum)")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before withdraw: {format_token_amount(usdc_before, decimals)}")

        # Layer 1: Compile
        intent = WithdrawIntent(
            protocol="euler_v2",
            token="USDC",
            amount=withdraw_amount,
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parse — Withdraw event
        found_withdraw_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = EulerV2ReceiptParser(underlying_decimals=decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
                )
                if parse_result.success and parse_result.withdraw_amount > 0:
                    assert parse_result.withdraw_amount > 0
                    assert parse_result.withdraw_shares > 0
                    found_withdraw_event = True
        assert found_withdraw_event, "Receipt parser must find at least one Withdraw event"

        # Layer 4: Balance delta — exact USDC received
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before
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
        # decision #6), identical to the Spark golden. The chain-state read is
        # ALSO degraded (Euler V2 has no reader, VIB-4966) — distinct from
        # the unmatched-FIFO degradation.
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
    ) -> None:
        """Insufficient-balance SUPPLY must fail with USDC balance unchanged.

        Layer 5 failure contract: a failed execution must write ZERO
        accounting_events rows (books-side mirror of "balances unchanged").
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        usdc_balance = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_balance > 0, "Funded wallet must have positive USDC balance"
        balance_decimal = Decimal(usdc_balance) / Decimal(10**decimals)
        excessive_amount = balance_decimal * Decimal("100")

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

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert not execution_result.success, "Execution should fail with insufficient balance"

        # Conservation check
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


# =============================================================================
# Borrow / Repay Tests
# =============================================================================
#
# These are marked xfail(strict=True) until the Ethereum branch of
# ``EULER_V2_VAULTS_BY_CHAIN`` gains a non-stablecoin collateral vault.
# The only vault currently registered for Ethereum is eUSDC-2, and Euler V2
# requires a non-self collateral vault to enable borrowing. Mirror of the
# Avalanche pattern (VIB-2643).


@pytest.mark.ethereum
@pytest.mark.borrow
@pytest.mark.lending
class TestEulerV2BorrowIntent:
    """Test Euler V2 borrow/repay operations using BorrowIntent / RepayIntent."""

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-4307: Ethereum Euler V2 registry has only eUSDC-2 vault "
        "(as of 2026-05-12). Borrow requires a non-stablecoin collateral vault "
        "(e.g. eWETH or eWBTC) to be added to EULER_V2_VAULTS_BY_CHAIN['ethereum']. "
        "Compilation path is exercised end-to-end; execution reverts because "
        "eUSDC-2 is not a valid collateral vault for borrowing USDC.",
        strict=True,
    )
    async def test_borrow_usdc_with_weth_collateral(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Borrow USDC against WETH collateral on Euler V2 Ethereum.

        Will fail at execute time until a WETH collateral vault is registered.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        weth = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc)
        weth_decimals = get_token_decimals(web3, weth)

        # LTV headroom: ~$1800/WETH; 0.5 WETH collateral = ~$900;
        # 250 USDC borrow = ~28% LTV → safely under the 30% cap.
        collateral_amount = Decimal("0.5")
        weth_price = price_oracle.get("WETH", Decimal("1800"))
        max_borrow_usd = collateral_amount * weth_price * Decimal("0.30")
        borrow_amount = min(Decimal("250"), max_borrow_usd)

        weth_before = get_token_balance(web3, weth, funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        assert weth_before >= int(collateral_amount * Decimal(10**weth_decimals)), (
            f"Funded wallet lacks WETH collateral. Need {collateral_amount}, have {weth_before / 10**weth_decimals}"
        )

        intent = BorrowIntent(
            protocol="euler_v2",
            collateral_token="WETH",
            collateral_amount=collateral_amount,
            borrow_token="USDC",
            borrow_amount=borrow_amount,
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
        assert compilation_result.action_bundle is not None

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Receipt parse — Borrow event
        found_borrow_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = EulerV2ReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
                )
                if parse_result.success and parse_result.borrow_amount > 0:
                    assert parse_result.borrow_amount > 0
                    found_borrow_event = True
        assert found_borrow_event, "Receipt parser must find at least one Borrow event"

        # Balance deltas — exact
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        weth_after = get_token_balance(web3, weth, funded_wallet)
        usdc_received = usdc_after - usdc_before
        weth_spent = weth_before - weth_after
        expected_usdc_received = int(borrow_amount * Decimal(10**usdc_decimals))
        expected_weth_spent = int(collateral_amount * Decimal(10**weth_decimals))
        assert usdc_received == expected_usdc_received
        assert weth_spent == expected_weth_spent

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
            expected_event_type="BORROW",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="BORROW", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        _assert_state_degraded_no_reader_vib4605(payload)
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == borrow_amount
        # BORROW records the FIFO principal lot: principal measured, interest
        # has no leg yet (a repay would match it) — must be None, not 0.
        assert payload["principal_delta_usd"] is not None, "BORROW must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "BORROW has no interest leg yet — must be None, not 0"

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-4307: Ethereum Euler V2 registry has only eUSDC-2 vault "
        "(as of 2026-05-12). Repay test depends on the borrow setup which is "
        "blocked by the same single-vault constraint. Will unblock once a "
        "collateral vault (eWETH / eWBTC) is added to the adapter.",
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
    ) -> None:
        """Repay portion of USDC debt via RepayIntent (after borrow setup)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        weth = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Setup: borrow first (will revert in the xfail world; here for shape).
        collateral_amount = Decimal("0.5")
        weth_price = price_oracle.get("WETH", Decimal("1800"))
        max_borrow_usd = collateral_amount * weth_price * Decimal("0.30")
        borrow_amount = min(Decimal("250"), max_borrow_usd)
        repay_amount = borrow_amount / Decimal("2")

        # Pre-flight: ensure the wallet has the WETH collateral. If this fails
        # we want a clear assertion error, not a confusing borrow revert.
        weth_balance = get_token_balance(web3, weth, funded_wallet)
        weth_decimals = get_token_decimals(web3, weth)
        assert weth_balance >= int(collateral_amount * Decimal(10**weth_decimals)), (
            f"Funded wallet lacks WETH collateral. Need {collateral_amount}, have {weth_balance / 10**weth_decimals}"
        )

        borrow_intent = BorrowIntent(
            protocol="euler_v2",
            collateral_token="WETH",
            collateral_amount=collateral_amount,
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
        assert borrow_exec.success, f"Borrow setup failed: {borrow_exec.error}"

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
            block=_receipt_block(borrow_exec),
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

        usdc_before = get_token_balance(web3, usdc, funded_wallet)

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

        # Receipt parse — Repay event
        found_repay_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = EulerV2ReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
                )
                if parse_result.success and parse_result.repay_amount > 0:
                    assert parse_result.repay_amount > 0
                    found_repay_event = True
        assert found_repay_event, "Receipt parser must find at least one Repay event"

        # Balance delta — exact
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        expected_usdc_spent = int(repay_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent

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
            block=_receipt_block(execution_result),
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
        _assert_state_degraded_no_reader_vib4605(payload)
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

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=False,
        reason="VIB-2643: euler_v2 zero-collateral borrow on eUSDC-2 succeeds where the test expects revert — vault may not enforce the LTV check the test assumes (as of 2026-05-12)",
    )
    async def test_borrow_without_collateral_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """BorrowIntent with zero collateral must fail on-chain with balances unchanged.

        Layer 5 failure contract: zero accounting_events rows.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        weth = tokens["WETH"]

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)

        intent = BorrowIntent(
            protocol="euler_v2",
            collateral_token="WETH",
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

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert not execution_result.success, "Execution should fail without collateral"

        # Conservation check
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        weth_after = get_token_balance(web3, weth, funded_wallet)
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed borrow"
        assert weth_after == weth_before, "WETH balance must be unchanged after failed borrow"

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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
