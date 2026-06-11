"""Production-grade lending intent tests for Fluid fTokens (ERC-4626) on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for the
Fluid fUSDC vault on Arbitrum (UAT card VIB-5030, D2.M1) — a DISTINCT chain
and a DISTINCT fToken deployment from the Base variant (asserted below, so a
copy-pasted base constant fails loudly):

1. Create lending intents (SupplyIntent, WithdrawIntent) with protocol="fluid"
2. Compile to ActionBundle using IntentCompiler (approve + ERC-4626 ``deposit``
   for supply; ``withdraw`` for exact amounts; full exits route through the
   share-based ``redeem`` path so no dust shares are stranded)
3. Execute via the orchestrator (full production pipeline)
4. Parse receipts using FluidReceiptParser (exact ERC-4626 ``Deposit`` /
   ``Withdraw`` ``assets``)
5. Verify EXACT wallet balance deltas and fToken share balances
6. Layer 5 — persist the real ExecutionResult through the real accounting
   pipeline (ledger -> outbox -> AccountingProcessor.drain_one) into a
   throwaway SQLite and assert the typed LendingAccountingEvent is correct,
   including the canonical position key ``lending:{chain}:fluid:{wallet}:{asset}``
   with NO market segment (VIB-4981 silent-join-miss class).

Fluid's fToken read (``fluid/lending_read.py``) is market-scoped on the
per-underlying fToken and enabled in ``_GENERIC_PRE_STATE_PROTOCOLS``, so the
Anvil ``eth_call`` adapter populates before/after collateral at
``confidence=HIGH``. fTokens have a single supply leg: debt is a measured
zero (Decimal("0"), not None) and ``health_factor`` is None — there is no
liquidation surface on a pure supply (Empty ≠ Zero ≠ None).

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/arbitrum/test_fluid_lending.py -v -s
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.fluid.lending_read import FLUID_FTOKEN_MARKETS
from almanak.connectors.fluid.receipt_parser import FluidReceiptParser
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
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"
PROTOCOL = "fluid"

# The chain's fUSDC ERC-4626 vault — read from the same market table the
# framework reader binds (single source of truth, verified live on-chain).
FTOKEN_ADDRESS = FLUID_FTOKEN_MARKETS[CHAIN_NAME]["usdc"]["comet_address"]


# =============================================================================
# Helper Functions
# =============================================================================


def get_ftoken_share_balance(web3: Web3, wallet: str) -> int:
    """Wallet's fToken share balance (fTokens are ERC-20 vault shares)."""
    return get_token_balance(web3, FTOKEN_ADDRESS, wallet)


def _decoded_4626_amount(execution_result: ExecutionResult, *, supply: bool) -> int:
    """Layer 3: exact ``assets`` decoded from the ERC-4626 event via the parser."""
    parser = FluidReceiptParser(chain=CHAIN_NAME)
    extract = parser.extract_supply_amount if supply else parser.extract_withdraw_amount
    decoded = 0
    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            amount = extract(tx_result.receipt.to_dict())
            if amount is not None:
                decoded += amount
    return decoded


def _assert_targets_ftoken(compilation_result: Any) -> None:
    """The lending transaction targets ARBITRUM's Fluid fUSDC vault.

    D2.M1 — the Arbitrum fUSDC is its own deployment: the compiled target
    must differ from the base constant (a copy-pasted base address, or an
    arbitrum resolution that silently returned base's vault, fails loudly).
    """
    base_ftoken = FLUID_FTOKEN_MARKETS["base"]["usdc"]["comet_address"]
    assert FTOKEN_ADDRESS.lower() != base_ftoken.lower(), (
        "arbitrum tests must target the arbitrum fToken deployment, not base's"
    )
    assert compilation_result.action_bundle.metadata["ftoken"].lower() == FTOKEN_ADDRESS.lower()
    assert compilation_result.action_bundle.metadata["ftoken"].lower() != base_ftoken.lower()
    lending_txs = [tx for tx in compilation_result.transactions if tx.tx_type.startswith("lending_")]
    assert lending_txs, "compiled bundle must contain a lending transaction"
    assert lending_txs[-1].to.lower() == FTOKEN_ADDRESS.lower(), (
        f"lending tx must target the Fluid fUSDC vault {FTOKEN_ADDRESS}, got {lending_txs[-1].to}"
    )


# =============================================================================
# Layer 5 helpers (shared)
# =============================================================================
#
# Mirror the merged Aave V3 golden (``tests/intents/arbitrum/test_aave_v3_lending.py``)
# and the Compound V3 market-scoped variant. ``enrich_result`` makes the ledger
# entry carry extracted_data; ``capture_lending_pre_state`` /
# ``capture_lending_post_state`` dispatch on ``intent.protocol`` and read the
# fToken share balance + share price via the test-scoped Anvil ``eth_call``
# adapter so the lending category handler emits ``confidence=HIGH``.


def _execution_context(wallet: str) -> ExecutionContext:
    return ExecutionContext(
        deployment_id="layer5-fluid-lending",
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
    block: int | None = None,
) -> dict | None:
    """Capture and serialize Fluid fToken pre/post state via the Anvil eth_call adapter.

    Returns the runner-shaped state dict (``lending_state_to_dict`` output) or
    ``None`` when the read genuinely yields nothing — never a fabricated zero.
    """
    # Post-state reads pin to the confirmed receipt block (the production
    # contract — VIB-4589): an unpinned "latest" read can race a later tx.
    capture = capture_lending_post_state if post else capture_lending_pre_state
    kwargs = {"block": block} if (post and block is not None) else {}
    state = capture(
        intent=intent,
        chain=CHAIN_NAME,
        wallet_address=wallet,
        gateway_client=reader,
        price_oracle=price_oracle,
        **kwargs,
    )
    return lending_state_to_dict(state, protocol=PROTOCOL)


def _receipt_block(execution_result: Any) -> int | None:
    """Highest confirmed receipt block in the execution result (pin target)."""
    blocks = [
        tx.receipt.to_dict().get("blockNumber") for tx in execution_result.transaction_results if tx.receipt is not None
    ]
    blocks = [b for b in blocks if isinstance(b, int)]
    return max(blocks) if blocks else None


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


def _assert_position_key(row: dict, wallet: str) -> None:
    """Canonical lending key — NO market segment (VIB-4981 regression class).

    The fToken address travels in the metadata/details for valuation, never
    in the key: Fluid lists exactly one fToken per underlying per chain, so
    wallet + asset uniquely identifies the position.
    """
    expected = f"lending:{CHAIN_NAME}:fluid:{wallet.lower()}:usdc"
    assert row["position_key"] == expected, (
        f"position_key must be the canonical lending shape {expected!r}, got {row['position_key']!r}"
    )
    assert len(row["position_key"].split(":")) == 5, "no market segment when the intent carries no market_id"
    assert FTOKEN_ADDRESS.lower() not in row["position_key"]


def _assert_no_lot_id(row: dict, payload: dict) -> None:
    """Epic decision #6: no lot_id on the persisted lending event."""
    assert "lot_id" not in row
    assert "lot_id" not in payload


def _assert_high_confidence_state(payload: dict) -> None:
    """Fluid has a pre/post-state reader → confidence=HIGH with state populated.

    fTokens are pure-supply ERC-4626 vaults: collateral is the measured share
    balance marked to underlying, debt is a measured zero (Decimal("0"), NOT
    None) and health_factor is None — no liquidation surface exists, so a
    fabricated HF would violate Empty ≠ Zero ≠ None.
    """
    assert payload["confidence"] == "HIGH", (
        f"Fluid lending must persist confidence=HIGH (reader + Anvil eth_call adapter), "
        f"got {payload['confidence']!r} (unavailable_reason={payload.get('unavailable_reason')!r})"
    )
    assert payload["collateral_value_before_usd"] is not None, "before-collateral must be populated"
    assert payload["collateral_value_after_usd"] is not None, "after-collateral must be populated"
    assert payload["debt_value_before_usd"] is not None, "fToken debt is a measured zero — not None"
    assert payload["debt_value_after_usd"] is not None, "fToken debt is a measured zero — not None"
    assert Decimal(payload["debt_value_before_usd"]) == 0, "fToken supply cannot create debt"
    assert Decimal(payload["debt_value_after_usd"]) == 0, "fToken supply cannot create debt"
    assert payload["health_factor_before"] is None, "no liquidation surface — HF must be None, not fabricated"
    assert payload["health_factor_after"] is None, "no liquidation surface — HF must be None, not fabricated"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    """Create ExecutionContext with simulation enabled for accurate gas estimation."""
    return ExecutionContext(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        simulation_enabled=True,
    )


# =============================================================================
# Supply / Withdraw Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.supply
@pytest.mark.lending
class TestFluidSupplyIntent:
    """Fluid fToken supply/withdraw on the Arbitrum fUSDC vault — all 5 layers."""

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
        """Supply 50 USDC into the Fluid fUSDC vault (approve + ERC-4626 deposit)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("50")

        print(f"\n{'=' * 80}")
        print(f"Test: Supply {supply_amount} USDC into Fluid fUSDC ({FTOKEN_ADDRESS}) on {CHAIN_NAME}")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        shares_before = get_ftoken_share_balance(web3, funded_wallet)
        expected_in = int(supply_amount * Decimal(10**decimals))
        assert usdc_before >= expected_in, (
            f"funded_wallet must hold at least {supply_amount} USDC before the supply; "
            f"got {format_token_amount(usdc_before, decimals)} — check the chain conftest's "
            f"wallet-funding fixture (fail fast on infra/setup regressions)."
        )
        print(f"USDC before:          {format_token_amount(usdc_before, decimals)}")
        print(f"fToken shares before: {shares_before}")

        # Layer 1: build + compile
        intent = SupplyIntent(
            protocol="fluid",
            token="USDC",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"
        _assert_targets_ftoken(compilation_result)
        print(f"ActionBundle has {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        # Layer 2: execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful, {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: ERC-4626 Deposit event carries assets EXACTLY == intent amount
        expected_assets = int(supply_amount * Decimal(10**decimals))
        decoded_assets = _decoded_4626_amount(execution_result, supply=True)
        print(f"Decoded Deposit assets: {decoded_assets} (expected exact: {expected_assets})")
        assert decoded_assets == expected_assets, (
            f"ERC-4626 Deposit assets must EXACTLY equal supply amount. "
            f"Expected: {expected_assets}, Got: {decoded_assets}"
        )

        # Layer 4: exact wallet delta + fToken shares minted
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        print(f"USDC spent: {format_token_amount(usdc_spent, decimals)}")
        assert usdc_spent == expected_assets, (
            f"USDC spent must EXACTLY equal supply amount. Expected: {expected_assets}, Got: {usdc_spent}"
        )
        shares_after = get_ftoken_share_balance(web3, funded_wallet)
        print(f"fToken shares after: {shares_after}")
        assert shares_after > shares_before, "wallet must hold fToken shares after supply"

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
        _assert_position_key(row, funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        _assert_high_confidence_state(payload)
        assert payload["asset"] == "USDC"
        assert payload["amount_token"] is not None, "supplied amount must be measured (Empty != Zero)"
        assert Decimal(payload["amount_token"]) == supply_amount
        # SUPPLY drains wallet inventory: principal_delta_usd is measured (the
        # supplied principal in USD); interest is not applicable on SUPPLY.
        assert payload["principal_delta_usd"] is not None, "SUPPLY must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "SUPPLY has no interest leg — must be None, not 0"
        # Supply increases the fToken position (tracked as collateral).
        assert Decimal(payload["collateral_value_after_usd"]) > Decimal(payload["collateral_value_before_usd"])

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
        """Supply 50 USDC, then withdraw 20 exactly (ERC-4626 ``withdraw(assets,...)``)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("50")
        withdraw_amount = Decimal("20")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        # Setup: supply 50 USDC first
        supply_intent = SupplyIntent(protocol="fluid", token="USDC", amount=supply_amount, chain=CHAIN_NAME)
        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS", f"Setup supply failed to compile: {supply_result.error}"
        assert supply_result.action_bundle is not None
        supply_exec_result = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_exec_result.success, f"Setup supply failed: {supply_exec_result.error}"

        print(f"\n{'=' * 80}")
        print(f"Test: Withdraw {withdraw_amount} USDC from Fluid fUSDC on {CHAIN_NAME}")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before withdraw: {format_token_amount(usdc_before, decimals)}")

        # Layer 1: build + compile the withdraw
        intent = WithdrawIntent(
            protocol="fluid",
            token="USDC",
            amount=withdraw_amount,
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None
        _assert_targets_ftoken(compilation_result)

        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        # Layer 2: execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: ERC-4626 Withdraw event carries assets EXACTLY == withdraw amount
        expected_assets = int(withdraw_amount * Decimal(10**decimals))
        decoded_assets = _decoded_4626_amount(execution_result, supply=False)
        print(f"Decoded Withdraw assets: {decoded_assets} (expected exact: {expected_assets})")
        assert decoded_assets == expected_assets, (
            f"ERC-4626 Withdraw assets must EXACTLY equal withdraw amount. "
            f"Expected: {expected_assets}, Got: {decoded_assets}"
        )

        # Layer 4: exact wallet delta
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before
        print(f"USDC received: {format_token_amount(usdc_received, decimals)}")
        assert usdc_received == expected_assets, (
            f"USDC received must EXACTLY equal withdraw amount. Expected: {expected_assets}, Got: {usdc_received}"
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
            expected_event_type="WITHDRAW",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="WITHDRAW", wallet=funded_wallet)
        _assert_position_key(row, funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        _assert_high_confidence_state(payload)
        assert payload["asset"] == "USDC"
        assert payload["amount_token"] is not None, "withdrawn amount must be measured (Empty != Zero)"
        assert Decimal(payload["amount_token"]) == withdraw_amount
        assert payload["principal_delta_usd"] is not None, "WITHDRAW must measure a principal leg"
        assert Decimal(payload["principal_delta_usd"]) > 0
        # Withdraw reduces the fToken position (tracked as collateral).
        assert Decimal(payload["collateral_value_after_usd"]) < Decimal(payload["collateral_value_before_usd"])

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_all_full_exit_zero_shares(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """D2.M2 — ``withdraw_all`` routes through share-based redeem: ZERO stranded shares.

        An assets-based exit path that rounds against the user strands dust
        shares; the full exit must redeem the EXACT share balance and return
        at least the originally supplied USDC minus 1 base-unit rounding.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("50")
        supply_base_units = int(supply_amount * Decimal(10**decimals))

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        # Setup: supply 50 USDC
        supply_intent = SupplyIntent(protocol="fluid", token="USDC", amount=supply_amount, chain=CHAIN_NAME)
        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS", f"Setup supply failed to compile: {supply_result.error}"
        assert supply_result.action_bundle is not None
        supply_exec_result = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_exec_result.success, f"Setup supply failed: {supply_exec_result.error}"

        shares_held = get_ftoken_share_balance(web3, funded_wallet)
        assert shares_held > 0, "setup must leave the wallet holding fToken shares"
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        print(f"\n{'=' * 80}")
        print(f"Test: withdraw_all full exit from Fluid fUSDC on {CHAIN_NAME} ({shares_held} shares)")
        print(f"{'=' * 80}")

        intent = WithdrawIntent(
            protocol="fluid",
            token="USDC",
            amount=Decimal("0"),  # ignored when withdraw_all=True
            withdraw_all=True,
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None
        _assert_targets_ftoken(compilation_result)
        assert compilation_result.action_bundle.metadata["mode"] == "redeem_all_shares", (
            "full exit must route through the share-based redeem path"
        )

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: the redeem emits the same ERC-4626 Withdraw event with exact assets
        decoded_assets = _decoded_4626_amount(execution_result, supply=False)
        assert decoded_assets > 0, "FluidReceiptParser must decode the Withdraw event of the full exit"

        # Layer 4: ZERO stranded shares + full value returned
        shares_after = get_ftoken_share_balance(web3, funded_wallet)
        print(f"fToken shares after full exit: {shares_after}")
        assert shares_after == 0, (
            f"full exit must leave EXACTLY 0 fToken shares (share-based redeem); got {shares_after}"
        )

        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before
        print(f"USDC received on exit: {format_token_amount(usdc_received, decimals)}")
        assert usdc_received == decoded_assets, "wallet delta must match the decoded Withdraw assets exactly"
        assert usdc_received >= supply_base_units - 1, (
            f"full exit must return at least the supplied amount minus 1 base-unit rounding. "
            f"Supplied: {supply_base_units}, Got: {usdc_received}"
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
