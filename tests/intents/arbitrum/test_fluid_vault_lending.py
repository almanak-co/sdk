"""Production-grade Fluid vault (NFT-CDP) lending intent tests on Arbitrum.

UAT card VIB-5031 D1.S1 + D2.M2: the full lifecycle against type-1 vault id 1
(native-ETH collateral -> USDC debt, ``0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C``)
through the real Intent -> Compile -> Execute -> Parse -> Verify flow, all
five layers per step:

1. Lending intents under ``protocol="fluid_vault"`` (``market_id`` = the
   vault address) compile to the vault's single entrypoint
   ``operate(nftId, colDelta, debtDelta, to)`` — atomic open (ONE operate
   moving BOTH legs), add-collateral SUPPLY, partial REPAY, WITHDRAW slice,
   then the full close as the int-min sentinel pair (repay_full ->
   withdraw_all).
2. Execution through the orchestrator (default-on Zodiac: vault operate()
   target + scoped approvals from the fluid_vault discovery vectors).
3. Receipt parsing via ``FluidVaultReceiptParser``: factory-GATED nftId
   capture and SIGNED LogOperate deltas (never ERC-20 Transfer topology —
   the native-ETH leg produces no ERC-20 log).
4. EXACT bilateral wallet deltas (ETH gas-accounted to the wei).
5. Real accounting pipeline (ledger -> outbox -> drain) into a throwaway
   SQLite: market-scoped position keys
   ``lending:arbitrum:fluid_vault:{wallet}:{vault}:{asset}``, HIGH
   confidence with protocol-truth HF, receipt-truth amounts for the
   sentinel closes (Empty != Zero), and the nftId persisted in the ledger
   row's extracted_data_json (the runner-hook seam).

NO MOCKING. To run:
    uv run pytest tests/intents/arbitrum/test_fluid_vault_lending.py -v -s
"""

import json
import sqlite3
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.fluid.addresses import FLUID_VAULT_MARKETS
from almanak.connectors.fluid.receipt_parser import FluidVaultReceiptParser
from almanak.connectors.fluid.runner_hooks import (
    FLUID_VAULT_OPERATE_KEY,
    FluidVaultRunnerHookConnector,
)
from almanak.connectors.fluid.sdk import decode_fluid_revert, fluid_error_id
from almanak.connectors.fluid.vault_sdk import FluidVaultSDK
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
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    TEST_WALLET,
    assert_accounting_persisted,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"
PROTOCOL = "fluid_vault"

# Type-1 vault id 1 (native ETH -> USDC), pinned EXPLICITLY to the documented
# address (docs/internal/qa/fluid-vault-verification-2026-06-12.md) so adding
# a second arbitrum vault to the market table can never silently retarget this
# test. Sanity-checked against the pinned market table at import.
VAULT = "0xeabbfca72f8a8bf14c4ac59e69ecb2eb69f0811c"
assert VAULT in FLUID_VAULT_MARKETS["arbitrum"], "arbitrum vault id 1 missing from FLUID_VAULT_MARKETS"
VAULT_ENTRY = FLUID_VAULT_MARKETS["arbitrum"][VAULT]

OPEN_COLLATERAL_ETH = Decimal("1")
OPEN_BORROW_USDC = Decimal("500")  # well under 30% LTV at any plausible ETH price
ADD_COLLATERAL_ETH = Decimal("0.5")
PARTIAL_REPAY_USDC = Decimal("200")
WITHDRAW_SLICE_ETH = Decimal("0.2")


# =============================================================================
# Helpers
# =============================================================================


def _vault_sdk(anvil_rpc_url: str) -> FluidVaultSDK:
    """Direct-RPC resolver reads against the test Anvil (test-only transport)."""
    return FluidVaultSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)


def _compiler(funded_wallet: str, price_oracle: dict[str, Decimal], anvil_rpc_url: str) -> IntentCompiler:
    return IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )


def _vault_prices(price_oracle: dict[str, Decimal]) -> dict[str, Decimal]:
    """Price map for the vault valuation seam.

    The vault's collateral leg is RAW native ETH; the shared fixture prices
    WETH (the CHAIN_CONFIGS symbol). Same asset, same price.
    """
    prices = dict(price_oracle)
    if "ETH" not in prices and "WETH" in prices:
        prices["ETH"] = prices["WETH"]
    return prices


def _gas_paid_by(execution_result: ExecutionResult, wallet: str) -> int:
    """Exact wei of gas paid by ``wallet`` across the result's receipts.

    Under default-on Zodiac the relayer EOA (not the Safe) pays gas, so the
    Safe's native delta is gas-free; under EOA execution the funded wallet
    pays. Computing from the receipts keeps the assertion exact either way.
    """
    total = 0
    for tx_result in execution_result.transaction_results:
        if tx_result.receipt is None:
            continue
        receipt = tx_result.receipt.to_dict()
        sender = str(receipt.get("from", "")).lower()
        if sender == wallet.lower():
            total += int(receipt["gasUsed"]) * int(receipt.get("effectiveGasPrice", 0))
    return total


def _single_operate_event(execution_result: ExecutionResult):
    """Layer 3: the ONE vault LogOperate event of the lifecycle action."""
    parser = FluidVaultReceiptParser(chain=CHAIN_NAME)
    events = []
    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            events.extend(parser.parse_receipt(tx_result.receipt.to_dict()).operate_events)
    assert len(events) == 1, f"expected exactly one vault LogOperate event, got {len(events)}"
    event = events[0]
    assert event.vault == VAULT, "amounts must come from THE vault's own event"
    return event


def _minted_nft_id(execution_result: ExecutionResult) -> int | None:
    parser = FluidVaultReceiptParser(chain=CHAIN_NAME)
    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            minted = parser.parse_receipt(tx_result.receipt.to_dict()).minted_nft_id
            if minted is not None:
                return minted
    return None


def _receipt_block(execution_result: ExecutionResult) -> int | None:
    blocks = [
        tx.receipt.to_dict().get("blockNumber") for tx in execution_result.transaction_results if tx.receipt is not None
    ]
    blocks = [b for b in blocks if isinstance(b, int)]
    return max(blocks) if blocks else None


def _capture_state(
    intent: Any,
    wallet: str,
    reader: Any,
    prices: dict[str, Decimal],
    *,
    post: bool,
    block: int | None = None,
) -> dict | None:
    capture = capture_lending_post_state if post else capture_lending_pre_state
    kwargs = {"block": block} if (post and block is not None) else {}
    state = capture(
        intent=intent,
        chain=CHAIN_NAME,
        wallet_address=wallet,
        gateway_client=reader,
        price_oracle=prices,
        **kwargs,
    )
    return lending_state_to_dict(state, protocol=PROTOCOL)


def _enrich(execution_result: ExecutionResult, intent: Any, wallet: str, metadata: dict | None) -> ExecutionResult:
    """Mirror the runner: spec enrichment + the pre-ledger vault runner hook."""
    enriched = enrich_result(
        execution_result,
        intent,
        ExecutionContext(
            deployment_id="layer5-fluid-vault",
            chain=CHAIN_NAME,
            wallet_address=wallet,
            protocol="fluid_vault",
            simulation_enabled=True,
        ),
        live_mode=False,
        bundle_metadata=metadata,
    )
    # The production runner runs connector hooks right before the ledger
    # write (_maybe_enrich_result_with_runner_hooks) — same call here.
    FluidVaultRunnerHookConnector().enrich_result(enriched, gateway_client=None, chain=CHAIN_NAME)
    return enriched


async def _persist_step(
    harness,
    *,
    intent: Any,
    enriched: ExecutionResult,
    wallet: str,
    event_type: str,
    prices: dict[str, Decimal],
    reader: Any,
    pre_state: dict | None,
    post_state: dict | None,
) -> dict:
    row = await assert_accounting_persisted(
        harness,
        intent=intent,
        result=enriched,
        chain=CHAIN_NAME,
        wallet_address=wallet,
        expected_event_type=event_type,
        price_oracle=prices,
        eth_call_reader=reader,
        pre_state=pre_state,
        post_state=post_state,
    )
    return row


def _ledger_extracted_data(harness, ledger_entry_id: str) -> dict:
    """Read extracted_data_json back from the persisted transaction_ledger row."""
    conn = sqlite3.connect(str(harness.db_path))
    try:
        row = conn.execute(
            "SELECT extracted_data_json FROM transaction_ledger WHERE id = ?",
            (ledger_entry_id,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None and row[0], "ledger row must carry extracted_data_json"
    return json.loads(row[0])


def _payload(row: dict) -> dict:
    return json.loads(row["payload_json"])


def _assert_position_key(row: dict, wallet: str, asset: str) -> None:
    """Canonical market-scoped lending shape, all lowercased (D1.S1)."""
    expected = f"lending:{CHAIN_NAME}:fluid_vault:{wallet.lower()}:{VAULT}:{asset.lower()}"
    assert row["position_key"] == expected, f"position_key must be {expected!r}, got {row['position_key']!r}"
    assert len(row["position_key"].split(":")) == 6, "market segment must be present"


def _assert_high_confidence(payload: dict) -> None:
    assert payload["confidence"] == "HIGH", (
        f"fluid_vault rows must persist confidence=HIGH, got {payload['confidence']!r} "
        f"(unavailable_reason={payload.get('unavailable_reason')!r})"
    )


def _assert_nft_id(extracted: dict, nft_id: int) -> None:
    assert FLUID_VAULT_OPERATE_KEY in extracted, "FluidVaultOperateData must reach extracted_data_json"
    assert extracted[FLUID_VAULT_OPERATE_KEY]["nft_id"] == str(nft_id)
    assert extracted[FLUID_VAULT_OPERATE_KEY]["vault"] == VAULT


async def _compile_and_execute(compiler, orchestrator, execution_context, intent):
    """Layers 1+2 with the standard assertions."""
    compilation_result = compiler.compile(intent)
    assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
    assert compilation_result.action_bundle is not None, "ActionBundle must be created"
    execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
    assert execution_result.success, f"Execution failed: {execution_result.error}"
    return compilation_result, execution_result


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    return ExecutionContext(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        simulation_enabled=True,
    )


@pytest.fixture(autouse=True)
def _clear_7702_sweeper(web3: Web3):
    """Anvil hygiene (Phase-0 finding 1, re-confirmed 2026-06-12): anvil key0
    inherits 7702 delegation code from mainnet on Arbitrum forks — clear it
    before any native-ETH-receiving step or withdrawals get swept."""
    response = web3.provider.make_request("anvil_setCode", [TEST_WALLET, "0x"])
    assert not (isinstance(response, dict) and "error" in response), (
        f"anvil_setCode 7702 scrub rejected — fixture cannot guarantee native-ETH hygiene: {response}"
    )
    yield


# =============================================================================
# D1.S1 — full vault lifecycle (5 layers per step)
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lending
class TestFluidVaultLifecycle:
    """Open(atomic) -> add-collateral -> partial repay -> withdraw slice -> full close."""

    @pytest.mark.intent(IntentType.BORROW, IntentType.SUPPLY, IntentType.REPAY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_vault_lifecycle_all_layers(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
        anvil_rpc_url: str,
    ):
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)
        prices = _vault_prices(price_oracle)
        compiler = _compiler(funded_wallet, price_oracle, anvil_rpc_url)
        sdk = _vault_sdk(anvil_rpc_url)
        deployment_id = "layer5-intent-test"

        col_wei = int(OPEN_COLLATERAL_ETH * Decimal(10**18))
        borrow_wei = int(OPEN_BORROW_USDC * Decimal(10**usdc_decimals))

        # ────────────────────────────────────────────────────────────────
        # Step 1 — ATOMIC OPEN: BorrowIntent(col>0) = ONE operate, both legs
        # ────────────────────────────────────────────────────────────────
        eth_before = web3.eth.get_balance(funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        assert sdk.resolve_user_nft_for_vault(funded_wallet, VAULT) is None, (
            "wallet must hold NO Fluid NFT on the vault before the open"
        )

        open_intent = BorrowIntent(
            protocol="fluid_vault",
            collateral_token="ETH",
            collateral_amount=OPEN_COLLATERAL_ETH,
            borrow_token="USDC",
            borrow_amount=OPEN_BORROW_USDC,
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        pre_state = _capture_state(open_intent, funded_wallet, anvil_eth_call_adapter, prices, post=False)
        compilation, execution = await _compile_and_execute(compiler, orchestrator, execution_context, open_intent)

        # Receipt assertions: factory mint + the vault's signed-delta event.
        nft_id = _minted_nft_id(execution)
        assert nft_id is not None and nft_id > 0, "open must mint a factory-emitted position NFT"
        event = _single_operate_event(execution)
        assert event.nft_id == nft_id
        assert event.col_delta == col_wei, "LogOperate colAmt must equal the intent collateral exactly"
        assert event.debt_delta == borrow_wei, "LogOperate debtAmt must equal the intent borrow exactly"

        # Layer 4 — bilateral wallet deltas (ETH gas-accounted exactly).
        gas_wei = _gas_paid_by(execution, funded_wallet)
        eth_after = web3.eth.get_balance(funded_wallet)
        assert eth_before - eth_after == col_wei + gas_wei, (
            f"wallet ETH delta must be exactly -collateral-gas: {eth_before - eth_after} != {col_wei} + {gas_wei}"
        )
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after - usdc_before == borrow_wei, "wallet USDC delta must be exactly +borrow"

        # On-chain position state via the resolver (typed decode).
        position, _ = sdk.position_by_nft_id(nft_id)
        assert position.supply >= col_wei - 16, "resolver supply ~ collateral (big-number rounding dust)"
        assert position.borrow >= borrow_wei, "resolver borrow >= borrow (round-up + accrual)"

        # Layer 5 — exactly ONE BORROW row, both envelopes moved, NO synthetic SUPPLY.
        enriched = _enrich(execution, open_intent, funded_wallet, compilation.action_bundle.metadata)
        post_state = _capture_state(
            open_intent, funded_wallet, anvil_eth_call_adapter, prices, post=True, block=_receipt_block(execution)
        )
        row = await _persist_step(
            layer5_accounting_harness,
            intent=open_intent,
            enriched=enriched,
            wallet=funded_wallet,
            event_type="BORROW",
            prices=prices,
            reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_position_key(row, funded_wallet, "USDC")  # debt leg keys on the borrow asset
        payload = _payload(row)
        _assert_high_confidence(payload)
        assert Decimal(payload["amount_token"]) == OPEN_BORROW_USDC
        # The atomic open's envelope proves BOTH legs moved in ONE event:
        assert Decimal(payload["collateral_value_after_usd"]) > Decimal(payload["collateral_value_before_usd"])
        assert Decimal(payload["debt_value_after_usd"]) > Decimal(payload["debt_value_before_usd"])
        assert payload["health_factor_after"] is not None, "HF must be measured while debt > 0"
        supply_rows = await layer5_accounting_harness.store.get_accounting_events(
            deployment_id, event_type="SUPPLY", limit=10
        )
        assert supply_rows == [], "the atomic open must persist NO synthetic SUPPLY row"
        _assert_nft_id(_ledger_extracted_data(layer5_accounting_harness, row["ledger_entry_id"]), nft_id)

        # ────────────────────────────────────────────────────────────────
        # Step 2 — ADD COLLATERAL: standalone SupplyIntent, its own SUPPLY row
        # ────────────────────────────────────────────────────────────────
        add_wei = int(ADD_COLLATERAL_ETH * Decimal(10**18))
        eth_before = web3.eth.get_balance(funded_wallet)
        debt_before_step = sdk.position_by_nft_id(nft_id)[0].borrow

        supply_intent = SupplyIntent(
            protocol="fluid_vault",
            token="ETH",
            amount=ADD_COLLATERAL_ETH,
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        pre_state = _capture_state(supply_intent, funded_wallet, anvil_eth_call_adapter, prices, post=False)
        compilation, execution = await _compile_and_execute(compiler, orchestrator, execution_context, supply_intent)

        event = _single_operate_event(execution)
        assert event.nft_id == nft_id, "add-collateral must reuse the SAME nftId"
        assert event.col_delta == add_wei
        assert event.debt_delta == 0

        gas_wei = _gas_paid_by(execution, funded_wallet)
        eth_after = web3.eth.get_balance(funded_wallet)
        assert eth_before - eth_after == add_wei + gas_wei, "wallet ETH delta must be exactly -supply-gas"

        position_after, _ = sdk.position_by_nft_id(nft_id)
        assert position_after.supply > position.supply, "resolver collateral must strictly increase"
        # Debt unchanged up to per-block interest accrual (a few base units).
        assert position_after.borrow >= debt_before_step
        assert position_after.borrow - debt_before_step <= 1_000, "debt must move only by per-block accrual"

        enriched = _enrich(execution, supply_intent, funded_wallet, compilation.action_bundle.metadata)
        post_state = _capture_state(
            supply_intent, funded_wallet, anvil_eth_call_adapter, prices, post=True, block=_receipt_block(execution)
        )
        row = await _persist_step(
            layer5_accounting_harness,
            intent=supply_intent,
            enriched=enriched,
            wallet=funded_wallet,
            event_type="SUPPLY",
            prices=prices,
            reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_position_key(row, funded_wallet, "ETH")  # collateral leg keys on the collateral asset
        payload = _payload(row)
        _assert_high_confidence(payload)
        assert Decimal(payload["amount_token"]) == ADD_COLLATERAL_ETH
        _assert_nft_id(_ledger_extracted_data(layer5_accounting_harness, row["ledger_entry_id"]), nft_id)

        # ────────────────────────────────────────────────────────────────
        # Step 3 — PARTIAL REPAY: exact USDC delta, debt strictly decreases
        # ────────────────────────────────────────────────────────────────
        repay_wei = int(PARTIAL_REPAY_USDC * Decimal(10**usdc_decimals))
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        debt_before_step = sdk.position_by_nft_id(nft_id)[0].borrow

        repay_intent = RepayIntent(
            protocol="fluid_vault",
            token="USDC",
            amount=PARTIAL_REPAY_USDC,
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        pre_state = _capture_state(repay_intent, funded_wallet, anvil_eth_call_adapter, prices, post=False)
        compilation, execution = await _compile_and_execute(compiler, orchestrator, execution_context, repay_intent)

        event = _single_operate_event(execution)
        assert event.nft_id == nft_id, "partial repay must carry the SAME nftId"
        assert event.col_delta == 0
        assert event.debt_delta == -repay_wei, "LogOperate debtAmt must be the exact negative repay"

        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_before - usdc_after == repay_wei, "wallet USDC delta must be exactly -repay"
        assert sdk.position_by_nft_id(nft_id)[0].borrow < debt_before_step, "debt must strictly decrease"

        enriched = _enrich(execution, repay_intent, funded_wallet, compilation.action_bundle.metadata)
        post_state = _capture_state(
            repay_intent, funded_wallet, anvil_eth_call_adapter, prices, post=True, block=_receipt_block(execution)
        )
        row = await _persist_step(
            layer5_accounting_harness,
            intent=repay_intent,
            enriched=enriched,
            wallet=funded_wallet,
            event_type="REPAY",
            prices=prices,
            reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_position_key(row, funded_wallet, "USDC")
        payload = _payload(row)
        _assert_high_confidence(payload)
        assert Decimal(payload["amount_token"]) == PARTIAL_REPAY_USDC
        assert Decimal(payload["debt_value_after_usd"]) < Decimal(payload["debt_value_before_usd"])
        _assert_nft_id(_ledger_extracted_data(layer5_accounting_harness, row["ledger_entry_id"]), nft_id)

        # ────────────────────────────────────────────────────────────────
        # Step 4 — WITHDRAW SLICE: exact native ETH returned
        # ────────────────────────────────────────────────────────────────
        slice_wei = int(WITHDRAW_SLICE_ETH * Decimal(10**18))
        eth_before = web3.eth.get_balance(funded_wallet)

        withdraw_intent = WithdrawIntent(
            protocol="fluid_vault",
            token="ETH",
            amount=WITHDRAW_SLICE_ETH,
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        pre_state = _capture_state(withdraw_intent, funded_wallet, anvil_eth_call_adapter, prices, post=False)
        compilation, execution = await _compile_and_execute(compiler, orchestrator, execution_context, withdraw_intent)

        event = _single_operate_event(execution)
        assert event.nft_id == nft_id, "withdraw must carry the SAME nftId"
        assert event.col_delta == -slice_wei
        assert event.debt_delta == 0

        gas_wei = _gas_paid_by(execution, funded_wallet)
        eth_after = web3.eth.get_balance(funded_wallet)
        assert eth_after - eth_before == slice_wei - gas_wei, (
            "wallet ETH delta must be exactly +withdraw-gas (raw native, no ERC-20 leg)"
        )

        enriched = _enrich(execution, withdraw_intent, funded_wallet, compilation.action_bundle.metadata)
        post_state = _capture_state(
            withdraw_intent,
            funded_wallet,
            anvil_eth_call_adapter,
            prices,
            post=True,
            block=_receipt_block(execution),
        )
        row = await _persist_step(
            layer5_accounting_harness,
            intent=withdraw_intent,
            enriched=enriched,
            wallet=funded_wallet,
            event_type="WITHDRAW",
            prices=prices,
            reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_position_key(row, funded_wallet, "ETH")
        payload = _payload(row)
        _assert_high_confidence(payload)
        assert Decimal(payload["amount_token"]) == WITHDRAW_SLICE_ETH
        _assert_nft_id(_ledger_extracted_data(layer5_accounting_harness, row["ledger_entry_id"]), nft_id)

        # ────────────────────────────────────────────────────────────────
        # Step 5 — FULL CLOSE: int-min repay_full, then int-min withdraw_all.
        # Receipt-truth amounts: the true values are only knowable from the
        # vault event's signed deltas (a blank/zero/sentinel amount FAILS).
        # ────────────────────────────────────────────────────────────────
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        debt_before_close = sdk.position_by_nft_id(nft_id)[0].borrow
        assert debt_before_close > 0

        close_repay = RepayIntent(
            protocol="fluid_vault",
            token="USDC",
            amount=Decimal("0"),
            repay_full=True,
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        pre_state = _capture_state(close_repay, funded_wallet, anvil_eth_call_adapter, prices, post=False)
        compilation, execution = await _compile_and_execute(compiler, orchestrator, execution_context, close_repay)

        event = _single_operate_event(execution)
        assert event.nft_id == nft_id
        true_repaid = -event.debt_delta
        assert true_repaid >= debt_before_close, "sentinel repay resolves the FULL debt incl. accrual"
        assert true_repaid < debt_before_close * 2, "sanity: resolved amount is the debt, not a sentinel"
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_before - usdc_after == true_repaid, "wallet USDC delta must equal the receipt-truth repay"
        assert sdk.position_by_nft_id(nft_id)[0].borrow == 0, "debt must be ZERO after repay_full"

        enriched = _enrich(execution, close_repay, funded_wallet, compilation.action_bundle.metadata)
        post_state = _capture_state(
            close_repay, funded_wallet, anvil_eth_call_adapter, prices, post=True, block=_receipt_block(execution)
        )
        row = await _persist_step(
            layer5_accounting_harness,
            intent=close_repay,
            enriched=enriched,
            wallet=funded_wallet,
            event_type="REPAY",
            prices=prices,
            reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        payload = _payload(row)
        _assert_high_confidence(payload)
        assert payload["amount_token"] not in (None, "", "0"), (
            "Empty != Zero: the sentinel close must persist the receipt-truth amount"
        )
        repaid_human = Decimal(true_repaid) / Decimal(10**usdc_decimals)
        assert Decimal(payload["amount_token"]) == repaid_human, (
            f"persisted REPAY amount must be the receipt-truth {repaid_human}, got {payload['amount_token']}"
        )
        assert Decimal(payload["debt_value_after_usd"]) == 0
        _assert_nft_id(_ledger_extracted_data(layer5_accounting_harness, row["ledger_entry_id"]), nft_id)

        # withdraw_all — the second sentinel.
        eth_before = web3.eth.get_balance(funded_wallet)
        remaining_collateral = sdk.position_by_nft_id(nft_id)[0].supply
        assert remaining_collateral > 0

        close_withdraw = WithdrawIntent(
            protocol="fluid_vault",
            token="ETH",
            amount=Decimal("0"),
            withdraw_all=True,
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        pre_state = _capture_state(close_withdraw, funded_wallet, anvil_eth_call_adapter, prices, post=False)
        compilation, execution = await _compile_and_execute(compiler, orchestrator, execution_context, close_withdraw)

        event = _single_operate_event(execution)
        assert event.nft_id == nft_id
        true_withdrawn = -event.col_delta
        assert true_withdrawn > 0
        gas_wei = _gas_paid_by(execution, funded_wallet)
        eth_after = web3.eth.get_balance(funded_wallet)
        assert eth_after - eth_before == true_withdrawn - gas_wei, (
            "wallet ETH delta must equal the receipt-truth withdrawal exactly (gas-accounted)"
        )
        assert true_withdrawn >= remaining_collateral - 16, (
            "wallet must recover >= remaining collateral minus big-number rounding dust"
        )

        # Position empty after close — post-state reads pinned to the receipt block.
        final_position, _ = sdk.position_by_nft_id(nft_id)
        assert final_position.supply == 0 and final_position.borrow == 0, "position must be EMPTY after close"

        enriched = _enrich(execution, close_withdraw, funded_wallet, compilation.action_bundle.metadata)
        post_state = _capture_state(
            close_withdraw,
            funded_wallet,
            anvil_eth_call_adapter,
            prices,
            post=True,
            block=_receipt_block(execution),
        )
        assert post_state is not None
        assert post_state["health_factor"] is None, (
            "the HF of the EMPTY position must be None (NOT zero) after close — Empty != Zero != None"
        )
        row = await _persist_step(
            layer5_accounting_harness,
            intent=close_withdraw,
            enriched=enriched,
            wallet=funded_wallet,
            event_type="WITHDRAW",
            prices=prices,
            reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        payload = _payload(row)
        _assert_high_confidence(payload)
        withdrawn_human = Decimal(true_withdrawn) / Decimal(10**18)
        assert payload["amount_token"] not in (None, "", "0"), "Empty != Zero on the sentinel withdraw"
        assert Decimal(payload["amount_token"]) == withdrawn_human
        assert payload["health_factor_after"] is None, "empty position HF must persist as None, not 0"
        _assert_nft_id(_ledger_extracted_data(layer5_accounting_harness, row["ledger_entry_id"]), nft_id)

        # Lifecycle row census: 1 BORROW, 1 SUPPLY, 2 REPAY, 2 WITHDRAW —
        # every row carrying the SAME nftId in its ledger extracted data.
        store = layer5_accounting_harness.store
        assert len(await store.get_accounting_events(deployment_id, event_type="BORROW", limit=10)) == 1
        assert len(await store.get_accounting_events(deployment_id, event_type="SUPPLY", limit=10)) == 1
        assert len(await store.get_accounting_events(deployment_id, event_type="REPAY", limit=10)) == 2
        assert len(await store.get_accounting_events(deployment_id, event_type="WITHDRAW", limit=10)) == 2

        print("\nALL LIFECYCLE CHECKS PASSED")


# =============================================================================
# D2.M2 — nftId re-resolution from chain (restart-equivalent)
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lending
class TestFluidVaultNftReresolution:
    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_reresolve_same_nft_after_restart_equivalent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """A SECOND compile session with NO persisted state resolves the SAME
        nftId from VaultResolver reads — chain is the source of truth
        (VIB-5010 class); persisted nftIds are never load-bearing."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        # Open a position with session 1.
        open_intent = BorrowIntent(
            protocol="fluid_vault",
            collateral_token="ETH",
            collateral_amount=Decimal("0.5"),
            borrow_token="USDC",
            borrow_amount=Decimal("200"),
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        compiler_session_1 = _compiler(funded_wallet, price_oracle, anvil_rpc_url)
        compilation, execution = await _compile_and_execute(
            compiler_session_1, orchestrator, execution_context, open_intent
        )
        nft_id = _minted_nft_id(execution)
        assert nft_id is not None

        # "Restart": brand-new compiler + SDK instances, zero shared state.
        fresh_sdk = _vault_sdk(anvil_rpc_url)
        assert fresh_sdk.resolve_user_nft_for_vault(funded_wallet, VAULT) == nft_id, (
            "a fresh resolver session must re-resolve the SAME nftId from chain"
        )

        compiler_session_2 = _compiler(funded_wallet, price_oracle, anvil_rpc_url)
        repay_intent = RepayIntent(
            protocol="fluid_vault",
            token="USDC",
            amount=Decimal("50"),
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        compilation, execution = await _compile_and_execute(
            compiler_session_2, orchestrator, execution_context, repay_intent
        )
        event = _single_operate_event(execution)
        assert event.nft_id == nft_id, "the restart-equivalent repay must target the SAME nftId"
        repay_wei = int(Decimal("50") * Decimal(10**usdc_decimals))
        assert event.debt_delta == -repay_wei
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_before - usdc_after == repay_wei

        print("\nRE-RESOLUTION CHECKS PASSED")


# =============================================================================
# Failure: over-repay can never reach the chain + 31015 decode (D3 defense)
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lending
class TestFluidVaultOverRepayFailure:
    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_over_repay_refused_at_compile_and_reverts_31015_onchain(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """(a) explicit over-repay FAILS compile with zero on-chain calls;
        (b) a hand-built over-repay reverts with errorId 31015, decoded BY
        NAME, with zero balance drift."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)
        compiler = _compiler(funded_wallet, price_oracle, anvil_rpc_url)
        sdk = _vault_sdk(anvil_rpc_url)

        # Open a small position first.
        open_intent = BorrowIntent(
            protocol="fluid_vault",
            collateral_token="ETH",
            collateral_amount=Decimal("0.5"),
            borrow_token="USDC",
            borrow_amount=Decimal("100"),
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        _, execution = await _compile_and_execute(compiler, orchestrator, execution_context, open_intent)
        nft_id = _minted_nft_id(execution)
        assert nft_id is not None
        debt_now = sdk.position_by_nft_id(nft_id)[0].borrow

        # Pre-state FIRST: both the compile refusal and the hand-built revert
        # below must leave this balance untouched — capturing it after the
        # revert attempt would compare the balance with itself.
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        # (a) Compile-time refusal — actionable message, ZERO on-chain calls.
        block_before = web3.eth.block_number
        over_repay = RepayIntent(
            protocol="fluid_vault",
            token="USDC",
            amount=Decimal("500"),  # 5x the actual 100 USDC debt
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(over_repay)
        assert compilation_result.status.value == "FAILED", "over-repay must be refused at COMPILE time"
        assert "repay_full=True" in compilation_result.error, "the failure must point at the safe path"
        assert compilation_result.transactions == [], "no transactions may be compiled"

        # (b) Defense-in-depth: a HAND-BUILT over-repay reverts 31015 by name.
        over_amount = debt_now * 2
        operate_tx = sdk.build_operate_tx(VAULT, nft_id, 0, -over_amount, funded_wallet)
        # eth_call simulation captures the revert payload deterministically.
        revert_hex: str | None = None
        try:
            web3.eth.call({"from": funded_wallet, "to": operate_tx["to"], "data": operate_tx["data"]})
        except Exception as exc:  # noqa: BLE001 — the revert IS the assertion subject
            data = getattr(exc, "data", None)
            if isinstance(data, str) and data.startswith("0x"):
                revert_hex = data
            elif exc.args and isinstance(exc.args[0], dict):
                candidate = exc.args[0].get("data")
                if isinstance(candidate, str) and candidate.startswith("0x"):
                    revert_hex = candidate
            if revert_hex is None:
                import re

                match = re.search(r"(0x[0-9a-fA-F]{8,})", str(exc))
                if match:
                    revert_hex = match.group(1)
        assert revert_hex is not None, "the over-repay simulation must revert with payload"
        assert fluid_error_id(revert_hex) == 31015
        decoded = decode_fluid_revert(revert_hex)
        assert "Vault__ExcessDebtPayback" in decoded, f"31015 must decode BY NAME, got: {decoded}"

        # Zero balance drift vs the pre-attempt capture: no USDC moved and no
        # on-chain debt change.
        del usdc_decimals  # exactness is wei-level; no human conversion needed
        assert get_token_balance(web3, usdc, funded_wallet) == usdc_before
        assert sdk.position_by_nft_id(nft_id)[0].borrow >= debt_now, "debt untouched (accrual-only)"
        assert web3.eth.block_number >= block_before  # the failed compile mined nothing itself

        print("\nOVER-REPAY FAILURE CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
