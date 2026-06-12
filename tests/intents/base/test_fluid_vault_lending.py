"""Production-grade Fluid vault (NFT-CDP) lending intent tests on Base.

UAT card VIB-5031 D2.M1: type-1 vault id 47 (sUSDai -> USDC,
``0x01F0D07fdE184614216e76782c6b7dF663F5375e``) — a DISTINCT fork, DISTINCT
vault, and DISTINCT collateral mechanics from the arbitrum variant: sUSDai
is an ERC-20 leg, so the compile emits an approve with ``spender == vault``
and EXACT amount (no MAX_UINT) and the operate carries ``msg.value == 0``.

ONE deliberate lifecycle difference from D1.S1: the position is opened
SUPPLY-FIRST — a standalone ``SupplyIntent(protocol="fluid_vault")``
against a vault where the wallet holds NO NFT mints the position
(``nftId=0`` path, factory mint captured, exactly one SUPPLY accounting
row) and a subsequent ``BorrowIntent`` reuses that SAME nftId (debt-only
operate). This proves the supply-only first-mint path D1 does not cover.

sUSDai funding: whale impersonation of the Fluid Liquidity layer (the
central custodian of every Fluid-supplied sUSDai balance — the deepest
deterministic holder on any fork block).

NO MOCKING. To run:
    uv run pytest tests/intents/base/test_fluid_vault_lending.py -v -s
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
from almanak.connectors.fluid.sdk import FLUID_ADDRESSES
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

CHAIN_NAME = "base"
PROTOCOL = "fluid_vault"

# Explicit pin: base vault id 47 (sUSDai -> USDC), lowercased because the
# canonical market_id form is lowercase. An anonymous next(iter(...)) would
# silently retarget the whole suite if the pinned-market table ever grows.
VAULT = "0x01F0D07fdE184614216e76782c6b7dF663F5375e".lower()
assert VAULT in FLUID_VAULT_MARKETS["base"], (
    f"pinned base vault {VAULT} is missing from FLUID_VAULT_MARKETS['base'] — "
    f"the addresses.py table and this suite must move together"
)
VAULT_ENTRY = FLUID_VAULT_MARKETS["base"][VAULT]
SUSDAI = VAULT_ENTRY["collateral_address"]
ARBITRUM_VAULT = next(iter(FLUID_VAULT_MARKETS["arbitrum"]))

# The Fluid Liquidity layer custodies every supplied sUSDai balance — the
# deepest deterministic on-fork holder (whale impersonation source).
FLUID_LIQUIDITY = "0x52Aa899454998Be5b000Ad077a46Bbe360F4e497"

SUPPLY_SUSDAI = Decimal("100")
BORROW_USDC = Decimal("20")  # far under the 88% CF on a ~$105 collateral

APPROVE_SELECTOR = "0x095ea7b3"
OPERATE_SELECTOR = "0x032d2276"


# =============================================================================
# Helpers
# =============================================================================


def _vault_sdk(anvil_rpc_url: str) -> FluidVaultSDK:
    return FluidVaultSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)


def _compiler(funded_wallet: str, price_oracle: dict[str, Decimal], anvil_rpc_url: str) -> IntentCompiler:
    return IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )


def _vault_prices(price_oracle: dict[str, Decimal]) -> dict[str, Decimal]:
    """sUSDai is a staked-dollar token with no CoinGecko row in the shared
    fixture; inject ~$1 for the USD-leg valuation seam (assertions on the
    USD legs are relative; the HF is protocol-truth from the vault oracle
    and never touches this price)."""
    prices = dict(price_oracle)
    prices.setdefault("sUSDai", Decimal("1"))
    return prices


def _single_operate_event(execution_result: ExecutionResult):
    parser = FluidVaultReceiptParser(chain=CHAIN_NAME)
    events = []
    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            events.extend(parser.parse_receipt(tx_result.receipt.to_dict()).operate_events)
    assert len(events) == 1, f"expected exactly one vault LogOperate event, got {len(events)}"
    assert events[0].vault == VAULT
    return events[0]


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


def _capture_state(intent: Any, wallet: str, reader: Any, prices, *, post: bool, block: int | None = None):
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
    enriched = enrich_result(
        execution_result,
        intent,
        ExecutionContext(
            deployment_id="layer5-fluid-vault-base",
            chain=CHAIN_NAME,
            wallet_address=wallet,
            protocol="fluid_vault",
            simulation_enabled=True,
        ),
        live_mode=False,
        bundle_metadata=metadata,
    )
    FluidVaultRunnerHookConnector().enrich_result(enriched, gateway_client=None, chain=CHAIN_NAME)
    return enriched


def _ledger_extracted_data(harness, ledger_entry_id: str) -> dict:
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


def _fund_susdai(web3: Web3, wallet: str, amount_wei: int) -> None:
    """Whale-impersonation funding: transfer sUSDai from the Fluid Liquidity
    layer (auto-impersonated Anvil) to the test wallet."""
    holder = Web3.to_checksum_address(FLUID_LIQUIDITY)
    token = Web3.to_checksum_address(SUSDAI)
    available = get_token_balance(web3, token, holder)
    if available < amount_wei:
        # The fork block is PINNED, so the holder's balance at that block is
        # deterministic — a shortfall means the harness was re-pinned wrong.
        # Skipping here would let the base-chain coverage pass vacuously.
        pytest.fail(
            f"sUSDai whale funding shortfall: holder {holder} (Fluid Liquidity layer) "
            f"holds only {available} sUSDai base units at the pinned fork block, but the "
            f"test needs {amount_wei}. The pinned fork is deterministic, so this means "
            f"the whale or fork block was re-pinned wrong — re-pin the whale or the fork "
            f"block instead of skipping."
        )
    web3.provider.make_request("anvil_impersonateAccount", [holder])
    try:
        web3.provider.make_request("anvil_setBalance", [holder, hex(10**18)])  # gas for the transfer
        transfer_data = "0xa9059cbb" + wallet[2:].lower().zfill(64) + f"{amount_wei:064x}"
        tx_hash = web3.eth.send_transaction({"from": holder, "to": token, "data": transfer_data})
        receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        assert receipt["status"] == 1, "sUSDai whale transfer must succeed"
    finally:
        # Always undo the impersonation — a failed transfer must not leak an
        # impersonated whale into the rest of the fork session.
        web3.provider.make_request("anvil_stopImpersonatingAccount", [holder])


async def _compile_and_execute(compiler, orchestrator, execution_context, intent):
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
    """Anvil hygiene (Phase-0 finding 1): clear any 7702 delegation code on
    anvil key0 before steps that move value through the relayer EOA."""
    response = web3.provider.make_request("anvil_setCode", [TEST_WALLET, "0x"])
    assert not (isinstance(response, dict) and "error" in response), (
        f"anvil_setCode 7702 scrub rejected — fixture cannot guarantee native-ETH hygiene: {response}"
    )
    yield


# =============================================================================
# D2.M1 — supply-FIRST mint, then borrow reusing the same nftId
# =============================================================================


@pytest.mark.base
@pytest.mark.lending
class TestFluidVaultSupplyFirstBase:
    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_supply_first_mint_then_borrow_reuses_nft(
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
        # Hard pin: base targets the BASE vault — not a copy of arbitrum's.
        assert VAULT != ARBITRUM_VAULT, "base tests must target the base vault deployment"
        # Cross-chain table integrity: Fluid deploys the VaultResolver at a
        # deterministic address on every supported chain (ADR Phase-0 table),
        # so a base/arbitrum divergence in addresses.py means a typo'd entry,
        # not a real deployment difference.
        assert FLUID_ADDRESSES["base"]["vault_resolver"] == FLUID_ADDRESSES["arbitrum"]["vault_resolver"], (
            "VaultResolver is deterministic across chains — an addresses.py divergence is a typo"
        )

        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)
        susdai_decimals = get_token_decimals(web3, SUSDAI)
        assert susdai_decimals == 18, "sUSDai is an 18-decimal token (verification report D3)"
        prices = _vault_prices(price_oracle)
        compiler = _compiler(funded_wallet, price_oracle, anvil_rpc_url)
        sdk = _vault_sdk(anvil_rpc_url)
        deployment_id = "layer5-intent-test"

        supply_wei = int(SUPPLY_SUSDAI * Decimal(10**susdai_decimals))
        _fund_susdai(web3, funded_wallet, supply_wei * 2)

        # ────────────────────────────────────────────────────────────────
        # Step 1 — SUPPLY-FIRST MINT: SupplyIntent against a vault with NO NFT
        # ────────────────────────────────────────────────────────────────
        assert sdk.resolve_user_nft_for_vault(funded_wallet, VAULT) is None, (
            "wallet must hold NO Fluid NFT on the base vault before the supply"
        )
        susdai_before = get_token_balance(web3, SUSDAI, funded_wallet)
        assert susdai_before >= supply_wei

        supply_intent = SupplyIntent(
            protocol="fluid_vault",
            token="sUSDai",
            amount=SUPPLY_SUSDAI,
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        pre_state = _capture_state(supply_intent, funded_wallet, anvil_eth_call_adapter, prices, post=False)
        compilation, execution = await _compile_and_execute(compiler, orchestrator, execution_context, supply_intent)

        # ERC-20 collateral mechanics: approve(spender==vault, EXACT amount),
        # operate carries msg.value == 0 (the D2.M1 contract).
        bundle_txs = compilation.action_bundle.transactions
        approve_txs = [tx for tx in bundle_txs if str(tx.get("data", "")).startswith(APPROVE_SELECTOR)]
        assert len(approve_txs) == 1, "ERC-20 collateral leg must compile exactly one approve"
        approve_data = str(approve_txs[0]["data"])
        assert approve_data[10:74].endswith(VAULT[2:].lower()), "approve spender must be the vault"
        assert int(approve_data[74:138], 16) == supply_wei, "approve amount must be EXACT (no MAX_UINT)"
        operate_txs = [tx for tx in bundle_txs if str(tx.get("data", "")).startswith(OPERATE_SELECTOR)]
        assert len(operate_txs) == 1
        assert int(operate_txs[0].get("value", 0) or 0) == 0, "ERC-20 collateral operate carries msg.value == 0"

        # Receipt: factory mint (nftId=0 path) + supply-only signed deltas.
        nft_id = _minted_nft_id(execution)
        assert nft_id is not None and nft_id > 0, "supply-first open must mint the factory NFT"
        event = _single_operate_event(execution)
        assert event.nft_id == nft_id
        assert event.col_delta == supply_wei
        assert event.debt_delta == 0, "supply-first mint is a collateral-only operate"

        # Layer 4 — exact ERC-20 delta.
        susdai_after = get_token_balance(web3, SUSDAI, funded_wallet)
        assert susdai_before - susdai_after == supply_wei, "wallet sUSDai delta must be exactly -supply"

        position, _ = sdk.position_by_nft_id(nft_id)
        assert position.supply >= supply_wei - 16, "resolver supply ~ collateral (rounding dust)"
        assert position.borrow == 0

        # Layer 5 — exactly ONE SUPPLY row, base-chain position key.
        enriched = _enrich(execution, supply_intent, funded_wallet, compilation.action_bundle.metadata)
        post_state = _capture_state(
            supply_intent, funded_wallet, anvil_eth_call_adapter, prices, post=True, block=_receipt_block(execution)
        )
        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=supply_intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="SUPPLY",
            price_oracle=prices,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        expected_key = f"lending:base:fluid_vault:{funded_wallet.lower()}:{VAULT}:susdai"
        assert row["position_key"] == expected_key, (
            f"base position key must carry the BASE chain + BASE vault segments: "
            f"{row['position_key']!r} != {expected_key!r}"
        )
        assert ARBITRUM_VAULT not in row["position_key"], "never a copy of the arbitrum constants"
        payload = _payload(row)
        assert payload["confidence"] == "HIGH", (
            f"got {payload['confidence']!r} (unavailable_reason={payload.get('unavailable_reason')!r})"
        )
        assert Decimal(payload["amount_token"]) == SUPPLY_SUSDAI
        extracted = _ledger_extracted_data(layer5_accounting_harness, row["ledger_entry_id"])
        assert extracted[FLUID_VAULT_OPERATE_KEY]["nft_id"] == str(nft_id)
        assert extracted[FLUID_VAULT_OPERATE_KEY]["vault"] == VAULT

        # ────────────────────────────────────────────────────────────────
        # Step 2 — BORROW reusing the SAME nftId (debt-only operate)
        # ────────────────────────────────────────────────────────────────
        borrow_wei = int(BORROW_USDC * Decimal(10**usdc_decimals))
        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        borrow_intent = BorrowIntent(
            protocol="fluid_vault",
            collateral_token="sUSDai",
            collateral_amount=Decimal("0"),  # borrow against the EXISTING collateral
            borrow_token="USDC",
            borrow_amount=BORROW_USDC,
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        pre_state = _capture_state(borrow_intent, funded_wallet, anvil_eth_call_adapter, prices, post=False)
        compilation, execution = await _compile_and_execute(compiler, orchestrator, execution_context, borrow_intent)

        assert _minted_nft_id(execution) is None, "the borrow must NOT mint a second NFT"
        event = _single_operate_event(execution)
        assert event.nft_id == nft_id, "the borrow must reuse the SAME nftId (debt-only operate)"
        assert event.col_delta == 0
        assert event.debt_delta == borrow_wei

        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after - usdc_before == borrow_wei, "wallet USDC delta must be exactly +borrow"

        position, _ = sdk.position_by_nft_id(nft_id)
        assert position.borrow >= borrow_wei

        enriched = _enrich(execution, borrow_intent, funded_wallet, compilation.action_bundle.metadata)
        post_state = _capture_state(
            borrow_intent, funded_wallet, anvil_eth_call_adapter, prices, post=True, block=_receipt_block(execution)
        )
        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=borrow_intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="BORROW",
            price_oracle=prices,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        assert row["position_key"] == f"lending:base:fluid_vault:{funded_wallet.lower()}:{VAULT}:usdc"
        payload = _payload(row)
        assert payload["confidence"] == "HIGH"
        assert Decimal(payload["amount_token"]) == BORROW_USDC
        assert Decimal(payload["debt_value_after_usd"]) > Decimal(payload["debt_value_before_usd"])
        assert payload["health_factor_after"] is not None, "HF must be measured while debt > 0"
        extracted = _ledger_extracted_data(layer5_accounting_harness, row["ledger_entry_id"])
        assert extracted[FLUID_VAULT_OPERATE_KEY]["nft_id"] == str(nft_id), "SAME nftId across lifecycle rows"

        # ────────────────────────────────────────────────────────────────
        # Step 3 — PARTIAL REPAY: exact ERC-20 USDC delta, debt decreases
        # (cross-chain consistency with the arbitrum lifecycle)
        # ────────────────────────────────────────────────────────────────
        repay_usdc = Decimal("5")
        repay_wei = int(repay_usdc * Decimal(10**usdc_decimals))
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        debt_before_step = sdk.position_by_nft_id(nft_id)[0].borrow

        repay_intent = RepayIntent(
            protocol="fluid_vault",
            token="USDC",
            amount=repay_usdc,
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        pre_state = _capture_state(repay_intent, funded_wallet, anvil_eth_call_adapter, prices, post=False)
        compilation, execution = await _compile_and_execute(compiler, orchestrator, execution_context, repay_intent)

        event = _single_operate_event(execution)
        assert event.nft_id == nft_id, "repay must carry the SAME nftId"
        assert event.col_delta == 0
        assert event.debt_delta == -repay_wei
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_before - usdc_after == repay_wei, "wallet USDC delta must be exactly -repay"
        assert sdk.position_by_nft_id(nft_id)[0].borrow < debt_before_step, "debt must strictly decrease"

        enriched = _enrich(execution, repay_intent, funded_wallet, compilation.action_bundle.metadata)
        post_state = _capture_state(
            repay_intent, funded_wallet, anvil_eth_call_adapter, prices, post=True, block=_receipt_block(execution)
        )
        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=repay_intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="REPAY",
            price_oracle=prices,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        assert row["position_key"] == f"lending:base:fluid_vault:{funded_wallet.lower()}:{VAULT}:usdc"
        assert Decimal(_payload(row)["amount_token"]) == repay_usdc

        # ────────────────────────────────────────────────────────────────
        # Step 4 — WITHDRAW SLICE: exact ERC-20 sUSDai returned, no approve
        # ────────────────────────────────────────────────────────────────
        withdraw_susdai = Decimal("10")
        withdraw_wei = int(withdraw_susdai * Decimal(10**susdai_decimals))
        susdai_before = get_token_balance(web3, SUSDAI, funded_wallet)

        withdraw_intent = WithdrawIntent(
            protocol="fluid_vault",
            token="sUSDai",
            amount=withdraw_susdai,
            market_id=VAULT,
            chain=CHAIN_NAME,
        )
        pre_state = _capture_state(withdraw_intent, funded_wallet, anvil_eth_call_adapter, prices, post=False)
        compilation, execution = await _compile_and_execute(compiler, orchestrator, execution_context, withdraw_intent)
        assert all(
            not str(tx.get("data", "")).startswith(APPROVE_SELECTOR) for tx in compilation.action_bundle.transactions
        ), "a withdraw compiles NO approve"

        event = _single_operate_event(execution)
        assert event.nft_id == nft_id, "withdraw must carry the SAME nftId"
        assert event.col_delta == -withdraw_wei
        assert event.debt_delta == 0
        susdai_after = get_token_balance(web3, SUSDAI, funded_wallet)
        assert susdai_after - susdai_before == withdraw_wei, "wallet sUSDai delta must be exactly +withdraw"

        enriched = _enrich(execution, withdraw_intent, funded_wallet, compilation.action_bundle.metadata)
        post_state = _capture_state(
            withdraw_intent, funded_wallet, anvil_eth_call_adapter, prices, post=True, block=_receipt_block(execution)
        )
        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=withdraw_intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="WITHDRAW",
            price_oracle=prices,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        assert row["position_key"] == f"lending:base:fluid_vault:{funded_wallet.lower()}:{VAULT}:susdai"
        assert Decimal(_payload(row)["amount_token"]) == withdraw_susdai

        # Row census: 1 SUPPLY, 1 BORROW, 1 REPAY, 1 WITHDRAW — no synthetic siblings.
        store = layer5_accounting_harness.store
        assert len(await store.get_accounting_events(deployment_id, event_type="SUPPLY", limit=10)) == 1
        assert len(await store.get_accounting_events(deployment_id, event_type="BORROW", limit=10)) == 1
        assert len(await store.get_accounting_events(deployment_id, event_type="REPAY", limit=10)) == 1
        assert len(await store.get_accounting_events(deployment_id, event_type="WITHDRAW", limit=10)) == 1

        print("\nBASE SUPPLY-FIRST CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
