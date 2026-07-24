"""Production-grade BridgeIntent tests for the LiFi meta-aggregator on Arbitrum.

Companion to ``test_lifi_bridge.py`` (the cross-chain SwapIntent flow): this
file exercises LiFi's OTHER bridge entry point — ``BridgeIntent(
preferred_bridge="lifi")`` — dispatched via ``_bridge_registry_protocol`` to
``LiFiCompiler.compile_bridge``, which emits a BRIDGE ActionBundle carrying
the BridgeCompiler metadata contract (from_chain / to_chain / token / amount /
bridge) plus LiFi's deferred-execution keys. This construction is what the
intent-coverage gate credits for the (lifi, BRIDGE, arbitrum) cell.

NO MOCKING. Tests execute real on-chain deposits against the live LiFi API
route on a mainnet-forked Anvil and verify source-chain state changes.

Route-agnosticism: LiFi's route picker selects the underlying bridge tool
(Across, Stargate, CCTP, Hop, ...) per request, so bridge-specific event
topics CANNOT be asserted — they vary call-to-call. Every Layer-3 assertion
keys on tool-agnostic surfaces only: the wallet-outgoing ERC-20 Transfer sum
(``parse_swap_receipt`` cross-chain mode), the typed ``extract_bridge_data``
payload, the full ResultEnricher round-trip, and exact source-chain balance
deltas.

Layer coverage on Anvil (single source-chain fork):

- Layer 1 (compilation): BridgeIntent -> BRIDGE ActionBundle with the
  BridgeCompiler metadata contract + deferred-refresh ``route_params``;
  the chain ids and exact input amount in ``route_params`` ARE the routing
  input, so asserting them is the LiFi analogue of decoding
  destinationChainId out of the Across depositV3 calldata.
- Layer 2 (execution): the source-chain deposit (approve + LiFi route tx;
  deferred refresh fetches fresh calldata at execution time). Destination
  delivery is asynchronous and off-fork — the documented bridge test limit.
- Layer 3 (receipt parsing): ``parse_swap_receipt`` (cross-chain mode) +
  ``extract_bridge_data`` + the full ResultEnricher round-trip over the
  real ``[approve, deposit]`` receipt list. The enricher pass guards the
  approve-receipt regression: ``bridge_data.source_tx_hash`` landing on
  the DEPOSIT hash proves the approve receipt did not satisfy the
  native-asset fallback (LiFiTransferStarted gate in extract_bridge_data).
- Layer 4 (balance deltas): source-chain USDC decreases by EXACTLY the
  bridged amount. The deposit counterparty balance and source-chain ETH
  drain are NOT asserted — the receiving contract varies with the tool
  LiFi picks, and some tools carry a native msg.value fee.

To run:
    ARBITRUM_RPC_URL=<rpc> uv run pytest tests/intents/arbitrum/test_lifi_bridge_intent.py -v -s

On a slow public fork RPC the deposit can exceed the default 30s receipt
wait (Anvil lazily downloads each tool's cold contract state from
upstream): raise it with ALMANAK_TEST_TX_TIMEOUT_SECONDS=90. CI's keyed
RPC does not need this.

Aggregator stability rule (intent-tests.md anti-pattern #12): this file
uses ``deferred_swap: True`` bundles — run it 10/10 locally before merging
changes.
"""

import time
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.lifi.receipt_parser import LiFiReceiptParser
from almanak.framework.execution.extracted_data import BridgeData
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import BridgeIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

pytestmark = pytest.mark.no_zodiac(
    reason="Aggregator routes non-deterministically; plan excludes lifi from Zodiac coverage"
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"
ARBITRUM_CHAIN_ID = 42161
DEST_CHAIN = "optimism"
OPTIMISM_CHAIN_ID = 10


def _norm_hash(tx_hash: str) -> str:
    """Lowercase a tx hash and strip the 0x prefix for comparison.

    TransactionResult.tx_hash and Receipt.to_dict()["tx_hash"] differ in
    0x-prefixing, so hash equality must compare the bare hex digits.
    (Mirrors test_lifi_bridge.py — duplicated to keep test files standalone.)
    """
    return tx_hash.lower().removeprefix("0x")


def _sync_anvil_time_to_wall_clock(web3: Web3) -> int:
    """Advance the Anvil fork's block timestamp to the current wall clock.

    LiFi may route the transfer through its Across facet, whose SpokePool
    rejects deposits with ``InvalidQuoteTimestamp()`` when the API-issued
    ``quoteTimestamp`` (wall-clock bound) falls outside the acceptable
    window relative to the fork's block timestamp. Same fix as
    test_across_bridge.py / test_lifi_bridge.py (duplicated to keep test
    files standalone).
    """
    current_block = web3.eth.get_block("latest")
    current_block_ts = current_block["timestamp"]
    wall_clock_ts = int(time.time())
    if wall_clock_ts > current_block_ts:
        # make_request() returns JSON-RPC error payloads instead of raising;
        # check both responses so an Anvil RPC rejection fails loudly here
        # instead of masquerading as a LiFi route failure downstream.
        set_ts_resp = web3.provider.make_request("evm_setNextBlockTimestamp", [wall_clock_ts])
        assert not set_ts_resp.get("error"), (
            f"evm_setNextBlockTimestamp({wall_clock_ts}) failed: {set_ts_resp['error']}"
        )
        mine_resp = web3.provider.make_request("evm_mine", [])
        assert not mine_resp.get("error"), f"evm_mine failed: {mine_resp['error']}"
    return wall_clock_ts


# =============================================================================
# BridgeIntent Tests (LiFiCompiler.compile_bridge)
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.bridge
class TestLiFiBridgeIntent:
    """BridgeIntent(preferred_bridge="lifi") end-to-end on an Arbitrum fork.

    Same corridor and tool-agnostic assertions as test_lifi_bridge.py's
    ``TestLiFiBridge``, but through the BRIDGE intent vocabulary:
    ``_bridge_registry_protocol`` dispatches to
    ``LiFiCompiler.compile_bridge``, the bundle carries the BridgeCompiler
    metadata contract, and the ResultEnricher round-trip proves BRIDGE
    enrichment attaches the deposit (not approve) tx hash.
    """

    @pytest.mark.intent(IntentType.BRIDGE)
    @pytest.mark.asyncio
    async def test_bridge_intent_usdc_arbitrum_to_optimism(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Bridge USDC Arbitrum -> Optimism via BridgeIntent + LiFi.

        Flow:
        1. Create BridgeIntent USDC arbitrum -> optimism, preferred_bridge="lifi"
        2. Compile via IntentCompiler (LiFiCompiler.compile_bridge: approve +
           bridge_deferred Diamond tx, BridgeCompiler metadata contract)
        3. Execute via ExecutionOrchestrator (deferred refresh)
        4. Parse: parse_swap_receipt (cross-chain) + extract_bridge_data +
           full ResultEnricher round-trip over [approve, deposit] receipts
        5. Verify source-chain USDC decreased by the exact bridge amount
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        in_decimals = get_token_decimals(web3, usdc_addr)
        assert in_decimals == 6, f"Arbitrum USDC is 6 decimals, got {in_decimals}"

        # Comfortably above every LiFi tool's minimum transfer / relay-fee
        # floor for the USDC arbitrum->optimism corridor.
        bridge_amount = Decimal("50")
        expected_wei = int(bridge_amount * Decimal(10**in_decimals))

        synced_ts = _sync_anvil_time_to_wall_clock(web3)

        print(f"\n{'='*80}")
        print("Test: USDC Arbitrum -> Optimism bridge via LiFi BridgeIntent")
        print(f"{'='*80}")
        print(f"Bridge amount: {bridge_amount} USDC")
        print(f"Fork time synced to wall clock: {synced_ts}")

        # --- Layer 4a: record balances BEFORE ---
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        print(f"Wallet USDC before: {format_token_amount(usdc_before, in_decimals)}")
        assert usdc_before >= expected_wei, "funded_wallet must have at least bridge_amount USDC"

        # --- Create BridgeIntent ---
        intent = BridgeIntent(
            token="USDC",
            amount=bridge_amount,
            from_chain=CHAIN_NAME,
            to_chain=DEST_CHAIN,
            max_slippage=Decimal("0.05"),  # 5% slippage for aggregator routes
            preferred_bridge="lifi",
        )
        print(f"\nCreated BridgeIntent: {intent.token} {intent.from_chain} -> {intent.to_chain}")

        # --- Layer 1: Compilation ---
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        print("Compiling BridgeIntent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        bundle = compilation_result.action_bundle
        assert bundle.intent_type == "BRIDGE", f"Bundle must be a BRIDGE bundle, got {bundle.intent_type}"
        metadata = bundle.metadata
        print(f"ActionBundle created with {len(bundle.transactions)} transactions")
        print(f"Tool (compile-time route): {metadata.get('tool')}")

        # BridgeCompiler metadata contract — ResultEnricher's bridge_data
        # hints and the runner's bridge lanes key on these.
        assert metadata["bridge"] == "lifi"
        assert metadata["from_chain"] == CHAIN_NAME
        assert metadata["to_chain"] == DEST_CHAIN
        assert metadata["token"] == "USDC"
        assert metadata["amount"] == str(bridge_amount)
        assert metadata["is_cross_chain"] is True
        # LiFi deferred-execution keys — fresh calldata fetched at execute.
        assert metadata["deferred_swap"] is True
        assert metadata["protocol"] == "lifi"

        # route_params IS the deferred-refresh routing input — asserting the
        # chain ids and amount catches a compiler bug that resolves the
        # destination to the wrong chain id or drops the input amount.
        route_params = metadata.get("route_params")
        assert route_params is not None, "LiFi bundle must carry route_params for deferred refresh"
        assert route_params["from_chain_id"] == ARBITRUM_CHAIN_ID
        assert route_params["to_chain_id"] == OPTIMISM_CHAIN_ID
        assert route_params["from_amount"] == str(expected_wei), (
            f"route_params.from_amount must be the exact input amount. "
            f"Expected: {expected_wei}, got: {route_params['from_amount']}"
        )

        # ERC-20 bridge: trailing deferred route tx, leading approve leg(s).
        tx_types = [tx.get("tx_type") for tx in bundle.transactions]
        assert tx_types[-1] == "bridge_deferred", (
            f"Final transaction must be bridge_deferred, got tx_types={tx_types}"
        )
        for tx_type in tx_types[:-1]:
            assert tx_type in ("approve", "approve_reset"), (
                f"Unexpected leading tx_type={tx_type}; expected approve/approve_reset. Full list: {tx_types}"
            )

        # --- Layer 2: Execution (deferred refresh runs inside the orchestrator) ---
        print("\nExecuting via ExecutionOrchestrator (with deferred refresh)...")
        execution_result = await orchestrator.execute(bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        assert len(execution_result.transaction_results) == len(bundle.transactions), (
            f"Expected {len(bundle.transactions)} tx results, "
            f"got {len(execution_result.transaction_results)}"
        )
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        deposit_result = execution_result.transaction_results[-1]
        assert deposit_result.receipt is not None, "bridge deposit tx must have a receipt"
        for idx, tx_result in enumerate(execution_result.transaction_results[:-1]):
            assert tx_result.receipt is not None, f"leading approve tx #{idx} must have a receipt"

        deposit_receipt = deposit_result.receipt.to_dict()
        # Receipt.to_dict() emits snake_case keys; ResultEnricher._collect_receipts
        # adds the camelCase aliases before invoking parsers. Mirror that here
        # since the parser is called directly on the dict.
        if "transactionHash" not in deposit_receipt and "tx_hash" in deposit_receipt:
            deposit_receipt["transactionHash"] = deposit_receipt["tx_hash"]
        assert deposit_receipt.get("status") == 1, (
            f"Deposit tx did not succeed on-chain: status={deposit_receipt.get('status')}"
        )
        print(f"\nDeposit tx: {deposit_result.tx_hash[:16]}... gas={deposit_result.gas_used}")

        # --- Layer 3: Receipt parsing (parse_swap_receipt, cross-chain mode) ---
        parser = LiFiReceiptParser(chain=CHAIN_NAME)
        parse_result = parser.parse_swap_receipt(
            receipt=deposit_receipt,
            wallet_address=funded_wallet,
            token_out=usdc_addr,
            token_in=usdc_addr,
            tool=metadata.get("tool"),
            is_cross_chain=True,
        )
        assert parse_result.success, f"parse_swap_receipt failed: {parse_result.error}"
        assert parse_result.is_cross_chain is True
        assert parse_result.amount_in == expected_wei, (
            f"Wallet-outgoing USDC in deposit receipt must equal bridge amount. "
            f"Expected: {expected_wei}, got: {parse_result.amount_in}"
        )

        # --- Layer 3.5: Bridge extraction via extract_bridge_data (VIB-3226) ---
        bridge_data = parser.extract_bridge_data(
            deposit_receipt,
            from_chain=CHAIN_NAME,
            to_chain=DEST_CHAIN,
            token="USDC",
            amount=str(bridge_amount),
            bridge="lifi",
        )
        assert bridge_data is not None, "LiFiReceiptParser must extract BridgeData from the deposit receipt"
        assert isinstance(bridge_data, BridgeData)
        assert bridge_data.bridge_name == "lifi"
        assert bridge_data.source_chain == CHAIN_NAME
        assert bridge_data.destination_chain == DEST_CHAIN
        assert bridge_data.token_symbol == "USDC"
        assert bridge_data.amount_sent_raw == expected_wei, (
            f"BridgeData.amount_sent_raw must equal bridge amount. "
            f"Expected: {expected_wei}, got: {bridge_data.amount_sent_raw}"
        )
        assert bridge_data.amount_sent == bridge_amount
        assert _norm_hash(bridge_data.source_tx_hash) == _norm_hash(deposit_result.tx_hash)
        print(
            f"BridgeData extracted: bridge={bridge_data.bridge_name}, "
            f"amount={bridge_data.amount_sent} {bridge_data.token_symbol}, "
            f"{bridge_data.source_chain}->{bridge_data.destination_chain}"
        )

        # --- Layer 3.6: Full ResultEnricher round-trip (VIB-3226) ---
        # Drive the same path the StrategyRunner uses in production. The
        # receipts list is [approve, deposit] and the enricher scans it
        # first-match — source_tx_hash landing on the deposit hash proves the
        # approve receipt did not satisfy the native-asset fallback
        # (LiFiTransferStarted gate in extract_bridge_data).
        from almanak.framework.execution.orchestrator import ExecutionContext
        from almanak.framework.execution.result_enricher import ResultEnricher

        enricher = ResultEnricher(live_mode=True)
        enrich_context = ExecutionContext(chain=CHAIN_NAME, wallet_address=funded_wallet)
        enriched = enricher.enrich(
            execution_result,
            intent,
            enrich_context,
            bundle_metadata=bundle.metadata,
        )
        assert enriched is execution_result, "enrich() should mutate the same ExecutionResult"
        assert enriched.bridge_data is not None, (
            "ResultEnricher must populate ExecutionResult.bridge_data for a BRIDGE intent"
        )
        assert enriched.bridge_data.bridge_name == "lifi"
        assert enriched.bridge_data.amount_sent_raw == expected_wei
        assert enriched.bridge_data.source_chain == CHAIN_NAME
        assert enriched.bridge_data.destination_chain == DEST_CHAIN
        assert enriched.bridge_data.token_symbol == "USDC"
        assert _norm_hash(enriched.bridge_data.source_tx_hash) == _norm_hash(deposit_result.tx_hash), (
            "source_tx_hash must be the DEPOSIT tx, not the approve tx "
            "(approve-receipt native-fallback regression)"
        )

        # --- Layer 4: Exact balance deltas (source chain) ---
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print("\n--- Results (source chain only) ---")
        print(f"Wallet USDC spent: {format_token_amount(usdc_spent, in_decimals)}")

        assert usdc_spent == expected_wei, (
            f"Wallet USDC must decrease by exactly {expected_wei} wei, got {usdc_spent}"
        )

        # NOTE: destination-chain USDC settlement is intentionally NOT
        # asserted — it is off-fork and asynchronous. See module docstring.

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BRIDGE)
    @pytest.mark.asyncio
    async def test_bridge_intent_insufficient_balance_fails_safely(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """BridgeIntent exceeding the wallet balance fails without moving funds.

        2x balance (not 100x): guaranteed to fail on-chain, but small enough
        that the LiFi API still quotes a route for the corridor — the test
        must fail at EXECUTION to exercise conservation.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        in_decimals = get_token_decimals(web3, usdc_addr)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        assert usdc_before > 0, "funded_wallet must have USDC for this test"
        balance_decimal = Decimal(usdc_before) / Decimal(10**in_decimals)

        excessive_amount = balance_decimal * Decimal("2")

        _sync_anvil_time_to_wall_clock(web3)

        print(f"\n{'='*80}")
        print("Test: LiFi BridgeIntent with Insufficient Balance")
        print(f"{'='*80}")
        print(f"Balance: {balance_decimal} USDC, trying: {excessive_amount} USDC")

        intent = BridgeIntent(
            token="USDC",
            amount=excessive_amount,
            from_chain=CHAIN_NAME,
            to_chain=DEST_CHAIN,
            max_slippage=Decimal("0.05"),
            preferred_bridge="lifi",
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

        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        # Conservation check — balance unchanged after failure.
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed bridge"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
