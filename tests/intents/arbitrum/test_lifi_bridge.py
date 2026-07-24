"""Production-grade cross-chain (bridge) tests for LiFi on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for a
USDC transfer from Arbitrum to Optimism routed through the LiFi aggregator:

1. Create a cross-chain SwapIntent (protocol="lifi", destination_chain="optimism")
2. Compile to ActionBundle via IntentCompiler (LiFiCompiler, deferred route)
3. Execute on an Arbitrum Anvil fork via ExecutionOrchestrator (deferred
   refresh fetches fresh route calldata from the LiFi API at execution time)
4. Parse the deposit receipt with LiFiReceiptParser: ``parse_swap_receipt``
   (cross-chain mode) AND ``extract_bridge_data`` (VIB-3226 typed BridgeData)
5. Verify source-chain balance deltas

NO MOCKING. All tests execute real on-chain deposits through the LiFi
Diamond on a mainnet-forked Anvil using real LiFi API routes, and verify
source-chain state changes.

Why a cross-chain SwapIntent and not a BridgeIntent:

- ``LiFiCompiler`` declares ``intents = {SWAP}`` only. A
  ``BridgeIntent(preferred_bridge="lifi")`` dispatches to it via
  ``_bridge_registry_protocol`` and fails compilation with "does not
  support intent type BRIDGE". The production LiFi bridge flow is the
  cross-chain SwapIntent (``SwapIntent.is_cross_chain``), which compiles a
  ``bridge_deferred`` transaction against the LiFi Diamond. The
  (lifi, BRIDGE, <chain>) triples therefore stay structurally excused in
  ``scripts/ci/intent-coverage-excused.yml``.

Layer coverage on Anvil (single source-chain fork), following the house
bridge pattern (test_across_bridge.py / test_stargate_bridge.py):

- Layer 1 (compilation): runs locally against the real LiFi quote API.
  ``route_params`` (the deferred-refresh routing input) is asserted to
  encode the Optimism chain id (10) and the exact input amount — the
  analogue of the Across test's calldata destinationChainId decode: it
  catches a compiler bug that silently routes to the wrong chain.
- Layer 2 (execution): verified against the source-chain deposit tx. The
  destination-chain delivery is asynchronous and off-fork (tracked via
  the LiFi status API in production), so destination settlement is NOT
  verifiable here. This is the documented bridge test limit.
- Layer 3 (receipt parsing): ``LiFiReceiptParser.parse_swap_receipt``
  with ``is_cross_chain=True`` (wallet-outgoing deposit sum), plus
  ``extract_bridge_data`` — the extractor the ResultEnricher invokes for
  BRIDGE bundles — called with the same hint kwargs the enricher threads
  from bundle metadata. ``destination_tx_hash`` is asserted None by
  design (async settlement).
- Layer 4 (balance deltas): source-chain USDC is asserted to decrease by
  EXACTLY the bridged amount. The deposit counterparty balance is NOT
  asserted (unlike Across/Stargate): LiFi picks the bridge tool per
  quote, so the receiving contract varies run-to-run. Source-chain
  native-token (ETH) drain is NOT asserted either — some tools carry a
  native msg.value fee, so the delta is not gas-only deterministic.

To run:
    ARBITRUM_RPC_URL=<rpc> uv run pytest tests/intents/arbitrum/test_lifi_bridge.py -v -s
"""

import time
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.lifi.receipt_parser import LiFiReceiptParser
from almanak.framework.execution.extracted_data import BridgeData
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
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
    """
    return tx_hash.lower().removeprefix("0x")


def _sync_anvil_time_to_wall_clock(web3: Web3) -> int:
    """Advance the Anvil fork's block timestamp to the current wall clock.

    LiFi may route the transfer through its Across facet, whose SpokePool
    rejects deposits with ``InvalidQuoteTimestamp()`` when the API-issued
    ``quoteTimestamp`` (wall-clock bound) falls outside the acceptable
    window relative to the fork's block timestamp. In CI the fork block
    can lag wall clock by minutes-to-hours. Same fix as
    test_across_bridge.py: snap the next block's timestamp to wall clock
    so any tool with a quote-freshness window accepts the deposit.
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
# Cross-chain SwapIntent (bridge) Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.bridge
class TestLiFiBridge:
    """LiFi cross-chain transfer tests exercising the bridge flow end-to-end.

    These tests verify the full cross-chain Intent flow on an Arbitrum
    Anvil fork:
    - Cross-chain SwapIntent creation (protocol="lifi", destination_chain set)
    - LiFiCompiler emits approve + a ``bridge_deferred`` Diamond transaction
    - Deferred refresh fetches fresh route calldata at execution time
    - The deposit executes against the real LiFi Diamond on the fork
    - LiFiReceiptParser.extract_bridge_data returns a typed BridgeData
      matching the on-chain deposit exactly (VIB-3226)
    - Source-chain USDC balance decreases by the exact bridged amount
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_bridge_usdc_arbitrum_to_optimism_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Bridge USDC Arbitrum -> Optimism via a cross-chain LiFi SwapIntent.

        Flow:
        1. Create SwapIntent USDC->USDC with destination_chain="optimism"
        2. Compile via IntentCompiler (approve + LiFi Diamond bridge_deferred)
        3. Execute via ExecutionOrchestrator on the Arbitrum Anvil fork
        4. Parse the deposit receipt: parse_swap_receipt (cross-chain) +
           extract_bridge_data -> typed BridgeData
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
        print("Test: USDC Arbitrum -> Optimism bridge via LiFi cross-chain SwapIntent")
        print(f"{'='*80}")
        print(f"Bridge amount: {bridge_amount} USDC")
        print(f"Fork time synced to wall clock: {synced_ts}")

        # --- Layer 4a: record balances BEFORE ---
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        print(f"Wallet USDC before: {format_token_amount(usdc_before, in_decimals)}")
        assert usdc_before >= expected_wei, "funded_wallet must have at least bridge_amount USDC"

        # --- Create cross-chain SwapIntent ---
        intent = SwapIntent(
            from_token="USDC",
            to_token="USDC",
            amount=bridge_amount,
            max_slippage=Decimal("0.05"),  # 5% slippage for aggregator routes
            protocol="lifi",
            chain=CHAIN_NAME,
            destination_chain=DEST_CHAIN,
        )
        assert intent.is_cross_chain, "destination_chain must make the intent cross-chain"
        print(f"\nCreated SwapIntent: USDC {CHAIN_NAME} -> USDC {DEST_CHAIN}")

        # --- Layer 1: Compilation ---
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        print("Compiling cross-chain SwapIntent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        bundle = compilation_result.action_bundle
        metadata = bundle.metadata
        print(f"ActionBundle created with {len(bundle.transactions)} transactions")
        print(f"Tool (compile-time route): {metadata.get('tool')}")

        assert metadata.get("protocol") == "lifi"
        assert metadata.get("deferred_swap") is True, "LiFi bundles must be deferred"
        assert metadata.get("is_cross_chain") is True, "Bundle must be flagged cross-chain"
        assert metadata.get("from_chain_id") == ARBITRUM_CHAIN_ID
        assert metadata.get("to_chain_id") == OPTIMISM_CHAIN_ID

        # The deferred-refresh route request is rebuilt from route_params at
        # execution time — these values ARE the routing input, so asserting
        # them is the LiFi analogue of decoding destinationChainId out of the
        # Across depositV3 calldata: it catches a compiler bug that resolves
        # "optimism" to the wrong chain id or drops the input amount.
        route_params = metadata.get("route_params")
        assert route_params is not None, "LiFi bundle must carry route_params for deferred refresh"
        assert route_params["from_chain_id"] == ARBITRUM_CHAIN_ID
        assert route_params["to_chain_id"] == OPTIMISM_CHAIN_ID
        assert route_params["from_amount"] == str(expected_wei), (
            f"route_params.from_amount must be the exact input amount. "
            f"Expected: {expected_wei}, got: {route_params['from_amount']}"
        )

        # The bundle must end with the cross-chain Diamond call; any leading
        # transactions are approvals for the Diamond.
        tx_types = [tx.get("tx_type") for tx in bundle.transactions]
        assert tx_types[-1] == "bridge_deferred", (
            f"Final transaction must be bridge_deferred, got tx_types={tx_types}"
        )
        for tx_type in tx_types[:-1]:
            assert tx_type in ("approve", "approve_reset"), (
                f"Unexpected leading tx_type={tx_type}; expected approve/approve_reset. Full list: {tx_types}"
            )

        # --- Layer 2: Execution (deferred refresh fetches a fresh route) ---
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
        # Cross-chain deposits move the wallet's tokens TO the bridge: both
        # amount_in and the cross-chain amount_out fallback resolve from the
        # wallet-outgoing USDC Transfer sum, which must equal the input exactly.
        assert parse_result.amount_in == expected_wei, (
            f"Wallet-outgoing USDC in deposit receipt must equal bridge amount. "
            f"Expected: {expected_wei}, got: {parse_result.amount_in}"
        )
        assert parse_result.amount_out == expected_wei, (
            f"Cross-chain amount_out (deposit into bridge) must equal bridge amount. "
            f"Expected: {expected_wei}, got: {parse_result.amount_out}"
        )
        assert _norm_hash(parse_result.tx_hash) == _norm_hash(deposit_result.tx_hash)
        print(
            f"parse_swap_receipt: amount_in={parse_result.amount_in}, "
            f"amount_out={parse_result.amount_out} (deposit into bridge)"
        )

        # --- Layer 3.5: Bridge extraction via extract_bridge_data (VIB-3226) ---
        # Exercise the extractor the ResultEnricher invokes for BRIDGE
        # bundles, on the real on-chain deposit receipt, threading the same
        # string-valued hint kwargs the enricher builds from bundle metadata
        # (ResultEnricher._build_extract_kwargs: from_chain / to_chain /
        # token / amount / bridge, expected_amount_out).
        extract_kwargs: dict = {
            "from_chain": CHAIN_NAME,
            "to_chain": DEST_CHAIN,
            "token": "USDC",
            "amount": str(bridge_amount),
            "bridge": "lifi",
        }
        expected_output_human = metadata.get("expected_output_human")
        if expected_output_human:
            extract_kwargs["expected_amount_out"] = expected_output_human

        bridge_data = parser.extract_bridge_data(deposit_receipt, **extract_kwargs)

        assert bridge_data is not None, (
            "LiFiReceiptParser must extract BridgeData from the deposit receipt"
        )
        assert isinstance(bridge_data, BridgeData)
        assert bridge_data.bridge_name == "lifi"
        assert bridge_data.source_chain == CHAIN_NAME
        assert bridge_data.destination_chain == DEST_CHAIN
        assert bridge_data.token_symbol == "USDC"
        assert bridge_data.amount_sent_raw == expected_wei, (
            f"BridgeData.amount_sent_raw must equal the on-chain deposit. "
            f"Expected: {expected_wei}, got: {bridge_data.amount_sent_raw}"
        )
        assert bridge_data.amount_sent == bridge_amount, (
            f"BridgeData.amount_sent must equal the human-readable bridge amount. "
            f"Expected: {bridge_amount}, got: {bridge_data.amount_sent}"
        )
        assert bridge_data.source_token_address == usdc_addr.lower(), (
            "BridgeData.source_token_address must be the source-chain USDC address"
        )
        assert _norm_hash(bridge_data.source_tx_hash) == _norm_hash(deposit_result.tx_hash)
        # Destination settlement is asynchronous (LiFi status API); the
        # source-chain parser must not fabricate destination fields.
        assert bridge_data.destination_tx_hash is None
        assert bridge_data.destination_token_address is None
        if expected_output_human:
            assert bridge_data.expected_amount_out == Decimal(str(expected_output_human))
        print(
            f"BridgeData extracted: bridge={bridge_data.bridge_name}, "
            f"amount={bridge_data.amount_sent} {bridge_data.token_symbol}, "
            f"{bridge_data.source_chain}->{bridge_data.destination_chain}, "
            f"expected_out={bridge_data.expected_amount_out}"
        )

        # --- Layer 4: Exact balance deltas (source chain) ---
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        usdc_spent = usdc_before - usdc_after

        print("\n--- Results (source chain only) ---")
        print(f"Wallet USDC spent: {format_token_amount(usdc_spent, in_decimals)}")

        assert usdc_spent == expected_wei, (
            f"Wallet USDC must decrease by exactly {expected_wei} wei, got {usdc_spent}"
        )

        # NOTE: destination-chain USDC settlement and the deposit
        # counterparty's balance are intentionally NOT asserted — the
        # counterparty contract varies with the tool LiFi picks per quote,
        # and destination delivery is off-fork. See module docstring.

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_bridge_insufficient_balance_fails_safely(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Cross-chain SwapIntent with insufficient balance fails + conserves.

        Compilation succeeds (LiFi quotes are balance-agnostic); execution
        must fail on-chain and leave the source-chain USDC balance
        unchanged (conservation check).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        in_decimals = get_token_decimals(web3, usdc_addr)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        assert usdc_before > 0, "funded_wallet must have USDC for this test"
        balance_decimal = Decimal(usdc_before) / Decimal(10**in_decimals)

        # 2x balance: guaranteed to fail on-chain, but small enough that the
        # LiFi API still quotes a route for the corridor (the same-chain
        # tests use 100x, but cross-chain tools carry per-transfer liquidity
        # caps that could reject a 100x quote at COMPILE time — this test
        # must fail at EXECUTION to exercise conservation).
        excessive_amount = balance_decimal * Decimal("2")

        _sync_anvil_time_to_wall_clock(web3)

        print(f"\n{'='*80}")
        print("Test: LiFi cross-chain SwapIntent with Insufficient Balance")
        print(f"{'='*80}")
        print(f"Balance: {balance_decimal} USDC")
        print(f"Trying:  {excessive_amount} USDC")

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDC",
            amount=excessive_amount,
            max_slippage=Decimal("0.05"),
            protocol="lifi",
            chain=CHAIN_NAME,
            destination_chain=DEST_CHAIN,
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

        # Conservation check — MANDATORY. The destination-side token lives on
        # another chain, so source-chain USDC conservation is the whole check.
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed bridge"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
