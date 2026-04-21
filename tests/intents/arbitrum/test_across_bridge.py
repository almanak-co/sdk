"""Production-grade BridgeIntent tests for the Across bridge on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for a
USDC bridge from Arbitrum to Optimism via Across Protocol V3:

1. Create BridgeIntent for USDC arbitrum -> optimism
2. Compile to ActionBundle via IntentCompiler (approve + depositV3 on SpokePool)
3. Execute on an Arbitrum Anvil fork via ExecutionOrchestrator
4. Parse the deposit receipt: verify FundsDeposited event + ERC-20 Transfer
   from wallet -> SpokePool
5. Verify balance deltas on the source chain

NO MOCKING. All tests execute real on-chain deposits against the Across V3
SpokePool on a mainnet-forked Anvil and verify source-chain state changes.

Layer coverage on Anvil (single source-chain fork):

- Layer 1 (compilation): verified on-chain-independent (runs locally).
  The compiled depositV3 calldata is decoded and destinationChainId is
  asserted to match Optimism (10) -- this catches a compiler bug that
  silently routes to the wrong chain even when every other assertion
  would still pass.
- Layer 2 (execution): verified against the source-chain deposit tx
  (SpokePool.depositV3). The relayer fill on the destination chain is
  asynchronous and off-fork, so destination-chain settlement is NOT
  verifiable here. This is the documented bridge test limit -- to verify
  destination settlement would require a second Anvil fork on the
  destination chain and an out-of-band relayer simulation.
- Layer 3 (receipt parsing): the bridge connectors do not ship a
  dedicated ReceiptParser class (bridges are stateless routers from the
  source chain's point of view). We verify the deposit on-chain by
  asserting the SpokePool-emitted FundsDeposited event topic is present
  and by decoding the ERC-20 Transfer event (wallet -> SpokePool) from
  the raw receipt logs. This satisfies Layer 3.
- Layer 4 (balance deltas): source-chain USDC is asserted to decrease by
  EXACTLY the bridge amount. Source-chain approve is asserted to have run.

To run:
    uv run pytest tests/intents/arbitrum/test_across_bridge.py -v -s
"""

import time
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.bridges.across.adapter import ACROSS_SPOKE_POOL_ADDRESSES
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import BridgeIntent
from almanak.framework.intents.compiler import IntentCompiler
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)


def _sync_anvil_time_to_wall_clock(web3: Web3) -> int:
    """Advance the Anvil fork's block timestamp to the current wall clock.

    The Across V3 SpokePool rejects deposits with `InvalidQuoteTimestamp()`
    (selector 0xf722177f) when the deposit's `quoteTimestamp` is outside an
    acceptable window relative to the on-chain block timestamp. The Across
    API issues quote timestamps bound to mainnet block timestamps (~wall
    clock), while an Anvil fork inherits the timestamp of the mainnet block
    it forked from. In CI, the fork block can be minutes-to-hours behind
    wall clock -- causing Across to reject the deposit even though the
    adapter computed the quote correctly.

    Fix: snap the fork's next block timestamp to wall clock so the
    wall-clock-derived `quoteTimestamp` sits inside the SpokePool's
    acceptable window. Returns the synced timestamp.
    """
    current_block = web3.eth.get_block("latest")
    current_block_ts = current_block["timestamp"]
    wall_clock_ts = int(time.time())
    if wall_clock_ts > current_block_ts:
        # Advance fork time to wall clock and mine one block.
        web3.provider.make_request("evm_setNextBlockTimestamp", [wall_clock_ts])
        web3.provider.make_request("evm_mine", [])
    return wall_clock_ts

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"
ARBITRUM_CHAIN_ID = 42161
DEST_CHAIN = "optimism"
# Ethereum mainnet chain IDs used by Across' cross-chain routing.
OPTIMISM_CHAIN_ID = 10

# Standard EVM event topics
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# keccak(
#   "FundsDeposited(bytes32,bytes32,uint256,uint256,uint256,uint256,"
#   "uint32,uint32,uint32,bytes32,bytes32,bytes32,bytes)"
# )
# Emitted by Across SpokePool V3.5+ (the upgraded bytes32 variant of the old
# `V3FundsDeposited(address,...)` event). The Arbitrum SpokePool at
# 0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A emits this topic on depositV3()
# calls; the legacy `V3FundsDeposited` topic is no longer emitted.
ACROSS_FUNDS_DEPOSITED_TOPIC = "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3"


# =============================================================================
# BridgeIntent Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.bridge
class TestAcrossBridgeIntent:
    """Across Protocol bridge tests exercising BridgeIntent end-to-end.

    These tests verify the full BridgeIntent flow on an Arbitrum Anvil fork:
    - BridgeIntent creation with proper parameters
    - IntentCompiler generates approve + Across depositV3 transactions
    - Transactions execute successfully against the real Arbitrum SpokePool
    - Receipt contains the expected FundsDeposited event + Transfer
    - Source-chain USDC balance decreases by the bridged amount
    """

    @pytest.mark.asyncio
    async def test_bridge_usdc_arbitrum_to_optimism_using_intent(  # noqa: layers
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Bridge USDC Arbitrum -> Optimism via Across using BridgeIntent.

        # noqa: layers -- Bridges don't ship a dedicated ReceiptParser; Layer 3
        # is satisfied by direct event-topic decoding (FundsDeposited +
        # ERC-20 Transfer) of the source-chain deposit receipt, as
        # documented in the module docstring.

        Flow:
        1. Create BridgeIntent for USDC arbitrum -> optimism
        2. Compile via IntentCompiler (approve + depositV3 on SpokePool)
        3. Execute via ExecutionOrchestrator on Arbitrum Anvil fork
        4. Parse the deposit receipt: verify FundsDeposited event + Transfer
        5. Verify source-chain USDC balance decreased by exact bridge amount
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        in_decimals = get_token_decimals(web3, usdc_addr)
        assert in_decimals == 6, f"Arbitrum USDC is 6 decimals, got {in_decimals}"

        # Small amount (well under SpokePool min transfer and Across API limits
        # for non-exclusive routes).
        bridge_amount = Decimal("5")  # 5 USDC

        spoke_pool_addr = ACROSS_SPOKE_POOL_ADDRESSES[ARBITRUM_CHAIN_ID]
        spoke_pool_checksummed = Web3.to_checksum_address(spoke_pool_addr)

        # Snap the fork's block timestamp to wall clock before the Across
        # adapter fetches a quote -- see _sync_anvil_time_to_wall_clock
        # docstring. Without this, the Arbitrum SpokePool reverts with
        # InvalidQuoteTimestamp() (selector 0xf722177f) in CI, where the
        # fork's block timestamp can sit far behind wall clock.
        synced_ts = _sync_anvil_time_to_wall_clock(web3)

        print(f"\n{'='*80}")
        print("Test: USDC Arbitrum -> Optimism bridge via Across BridgeIntent")
        print(f"{'='*80}")
        print(f"Bridge amount:   {bridge_amount} USDC")
        print(f"SpokePool addr:  {spoke_pool_addr}")
        print(f"Fork time synced to wall clock: {synced_ts}")

        # --- Layer 4a: record balances BEFORE ---
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        pool_usdc_before = get_token_balance(web3, usdc_addr, spoke_pool_checksummed)
        print(f"Wallet USDC before:     {format_token_amount(usdc_before, in_decimals)}")
        print(f"SpokePool USDC before:  {format_token_amount(pool_usdc_before, in_decimals)}")
        assert usdc_before >= int(bridge_amount * Decimal(10**in_decimals)), (
            "funded_wallet must have at least bridge_amount USDC"
        )

        # --- Create BridgeIntent ---
        intent = BridgeIntent(
            token="USDC",
            amount=bridge_amount,
            from_chain=CHAIN_NAME,
            to_chain=DEST_CHAIN,
            max_slippage=Decimal("0.01"),
            preferred_bridge="Across",
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
        print(f"ActionBundle created with {len(bundle.transactions)} transactions")

        # ERC-20 bridge must always end with a bridge_deposit transaction; the
        # compiler may emit zero, one, or two approve transactions (approve /
        # approve_reset + approve) depending on the wallet's current allowance,
        # so we identify the deposit as the trailing transaction instead of
        # pinning the bundle length.
        tx_types = [tx.get("tx_type") for tx in bundle.transactions]
        assert len(bundle.transactions) >= 1, "ActionBundle must contain at least the bridge_deposit transaction"
        assert tx_types[-1] == "bridge_deposit", (
            f"Final transaction must be bridge_deposit, got tx_types={tx_types}"
        )
        # Every non-deposit leading tx must be an approve / approve_reset
        for tx_type in tx_types[:-1]:
            assert tx_type in ("approve", "approve_reset"), (
                f"Unexpected leading tx_type={tx_type}; expected approve/approve_reset. Full list: {tx_types}"
            )

        # Deposit destination must be the Across Arbitrum SpokePool
        deposit_tx = bundle.transactions[-1]
        assert deposit_tx["to"].lower() == spoke_pool_addr.lower(), (
            f"Deposit must target SpokePool {spoke_pool_addr}, got {deposit_tx['to']}"
        )
        # depositV3 selector = 0x7b939232
        assert deposit_tx["data"].lower().startswith("0x7b939232"), (
            f"Deposit calldata must use depositV3 selector, got {deposit_tx['data'][:10]}"
        )

        # Decode the destinationChainId from the depositV3 calldata. Across
        # routes entirely on this field -- asserting metadata alone reads back
        # what the compiler wrote; asserting the calldata catches a compiler
        # bug that translates "optimism" to the wrong chain id. depositV3
        # signature ordering (see across/adapter.py _build_depositv3_calldata):
        #   [depositor, recipient, inputToken, outputToken,
        #    inputAmount, outputAmount, destinationChainId, ...]
        # destinationChainId is the 7th fixed 32-byte slot after the 4-byte
        # selector, i.e. bytes [4 + 32*6 : 4 + 32*7] = [196:228].
        deposit_calldata = bytes.fromhex(deposit_tx["data"].removeprefix("0x"))
        encoded_dest_chain_id = int.from_bytes(deposit_calldata[196:228], "big")
        assert encoded_dest_chain_id == OPTIMISM_CHAIN_ID, (
            f"depositV3 calldata must encode destinationChainId={OPTIMISM_CHAIN_ID} (Optimism), "
            f"got {encoded_dest_chain_id}"
        )

        metadata = bundle.metadata
        assert metadata["bridge"].lower() == "across", f"Expected Across bridge, got {metadata['bridge']}"
        assert metadata["from_chain"] == CHAIN_NAME
        assert metadata["to_chain"] == DEST_CHAIN
        assert metadata["token"] == "USDC"
        assert metadata["is_cross_chain"] is True

        # --- Layer 2: Execution ---
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        assert len(execution_result.transaction_results) == len(bundle.transactions), (
            f"Expected {len(bundle.transactions)} tx results, "
            f"got {len(execution_result.transaction_results)}"
        )
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        deposit_result = execution_result.transaction_results[-1]
        assert deposit_result.receipt is not None, "deposit tx must have a receipt"
        for idx, tx_result in enumerate(execution_result.transaction_results[:-1]):
            assert tx_result.receipt is not None, f"leading approve tx #{idx} must have a receipt"

        print(f"\nDeposit tx:  {deposit_result.tx_hash[:16]}... gas={deposit_result.gas_used}")

        # --- Layer 3: Receipt parsing ---
        # Bridges don't ship a dedicated ReceiptParser. We assert the deposit
        # receipt contains:
        #   a) The Across FundsDeposited event on the SpokePool
        #   b) An ERC-20 Transfer event pulling USDC from the wallet
        deposit_receipt = deposit_result.receipt.to_dict()
        deposit_logs = deposit_receipt.get("logs", [])

        # On success the deposit tx status must be 1
        assert deposit_receipt.get("status") == 1, (
            f"Deposit tx did not succeed on-chain: status={deposit_receipt.get('status')}"
        )

        def _norm(topic: str) -> str:
            return topic.lower() if topic else ""

        # a) FundsDeposited present, emitted by the SpokePool
        funds_deposited_logs = [
            log
            for log in deposit_logs
            if log.get("topics")
            and _norm(log["topics"][0]) == ACROSS_FUNDS_DEPOSITED_TOPIC
            and log.get("address", "").lower() == spoke_pool_addr.lower()
        ]
        assert len(funds_deposited_logs) == 1, (
            f"Expected exactly 1 FundsDeposited event on SpokePool, "
            f"got {len(funds_deposited_logs)}"
        )

        # b) ERC-20 Transfer from funded_wallet -> SpokePool with positive value
        wallet_topic = "0x" + funded_wallet[2:].lower().rjust(64, "0")
        pool_topic = "0x" + spoke_pool_addr[2:].lower().rjust(64, "0")

        matching_transfers: list[int] = []
        for log in deposit_logs:
            topics = log.get("topics", [])
            if len(topics) < 3 or _norm(topics[0]) != ERC20_TRANSFER_TOPIC:
                continue
            if log.get("address", "").lower() != usdc_addr.lower():
                continue
            if _norm(topics[1]) != wallet_topic.lower():
                continue
            if _norm(topics[2]) != pool_topic.lower():
                continue
            data_hex = log.get("data", "0x")
            if data_hex.startswith("0x"):
                data_hex = data_hex[2:]
            if data_hex:
                matching_transfers.append(int(data_hex, 16))

        assert len(matching_transfers) == 1, (
            f"Expected exactly 1 ERC-20 Transfer(wallet -> SpokePool) on USDC, "
            f"got {len(matching_transfers)}"
        )
        total_transferred_wei = matching_transfers[0]
        expected_wei = int(bridge_amount * Decimal(10**in_decimals))
        assert total_transferred_wei == expected_wei, (
            f"Transfer(wallet -> SpokePool) must equal bridge amount. "
            f"Expected: {expected_wei}, got: {total_transferred_wei}"
        )
        print(
            f"Parsed deposit events: FundsDeposited=1, "
            f"Transfer(wallet->SpokePool)={len(matching_transfers)}, "
            f"total={format_token_amount(total_transferred_wei, in_decimals)} USDC"
        )

        # --- Layer 3.5: Bridge extraction via AcrossReceiptParser (VIB-3226) ---
        # Exercise the parser the ResultEnricher would invoke for BRIDGE intents
        # on a real on-chain receipt, and assert the typed BridgeData payload
        # matches the deposit's actual amount, token, and route.
        from almanak.framework.connectors.bridges.across.receipt_parser import (
            AcrossReceiptParser,
        )

        parser = AcrossReceiptParser(chain=CHAIN_NAME)
        bridge_data = parser.extract_bridge_data(
            deposit_receipt,
            from_chain=CHAIN_NAME,
            to_chain=DEST_CHAIN,
            token="USDC",
            amount=bridge_amount,
            bridge="across",
        )
        assert bridge_data is not None, "AcrossReceiptParser must extract BridgeData from the deposit receipt"
        assert bridge_data.bridge_name == "across"
        assert bridge_data.source_chain == CHAIN_NAME
        assert bridge_data.destination_chain == DEST_CHAIN
        assert bridge_data.token_symbol == "USDC"
        assert bridge_data.amount_sent_raw == expected_wei, (
            f"BridgeData.amount_sent_raw must equal bridge amount. "
            f"Expected: {expected_wei}, got: {bridge_data.amount_sent_raw}"
        )
        assert bridge_data.amount_sent == bridge_amount, (
            f"BridgeData.amount_sent must equal human-readable bridge amount. "
            f"Expected: {bridge_amount}, got: {bridge_data.amount_sent}"
        )
        assert bridge_data.source_tx_hash.lower() == deposit_result.tx_hash.lower()
        print(
            f"BridgeData extracted: bridge={bridge_data.bridge_name}, "
            f"amount={bridge_data.amount_sent} {bridge_data.token_symbol}, "
            f"{bridge_data.source_chain}->{bridge_data.destination_chain}"
        )

        # --- Layer 3.6: Full ResultEnricher round-trip (VIB-3226) ---
        # Drive the same path the StrategyRunner uses in production: invoke
        # ResultEnricher on the ExecutionResult and assert bridge_data ends
        # up on the result object (not just on a parser return value).
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
        assert enriched.bridge_data.bridge_name == "across"
        assert enriched.bridge_data.amount_sent_raw == expected_wei
        assert enriched.bridge_data.source_chain == CHAIN_NAME
        assert enriched.bridge_data.destination_chain == DEST_CHAIN
        assert enriched.bridge_data.token_symbol == "USDC"

        # --- Layer 4: Exact balance deltas (source chain) ---
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        pool_usdc_after = get_token_balance(web3, usdc_addr, spoke_pool_checksummed)

        usdc_spent = usdc_before - usdc_after
        pool_gained = pool_usdc_after - pool_usdc_before

        print("\n--- Results (source chain only) ---")
        print(f"Wallet USDC spent:       {format_token_amount(usdc_spent, in_decimals)}")
        print(f"SpokePool USDC gained:   {format_token_amount(pool_gained, in_decimals)}")

        assert usdc_spent == expected_wei, (
            f"Wallet USDC must decrease by exactly {expected_wei} wei, got {usdc_spent}"
        )
        assert pool_gained == expected_wei, (
            f"SpokePool USDC must increase by exactly {expected_wei} wei, got {pool_gained}"
        )

        # NOTE: destination chain USDC settlement (via Across relayer) is
        # intentionally NOT asserted here -- it is off-fork and asynchronous.
        # See module docstring for the rationale.

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
