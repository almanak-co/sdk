"""Production-grade BridgeIntent tests for the Stargate bridge on BNB Chain.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for a
USDT bridge from BNB Chain to Arbitrum via Stargate V2:

1. Create BridgeIntent for USDT bsc -> arbitrum
2. Compile to ActionBundle via IntentCompiler (approve + send() on Stargate pool)
3. Execute on a BNB Chain Anvil fork via ExecutionOrchestrator
4. Parse the deposit receipt: verify OFTSent event + ERC-20 Transfer
   from wallet -> Stargate pool
5. Verify balance deltas on the source chain

NO MOCKING. All tests execute real on-chain deposits against the Stargate V2
USDT pool on a mainnet-forked Anvil and verify source-chain state changes.

Critical constraint: USDT-only.

Stargate on BSC has a **USDT-only pool** -- the router map at
``STARGATE_ROUTER_ADDRESSES[56]`` (almanak/connectors/stargate/adapter.py)
contains only ``USDT``. There is no Stargate V2 USDC or ETH pool on BSC, so
this test must bridge USDT specifically. Sibling-chain Stargate tests bridge
USDC because the BSC USDT-only pool was the documented exception in the
VIB-4343 audit.

Layer coverage on Anvil (single source-chain fork):

- Layer 1 (compilation): verified on-chain-independent (runs locally).
  The compiled send() calldata is decoded and the LayerZero dstEid is
  asserted to match Arbitrum (30110) -- this catches a compiler bug
  that silently routes to the wrong chain even when every other
  assertion would still pass.
- Layer 2 (execution): verified against the source-chain deposit tx
  (Stargate OFT pool send()). The LayerZero delivery on the destination
  chain is asynchronous and off-fork, so destination-chain settlement
  is NOT verifiable here. This is the documented bridge test limit --
  to verify destination settlement would require a second Anvil fork on
  the destination chain and an out-of-band LayerZero executor
  simulation.
- Layer 3 (receipt parsing): the Stargate connector ships
  ``StargateReceiptParser`` (the same parser the production
  ``ResultEnricher`` calls). We exercise it on the deposit receipt:
  ``parse_receipt(...)`` asserts on-chain status == 1, and
  ``extract_bridge_data(...)`` is asserted to return a typed
  ``BridgeData`` whose ``amount_sent`` matches the bridge amount,
  ``source_chain == "bsc"``, ``destination_chain == "arbitrum"``, and
  ``bridge_name == "stargate"``. Raw OFTSent / ERC-20 Transfer log
  checks are kept as extra invariants below the parser assertions.
- Layer 4 (balance deltas): source-chain USDT is asserted to decrease by
  EXACTLY the bridge amount, and the Stargate pool USDT balance is
  asserted to increase by the same amount. Source-chain native-token
  (BNB) drain is NOT asserted -- see the in-function NOTE for why.

To run:
    uv run pytest tests/intents/bnb/test_stargate_bridge.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.stargate.adapter import STARGATE_ROUTER_ADDRESSES
from almanak.connectors.stargate.receipt_parser import StargateReceiptParser
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

pytestmark = pytest.mark.no_zodiac(reason="Phase E BRIDGE not landed: stargate not in synthetic-intents matrix")

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "bsc"
BSC_CHAIN_ID = 56
DEST_CHAIN = "arbitrum"
# LayerZero endpoint ID for Arbitrum (used as dstEid on the Stargate send()).
ARBITRUM_LZ_EID = 30110

# Standard EVM event topics
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# keccak("OFTSent(bytes32,uint32,address,uint256,uint256)")
STARGATE_OFT_SENT_TOPIC = "0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a"


# =============================================================================
# BridgeIntent Tests
# =============================================================================


@pytest.mark.bsc
@pytest.mark.bridge
class TestStargateBridgeIntent:
    """Stargate V2 bridge tests exercising BridgeIntent end-to-end.

    These tests verify the full BridgeIntent flow on a BNB Chain Anvil fork:
    - BridgeIntent creation with proper parameters
    - IntentCompiler generates approve + Stargate send() transactions
    - Transactions execute successfully against the real Stargate USDT pool
    - Receipt contains the expected OFTSent event + ERC-20 Transfer
    - Source-chain USDT balance decreases by the bridged amount
    """

    @pytest.mark.intent(IntentType.BRIDGE)
    @pytest.mark.asyncio
    async def test_bridge_usdt_bsc_to_arbitrum_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Bridge USDT BSC -> Arbitrum via Stargate V2 using BridgeIntent.

        Flow:
        1. Create BridgeIntent for USDT bsc -> arbitrum (preferred Stargate)
        2. Compile via IntentCompiler (approve + Stargate pool send())
        3. Execute via ExecutionOrchestrator on BSC Anvil fork
        4. Parse the deposit receipt with ``StargateReceiptParser`` (the
           same parser ``ResultEnricher`` runs in production) and verify
           the typed ``BridgeData`` plus raw OFTSent / Transfer invariants
        5. Verify source-chain USDT balance decreased by exact bridge amount

        USDT-only constraint: Stargate V2 on BSC has only a USDT pool (no
        USDC or ETH pools). The router map ``STARGATE_ROUTER_ADDRESSES[56]``
        in ``almanak/connectors/stargate/adapter.py`` is the
        single source of truth. BSC USDT is 18 decimals (Binance-Peg USDT),
        not the 6 decimals other chains' USDT/USDC use -- the test asserts
        the decimals explicitly to catch a chain-mapping regression.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]

        in_decimals = get_token_decimals(web3, usdt_addr)
        # Binance-Peg USDT on BSC uses 18 decimals (unlike ERC-20 USDT on
        # other chains which uses 6). Pin the value explicitly so a chain
        # config drift or token-address swap fails loudly here rather than
        # producing a wrong amount_wei downstream.
        assert in_decimals == 18, f"BSC USDT is 18 decimals, got {in_decimals}"

        # Stargate V2 USDT pool on BSC.
        stargate_pool_addr = STARGATE_ROUTER_ADDRESSES[BSC_CHAIN_ID]["USDT"]
        stargate_pool_checksummed = Web3.to_checksum_address(stargate_pool_addr)

        # Small amount (Stargate V2 USDT pool supports small transfers; fee
        # is ~0.06% of notional).
        bridge_amount = Decimal("5")  # 5 USDT

        print(f"\n{'=' * 80}")
        print("Test: USDT BSC -> Arbitrum bridge via Stargate BridgeIntent")
        print(f"{'=' * 80}")
        print(f"Bridge amount:      {bridge_amount} USDT")
        print(f"Stargate pool addr: {stargate_pool_addr}")

        # --- Layer 4a: record balances BEFORE ---
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        pool_usdt_before = get_token_balance(web3, usdt_addr, stargate_pool_checksummed)

        print(f"Wallet USDT before:     {format_token_amount(usdt_before, in_decimals)}")
        print(f"Pool USDT before:       {format_token_amount(pool_usdt_before, in_decimals)}")
        assert usdt_before >= int(bridge_amount * Decimal(10**in_decimals)), (
            "funded_wallet must have at least bridge_amount USDT"
        )

        # --- Create BridgeIntent ---
        intent = BridgeIntent(
            token="USDT",
            amount=bridge_amount,
            from_chain=CHAIN_NAME,
            to_chain=DEST_CHAIN,
            max_slippage=Decimal("0.01"),
            preferred_bridge="Stargate",
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

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
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
        assert tx_types[-1] == "bridge_deposit", f"Final transaction must be bridge_deposit, got tx_types={tx_types}"
        for tx_type in tx_types[:-1]:
            assert tx_type in ("approve", "approve_reset"), (
                f"Unexpected leading tx_type={tx_type}; expected approve/approve_reset. Full list: {tx_types}"
            )

        # Deposit destination must be the Stargate USDT pool on BSC
        deposit_tx = bundle.transactions[-1]
        assert deposit_tx["to"].lower() == stargate_pool_addr.lower(), (
            f"Deposit must target Stargate pool {stargate_pool_addr}, got {deposit_tx['to']}"
        )
        # Stargate V2 send() selector = 0xc7c7f5b3 for
        # send((uint32,bytes32,uint256,uint256,bytes,bytes,bytes),(uint256,uint256),address)
        assert deposit_tx["data"].lower().startswith("0xc7c7f5b3"), (
            f"Deposit calldata must use Stargate send() selector, got {deposit_tx['data'][:10]}"
        )

        # Decode the destination LayerZero endpoint id (dstEid) from the
        # send() calldata. Stargate routes entirely on this field; asserting
        # the metadata alone reads back what the compiler wrote, whereas
        # decoding the calldata catches a compiler bug that translates
        # "arbitrum" to the wrong LZ id. Stargate V2 send() signature:
        #   send(SendParam, MessagingFee, address)
        # where SendParam is an ABI tuple (uint32 dstEid, bytes32 to,
        # uint256 amountLD, uint256 minAmountLD, bytes extraOptions,
        # bytes composeMsg, bytes oftCmd). Because SendParam contains
        # dynamic `bytes` fields, Solidity ABI-encodes it as a dynamic
        # tuple: the outer call's first static slot (bytes [4:36]) is an
        # ABI offset pointer to SendParam's data. dstEid sits at the
        # first 32-byte slot of that tuple, right-aligned in its uint256
        # word.
        deposit_calldata = bytes.fromhex(deposit_tx["data"].removeprefix("0x"))
        send_param_offset = int.from_bytes(deposit_calldata[4:36], "big")
        # dstEid is the first word inside SendParam; uint32 right-aligned in
        # a 32-byte word, so the lowest 4 bytes carry the value.
        dst_eid_word = deposit_calldata[4 + send_param_offset : 4 + send_param_offset + 32]
        encoded_dst_eid = int.from_bytes(dst_eid_word, "big")
        assert encoded_dst_eid == ARBITRUM_LZ_EID, (
            f"Stargate send() calldata must encode dstEid={ARBITRUM_LZ_EID} (Arbitrum LZ id), got {encoded_dst_eid}"
        )

        # Native fee (LZ messaging) must be attached as tx value for ERC-20 bridges
        # and must be within a sane bound. The Stargate adapter applies a 3x
        # safety multiplier on a base fee (~0.001 BNB source-chain default for
        # BSC routes); for a 5 USDT bridge that budget is well under 0.1 BNB.
        # We cap at 0.1 BNB (~0.1 native unit) to catch a class of bugs where
        # the compiler accidentally sets value to the bridged token amount
        # (which for an 18-decimal USDT would be 5e18 wei, far exceeding the
        # cap).
        deposit_value = int(deposit_tx.get("value", 0))
        assert deposit_value > 0, "Stargate deposit must carry a nonzero native value (LayerZero fee)"
        max_reasonable_fee_wei = int(Decimal("0.1") * Decimal(10**18))
        assert deposit_value < max_reasonable_fee_wei, (
            f"Stargate native fee looks unreasonable: {deposit_value} wei >= {max_reasonable_fee_wei} wei "
            f"(0.1 BNB). Possible compiler bug setting value to the bridged token amount."
        )

        metadata = bundle.metadata
        assert metadata["bridge"].lower() == "stargate", f"Expected Stargate bridge, got {metadata['bridge']}"
        assert metadata["from_chain"] == CHAIN_NAME
        assert metadata["to_chain"] == DEST_CHAIN
        assert metadata["token"] == "USDT"
        assert metadata["is_cross_chain"] is True

        # --- Layer 2: Execution ---
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        assert len(execution_result.transaction_results) == len(bundle.transactions), (
            f"Expected {len(bundle.transactions)} tx results, got {len(execution_result.transaction_results)}"
        )
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        deposit_result = execution_result.transaction_results[-1]
        assert deposit_result.receipt is not None, "deposit tx must have a receipt"
        for idx, tx_result in enumerate(execution_result.transaction_results[:-1]):
            assert tx_result.receipt is not None, f"leading approve tx #{idx} must have a receipt"

        print(f"\nDeposit tx:  {deposit_result.tx_hash[:16]}... gas={deposit_result.gas_used}")

        # --- Layer 3: Receipt parsing ---
        # Stargate ships ``StargateReceiptParser`` (the same parser
        # ``ResultEnricher`` runs in production). We exercise both of its
        # public entry points on the deposit receipt:
        #   a) ``parse_receipt(...)`` — used by the receipt cache to
        #      memoise per-tx state. We assert it reports ``status == 1``
        #      and a non-empty tx hash.
        #   b) ``extract_bridge_data(...)`` — returns a typed ``BridgeData``
        #      that the framework forwards to ``AccountingWriter``. We
        #      assert it decodes the same bridge amount the wallet
        #      actually sent, attributes it to the right source/dest
        #      chains, and labels it as ``stargate``.
        # Raw OFTSent / ERC-20 Transfer invariants stay below as
        # extra defence — they catch the case where the parser is
        # wrong but the on-chain logs are still well-formed.
        deposit_receipt = deposit_result.receipt.to_dict()
        deposit_logs = deposit_receipt.get("logs", [])

        # On success the deposit tx status must be 1 (symmetric with the
        # Across test; execution_result.success covers it upstream but the
        # explicit check gives better forensic evidence on failure).
        assert deposit_receipt.get("status") == 1, (
            f"Deposit tx did not succeed on-chain: status={deposit_receipt.get('status')}"
        )

        # Layer 3a: ``StargateReceiptParser.parse_receipt`` — minimal view.
        parser = StargateReceiptParser(chain=CHAIN_NAME)
        parse_result = parser.parse_receipt(deposit_receipt)
        assert parse_result["status"] == 1, (
            f"StargateReceiptParser.parse_receipt must report status=1, got {parse_result['status']}"
        )
        assert parse_result["tx_hash"], "StargateReceiptParser.parse_receipt must report a non-empty tx hash"

        # Layer 3b: ``StargateReceiptParser.extract_bridge_data`` — typed view.
        bridge_data = parser.extract_bridge_data(
            deposit_receipt,
            from_chain=CHAIN_NAME,
            to_chain=DEST_CHAIN,
            token="USDT",
            amount=str(bridge_amount),
            bridge="stargate",
        )
        expected_wei = int(bridge_amount * Decimal(10**in_decimals))
        assert bridge_data is not None, (
            "StargateReceiptParser.extract_bridge_data must return a BridgeData for a successful Stargate deposit"
        )
        assert bridge_data.bridge_name == "stargate", (
            f"BridgeData.bridge_name must be 'stargate', got {bridge_data.bridge_name!r}"
        )
        assert bridge_data.source_chain == CHAIN_NAME, (
            f"BridgeData.source_chain must be {CHAIN_NAME!r}, got {bridge_data.source_chain!r}"
        )
        assert bridge_data.destination_chain == DEST_CHAIN, (
            f"BridgeData.destination_chain must be {DEST_CHAIN!r}, got {bridge_data.destination_chain!r}"
        )
        assert bridge_data.token_symbol == "USDT", (
            f"BridgeData.token_symbol must be 'USDT', got {bridge_data.token_symbol!r}"
        )
        assert bridge_data.amount_sent_raw == expected_wei, (
            f"BridgeData.amount_sent_raw must equal bridge amount in wei. "
            f"Expected: {expected_wei}, got: {bridge_data.amount_sent_raw}"
        )
        assert bridge_data.amount_sent == bridge_amount, (
            f"BridgeData.amount_sent must equal bridge amount as Decimal. "
            f"Expected: {bridge_amount}, got: {bridge_data.amount_sent}"
        )
        print(
            f"StargateReceiptParser.extract_bridge_data: "
            f"bridge={bridge_data.bridge_name}, "
            f"{bridge_data.source_chain} -> {bridge_data.destination_chain}, "
            f"amount_sent={bridge_data.amount_sent} {bridge_data.token_symbol}"
        )

        def _norm(topic: str) -> str:
            return topic.lower() if topic else ""

        # a) OFTSent present on the pool
        oft_sent_logs = [
            log
            for log in deposit_logs
            if log.get("topics")
            and _norm(log["topics"][0]) == STARGATE_OFT_SENT_TOPIC
            and log.get("address", "").lower() == stargate_pool_addr.lower()
        ]
        assert len(oft_sent_logs) == 1, f"Expected exactly 1 OFTSent event on Stargate pool, got {len(oft_sent_logs)}"

        # b) ERC-20 Transfer from funded_wallet -> pool with positive value
        wallet_topic = "0x" + funded_wallet[2:].lower().rjust(64, "0")
        pool_topic = "0x" + stargate_pool_addr[2:].lower().rjust(64, "0")

        matching_transfers: list[int] = []
        for log in deposit_logs:
            topics = log.get("topics", [])
            if len(topics) < 3 or _norm(topics[0]) != ERC20_TRANSFER_TOPIC:
                continue
            if log.get("address", "").lower() != usdt_addr.lower():
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
            f"Expected exactly 1 ERC-20 Transfer(wallet -> Stargate pool) on USDT, got {len(matching_transfers)}"
        )
        total_transferred_wei = matching_transfers[0]
        expected_wei = int(bridge_amount * Decimal(10**in_decimals))
        assert total_transferred_wei == expected_wei, (
            f"Transfer(wallet -> pool) must equal bridge amount. Expected: {expected_wei}, got: {total_transferred_wei}"
        )
        print(
            f"Parsed deposit events: OFTSent=1, "
            f"Transfer(wallet->pool)={len(matching_transfers)}, "
            f"total={format_token_amount(total_transferred_wei, in_decimals)} USDT"
        )

        # --- Layer 4: Exact balance deltas (source chain) ---
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        pool_usdt_after = get_token_balance(web3, usdt_addr, stargate_pool_checksummed)

        usdt_spent = usdt_before - usdt_after
        pool_gained = pool_usdt_after - pool_usdt_before

        print("\n--- Results (source chain only) ---")
        print(f"Wallet USDT spent:       {format_token_amount(usdt_spent, in_decimals)}")
        print(f"Pool USDT gained:        {format_token_amount(pool_gained, in_decimals)}")

        assert usdt_spent == expected_wei, f"Wallet USDT must decrease by exactly {expected_wei} wei, got {usdt_spent}"
        assert pool_gained == expected_wei, f"Pool USDT must increase by exactly {expected_wei} wei, got {pool_gained}"
        # NOTE: a full BNB-balance conservation check was intentionally NOT
        # added here. The StargateBridgeAdapter (see
        # connectors/stargate/adapter.py
        # `_estimate_layerzero_fee`) applies a **3x safety multiplier** on
        # the base LayerZero messaging fee so a live bridge won't revert on
        # fee underestimation. Combined with the absence of the off-chain
        # LayerZero DVN/executor refund flow on a single-chain Anvil fork,
        # the pool's `send()` drains substantially more native BNB than the
        # real cost. This is a known artifact of paper-trading Stargate on
        # a single-chain fork (no DVN/executor economy to settle refunds)
        # and does NOT affect the source-chain USDT bilateral accounting
        # asserted above.

        # NOTE: destination chain USDT settlement (via LayerZero message) is
        # intentionally NOT asserted here -- it is off-fork and asynchronous.
        # See module docstring for the rationale.

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
