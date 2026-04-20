"""Intent-level tests for PancakeSwap Perps PERP_CLOSE on BSC.

Exercises the new ``PerpCloseIntent.position_id`` field that lets strategies
close ApolloX positions through the IntentCompiler instead of the
direct-SDK ``build_close_transaction`` workaround.

Lifecycle (mirrors ``test_pancakeswap_perps_close.py`` but routes the close
through ``IntentCompiler.compile(PerpCloseIntent(...))``):

  1. Open a position via direct-SDK (we want to reach a state where there is
     an open position whose ``tradeHash`` we can hand to PerpCloseIntent).
  2. Impersonate a PRICE_FEEDER_ROLE keeper to settle the open into a real
     on-chain position.
  3. Compile a ``PerpCloseIntent(position_id=tradeHash, protocol='pancakeswap_perps')``
     -- this is the path we are validating.
  4. Execute via the orchestrator.
  5. Keeper-settle the close.
  6. Verify all 4 layers (compile, execute, parse, balance deltas).

To run:
    uv run pytest tests/intents/bnb/test_pancakeswap_perps_close_intent.py -v -s
"""

import time
from decimal import Decimal

import pytest
from pydantic import ValidationError
from web3 import Web3

from almanak.core.contracts import PANCAKESWAP_PERPS
from almanak.framework.connectors.pancakeswap_perps import (
    PancakeSwapPerpsReceiptParser,
    encode_get_pending_trade_calldata,
    encode_get_position_by_hash_calldata,
)
from almanak.framework.connectors.pancakeswap_perps.sdk import (
    OpenTradeStruct,
    encode_open_market_trade_calldata,
    slippage_to_limit_price,
    usd_size_to_qty,
)
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.perp_intents import PerpCloseIntent
from tests.intents.bnb.conftest import (
    pcs_perps_extract_price_request_id,
    pcs_perps_keeper_fulfill,
)

CHAIN_NAME = "bsc"


@pytest.fixture(scope="session")
def perps_price_oracle() -> dict[str, Decimal]:
    """Static prices (matches the open-intent test for consistency)."""
    return {
        "BTC": Decimal("95000"),
        "ETH": Decimal("3500"),
        "BNB": Decimal("600"),
        "WBNB": Decimal("600"),
        "USDT": Decimal("1"),
        "USDC": Decimal("1"),
    }


@pytest.mark.bsc
@pytest.mark.asyncio
class TestPancakeSwapPerpsCloseViaIntent:
    """Test PancakeSwap Perps PERP_CLOSE through the IntentCompiler."""

    async def test_close_btc_long_via_intent_compiler(
        self,
        web3: Web3,
        funded_wallet: str,
        test_private_key: str,
        anvil_rpc_url: str,
        orchestrator: ExecutionOrchestrator,
        perps_price_oracle: dict[str, Decimal],
    ):
        """Open -> keeper-fill -> CLOSE-via-intent -> keeper-settle, verify 4 layers.

        The test focuses on layers for the CLOSE step. The OPEN + first keeper
        fill use the same direct-SDK flow as ``test_pancakeswap_perps_close.py``
        because their job is just to produce a tradeHash that PerpCloseIntent
        can target.
        """
        router = PANCAKESWAP_PERPS[CHAIN_NAME]["router"]
        btc_pair_base = "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c"
        margin_bnb = Decimal("0.3")
        margin_wei = int(margin_bnb * Decimal(10**18))
        size_usd = Decimal("500")
        mark_price = Decimal("95000")

        print(f"\n{'=' * 80}")
        print("Test: PCS Perps OPEN -> CLOSE-VIA-INTENT cycle (BTC/USD long)")
        print(f"{'=' * 80}")

        # =============================================================
        # Setup phase — open a position so there is something to close.
        # =============================================================
        qty = usd_size_to_qty(size_usd, mark_price)
        limit_price = slippage_to_limit_price(mark_price, Decimal("0.01"), is_long=True)
        open_struct = OpenTradeStruct(
            pair_base=btc_pair_base,
            is_long=True,
            token_in="0x0000000000000000000000000000000000000000",
            amount_in=margin_wei,
            qty=qty,
            price=limit_price,
            broker=2,
        )
        open_calldata = encode_open_market_trade_calldata(open_struct, native=True)
        nonce = web3.eth.get_transaction_count(funded_wallet)
        open_tx = {
            "from": funded_wallet,
            "to": router,
            "value": margin_wei,
            "data": "0x" + open_calldata.hex(),
            "gas": 900_000,
            "gasPrice": web3.eth.gas_price,
            "nonce": nonce,
            "chainId": 56,
        }
        signed = web3.eth.account.sign_transaction(open_tx, test_private_key)
        open_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
        open_receipt = dict(web3.eth.wait_for_transaction_receipt(open_hash, timeout=60))
        assert open_receipt["status"] == 1
        parser = PancakeSwapPerpsReceiptParser(chain=CHAIN_NAME)
        parsed_open = parser.parse_receipt(open_receipt)
        assert len(parsed_open.market_pending_trades) == 1
        trade_hash = parsed_open.market_pending_trades[0].trade_hash
        print(f"Setup: opened pending trade tradeHash={trade_hash}")

        # Keeper-fill the open at the on-chain mark to ensure acceptance.
        open_price_req_id = pcs_perps_extract_price_request_id(open_receipt)
        assert open_price_req_id is not None
        (on_chain_mark_1e8, _) = (
            web3.eth.contract(
                address=Web3.to_checksum_address(router),
                abi=[
                    {
                        "inputs": [{"type": "address", "name": "token"}],
                        "name": "getPriceFromCacheOrOracle",
                        "outputs": [{"type": "uint64"}, {"type": "uint40"}],
                        "stateMutability": "view",
                        "type": "function",
                    }
                ],
            )
            .functions.getPriceFromCacheOrOracle(Web3.to_checksum_address(btc_pair_base))
            .call()
        )
        fill_price = on_chain_mark_1e8
        fill_receipt = pcs_perps_keeper_fulfill(web3, open_price_req_id, fill_price)
        assert fill_receipt["status"] == 1
        # Sanity: position now exists on-chain.
        pos_data = web3.eth.call({"to": router, "data": "0x" + encode_get_position_by_hash_calldata(trade_hash).hex()})
        nonzero_words = sum(
            1
            for i in range(1, min(16, len(pos_data) // 32))
            if int.from_bytes(pos_data[i * 32 : (i + 1) * 32], "big") != 0
        )
        assert nonzero_words >= 3, "Position not opened; cannot test close"
        # Pending trade should be cleared.
        pending_data = web3.eth.call({"to": router, "data": "0x" + encode_get_pending_trade_calldata(trade_hash).hex()})
        assert int.from_bytes(pending_data[12:32], "big") == 0
        print("Setup OK: position confirmed open on-chain")

        # =============================================================
        # Pre-close balance snapshot
        # =============================================================
        wbnb = Web3.to_checksum_address("0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c")

        def _wbnb_balance(addr: str) -> int:
            data = web3.eth.call(
                {
                    "to": wbnb,
                    "data": "0x70a08231" + bytes.fromhex(addr[2:].zfill(40)).rjust(32, b"\x00").hex(),
                }
            )
            return int.from_bytes(data, "big")

        bnb_before = web3.eth.get_balance(funded_wallet)
        wbnb_before = _wbnb_balance(funded_wallet)

        # =============================================================
        # Layer 1 — Compile PerpCloseIntent through IntentCompiler
        # =============================================================
        intent = PerpCloseIntent(
            market="BTC/USD",
            collateral_token="BNB",
            is_long=True,
            protocol="pancakeswap_perps",
            position_id=trade_hash,  # the new field
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=perps_price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation = compiler.compile(intent)
        assert compilation.status.value == "SUCCESS", f"Compilation failed: {compilation.error}"
        assert compilation.action_bundle is not None
        assert len(compilation.action_bundle.transactions) == 1, (
            f"Expected exactly 1 TX (closeTrade), got {len(compilation.action_bundle.transactions)}"
        )
        tx = compilation.action_bundle.transactions[0]
        # closeTrade(bytes32) selector is 0x5177fd3b
        assert tx["data"].startswith("0x5177fd3b"), f"Expected closeTrade selector 0x5177fd3b, got {tx['data'][:10]}"
        # The bytes32 argument should be the tradeHash, padded to 32 bytes.
        encoded_hash = tx["data"][10:]  # strip selector
        assert encoded_hash.lower() == trade_hash[2:].lower(), (
            f"Encoded tradeHash mismatch: {encoded_hash} vs {trade_hash[2:]}"
        )
        assert int(tx["value"]) == 0, "closeTrade is not payable"
        print(
            f"Compile OK: to={tx['to']} selector={tx['data'][:10]} "
            f"position_id={compilation.action_bundle.metadata['position_id'][:18]}..."
        )

        # =============================================================
        # Layer 2 — Execute the close (request leg)
        # =============================================================
        execution = await orchestrator.execute(compilation.action_bundle)
        assert execution.success, f"Execution failed: {execution.error}"
        assert len(execution.transaction_results) == 1
        tx_result = execution.transaction_results[0]
        assert tx_result.receipt is not None
        close_receipt = tx_result.receipt.to_dict()
        status = close_receipt.get("status")
        if isinstance(status, str):
            status = int(status, 16)
        assert status == 1
        print(f"Execute OK: tx={tx_result.tx_hash[:18]} gas={tx_result.gas_used}")

        # =============================================================
        # Setup: keeper-fill the close so the settlement events emit
        # =============================================================
        close_price_req_id = pcs_perps_extract_price_request_id(close_receipt)
        assert close_price_req_id is not None, "Close TX did not emit a priceRequestId"
        time.sleep(0.1)
        settle_receipt = pcs_perps_keeper_fulfill(web3, close_price_req_id, fill_price)
        assert settle_receipt["status"] == 1
        print(f"Keeper settle OK: tx={settle_receipt['transactionHash'].hex()[:18]}")

        # =============================================================
        # Layer 3 — Receipt parser decodes settlement
        # =============================================================
        parsed_settle = parser.parse_receipt(settle_receipt)
        assert len(parsed_settle.close_trade_successful) == 1, (
            f"Expected 1 CloseTradeSuccessful event, got {len(parsed_settle.close_trade_successful)}"
        )
        cts = parsed_settle.close_trade_successful[0]
        assert cts.trade_hash == trade_hash, f"CloseTradeSuccessful.tradeHash {cts.trade_hash} != opened {trade_hash}"
        assert cts.close_price > 0
        exit_price = parser.extract_exit_price(settle_receipt)
        assert exit_price is not None and exit_price > 0
        print(
            f"Parse OK: closePrice={cts.close_price / 1e8:.2f} pnl={cts.pnl} "
            f"closeFee={cts.close_fee} fundingFee={cts.funding_fee}"
        )

        # =============================================================
        # Layer 4 — Balance deltas + on-chain state
        # =============================================================
        # The trader spent gas on the close-request leg. They should receive
        # back a WBNB payout when the keeper settles. WBNB balance MUST
        # increase strictly (no-op guard); BNB balance decreases by gas only.
        bnb_after = web3.eth.get_balance(funded_wallet)
        wbnb_after = _wbnb_balance(funded_wallet)
        bnb_delta = bnb_before - bnb_after
        wbnb_delta = wbnb_after - wbnb_before
        print(
            f"Balance deltas: BNB spent={bnb_delta / 1e18:.6f} (gas only), "
            f"WBNB received={wbnb_delta / 1e18:.6f} (close payout)"
        )
        # BNB delta should be just gas (closeTrade has value=0).
        # Allow up to 0.05 BNB for gas variance on the close TX.
        assert 0 <= bnb_delta <= 5 * 10**16, f"BNB delta {bnb_delta / 1e18:.6f} should be gas-only on a value=0 close"
        # WBNB MUST increase: the close payout returns the margin (less fees) as WBNB.
        assert wbnb_delta > 0, f"WBNB balance must increase after close (margin payout); delta={wbnb_delta}"

        # On-chain state: position MUST be substantially cleared. ApolloX
        # semantics: closeTrade zeroes qty/margin but may leave the bookkeeping
        # identifiers (positionHash key, pairBase, marginToken) in storage. A
        # live position carries >= 3 nonzero words (see pre-fill check in the
        # sibling test_pancakeswap_perps_close.py). A fully-closed position
        # should have materially fewer (<= 2 observed on ApolloX BNB mainnet).
        pos_data_after = web3.eth.call(
            {"to": router, "data": "0x" + encode_get_position_by_hash_calldata(trade_hash).hex()}
        )
        nonzero_after = sum(
            1
            for i in range(1, min(16, len(pos_data_after) // 32))
            if int.from_bytes(pos_data_after[i * 32 : (i + 1) * 32], "big") != 0
        )
        assert nonzero_after <= 2, (
            f"Position should be closed (qty/margin zeroed); getPositionByHashV2 "
            f"still has {nonzero_after} nonzero words (expected <= 2 residual bookkeeping)"
        )
        print(f"On-chain: position closed ({nonzero_after} residual bookkeeping words)")
        print("\nALL 4 LAYERS PASSED (CLOSE-VIA-INTENT path)")

    async def test_close_intent_missing_position_id_fails(
        self,
        funded_wallet: str,
        anvil_rpc_url: str,
        perps_price_oracle: dict[str, Decimal],
    ):
        """Compiler MUST fail-fast when position_id is omitted for PCS Perps.

        Failure-mode test: layers 1-2 (compile fails) + balance conservation
        is trivially preserved because we never execute.
        """
        intent = PerpCloseIntent(
            market="BTC/USD",
            collateral_token="BNB",
            is_long=True,
            protocol="pancakeswap_perps",
            # position_id intentionally omitted
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=perps_price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation = compiler.compile(intent)
        assert compilation.status.value == "FAILED", (
            f"Expected FAILED for missing position_id, got {compilation.status.value}"
        )
        assert compilation.action_bundle is None
        assert "position_id" in (compilation.error or "").lower(), (
            f"Error message should mention position_id: {compilation.error}"
        )
        print(f"Correctly rejected missing position_id: {compilation.error[:80]}...")

    async def test_close_intent_invalid_position_id_length_fails(
        self,
        funded_wallet: str,
        anvil_rpc_url: str,
        perps_price_oracle: dict[str, Decimal],
    ):
        """Compiler MUST reject a position_id that is not a bytes32 (66-char hex).

        Failure-mode test: vocabulary accepts any 0x-hex; compiler enforces strict bytes32.
        """
        # Intent vocabulary accepts any positive-length 0x-hex; the PCS Perps
        # compiler is the layer that enforces bytes32 (66-char) length.
        too_short = "0x1234"  # 6 chars total — valid hex, wrong length for ApolloX.
        intent = PerpCloseIntent(
            market="BTC/USD",
            collateral_token="BNB",
            is_long=True,
            protocol="pancakeswap_perps",
            position_id=too_short,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=perps_price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation = compiler.compile(intent)
        assert compilation.status.value == "FAILED"
        assert compilation.action_bundle is None
        assert "bytes32" in (compilation.error or "").lower() or "66" in (compilation.error or "")
        print(f"Correctly rejected wrong-length position_id: {compilation.error[:80]}...")

    async def test_close_intent_non_hex_position_id_fails(
        self,
        funded_wallet: str,
        anvil_rpc_url: str,
        perps_price_oracle: dict[str, Decimal],
    ):
        """PerpCloseIntent MUST reject a 66-char position_id that contains non-hex characters.

        Failure-mode test: guards against malformed trade hashes reaching the adapter
        where they would surface as opaque encoding errors instead of a deterministic
        validation failure.
        """
        # 0x + 64 chars, but contains 'z' which is not a valid hex character.
        # PerpCloseIntent's model_validator catches this at construction time.
        non_hex = "0x" + ("z" * 64)
        with pytest.raises(ValidationError, match="position_id must be valid hex"):
            PerpCloseIntent(
                market="BTC/USD",
                collateral_token="BNB",
                is_long=True,
                protocol="pancakeswap_perps",
                position_id=non_hex,
            )
        print(f"Correctly rejected non-hex position_id at validation time")

    async def test_close_intent_partial_close_via_size_usd_fails(
        self,
        funded_wallet: str,
        anvil_rpc_url: str,
        perps_price_oracle: dict[str, Decimal],
    ):
        """Compiler MUST reject PerpCloseIntent with size_usd for PCS Perps.

        ApolloX's ``closeTrade(bytes32)`` always closes 100% of the position. Silently
        dropping size_usd would let a caller asking for a partial close inadvertently
        flatten the entire position — we fail fast instead.
        """
        valid_hash = "0x" + ("ab" * 32)
        intent = PerpCloseIntent(
            market="BTC/USD",
            collateral_token="BNB",
            is_long=True,
            protocol="pancakeswap_perps",
            position_id=valid_hash,
            size_usd=Decimal("100"),  # partial close request — must be rejected
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=perps_price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation = compiler.compile(intent)
        assert compilation.status.value == "FAILED"
        assert compilation.action_bundle is None
        err = (compilation.error or "").lower()
        assert "partial" in err or "size_usd" in err
        print(f"Correctly rejected partial close via size_usd: {compilation.error[:80]}...")
