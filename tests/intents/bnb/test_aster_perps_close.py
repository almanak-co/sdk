"""Intent-level tests for Aster Perps PERP_CLOSE on BSC (VIB-3053).

Mirrors ``test_pancakeswap_perps_close_intent.py`` but routes the close
through ``PerpCloseIntent(protocol='aster_perps', ...)`` — the canonical
post-rebrand protocol key that attributes to broker_id=0 (raw Aster,
no PancakeSwap attribution).

Lifecycle:

  1. Open a position via direct-SDK (setup; we want a tradeHash to close).
     We intentionally use ``broker=ASTER_BROKER_RAW`` in the open so the
     whole lifecycle (open events, close events) carries broker=0.
  2. Impersonate a PRICE_FEEDER_ROLE keeper to settle the open.
  3. Compile ``PerpCloseIntent(position_id=tradeHash, protocol='aster_perps')``.
  4. Execute via the orchestrator.
  5. Keeper-settle the close.
  6. Verify all 4 layers (compile, execute, parse, balance deltas).

To run:
    uv run pytest tests/intents/bnb/test_aster_perps_close.py -v -s
"""

import asyncio
from decimal import Decimal

import pytest
from pydantic import ValidationError
from web3 import Web3

from almanak.core.contracts import ASTER_PERPS
from almanak.framework.connectors.aster_perps import (
    ASTER_BROKER_RAW,
    AsterPerpsReceiptParser,
    encode_get_pending_trade_calldata,
    encode_get_position_by_hash_calldata,
)
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.perp_intents import PerpCloseIntent
from tests.intents.bnb.conftest import (
    open_aster_perps_position_via_intent,
    pcs_perps_extract_price_request_id,
    pcs_perps_keeper_fulfill,
)

CHAIN_NAME = "bsc"


@pytest.fixture(scope="session")
def perps_price_oracle() -> dict[str, Decimal]:
    """Static prices (matches sibling open/close tests)."""
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
class TestAsterPerpsCloseIntent:
    """Test Aster Perps PERP_CLOSE via the IntentCompiler, broker_id=0."""

    async def test_close_btc_long_via_aster_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        anvil_rpc_url: str,
        orchestrator: ExecutionOrchestrator,
        perps_price_oracle: dict[str, Decimal],
    ):
        """Open (broker=0) -> keeper-fill -> CLOSE via aster_perps intent -> settle."""
        router = ASTER_PERPS[CHAIN_NAME]["router"]
        btc_pair_base = "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c"  # BTCB on BSC
        margin_bnb = Decimal("0.3")
        size_usd = Decimal("500")

        print(f"\n{'=' * 80}")
        print("Test: Aster Perps OPEN(broker=0) -> CLOSE-VIA-INTENT cycle")
        print(f"{'=' * 80}")

        # =============================================================
        # Setup — open a broker=0 position via Intent + orchestrator.
        #
        # Routing the open through the orchestrator (instead of a raw
        # ``send_raw_transaction``) keeps the setup compatible with both
        # default-on Zodiac (``funded_wallet`` is the Safe; the harness
        # wraps the call into ``execTransactionWithRole``) and the
        # ``no_zodiac`` opt-out (direct EOA submission). The legacy
        # raw-sign path failed under Zodiac because the EOA private key
        # cannot sign for the Safe address.
        # =============================================================
        open_receipt = await open_aster_perps_position_via_intent(
            orchestrator=orchestrator,
            web3=web3,
            funded_wallet=funded_wallet,
            anvil_rpc_url=anvil_rpc_url,
            perps_price_oracle=perps_price_oracle,
            protocol="aster_perps",
            market="BTC/USD",
            collateral_amount=margin_bnb,
            size_usd=size_usd,
        )

        parser = AsterPerpsReceiptParser(chain=CHAIN_NAME)
        parsed_open = parser.parse_receipt(open_receipt)
        assert len(parsed_open.market_pending_trades) == 1
        pending = parsed_open.market_pending_trades[0]
        assert pending.broker == ASTER_BROKER_RAW, (
            f"Setup open must carry broker=0, got {pending.broker}"
        )
        trade_hash = pending.trade_hash
        print(f"Setup: opened pending trade tradeHash={trade_hash} broker={pending.broker}")

        # Keeper-fill the open at on-chain mark.
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

        # Sanity: position exists on-chain.
        pos_data = web3.eth.call(
            {"to": router, "data": "0x" + encode_get_position_by_hash_calldata(trade_hash).hex()}
        )
        nonzero_words = sum(
            1
            for i in range(1, min(16, len(pos_data) // 32))
            if int.from_bytes(pos_data[i * 32 : (i + 1) * 32], "big") != 0
        )
        assert nonzero_words >= 3, "Position not opened; cannot test close"
        pending_data = web3.eth.call(
            {"to": router, "data": "0x" + encode_get_pending_trade_calldata(trade_hash).hex()}
        )
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
        # Layer 1 — Compile PerpCloseIntent with protocol='aster_perps'
        # =============================================================
        intent = PerpCloseIntent(
            market="BTC/USD",
            collateral_token="BNB",
            is_long=True,
            protocol="aster_perps",  # canonical key — broker=0 path
            position_id=trade_hash,
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
        assert len(compilation.action_bundle.transactions) == 1
        tx = compilation.action_bundle.transactions[0]
        # closeTrade(bytes32) selector is 0x5177fd3b
        assert tx["data"].startswith("0x5177fd3b"), (
            f"Expected closeTrade selector 0x5177fd3b, got {tx['data'][:10]}"
        )
        # The bytes32 argument is the tradeHash, padded to 32 bytes.
        encoded_hash = tx["data"][10:]
        assert encoded_hash.lower() == trade_hash[2:].lower(), (
            f"Encoded tradeHash mismatch: {encoded_hash} vs {trade_hash[2:]}"
        )
        assert int(tx["value"]) == 0, "closeTrade is not payable"
        print(
            f"Compile OK: selector={tx['data'][:10]} "
            f"position_id={compilation.action_bundle.metadata['position_id'][:18]}..."
        )

        # =============================================================
        # Layer 2 — Execute the close (request leg)
        # =============================================================
        execution = await orchestrator.execute(compilation.action_bundle)
        assert execution.success, f"Execution failed: {execution.error}"
        tx_result = execution.transaction_results[0]
        assert tx_result.receipt is not None
        close_receipt = tx_result.receipt.to_dict()
        status = close_receipt.get("status")
        if isinstance(status, str):
            status = int(status, 16)
        assert status == 1
        print(f"Execute OK: tx={tx_result.tx_hash[:18]} gas={tx_result.gas_used}")

        # Setup — keeper-fill the close
        close_price_req_id = pcs_perps_extract_price_request_id(close_receipt)
        assert close_price_req_id is not None, "Close TX did not emit a priceRequestId"
        await asyncio.sleep(0.1)
        settle_receipt = pcs_perps_keeper_fulfill(web3, close_price_req_id, fill_price)
        assert settle_receipt["status"] == 1
        print(f"Keeper settle OK: tx={settle_receipt['transactionHash'].hex()[:18]}")

        # =============================================================
        # Layer 3 — Parser decodes the settlement
        # =============================================================
        parsed_settle = parser.parse_receipt(settle_receipt)
        assert len(parsed_settle.close_trade_successful) == 1, (
            f"Expected 1 CloseTradeSuccessful, got {len(parsed_settle.close_trade_successful)}"
        )
        cts = parsed_settle.close_trade_successful[0]
        assert cts.trade_hash == trade_hash, (
            f"CloseTradeSuccessful.tradeHash {cts.trade_hash} != opened {trade_hash}"
        )
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
        bnb_after = web3.eth.get_balance(funded_wallet)
        wbnb_after = _wbnb_balance(funded_wallet)
        bnb_delta = bnb_before - bnb_after
        wbnb_delta = wbnb_after - wbnb_before
        print(
            f"Balance deltas: BNB spent={bnb_delta / 1e18:.6f} (gas only), "
            f"WBNB received={wbnb_delta / 1e18:.6f} (close payout)"
        )
        # closeTrade has value=0 — BNB delta is gas only.
        assert 0 <= bnb_delta <= 5 * 10**16, (
            f"BNB delta {bnb_delta / 1e18:.6f} should be gas-only on a value=0 close"
        )
        # WBNB MUST increase: the close payout returns margin (less fees) as WBNB.
        assert wbnb_delta > 0, (
            f"WBNB balance must increase after close (margin payout); delta={wbnb_delta}"
        )

        # On-chain state: position is substantially cleared (qty/margin zeroed).
        pos_data_after = web3.eth.call(
            {"to": router, "data": "0x" + encode_get_position_by_hash_calldata(trade_hash).hex()}
        )
        nonzero_after = sum(
            1
            for i in range(1, min(16, len(pos_data_after) // 32))
            if int.from_bytes(pos_data_after[i * 32 : (i + 1) * 32], "big") != 0
        )
        assert nonzero_after <= 2, (
            f"Position should be closed; getPositionByHashV2 still has {nonzero_after} "
            "nonzero words (expected <= 2 residual bookkeeping)"
        )
        print(f"On-chain: position closed ({nonzero_after} residual bookkeeping words)")
        print("\nALL 4 LAYERS PASSED (aster_perps CLOSE-VIA-INTENT path)")

    async def test_close_intent_missing_position_id_fails(
        self,
        funded_wallet: str,
        anvil_rpc_url: str,
        perps_price_oracle: dict[str, Decimal],
    ):
        """aster_perps compiler MUST fail-fast when position_id is omitted."""
        intent = PerpCloseIntent(
            market="BTC/USD",
            collateral_token="BNB",
            is_long=True,
            protocol="aster_perps",
            # position_id intentionally omitted
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
        assert "position_id" in (compilation.error or "").lower()
        print(f"Correctly rejected missing position_id: {compilation.error[:80]}...")

    async def test_close_intent_non_hex_position_id_fails(
        self,
        funded_wallet: str,
    ):
        """PerpCloseIntent rejects non-hex position_id at validation time."""
        non_hex = "0x" + ("z" * 64)
        with pytest.raises(ValidationError, match="position_id must be valid hex"):
            PerpCloseIntent(
                market="BTC/USD",
                collateral_token="BNB",
                is_long=True,
                protocol="aster_perps",
                position_id=non_hex,
            )
        print("Correctly rejected non-hex position_id at validation time")
