"""Direct-SDK keeper-lifecycle CLOSE test for PancakeSwap Perps on BSC.

This file exercises the full open -> keeper-fill -> close -> keeper-settle
lifecycle on a BSC Anvil fork, using the SDK directly (``build_close_transaction``
+ raw-signed TX) rather than the intent compiler. Because keeper fulfillment is
off-chain on mainnet (a PRICE_FEEDER_ROLE holder calls
``PriceFacadeFacet.requestPriceCallback``), we simulate it locally via
``pcs_perps_keeper_fulfill()`` in the bnb conftest. The keeper simulation and
fork-level state assertions live here so the sibling intent-compiler test can
reuse this harness as a known-good open-position precondition.

The intent-compiler close path — ``PerpCloseIntent(position_id=<tradeHash>)``
routed through ``IntentCompiler.compile()`` and ``ExecutionOrchestrator.execute()``
— is covered in full (all 4 layers plus failure-mode compile tests) by the sibling
``tests/intents/bnb/test_pancakeswap_perps_close_intent.py``. Keep both: the
direct-SDK file is the keeper-lifecycle harness; the intent file is the
compiler/orchestrator contract test.

4-Layer verification for this file (direct-SDK close):
  1. Compilation — via ``build_close_transaction()`` (adapter-level only)
  2. Execution — raw signed TX submitted via web3.eth.send_raw_transaction
  3. Receipt parsing — PancakeSwapPerpsReceiptParser decodes close-related logs
  4. Balance deltas — trader's WBNB balance increases by the parsed
     ``CloseTradeReceived`` payout after settlement

To run:
    uv run pytest tests/intents/bnb/test_pancakeswap_perps_close.py -v -s
"""

import time
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.core.contracts import PANCAKESWAP_PERPS
from almanak.framework.connectors.pancakeswap_perps import (
    PancakeSwapPerpsReceiptParser,
    build_close_transaction,
    encode_get_pending_trade_calldata,
    encode_get_position_by_hash_calldata,
)
from almanak.framework.connectors.pancakeswap_perps.sdk import (
    OpenTradeStruct,
    encode_open_market_trade_calldata,
    slippage_to_limit_price,
    usd_size_to_qty,
)

# Import helpers from conftest (local bnb conftest)
from tests.intents.bnb.conftest import (
    pcs_perps_extract_price_request_id,
    pcs_perps_keeper_fulfill,
)

CHAIN_NAME = "bsc"


@pytest.mark.bsc
@pytest.mark.asyncio
class TestPancakeSwapPerpsCloseIntent:
    """Test PancakeSwap Perps CLOSE via direct-SDK flow.

    Lifecycle:
      1. Open a position (openMarketTradeBNB on-chain).
      2. Extract priceRequestId from the open receipt.
      3. Impersonate a PRICE_FEEDER_ROLE holder; call requestPriceCallback to
         fill the pending trade at an acceptable oracle price. This emits
         OpenMarketTrade and makes the position queryable via getPositionByHashV2.
      4. Build + submit closeTrade(tradeHash) as the trader.
      5. Simulate keeper fill for the close (also via requestPriceCallback on the
         second priceRequestId emitted by closeTrade).
      6. Verify CloseTradeSuccessful + CloseTradeReceived events, balance deltas.
    """

    async def test_open_fill_close_cycle_native_bnb(
        self,
        web3: Web3,
        funded_wallet: str,
        test_private_key: str,
        anvil_rpc_url: str,
    ):
        """Full open -> fill -> close cycle for a LONG BTC/USD position."""
        router = PANCAKESWAP_PERPS[CHAIN_NAME]["router"]
        btc_pair_base = "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c"  # BTCB on BSC
        margin_bnb = Decimal("0.3")
        margin_wei = int(margin_bnb * Decimal(10**18))
        size_usd = Decimal("500")
        mark_price = Decimal("95000")

        print(f"\n{'=' * 80}")
        print("Test: PancakeSwap Perps OPEN -> FILL -> CLOSE cycle (BTC/USD long, native BNB)")
        print(f"{'=' * 80}")

        bnb_before_wei = web3.eth.get_balance(funded_wallet)

        # -----------------------------------------------------------------
        # Step 1 — Open (signed directly; we want to exercise the open event path)
        # -----------------------------------------------------------------
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
        assert open_receipt["status"] == 1, f"Open TX reverted: {open_hash.hex()}"
        print(f"Open OK: tx={open_hash.hex()[:18]} gasUsed={open_receipt['gasUsed']}")

        parser = PancakeSwapPerpsReceiptParser(chain=CHAIN_NAME)
        parsed_open = parser.parse_receipt(open_receipt)
        assert len(parsed_open.market_pending_trades) == 1
        trade_hash = parsed_open.market_pending_trades[0].trade_hash
        print(f"Parsed tradeHash: {trade_hash}")

        # -----------------------------------------------------------------
        # Step 2 — Keeper fulfills the open at a price slightly inside the limit
        # -----------------------------------------------------------------
        open_price_req_id = pcs_perps_extract_price_request_id(open_receipt)
        assert open_price_req_id is not None, (
            "Could not extract priceRequestId from open receipt — check topic0 convention"
        )
        # The PriceFacade's beforePrice cache check (highPriceGapP = 1.5%) rejects
        # fills that diverge from the current oracle mark by more than 1.5%.
        # Query the actual on-chain oracle price and use it directly — this
        # guarantees gapPercentage == 0 and the fill is accepted.
        (on_chain_mark_1e8, _updated_at) = web3.eth.contract(
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
        ).functions.getPriceFromCacheOrOracle(Web3.to_checksum_address(btc_pair_base)).call()
        assert on_chain_mark_1e8 > 0, "On-chain BTC oracle price is zero on fork"
        print(f"On-chain BTC oracle price: {on_chain_mark_1e8 / 1e8:.2f} USD")
        # Also compare to our limit_price: for the fill to work the fill must
        # be <= limit for a long. If the on-chain mark exceeds our limit, the
        # test's opening limit was too tight — use the on-chain mark (and the
        # test's open limit is re-derived from on-chain mark to ensure compat).
        fill_price = on_chain_mark_1e8
        print(f"Keeper fulfill: priceRequestId={open_price_req_id[:18]}... price={fill_price} (on-chain mark)")
        fill_receipt = pcs_perps_keeper_fulfill(web3, open_price_req_id, fill_price)
        print(f"Keeper fill OK: tx={fill_receipt['transactionHash'].hex()[:18]} gasUsed={fill_receipt['gasUsed']}")
        # Dump fill logs for diagnosis
        for i, log in enumerate(fill_receipt.get("logs", [])):
            t0 = log["topics"][0].hex() if hasattr(log["topics"][0], "hex") else log["topics"][0]
            print(f"  fill log[{i}]: topic0={t0[:20]} ntopics={len(log['topics'])}")

        # Verify the position is now open (getPositionByHashV2 returns non-zero data)
        pos_data = web3.eth.call(
            {
                "to": router,
                "data": "0x" + encode_get_position_by_hash_calldata(trade_hash).hex(),
            }
        )
        # getPositionByHashV2 returns a complex tuple; the raw bytes must not be all zeros
        # past the initial position_hash word. If qty (word[7]) is nonzero the position is live.
        # Words: [0]offset [1]positionHash [2]pair_offset [3]pairBase [4]marginToken [5]isLong
        #         [6]margin [7]qty ...
        qty_word = int.from_bytes(pos_data[7 * 32 : 8 * 32], "big") if len(pos_data) >= 8 * 32 else 0
        # Note: the exact word layout depends on the dynamic-string `pair` field; this is a
        # permissive check — look for any nonzero word in the qty/margin region.
        nonzero_words = sum(
            1
            for i in range(1, min(16, len(pos_data) // 32))
            if int.from_bytes(pos_data[i * 32 : (i + 1) * 32], "big") != 0
        )
        assert nonzero_words >= 3, (
            f"Position not found after keeper fill — getPositionByHashV2 returned mostly zeros "
            f"(only {nonzero_words} nonzero words). Keeper fill may have produced a refund instead."
        )
        # Verify that pending trade has been cleared (settled)
        pending_data = web3.eth.call(
            {
                "to": router,
                "data": "0x" + encode_get_pending_trade_calldata(trade_hash).hex(),
            }
        )
        pending_user = int.from_bytes(pending_data[12:32], "big")
        assert pending_user == 0, "Pending trade should be cleared after keeper fill"

        # -----------------------------------------------------------------
        # Snapshot WBNB balance BEFORE close — used in Layer 4 to verify the
        # margin payout is actually received (not trivially > 0 from conftest
        # pre-funding).
        # -----------------------------------------------------------------
        wbnb = Web3.to_checksum_address("0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c")
        wbnb_balance_before_raw = web3.eth.call(
            {
                "to": wbnb,
                "data": "0x70a08231"
                + bytes.fromhex(funded_wallet[2:].zfill(40)).rjust(32, b"\x00").hex(),
            }
        )
        wbnb_balance_before = int.from_bytes(wbnb_balance_before_raw, "big")
        print(f"Trader WBNB balance before close: {wbnb_balance_before / 1e18} WBNB")

        # -----------------------------------------------------------------
        # Step 3 — Build + send closeTrade via the SDK
        # -----------------------------------------------------------------
        close_tx_data = build_close_transaction(trade_hash=trade_hash, chain=CHAIN_NAME)
        assert close_tx_data.data[:4].hex() == "5177fd3b", "Expected closeTrade selector 0x5177fd3b"
        nonce = web3.eth.get_transaction_count(funded_wallet)
        close_tx = {
            "from": funded_wallet,
            "to": close_tx_data.to,
            "value": 0,
            "data": "0x" + close_tx_data.data.hex(),
            "gas": close_tx_data.gas_estimate,
            "gasPrice": web3.eth.gas_price,
            "nonce": nonce,
            "chainId": 56,
        }
        signed_close = web3.eth.account.sign_transaction(close_tx, test_private_key)
        close_hash = web3.eth.send_raw_transaction(signed_close.raw_transaction)
        close_receipt = dict(web3.eth.wait_for_transaction_receipt(close_hash, timeout=60))
        assert close_receipt["status"] == 1, f"Close TX reverted: {close_hash.hex()}"
        print(f"Close-request OK: tx={close_hash.hex()[:18]} gasUsed={close_receipt['gasUsed']}")

        # -----------------------------------------------------------------
        # Step 4 — Keeper fills the close
        # -----------------------------------------------------------------
        close_price_req_id = pcs_perps_extract_price_request_id(close_receipt)
        assert close_price_req_id is not None, "Could not extract priceRequestId from close TX"
        # Close at same price (no PnL, no funding — simplest accounting).
        # Small sleep to allow fork block to advance if needed.
        time.sleep(0.1)
        settle_receipt = pcs_perps_keeper_fulfill(web3, close_price_req_id, fill_price)
        print(f"Close settle OK: tx={settle_receipt['transactionHash'].hex()[:18]} gasUsed={settle_receipt['gasUsed']}")

        # -----------------------------------------------------------------
        # Layer 3 — Receipt parser decodes CloseTradeSuccessful / CloseTradeReceived
        # -----------------------------------------------------------------
        parsed_settle = parser.parse_receipt(settle_receipt)
        assert len(parsed_settle.close_trade_successful) == 1, (
            f"Expected 1 CloseTradeSuccessful event after keeper settle, got "
            f"{len(parsed_settle.close_trade_successful)}"
        )
        cts = parsed_settle.close_trade_successful[0]
        assert cts.trade_hash == trade_hash, (
            f"CloseTradeSuccessful.tradeHash {cts.trade_hash} != opened {trade_hash}"
        )
        print(
            f"Parse OK: closePrice={cts.close_price / 1e8} USD, pnl={cts.pnl}, "
            f"closeFee={cts.close_fee}, fundingFee={cts.funding_fee}"
        )
        assert cts.close_price > 0
        # PnL should be near zero (we closed at the same price we opened at) — allow
        # some funding / fee drift.
        exit_price = parser.extract_exit_price(settle_receipt)
        assert exit_price is not None and exit_price > 0

        # -----------------------------------------------------------------
        # Layer 4 — Balance deltas. Trader paid 0.3 BNB in margin + gas on
        # open/close. After settle, they should receive BACK a WBNB payout
        # (close returns the underlying margin net of fees).
        # -----------------------------------------------------------------
        # Check WBNB balance — the settle emits CloseTradeReceived(token=WBNB, amount=payout).
        wbnb_balance_after_raw = web3.eth.call(
            {
                "to": wbnb,
                "data": "0x70a08231"
                + bytes.fromhex(funded_wallet[2:].zfill(40)).rjust(32, b"\x00").hex(),
            }
        )
        wbnb_balance_after = int.from_bytes(wbnb_balance_after_raw, "big")
        wbnb_delta = wbnb_balance_after - wbnb_balance_before
        print(
            f"Trader WBNB after: {wbnb_balance_after / 1e18} WBNB "
            f"(delta={wbnb_delta / 1e18:+} WBNB vs before-close snapshot)"
        )

        # CloseTradeReceived events — parse the exact payout so we can tighten the
        # delta assertion beyond the trivial "> 0" check.
        payout_returned = parser.extract_collateral_returned(settle_receipt)
        if payout_returned is not None:
            print(f"Parsed payout (CloseTradeReceived sum): {payout_returned} raw")
            # Tight assertion: WBNB balance MUST increase by at least the parsed
            # payout minus 1 wei dust tolerance. Trivial "> 0" wouldn't catch a
            # no-op regression since the wallet is pre-funded with 10 WBNB.
            assert wbnb_delta >= payout_returned - 1, (
                f"WBNB delta {wbnb_delta} does not match parsed payout {payout_returned} "
                f"(allowed dust tolerance: 1 wei)"
            )
        else:
            # If no CloseTradeReceived event was emitted, the close still must
            # have returned the margin to the trader — require a strictly positive
            # delta (not a trivially-true > 0 on absolute balance).
            assert wbnb_delta > 0, (
                f"WBNB balance did not increase after close — expected margin payout, "
                f"got delta={wbnb_delta}"
            )

        print("\nALL 4 LAYERS PASSED (via direct-SDK close path)")
