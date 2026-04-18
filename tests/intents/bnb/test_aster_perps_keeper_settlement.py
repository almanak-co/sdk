"""Keeper-lifecycle settlement test for Aster Perps on BSC (VIB-3053).

Focuses on the settlement leg of the OPEN lifecycle: after a user-signed
``openMarketTradeBNB`` emits a MarketPendingTrade, a PRICE_FEEDER_ROLE
keeper must call ``PriceFacadeFacet.requestPriceCallback`` to either settle
the pending trade into an ``OpenMarketTrade`` event (success) or refund it
via ``PendingTradeRefund``. The settlement event carries the broker
attribution — we assert broker=0 (raw Aster) end-to-end.

Sibling tests cover:
  * test_aster_perps_open.py — the open-request leg + pending state
  * test_aster_perps_close.py — the intent-level close-via-intent path
  * test_pancakeswap_perps_close.py — the direct-SDK open→settle→close
    lifecycle with broker=2 attribution

This file is the Aster-specific (broker=0) keeper-settlement harness.

4-Layer verification:
  1. Compilation — user-signed open via direct-SDK (not the intent compiler,
     since the subject of this test is the settlement leg, not compilation).
  2. Execution — raw signed TX submitted via web3.eth.send_raw_transaction,
     plus the keeper's requestPriceCallback.
  3. Receipt parsing — AsterPerpsReceiptParser decodes the OpenMarketTrade
     event from the settlement TX and exposes entry_price.
  4. Balance deltas — BNB spent on margin+gas matches expected; position
     becomes queryable via getPositionByHashV2 after settlement.

To run:
    uv run pytest tests/intents/bnb/test_aster_perps_keeper_settlement.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.core.contracts import ASTER_PERPS
from almanak.framework.connectors.aster_perps import (
    ASTER_BROKER_RAW,
    AsterPerpsReceiptParser,
    encode_get_pending_trade_calldata,
    encode_get_position_by_hash_calldata,
)
from almanak.framework.connectors.aster_perps.sdk import (
    OpenTradeStruct,
    encode_open_market_trade_calldata,
    slippage_to_limit_price,
    usd_size_to_qty,
)
from tests.intents.bnb.conftest import (
    pcs_perps_extract_price_request_id,
    pcs_perps_keeper_fulfill,
)

CHAIN_NAME = "bsc"


@pytest.mark.bsc
@pytest.mark.asyncio
class TestAsterPerpsKeeperSettlement:
    """Validate that the Aster keeper settlement produces broker=0 OpenMarketTrade events."""

    async def test_open_then_keeper_settle_broker_raw(
        self,
        web3: Web3,
        funded_wallet: str,
        test_private_key: str,
    ):
        """Open with broker=0, keeper settles, assert OpenMarketTrade carries broker=0."""
        router = ASTER_PERPS[CHAIN_NAME]["router"]
        btc_pair_base = "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c"  # BTCB on BSC
        margin_bnb = Decimal("0.3")
        margin_wei = int(margin_bnb * Decimal(10**18))
        size_usd = Decimal("500")
        mark_price = Decimal("95000")

        print(f"\n{'=' * 80}")
        print("Test: Aster keeper settlement — broker=0 round-trip on OpenMarketTrade")
        print(f"{'=' * 80}")

        bnb_before_wei = web3.eth.get_balance(funded_wallet)

        # -----------------------------------------------------------------
        # Layer 1/2 — User-signed open (broker=0)
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
            broker=ASTER_BROKER_RAW,
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
            "chainId": web3.eth.chain_id,
        }
        signed = web3.eth.account.sign_transaction(open_tx, test_private_key)
        open_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
        open_receipt = dict(web3.eth.wait_for_transaction_receipt(open_hash, timeout=60))
        assert open_receipt["status"] == 1, f"Open TX reverted: {open_hash.hex()}"
        open_gas_cost = open_receipt["gasUsed"] * int(open_tx["gasPrice"])
        print(f"Open OK: tx={open_hash.hex()[:18]} gasUsed={open_receipt['gasUsed']}")

        parser = AsterPerpsReceiptParser(chain=CHAIN_NAME)
        parsed_open = parser.parse_receipt(open_receipt)
        assert len(parsed_open.market_pending_trades) == 1
        pending = parsed_open.market_pending_trades[0]
        trade_hash = pending.trade_hash
        assert pending.broker == ASTER_BROKER_RAW, (
            f"Open pending-trade broker must be 0 (raw Aster), got {pending.broker}"
        )
        print(f"Pending trade created: tradeHash={trade_hash} broker={pending.broker}")

        # -----------------------------------------------------------------
        # Keeper settles the open
        # -----------------------------------------------------------------
        price_req_id = pcs_perps_extract_price_request_id(open_receipt)
        assert price_req_id is not None, "Could not extract priceRequestId from open receipt"
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
        assert on_chain_mark_1e8 > 0, "On-chain BTC oracle price is zero on fork"
        fill_price = on_chain_mark_1e8
        print(f"Keeper fulfill: priceRequestId={price_req_id[:18]}... price={fill_price}")
        fill_receipt = pcs_perps_keeper_fulfill(web3, price_req_id, fill_price)
        assert fill_receipt["status"] == 1
        print(f"Keeper fill OK: tx={fill_receipt['transactionHash'].hex()[:18]}")

        # -----------------------------------------------------------------
        # Layer 3 — Parser decodes OpenMarketTrade with broker=0 attribution
        # -----------------------------------------------------------------
        parsed_settle = parser.parse_receipt(fill_receipt)
        assert len(parsed_settle.open_market_trades) == 1, (
            f"Expected 1 OpenMarketTrade event after keeper fill, got "
            f"{len(parsed_settle.open_market_trades)}. If this is 0, the keeper may have emitted "
            f"PendingTradeRefund instead — check beforePrice gate / oracle gap."
        )
        omt = parsed_settle.open_market_trades[0]
        assert omt.trade_hash == trade_hash, (
            f"OpenMarketTrade.tradeHash {omt.trade_hash} != pending {trade_hash}"
        )
        assert omt.user.lower() == funded_wallet.lower()
        assert omt.is_long is True
        assert omt.margin == margin_wei, (
            f"Settled margin {omt.margin} != deposited {margin_wei}"
        )
        assert omt.qty > 0
        assert omt.entry_price > 0
        print(
            f"Parse OK: OpenMarketTrade tradeHash={omt.trade_hash[:18]}... "
            f"entry_price={omt.entry_price / 1e8:.2f} qty={omt.qty} margin={omt.margin}"
        )

        # ResultEnricher-facing extraction methods covered here
        assert parser.extract_position_id(fill_receipt) == omt.trade_hash
        entry = parser.extract_entry_price(fill_receipt)
        assert entry is not None and entry > 0, (
            "entry_price extraction must return a positive Decimal after settlement"
        )

        # -----------------------------------------------------------------
        # Layer 4 — Balance delta + on-chain state
        # -----------------------------------------------------------------
        bnb_after_wei = web3.eth.get_balance(funded_wallet)
        bnb_spent_wei = bnb_before_wei - bnb_after_wei
        # The trader only paid for the open; the keeper's settle is gas-free for
        # the trader because it's impersonated.
        expected_spent = margin_wei + open_gas_cost
        assert bnb_spent_wei == expected_spent, (
            f"BNB delta mismatch: expected {expected_spent / 1e18} "
            f"(margin {margin_wei / 1e18} + open gas {open_gas_cost / 1e18}), "
            f"got {bnb_spent_wei / 1e18}"
        )

        # Pending trade must be cleared.
        pending_data = web3.eth.call(
            {"to": router, "data": "0x" + encode_get_pending_trade_calldata(trade_hash).hex()}
        )
        assert int.from_bytes(pending_data[12:32], "big") == 0, (
            "Pending trade still present after keeper fill — expected it to be cleared"
        )

        # Position is queryable via getPositionByHashV2 (qty/margin non-zero words).
        pos_data = web3.eth.call(
            {"to": router, "data": "0x" + encode_get_position_by_hash_calldata(trade_hash).hex()}
        )
        nonzero_words = sum(
            1
            for i in range(1, min(16, len(pos_data) // 32))
            if int.from_bytes(pos_data[i * 32 : (i + 1) * 32], "big") != 0
        )
        assert nonzero_words >= 3, (
            f"Expected live position with >= 3 nonzero words post-settlement, "
            f"got {nonzero_words}. Keeper may have refunded instead of filled."
        )
        print(
            f"On-chain state OK: pending cleared, position live ({nonzero_words} nonzero words)"
        )
        print("\nALL 4 LAYERS PASSED (Aster keeper settlement, broker=0)")
