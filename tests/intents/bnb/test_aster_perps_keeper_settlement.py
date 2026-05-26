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
from almanak.connectors.aster_perps import (
    ASTER_BROKER_RAW,
    AsterPerpsReceiptParser,
    encode_get_pending_trade_calldata,
    encode_get_position_by_hash_calldata,
)
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.bnb.conftest import (
    open_aster_perps_position_via_intent,
    pcs_perps_extract_price_request_id,
    pcs_perps_keeper_fulfill,
)
from tests.intents.conftest import TEST_WALLET as _EOA_ADDR

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
class TestAsterPerpsKeeperSettlement:
    """Validate that the Aster keeper settlement produces broker=0 OpenMarketTrade events."""

    @pytest.mark.intent(IntentType.PERP_OPEN)
    async def test_open_then_keeper_settle_broker_raw(
        self,
        web3: Web3,
        funded_wallet: str,
        anvil_rpc_url: str,
        orchestrator: ExecutionOrchestrator,
        perps_price_oracle: dict[str, Decimal],
    ):
        """Open with broker=0, keeper settles, assert OpenMarketTrade carries broker=0."""
        router = ASTER_PERPS[CHAIN_NAME]["router"]
        btc_pair_base = "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c"  # BTCB on BSC
        margin_bnb = Decimal("0.3")
        margin_wei = int(margin_bnb * Decimal(10**18))
        size_usd = Decimal("500")

        print(f"\n{'=' * 80}")
        print("Test: Aster keeper settlement — broker=0 round-trip on OpenMarketTrade")
        print(f"{'=' * 80}")

        bnb_before_wei = web3.eth.get_balance(funded_wallet)

        # -----------------------------------------------------------------
        # Layer 1/2 — Open through Intent + orchestrator.
        #
        # Routing through the orchestrator (instead of a raw
        # ``send_raw_transaction``) keeps the setup compatible with both
        # default-on Zodiac (``funded_wallet`` is the Safe) and the
        # ``no_zodiac`` opt-out (direct EOA submission). Under Zodiac the
        # outer Safe TX is paid for by the EOA member, so balance-delta
        # accounting only debits the Safe by the margin value.
        # -----------------------------------------------------------------
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
        assert open_receipt["status"] == 1
        # Gas cost for the outer open TX. Used below for the no_zodiac balance
        # check; under Zodiac the Safe doesn't pay this gas (the member EOA
        # pays it on the outer ``execTransactionWithRole`` call).
        # ``TransactionReceipt.to_dict()`` (interfaces.py:638) uses snake_case
        # keys and stringifies effective_gas_price.
        open_gas_used = int(open_receipt["gas_used"])
        open_gas_cost = open_gas_used * int(open_receipt["effective_gas_price"])
        print(f"Open OK: gasUsed={open_gas_used}")

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
        # Aster deducts openFee + executionFee from the deposited amount at
        # settlement: OpenMarketTrade.margin is the post-fee value credited to
        # the position, not the raw amount transferred by the trader. The
        # wallet-level balance-delta check below still uses margin_wei because
        # the trader is only debited the pre-fee amount.
        deposited = omt.margin + omt.open_fee + omt.execution_fee
        assert deposited == margin_wei, (
            f"Settled margin+fees {omt.margin}+{omt.open_fee}+{omt.execution_fee}"
            f"={deposited} != deposited {margin_wei}"
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
        # the trader because it's impersonated. Under Zodiac the Safe is
        # ``funded_wallet`` and gas is paid by the member EOA on the outer
        # ``execTransactionWithRole`` — so the Safe balance is only debited
        # by ``margin_wei``. Under no_zodiac the EOA is ``funded_wallet`` and
        # pays both margin and gas.
        is_zodiac_mode = funded_wallet.lower() != _EOA_ADDR.lower()
        expected_spent = margin_wei if is_zodiac_mode else margin_wei + open_gas_cost
        assert bnb_spent_wei == expected_spent, (
            f"BNB delta mismatch (zodiac={is_zodiac_mode}): "
            f"expected {expected_spent / 1e18} "
            f"(margin {margin_wei / 1e18}"
            + (f" + open gas {open_gas_cost / 1e18}" if not is_zodiac_mode else "")
            + f"), got {bnb_spent_wei / 1e18}"
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
