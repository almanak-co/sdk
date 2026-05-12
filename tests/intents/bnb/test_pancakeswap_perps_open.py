"""Intent-level tests for PancakeSwap Perps OPEN on BSC.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
PerpOpenIntent(protocol='pancakeswap_perps'):

  1. PerpOpenIntent is created (market='BTC/USD', native BNB margin)
  2. IntentCompiler compiles to an ActionBundle with a single openMarketTradeBNB TX
  3. ExecutionOrchestrator executes on the Anvil BSC fork
  4. PancakeSwapPerpsReceiptParser decodes the MarketPendingTrade event and
     yields the tradeHash (== position_id) plus margin/qty/price details
  5. Native BNB balance decreases by exactly collateral_amount + gas, and a
     pending trade is registered on-chain (getPendingTrade returns populated struct)

Key semantics vs a swap/lending test:
  - PancakeSwap Perps is oracle-priced; the user's open TX only emits a
    MarketPendingTrade event. An off-chain keeper subsequently settles the
    position via TradingOpenFacet.marketTradeCallback. Because no keeper runs
    on the Anvil fork, we do NOT assert that an OpenMarketTrade event is present
    — we only verify the pending leg, which is the user-synchronous contract.
  - The payout side of the bilateral delta check is therefore the on-chain
    pendingTrade record, not a token balance increase.

To run:
    uv run pytest tests/intents/bnb/test_pancakeswap_perps_open.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.pancakeswap_perps import (
    EVENT_MARKET_PENDING_TRADE,
    PancakeSwapPerpsReceiptParser,
    encode_get_pending_trade_calldata,
)
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.perp_intents import PerpOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import TEST_WALLET as _EOA_ADDR

CHAIN_NAME = "bsc"


# =============================================================================
# Helper: static price oracle — perps tests don't need CoinGecko since the mark
# price is a test parameter, not an execution-time lookup. Using a fixed dict
# keeps these tests deterministic and independent of network state.
# =============================================================================


@pytest.fixture(scope="session")
def perps_price_oracle() -> dict[str, Decimal]:
    """Static prices for PCS Perps open tests.

    The compiler uses these to derive qty (size_usd / mark_price) and the
    slippage-to-limit-price bound. We pick values close enough to real-market
    that the computed limit passes ApolloX's internal "beforePrice" sanity gate
    (which rejects fills diverging from the oracle price by more than the
    PriceFacade's highPriceGapP parameter).
    """
    return {
        "BTC": Decimal("95000"),
        "ETH": Decimal("3500"),
        "BNB": Decimal("600"),
        "WBNB": Decimal("600"),
        "USDT": Decimal("1"),
        "USDC": Decimal("1"),
    }


def _call_get_pending_trade(web3: Web3, router: str, trade_hash: str) -> bytes:
    """Call TradingReaderFacet.getPendingTrade(bytes32) and return raw bytes."""
    calldata = encode_get_pending_trade_calldata(trade_hash)
    return web3.eth.call({"to": router, "data": "0x" + calldata.hex()})


@pytest.mark.bsc
@pytest.mark.asyncio
class TestPancakeSwapPerpsOpenIntent:
    """Test PancakeSwap Perps OPEN via PerpOpenIntent on BSC."""

    @pytest.mark.intent(IntentType.PERP_OPEN)
    async def test_open_btc_long_native_bnb_margin(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        perps_price_oracle: dict[str, Decimal],
    ):
        """Open a long BTC/USD position with 0.3 BNB native margin.

        Verifies all 4 intent-test layers:
          1. Compilation SUCCESS
          2. Execution success (TX mined, status=1)
          3. Receipt parser extracts MarketPendingTrade with expected fields
          4. Balance delta: BNB decreased by exactly margin + gas; pending trade
             exists on-chain
        """
        from almanak.core.contracts import PANCAKESWAP_PERPS

        router = PANCAKESWAP_PERPS[CHAIN_NAME]["router"]
        # ApolloX enforces a minimum position notional (TradingCheckerFacet:
        # "Position is too small"). $500 is well above the floor for BTC/USD and
        # matches the magnitude of the live reference TX on BSC.
        collateral_amount = Decimal("0.3")  # 0.3 BNB (~ $180 at $600/BNB)
        size_usd = Decimal("500")  # $500 notional
        mark_price = perps_price_oracle["BTC"]

        print(f"\n{'=' * 80}")
        print("Test: PancakeSwap Perps OPEN — LONG BTC/USD, native BNB margin")
        print(f"{'=' * 80}")
        print(f"Collateral: {collateral_amount} BNB")
        print(f"Size:       ${size_usd}")
        print(f"Mark price: ${mark_price}")

        bnb_before_wei = web3.eth.get_balance(funded_wallet)
        print(f"BNB balance before: {bnb_before_wei / 1e18:.6f}")

        # -----------------------------------------------------------------
        # Layer 1 — Compilation
        # -----------------------------------------------------------------
        intent = PerpOpenIntent(
            market="BTC/USD",
            collateral_token="BNB",
            collateral_amount=collateral_amount,
            size_usd=size_usd,
            is_long=True,
            max_slippage=Decimal("0.01"),
            protocol="pancakeswap_perps",
            leverage=Decimal("1"),
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=perps_price_oracle,
            rpc_url=orchestrator.rpc_url,
        )
        compilation = compiler.compile(intent)
        assert compilation.status.value == "SUCCESS", (
            f"Compilation failed: {compilation.error}"
        )
        assert compilation.action_bundle is not None
        assert len(compilation.action_bundle.transactions) == 1, (
            f"Expected exactly 1 TX (openMarketTradeBNB), got "
            f"{len(compilation.action_bundle.transactions)}"
        )
        tx = compilation.action_bundle.transactions[0]
        assert tx["data"].startswith("0xb7aeae66"), (
            f"Expected openMarketTradeBNB selector 0xb7aeae66, got {tx['data'][:10]}"
        )
        assert int(tx["value"]) == int(collateral_amount * Decimal(10**18)), (
            "TX value must equal native BNB margin"
        )
        print(
            f"Compile OK: to={tx['to']} value={int(tx['value']) / 1e18} BNB "
            f"selector={tx['data'][:10]} qty_1e10={compilation.action_bundle.metadata['qty_1e10']}"
        )

        # -----------------------------------------------------------------
        # Layer 2 — Execution
        # -----------------------------------------------------------------
        execution = await orchestrator.execute(compilation.action_bundle)
        assert execution.success, f"Execution failed: {execution.error}"
        assert len(execution.transaction_results) == 1
        tx_result = execution.transaction_results[0]
        assert tx_result.receipt is not None
        receipt = tx_result.receipt.to_dict()
        status = receipt.get("status")
        status_int = int(status, 16) if isinstance(status, str) else status
        assert status_int == 1, f"TX status must be success, got {status!r}"
        print(f"Execute OK: tx={tx_result.tx_hash[:18]} gas={tx_result.gas_used}")

        # -----------------------------------------------------------------
        # Layer 3 — Receipt parser
        # -----------------------------------------------------------------
        parser = PancakeSwapPerpsReceiptParser(chain=CHAIN_NAME)
        parsed = parser.parse_receipt(receipt)
        assert len(parsed.market_pending_trades) == 1, (
            f"Expected exactly 1 MarketPendingTrade event, got {len(parsed.market_pending_trades)}. "
            f"Event topic expected: {EVENT_MARKET_PENDING_TRADE}"
        )
        ev = parsed.market_pending_trades[0]
        assert ev.user.lower() == funded_wallet.lower()
        assert ev.is_long is True
        assert ev.amount_in == int(collateral_amount * Decimal(10**18)), (
            f"Event amount_in {ev.amount_in} != expected {int(collateral_amount * Decimal(10**18))}"
        )
        assert ev.broker == 2, f"Broker must be PCS=2, got {ev.broker}"
        assert ev.qty > 0
        assert ev.price > 0
        assert ev.stop_loss == 0
        assert ev.take_profit == 0
        print(
            f"Parse OK: tradeHash={ev.trade_hash}, amountIn={ev.amount_in} wei, "
            f"qty={ev.qty} (1e10-scaled), price={ev.price} (1e8-scaled), broker={ev.broker}"
        )

        # Also verify the ResultEnricher-facing extraction methods work
        pos_id = parser.extract_position_id(receipt)
        assert pos_id == ev.trade_hash, "extract_position_id must match event.trade_hash"
        size_delta = parser.extract_size_delta(receipt)
        assert size_delta is not None and size_delta > 0
        collateral = parser.extract_collateral(receipt)
        assert collateral == Decimal(ev.amount_in)

        # -----------------------------------------------------------------
        # Layer 4 — Balance delta + on-chain state verification
        # -----------------------------------------------------------------
        bnb_after_wei = web3.eth.get_balance(funded_wallet)
        bnb_spent_wei = bnb_before_wei - bnb_after_wei
        margin_wei = int(collateral_amount * Decimal(10**18))
        # Use the orchestrator's pre-computed gas cost — it reads effective_gas_price
        # from the TransactionReceipt dataclass directly, avoiding camelCase/snake_case
        # mismatches when reading from the serialized receipt dict.
        gas_cost_wei = tx_result.gas_cost_wei
        # Exact native-BNB delta: trader pays margin + gas, nothing else moves native BNB
        # on the open path (margin is wrapped to WBNB inside the router via msg.value).
        # Mode-aware: under Zodiac the Safe is ``funded_wallet`` and only loses
        # the margin; the member EOA pays gas on the outer
        # ``execTransactionWithRole``. Under no_zodiac the EOA is
        # ``funded_wallet`` and pays both.
        is_zodiac_mode = funded_wallet.lower() != _EOA_ADDR.lower()
        expected_spent = margin_wei if is_zodiac_mode else margin_wei + gas_cost_wei
        assert bnb_spent_wei == expected_spent, (
            f"BNB delta mismatch (zodiac={is_zodiac_mode}): "
            f"expected {expected_spent / 1e18} (margin {margin_wei / 1e18}"
            + (f" + gas {gas_cost_wei / 1e18}" if not is_zodiac_mode else "")
            + f"), got {bnb_spent_wei / 1e18}"
        )
        print(f"BNB spent: {bnb_spent_wei / 1e18:.6f} (margin {margin_wei / 1e18}"
              + ("" if is_zodiac_mode else " + gas") + ")")

        # On-chain state: pending trade MUST exist for the returned tradeHash.
        pending_trade_data = _call_get_pending_trade(web3, router, ev.trade_hash)
        # First word is user address (low 20 bytes) — must equal our wallet
        first_word = pending_trade_data[:32]
        pending_user = "0x" + first_word[-20:].hex()
        assert pending_user.lower() == funded_wallet.lower(), (
            f"getPendingTrade user mismatch: expected {funded_wallet}, got {pending_user}"
        )
        # Second word is broker/isLong packed — at least some data must be non-zero (sanity check)
        assert pending_trade_data != b"\x00" * 32 * 11, "getPendingTrade returned all zeros — pending trade not found"
        print(f"On-chain: getPendingTrade({ev.trade_hash[:18]}...) user={pending_user} (matches)")

        print("\nALL 4 LAYERS PASSED")
