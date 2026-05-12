"""Intent-level tests for Aster Perps OPEN on BSC (VIB-3053).

Mirrors ``test_pancakeswap_perps_open.py`` but exercises ``protocol="aster_perps"``
— the canonical post-rebrand protocol key. The raw-Aster path uses broker_id=0
(no attribution) instead of 2 (PancakeSwap). Both routes compile to the same
on-chain Diamond; the only difference is the broker attribution in the
MarketPendingTrade event.

Runs the full Intent -> Compile -> Execute -> Parse -> Verify flow for
PerpOpenIntent(protocol='aster_perps'):

  1. PerpOpenIntent is created (market='BTC/USD', native BNB margin)
  2. IntentCompiler compiles to an ActionBundle with a single openMarketTradeBNB TX
  3. ExecutionOrchestrator executes on the Anvil BSC fork
  4. AsterPerpsReceiptParser decodes the MarketPendingTrade event and
     yields the tradeHash (== position_id) plus margin/qty/price details
  5. Native BNB balance decreases by exactly collateral_amount + gas, and a
     pending trade is registered on-chain (getPendingTrade returns populated struct)

To run:
    uv run pytest tests/intents/bnb/test_aster_perps_open.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.aster_perps import (
    ASTER_BROKER_RAW,
    EVENT_MARKET_PENDING_TRADE,
    AsterPerpsReceiptParser,
    encode_get_pending_trade_calldata,
)
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.perp_intents import PerpOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import TEST_WALLET as _EOA_ADDR

CHAIN_NAME = "bsc"


@pytest.fixture(scope="session")
def perps_price_oracle() -> dict[str, Decimal]:
    """Static prices for Aster Perps open tests.

    The compiler uses these to derive qty (size_usd / mark_price) and the
    slippage-to-limit-price bound. We pick values close enough to real-market
    that the computed limit passes Aster's internal "beforePrice" sanity gate
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
class TestAsterPerpsOpenIntent:
    """Test Aster Perps OPEN via PerpOpenIntent on BSC (raw broker, no attribution)."""

    @pytest.mark.intent(IntentType.PERP_OPEN)
    async def test_open_btc_long_native_bnb_margin(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        perps_price_oracle: dict[str, Decimal],
    ):
        """Open a long BTC/USD position with 0.3 BNB native margin, protocol=aster_perps.

        Verifies all 4 intent-test layers:
          1. Compilation SUCCESS with broker_id=0 (raw Aster, no attribution)
          2. Execution success (TX mined, status=1)
          3. Receipt parser extracts MarketPendingTrade with expected fields
             including broker=0
          4. Balance delta: BNB decreased by exactly margin + gas; pending trade
             exists on-chain
        """
        from almanak.core.contracts import ASTER_PERPS

        router = ASTER_PERPS[CHAIN_NAME]["router"]
        # Aster enforces a minimum position notional (TradingCheckerFacet:
        # "Position is too small"). $500 is well above the floor for BTC/USD.
        collateral_amount = Decimal("0.3")  # 0.3 BNB (~ $180 at $600/BNB)
        size_usd = Decimal("500")  # $500 notional
        mark_price = perps_price_oracle["BTC"]

        print(f"\n{'=' * 80}")
        print("Test: Aster Perps OPEN — LONG BTC/USD, native BNB margin, broker=0")
        print(f"{'=' * 80}")

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
            protocol="aster_perps",  # canonical key — raw Aster path
            leverage=Decimal("1"),
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=perps_price_oracle,
            rpc_url=orchestrator.rpc_url,
        )
        compilation = compiler.compile(intent)
        assert compilation.status.value == "SUCCESS", f"Compilation failed: {compilation.error}"
        assert compilation.action_bundle is not None
        assert len(compilation.action_bundle.transactions) == 1, (
            f"Expected exactly 1 TX (openMarketTradeBNB), got "
            f"{len(compilation.action_bundle.transactions)}"
        )
        # aster_perps routes to broker_id=0 (raw Aster, no attribution)
        assert compilation.action_bundle.metadata["broker_id"] == ASTER_BROKER_RAW, (
            f"protocol='aster_perps' must attribute to broker=0, got "
            f"{compilation.action_bundle.metadata['broker_id']}"
        )
        tx = compilation.action_bundle.transactions[0]
        assert tx["data"].startswith("0xb7aeae66"), (
            f"Expected openMarketTradeBNB selector 0xb7aeae66, got {tx['data'][:10]}"
        )
        assert int(tx["value"]) == int(collateral_amount * Decimal(10**18)), (
            "TX value must equal native BNB margin"
        )
        print(
            f"Compile OK: broker_id=0, to={tx['to']} value={int(tx['value']) / 1e18} BNB "
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
        # Layer 3 — Receipt parser (AsterPerpsReceiptParser, canonical)
        # -----------------------------------------------------------------
        parser = AsterPerpsReceiptParser(chain=CHAIN_NAME)
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
        # Raw Aster path: broker id = 0, NOT 2.
        assert ev.broker == ASTER_BROKER_RAW, (
            f"Broker must be raw Aster=0 (aster_perps protocol key), got {ev.broker}"
        )
        assert ev.qty > 0
        assert ev.price > 0
        assert ev.stop_loss == 0
        assert ev.take_profit == 0
        print(
            f"Parse OK: tradeHash={ev.trade_hash}, amountIn={ev.amount_in} wei, "
            f"qty={ev.qty} (1e10-scaled), price={ev.price} (1e8-scaled), broker={ev.broker}"
        )

        # ResultEnricher-facing extraction methods
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
        gas_cost_wei = tx_result.gas_cost_wei
        # Mode-aware: under Zodiac the Safe is ``funded_wallet`` and only loses
        # the margin (gas is paid by the member EOA on the outer
        # ``execTransactionWithRole``). Under no_zodiac the EOA is
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
        first_word = pending_trade_data[:32]
        pending_user = "0x" + first_word[-20:].hex()
        assert pending_user.lower() == funded_wallet.lower(), (
            f"getPendingTrade user mismatch: expected {funded_wallet}, got {pending_user}"
        )
        assert pending_trade_data != b"\x00" * 32 * 11, (
            "getPendingTrade returned all zeros — pending trade not found"
        )
        print(f"On-chain: getPendingTrade({ev.trade_hash[:18]}...) user={pending_user} (matches)")

        print("\nALL 4 LAYERS PASSED (aster_perps, broker_id=0)")
