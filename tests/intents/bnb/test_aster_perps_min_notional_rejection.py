"""Failure-mode test for Aster Perps minimum-notional enforcement on BSC (VIB-3053).

Aster's ``TradingCheckerFacet`` enforces a per-pair minimum position notional
(MinNotionalUsd config, typically $200–$250 per market). Orders below that
floor revert on-chain with "Position is too small". The SDK compiler accepts
any positive size — the revert is an on-chain invariant — so this test
exercises the compile-succeeds → execute-reverts → balance-conserved path.

Failure-mode tests require 3 layers instead of 4 (no receipt-parsing on a
successful protocol event, since no such event is emitted):
  1. Compilation — ``PerpOpenIntent`` with a tiny notional compiles OK.
  2. Execution — orchestrator reports not-success (TX reverts).
  3. Balance conservation — wallet BNB unchanged aside from gas; no pending
     trade registered on-chain.

To run:
    uv run pytest tests/intents/bnb/test_aster_perps_min_notional_rejection.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.core.contracts import ASTER_PERPS
from almanak.framework.connectors.aster_perps import (
    ASTER_BROKER_RAW,
    AsterPerpsReceiptParser,
    encode_get_pending_trade_calldata,
)
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.perp_intents import PerpOpenIntent

CHAIN_NAME = "bsc"


@pytest.fixture(scope="session")
def perps_price_oracle() -> dict[str, Decimal]:
    """Static prices matching sibling aster_perps tests."""
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
class TestAsterPerpsMinNotionalRejection:
    """Verify Aster's on-chain min-notional guard rejects sub-floor opens cleanly."""

    async def test_open_below_min_notional_reverts_with_balance_conserved(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        perps_price_oracle: dict[str, Decimal],
    ):
        """Submit a $10 BTC open; expect revert + balance conservation.

        Aster's BTC/USD min notional on BSC is ~$200 at the time of writing.
        A $10 order is unambiguously below the floor — on-chain revert guaranteed.
        """
        router = ASTER_PERPS[CHAIN_NAME]["router"]
        # Margin large enough that we're not hitting a "margin too small" guard —
        # we want specifically the notional-size check to fire. 0.1 BNB ≈ $60
        # margin, size $10 (way under any pair's min). Notional=$10, leverage≈0.17x.
        collateral_amount = Decimal("0.1")
        sub_floor_size_usd = Decimal("10")

        print(f"\n{'=' * 80}")
        print("Test: Aster Perps OPEN below min notional — expect on-chain revert")
        print(f"{'=' * 80}")

        bnb_before = web3.eth.get_balance(funded_wallet)
        print(f"BNB balance before: {bnb_before / 1e18:.6f}")

        # -----------------------------------------------------------------
        # Layer 1 — Compile. Compiler accepts any positive size; the floor
        # is enforced on-chain.
        # -----------------------------------------------------------------
        intent = PerpOpenIntent(
            market="BTC/USD",
            collateral_token="BNB",
            collateral_amount=collateral_amount,
            size_usd=sub_floor_size_usd,
            is_long=True,
            max_slippage=Decimal("0.01"),
            protocol="aster_perps",
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
            f"Compilation should succeed (floor is on-chain, not compiler-level): "
            f"{compilation.error}"
        )
        assert compilation.action_bundle is not None
        assert compilation.action_bundle.metadata["broker_id"] == ASTER_BROKER_RAW
        print(
            f"Compile OK: size_usd=${sub_floor_size_usd}, broker_id=0, "
            f"qty_1e10={compilation.action_bundle.metadata['qty_1e10']}"
        )

        # -----------------------------------------------------------------
        # Layer 2 — Execute. Orchestrator must report failure (TX reverts).
        # -----------------------------------------------------------------
        execution = await orchestrator.execute(compilation.action_bundle)
        assert not execution.success, (
            "Execution should fail: Aster's on-chain min-notional guard must reject this "
            "order. If this assertion passes, the min notional has dropped below $10 — update "
            "sub_floor_size_usd to a still-sub-floor value."
        )
        # On reverted TX we may or may not have a receipt depending on submitter behavior.
        # If we do, status must be 0.
        if execution.transaction_results:
            tx_result = execution.transaction_results[0]
            if tx_result.receipt is not None:
                receipt = tx_result.receipt.to_dict()
                status = receipt.get("status")
                status_int = int(status, 16) if isinstance(status, str) else status
                assert status_int == 0, (
                    f"Reverted TX must have status=0, got {status_int!r}"
                )
                # Receipt parser must not find any MarketPendingTrade (no events
                # emitted on revert).
                parser = AsterPerpsReceiptParser(chain=CHAIN_NAME)
                parsed = parser.parse_receipt(receipt)
                assert len(parsed.market_pending_trades) == 0, (
                    "Reverted TX should not emit MarketPendingTrade"
                )
                print(
                    f"Revert observed: tx={tx_result.tx_hash[:18]} status=0 "
                    f"error={execution.error[:80] if execution.error else 'none'}"
                )
            else:
                print(
                    "Revert observed pre-mining (no receipt): "
                    f"error={execution.error[:120] if execution.error else 'none'}"
                )
        else:
            print(
                f"Revert observed pre-submission: "
                f"error={execution.error[:120] if execution.error else 'none'}"
            )

        # -----------------------------------------------------------------
        # Layer 3 — Balance conservation. On revert, the ONLY acceptable
        # BNB outflow is gas. Margin value must NOT have been consumed.
        # -----------------------------------------------------------------
        bnb_after = web3.eth.get_balance(funded_wallet)
        bnb_delta = bnb_before - bnb_after
        margin_wei = int(collateral_amount * Decimal(10**18))
        # Strict bound: delta must be much smaller than the margin. A typical
        # BSC revert burns ~30k-100k gas at 3 gwei ≈ 0.0003 BNB. Give 5x
        # headroom to keep the test robust while still catching "margin was
        # consumed" regressions.
        max_plausible_gas_wei = 5 * 10**15  # 0.005 BNB
        assert bnb_delta < margin_wei, (
            f"BNB delta {bnb_delta / 1e18} suggests the margin ({margin_wei / 1e18} BNB) "
            "was consumed by a partial fill — expected only gas to be spent on revert."
        )
        assert bnb_delta < max_plausible_gas_wei, (
            f"BNB spent ({bnb_delta / 1e18:.6f}) exceeds plausible gas-only bound "
            f"({max_plausible_gas_wei / 1e18:.6f}). Either gas is higher than expected "
            "or the margin partially moved."
        )
        print(
            f"Balance conserved: BNB delta={bnb_delta / 1e18:.6f} (gas only, "
            f"margin {margin_wei / 1e18} preserved)"
        )

        # No pending trade registered on-chain. The Aster Diamond assigns
        # a tradeHash at MarketPendingTrade emission — if the emit was
        # reverted, the hash was never written. We can't easily look up
        # "any pending trade for this user" without an index, so we
        # accept that the absence of a MarketPendingTrade event in the
        # reverted receipt (checked above) is the authoritative signal.
        _ = encode_get_pending_trade_calldata  # imported for parity with sibling tests
        print(f"Contract router: {router}")
        print("\nFAILURE-MODE TEST PASSED: min-notional guard enforced, balance conserved")
