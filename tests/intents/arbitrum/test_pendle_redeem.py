"""4-layer PT_REDEEM intent test for Pendle on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
PT redemption at maturity:

1. Buy PT-wstETH-25JUN2026 via WETH->PT SwapIntent
2. Warp Anvil time past PT maturity (June 25, 2026)
3. Redeem PT via WithdrawIntent (redeemPyToToken)
4. Verify RedeemPY receipt event + balance deltas

After maturity, redeemPyToToken only requires PT (YT is expired/worthless).

To run:
    uv run pytest tests/intents/arbitrum/test_pendle_redeem.py -v -s -n0 --import-mode=importlib
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.pendle.receipt_parser import PendleReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent, WithdrawIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"

# PT-wstETH-25JUN2026 on Arbitrum
PENDLE_WSTETH_MARKET = "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b"
PT_WSTETH_ADDRESS = "0x71fbf40651e9d4278a74586afc99f307f369ce9a"

# YT-wstETH-25JUN2026 — required as market_id for WithdrawIntent redeem.
# Verified via readTokens() on the market contract.
YT_WSTETH_ADDRESS = "0x25bda1edd6af17c61399aa0eb84b93daa3069764"

# wstETH: output token for redemption
WSTETH_ADDRESS = "0x5979D7b546E38E414F7E9822514be443A4800529"

# PT maturity: June 25, 2026.
# Advance 61 days (to June 26) to be safely past maturity.
_SECONDS_PAST_MATURITY = 61 * 24 * 60 * 60  # 61 days

# Small PT buy amount to avoid heavy pre-swap impact
_WETH_BUY_AMOUNT = Decimal("0.01")


def _advance_time_past_maturity(web3: Web3) -> None:
    """Advance Anvil clock 61 days past PT maturity and mine one block."""
    web3.provider.make_request("evm_increaseTime", [_SECONDS_PAST_MATURITY])  # type: ignore[attr-defined]
    web3.provider.make_request("evm_mine", [])  # type: ignore[attr-defined]


def _enrich_oracle_with_wsteth(price_oracle: dict[str, Decimal]) -> dict[str, Decimal]:
    enriched = dict(price_oracle)
    if "WSTETH" not in enriched and "WETH" in enriched:
        enriched["WSTETH"] = enriched["WETH"] * Decimal("1.17")
    return enriched


# =============================================================================
# PT_REDEEM Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.swap
class TestPendlePTRedeemIntent:
    """4-layer tests for Pendle PT_REDEEM on Arbitrum.

    Flow: buy PT -> warp past maturity -> redeem -> verify.
    """

    @pytest.mark.intent(IntentType.SWAP, IntentType.WITHDRAW)
    # xfail-grandfathered: #1694 (pre-dates xfail-hygiene rule)
    @pytest.mark.xfail(
        strict=False,
        reason="PT approval (ERC20: insufficient allowance) is flaky in CI due to Anvil "
        "fork-block pinning + cached state: the approve tx mines but its state is not "
        "visible to the redeem tx during simulation, causing the orchestrator to submit "
        "both txs but the redeem reverts. Passes reliably on fresh local forks. "
        "Follow-up: investigate RollingForkManager cache invalidation after evm_increaseTime.",
    )
    @pytest.mark.asyncio
    async def test_redeem_pt_wsteth_at_maturity(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Buy PT-wstETH-25JUN2026, warp past maturity, redeem for wstETH.

        Layers:
        1. Compilation of WithdrawIntent → SUCCESS
        2. Execution on Anvil fork after time warp
        3. PendleReceiptParser finds RedeemPY event with sy_received > 0
        4. PT balance → 0, wstETH balance increases
        """
        tokens_cfg = {
            "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        }
        weth = tokens_cfg["WETH"]
        weth_decimals = get_token_decimals(web3, weth)
        wsteth_decimals = get_token_decimals(web3, WSTETH_ADDRESS)

        print(f"\n{'='*80}")
        print("Test: PT-wstETH-25JUN2026 Redeem at Maturity (Pendle)")
        print(f"{'='*80}")

        # ── Step 1: Buy some PT (sets up a PT balance to redeem) ──────────────
        print(f"\n--- Step 1: Buy PT via WETH→PT swap ({_WETH_BUY_AMOUNT} WETH) ---")
        buy_intent = SwapIntent(
            from_token="WETH",
            to_token="PT-WSTETH-25JUN2026",
            amount=_WETH_BUY_AMOUNT,
            max_slippage=Decimal("0.20"),
            protocol="pendle",
            chain=CHAIN_NAME,
        )
        buy_compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_wsteth(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        buy_result = buy_compiler.compile(buy_intent)
        assert buy_result.status.value == "SUCCESS", f"PT buy compilation failed: {buy_result.error}"
        buy_exec = await orchestrator.execute(buy_result.action_bundle)
        assert buy_exec.success, f"PT buy execution failed: {buy_exec.error}"

        pt_balance = get_token_balance(web3, PT_WSTETH_ADDRESS, funded_wallet)
        assert pt_balance > 0, "PT buy must yield positive PT balance"
        pt_amount_decimal = Decimal(pt_balance) / Decimal(10**18)
        print(f"PT acquired: {format_token_amount(pt_balance, 18)} PT ({pt_amount_decimal})")

        # ── Step 2: Warp past PT maturity ─────────────────────────────────────
        print("\n--- Step 2: Advancing Anvil time 61 days past maturity ---")
        _advance_time_past_maturity(web3)
        print("Time advanced. PT is now redeemable 1:1 for SY (wstETH).")

        # ── Layer 4 setup: record balances BEFORE redeem ──────────────────────
        wsteth_before = get_token_balance(web3, WSTETH_ADDRESS, funded_wallet)
        pt_before_redeem = get_token_balance(web3, PT_WSTETH_ADDRESS, funded_wallet)
        print(f"wstETH before: {format_token_amount(wsteth_before, wsteth_decimals)}")
        print(f"PT before:     {format_token_amount(pt_before_redeem, 18)}")

        # ── Layer 1: Compile WithdrawIntent ───────────────────────────────────
        print("\n--- Layer 1: Compile WithdrawIntent ---")
        # WithdrawIntent.market_id = YT address (required by compile_pendle_redeem)
        redeem_intent = WithdrawIntent(
            token="wstETH",
            amount=pt_amount_decimal,
            market_id=YT_WSTETH_ADDRESS,
            protocol="pendle",
            chain=CHAIN_NAME,
        )
        redeem_compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_wsteth(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        compilation_result = redeem_compiler.compile(redeem_intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None
        print(f"ActionBundle: {len(compilation_result.action_bundle.transactions)} transaction(s)")

        # ── Layer 2: Execute ──────────────────────────────────────────────────
        print("\n--- Layer 2: Execute ---")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful: {len(execution_result.transaction_results)} tx(s) confirmed")

        # ── Layer 3: Receipt parsing — expect RedeemPY event ─────────────────
        print("\n--- Layer 3: Receipt parsing ---")
        parser = PendleReceiptParser(chain=CHAIN_NAME)
        sy_received_raw: int | None = None
        py_redeemed_raw: int | None = None

        for i, tx_result in enumerate(execution_result.transaction_results):
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)

            # Pre-maturity path: RedeemPY from YT contract
            if parse_result.redeem_events:
                redeem = parse_result.redeem_events[0]
                sy_received_raw = redeem.net_sy_redeemed
                py_redeemed_raw = redeem.net_py_redeemed
                print(
                    f"\nTx {i+1} RedeemPY event:"
                    f"\n  net_py_redeemed: {redeem.net_py_redeemed}"
                    f"\n  net_sy_redeemed: {redeem.net_sy_redeemed}"
                )

            # Post-maturity path: SY Redeem (PT→SY→token, no YT involvement)
            if parse_result.redeem_sy_events:
                r = parse_result.redeem_sy_events[0]
                sy_received_raw = r.amount_sy_to_redeem
                py_redeemed_raw = r.amount_sy_to_redeem  # 1:1 at maturity
                print(
                    f"\nTx {i+1} RedeemSY event:"
                    f"\n  amount_sy_to_redeem: {r.amount_sy_to_redeem}"
                    f"\n  amount_token_out:    {r.amount_token_out}"
                    f"\n  token_out:           {r.token_out}"
                )

            redemption_amounts = parser.extract_redemption_amounts(receipt_dict)
            if redemption_amounts:
                assert redemption_amounts["sy_received"] > 0, (
                    "extract_redemption_amounts.sy_received must be positive"
                )

        assert sy_received_raw is not None, (
            "No RedeemPY or RedeemSY event found in any receipt"
        )
        assert sy_received_raw > 0, f"SY received must be positive, got {sy_received_raw}"
        assert py_redeemed_raw is not None and py_redeemed_raw > 0, (
            f"PT redeemed must be positive, got {py_redeemed_raw}"
        )

        # ── Layer 4: Balance deltas ───────────────────────────────────────────
        print("\n--- Layer 4: Balance deltas ---")
        wsteth_after = get_token_balance(web3, WSTETH_ADDRESS, funded_wallet)
        pt_after_redeem = get_token_balance(web3, PT_WSTETH_ADDRESS, funded_wallet)

        wsteth_received = wsteth_after - wsteth_before
        pt_spent = pt_before_redeem - pt_after_redeem

        print(f"wstETH received: {format_token_amount(wsteth_received, wsteth_decimals)}")
        print(f"PT burned:       {format_token_amount(pt_spent, 18)}")

        # PT balance must have decreased by the redeemed amount
        assert pt_spent == pt_balance, (
            f"PT burned must equal PT balance before redeem. "
            f"Expected: {pt_balance}, Got: {pt_spent}"
        )
        assert pt_after_redeem == 0, (
            f"PT balance must be zero after full redemption, got {pt_after_redeem}"
        )

        # wstETH received must be positive.
        # Note: 1 PT-wstETH redeems for 1 SY-wstETH which converts to <1 wstETH
        # because the SY-wstETH exchange rate grows over the market lifetime.
        # Only check that wstETH is positive — the exact amount depends on the
        # current SY exchange rate (not knowable at test time).
        assert wsteth_received > 0, "Must receive positive wstETH after PT_REDEEM"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    import pytest as _pytest

    _pytest.main([__file__, "-v", "-s", "-n0", "--import-mode=importlib"])
