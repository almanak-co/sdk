"""On-chain SwapIntent tests for Pendle YT swaps on Ethereum (VIB-3751).

Background — original bug:
    The Pendle Market ``Swap`` event reflects an *internal* PT flash-mint+sell
    that the router uses to synthesize YT exposure. For YT swaps its
    ``pt_amount`` / ``sy_amount`` are NOT the user-facing trade. Prior to the
    VIB-3751 fix the receipt parser misclassified YT swaps as PT sells and
    produced inflated ``amount_in`` (~60_898 sUSDe-shaped wei) which the QA
    harness rendered as a ``deployed_usd=$56,196.22`` for a $50 budget.

What this file guards:
    Compile -> Execute -> Receipt-parse -> Enrich path for a YT swap on
    Ethereum mainnet (forked via Anvil). The end-to-end assertion is that the
    enriched ``swap_amounts.amount_in_decimal`` matches the user's intent
    (~50 sUSDe, NOT ~60_898) AND that the wallet's actual sUSDe balance delta
    matches it.

This is the integration test CodeRabbit flagged as MAJOR-blocking on PR #1973
(`tests/intents/{chain}/...` regression for receipt-parser / compiler /
enricher changes is mandated by repo coding rules).

NOTE: The default ``funded_wallet`` for Ethereum does not seed sUSDe (it's not
in ``CHAIN_CONFIGS["ethereum"]["tokens"]``). We seed it locally via
``fund_erc20_token`` using sUSDe's _balances mapping at slot 4 (verified
live 2026-04-30 — the contract is StakedUSDeV2, a non-proxy ERC4626 with
custom inheritance that places _balances at slot 4, NOT slot 0).

To run (requires Ethereum Anvil fork on :8545):
    uv run pytest tests/intents/ethereum/test_pendle_yt_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.pendle.receipt_parser import PendleReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.execution.result_enricher import ResultEnricher
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from tests.intents.conftest import (
    fund_erc20_token,
    get_token_balance,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "ethereum"

# Ethena sUSDe (the YT underlying for YT-sUSDe-*).
SUSDE_ADDRESS = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
SUSDE_DECIMALS = 18
# sUSDe is StakedUSDeV2 — a non-proxy ERC4626 with custom inheritance
# (StakedUSDe + ERC20Permit + ERC20Votes + ERC4626 + ReentrancyGuard +
# AccessControl). The _balances mapping is at slot 4 (verified live
# 2026-04-30 via eth_getStorageAt against keccak(holder, 4) for the contract
# holding 10 sUSDe of itself — slot 0 is OZ ERC20Votes' _checkpoints).
# NOT slot 0 — the legacy ethena unstake intent test funds USDe (slot 0)
# and stakes it for sUSDe rather than seeding sUSDe directly.
SUSDE_BALANCE_SLOT = 4

# Currently active sUSDe YT market on Ethereum (Pendle market 0x177768...,
# expires 2026-08-13). The prior sUSDe YT (expired 2026-05-07) broke this
# test on 2026-05-11 when the weekly fork-block rollover bootstrapped a
# post-expiry pin and the Pendle Router started reverting
# `swapExactTokenForYt`. Rotate this address when the new market gets
# within 30 days of expiry — scripts/ci/check_pendle_expiry.py will flag
# it. The VIB-3751 bug this test guards is decimals + swap-type
# classification; it does not depend on the specific YT identity. From
# almanak/framework/connectors/pendle/sdk.py YT_TOKEN_INFO["ethereum"].
YT_SUSDE_ADDRESS = "0x45a699a11a4a17fe0931ef3cea4bfc3235e659f2"
YT_SUSDE_DECIMALS = 18


def _enrich_oracle_with_susde(price_oracle: dict[str, Decimal]) -> dict[str, Decimal]:
    """Add SUSDE/sUSDe price to oracle if missing.

    The default Ethereum oracle in ``conftest.py::_fetch_prices_sync`` only
    fetches symbols listed in ``CHAIN_CONFIGS["ethereum"]["tokens"]`` (USDC,
    WETH, USDT, wstETH). The compiler needs an sUSDe price to size the swap
    when ``amount_usd`` is used and to drive slippage estimation.

    sUSDe is roughly pegged to USDe ($1) plus accrued staking yield; the
    in-test price doesn't have to be exact because the swap uses
    ``amount`` (not ``amount_usd``) — but the compiler's pre-flight valuation
    code path still reads it for slippage / sanity checks.
    """
    enriched = dict(price_oracle)
    if "SUSDE" not in enriched:
        # Conservative default: sUSDe trades within a few % of USDe.
        # Real prices will be fetched by tests that pass enrich_oracle through.
        enriched["SUSDE"] = Decimal("1.10")
    if "sUSDe" not in enriched:
        enriched["sUSDe"] = enriched["SUSDE"]
    return enriched


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.swap
class TestPendleYTSwapIntent:
    """End-to-end YT-swap regression for VIB-3751.

    See module docstring for the bug context. The critical assertion is on
    ``swap_amounts.amount_in_decimal`` — the field the QA-harness
    ``deployed_usd`` calculation reads. Before the fix, this field reported
    ``~60_898`` (the internal flash-mint PT amount) instead of the user's
    ~50 sUSDe.
    """

    @pytest.mark.asyncio
    async def test_swap_susde_to_yt_susde_amount_in_matches_user_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """SwapIntent(50 sUSDe -> YT-sUSDe-13AUG2026) must report amount_in ≈ 50,
        NOT ≈ 60_898 (internal PT flash-mint), AND the wallet's sUSDe balance
        delta must match it. Guards the full Compile -> Execute -> Receipt
        -> Enrich pipeline.
        """
        # --- Seed wallet with sUSDe (not in default CHAIN_CONFIGS) ---
        susde_amount_human = Decimal("50")
        susde_amount_wei = int(susde_amount_human * Decimal(10**SUSDE_DECIMALS))
        # Fund 10x the trade amount so we can be sure about delta direction.
        fund_amount = susde_amount_wei * 10
        fund_erc20_token(
            funded_wallet,
            SUSDE_ADDRESS,
            fund_amount,
            SUSDE_BALANCE_SLOT,
            anvil_rpc_url,
        )

        susde_before = get_token_balance(web3, SUSDE_ADDRESS, funded_wallet)
        yt_before = get_token_balance(web3, YT_SUSDE_ADDRESS, funded_wallet)
        assert susde_before >= fund_amount, (
            f"sUSDe seeding failed: have {susde_before}, expected >= {fund_amount}. "
            f"Check SUSDE_BALANCE_SLOT={SUSDE_BALANCE_SLOT} (StakedUSDeV2 _balances slot)."
        )

        print(f"\n{'='*80}")
        print("VIB-3751 regression: 50 sUSDe -> YT-sUSDe-13AUG2026 via Pendle")
        print(f"{'='*80}")
        print(f"sUSDe before: {susde_before / 10**SUSDE_DECIMALS:.4f}")
        print(f"YT before:    {yt_before / 10**YT_SUSDE_DECIMALS:.4f}")

        # --- Build & compile intent ---
        intent = SwapIntent(
            from_token="sUSDe",
            to_token="YT-sUSDe-13AUG2026",
            amount=susde_amount_human,
            max_slippage=Decimal("0.20"),
            protocol="pendle",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_susde(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        bundle = compilation_result.action_bundle
        assert bundle is not None

        # Verify the compiler exports the metadata fields the receipt parser
        # needs to reconstruct user-facing YT amounts (the BUG-59 fix payload).
        meta = bundle.metadata
        assert meta.get("protocol") == "pendle"
        assert meta.get("swap_type") == "token_to_yt"
        assert meta.get("to_token_address", "").lower() == YT_SUSDE_ADDRESS.lower()
        assert int(meta.get("to_token_decimals") or 0) == YT_SUSDE_DECIMALS
        assert meta.get("wallet_address", "").lower() == funded_wallet.lower()
        from_meta = meta.get("from_token") or {}
        assert from_meta.get("address", "").lower() == SUSDE_ADDRESS.lower()
        assert int(from_meta.get("decimals") or 0) == SUSDE_DECIMALS

        # --- Execute on-chain ---
        execution_result = await orchestrator.execute(bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"\nExecution: {len(execution_result.transaction_results)} tx(s) confirmed")

        # --- Find the swap tx (skip the approve tx) and parse its receipt ---
        swap_tx = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            # The swap tx will have a Pendle Market Swap event (large logs); the
            # approve tx has only an ERC20 Approval. Cheap discriminator:
            # number of logs.
            log_count = len(tx_result.receipt.logs or [])
            if log_count > 5:
                swap_tx = tx_result
                break
        assert swap_tx is not None, "Could not locate the Pendle swap tx in the bundle"

        # Use the parser directly with the compiler-supplied context, exactly
        # mirroring what ResultEnricher does in production.
        parser = PendleReceiptParser(
            chain=CHAIN_NAME,
            token_in_decimals=SUSDE_DECIMALS,
            token_out_decimals=YT_SUSDE_DECIMALS,
        )
        receipt_dict = swap_tx.receipt.to_dict()
        parse_result = parser.parse_receipt(
            receipt_dict,
            intent_swap_type="token_to_yt",
            token_in_address=SUSDE_ADDRESS,
            token_out_address=YT_SUSDE_ADDRESS,
            token_in_decimals=SUSDE_DECIMALS,
            token_out_decimals=YT_SUSDE_DECIMALS,
            wallet_address=funded_wallet,
        )
        assert parse_result.success, f"Receipt parse failed: {parse_result.error}"
        sr = parse_result.swap_result
        assert sr is not None, "Pendle parser returned no swap_result"

        # The user-facing label (the bug guard) — pre-fix this would have been
        # "sell_pt".
        assert sr.swap_type == "buy_yt", (
            f"Expected swap_type='buy_yt' (user-facing YT trade), got '{sr.swap_type}'. "
            "If this is 'sell_pt' the fix has regressed and YT receipt parsing is "
            "back on the broken legacy PT-direction path."
        )

        # The dollar/amount guard. Pre-fix this was ~60_898 (the internal
        # flash-mint PT amount). Post-fix it must be the user's intent (50)
        # within slippage tolerance.
        amount_in = sr.amount_in_decimal
        assert amount_in == susde_amount_human, (
            f"amount_in_decimal={amount_in}, expected {susde_amount_human}. "
            "If this is ~60_898 the YT swap is being misclassified as sell_pt "
            "again — the QA harness will report deployed_usd ≈ $60_898 * sUSDe "
            "price instead of ~$50."
        )

        # YT received must be reasonable for a YT trade — typically ~10-100x
        # the underlying input depending on time-to-expiry and yield, so a
        # broad sanity range is enough.
        amount_out = sr.amount_out_decimal
        assert amount_out > 0, "Must receive positive YT tokens"
        assert amount_out < susde_amount_human * Decimal("10000"), (
            "YT amount unreasonably large — likely decimals applied wrong"
        )

        # --- Bilateral on-chain conservation check ---
        susde_after = get_token_balance(web3, SUSDE_ADDRESS, funded_wallet)
        yt_after = get_token_balance(web3, YT_SUSDE_ADDRESS, funded_wallet)
        susde_spent_wei = susde_before - susde_after
        yt_received_wei = yt_after - yt_before

        # Wallet sUSDe delta MUST equal the user's intent — this is the primary
        # invariant the QA harness's deployed_usd column reads.
        assert susde_spent_wei == susde_amount_wei, (
            f"sUSDe wallet delta={susde_spent_wei} wei, expected exactly "
            f"{susde_amount_wei} wei (50 sUSDe). The receipt parser's "
            f"amount_in must match the on-chain delta."
        )
        assert yt_received_wei > 0, "YT balance must increase"

        # The receipt-parser-reported amount_in MUST match the on-chain delta
        # in raw units (this is the real teeth of VIB-3751 — the parser must
        # see what the wallet sees).
        assert sr.amount_in == susde_amount_wei, (
            f"parser amount_in={sr.amount_in} wei != on-chain delta="
            f"{susde_amount_wei} wei. The receipt parser is reporting a "
            f"different number than the user actually paid."
        )
        # Likewise, parser's amount_out must match the on-chain YT delta.
        assert sr.amount_out == yt_received_wei, (
            f"parser amount_out={sr.amount_out} wei != on-chain YT delta="
            f"{yt_received_wei} wei."
        )

        print("\n--- Receipt parser results ---")
        print(f"  swap_type:          {sr.swap_type}")
        print(f"  amount_in_decimal:  {sr.amount_in_decimal} (raw {sr.amount_in})")
        print(f"  amount_out_decimal: {sr.amount_out_decimal} (raw {sr.amount_out})")
        print(f"  effective_price:    {sr.effective_price}")
        print("\n--- On-chain wallet deltas ---")
        print(f"  sUSDe: -{susde_spent_wei / 10**SUSDE_DECIMALS}")
        print(f"  YT:    +{yt_received_wei / 10**YT_SUSDE_DECIMALS}")
        print("\nVIB-3751 INVARIANTS HOLD: amount_in matches user intent (~50, not ~60_898).")

    @pytest.mark.asyncio
    async def test_yt_swap_enricher_threads_decimals_through_metadata(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Verify the production ResultEnricher path threads compiler-supplied
        decimals end-to-end so non-18-decimal markets would not silently
        revert to the parser's 18-decimal default. (The full pipeline check.)

        We can't easily exercise a non-18-decimal Pendle market on Ethereum
        (sUSDe is 18 decimals), but we can verify the wiring: that the
        enricher produces parser kwargs that include
        ``token_in_decimals`` / ``token_out_decimals`` from the compiler's
        ``ActionBundle.metadata``. That is the regression CodeRabbit's
        Major-finding asks the integration layer to guard.
        """
        # Seed sUSDe (same as test 1) and compile a real bundle.
        susde_amount_human = Decimal("10")
        susde_amount_wei = int(susde_amount_human * Decimal(10**SUSDE_DECIMALS))
        fund_erc20_token(
            funded_wallet,
            SUSDE_ADDRESS,
            susde_amount_wei * 10,
            SUSDE_BALANCE_SLOT,
            anvil_rpc_url,
        )

        intent = SwapIntent(
            from_token="sUSDe",
            to_token="YT-sUSDe-13AUG2026",
            amount=susde_amount_human,
            max_slippage=Decimal("0.20"),
            protocol="pendle",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_susde(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        bundle = compilation_result.action_bundle
        assert bundle is not None

        # Drive the enricher's ``_build_extract_kwargs`` directly — same code
        # path the orchestrator runs in production after every SWAP.
        # ``_build_extract_kwargs`` is a @staticmethod (see result_enricher.py).
        kwargs = ResultEnricher._build_extract_kwargs("swap_amounts", bundle.metadata)

        # The Pendle YT context that VIB-3751 introduced. ALL must be present
        # — Gemini's PR #1973 high-priority concern was that the enricher
        # might silently drop these for non-18-decimal markets.
        assert kwargs.get("intent_swap_type") == "token_to_yt"
        assert kwargs.get("token_out_address", "").lower() == YT_SUSDE_ADDRESS.lower()
        assert kwargs.get("wallet_address", "").lower() == funded_wallet.lower()
        assert kwargs.get("token_in_address", "").lower() == SUSDE_ADDRESS.lower()

        # The decimals fields — Gemini's L535/L1094/L1210/null findings. They
        # must be int-coerced (the receipt parser uses ``10**decimals``).
        token_in_decimals = kwargs.get("token_in_decimals")
        token_out_decimals = kwargs.get("token_out_decimals")
        assert token_in_decimals == SUSDE_DECIMALS, (
            f"token_in_decimals={token_in_decimals!r} not threaded from compiler. "
            "If None or missing, non-18-decimal Pendle markets (e.g. Plasma "
            "fUSDT0=6) would be off by 10^12 in production."
        )
        assert token_out_decimals == YT_SUSDE_DECIMALS, (
            f"token_out_decimals={token_out_decimals!r} not threaded from compiler."
        )
        assert isinstance(token_in_decimals, int)
        assert isinstance(token_out_decimals, int)

        # And confirm the test 1 invariants still hold under live execution
        # (defense-in-depth — if test 1 ever stops running this still catches
        # the dollar bug).
        execution_result = await orchestrator.execute(bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        # Find the swap tx and confirm its parser output uses the threaded decimals.
        swap_tx = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt and len(tx_result.receipt.logs or []) > 5:
                swap_tx = tx_result
                break
        assert swap_tx is not None
        parser = PendleReceiptParser(chain=CHAIN_NAME)  # constructor decimals at default 18
        amounts = parser.extract_swap_amounts(swap_tx.receipt.to_dict(), **kwargs)
        assert amounts is not None
        # If decimals had silently fallen back to constructor defaults this
        # value would still be 10 because sUSDe IS 18-decimal — but a future
        # non-18-decimal market would corrupt here. We assert the value is
        # the user's intent (10) to catch any future scaling regression.
        assert amounts.amount_in_decimal == susde_amount_human, (
            f"amount_in_decimal={amounts.amount_in_decimal} != {susde_amount_human}. "
            "Decimals not honored end-to-end through enricher -> parser."
        )

        print(f"\nEnricher threads decimals: token_in={token_in_decimals}, "
              f"token_out={token_out_decimals}. amount_in={amounts.amount_in_decimal}.")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
