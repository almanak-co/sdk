"""Production-grade SwapIntent tests for Pendle on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent for Pendle token -> PT swaps
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using PendleReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

NOTE: PT -> token (sell PT) path is blocked on Arbitrum (VIB-568).
Tests cover the working token -> PT (buy PT) path.

To run:
    uv run pytest tests/intents/arbitrum/test_pendle_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.pendle.receipt_parser import PendleReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"

# Pendle market addresses on Arbitrum.
# PT-sUSDai-15OCT2026 market — a live, deeply-liquid Pendle market (~$13M TVL;
# expiry() = 1792022400 = 2026-10-15 UTC). It replaced the former
# PT-wstETH-25JUN2026 market, which expired 2026-06-25 and now reverts every
# swap into it (all wstETH Pendle markets on Arbitrum have matured).
PENDLE_SUSDAI_MARKET = "0xcbf629c8d396b1261f81f55175afa010e94787d8"
# PT-sUSDai-15OCT2026 token address.
PT_SUSDAI_ADDRESS = "0xb459db106f645d698e74027eef6019a26a0675cc"
# sUSDai is the market's SY-mint token (verified via SY.getTokensIn()), so a
# sUSDai -> PT buy needs no pre-swap.
SUSDAI_ADDRESS = "0x0B2b2B2076d95dda7817e785989fE353fe955ef9"
SUSDAI_SYMBOL = "sUSDai"


def _enrich_oracle_with_susdai(price_oracle: dict[str, Decimal]) -> dict[str, Decimal]:
    """Add a SUSDAI price to the oracle if missing (needed for compile estimation).

    sUSDai has no CoinGecko id, so the session price-oracle fixture never carries
    it. sUSDai is staked USDai and trades near $1; anchor it to USDC. When the
    input token IS sUSDai (== SY-mint token) no pre-swap is inserted, so the
    exact figure only affects the loose compile-time min-out estimate.
    """
    enriched = dict(price_oracle)
    if "SUSDAI" not in enriched:
        enriched["SUSDAI"] = enriched.get("USDC", Decimal("1"))
    return enriched


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.swap
class TestPendleSwapIntent:
    """Test Pendle swaps using SwapIntent.

    These tests verify the full Intent flow:
    - SwapIntent creation for token -> PT swaps (buy PT)
    - IntentCompiler generates correct Pendle router transactions
    - Transactions execute successfully on-chain
    - PendleReceiptParser correctly interprets Swap events
    - Balance changes match expected amounts

    NOTE: PT -> token (sell PT) path is blocked on Arbitrum (VIB-568).
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_susdai_to_pt_susdai_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test sUSDai -> PT-sUSDai-15OCT2026 swap using SwapIntent.

        This tests the token -> PT (buy PT) path in PendleCompiler.compile_swap.
        sUSDai IS the market's SY-mint token, so the compiler mints SY directly
        and swaps SY -> PT with NO V3 pre-swap leg.

        Flow:
        1. Create SwapIntent for sUSDai -> PT-SUSDAI-15OCT2026
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Parse receipt for Pendle Swap event
        5. Verify sUSDai decreased and PT tokens received
        """
        susdai = SUSDAI_ADDRESS
        susdai_decimals = get_token_decimals(web3, susdai)

        # Swap a small amount of sUSDai to PT
        swap_amount = Decimal("100")  # 100 sUSDai (~$105)

        print(f"\n{'=' * 80}")
        print("Test: sUSDai -> PT-sUSDai-15OCT2026 Swap via SwapIntent (Pendle)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} sUSDai")

        # Record balances BEFORE
        susdai_before = get_token_balance(web3, susdai, funded_wallet)
        pt_before = get_token_balance(web3, PT_SUSDAI_ADDRESS, funded_wallet)

        print(f"sUSDai before: {format_token_amount(susdai_before, susdai_decimals)}")
        print(f"PT before: {format_token_amount(pt_before, 18)}")  # PT has 18 decimals

        # Create SwapIntent
        intent = SwapIntent(
            from_token="sUSDai",
            to_token="PT-SUSDAI-15OCT2026",
            amount=swap_amount,
            max_slippage=Decimal("0.20"),  # 20% slippage for oracle-based min-out quoting
            protocol="pendle",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Compile intent with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_susdai(price_oracle),
            rpc_url=anvil_rpc_url,
        )

        print("Compiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Execute via ExecutionOrchestrator
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts — Layer 3 must fail closed: assert a swap was actually
        # parsed, otherwise a PendleReceiptParser regression (no swap_result) would
        # silently pass on the balance-delta layer alone.
        saw_swap = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = PendleReceiptParser(
                    chain=CHAIN_NAME,
                    token_in_decimals=18,  # sUSDai (SY-mint token; no pre-swap)
                    token_out_decimals=18,  # PT decimals
                )
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.swap_result:
                    saw_swap = True
                    print(f"  Swap type: {parse_result.swap_result.swap_type}")
                    print(f"  Amount in:  {parse_result.swap_result.amount_in_decimal}")
                    print(f"  Amount out: {parse_result.swap_result.amount_out_decimal}")
                    print(f"  Price:      {parse_result.swap_result.effective_price}")
                    assert parse_result.swap_result.amount_in_decimal > 0
                    assert parse_result.swap_result.amount_out_decimal > 0
                    assert parse_result.swap_result.effective_price > 0
        assert saw_swap, "PendleReceiptParser must parse a swap_result from the PT-buy receipt (Layer 3)"

        # Verify balance changes
        susdai_after = get_token_balance(web3, susdai, funded_wallet)
        pt_after = get_token_balance(web3, PT_SUSDAI_ADDRESS, funded_wallet)

        susdai_spent = susdai_before - susdai_after
        pt_received = pt_after - pt_before

        print("\n--- Results ---")
        print(f"sUSDai spent:  {format_token_amount(susdai_spent, susdai_decimals)}")
        print(f"PT received:   {format_token_amount(pt_received, 18)}")

        # Verify sUSDai was spent
        expected_susdai_spent = int(swap_amount * Decimal(10**susdai_decimals))
        assert susdai_spent == expected_susdai_spent, (
            f"sUSDai spent must EXACTLY equal swap amount. Expected: {expected_susdai_spent}, Got: {susdai_spent}"
        )

        # Verify PT tokens were received
        assert pt_received > 0, "Must receive positive PT tokens"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason=(
            "#2635: the USDC-input Pendle PT buy needs a pre-swap from USDC to "
            "the SY-mint token (sUSDai), and Arbitrum has no reliable on-fork "
            "USDC->sUSDai route, so compile/execute can fail on the CI pinned "
            "fork while the sUSDai->PT sibling (== SY-mint token, no pre-swap) "
            "passes. Kept strict=False: latest local forks may xpass when a "
            "route exists (as of 2026-06-29, updated for the sUSDai market roll)."
        ),
        strict=False,
    )
    async def test_swap_usdc_to_pt_susdai_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC -> PT-sUSDai-15OCT2026 swap using SwapIntent.

        This tests the token_mint_sy pre-swap path in PendleCompiler.compile_swap.
        USDC is not the SY-mint token (sUSDai), so the compiler must insert a
        pre-swap from USDC -> sUSDai before the Pendle swap.

        Flow:
        1. Create SwapIntent for USDC -> PT-SUSDAI-15OCT2026
        2. Compile to ActionBundle (includes pre-swap)
        3. Execute via ExecutionOrchestrator
        4. Parse receipt for Pendle Swap event
        5. Verify USDC decreased and PT tokens received
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        # Swap 100 USDC to PT
        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'=' * 80}")
        print("Test: USDC -> PT-sUSDai-15OCT2026 Swap via SwapIntent (Pendle with pre-swap)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        pt_before = get_token_balance(web3, PT_SUSDAI_ADDRESS, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"PT before: {format_token_amount(pt_before, 18)}")

        # Create SwapIntent
        intent = SwapIntent(
            from_token="USDC",
            to_token="PT-SUSDAI-15OCT2026",
            amount=swap_amount,
            max_slippage=Decimal("0.20"),  # 20% slippage for multi-hop (pre-swap + Pendle swap)
            protocol="pendle",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Compile intent with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_susdai(price_oracle),
            rpc_url=anvil_rpc_url,
        )

        print("Compiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Execute via ExecutionOrchestrator
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = PendleReceiptParser(
                    chain=CHAIN_NAME,
                    token_in_decimals=18,  # sUSDai (the actual Pendle swap input after pre-swap)
                    token_out_decimals=18,  # PT decimals
                )
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.swap_result:
                    print(f"  Swap type: {parse_result.swap_result.swap_type}")
                    print(f"  Amount in:  {parse_result.swap_result.amount_in_decimal}")
                    print(f"  Amount out: {parse_result.swap_result.amount_out_decimal}")
                    print(f"  Price:      {parse_result.swap_result.effective_price}")
                    assert parse_result.swap_result.amount_in_decimal > 0
                    assert parse_result.swap_result.amount_out_decimal > 0
                    assert parse_result.swap_result.effective_price > 0

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        pt_after = get_token_balance(web3, PT_SUSDAI_ADDRESS, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        pt_received = pt_after - pt_before

        print("\n--- Results ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"PT received:   {format_token_amount(pt_received, 18)}")

        # Verify USDC was spent
        expected_usdc_spent = int(swap_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal swap amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify PT tokens were received
        assert pt_received > 0, "Must receive positive PT tokens"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_intent_with_insufficient_balance_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test that SwapIntent with insufficient balance fails gracefully."""
        susdai = SUSDAI_ADDRESS

        # Get current balance and guard against zero
        susdai_balance = get_token_balance(web3, susdai, funded_wallet)
        pt_before = get_token_balance(web3, PT_SUSDAI_ADDRESS, funded_wallet)
        assert susdai_balance > 0, "Funded wallet must have positive sUSDai balance for this test"
        susdai_decimals = get_token_decimals(web3, susdai)
        balance_decimal = Decimal(susdai_balance) / Decimal(10**susdai_decimals)

        # Try to swap more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'=' * 80}")
        print("Test: SwapIntent with Insufficient Balance (Pendle)")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} sUSDai")
        print(f"Trying:    {excessive_amount} sUSDai")

        intent = SwapIntent(
            from_token="sUSDai",
            to_token="PT-SUSDAI-15OCT2026",
            amount=excessive_amount,
            max_slippage=Decimal("0.20"),
            protocol="pendle",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_susdai(price_oracle),
            rpc_url=anvil_rpc_url,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        # Try to execute - should fail
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        # Verify balances unchanged (bilateral conservation check)
        susdai_after = get_token_balance(web3, susdai, funded_wallet)
        pt_after = get_token_balance(web3, PT_SUSDAI_ADDRESS, funded_wallet)
        assert susdai_after == susdai_balance, "Input token balance must be unchanged after failed swap"
        assert pt_after == pt_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
