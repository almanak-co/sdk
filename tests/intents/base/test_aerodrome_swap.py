"""Production-grade SwapIntent tests for Aerodrome on Base.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using AerodromeReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/base/test_aerodrome_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_swap_semantic_match,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_aerodrome_cl_pool_missing, fail_if_aerodrome_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "base"


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.swap
class TestAerodromeSwapIntent:
    """Test Aerodrome swaps using SwapIntent.

    These tests verify the full Intent flow:
    - SwapIntent creation with proper parameters
    - IntentCompiler generates correct Aerodrome transactions
    - Transactions execute successfully on-chain
    - AerodromeReceiptParser correctly interprets results
    - Balance changes match expected amounts
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdc_to_weth_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC -> WETH swap using SwapIntent.

        Flow:
        1. Create SwapIntent for USDC -> WETH
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify balances changed correctly
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        # Validate pool exists before running test
        fail_if_aerodrome_cl_pool_missing(web3, CHAIN_NAME, token_in, token_out, 100)

        # Get decimals
        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        # Amount to swap
        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'=' * 80}")
        print("Test: USDC -> WETH Swap via SwapIntent")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Record balances before
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, out_decimals)}")

        # Create SwapIntent
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=Decimal("0.20"),  # 20% slippage for oracle-based quoting
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Compile intent with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
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

        # Layer 3: parse receipts — enforce that at least one swap event was
        # successfully parsed with positive amounts. The for-loop walks every tx
        # in the bundle (approve + swap); only the swap tx carries a Swap event,
        # so we require >= 1 parsed swap result across the bundle rather than per
        # tx. A conditional parse that silently passes on zero parsed events
        # would leave the 4-layer receipt verification uncovered.
        swap_results_parsed = 0
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt is None:
                continue

            from almanak.connectors.aerodrome.receipt_parser import AerodromeReceiptParser

            # Pass token0/token1 (sorted ascending by address) so the parser can
            # resolve decimals and build a high-level swap_result for the CL
            # (SwapCL) event — base Aerodrome routes USDC/WETH through a
            # Slipstream CL pool, whose Swap event carries signed amount0/amount1
            # that need the token mapping to decode. Mirrors production wiring
            # and the optimism test.
            token0_addr, token1_addr = sorted([token_in.lower(), token_out.lower()])
            parser = AerodromeReceiptParser(
                chain=CHAIN_NAME,
                token0_address=token0_addr,
                token1_address=token1_addr,
            )
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

            if parse_result.success and parse_result.swap_result:
                swap_results_parsed += 1
                print(f"  Amount in:  {parse_result.swap_result.amount_in_decimal}")
                print(f"  Amount out: {parse_result.swap_result.amount_out_decimal}")
                print(f"  Price:      {parse_result.swap_result.effective_price}")

                assert parse_result.swap_result.amount_in_decimal > 0, (
                    "Receipt parser: amount_in_decimal must be positive"
                )
                assert parse_result.swap_result.amount_out_decimal > 0, (
                    "Receipt parser: amount_out_decimal must be positive"
                )
                assert parse_result.swap_result.effective_price > 0, "Receipt parser: effective_price must be positive"

                # L3 semantic verification
                assert_swap_semantic_match(
                    intent_amount=swap_amount,
                    intent_from_token="USDC",
                    intent_to_token="WETH",
                    swap_result=parse_result.swap_result,
                    chain=CHAIN_NAME,
                )
                print("  L3 semantic check: PASSED")

        assert swap_results_parsed >= 1, (
            "Layer 3 (receipt parsing) must parse at least one swap event across "
            f"the executed bundle. Got {swap_results_parsed} parsed swap results."
        )

        # Verify balance changes
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before

        print("\n--- Results ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WETH received: {format_token_amount(weth_received, out_decimals)}")

        # Verify USDC was spent
        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must equal swap amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify WETH was received
        assert weth_received > 0, "Must receive positive WETH"

        print("\nALL CHECKS PASSED ✓")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_weth_to_usdc_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WETH -> USDC swap using SwapIntent (reverse direction)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WETH"]
        token_out = tokens["USDC"]

        # Validate pool exists before running test
        fail_if_aerodrome_cl_pool_missing(web3, CHAIN_NAME, token_in, token_out, 100)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("0.05")  # 0.05 WETH

        print(f"\n{'=' * 80}")
        print("Test: WETH -> USDC Swap via SwapIntent")
        print(f"{'=' * 80}")

        weth_before = get_token_balance(web3, token_in, funded_wallet)
        usdc_before = get_token_balance(web3, token_out, funded_wallet)

        # Create intent
        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=Decimal("0.20"),  # 20% slippage for oracle-based quoting (VIB-2297)
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        # Compile with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        # Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success

        # Verify
        weth_after = get_token_balance(web3, token_in, funded_wallet)
        usdc_after = get_token_balance(web3, token_out, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_received = usdc_after - usdc_before

        expected_weth_spent = int(swap_amount * Decimal(10**in_decimals))
        assert weth_spent == expected_weth_spent
        assert usdc_received > 0

        print(f"WETH spent:    {format_token_amount(weth_spent, in_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, out_decimals)}")
        print("\nALL CHECKS PASSED ✓")

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
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]

        token_out = tokens["WETH"]

        # Get current balance
        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)

        # Try to swap more than we have.
        excessive_amount = balance_decimal * Decimal("2")

        print(f"\n{'=' * 80}")
        print("Test: SwapIntent with Insufficient Balance")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=excessive_amount,
            max_slippage=Decimal("0.01"),
            # This test exercises execution-level balance failure, not the
            # ALM-2890 price-impact guard. An oversized swap (here, larger than
            # the funded balance) would otherwise be rejected at compile time by
            # the guard; allow any impact (1 = 100%) so compilation succeeds and
            # the swap fails at execution on insufficient balance instead.
            max_price_impact=Decimal("1"),
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
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
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_after == usdc_balance, "Input token balance must be unchanged after failed swap"
        assert weth_after == weth_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED ✓")

    # =========================================================================
    # VIB-5548 / ALM-2889 — reachable swap_params + per-pair routing
    # =========================================================================

    # DAI on Base is not in the shared chain config (no funding slot), but it is
    # only the OUTPUT of the fallback swap below, so it never needs funding — we
    # just need its address to read the received balance.
    _DAI_BASE = "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb"

    @pytest.mark.no_zodiac(
        reason="VIB-5548: classic-routed Aerodrome swap targets the Classic router, "
        "which the default-on Zodiac manifest (derived from synthetic intents that only "
        "emit the default CL route) does not authorize. Routing/fallback correctness is "
        "what's under test; Zodiac authz of the alternate Classic route is a separate "
        "permission-discovery follow-up (the (aerodrome, SWAP) coverage gate is satisfied "
        "by the CL test). As of 2026-06-30."
    )
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdbc_to_dai_classic_fallback_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """USDC -> DAI: no Aerodrome CL pool exists at any candidate tick spacing,
        but a Classic stable pool does. The per-pair resolver (VIB-5548) must
        auto-fall-back to Classic (loud + metadata-stamped) and execute — the
        exact ALM-2889 case that previously hard-failed with no recovery.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = self._DAI_BASE

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)
        swap_amount = Decimal("50")  # 50 USDC

        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        dai_before = get_token_balance(web3, token_out, funded_wallet)

        intent = SwapIntent(
            from_token="USDC",
            to_token="DAI",
            amount=swap_amount,
            max_slippage=Decimal("0.20"),
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        # The fallback must be visible in metadata.
        meta = compilation_result.action_bundle.metadata
        assert meta["routing"] == "classic", f"Expected Classic fallback routing, got {meta.get('routing')}"
        assert meta["routing_fallback"] is True, "routing_fallback must be stamped True on auto CL->Classic fallback"
        assert meta["stable"] is True, "USDC/DAI is a stable/stable pair -> Classic stable pool"

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: receipt parse (Classic Solidly Swap event).
        swap_results_parsed = 0
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            from almanak.connectors.aerodrome.receipt_parser import AerodromeReceiptParser

            token0_addr, token1_addr = sorted([token_in.lower(), token_out.lower()])
            parser = AerodromeReceiptParser(chain=CHAIN_NAME, token0_address=token0_addr, token1_address=token1_addr)
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            if parse_result.success and parse_result.swap_result:
                swap_results_parsed += 1
                assert parse_result.swap_result.amount_in_decimal > 0
                assert parse_result.swap_result.amount_out_decimal > 0
        assert swap_results_parsed >= 1, "Layer 3: must parse at least one Classic swap event"

        # Layer 4: bilateral balance deltas.
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        dai_after = get_token_balance(web3, token_out, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        dai_received = dai_after - dai_before

        expected_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_spent == expected_spent, f"USDC spent must equal swap amount: {expected_spent} vs {usdc_spent}"
        assert dai_received > 0, "Must receive positive DAI via the Classic fallback pool"
        print(f"\nClassic fallback OK: spent {usdc_spent} USDC, received {dai_received} DAI (decimals={out_decimals})")

    @pytest.mark.no_zodiac(
        reason="VIB-5548: swap_params={'classic': True} routes through the Classic router, "
        "which the default-on Zodiac manifest (synthetic-intent CL route only) does not "
        "authorize. Override-routing correctness is under test; Zodiac authz of the Classic "
        "route is a separate permission-discovery follow-up. As of 2026-06-30."
    )
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_classic_override_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """swap_params={'classic': True} must force Classic routing even where a CL
        pool exists. USDC/USDbC has both a CL pool (ts=1) and a deep Classic stable
        pool; the override must pick Classic, not CL."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["USDbC"]

        # Pre-test validation: the Classic stable pool this override targets must
        # exist (USDC/USDbC is a stable/stable pair), else the env prerequisite
        # masquerades as a routing failure.
        fail_if_aerodrome_pool_missing(web3, CHAIN_NAME, token_in, token_out, stable=True)

        in_decimals = get_token_decimals(web3, token_in)
        swap_amount = Decimal("50")  # 50 USDC

        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        usdbc_before = get_token_balance(web3, token_out, funded_wallet)

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDbC",
            amount=swap_amount,
            max_slippage=Decimal("0.20"),
            protocol="aerodrome",
            chain=CHAIN_NAME,
            swap_params={"classic": True},
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        meta = compilation_result.action_bundle.metadata
        assert meta["routing"] == "classic", f"classic=True must route Classic, got {meta.get('routing')}"
        assert meta["routing_fallback"] is False, "explicit classic=True is not a fallback"

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: receipt parse (Classic Solidly Swap event).
        swap_results_parsed = 0
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            from almanak.connectors.aerodrome.receipt_parser import AerodromeReceiptParser

            token0_addr, token1_addr = sorted([token_in.lower(), token_out.lower()])
            parser = AerodromeReceiptParser(chain=CHAIN_NAME, token0_address=token0_addr, token1_address=token1_addr)
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            if parse_result.success and parse_result.swap_result:
                swap_results_parsed += 1
                assert parse_result.swap_result.amount_in_decimal > 0
                assert parse_result.swap_result.amount_out_decimal > 0
        assert swap_results_parsed >= 1, "Layer 3: must parse at least one Classic swap event"

        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        usdbc_after = get_token_balance(web3, token_out, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        usdbc_received = usdbc_after - usdbc_before

        assert usdc_spent == int(swap_amount * Decimal(10**in_decimals))
        assert usdbc_received > 0, "Must receive positive USDbC via the Classic stable pool"
        print(f"\nclassic=True override OK: spent {usdc_spent} USDC, received {usdbc_received} USDbC")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_tick_spacing_override_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """swap_params={'tick_spacing': N} must reach the resolver and pin a CL pool
        at exactly that spacing (no fallback), end-to-end on-chain.

        We pin ts=100 here: it is the deep, executable USDC/WETH CL pool and shares
        the cl_router target with the default route, so the explicit escape hatch is
        proven to flow through compile -> execute -> balance delta. The on-chain
        USDC/WETH ts=200 pool is effectively empty (a 100-USDC probe implies ~98%
        price impact, correctly blocked by the ALM-2890 guard), so the *exact-200*
        resolution / probe-once behaviour is covered by the unit matrix
        (tests/unit/intents/test_aerodrome_routing.py::test_explicit_tick_spacing_*)
        rather than executed here."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        pinned_tick_spacing = 100
        fail_if_aerodrome_cl_pool_missing(web3, CHAIN_NAME, token_in, token_out, pinned_tick_spacing)

        in_decimals = get_token_decimals(web3, token_in)
        swap_amount = Decimal("100")  # 100 USDC

        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=Decimal("0.20"),
            protocol="aerodrome",
            chain=CHAIN_NAME,
            swap_params={"tick_spacing": pinned_tick_spacing},
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        meta = compilation_result.action_bundle.metadata
        assert meta["routing"] == "cl", f"tick_spacing override must route CL, got {meta.get('routing')}"
        assert meta["tick_spacing"] == pinned_tick_spacing, (
            f"Expected pinned tick_spacing={pinned_tick_spacing}, got {meta.get('tick_spacing')}"
        )
        assert meta["routing_fallback"] is False, "explicit tick_spacing is not a fallback"

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: receipt parse (Slipstream CL SwapCL event).
        swap_results_parsed = 0
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            from almanak.connectors.aerodrome.receipt_parser import AerodromeReceiptParser

            token0_addr, token1_addr = sorted([token_in.lower(), token_out.lower()])
            parser = AerodromeReceiptParser(chain=CHAIN_NAME, token0_address=token0_addr, token1_address=token1_addr)
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            if parse_result.success and parse_result.swap_result:
                swap_results_parsed += 1
                assert parse_result.swap_result.amount_in_decimal > 0
                assert parse_result.swap_result.amount_out_decimal > 0
        assert swap_results_parsed >= 1, "Layer 3: must parse at least one CL swap event"

        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before

        assert usdc_spent == int(swap_amount * Decimal(10**in_decimals))
        assert weth_received > 0, "Must receive positive WETH via the pinned CL pool"
        print(
            f"\ntick_spacing={pinned_tick_spacing} override OK: spent {usdc_spent} USDC, received {weth_received} WETH"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
