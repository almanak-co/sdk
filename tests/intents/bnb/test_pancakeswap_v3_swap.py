"""Production-grade SwapIntent tests for PancakeSwap V3 on BSC.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using PancakeSwapV3ReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/bnb/test_pancakeswap_v3_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.core.rpc_network import Network
from almanak.framework.data.tokens.resolver import TokenResolver
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import Intent, SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.teardown import PositionInfo, PositionType, full_close_intents
from almanak.integrations.chainlink.gateway.live import ChainlinkPriceSource
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    SWAP_MAX_SLIPPAGE,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_v3_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "bsc"
XAUT0 = "0x21caef8a43163eea865baee23b9c2e327696a3bf"
BSC_USD = "0x55d398326f99059ff775485246999027b3197955"
APPROVED_XAUT0_POOL = "0xc655e1a100a084d9ac91c269b0a7cb0e62263fcf"


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.bsc
@pytest.mark.swap
class TestPancakeSwapV3SwapIntent:
    """Test PancakeSwap V3 swaps using SwapIntent on BSC.

    These tests verify the full Intent flow:
    - SwapIntent creation with proper parameters
    - IntentCompiler generates correct PancakeSwap V3 transactions
    - Transactions execute successfully on-chain
    - PancakeSwapV3ReceiptParser correctly interprets results
    - Balance changes match expected amounts
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_xaut0_exact_pool_entry_and_teardown_round_trip(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        anvil_eth_call_adapter,
        price_oracle: dict[str, Decimal],
        tmp_path,
    ):
        """A $1 virtual-fork entry and unwind both use only the approved pool.

        This test is isolated by ``anvil_bsc`` and cannot submit to mainnet.
        """
        from almanak.connectors.pancakeswap_v3.receipt_parser import PancakeSwapV3ReceiptParser

        xaut0 = Web3.to_checksum_address(XAUT0)
        bsc_usd = Web3.to_checksum_address(BSC_USD)
        approved_pool = APPROVED_XAUT0_POOL.lower()
        bsc_usd_decimals = get_token_decimals(web3, bsc_usd)
        xaut0_decimals = get_token_decimals(web3, xaut0)
        assert (bsc_usd_decimals, xaut0_decimals) == (18, 6)

        resolver = TokenResolver(cache_file=str(tmp_path / "tokens.json"))
        resolver.register_token(
            symbol="XAUT0",
            chain=CHAIN_NAME,
            address=xaut0,
            decimals=xaut0_decimals,
            name="Tether Gold",
        )
        reference_source = ChainlinkPriceSource(chain=CHAIN_NAME, network=Network.ANVIL)
        try:
            xau_reference = await reference_source.get_reference_price("XAU", "USD")
        finally:
            await reference_source.close()
        assert xau_reference.price > 0
        assert xau_reference.source.endswith("0x86896feb19d8a607c3b11f2af50a0f239bd71cd0")
        exact_prices = dict(price_oracle)
        exact_prices.update(
            {
                "XAUT0": xau_reference.price,
                xaut0.lower(): xau_reference.price,
                f"bsc:{xaut0.lower()}": xau_reference.price,
            }
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=exact_prices,
            rpc_url=orchestrator.rpc_url,
            token_resolver=resolver,
            venue_verification_gateway_factory=lambda: anvil_eth_call_adapter,
        )
        parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)

        # Entry layers 1-2: compile and execute one pool-pinned virtual $1 swap.
        entry_amount = Decimal("1")
        bsc_usd_before = get_token_balance(web3, bsc_usd, funded_wallet)
        xaut0_before = get_token_balance(web3, xaut0, funded_wallet)
        entry = SwapIntent(
            from_token=bsc_usd,
            to_token=xaut0,
            amount=entry_amount,
            max_slippage=Decimal("0.0075"),
            max_price_impact=Decimal("1"),
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
            swap_params={"pool": Web3.to_checksum_address(APPROVED_XAUT0_POOL)},
        )
        entry_compilation = compiler.compile(entry)
        assert entry_compilation.status.value == "SUCCESS", f"Layer 1 entry: {entry_compilation.error}"
        assert entry_compilation.action_bundle is not None
        entry_execution = await orchestrator.execute(entry_compilation.action_bundle)
        assert entry_execution.success, f"Layer 2 entry: {entry_execution.error}"

        # Entry layer 3: parse non-zero swap data and prove the event emitter.
        entry_swaps = []
        for tx_result in entry_execution.transaction_results:
            if tx_result.receipt:
                parsed = parser.parse_receipt(tx_result.receipt.to_dict())
                assert parsed.success, f"Layer 3 entry parser: {parsed.error}"
                entry_swaps.extend(parsed.swaps)
        assert entry_swaps and all(swap.amount0 != 0 and swap.amount1 != 0 for swap in entry_swaps)
        assert {swap.pool.lower() for swap in entry_swaps} == {approved_pool}

        # Entry layer 4: exact input and positive output balance deltas.
        bsc_usd_after_entry = get_token_balance(web3, bsc_usd, funded_wallet)
        xaut0_after_entry = get_token_balance(web3, xaut0, funded_wallet)
        assert bsc_usd_before - bsc_usd_after_entry == int(entry_amount * Decimal(10**bsc_usd_decimals))
        xaut0_received = xaut0_after_entry - xaut0_before
        assert xaut0_received > 0

        # Teardown layers 1-2: the generic position close resolves the live
        # wallet balance and preserves the same exact-pool pin.
        teardown_position = PositionInfo(
            position_type=PositionType.TOKEN,
            position_id=xaut0,
            chain=CHAIN_NAME,
            protocol="pancakeswap_v3",
            value_usd=entry_amount,
            details={
                "token_address": xaut0,
                "close_swap_params": {"pool": Web3.to_checksum_address(APPROVED_XAUT0_POOL)},
            },
        )
        teardown_intents = full_close_intents(
            [teardown_position],
            target_token=bsc_usd,
            max_slippage=Decimal("0.0075"),
        )
        assert len(teardown_intents) == 1
        teardown = teardown_intents[0]
        assert teardown.from_token == xaut0
        assert teardown.amount == "all"
        assert teardown.protocol == "pancakeswap_v3"
        assert teardown.swap_params == {"pool": Web3.to_checksum_address(APPROVED_XAUT0_POOL)}
        teardown_amount = Decimal(xaut0_received) / Decimal(10**xaut0_decimals)
        resolved_teardown = Intent.set_resolved_amount(teardown, teardown_amount)
        assert resolved_teardown.swap_params == teardown.swap_params
        teardown_compilation = compiler.compile(resolved_teardown)
        assert teardown_compilation.status.value == "SUCCESS", f"Layer 1 teardown: {teardown_compilation.error}"
        assert teardown_compilation.action_bundle is not None
        teardown_execution = await orchestrator.execute(teardown_compilation.action_bundle)
        assert teardown_execution.success, f"Layer 2 teardown: {teardown_execution.error}"

        # Teardown layer 3: the reverse swap event also comes only from the approved pool.
        teardown_swaps = []
        for tx_result in teardown_execution.transaction_results:
            if tx_result.receipt:
                parsed = parser.parse_receipt(tx_result.receipt.to_dict())
                assert parsed.success, f"Layer 3 teardown parser: {parsed.error}"
                teardown_swaps.extend(parsed.swaps)
        assert teardown_swaps and all(swap.amount0 != 0 and swap.amount1 != 0 for swap in teardown_swaps)
        assert {swap.pool.lower() for swap in teardown_swaps} == {approved_pool}

        # Teardown layer 4: exact XAUT0 input and positive BSC-USD output deltas.
        xaut0_after_teardown = get_token_balance(web3, xaut0, funded_wallet)
        bsc_usd_after_teardown = get_token_balance(web3, bsc_usd, funded_wallet)
        assert xaut0_after_entry - xaut0_after_teardown == xaut0_received
        assert bsc_usd_after_teardown - bsc_usd_after_entry > 0

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdt_to_wbnb_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDT -> WBNB swap using SwapIntent with PancakeSwap V3.

        Flow:
        1. Create SwapIntent for USDT -> WBNB
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Parse receipts with PancakeSwapV3ReceiptParser
        5. Verify balances changed correctly
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDT"]
        token_out = tokens["WBNB"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "pancakeswap_v3", token_in, token_out, 500)

        # Get decimals
        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        # Amount to swap
        swap_amount = Decimal("100")  # 100 USDT

        print(f"\n{'=' * 80}")
        print("Test: USDT -> WBNB Swap via SwapIntent (PancakeSwap V3)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDT")

        # Record balances before
        usdt_before = get_token_balance(web3, token_in, funded_wallet)
        wbnb_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDT before: {format_token_amount(usdt_before, in_decimals)}")
        print(f"WBNB before: {format_token_amount(wbnb_before, out_decimals)}")

        # Create SwapIntent
        # Note: Higher slippage needed because CoinGecko prices may differ from on-chain pool prices
        intent = SwapIntent(
            from_token="USDT",
            to_token="WBNB",
            amount=swap_amount,
            max_slippage=Decimal("0.20"),  # 20% slippage for testing
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Compile intent with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=orchestrator.rpc_url,
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

        # Parse receipts with PancakeSwapV3ReceiptParser
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            # Parse swap receipt
            if tx_result.receipt:
                from almanak.connectors.pancakeswap_v3.receipt_parser import PancakeSwapV3ReceiptParser

                parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.swaps:
                    for swap_data in parse_result.swaps:
                        print(f"  Amount0:    {swap_data.amount0}")
                        print(f"  Amount1:    {swap_data.amount1}")
                        print(f"  Pool:       {swap_data.pool[:16]}...")
                        print(f"  Recipient:  {swap_data.recipient[:16]}...")

                        # Verify swap amounts are non-zero
                        assert swap_data.amount0 != 0, "Amount0 must be non-zero"
                        assert swap_data.amount1 != 0, "Amount1 must be non-zero"

        # Verify balance changes
        usdt_after = get_token_balance(web3, token_in, funded_wallet)
        wbnb_after = get_token_balance(web3, token_out, funded_wallet)

        usdt_spent = usdt_before - usdt_after
        wbnb_received = wbnb_after - wbnb_before

        print("\n--- Results ---")
        print(f"USDT spent:    {format_token_amount(usdt_spent, in_decimals)}")
        print(f"WBNB received: {format_token_amount(wbnb_received, out_decimals)}")

        # Verify USDT was spent (exact match)
        expected_usdt_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdt_spent == expected_usdt_spent, (
            f"USDT spent must EXACTLY equal swap amount. Expected: {expected_usdt_spent}, Got: {usdt_spent}"
        )

        # Verify WBNB was received
        assert wbnb_received > 0, "Must receive positive WBNB"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_wbnb_to_usdt_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WBNB -> USDT swap using SwapIntent with PancakeSwap V3 (reverse direction)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WBNB"]
        token_out = tokens["USDT"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "pancakeswap_v3", token_in, token_out, 500)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("0.1")  # 0.1 WBNB

        print(f"\n{'=' * 80}")
        print("Test: WBNB -> USDT Swap via SwapIntent (PancakeSwap V3)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} WBNB")

        wbnb_before = get_token_balance(web3, token_in, funded_wallet)
        usdt_before = get_token_balance(web3, token_out, funded_wallet)

        # Create intent
        # Note: Higher slippage needed because CoinGecko prices may differ from on-chain pool prices
        intent = SwapIntent(
            from_token="WBNB",
            to_token="USDT",
            amount=swap_amount,
            max_slippage=Decimal("0.20"),  # 20% slippage for testing
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
        )

        # Compile with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=orchestrator.rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        # Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success

        # Parse receipts
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                from almanak.connectors.pancakeswap_v3.receipt_parser import PancakeSwapV3ReceiptParser

                parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.swaps:
                    for swap_data in parse_result.swaps:
                        assert swap_data.amount0 != 0, "Amount0 must be non-zero"
                        assert swap_data.amount1 != 0, "Amount1 must be non-zero"

        # Verify balance changes
        wbnb_after = get_token_balance(web3, token_in, funded_wallet)
        usdt_after = get_token_balance(web3, token_out, funded_wallet)

        wbnb_spent = wbnb_before - wbnb_after
        usdt_received = usdt_after - usdt_before

        expected_wbnb_spent = int(swap_amount * Decimal(10**in_decimals))
        assert wbnb_spent == expected_wbnb_spent, (
            f"WBNB spent must EXACTLY equal swap amount. Expected: {expected_wbnb_spent}, Got: {wbnb_spent}"
        )
        assert usdt_received > 0, "Must receive positive USDT"

        print(f"WBNB spent:    {format_token_amount(wbnb_spent, in_decimals)}")
        print(f"USDT received: {format_token_amount(usdt_received, out_decimals)}")
        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_intent_with_insufficient_balance_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test that SwapIntent with insufficient balance fails gracefully."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDT"]
        token_out = tokens["WBNB"]

        # Get current balance
        usdt_balance = get_token_balance(web3, token_in, funded_wallet)
        wbnb_before = get_token_balance(web3, token_out, funded_wallet)
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdt_balance) / Decimal(10**in_decimals)

        # Exceed balance by 2x so execution fails on-chain with insufficient balance,
        # but stay inside the compiler's price-impact guard (default 30%) so this path
        # exercises execution-level failure rather than compile-time rejection.
        # See bnb test_uniswap_swap.py for the sibling pattern (issue #2150).
        excessive_amount = balance_decimal * Decimal("2")

        print(f"\n{'=' * 80}")
        print("Test: SwapIntent with Insufficient Balance (PancakeSwap V3)")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDT")
        print(f"Trying:    {excessive_amount} USDT")

        intent = SwapIntent(
            from_token="USDT",
            to_token="WBNB",
            amount=excessive_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)

        # Accept BOTH outcomes as valid insufficient-balance signals, but
        # narrowly — the failure message in EITHER branch must point to the
        # insufficient-balance / price-impact family. Anything else (router
        # errors, slippage misconfig, liquidity gaps) would be masked by a
        # permissive "any failure wins" check, hiding unrelated regressions
        # in what is meant to be a balance-guard test.
        #
        #   1) Compilation SUCCESS -> execution must fail with an
        #      insufficient-balance error (balance check trips at execute time).
        #   2) Compilation FAILED -> compiler error must contain a price-impact
        #      or insufficient-balance phrase (the compiler guard trips first
        #      because an excessive amount also tips the price-impact guard).
        # Narrow phrases: bare "balance" would also match unrelated errors like
        # "balance check failed for token X"; use explicit failure shapes from
        # the insufficient-balance / price-impact family.
        _expected_phrases = (
            "insufficient balance",
            "insufficient funds",
            "transfer amount exceeds balance",
            "price impact",
            # Zodiac wraps inner reverts in ``ModuleTransactionFailed()``;
            # the inner reason isn't recoverable from the eth_call replay.
            # The bilateral conservation check below is the load-bearing
            # signal under Zodiac. EOA-mode error messages don't contain
            # ``execTransactionWithRole``, so this stays strict for EOA.
            "exectransactionwithrole",
        )
        if compilation_result.status.value == "SUCCESS":
            assert compilation_result.action_bundle is not None
            execution_result = await orchestrator.execute(compilation_result.action_bundle)
            assert not execution_result.success, "Execution should fail with insufficient balance"
            exec_err = (execution_result.error or "").lower()
            assert any(p in exec_err for p in _expected_phrases), (
                f"Execution failed but not with an expected insufficient-balance signal: {execution_result.error!r}"
            )
            print(f"Execution failed as expected: {execution_result.error}")
        else:
            err = (compilation_result.error or "").lower()
            assert any(p in err for p in _expected_phrases), (
                f"Compilation failed but not with an expected insufficient-balance signal: {compilation_result.error!r}"
            )
            print(f"Compilation failed as expected: {compilation_result.error}")

        # Verify balances unchanged (bilateral conservation check)
        usdt_after = get_token_balance(web3, token_in, funded_wallet)
        wbnb_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdt_after == usdt_balance, "Input token balance must be unchanged after failed swap"
        assert wbnb_after == wbnb_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
