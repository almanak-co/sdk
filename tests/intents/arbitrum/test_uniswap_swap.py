"""Production-grade SwapIntent tests for Uniswap V3 on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV3ReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/arbitrum/test_uniswap_swap.py -v -s
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
    SWAP_MAX_SLIPPAGE,
    format_token_amount,
    fund_erc20_token,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.intent_evidence import decode_explorer_view
from tests.intents.pool_helpers import fail_if_v3_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.swap
class TestUniswapV3SwapIntent:
    """Test Uniswap V3 swaps using SwapIntent.

    These tests verify the full Intent flow:
    - SwapIntent creation with proper parameters
    - IntentCompiler generates correct Uniswap V3 transactions
    - Transactions execute successfully on-chain
    - UniswapV3ReceiptParser correctly interprets results
    - Balance changes match expected amounts
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdc_to_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        intent_evidence,
    ):
        """Run the exact receipt-fidelity scenario through Safe + Zodiac."""
        await self._run_swap_usdc_to_weth(web3, funded_wallet, orchestrator, price_oracle, intent_evidence)

    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdc_to_weth_using_intent_eoa(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        intent_evidence,
    ):
        """Run the exact receipt-fidelity scenario through the EOA path."""
        await self._run_swap_usdc_to_weth(web3, funded_wallet, orchestrator, price_oracle, intent_evidence)

    async def _run_swap_usdc_to_weth(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        intent_evidence,
    ):
        """Test USDC -> WETH swap using SwapIntent.

        Flow:
        1. Create SwapIntent for USDC -> WETH
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify balances changed correctly

        Phase G.1 pilot: the ``uses_zodiac`` marker routes this test through
        Safe + Zodiac Roles. ``funded_wallet`` returns the Safe address;
        ``orchestrator`` is a ``ZodiacOrchestrator`` that wraps each inner tx
        into ``Roles.execTransactionWithRole``. The test body is unchanged —
        the same assertions hold because the balance deltas land on the Safe.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, 500)

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
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )
        # Bind the declared cell before compilation so an early compile/execute
        # failure remains attributable without attaching a non-swap receipt.
        intent_evidence.bind(intent)

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Compile intent with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
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

        # Parse only the transaction that emitted the protocol Swap event. A
        # Safe/Zodiac bundle can also contain approval transactions; binding
        # those receipts to the SWAP cell would manufacture receipt evidence.
        from almanak.connectors.uniswap_v3.receipt_parser import SWAP_EVENT_TOPIC, UniswapV3ReceiptParser

        swap_tx_results = []
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")
            if tx_result.receipt is None:
                continue
            receipt = tx_result.receipt.to_dict()
            if any(
                log.get("topics") and str(log["topics"][0]).lower() == SWAP_EVENT_TOPIC
                for log in receipt.get("logs", [])
            ):
                swap_tx_results.append(tx_result)

        assert len(swap_tx_results) == 1, (
            f"Expected exactly one Uniswap V3 Swap-emitting receipt in the Safe bundle, got {len(swap_tx_results)}"
        )
        swap_tx_result = swap_tx_results[0]
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        parse_result = intent_evidence.capture_parse(
            intent=intent,
            transaction_result=swap_tx_result,
            parser=lambda receipt: parser.parse_receipt(receipt),
        )
        assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
        assert parse_result.swap_result is not None, "Receipt parser must extract a swap result"
        swap_result = parse_result.swap_result
        print(f"  Amount in:  {swap_result.amount_in_decimal}")
        print(f"  Amount out: {swap_result.amount_out_decimal}")
        print(f"  Price:      {swap_result.effective_price}")

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

        # Hard L3: compare raw parser facts to independent wallet state, not
        # merely positive decimal displays. Preserve token identity and the
        # parser's explicit decimal-resolution witnesses.
        assert swap_result.amount_in == usdc_spent
        assert swap_result.amount_out == weth_received
        assert swap_result.token_in.lower() == token_in.lower()
        assert swap_result.token_out.lower() == token_out.lower()
        assert swap_result.token_in_decimals_resolved is True
        assert swap_result.token_out_decimals_resolved is True

        raw_receipt = swap_tx_result.receipt.to_dict()
        explorer_logs = decode_explorer_view(raw_receipt)["logs"]
        wallet = funded_wallet.lower()
        input_transfers = [
            log
            for log in explorer_logs
            if log.get("name") == "Transfer"
            and str(log.get("address", "")).lower() == token_in.lower()
            and str((log.get("args") or {}).get("from", "")).lower() == wallet
        ]
        output_transfers = [
            log
            for log in explorer_logs
            if log.get("name") == "Transfer"
            and str(log.get("address", "")).lower() == token_out.lower()
            and str((log.get("args") or {}).get("to", "")).lower() == wallet
        ]
        transfers_unambiguous = len(input_transfers) == 1 and len(output_transfers) == 1
        if transfers_unambiguous:
            assert int(input_transfers[0]["args"]["value"]) == usdc_spent
            assert int(output_transfers[0]["args"]["value"]) == weth_received

        flags = {
            "parse_success": parse_result.success,
            "swap_result_present": parse_result.swap_result is not None,
            "amount_in_eq_wallet_delta": swap_result.amount_in == usdc_spent,
            "amount_out_eq_wallet_delta": swap_result.amount_out == weth_received,
            "token_in_match": swap_result.token_in.lower() == token_in.lower(),
            "token_out_match": swap_result.token_out.lower() == token_out.lower(),
            "input_decimals_resolved": swap_result.token_in_decimals_resolved is True,
            "output_decimals_resolved": swap_result.token_out_decimals_resolved is True,
            "input_transfer_unambiguous": len(input_transfers) == 1,
            "output_transfer_unambiguous": len(output_transfers) == 1,
            "amount_in_eq_transfer": transfers_unambiguous
            and int(input_transfers[0]["args"]["value"]) == swap_result.amount_in,
            "amount_out_eq_transfer": transfers_unambiguous
            and int(output_transfers[0]["args"]["value"]) == swap_result.amount_out,
        }
        intent_evidence.record_fidelity(
            hard=transfers_unambiguous,
            flags=flags,
            witnesses=[
                {
                    "kind": "wallet_balance_deltas",
                    "token_in": token_in,
                    "token_out": token_out,
                    "amount_in_raw": usdc_spent,
                    "amount_out_raw": weth_received,
                },
                {
                    "kind": "independent_transfer_logs",
                    "input_matches": input_transfers,
                    "output_matches": output_transfers,
                },
            ],
            notes=[]
            if transfers_unambiguous
            else ["Transfer-log wallet-direction match was ambiguous; keep this receipt SOFT."],
        )
        intent_evidence.record_balance_deltas(
            checks={"wallet_deltas_eq_independent_transfer_logs": transfers_unambiguous},
            token_in={
                "address": token_in,
                "symbol": "USDC",
                "before": usdc_before,
                "after": usdc_after,
                "delta": -usdc_spent,
            },
            token_out={
                "address": token_out,
                "symbol": "WETH",
                "before": weth_before,
                "after": weth_after,
                "delta": weth_received,
            },
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_weth_to_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WETH -> USDC swap using SwapIntent (reverse direction).

        Phase G.1 pilot: routes through Safe + Zodiac Roles via the
        ``uses_zodiac`` marker. See ``test_swap_usdc_to_weth_using_intent``
        for details on the fixture substitution.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WETH"]
        token_out = tokens["USDC"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, 500)

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
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        # Compile with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
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
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]
        in_decimals = get_token_decimals(web3, token_in)

        # Shrink the input-token balance to a small known amount so the over-balance
        # swap stays tiny in ABSOLUTE terms — far below the compiler's price-impact
        # guard (default 30%) regardless of pool depth — while still exceeding the
        # wallet balance so execution reverts on-chain for insufficient funds.
        # Sizing the 2x amount off the full seeded balance (100k USDC) tripped the
        # guard once the weekly fork-block roll moved to a thinner-pool block,
        # flipping this into a compile-time rejection instead of the intended
        # execution-level failure.
        small_balance = Decimal("100")
        fund_erc20_token(
            funded_wallet,
            token_in,
            int(small_balance * Decimal(10**in_decimals)),
            CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["USDC"],
            anvil_rpc_url,
        )

        # Get current (now small) balance
        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)

        # Exceed the (now small) balance by 2x: ~200 USDC clears the price-impact
        # guard but is still > balance, so execution fails with insufficient funds.
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
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
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

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
