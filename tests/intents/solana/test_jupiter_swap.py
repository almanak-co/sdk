"""Jupiter swap intent tests for Solana.

Layer 1 (Compilation): Verifies the full intent compilation pipeline against
the REAL Jupiter API. Always runs — no solana-test-validator required.

Layers 2-4 (Execution + Receipt + Balance Deltas): Require solana-test-validator.
These sign, submit, parse receipts, and verify exact balance changes on-chain.

Run compilation tests:
    uv run pytest tests/intents/solana/test_jupiter_swap.py -v -s -k Compilation

Run all tests (needs solana-test-validator):
    uv run pytest tests/intents/solana/test_jupiter_swap.py -v -s
"""

import base64
from decimal import Decimal

import pytest

from almanak.framework.intents.vocabulary import SwapIntent
from tests.intents.solana.conftest import (
    CHAIN_NAME,
    SOLANA_TOKEN_DECIMALS,
    SOLANA_TOKENS,
    get_sol_balance,
    get_spl_token_balance,
    requires_solana_validator,
)


# =============================================================================
# Layer 1: Compilation Tests (always run — hit real Jupiter API)
# =============================================================================


class TestJupiterSwapCompilation:
    """Jupiter swap: Intent -> Compile -> ActionBundle (real API calls)."""

    @pytest.mark.asyncio
    async def test_compile_usdc_to_sol_swap(self, solana_compiler, solana_wallet):
        """USDC -> SOL swap compiles to a valid ActionBundle via Jupiter API."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount=Decimal("0.10"),  # 0.10 USDC (tiny amount for testing)
            max_slippage=Decimal("0.05"),  # 5% slippage
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        # Layer 1: Compilation succeeds
        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None, "ActionBundle must be created"

        bundle = result.action_bundle

        # ActionBundle has transactions
        assert bundle.transactions, "Bundle must contain transactions"
        assert len(bundle.transactions) >= 1, "At least 1 transaction expected"

        # Transaction is a valid base64-encoded Solana tx
        tx_data = bundle.transactions[0]
        serialized_tx = tx_data.get("serialized_transaction", "")
        assert serialized_tx, "Transaction must have serialized_transaction"

        # Verify it's valid base64
        decoded = base64.b64decode(serialized_tx)
        assert len(decoded) > 100, f"Decoded tx too small ({len(decoded)} bytes), likely invalid"

        # Metadata is correct
        metadata = bundle.metadata
        assert metadata.get("chain") == "solana"
        assert metadata.get("chain_family") == "SOLANA"
        assert metadata.get("protocol") == "jupiter"
        assert metadata.get("from_token") == "USDC"
        assert metadata.get("to_token") == "SOL"
        assert metadata.get("deferred_swap") is True, "Jupiter swaps must use deferred_swap"
        assert metadata.get("input_mint") == SOLANA_TOKENS["USDC"]
        assert metadata.get("output_mint") == SOLANA_TOKENS["SOL"]

        # Route params present for refresh at execution time
        route_params = metadata.get("route_params")
        assert route_params, "route_params required for deferred swap"
        assert route_params["input_mint"] == SOLANA_TOKENS["USDC"]
        assert route_params["output_mint"] == SOLANA_TOKENS["SOL"]
        assert route_params["amount"] > 0
        assert route_params["slippage_bps"] == 500  # 5% = 500 bps

    @pytest.mark.asyncio
    async def test_compile_sol_to_usdc_swap(self, solana_compiler, solana_wallet):
        """SOL -> USDC swap (reverse direction) compiles successfully."""
        intent = SwapIntent(
            from_token="SOL",
            to_token="USDC",
            amount=Decimal("0.001"),  # 0.001 SOL
            max_slippage=Decimal("0.05"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None

        bundle = result.action_bundle
        assert bundle.transactions
        assert bundle.metadata.get("protocol") == "jupiter"
        assert bundle.metadata.get("input_mint") == SOLANA_TOKENS["SOL"]
        assert bundle.metadata.get("output_mint") == SOLANA_TOKENS["USDC"]

    @pytest.mark.asyncio
    async def test_compile_usdc_to_usdt_swap(self, solana_compiler):
        """USDC -> USDT stablecoin swap compiles successfully."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount=Decimal("1.0"),
            max_slippage=Decimal("0.01"),  # 1% tight slippage for stables
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None
        assert result.action_bundle.metadata.get("input_mint") == SOLANA_TOKENS["USDC"]
        assert result.action_bundle.metadata.get("output_mint") == SOLANA_TOKENS["USDT"]

    @pytest.mark.asyncio
    async def test_compile_amount_usd_swap(self, solana_compiler):
        """SwapIntent with amount_usd resolves to token amount correctly."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount_usd=Decimal("0.50"),  # $0.50 worth
            max_slippage=Decimal("0.05"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None
        # amount_in should be roughly 500000 (0.50 USDC in 6-decimal units)
        amount_in = int(result.action_bundle.metadata.get("amount_in", "0"))
        assert 400_000 <= amount_in <= 600_000, (
            f"Expected ~500000 lamports (0.50 USDC), got {amount_in}"
        )

    @pytest.mark.asyncio
    async def test_compile_swap_has_valid_base64_transaction(self, solana_compiler):
        """The serialized transaction decodes to a valid Solana VersionedTransaction."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount=Decimal("0.10"),
            max_slippage=Decimal("0.05"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)
        assert result.status.value == "SUCCESS"

        tx_data = result.action_bundle.transactions[0]
        serialized_tx = tx_data["serialized_transaction"]

        # Decode and verify it's a valid Solana transaction
        decoded = base64.b64decode(serialized_tx)

        assert len(decoded) > 200, (
            f"Transaction too small ({len(decoded)} bytes), expected a full Jupiter swap tx"
        )

        # Verify we can deserialize with solders
        from solders.transaction import VersionedTransaction

        tx = VersionedTransaction.from_bytes(decoded)
        assert tx is not None, "Must deserialize as a valid VersionedTransaction"

    @pytest.mark.asyncio
    async def test_compile_swap_intent_type(self, solana_compiler):
        """ActionBundle has correct intent_type."""
        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount=Decimal("0.10"),
            max_slippage=Decimal("0.05"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)
        assert result.status.value == "SUCCESS"
        assert result.action_bundle.intent_type == "SWAP"


# =============================================================================
# Layers 2-4: Execution Tests (require solana-test-validator)
# =============================================================================


@requires_solana_validator
class TestJupiterSwapExecution:
    """Jupiter swap: full 4-layer verification on local test-validator.

    Layer 1: Compilation success (Intent -> ActionBundle)
    Layer 2: Execution success (ActionBundle -> on-chain tx)
    Layer 3: Receipt parser integration (parse swap amounts)
    Layer 4: Exact balance deltas (before/after verification)
    """

    @pytest.mark.asyncio
    async def test_swap_usdc_to_sol(
        self, solana_fork, funded_solana_wallet, solana_orchestrator, execution_compiler,
    ):
        """USDC -> SOL swap: full Intent -> Execute -> Verify pipeline."""
        wallet_address, _ = funded_solana_wallet
        swap_amount = Decimal("10")  # 10 USDC

        # Layer 4 setup: Record balances BEFORE
        usdc_before = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )
        sol_before = await get_sol_balance(solana_fork, wallet_address)

        # Layer 1: Compile
        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount=swap_amount,
            max_slippage=Decimal("0.05"),
            chain=CHAIN_NAME,
        )
        compilation_result = execution_compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Layer 2: Execute
        execution_result = await solana_orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parser integration
        from almanak.framework.connectors.jupiter import JupiterReceiptParser

        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parser = JupiterReceiptParser(
                    wallet_address=wallet_address, chain=CHAIN_NAME,
                )
                swap_amounts = parser.extract_swap_amounts(receipt_dict)
                if swap_amounts:
                    assert swap_amounts.amount_in_decimal > 0, "Must have positive input amount"
                    assert swap_amounts.amount_out_decimal > 0, "Must have positive output amount"
                    assert swap_amounts.effective_price > 0, "Must have positive effective price"

        # Layer 4: Exact balance deltas
        usdc_after = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )
        sol_after = await get_sol_balance(solana_fork, wallet_address)

        usdc_spent = usdc_before - usdc_after
        sol_received = sol_after - sol_before  # May be net of gas fees

        expected_usdc_spent = int(swap_amount * Decimal(10 ** SOLANA_TOKEN_DECIMALS["USDC"]))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        # SOL received should be positive (even after gas deduction)
        assert sol_received > 0, "Must receive positive SOL from swap"

    @pytest.mark.asyncio
    async def test_swap_sol_to_usdc(
        self, solana_fork, funded_solana_wallet, solana_orchestrator, execution_compiler,
    ):
        """SOL -> USDC swap (reverse direction): full 4-layer verification."""
        wallet_address, _ = funded_solana_wallet
        swap_amount = Decimal("0.1")  # 0.1 SOL

        # Layer 4 setup: Record balances BEFORE
        sol_before = await get_sol_balance(solana_fork, wallet_address)
        usdc_before = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )

        # Layer 1: Compile
        intent = SwapIntent(
            from_token="SOL",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=Decimal("0.05"),
            chain=CHAIN_NAME,
        )
        compilation_result = execution_compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Layer 2: Execute
        execution_result = await solana_orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parser
        from almanak.framework.connectors.jupiter import JupiterReceiptParser

        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parser = JupiterReceiptParser(
                    wallet_address=wallet_address, chain=CHAIN_NAME,
                )
                swap_amounts = parser.extract_swap_amounts(receipt_dict)
                if swap_amounts:
                    assert swap_amounts.amount_in_decimal > 0
                    assert swap_amounts.amount_out_decimal > 0
                    assert swap_amounts.effective_price > 0

        # Layer 4: Exact balance deltas
        sol_after = await get_sol_balance(solana_fork, wallet_address)
        usdc_after = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )

        usdc_received = usdc_after - usdc_before
        assert usdc_received > 0, "Must receive positive USDC from swap"

        # SOL spent = swap amount + gas. At minimum, the swap amount was spent.
        sol_spent = sol_before - sol_after
        expected_sol_spent_min = int(swap_amount * Decimal(10 ** SOLANA_TOKEN_DECIMALS["SOL"]))
        assert sol_spent >= expected_sol_spent_min, (
            f"SOL spent ({sol_spent}) must be >= swap amount ({expected_sol_spent_min}) + gas"
        )

    @pytest.mark.asyncio
    async def test_swap_insufficient_balance_fails(
        self, solana_fork, funded_solana_wallet, solana_orchestrator, execution_compiler,
    ):
        """SwapIntent with 100x balance should fail. Balances must be conserved."""
        wallet_address, _ = funded_solana_wallet

        # Record balances BEFORE
        usdc_before = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )

        # Create intent with excessive amount (100x funded balance)
        excessive_amount = Decimal("1000000")  # 1M USDC — far more than funded
        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount=excessive_amount,
            max_slippage=Decimal("0.05"),
            chain=CHAIN_NAME,
        )

        compilation_result = execution_compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            "Compilation should succeed (insufficient balance is a runtime error)"
        )

        execution_result = await solana_orchestrator.execute(compilation_result.action_bundle)
        assert not execution_result.success, "Execution should fail with insufficient balance"

        # Conservation check — MANDATORY
        usdc_after = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )
        assert usdc_after == usdc_before, (
            f"USDC balance must be unchanged after failed swap. "
            f"Before: {usdc_before}, After: {usdc_after}"
        )
