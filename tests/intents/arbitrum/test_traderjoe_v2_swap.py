"""TraderJoe V2 Liquidity Book swap intent tests on Arbitrum (VIB-4374).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
TraderJoe V2 swaps on Arbitrum using the LBRouter v2.1 interface.

Background:
    TraderJoe deployed LBRouter v2.1 on Arbitrum at the same CREATE2 address
    used on Avalanche/BSC (``0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30``).
    The dedicated ``_compile_swap_traderjoe_v2`` compilation path (VIB-1928)
    is chain-agnostic so long as the chain appears in
    ``TRADERJOE_V2`` in ``almanak/core/contracts.py``. This test exercises
    that path against Arbitrum's WETH/USDC pair.

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/arbitrum/test_traderjoe_v2_swap.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.traderjoe_v2.receipt_parser import TraderJoeV2ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"

# Token addresses on Arbitrum (mirrors CHAIN_CONFIGS and TRADERJOE_V2_TOKENS).
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


# =============================================================================
# Layer 1: Compilation Tests (No Anvil Required)
# =============================================================================


class TestTraderJoeV2SwapCompilation:
    """Layer 1: Verify SwapIntent compiles correctly for TraderJoe V2 on Arbitrum."""

    def _make_compiler(self) -> IntentCompiler:
        return IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    @pytest.mark.intent(IntentType.SWAP)
    def test_vib_1406_guard_removed(self):
        """SwapIntent(protocol='traderjoe_v2') must NOT return VIB-1406 block error on arbitrum."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=Decimal("0.01"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        # VIB-1928: must NOT fail with VIB-1406 guard error
        if result.status == CompilationStatus.FAILED:
            assert "VIB-1406" not in (result.error or ""), (
                "TraderJoe V2 swap still blocked by VIB-1406 guard!"
            )
            assert "not yet supported" not in (result.error or ""), (
                "TraderJoe V2 swap still returns 'not yet supported' error!"
            )
        # If compilation succeeds (with placeholder prices + local RPC), verify bundle
        if result.status == CompilationStatus.SUCCESS:
            assert result.action_bundle is not None
            assert result.action_bundle.metadata["protocol"] == "traderjoe_v2"


# =============================================================================
# Layers 2-4: Full On-Chain Swap Tests (Requires Arbitrum Anvil Fork)
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.swap
class TestTraderJoeV2SwapExecution:
    """Layers 2-4: Full on-chain TJ V2 swap tests on Arbitrum Anvil fork.

    Tests WETH <-> USDC swaps via LBRouter v2.1 with:
    - Layer 2: Transaction execution on Anvil
    - Layer 3: Transfer event parsing via TraderJoeV2ReceiptParser
    - Layer 4: Exact bilateral balance delta verification
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_weth_to_usdc_full_lifecycle(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Layer 2-4: Execute WETH -> USDC swap via TraderJoe V2 LBRouter v2.1.

        Verifies:
        - Compilation succeeds with auto-detected bin_step (Layer 1)
        - Execution succeeds on Anvil (Layer 2)
        - Receipt parser extracts swap amounts (Layer 3)
        - WETH balance decreased exactly, USDC balance increased (Layer 4)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]

        swap_amount = Decimal("0.01")  # 0.01 WETH (~$30) — small to stay within bin liquidity

        logger.info("Test: WETH -> USDC TraderJoe V2 swap on Arbitrum")

        # --- Layer 4 setup: record balances BEFORE ---
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        logger.info("WETH before: %.4f", weth_before / 10**18)
        logger.info("USDC before: %.2f", usdc_before / 10**6)
        assert weth_before > 0, "Test wallet has no WETH -- funding failed"

        # --- Layer 1: Compile ---
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=Decimal("0.03"),  # 3% slippage for DEX
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )

        compile_result = compiler.compile(intent)
        assert compile_result.status == CompilationStatus.SUCCESS, (
            f"TraderJoe V2 swap compilation failed: {compile_result.error}"
        )
        assert compile_result.action_bundle is not None
        assert compile_result.action_bundle.metadata.get("protocol") == "traderjoe_v2"
        logger.info(
            "Compiled %d transactions, bin_step=%s",
            len(compile_result.transactions),
            compile_result.action_bundle.metadata.get("bin_step"),
        )

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compile_result.action_bundle)
        assert execution_result.success, (
            f"TraderJoe V2 swap execution failed: {execution_result.error}"
        )
        logger.info("Execution success")

        # --- Layer 3: Parse receipt ---
        parser = TraderJoeV2ReceiptParser(chain=CHAIN_NAME)
        swap_amounts_extracted = False

        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = (
                tx_result.receipt if isinstance(tx_result.receipt, dict)
                else tx_result.receipt.to_dict()
            )

            swap_amounts = parser.extract_swap_amounts(receipt_dict)
            if swap_amounts is not None:
                swap_amounts_extracted = True
                assert swap_amounts.amount_in > 0, "SwapAmounts.amount_in must be > 0"
                assert swap_amounts.amount_out > 0, "SwapAmounts.amount_out must be > 0"
                logger.info(
                    "SwapAmounts: in=%s out=%s effective_price=%s",
                    swap_amounts.amount_in,
                    swap_amounts.amount_out,
                    swap_amounts.effective_price,
                )

        assert swap_amounts_extracted, (
            "TraderJoeV2ReceiptParser.extract_swap_amounts() returned None. "
            "Verify Transfer event parsing works for LBRouter v2.1 swaps."
        )

        # --- Layer 4: Bilateral balance deltas (no-op guard) ---
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_received = usdc_after - usdc_before
        expected_weth_spent = int(swap_amount * Decimal(10**18))

        logger.info("WETH after: %.4f (spent: %.4f)", weth_after / 10**18, weth_spent / 10**18)
        logger.info("USDC after: %.2f (received: %.2f)", usdc_after / 10**6, usdc_received / 10**6)

        assert weth_spent == expected_weth_spent, (
            f"WETH spent must EXACTLY equal swap amount. "
            f"Expected: {expected_weth_spent}, Got: {weth_spent}"
        )
        assert usdc_received > 0, (
            "USDC balance did not increase after TraderJoe V2 swap (no-op guard)!"
        )

        logger.info(
            "SUCCESS: Swapped %.4f WETH -> %.2f USDC via TraderJoe V2",
            weth_spent / 10**18,
            usdc_received / 10**6,
        )

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason=(
            "VIB-4374: USDC->WETH bin_step=15 reverts under execTransactionWithRole on the "
            "Anvil fork even though mainnet RPC quotes succeed (selector 0xd27b44a9 — "
            "swapExactTokensForTokens). Same fork-specific revert class as the avalanche "
            "USDC->WAVAX xfail; tracked separately because the underlying cause (bin "
            "liquidity simulation vs allowance race) hasn't been root-caused yet "
            "(as of 2026-05-14). strict=False because if a new fork block makes the "
            "swap succeed we want it to pass without breaking CI — the failure is "
            "fork-state-dependent, not a permanent semantic bug."
        ),
        strict=False,
    )
    async def test_usdc_to_weth_reverse_direction(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Layer 2-4: Execute USDC -> WETH reverse swap via TraderJoe V2."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]

        swap_amount = Decimal("10")  # 10 USDC

        # --- Layer 4 BEFORE ---
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        assert usdc_before > 0, "Test wallet has no USDC -- funding failed"

        # --- Layer 1: Compile ---
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=Decimal("0.03"),
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )

        compile_result = compiler.compile(intent)
        assert compile_result.status == CompilationStatus.SUCCESS, (
            f"Reverse TJ V2 swap compilation failed: {compile_result.error}"
        )

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compile_result.action_bundle)
        assert execution_result.success, (
            f"Reverse TJ V2 swap execution failed: {execution_result.error}"
        )

        # --- Layer 3: Parse receipt ---
        parser = TraderJoeV2ReceiptParser(chain=CHAIN_NAME)
        swap_amounts_extracted = False

        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = (
                tx_result.receipt if isinstance(tx_result.receipt, dict)
                else tx_result.receipt.to_dict()
            )
            swap_amounts = parser.extract_swap_amounts(receipt_dict)
            if swap_amounts is not None:
                swap_amounts_extracted = True
                assert swap_amounts.amount_in > 0
                assert swap_amounts.amount_out > 0

        assert swap_amounts_extracted, "Receipt parser must extract swap amounts for reverse direction"

        # --- Layer 4: Bilateral balance deltas (no-op guard) ---
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before
        expected_usdc_spent = int(swap_amount * Decimal(10**6))

        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        assert weth_received > 0, "WETH balance did not increase after reverse swap (no-op guard)!"

        logger.info(
            "SUCCESS: Reverse swap %.2f USDC -> %.4f WETH via TraderJoe V2",
            usdc_spent / 10**6,
            weth_received / 10**18,
        )
