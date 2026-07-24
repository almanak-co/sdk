"""TraderJoe V2 Liquidity Book swap intent tests on BNB Chain (VIB-4376).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
TraderJoe V2 swaps on BNB Chain using the LBRouter v2.1 interface.

Background:
    TraderJoe deployed LBRouter v2.1 on BNB Chain at the same CREATE2 address
    used on Avalanche/Arbitrum (``0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30``).
    The dedicated ``_compile_swap_traderjoe_v2`` compilation path (VIB-1928)
    is chain-agnostic so long as the chain appears in ``TRADERJOE_V2`` in
    ``almanak/core/contracts.py``. This test exercises that path against
    BNB Chain's WBNB/USDT pair.

bsc/bnb alias note: the IntentCompiler / connectors key off ``chain="bsc"``
(the SDK canonical name). The directory is ``tests/intents/bnb/`` to match
the framework's user-facing chain alias ``bnb``. The conftest in this
directory pins ``CHAIN_NAME = "bsc"``.

BSC-specific decimals:
    BSC USDT and USDC are Binance-Peg tokens that use **18 decimals**, not 6.
    WBNB also uses 18 decimals. All token math in this file therefore uses
    ``10**18`` instead of the 6-decimal stablecoin convention used on
    Ethereum / Arbitrum / Avalanche.

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/bnb/test_traderjoe_v2_swap.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.traderjoe_v2.receipt_parser import TraderJoeV2ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "bsc"

# Token addresses on BNB Chain (mirrors CHAIN_CONFIGS["bsc"] and TRADERJOE_V2_TOKENS["bsc"]).
WBNB_ADDRESS = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
USDT_ADDRESS = "0x55d398326f99059fF775485246999027B3197955"

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


# =============================================================================
# Layer 1: Compilation Tests (No Anvil Required)
# =============================================================================


class TestTraderJoeV2SwapCompilation:
    """Layer 1: Verify SwapIntent compiles correctly for TraderJoe V2 on BNB Chain."""

    def _make_compiler(self) -> IntentCompiler:
        return IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    @pytest.mark.intent(IntentType.SWAP)
    def test_vib_1406_guard_removed(self):
        """SwapIntent(protocol='traderjoe_v2') must NOT return VIB-1406 block error on bsc."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="WBNB",
            to_token="USDT",
            amount=Decimal("0.01"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        # Layer 1 must unconditionally assert SUCCESS (per intent-tests.md);
        # the historical "if FAILED / if SUCCESS" pattern silently masked any
        # new compile regression that wasn't the VIB-1406 substring. Surface
        # any other error explicitly via the assertion message instead.
        assert result.status == CompilationStatus.SUCCESS, (
            f"TraderJoe V2 swap compilation failed: {result.error}"
        )
        assert result.action_bundle is not None, "ActionBundle must be created"
        assert result.action_bundle.metadata["protocol"] == "traderjoe_v2"

        # VIB-1928 regression guards: even on SUCCESS the error string should
        # not contain the pre-VIB-1928 block markers (defensive — kept as
        # additional diagnostics so a partial regression in error formatting
        # still fails the test loudly).
        assert "VIB-1406" not in (result.error or ""), (
            "TraderJoe V2 swap still blocked by VIB-1406 guard!"
        )
        assert "not yet supported" not in (result.error or ""), (
            "TraderJoe V2 swap still returns 'not yet supported' error!"
        )


# =============================================================================
# Layers 2-4: Full On-Chain Swap Tests (Requires BNB Chain Anvil Fork)
# =============================================================================


@pytest.mark.bsc
@pytest.mark.swap
class TestTraderJoeV2SwapExecution:
    """Layers 2-4: Full on-chain TJ V2 swap tests on BNB Chain Anvil fork.

    Tests WBNB <-> USDT swaps via LBRouter v2.1 with:
    - Layer 2: Transaction execution on Anvil
    - Layer 3: Transfer event parsing via TraderJoeV2ReceiptParser
    - Layer 4: Exact bilateral balance delta verification
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason=(
            "VIB-5973: WBNB->USDT bin_step=15 LBPair has thin / one-sided "
            "reserves on the BSC TJv2 deployment (~467 USDT / 0 WBNB across "
            "all bin steps at the current fork latest as of 2026-05-14). The "
            "LBRouter's swapExactTokensForTokens reverts inside "
            "execTransactionWithRole with ModuleTransactionFailed (0xd27b44a9) "
            "when the active bin lacks output-side liquidity. Same fork-state-"
            "dependent class as the avalanche WAVAX->USDC and arbitrum USDC->"
            "WETH xfails — kept strict=False because a future fork block with "
            "fresh LP activity should let the test pass on its own without a "
            "CI break. Re-pointed to VIB-5973 2026-07-24."
        ),
        strict=False,
    )
    async def test_wbnb_to_usdt_full_lifecycle(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Layer 2-4: Execute WBNB -> USDT swap via TraderJoe V2 LBRouter v2.1.

        Verifies:
        - Compilation succeeds with auto-detected bin_step (Layer 1)
        - Execution succeeds on Anvil (Layer 2)
        - Receipt parser extracts swap amounts (Layer 3)
        - WBNB balance decreased exactly, USDT balance increased (Layer 4)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wbnb_addr = tokens["WBNB"]
        usdt_addr = tokens["USDT"]

        swap_amount = Decimal("0.05")  # 0.05 WBNB (~$30) — small to stay within bin liquidity

        logger.info("Test: WBNB -> USDT TraderJoe V2 swap on BNB Chain")

        # --- Layer 4 setup: record balances BEFORE ---
        wbnb_before = get_token_balance(web3, wbnb_addr, funded_wallet)
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        logger.info("WBNB before: %.4f", wbnb_before / 10**18)
        logger.info("USDT before: %.2f", usdt_before / 10**18)  # BSC USDT is 18-decimal (Binance-Peg)
        assert wbnb_before > 0, "Test wallet has no WBNB -- funding failed"

        # --- Layer 1: Compile ---
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        intent = SwapIntent(
            from_token="WBNB",
            to_token="USDT",
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

        # --- Layer 3: Parse receipt via parse_receipt() (per intent-tests.md) ---
        parser = TraderJoeV2ReceiptParser(chain=CHAIN_NAME)
        swap_parsed = False

        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = (
                tx_result.receipt if isinstance(tx_result.receipt, dict)
                else tx_result.receipt.to_dict()
            )

            parse_result = parser.parse_receipt(receipt_dict)
            if parse_result.success and parse_result.swap_result and parse_result.swap_result.success:
                swap_parsed = True
                assert parse_result.swap_result.amount_in_decimal > 0, (
                    "ParsedSwapResult.amount_in_decimal must be > 0"
                )
                assert parse_result.swap_result.amount_out_decimal > 0, (
                    "ParsedSwapResult.amount_out_decimal must be > 0"
                )
                assert parse_result.swap_result.effective_price > 0, (
                    "ParsedSwapResult.effective_price must be > 0"
                )
                logger.info(
                    "ParsedSwapResult: in=%s out=%s effective_price=%s",
                    parse_result.swap_result.amount_in_decimal,
                    parse_result.swap_result.amount_out_decimal,
                    parse_result.swap_result.effective_price,
                )

        assert swap_parsed, (
            "TraderJoeV2ReceiptParser.parse_receipt() did not return a successful "
            "ParsedSwapResult. Verify Transfer event parsing works for LBRouter v2.1 swaps."
        )

        # --- Layer 4: Bilateral balance deltas (no-op guard) ---
        wbnb_after = get_token_balance(web3, wbnb_addr, funded_wallet)
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)

        wbnb_spent = wbnb_before - wbnb_after
        usdt_received = usdt_after - usdt_before
        expected_wbnb_spent = int(swap_amount * Decimal(10**18))

        logger.info("WBNB after: %.4f (spent: %.4f)", wbnb_after / 10**18, wbnb_spent / 10**18)
        logger.info("USDT after: %.2f (received: %.2f)", usdt_after / 10**18, usdt_received / 10**18)

        assert wbnb_spent == expected_wbnb_spent, (
            f"WBNB spent must EXACTLY equal swap amount. "
            f"Expected: {expected_wbnb_spent}, Got: {wbnb_spent}"
        )
        assert usdt_received > 0, (
            "USDT balance did not increase after TraderJoe V2 swap (no-op guard)!"
        )

        logger.info(
            "SUCCESS: Swapped %.4f WBNB -> %.2f USDT via TraderJoe V2",
            wbnb_spent / 10**18,
            usdt_received / 10**18,
        )

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason=(
            "VIB-5973: USDT->WBNB reverse direction reverts on the BSC TJv2 "
            "fork for the same root cause as the forward direction — the "
            "WBNB/USDT LBPairs on BSC have thin one-sided reserves (all "
            "non-zero side is USDT, the WBNB side is empty across every bin "
            "step at the current fork latest as of 2026-05-14). The router "
            "wraps the inner revert as ModuleTransactionFailed (0xd27b44a9). "
            "Mirrors the avalanche WAVAX-side xfails — kept strict=False "
            "because a future fork block with rebalanced LP should let the "
            "test pass without a CI break. Re-pointed to VIB-5973 2026-07-24."
        ),
        strict=False,
    )
    async def test_usdt_to_wbnb_reverse_direction(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Layer 2-4: Execute USDT -> WBNB reverse swap via TraderJoe V2."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wbnb_addr = tokens["WBNB"]
        usdt_addr = tokens["USDT"]

        swap_amount = Decimal("10")  # 10 USDT (18-decimal Binance-Peg)

        # --- Layer 4 BEFORE ---
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_before = get_token_balance(web3, wbnb_addr, funded_wallet)
        assert usdt_before > 0, "Test wallet has no USDT -- funding failed"

        # --- Layer 1: Compile ---
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        intent = SwapIntent(
            from_token="USDT",
            to_token="WBNB",
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

        # --- Layer 3: Parse receipt via parse_receipt() (per intent-tests.md) ---
        parser = TraderJoeV2ReceiptParser(chain=CHAIN_NAME)
        swap_parsed = False

        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = (
                tx_result.receipt if isinstance(tx_result.receipt, dict)
                else tx_result.receipt.to_dict()
            )
            parse_result = parser.parse_receipt(receipt_dict)
            if parse_result.success and parse_result.swap_result and parse_result.swap_result.success:
                swap_parsed = True
                assert parse_result.swap_result.amount_in_decimal > 0
                assert parse_result.swap_result.amount_out_decimal > 0
                assert parse_result.swap_result.effective_price > 0

        assert swap_parsed, (
            "TraderJoeV2ReceiptParser.parse_receipt() did not return a successful "
            "ParsedSwapResult for the reverse direction."
        )

        # --- Layer 4: Bilateral balance deltas (no-op guard) ---
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_after = get_token_balance(web3, wbnb_addr, funded_wallet)

        usdt_spent = usdt_before - usdt_after
        wbnb_received = wbnb_after - wbnb_before
        expected_usdt_spent = int(swap_amount * Decimal(10**18))

        assert usdt_spent == expected_usdt_spent, (
            f"USDT spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdt_spent}, Got: {usdt_spent}"
        )
        assert wbnb_received > 0, "WBNB balance did not increase after reverse swap (no-op guard)!"

        logger.info(
            "SUCCESS: Reverse swap %.2f USDT -> %.4f WBNB via TraderJoe V2",
            usdt_spent / 10**18,
            wbnb_received / 10**18,
        )
