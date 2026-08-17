"""Curve SwapIntent tests for Arbitrum (VIB-4307).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
Curve Finance swaps on Arbitrum via tricrypto pool.

Pool: Curve tricrypto (USDT/WBTC/WETH) on Arbitrum
Address: 0x960ea3e3C7FB317332d990873d354E18d7645590
Coin order: USDT (index 0), WBTC (index 1), WETH (index 2)
Pool type: tricrypto (CryptoSwap-style, uint256 indices, TokenExchangeCrypto event)

The USDT/WETH pair is used because both tokens are funded by the standard
arbitrum conftest, and the pair has deep liquidity (~$30M+ TVL).

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/arbitrum/test_curve_swap.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    SWAP_MAX_SLIPPAGE,
    get_token_balance,
)

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"
# Curve tricrypto pool on Arbitrum
EXPECTED_POOL_ADDRESS = "0x960ea3e3C7FB317332d990873d354E18d7645590"

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


# =============================================================================
# Layer 1: SwapIntent Compilation Tests (No Anvil Required)
# =============================================================================


class TestCurveArbitrumSwapCompilation:
    """Layer 1: Verify SwapIntent compiles correctly using tricrypto pool."""

    def _make_compiler(self) -> IntentCompiler:
        return IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    @pytest.mark.intent(IntentType.SWAP)
    def test_usdt_to_weth_swap_compiles(self):
        """SwapIntent USDT -> WETH on Arbitrum tricrypto must compile successfully."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDT",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, f"USDT -> WETH Curve swap compilation failed: {result.error}"
        assert result.action_bundle is not None

    @pytest.mark.intent(IntentType.SWAP)
    def test_weth_to_usdt_swap_compiles(self):
        """SwapIntent WETH -> USDT (reverse direction) must compile."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="WETH",
            to_token="USDT",
            amount=Decimal("0.05"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, f"WETH -> USDT Curve swap compilation failed: {result.error}"
        assert result.action_bundle is not None

    @pytest.mark.intent(IntentType.SWAP)
    def test_compiled_swap_targets_tricrypto_pool(self):
        """Compiled transactions must target the tricrypto pool address."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDT",
            to_token="WETH",
            amount=Decimal("50"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS

        swap_txs = [tx for tx in result.transactions if tx.to.lower() == EXPECTED_POOL_ADDRESS.lower()]
        assert len(swap_txs) > 0, (
            f"No transaction targeting tricrypto pool {EXPECTED_POOL_ADDRESS}. "
            f"Transactions: {[(tx.to, tx.description) for tx in result.transactions]}"
        )


# =============================================================================
# Layers 2-4: Full On-Chain Swap Tests (Requires Arbitrum Anvil Fork)
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.swap
class TestCurveArbitrumSwapExecution:
    """Layers 2-4: Full on-chain Curve swap tests on Arbitrum Anvil fork.

    Tests USDT -> WETH swap via tricrypto with:
    - Layer 2: Transaction execution on Anvil
    - Layer 3: TokenExchangeCrypto event parsing
    - Layer 4: Exact balance delta verification
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_usdt_to_weth_full_lifecycle(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Layer 2-4: Execute USDT -> WETH Curve swap on Arbitrum Anvil.

        Verifies:
        - Compilation succeeds with real prices (Layer 1)
        - Execution succeeds on Anvil (Layer 2)
        - TokenExchangeCrypto event parsed (Layer 3)
        - USDT balance decreased exactly, WETH balance increased (Layer 4)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]
        weth_addr = tokens["WETH"]

        swap_amount = Decimal("100")  # 100 USDT

        logger.info(
            "Test: USDT -> WETH Curve swap on Arbitrum (tricrypto)\nPool: %s",
            EXPECTED_POOL_ADDRESS,
        )

        # --- Layer 4 setup: record balances BEFORE ---
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        logger.info("USDT before: %.2f", usdt_before / 10**6)
        logger.info("WETH before: %.6f", weth_before / 10**18)
        assert usdt_before > 0, "Test wallet has no USDT — funding failed"

        # --- Layer 1: Compile ---
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        # Exercise the address-first USD conversion path added by ALM-3190.
        # Multiplying by the same measured price used by the compiler keeps the
        # expected on-chain input exactly 100 USDT.
        intent = SwapIntent(
            from_token=usdt_addr,
            to_token=weth_addr,
            amount_usd=swap_amount * price_oracle["USDT"],
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="curve",
            chain=CHAIN_NAME,
        )

        compile_result = compiler.compile(intent)
        assert compile_result.status.value == "SUCCESS", f"Curve swap compilation failed: {compile_result.error}"
        assert compile_result.action_bundle is not None
        logger.info("Compiled %d transactions", len(compile_result.transactions))

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compile_result.action_bundle)
        assert execution_result.success is True, (
            f"Curve swap execution failed: {execution_result.error}\nCheck tricrypto pool address and coin indices."
        )

        logger.info("Execution success")

        # --- Layer 3: Parse receipt ---
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        swap_event_found = False
        parsed_usdt_spent = 0
        parsed_weth_received = 0

        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt if isinstance(tx_result.receipt, dict) else tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result is not None, "CurveReceiptParser returned None"

            if parse_result.success and parse_result.events:
                for event in parse_result.events:
                    if event.event_type == CurveEventType.TOKEN_EXCHANGE:
                        swap_event_found = True
                        assert "tokens_sold" in event.data, "Missing tokens_sold in swap event"
                        assert "tokens_bought" in event.data, "Missing tokens_bought in swap event"
                        assert event.data["tokens_sold"] > 0, "tokens_sold must be > 0"
                        assert event.data["tokens_bought"] > 0, "tokens_bought must be > 0"
                        parsed_usdt_spent += event.data["tokens_sold"]
                        parsed_weth_received += event.data["tokens_bought"]
                        logger.info(
                            "Swap event: sold_id=%s tokens_sold=%s bought_id=%s tokens_bought=%s",
                            event.data.get("sold_id"),
                            event.data.get("tokens_sold"),
                            event.data.get("bought_id"),
                            event.data.get("tokens_bought"),
                        )

        assert swap_event_found, (
            "CurveReceiptParser did not find TokenExchangeCrypto event. "
            "Verify receipt_parser handles CryptoSwap event topic."
        )

        # --- Layer 4: Balance deltas ---
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)

        usdt_spent = usdt_before - usdt_after
        weth_received = weth_after - weth_before
        expected_usdt_spent = int(swap_amount * Decimal(10**6))

        logger.info(
            "USDT after: %.2f (spent: %.2f)",
            usdt_after / 10**6,
            usdt_spent / 10**6,
        )
        logger.info(
            "WETH after: %.6f (received: %.6f)",
            weth_after / 10**18,
            weth_received / 10**18,
        )

        assert usdt_spent == expected_usdt_spent, (
            f"USDT spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdt_spent} ({swap_amount} USDT), Got: {usdt_spent}"
        )
        assert parsed_usdt_spent == usdt_spent, (
            f"Curve receipt sold amount must equal wallet delta: parsed={parsed_usdt_spent}, delta={usdt_spent}"
        )
        assert parsed_weth_received == weth_received, (
            f"Curve receipt bought amount must equal wallet delta: parsed={parsed_weth_received}, delta={weth_received}"
        )
        assert weth_received > 0, (
            "WETH balance did not increase after Curve swap! Check coin indices in tricrypto pool config."
        )

        logger.info(
            "SUCCESS: Swapped %.2f USDT -> %.6f WETH via tricrypto",
            usdt_spent / 10**6,
            weth_received / 10**18,
        )
