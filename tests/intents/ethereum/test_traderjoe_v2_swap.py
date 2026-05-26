"""TraderJoe V2 Liquidity Book swap intent tests on Ethereum (VIB-4378).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
TraderJoe V2 swaps on Ethereum using the ethereum-specific LBRouter v2.1.

Background:
    TraderJoe deployed a **distinct** LBRouter v2.1 on Ethereum at
    ``0x9A93a421b74F1c5755b83dD2C211614dC419C44b`` (factory:
    ``0xDC8d77b69155c7E68A95a4fb0f06a71FF90B943a``). Unlike Avalanche /
    Arbitrum / BSC -- which share a CREATE2-deployed router at
    ``0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30`` -- Ethereum's router
    address is different. The dedicated ``_compile_swap_traderjoe_v2``
    compilation path (VIB-1928) is chain-agnostic so long as the chain
    appears in ``TRADERJOE_V2`` in ``almanak/core/contracts.py``; this
    test exercises that path against Ethereum's distinct deployment.

Pool choice:
    USDT/USDC bin_step=1 (LBPair ``0x47B1CEC2D2370E11B049c73aB6732F03E920C71a``)
    is the most liquid TJv2 pair on Ethereum as of 2026-05-14 (~497 USDT /
    ~70 USDC reserves). WETH/USDC pairs exist (bin_step=25, bin_step=100)
    but are essentially empty at the fork block. Bin_step is auto-detected
    by the compiler across the standard list (20, 25, 15, 10, 50, 5, 100, 1)
    so the test doesn't pin a specific step.

Coordination note (VIB-4378):
    The ConnectorRegistry edit for ``"ethereum"`` is intentionally
    DEFERRED to a follow-up ticket to avoid same-line merge conflicts
    with sibling VIB-4376 (bnb SWAP), which is concurrently extending
    the same ``register_connector(chains=...)`` tuple. This test will
    pass but is not yet credited by the intent-coverage gate.

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/ethereum/test_traderjoe_v2_swap.py -v -s
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

CHAIN_NAME = "ethereum"

# Token addresses on Ethereum (mirrors CHAIN_CONFIGS and TRADERJOE_V2_TOKENS).
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7"

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


# =============================================================================
# Layer 1: Compilation Tests (No Anvil Required)
# =============================================================================


class TestTraderJoeV2SwapCompilation:
    """Layer 1: Verify SwapIntent compiles correctly for TraderJoe V2 on Ethereum."""

    def _make_compiler(self) -> IntentCompiler:
        return IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    @pytest.mark.intent(IntentType.SWAP)
    def test_vib_1406_guard_removed(self):
        """SwapIntent(protocol='traderjoe_v2') must NOT return VIB-1406 block error on ethereum."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDT",
            to_token="USDC",
            amount=Decimal("1.0"),
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
# Layers 2-4: Full On-Chain Swap Tests (Requires Ethereum Anvil Fork)
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.swap
class TestTraderJoeV2SwapExecution:
    """Layers 2-4: Full on-chain TJ V2 swap tests on Ethereum Anvil fork.

    Tests USDT <-> USDC swaps via the ethereum-specific LBRouter v2.1 with:
    - Layer 2: Transaction execution on Anvil
    - Layer 3: Transfer event parsing via TraderJoeV2ReceiptParser
    - Layer 4: Exact bilateral balance delta verification (no-op guard)
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_usdt_to_usdc_full_lifecycle(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Layer 2-4: Execute USDT -> USDC swap via the ethereum-specific TJ V2 LBRouter v2.1.

        Verifies:
        - Compilation succeeds with auto-detected bin_step (Layer 1)
        - Execution succeeds on Anvil (Layer 2)
        - Receipt parser extracts swap amounts (Layer 3)
        - USDT balance decreased exactly, USDC balance increased (Layer 4)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]
        usdc_addr = tokens["USDC"]

        # 5 USDT — small to stay within bin liquidity on the only liquid TJv2
        # pair on Ethereum (USDT/USDC bin_step=1, reserves ~497 USDT / ~70 USDC
        # as of 2026-05-14).
        swap_amount = Decimal("5")

        logger.info("Test: USDT -> USDC TraderJoe V2 swap on Ethereum")

        # --- Layer 4 setup: record balances BEFORE ---
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        logger.info("USDT before: %.2f", usdt_before / 10**6)
        logger.info("USDC before: %.2f", usdc_before / 10**6)
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
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        usdt_spent = usdt_before - usdt_after
        usdc_received = usdc_after - usdc_before
        expected_usdt_spent = int(swap_amount * Decimal(10**6))

        logger.info("USDT after: %.2f (spent: %.2f)", usdt_after / 10**6, usdt_spent / 10**6)
        logger.info("USDC after: %.2f (received: %.2f)", usdc_after / 10**6, usdc_received / 10**6)

        assert usdt_spent == expected_usdt_spent, (
            f"USDT spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdt_spent}, Got: {usdt_spent}"
        )
        assert usdc_received > 0, (
            "USDC balance did not increase after TraderJoe V2 swap (no-op guard)!"
        )

        logger.info(
            "SUCCESS: Swapped %.2f USDT -> %.2f USDC via TraderJoe V2 on Ethereum",
            usdt_spent / 10**6,
            usdc_received / 10**6,
        )

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_usdc_to_usdt_reverse_direction(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Layer 2-4: Execute USDC -> USDT reverse swap via TraderJoe V2 on Ethereum."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]
        usdc_addr = tokens["USDC"]

        swap_amount = Decimal("5")  # 5 USDC

        # --- Layer 4 BEFORE ---
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
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
            to_token="USDT",
            amount=swap_amount,
            max_slippage=Decimal("0.03"),
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )

        compile_result = compiler.compile(intent)
        assert compile_result.status == CompilationStatus.SUCCESS, (
            f"Reverse TJ V2 swap compilation failed: {compile_result.error}"
        )
        assert compile_result.action_bundle is not None, "ActionBundle must be created"
        assert compile_result.action_bundle.metadata.get("protocol") == "traderjoe_v2"

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
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        usdt_received = usdt_after - usdt_before
        expected_usdc_spent = int(swap_amount * Decimal(10**6))

        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        assert usdt_received > 0, "USDT balance did not increase after reverse swap (no-op guard)!"

        logger.info(
            "SUCCESS: Reverse swap %.2f USDC -> %.2f USDT via TraderJoe V2 on Ethereum",
            usdc_spent / 10**6,
            usdt_received / 10**6,
        )
