"""Curve SwapIntent tests for Base (VIB-4307).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
Curve Finance swaps on Base via 4pool (StableSwap NG, 4-coin).

Pool: Curve 4pool (USDC/USDbC/axlUSDC/crvUSD) on Base
- Address: 0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f
- LP token: same as pool (StableSwap NG: LP = pool)
- Coins[0]: USDC (native, 0x833589fC...)
- Coins[1]: USDbC (bridged, 0xd9aAEc86...)
- Coins[2]: axlUSDC (0xEB466342...)
- Coins[3]: crvUSD (0x417Ac0e0...)
- Type: stableswap (4-coin NG)

We test USDC -> USDbC swap because both are funded by the standard base conftest
(see CHAIN_CONFIGS["base"]["tokens"] — both have balance slots configured).

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/base/test_curve_swap.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.curve.adapter import CURVE_POOLS
from almanak.framework.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
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

CHAIN_NAME = "base"
POOL_KEY = "4pool"

# Curve 4pool on Base
EXPECTED_POOL_ADDRESS = "0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f"

# Token addresses (coin order: USDC=0, USDbC=1, axlUSDC=2, crvUSD=3)
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"      # native USDC
USDBC_ADDRESS = "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA"     # bridged USDC (USDbC)
AXLUSDC_ADDRESS = "0xEB466342C4d449BC9f53A865D5Cb90586f405215"
CRVUSD_ADDRESS = "0x417Ac0e078398C154EdFadD9Ef675d30Be60Af93"

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


# =============================================================================
# Layer 1a: Pool Configuration Tests (No Anvil Required)
# =============================================================================


class TestCurveBasePoolConfig:
    """Verify 4pool is correctly configured in CURVE_POOLS."""

    @pytest.mark.intent(IntentType.SWAP)
    def test_base_in_curve_pools(self):
        """'base' chain must have a CURVE_POOLS entry."""
        assert "base" in CURVE_POOLS

    @pytest.mark.intent(IntentType.SWAP)
    def test_4pool_present(self):
        """4pool must be in CURVE_POOLS['base']."""
        assert POOL_KEY in CURVE_POOLS.get("base", {}), (
            f"'{POOL_KEY}' not found in CURVE_POOLS['base']. "
            f"Found: {list(CURVE_POOLS.get('base', {}).keys())}"
        )

    @pytest.mark.intent(IntentType.SWAP)
    def test_pool_address_correct(self):
        """Pool address must match deployed 4pool contract."""
        pool = CURVE_POOLS["base"][POOL_KEY]
        assert pool["address"].lower() == EXPECTED_POOL_ADDRESS.lower()

    @pytest.mark.intent(IntentType.SWAP)
    def test_pool_type_is_stableswap(self):
        """Pool type must be 'stableswap' (NG variant)."""
        pool = CURVE_POOLS["base"][POOL_KEY]
        assert pool["pool_type"] == "stableswap"

    @pytest.mark.intent(IntentType.SWAP)
    def test_pool_has_4_coins(self):
        """4pool has 4 coins: USDC, USDbC, axlUSDC, crvUSD."""
        pool = CURVE_POOLS["base"][POOL_KEY]
        assert pool["n_coins"] == 4
        assert len(pool["coin_addresses"]) == 4

    @pytest.mark.intent(IntentType.SWAP)
    def test_pool_contains_native_usdc(self):
        """4pool must contain native USDC at index 0."""
        pool = CURVE_POOLS["base"][POOL_KEY]
        assert pool["coin_addresses"][0].lower() == USDC_ADDRESS.lower()

    @pytest.mark.intent(IntentType.SWAP)
    def test_pool_contains_usdbc(self):
        """4pool must contain USDbC at index 1."""
        pool = CURVE_POOLS["base"][POOL_KEY]
        assert pool["coin_addresses"][1].lower() == USDBC_ADDRESS.lower()


# =============================================================================
# Layer 1b: SwapIntent Compilation Tests (No Anvil Required)
# =============================================================================


class TestCurveBaseSwapCompilation:
    """Layer 1: Verify SwapIntent compiles correctly using 4pool."""

    def _make_compiler(self) -> IntentCompiler:
        return IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    @pytest.mark.intent(IntentType.SWAP)
    def test_usdc_to_usdbc_swap_compiles(self):
        """SwapIntent USDC -> USDbC on Base 4pool must compile successfully."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="USDbC",
            amount=Decimal("100"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"USDC -> USDbC Curve swap compilation failed: {result.error}"
        )
        assert result.action_bundle is not None

    @pytest.mark.intent(IntentType.SWAP)
    def test_usdbc_to_usdc_swap_compiles(self):
        """SwapIntent USDbC -> USDC (reverse direction) must compile."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDbC",
            to_token="USDC",
            amount=Decimal("100"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"USDbC -> USDC Curve swap compilation failed: {result.error}"
        )
        assert result.action_bundle is not None

    @pytest.mark.intent(IntentType.SWAP)
    def test_compiled_swap_targets_4pool(self):
        """Compiled transactions must target the 4pool address."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="USDbC",
            amount=Decimal("50"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS

        swap_txs = [
            tx for tx in result.transactions
            if tx.to.lower() == EXPECTED_POOL_ADDRESS.lower()
        ]
        assert len(swap_txs) > 0, (
            f"No transaction targeting 4pool {EXPECTED_POOL_ADDRESS}. "
            f"Transactions: {[(tx.to, tx.description) for tx in result.transactions]}"
        )


# =============================================================================
# Layers 2-4: Full On-Chain Swap Tests (Requires Base Anvil Fork)
# =============================================================================


@pytest.mark.base
@pytest.mark.swap
class TestCurveBaseSwapExecution:
    """Layers 2-4: Full on-chain Curve swap tests on Base Anvil fork.

    Tests USDC -> USDbC swap via 4pool with:
    - Layer 2: Transaction execution on Anvil
    - Layer 3: TokenExchange event parsing (StableSwap NG)
    - Layer 4: Exact balance delta verification
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_usdc_to_usdbc_full_lifecycle(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Layer 2-4: Execute USDC -> USDbC Curve swap on Base Anvil.

        Verifies:
        - Compilation succeeds with real prices (Layer 1)
        - Execution succeeds on Anvil (Layer 2)
        - TokenExchange event parsed from receipt (Layer 3)
        - USDC balance decreased exactly, USDbC balance increased (Layer 4)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        usdbc_addr = tokens["USDbC"]

        swap_amount = Decimal("100")  # 100 USDC

        logger.info(
            "Test: USDC -> USDbC Curve swap on Base (4pool)\nPool: %s",
            EXPECTED_POOL_ADDRESS,
        )

        # --- Layer 4 setup: record balances BEFORE ---
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        usdbc_before = get_token_balance(web3, usdbc_addr, funded_wallet)
        logger.info("USDC before: %.2f", usdc_before / 10**6)
        logger.info("USDbC before: %.2f", usdbc_before / 10**6)
        assert usdc_before > 0, "Test wallet has no USDC — funding failed"

        # --- Layer 1: Compile ---
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="USDbC",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="curve",
            chain=CHAIN_NAME,
        )

        compile_result = compiler.compile(intent)
        assert compile_result.status == CompilationStatus.SUCCESS, (
            f"Curve swap compilation failed: {compile_result.error}"
        )
        assert compile_result.action_bundle is not None
        logger.info("Compiled %d transactions", len(compile_result.transactions))

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compile_result.action_bundle)
        assert execution_result.success, (
            f"Curve swap execution failed: {execution_result.error}\n"
            "Check 4pool coin indices."
        )

        logger.info("Execution success")

        # --- Layer 3: Parse receipt ---
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        swap_event_found = False

        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = (
                tx_result.receipt if isinstance(tx_result.receipt, dict)
                else tx_result.receipt.to_dict()
            )
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
                        logger.info(
                            "Swap event: sold_id=%s tokens_sold=%s bought_id=%s tokens_bought=%s",
                            event.data.get("sold_id"),
                            event.data.get("tokens_sold"),
                            event.data.get("bought_id"),
                            event.data.get("tokens_bought"),
                        )

        assert swap_event_found, (
            "CurveReceiptParser did not find TokenExchange event. "
            "Verify receipt_parser handles StableSwap NG 4pool events."
        )

        # --- Layer 4: Balance deltas ---
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        usdbc_after = get_token_balance(web3, usdbc_addr, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        usdbc_received = usdbc_after - usdbc_before
        expected_usdc_spent = int(swap_amount * Decimal(10**6))

        logger.info(
            "USDC after: %.2f (spent: %.2f)",
            usdc_after / 10**6, usdc_spent / 10**6,
        )
        logger.info(
            "USDbC after: %.2f (received: %.2f)",
            usdbc_after / 10**6, usdbc_received / 10**6,
        )

        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdc_spent} ({swap_amount} USDC), Got: {usdc_spent}"
        )
        assert usdbc_received > 0, (
            "USDbC balance did not increase after Curve swap! "
            "Check coin indices in 4pool config."
        )

        logger.info(
            "SUCCESS: Swapped %.2f USDC -> %.2f USDbC via 4pool",
            usdc_spent / 10**6,
            usdbc_received / 10**6,
        )
