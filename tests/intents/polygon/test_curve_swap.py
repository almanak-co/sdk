"""Curve SwapIntent tests for Polygon (VIB-4307).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
Curve Finance swaps on Polygon via am3pool (aave-type StableSwap).

Pool: Curve am3pool (DAI/USDC.e/USDT) on Polygon
- Address: 0x445FE580eF8d70FF569aB36e80c647af338db351
- Coin order: DAI (index 0), USDC.e (index 1), USDT (index 2)
- Type: stableswap with use_underlying=True (aave-type)
- Swap path: exchange_underlying() with underlying tokens (DAI/USDC.e/USDT)

We test USDC.e -> USDT (both are stablecoins, deep liquidity, lower price impact).
USDC.e must be funded via storage slot since it's not in CHAIN_CONFIGS for polygon.

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/polygon/test_curve_swap.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.curve.adapter import CURVE_POOLS
from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    SWAP_MAX_SLIPPAGE,
    fund_erc20_token,
    get_token_balance,
)

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "polygon"
POOL_KEY = "3pool"

# Curve am3pool on Polygon
# VIB-5434: corrected from the dead 0x445Fe580…898ed8631406dB5f literal (no code on
# Polygon) to the real am3pool. Verified on-fork 2026-06-30.
EXPECTED_POOL_ADDRESS = "0x445FE580eF8d70FF569aB36e80c647af338db351"

# Token addresses (coin order: DAI=0, USDC.e=1, USDT=2)
DAI_ADDRESS = "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063"
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # bridged USDC
USDT_ADDRESS = "0xc2132D05D31c914a87C6611C10748AEb04B58e8F"

# Storage slot for USDC.e (Polygon PoS-bridged tokens use slot 0)
USDC_E_BALANCE_SLOT = 0

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


# =============================================================================
# Helpers
# =============================================================================


def _fund_usdc_e(wallet: str, rpc_url: str, amount: Decimal = Decimal("10000")) -> None:
    """Fund test wallet with USDC.e (bridged USDC) on Polygon via storage slot."""
    decimals = 6
    amount_wei = int(amount * Decimal(10**decimals))
    fund_erc20_token(wallet, USDC_E_ADDRESS, amount_wei, USDC_E_BALANCE_SLOT, rpc_url)


# =============================================================================
# Layer 1a: Pool Configuration Tests (No Anvil Required)
# =============================================================================


class TestCurvePolygonPoolConfig:
    """Verify am3pool is correctly configured in CURVE_POOLS."""

    @pytest.mark.intent(IntentType.SWAP)
    def test_polygon_in_curve_pools(self):
        """'polygon' chain must have a CURVE_POOLS entry."""
        assert "polygon" in CURVE_POOLS

    @pytest.mark.intent(IntentType.SWAP)
    def test_3pool_present(self):
        """3pool (am3pool) must be in CURVE_POOLS['polygon']."""
        assert POOL_KEY in CURVE_POOLS.get("polygon", {}), (
            f"'{POOL_KEY}' not found in CURVE_POOLS['polygon']. Found: {list(CURVE_POOLS.get('polygon', {}).keys())}"
        )

    @pytest.mark.intent(IntentType.SWAP)
    def test_pool_address_correct(self):
        """Pool address must match deployed am3pool contract."""
        pool = CURVE_POOLS["polygon"][POOL_KEY]
        assert pool["address"].lower() == EXPECTED_POOL_ADDRESS.lower()

    @pytest.mark.intent(IntentType.SWAP)
    def test_pool_uses_underlying(self):
        """am3pool must have use_underlying=True (aave-type)."""
        pool = CURVE_POOLS["polygon"][POOL_KEY]
        assert pool.get("use_underlying") is True, (
            f"am3pool must have use_underlying=True; got {pool.get('use_underlying')}"
        )

    @pytest.mark.intent(IntentType.SWAP)
    def test_pool_has_3_coins(self):
        """am3pool is a 3-coin pool: DAI, USDC.e, USDT."""
        pool = CURVE_POOLS["polygon"][POOL_KEY]
        assert pool["n_coins"] == 3
        assert len(pool["coin_addresses"]) == 3


# =============================================================================
# Layer 1b: SwapIntent Compilation Tests (No Anvil Required)
# =============================================================================


class TestCurvePolygonSwapCompilation:
    """Layer 1: Verify SwapIntent compiles correctly using am3pool."""

    def _make_compiler(self) -> IntentCompiler:
        return IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    @pytest.mark.intent(IntentType.SWAP)
    def test_usdc_e_to_usdt_swap_compiles(self):
        """SwapIntent USDC.e -> USDT on Polygon am3pool must compile successfully."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDC.e",
            to_token="USDT",
            amount=Decimal("100"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"USDC.e -> USDT Curve swap compilation failed: {result.error}"
        )
        assert result.action_bundle is not None

    @pytest.mark.intent(IntentType.SWAP)
    def test_dai_to_usdt_swap_compiles(self):
        """SwapIntent DAI -> USDT (alternative direction) must compile."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="DAI",
            to_token="USDT",
            amount=Decimal("100"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, f"DAI -> USDT Curve swap compilation failed: {result.error}"
        assert result.action_bundle is not None

    @pytest.mark.intent(IntentType.SWAP)
    def test_compiled_swap_targets_am3pool(self):
        """Compiled transactions must target the am3pool address."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDC.e",
            to_token="USDT",
            amount=Decimal("50"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS

        swap_txs = [tx for tx in result.transactions if tx.to.lower() == EXPECTED_POOL_ADDRESS.lower()]
        assert len(swap_txs) > 0, (
            f"No transaction targeting am3pool {EXPECTED_POOL_ADDRESS}. "
            f"Transactions: {[(tx.to, tx.description) for tx in result.transactions]}"
        )


# =============================================================================
# Layers 2-4: Full On-Chain Swap Tests (Requires Polygon Anvil Fork)
# =============================================================================


@pytest.mark.polygon
@pytest.mark.swap
class TestCurvePolygonSwapExecution:
    """Layers 2-4: Full on-chain Curve swap tests on Polygon Anvil fork.

    Tests USDC.e -> USDT swap via am3pool with:
    - Layer 2: Transaction execution on Anvil
    - Layer 3: TokenExchangeUnderlying event parsing (use_underlying=True)
    - Layer 4: Exact balance delta verification
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=True,
        reason=(
            "VIB-5551: am3pool exchange_underlying reverts on a current Polygon fork — the "
            "aave-type swap deposits the underlying into the FROZEN Aave V2 Polygon "
            "LendingPool (VL_RESERVE_FROZEN). The TokenExchangeUnderlying signature itself "
            "IS decoded by CurveReceiptParser (the VIB-4307 'missing signatures' claim was "
            "stale; see tests/unit/connectors/curve/test_am3pool_real_logs.py). as of 2026-06-30."
        ),
    )
    async def test_usdc_e_to_usdt_full_lifecycle(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Layer 2-4: Execute USDC.e -> USDT Curve swap on Polygon Anvil.

        Verifies:
        - Compilation succeeds with real prices (Layer 1)
        - Execution succeeds on Anvil (Layer 2)
        - TokenExchange/TokenExchangeUnderlying event parsed (Layer 3)
        - USDC.e balance decreased exactly, USDT balance increased (Layer 4)
        """
        # Fund USDC.e (not in standard CHAIN_CONFIGS for polygon)
        _fund_usdc_e(funded_wallet, anvil_rpc_url)
        usdc_e_funded = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        if usdc_e_funded == 0:
            pytest.skip(
                f"USDC.e funding failed at slot {USDC_E_BALANCE_SLOT}. Polygon USDC.e may use different storage layout."
            )

        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]

        swap_amount = Decimal("100")  # 100 USDC.e

        logger.info(
            "Test: USDC.e -> USDT Curve swap on Polygon (am3pool)\nPool: %s",
            EXPECTED_POOL_ADDRESS,
        )

        # --- Layer 4 setup: record balances BEFORE ---
        usdc_e_before = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        logger.info("USDC.e before: %.2f", usdc_e_before / 10**6)
        logger.info("USDT before: %.2f", usdt_before / 10**6)
        assert usdc_e_before > 0, "Test wallet has no USDC.e — funding failed"

        # --- Layer 1: Compile ---
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        intent = SwapIntent(
            from_token="USDC.e",
            to_token="USDT",
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
            "Check am3pool coin indices and use_underlying setting."
        )

        logger.info("Execution success")

        # --- Layer 3: Parse receipt ---
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        swap_event_found = False

        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt if isinstance(tx_result.receipt, dict) else tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result is not None, "CurveReceiptParser returned None"

            if parse_result.success and parse_result.events:
                for event in parse_result.events:
                    # Aave-type pools emit TokenExchangeUnderlying instead of
                    # TokenExchange when called via exchange_underlying().
                    if event.event_type in (
                        CurveEventType.TOKEN_EXCHANGE,
                        CurveEventType.TOKEN_EXCHANGE_UNDERLYING,
                    ):
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
            "CurveReceiptParser did not find TokenExchange or TokenExchangeUnderlying event. "
            "Verify receipt_parser handles aave-type am3pool swaps."
        )

        # --- Layer 4: Balance deltas ---
        usdc_e_after = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)

        usdc_e_spent = usdc_e_before - usdc_e_after
        usdt_received = usdt_after - usdt_before
        expected_usdc_e_spent = int(swap_amount * Decimal(10**6))

        logger.info(
            "USDC.e after: %.2f (spent: %.2f)",
            usdc_e_after / 10**6,
            usdc_e_spent / 10**6,
        )
        logger.info(
            "USDT after: %.2f (received: %.2f)",
            usdt_after / 10**6,
            usdt_received / 10**6,
        )

        assert usdc_e_spent == expected_usdc_e_spent, (
            f"USDC.e spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdc_e_spent} ({swap_amount} USDC.e), Got: {usdc_e_spent}"
        )
        assert usdt_received > 0, (
            "USDT balance did not increase after Curve swap! Check coin indices in am3pool config."
        )

        logger.info(
            "SUCCESS: Swapped %.2f USDC.e -> %.2f USDT via am3pool",
            usdc_e_spent / 10**6,
            usdt_received / 10**6,
        )
