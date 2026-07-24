"""Curve SwapIntent tests for Polygon (VIB-4307, reworked for VIB-5551).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
Curve Finance swaps on Polygon via the frxUSD/USDT StableSwap NG pool.

Pool: Curve "FrxUSD USDT0 v1" (StableSwap NG) on Polygon
- Address: 0x5BC930b8f81F4cEEE3E3527159C3bDF453BcaAe9
- Coin order: USDT (index 0), frxUSD (index 1)
- Type: stableswap, is_ng=True (LP token IS the pool address)
- Swap path: exchange(int128,int128,uint256,uint256) — plain NG StableSwap

VIB-5551: this pool replaces the aave-type am3pool
(0x445FE580eF8d70FF569aB36e80c647af338db351) as the Polygon Curve
representative. am3pool's deposit flow routed the underlying into the FROZEN
Aave V2 Polygon LendingPool (VL_RESERVE_FROZEN), so every add_liquidity /
exchange_underlying reverted on current forks — it is removed from
CURVE_POOLS entirely. Coin order / liquidity verified on-chain 2026-07-24
(~45.2K USDT + ~47.3K frxUSD); real-fork proof:
tests/reports/vib-5551-polygon-frxusd-usdt-realfork.md.

We test USDT -> frxUSD (both stablecoins, balanced pool, low price impact).
USDT is part of the standard polygon funded_wallet seeding.

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
    get_token_balance,
)

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "polygon"
POOL_KEY = "frxusd_usdt"

# Curve frxUSD/USDT StableSwap NG pool on Polygon (VIB-5551).
# Coin order verified on-chain 2026-07-24: coins(0)=USDT, coins(1)=frxUSD.
EXPECTED_POOL_ADDRESS = "0x5BC930b8f81F4cEEE3E3527159C3bDF453BcaAe9"

# Token addresses (coin order: USDT=0, frxUSD=1)
USDT_ADDRESS = "0xc2132D05D31c914a87C6611C10748AEb04B58e8F"
FRXUSD_ADDRESS = "0x80Eede496655FB9047dd39d9f418d5483ED600df"

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


# =============================================================================
# Layer 1a: Pool Configuration Tests (No Anvil Required)
# =============================================================================


class TestCurvePolygonPoolConfig:
    """Verify frxusd_usdt is correctly configured in CURVE_POOLS."""

    @pytest.mark.intent(IntentType.SWAP)
    def test_polygon_in_curve_pools(self):
        """'polygon' chain must have a CURVE_POOLS entry."""
        assert "polygon" in CURVE_POOLS

    @pytest.mark.intent(IntentType.SWAP)
    def test_frxusd_usdt_present(self):
        """frxusd_usdt must be in CURVE_POOLS['polygon']."""
        assert POOL_KEY in CURVE_POOLS.get("polygon", {}), (
            f"'{POOL_KEY}' not found in CURVE_POOLS['polygon']. Found: {list(CURVE_POOLS.get('polygon', {}).keys())}"
        )

    @pytest.mark.intent(IntentType.SWAP)
    def test_am3pool_removed(self):
        """The frozen aave-type am3pool must NOT be registered (VIB-5551).

        Its deposit flow reverts at the frozen Aave V2 Polygon LendingPool, so
        re-adding it would route swaps/LP into a non-executable pool.
        """
        for name, data in CURVE_POOLS["polygon"].items():
            assert data["address"].lower() != "0x445fe580ef8d70ff569ab36e80c647af338db351", (
                f"am3pool (frozen Aave V2 backing) must stay out of the registry; found as '{name}'"
            )
            assert not data.get("use_underlying"), (
                f"No polygon pool may require the frozen Aave V2 underlying-deposit flow; '{name}' does"
            )

    @pytest.mark.intent(IntentType.SWAP)
    def test_pool_address_correct(self):
        """Pool address must match the deployed StableSwap NG contract."""
        pool = CURVE_POOLS["polygon"][POOL_KEY]
        assert pool["address"].lower() == EXPECTED_POOL_ADDRESS.lower()

    @pytest.mark.intent(IntentType.SWAP)
    def test_pool_is_ng(self):
        """frxusd_usdt is StableSwap NG: is_ng=True and LP token == pool address."""
        pool = CURVE_POOLS["polygon"][POOL_KEY]
        assert pool.get("is_ng") is True, f"frxusd_usdt must have is_ng=True; got {pool.get('is_ng')}"
        assert pool["lp_token"].lower() == pool["address"].lower(), "StableSwap NG: LP token IS the pool address"

    @pytest.mark.intent(IntentType.SWAP)
    def test_pool_has_2_coins_in_onchain_order(self):
        """frxusd_usdt is a 2-coin pool: coins(0)=USDT, coins(1)=frxUSD."""
        pool = CURVE_POOLS["polygon"][POOL_KEY]
        assert pool["n_coins"] == 2
        assert len(pool["coin_addresses"]) == 2
        assert pool["coin_addresses"][0].lower() == USDT_ADDRESS.lower(), "coins(0) must be USDT"
        assert pool["coin_addresses"][1].lower() == FRXUSD_ADDRESS.lower(), "coins(1) must be frxUSD"


# =============================================================================
# Layer 1b: SwapIntent Compilation Tests (No Anvil Required)
# =============================================================================


class TestCurvePolygonSwapCompilation:
    """Layer 1: Verify SwapIntent compiles correctly using frxusd_usdt."""

    def _make_compiler(self) -> IntentCompiler:
        return IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    @pytest.mark.intent(IntentType.SWAP)
    def test_usdt_to_frxusd_swap_compiles(self):
        """SwapIntent USDT -> frxUSD on Polygon frxusd_usdt must compile successfully."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDT",
            to_token="frxUSD",
            amount=Decimal("100"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"USDT -> frxUSD Curve swap compilation failed: {result.error}"
        )
        assert result.action_bundle is not None

    @pytest.mark.intent(IntentType.SWAP)
    def test_frxusd_to_usdt_swap_compiles(self):
        """SwapIntent frxUSD -> USDT (reverse direction) must compile."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="frxUSD",
            to_token="USDT",
            amount=Decimal("100"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"frxUSD -> USDT Curve swap compilation failed: {result.error}"
        )
        assert result.action_bundle is not None

    @pytest.mark.intent(IntentType.SWAP)
    def test_compiled_swap_targets_frxusd_usdt_pool(self):
        """Compiled transactions must target the frxusd_usdt pool address."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDT",
            to_token="frxUSD",
            amount=Decimal("50"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS

        swap_txs = [tx for tx in result.transactions if tx.to.lower() == EXPECTED_POOL_ADDRESS.lower()]
        assert len(swap_txs) > 0, (
            f"No transaction targeting frxusd_usdt {EXPECTED_POOL_ADDRESS}. "
            f"Transactions: {[(tx.to, tx.description) for tx in result.transactions]}"
        )


# =============================================================================
# Layers 2-4: Full On-Chain Swap Tests (Requires Polygon Anvil Fork)
# =============================================================================


@pytest.mark.polygon
@pytest.mark.swap
class TestCurvePolygonSwapExecution:
    """Layers 2-4: Full on-chain Curve swap tests on Polygon Anvil fork.

    Tests USDT -> frxUSD swap via frxusd_usdt (StableSwap NG) with:
    - Layer 2: Transaction execution on Anvil
    - Layer 3: TokenExchange event parsing
    - Layer 4: Exact bilateral balance delta verification
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_usdt_to_frxusd_full_lifecycle(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Layer 2-4: Execute USDT -> frxUSD Curve swap on Polygon Anvil.

        Verifies:
        - Compilation succeeds with real prices (Layer 1)
        - Execution succeeds on Anvil (Layer 2)
        - TokenExchange event parsed (Layer 3)
        - USDT balance decreased exactly, frxUSD balance increased (Layer 4)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]
        assert usdt_addr.lower() == USDT_ADDRESS.lower()

        swap_amount = Decimal("100")  # 100 USDT

        logger.info(
            "Test: USDT -> frxUSD Curve swap on Polygon (frxusd_usdt NG)\nPool: %s",
            EXPECTED_POOL_ADDRESS,
        )

        # --- Layer 4 setup: record balances BEFORE ---
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        frxusd_before = get_token_balance(web3, FRXUSD_ADDRESS, funded_wallet)
        logger.info("USDT before: %.2f", usdt_before / 10**6)
        logger.info("frxUSD before: %.2f", frxusd_before / 10**18)
        assert usdt_before >= int(swap_amount * 10**6), (
            "Test wallet lacks USDT — polygon funded_wallet seeding failed"
        )

        # --- Layer 1: Compile ---
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        intent = SwapIntent(
            from_token="USDT",
            to_token="frxUSD",
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
            "Check frxusd_usdt coin indices and is_ng setting."
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
            "CurveReceiptParser did not find a TokenExchange event. "
            "Verify receipt_parser handles StableSwap NG frxusd_usdt swaps."
        )

        # --- Layer 4: Bilateral balance deltas ---
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        frxusd_after = get_token_balance(web3, FRXUSD_ADDRESS, funded_wallet)

        usdt_spent = usdt_before - usdt_after
        frxusd_received = frxusd_after - frxusd_before
        expected_usdt_spent = int(swap_amount * Decimal(10**6))

        logger.info(
            "USDT after: %.2f (spent: %.2f)",
            usdt_after / 10**6,
            usdt_spent / 10**6,
        )
        logger.info(
            "frxUSD after: %.2f (received: %.2f)",
            frxusd_after / 10**18,
            frxusd_received / 10**18,
        )

        assert usdt_spent == expected_usdt_spent, (
            f"USDT spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdt_spent} ({swap_amount} USDT), Got: {usdt_spent}"
        )
        assert frxusd_received > 0, (
            "frxUSD balance did not increase after Curve swap! Check coin indices in frxusd_usdt config."
        )

        logger.info(
            "SUCCESS: Swapped %.2f USDT -> %.2f frxUSD via frxusd_usdt",
            usdt_spent / 10**6,
            frxusd_received / 10**18,
        )
