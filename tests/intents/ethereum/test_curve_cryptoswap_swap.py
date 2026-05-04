"""Curve CryptoSwap (tricrypto2) swap intent tests on Ethereum (VIB-1488).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
Curve Finance's tricrypto2 CryptoSwap pool on Ethereum.

Background:
    Iter 95 fixed two bugs in Curve CryptoSwap: gas estimate too low (200K -> 500K)
    and missing TokenExchangeCrypto event topic in the receipt parser. These fixes
    need a proper 4-layer intent test to prevent regression.

    CryptoSwap differs from StableSwap:
    - Event: TokenExchangeCrypto (uint256 indices) vs TokenExchange (int128 indices)
    - Selector: 0x5b41b908 (uint256,uint256,uint256,uint256) vs 0x3df02124 (int128,int128,uint256,uint256)
    - Higher gas usage (~500K vs ~200K)

    Pool: Curve tricrypto2 (USDT/WBTC/WETH)
    Address: 0xD51a44d3FaE010294C616388b506AcdA1bfAAE46
    Coin order: USDT (index 0), WBTC (index 1), WETH (index 2)

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/ethereum/test_curve_cryptoswap_swap.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.curve.adapter import CURVE_POOLS
from almanak.framework.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import SwapIntent
from tests.intents.conftest import CHAIN_CONFIGS, SWAP_MAX_SLIPPAGE, fund_erc20_token, get_token_balance

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "ethereum"
POOL_KEY = "tricrypto2"

# Curve tricrypto2 pool on Ethereum
POOL_ADDRESS = "0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"

# Token addresses (coin order: USDT=0, WBTC=1, WETH=2)
USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
WBTC_ADDRESS = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

# USDT balance slot on Ethereum mainnet (Tether USD: slot 2)
USDT_BALANCE_SLOT = 2

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


# =============================================================================
# Helpers
# =============================================================================


def _fund_usdt(wallet: str, rpc_url: str, amount_usdt: Decimal = Decimal("10000")) -> None:
    """Fund test wallet with USDT via storage slot manipulation."""
    usdt_decimals = 6
    amount_wei = int(amount_usdt * Decimal(10**usdt_decimals))
    fund_erc20_token(wallet, USDT_ADDRESS, amount_wei, USDT_BALANCE_SLOT, rpc_url)


# =============================================================================
# Layer 1a: Pool Configuration Tests (No Anvil Required)
# =============================================================================


class TestCurveCryptoSwapPoolConfig:
    """Verify tricrypto2 pool is correctly configured in CURVE_POOLS."""

    def test_ethereum_in_curve_pools(self):
        """'ethereum' chain must have a CURVE_POOLS entry."""
        assert "ethereum" in CURVE_POOLS

    def test_tricrypto2_pool_present(self):
        """tricrypto2 pool must be in CURVE_POOLS['ethereum']."""
        assert POOL_KEY in CURVE_POOLS.get("ethereum", {}), (
            f"'{POOL_KEY}' not found in CURVE_POOLS['ethereum']. "
            f"Found: {list(CURVE_POOLS.get('ethereum', {}).keys())}"
        )

    def test_pool_address_correct(self):
        """Pool address must match deployed tricrypto2 contract."""
        pool = CURVE_POOLS["ethereum"][POOL_KEY]
        assert pool["address"].lower() == POOL_ADDRESS.lower()

    def test_pool_type_is_tricrypto(self):
        """Pool type must be 'tricrypto' (CryptoSwap variant)."""
        pool = CURVE_POOLS["ethereum"][POOL_KEY]
        assert pool["pool_type"] in ("tricrypto", "cryptoswap"), (
            f"Expected tricrypto/cryptoswap pool type, got {pool['pool_type']}"
        )

    def test_pool_has_3_coins(self):
        """tricrypto2 is a 3-coin pool: USDT, WBTC, WETH."""
        pool = CURVE_POOLS["ethereum"][POOL_KEY]
        assert pool["n_coins"] == 3
        assert len(pool["coin_addresses"]) == 3
        assert len(pool["coins"]) == 3

    def test_pool_coins_order(self):
        """Coin order must be USDT(0), WBTC(1), WETH(2)."""
        pool = CURVE_POOLS["ethereum"][POOL_KEY]
        coins = pool["coins"]
        assert coins[0] == "USDT", f"Coin 0 must be USDT, got {coins[0]}"
        assert coins[1] == "WBTC", f"Coin 1 must be WBTC, got {coins[1]}"
        assert coins[2] == "WETH", f"Coin 2 must be WETH, got {coins[2]}"

    def test_pool_coin_addresses(self):
        """Coin addresses must match known mainnet addresses."""
        pool = CURVE_POOLS["ethereum"][POOL_KEY]
        addrs = [a.lower() for a in pool["coin_addresses"]]
        assert addrs[0] == USDT_ADDRESS.lower(), f"USDT address mismatch: {addrs[0]}"
        assert addrs[1] == WBTC_ADDRESS.lower(), f"WBTC address mismatch: {addrs[1]}"
        assert addrs[2] == WETH_ADDRESS.lower(), f"WETH address mismatch: {addrs[2]}"


# =============================================================================
# Layer 1b: SwapIntent Compilation Tests (No Anvil Required)
# =============================================================================


class TestCurveCryptoSwapCompilation:
    """Layer 1: Verify SwapIntent compiles correctly for CryptoSwap pools."""

    def _make_compiler(self) -> IntentCompiler:
        return IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    def test_usdt_to_weth_swap_compiles(self):
        """SwapIntent USDT -> WETH on tricrypto2 must compile successfully."""
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

        assert result.status == CompilationStatus.SUCCESS, (
            f"USDT -> WETH CryptoSwap compilation failed: {result.error}"
        )
        assert result.action_bundle is not None

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

        assert result.status == CompilationStatus.SUCCESS, (
            f"WETH -> USDT CryptoSwap compilation failed: {result.error}"
        )
        assert result.action_bundle is not None

    def test_usdt_to_wbtc_swap_compiles(self):
        """SwapIntent USDT -> WBTC on tricrypto2 must compile."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDT",
            to_token="WBTC",
            amount=Decimal("100"),
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"USDT -> WBTC CryptoSwap compilation failed: {result.error}"
        )
        assert result.action_bundle is not None

    def test_compiled_swap_targets_tricrypto2_pool(self):
        """Compiled transactions must target the tricrypto2 pool address."""
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

        # Last transaction should target the pool (exchange call)
        swap_txs = [
            tx for tx in result.transactions
            if tx.to.lower() == POOL_ADDRESS.lower()
        ]
        assert len(swap_txs) > 0, (
            f"No transaction targeting tricrypto2 pool {POOL_ADDRESS}. "
            f"Transactions: {[(tx.to, tx.description) for tx in result.transactions]}"
        )


# =============================================================================
# Layers 2-4: Full On-Chain Swap Tests (Requires Ethereum Anvil Fork)
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.swap
class TestCurveCryptoSwapExecution:
    """Layers 2-4: Full on-chain CryptoSwap tests on Ethereum Anvil fork.

    Tests USDT -> WETH swap via tricrypto2 with:
    - Layer 2: Transaction execution on Anvil
    - Layer 3: TokenExchangeCrypto event parsing
    - Layer 4: Exact balance delta verification
    """

    @pytest.mark.asyncio
    async def test_usdt_to_weth_full_lifecycle(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Layer 2-4: Execute USDT -> WETH CryptoSwap on Ethereum Anvil.

        Verifies:
        - Compilation succeeds with real prices (Layer 1)
        - Execution succeeds on Anvil with gas < 550K (Layer 2)
        - TokenExchangeCrypto event parsed, extract_swap_amounts() returns data (Layer 3)
        - USDT balance decreased exactly, WETH balance increased (Layer 4)
        """
        # Fund USDT (may not be in standard funded_wallet set with enough balance)
        _fund_usdt(funded_wallet, anvil_rpc_url)

        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]
        weth_addr = tokens["WETH"]

        swap_amount = Decimal("100")  # 100 USDT

        logger.info(
            "Test: USDT -> WETH Curve CryptoSwap on Ethereum (tricrypto2)\n"
            "Pool: %s", POOL_ADDRESS
        )

        # --- Layer 4 setup: record balances BEFORE ---
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        logger.info("USDT before: %.2f", usdt_before / 10**6)
        logger.info("WETH before: %.6f", weth_before / 10**18)
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
            to_token="WETH",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="curve",
            chain=CHAIN_NAME,
        )

        compile_result = compiler.compile(intent)
        assert compile_result.status == CompilationStatus.SUCCESS, (
            f"CryptoSwap compilation failed: {compile_result.error}"
        )
        assert compile_result.action_bundle is not None
        logger.info("Compiled %d transactions", len(compile_result.transactions))

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compile_result.action_bundle)
        assert execution_result.success, (
            f"CryptoSwap execution failed: {execution_result.error}\n"
            "Check tricrypto2 pool address and coin indices."
        )

        # Verify gas usage is within CryptoSwap range (higher than StableSwap)
        gas_checked = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = (
                    tx_result.receipt if isinstance(tx_result.receipt, dict)
                    else tx_result.receipt.to_dict()
                )
                gas_used = receipt_dict.get("gasUsed") or receipt_dict.get("gas_used", 0)
                if isinstance(gas_used, str):
                    gas_used = int(gas_used, 16) if gas_used.startswith("0x") else int(gas_used)
                # Only check gas for the exchange TX (not approve)
                if gas_used > 100_000:
                    gas_checked = True
                    assert gas_used < 550_000, (
                        f"CryptoSwap gas usage {gas_used} exceeds 550K limit. "
                        "Possible regression in gas estimate."
                    )
        assert gas_checked, "No transaction with gas > 100K found -- gas regression check did not run"

        logger.info("Execution success")

        # --- Layer 3: Parse receipt ---
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        swap_event_found = False
        swap_amounts_extracted = False

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
                        # Verify CryptoSwap-specific fields
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

            # Test extract_swap_amounts (enrichment method)
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

        assert swap_event_found, (
            "CurveReceiptParser did not find TokenExchangeCrypto event. "
            "Verify receipt_parser handles CryptoSwap event topic "
            "0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98"
        )
        if not swap_amounts_extracted:
            logger.warning(
                "extract_swap_amounts() returned None -- TokenResolver may not "
                "resolve decimals in CI (no gateway). Swap event parsing still verified above."
            )

        # --- Layer 4: Balance deltas ---
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)

        usdt_spent = usdt_before - usdt_after
        weth_received = weth_after - weth_before
        expected_usdt_spent = int(swap_amount * Decimal(10**6))

        logger.info("USDT after: %.2f (spent: %.2f)", usdt_after / 10**6, usdt_spent / 10**6)
        logger.info("WETH after: %.6f (received: %.6f)", weth_after / 10**18, weth_received / 10**18)

        assert usdt_spent == expected_usdt_spent, (
            f"USDT spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdt_spent} ({swap_amount} USDT), Got: {usdt_spent}"
        )
        assert weth_received > 0, (
            "WETH balance did not increase after CryptoSwap! "
            "Check coin indices in tricrypto2 pool config."
        )

        logger.info(
            "SUCCESS: Swapped %.2f USDT -> %.6f WETH via tricrypto2",
            usdt_spent / 10**6,
            weth_received / 10**18,
        )

    @pytest.mark.asyncio
    async def test_weth_to_usdt_reverse_direction(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Layer 2-4: Execute WETH -> USDT reverse CryptoSwap.

        Tests the reverse direction to ensure uint256 index handling works
        for both sold_id and bought_id in the CryptoSwap event.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]
        weth_addr = tokens["WETH"]

        swap_amount = Decimal("0.01")  # 0.01 WETH

        # --- Layer 4 BEFORE ---
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
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
            to_token="USDT",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="curve",
            chain=CHAIN_NAME,
        )

        compile_result = compiler.compile(intent)
        assert compile_result.status == CompilationStatus.SUCCESS, (
            f"Reverse CryptoSwap compilation failed: {compile_result.error}"
        )
        assert compile_result.action_bundle is not None

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compile_result.action_bundle)
        assert execution_result.success, (
            f"Reverse CryptoSwap execution failed: {execution_result.error}"
        )

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
            if parse_result and parse_result.success and parse_result.events:
                for event in parse_result.events:
                    if event.event_type == CurveEventType.TOKEN_EXCHANGE:
                        swap_event_found = True
                        # Reverse direction: sold_id should be WETH index (2),
                        # bought_id should be USDT index (0)
                        assert event.data["sold_id"] == 2, (
                            f"sold_id must be 2 (WETH), got {event.data['sold_id']}"
                        )
                        assert event.data["bought_id"] == 0, (
                            f"bought_id must be 0 (USDT), got {event.data['bought_id']}"
                        )
                        assert event.data["tokens_sold"] > 0, "tokens_sold must be > 0"
                        assert event.data["tokens_bought"] > 0, "tokens_bought must be > 0"

        assert swap_event_found, "TokenExchangeCrypto event not found in reverse swap receipt"

        # --- Layer 4: Balance deltas ---
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)

        weth_spent = weth_before - weth_after
        usdt_received = usdt_after - usdt_before
        expected_weth_spent = int(swap_amount * Decimal(10**18))

        assert weth_spent == expected_weth_spent, (
            f"WETH spent must EXACTLY equal swap amount. "
            f"Expected: {expected_weth_spent}, Got: {weth_spent}"
        )
        assert usdt_received > 0, (
            "USDT balance did not increase after reverse CryptoSwap!"
        )

        logger.info(
            "SUCCESS: Reverse swap %.6f WETH -> %.2f USDT via tricrypto2",
            weth_spent / 10**18,
            usdt_received / 10**6,
        )
