"""Curve crvUSD/USDC swap intent tests on Optimism (VIB-1587).

Tests the SwapIntent -> Compile -> Execute -> Parse -> Verify flow for
Curve Finance's crvUSD/USDC StableSwap NG pool on Optimism.

Background:
    The existing CURVE_POOLS["optimism"]["3pool"] uses USDC.e (bridged),
    not native USDC. Strategy authors on Optimism with native USDC had no
    Curve pool to swap into crvUSD. VIB-1587 adds the crvusd_usdc pool
    (StableSwap NG, 0x03771e24...) which contains native USDC.

    Pool: Curve "crvUSDC Pool" (StableSwap NG)
    Address: 0x03771e24b7c9172d163bf447490b142a15be3485
    Coins[0]: USDC  (native) = 0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85
    Coins[1]: crvUSD         = 0xC52D7F23a2e460248Db6eE192Cb23dD12bDDCbf6

To run (requires Optimism Anvil fork on port 8545):
    uv run pytest tests/intents/optimism/test_curve_crvusd_usdc_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.curve.adapter import CURVE_POOLS
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import SwapIntent
from tests.intents.conftest import CHAIN_CONFIGS, get_token_balance

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "optimism"
POOL_KEY = "crvusd_usdc"

# Expected pool parameters
EXPECTED_POOL_ADDRESS = "0x03771e24b7c9172d163bf447490b142a15be3485"
USDC_ADDRESS = "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85"  # native USDC on Optimism
CRVUSD_ADDRESS = "0xC52D7F23a2e460248Db6eE192Cb23dD12bDDCbf6"  # crvUSD on Optimism

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
ANVIL_URL = "http://localhost:8545"


def _is_anvil_running(url: str = ANVIL_URL) -> bool:
    try:
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": (2, 5)}))
        return w3.is_connected()
    except Exception:
        return False


# =============================================================================
# Layer 1a: Pool Configuration Tests (No Anvil Required)
# =============================================================================


class TestCurveOptimismPoolConfig:
    """Verify crvusd_usdc pool is correctly configured in CURVE_POOLS."""

    def test_optimism_in_curve_pools(self):
        """'optimism' chain must have a CURVE_POOLS entry."""
        assert "optimism" in CURVE_POOLS, (
            "'optimism' not found in CURVE_POOLS. "
            "Add CURVE_POOLS['optimism'] with at least the crvusd_usdc pool."
        )

    def test_crvusd_usdc_pool_present(self):
        """crvusd_usdc pool must be in CURVE_POOLS['optimism']."""
        assert POOL_KEY in CURVE_POOLS.get("optimism", {}), (
            f"'{POOL_KEY}' not found in CURVE_POOLS['optimism']. "
            f"Found: {list(CURVE_POOLS.get('optimism', {}).keys())}"
        )

    def test_pool_address_correct(self):
        """Pool address must match deployed StableSwap NG contract."""
        pool = CURVE_POOLS["optimism"][POOL_KEY]
        assert pool["address"].lower() == EXPECTED_POOL_ADDRESS.lower(), (
            f"Pool address mismatch: got {pool['address']}, "
            f"expected {EXPECTED_POOL_ADDRESS}"
        )

    def test_pool_contains_native_usdc(self):
        """Pool coin_addresses must include native USDC (not USDC.e)."""
        pool = CURVE_POOLS["optimism"][POOL_KEY]
        addresses_lower = [a.lower() for a in pool["coin_addresses"]]
        assert USDC_ADDRESS.lower() in addresses_lower, (
            f"Native USDC ({USDC_ADDRESS}) not found in pool coin_addresses: "
            f"{pool['coin_addresses']}. Ensure this is native USDC, not USDC.e."
        )

    def test_pool_contains_crvusd(self):
        """Pool coin_addresses must include crvUSD."""
        pool = CURVE_POOLS["optimism"][POOL_KEY]
        addresses_lower = [a.lower() for a in pool["coin_addresses"]]
        assert CRVUSD_ADDRESS.lower() in addresses_lower, (
            f"crvUSD ({CRVUSD_ADDRESS}) not found in pool coin_addresses: "
            f"{pool['coin_addresses']}"
        )

    def test_pool_is_stableswap_type(self):
        """Pool type must be 'stableswap' for StableSwap NG."""
        pool = CURVE_POOLS["optimism"][POOL_KEY]
        assert pool["pool_type"] == "stableswap"

    def test_pool_n_coins_is_2(self):
        """crvusd_usdc is a 2-coin pool."""
        pool = CURVE_POOLS["optimism"][POOL_KEY]
        assert pool["n_coins"] == 2
        assert len(pool["coin_addresses"]) == 2
        assert len(pool["coins"]) == 2

    def test_lp_token_equals_pool_address(self):
        """StableSwap NG: LP token IS the pool contract address."""
        pool = CURVE_POOLS["optimism"][POOL_KEY]
        assert pool["lp_token"].lower() == pool["address"].lower(), (
            "For StableSwap NG pools, lp_token must equal pool address"
        )

    def test_no_bridged_usdc_in_native_usdc_pool(self):
        """Pool must NOT contain USDC.e (bridged) — only native USDC."""
        bridged_usdc_e = "0x7F5c764cBc14f9669B88837ca1490cca17c31607"
        pool = CURVE_POOLS["optimism"][POOL_KEY]
        addresses_lower = [a.lower() for a in pool["coin_addresses"]]
        assert bridged_usdc_e.lower() not in addresses_lower, (
            f"Pool contains USDC.e (bridged) instead of native USDC! "
            f"Expected native USDC at {USDC_ADDRESS}"
        )


# =============================================================================
# Layer 1b: SwapIntent Compilation Tests (No Anvil Required)
# =============================================================================


class TestCurveOptimismSwapCompilation:
    """Layer 1: Verify SwapIntent compiles correctly using the new crvusd_usdc pool."""

    def _make_compiler(self) -> IntentCompiler:
        return IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    def test_usdc_to_crvusd_swap_compiles(self):
        """SwapIntent USDC -> crvUSD on Optimism must compile successfully."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="crvUSD",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"USDC -> crvUSD swap compilation failed: {result.error}\n"
            "Ensure CURVE_POOLS['optimism']['crvusd_usdc'] is correctly configured."
        )
        assert result.action_bundle is not None

    def test_crvusd_to_usdc_swap_compiles(self):
        """SwapIntent crvUSD -> USDC on Optimism must compile successfully."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="crvUSD",
            to_token="USDC",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"crvUSD -> USDC swap compilation failed: {result.error}"
        )

    def test_compiled_swap_targets_correct_pool(self):
        """Compiled transactions must target the crvusd_usdc pool address."""
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="crvUSD",
            amount=Decimal("50"),
            max_slippage=Decimal("0.01"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS

        # Last transaction should be the exchange call targeting the pool
        swap_txs = [tx for tx in result.transactions if "exchange" in tx.description.lower()
                    or tx.to.lower() == EXPECTED_POOL_ADDRESS.lower()]
        assert len(swap_txs) > 0, (
            f"No exchange transaction targeting pool {EXPECTED_POOL_ADDRESS} found. "
            f"Transactions: {[(tx.to, tx.description) for tx in result.transactions]}"
        )

    def test_native_usdc_not_usdc_e(self):
        """Compiled approve must use native USDC address (not USDC.e)."""
        bridged_usdc_e = "0x7F5c764cBc14f9669B88837ca1490cca17c31607".lower()
        compiler = self._make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="crvUSD",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS

        # Check that approve TX targets native USDC, not USDC.e
        approve_txs = [tx for tx in result.transactions if "approve" in tx.description.lower()]
        if approve_txs:
            approve_target = approve_txs[0].to.lower()
            assert approve_target == USDC_ADDRESS.lower(), (
                f"Approve targets wrong token! Got {approve_target}, "
                f"expected native USDC {USDC_ADDRESS}. "
                f"Bridged USDC.e ({bridged_usdc_e}) would be wrong."
            )


# =============================================================================
# Layers 2-4: Full On-Chain Swap Test (Requires Optimism Anvil Fork)
# =============================================================================


@pytest.mark.optimism
@pytest.mark.integration
@pytest.mark.skipif(not _is_anvil_running(), reason="Anvil not running (Optimism fork required)")
class TestCurveOptimismSwapOnAnvil:
    """Layers 2-4: Full on-chain USDC -> crvUSD swap test on Optimism Anvil fork.

    Requires Anvil running on port 8545 as an Optimism mainnet fork.
    Start with: anvil --fork-url https://mainnet.optimism.io --port 8545
    """

    @pytest.mark.asyncio
    async def test_usdc_to_crvusd_swap_full_lifecycle(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator,
        price_oracle: dict,
    ):
        """Layer 2-4: Execute USDC -> crvUSD swap on Optimism Anvil.

        Verifies:
        - Compilation succeeds (Layer 1)
        - Execution succeeds on Anvil (Layer 2)
        - Receipt parsed correctly (Layer 3)
        - USDC balance decreased, crvUSD balance increased (Layer 4)
        """
        from almanak.framework.connectors.curve.receipt_parser import CurveReceiptParser
        from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig

        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_address = tokens["USDC"]
        crvusd_address = CRVUSD_ADDRESS

        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'='*80}")
        print("Test: USDC -> crvUSD Curve Swap on Optimism (crvusd_usdc pool)")
        print(f"Pool: {EXPECTED_POOL_ADDRESS}")
        print(f"{'='*80}")

        # --- Layer 4 setup: record balances before ---
        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)
        crvusd_before = get_token_balance(web3, crvusd_address, funded_wallet)
        print(f"USDC before: {usdc_before / 10**6:.2f}")
        print(f"crvUSD before: {crvusd_before / 10**18:.6f}")
        assert usdc_before > 0, "Test wallet has no USDC -- funding failed"

        # --- Layer 1: Compile ---
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="crvUSD",
            amount=swap_amount,
            max_slippage=Decimal("0.02"),
            protocol="curve",
            chain=CHAIN_NAME,
        )

        compile_result = compiler.compile(intent)
        assert compile_result.status == CompilationStatus.SUCCESS, (
            f"Compilation failed: {compile_result.error}"
        )
        print(f"Compiled {len(compile_result.transactions)} transactions")

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compile_result.action_bundle)
        assert execution_result.success, (
            f"Execution failed: {execution_result.error}\n"
            "Check pool address and coin indices are correct."
        )
        print(f"Execution success: {execution_result.success}")

        # --- Layer 3: Parse receipt ---
        parser = CurveReceiptParser()
        parsed_swap = False
        if execution_result.transaction_results:
            for tx_result in execution_result.transaction_results:
                if hasattr(tx_result, "receipt") and tx_result.receipt:
                    receipt_dict = tx_result.receipt if isinstance(tx_result.receipt, dict) else tx_result.receipt.to_dict()
                    parsed = parser.parse_receipt(receipt_dict)
                    assert parsed is not None, "CurveReceiptParser returned None"
                    if parsed and hasattr(parsed, "events") and parsed.events:
                        swap_events = [e for e in parsed.events
                                       if "TokenExchange" in str(type(e).__name__)]
                        if swap_events:
                            parsed_swap = True
                            print(f"Parsed swap event: {swap_events[0]}")
        assert parsed_swap, (
            "CurveReceiptParser did not find any TokenExchange events. "
            "Verify receipt_parser handles StableSwap NG pools."
        )

        # --- Layer 4: Balance deltas ---
        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)
        crvusd_after = get_token_balance(web3, crvusd_address, funded_wallet)

        amount_spent = usdc_before - usdc_after
        amount_received = crvusd_after - crvusd_before
        expected_spent = int(swap_amount * Decimal(10**6))

        print(f"USDC after: {usdc_after / 10**6:.2f} (spent: {amount_spent / 10**6:.2f})")
        print(f"crvUSD after: {crvusd_after / 10**18:.6f} (received: {amount_received / 10**18:.6f})")

        assert amount_spent == expected_spent, (
            f"USDC spent must EXACTLY equal swap amount. "
            f"Expected: {expected_spent} ({swap_amount} USDC), Got: {amount_spent}"
        )
        assert amount_received > 0, (
            "crvUSD balance did not increase after swap! "
            "Check coin indices in CURVE_POOLS['optimism']['crvusd_usdc']."
        )

        print(f"\nSUCCESS: Swapped {amount_spent / 10**6:.2f} USDC -> {amount_received / 10**18:.4f} crvUSD")
        print(f"Effective rate: {(amount_received / 10**18) / (amount_spent / 10**6):.6f} crvUSD per USDC")
