"""4-layer intent tests for Uniswap V4 LP_CLOSE on Ethereum Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
closing V4 LP positions via PositionManager:
1. Open a position first (LP_OPEN as setup)
2. Create LPCloseIntent with position_id and protocol_params
3. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
4. Execute via ExecutionOrchestrator (full production pipeline)
5. Parse receipts using UniswapV4ReceiptParser (liquidity removed, tokens returned)
6. Verify balances changed correctly (tokens returned from pool)

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

To run:
    uv run pytest tests/intents/ethereum/test_uniswap_v4_lp_close.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "ethereum"

# WETH/USDC pool with 0.3% fee tier
LP_POOL = "WETH/USDC/3000"

# Small amounts for setup LP_OPEN
LP_AMOUNT_WETH = Decimal("0.01")
LP_AMOUNT_USDC = Decimal("25")
LP_RANGE_LOWER = Decimal("1000")
LP_RANGE_UPPER = Decimal("10000")

# =============================================================================
# Helper: Open a position (setup for close tests)
# =============================================================================


async def _open_v4_position(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> tuple[int, int, str, str]:
    """Open a V4 LP position and return (position_id, liquidity, currency0, currency1).

    Raises AssertionError if the setup LP_OPEN fails.
    """
    intent = LPOpenIntent(
        pool=LP_POOL,
        amount0=LP_AMOUNT_WETH,
        amount1=LP_AMOUNT_USDC,
        range_lower=LP_RANGE_LOWER,
        range_upper=LP_RANGE_UPPER,
        protocol="uniswap_v4",
        chain=CHAIN_NAME,
    )

    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
    )

    compilation_result = compiler.compile(intent)
    assert compilation_result.status.value == "SUCCESS", (
        f"Setup LP_OPEN compilation failed: {compilation_result.error}"
    )
    bundle = compilation_result.action_bundle
    assert bundle is not None

    execution_result = await orchestrator.execute(bundle)
    assert execution_result.success, f"Setup LP_OPEN execution failed: {execution_result.error}"

    # Extract position_id and liquidity from receipt
    parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
    position_id = None
    liquidity = None

    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            receipt_dict = tx_result.receipt.to_dict()
            pid = parser.extract_position_id(receipt_dict)
            if pid is not None:
                position_id = pid
            liq = parser.extract_liquidity(receipt_dict)
            if liq is not None:
                liquidity = liq

    assert position_id is not None, "Setup LP_OPEN must yield a position_id"
    assert liquidity is not None and liquidity > 0, "Setup LP_OPEN must yield positive liquidity"

    # Get currency addresses from bundle metadata
    token0 = bundle.metadata.get("token0", {})
    token1 = bundle.metadata.get("token1", {})
    currency0 = token0.get("address", "")
    currency1 = token1.get("address", "")

    assert currency0 and currency1, "Must extract currency addresses from bundle metadata"

    return position_id, liquidity, currency0, currency1


# =============================================================================
# LPCloseIntent Tests -- Uniswap V4 on Ethereum
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.lp
class TestUniswapV4LPCloseIntent:
    """Test Uniswap V4 LP_CLOSE using LPCloseIntent on Ethereum.

    These tests verify the full LP close flow:
    - First open a position (setup)
    - LPCloseIntent creation with position_id and protocol_params
    - IntentCompiler routes to UniswapV4Adapter.compile_lp_close_intent()
    - Transactions execute successfully on-chain via PositionManager
    - UniswapV4ReceiptParser correctly extracts close data
    - Balance changes match expected token returns
    """

    @pytest.mark.xfail(
        reason="V4 LP on Anvil fork may fail due to PositionManager event differences (VIB-2025)",
        strict=False,
    )
    @pytest.mark.asyncio
    async def test_lp_close_weth_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test full LP_OPEN -> LP_CLOSE lifecycle for WETH/USDC via V4.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> lp_close_data extracted
        4. Balance Deltas: WETH and USDC returned from pool
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]

        weth_decimals = get_token_decimals(web3, weth_addr)
        usdc_decimals = get_token_decimals(web3, usdc_addr)

        print(f"\n{'='*80}")
        print("Test: LP_CLOSE WETH/USDC via Uniswap V4 on Ethereum")
        print(f"{'='*80}")

        # Setup: Open a position first
        print("\n--- Setup: Opening LP position ---")
        position_id, liquidity, currency0, currency1 = await _open_v4_position(
            web3, funded_wallet, orchestrator, price_oracle,
        )
        print(f"Opened position: id={position_id}, liquidity={liquidity}")
        print(f"Currencies: {currency0[:10]}.../{currency1[:10]}...")

        # Record balances before close
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        print(f"\n--- Closing LP position ---")
        print(f"WETH before close: {format_token_amount(weth_before, weth_decimals)}")
        print(f"USDC before close: {format_token_amount(usdc_before, usdc_decimals)}")

        # Layer 1: Compilation
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            protocol_params={
                "liquidity": liquidity,
                "currency0": currency0,
                "currency1": currency1,
            },
        )

        print(f"Created LPCloseIntent: position_id={close_intent.position_id}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"LP_CLOSE compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        bundle = compilation_result.action_bundle
        print(f"ActionBundle created with {len(bundle.transactions)} transactions")

        # Layer 2: Execution
        print("\nExecuting LP_CLOSE via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(bundle)

        assert execution_result.success, f"LP_CLOSE execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Receipt Parsing
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        lp_close_data = None

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                close_data = parser.extract_lp_close_data(receipt_dict)
                if close_data is not None:
                    lp_close_data = close_data
                    print(f"  LP Close Data:")
                    print(f"    amount0_collected: {close_data.amount0_collected}")
                    print(f"    amount1_collected: {close_data.amount1_collected}")
                    print(f"    liquidity_removed: {close_data.liquidity_removed}")

        assert lp_close_data is not None, "Must extract LP close data from receipt"
        assert lp_close_data.liquidity_removed is not None and lp_close_data.liquidity_removed > 0, (
            "Must remove positive liquidity"
        )

        # Layer 4: Balance Deltas
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        weth_received = weth_after - weth_before
        usdc_received = usdc_after - usdc_before

        print("\n--- Balance Deltas ---")
        print(f"WETH received: {format_token_amount(weth_received, weth_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, usdc_decimals)}")

        # At least one token must have been returned
        assert weth_received > 0 or usdc_received > 0, (
            "Must receive at least one token back from LP_CLOSE"
        )

        print(f"\nPosition {position_id} successfully closed")
        print("\nALL 4 LAYERS PASSED")

    @pytest.mark.asyncio
    async def test_lp_close_without_liquidity_fails_compilation(
        self,
        web3: Web3,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
    ):
        """Test that LP_CLOSE without liquidity in protocol_params fails at compilation.

        V4 LP_CLOSE requires on-chain position data (liquidity, currencies).
        """
        print(f"\n{'='*80}")
        print("Test: LP_CLOSE without liquidity (should fail compilation)")
        print(f"{'='*80}")

        close_intent = LPCloseIntent(
            position_id="99999",
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            # No protocol_params -- missing liquidity
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "FAILED", (
            "Compilation should fail without liquidity in protocol_params"
        )
        assert "liquidity" in compilation_result.error.lower(), (
            f"Error should mention liquidity requirement, got: {compilation_result.error}"
        )
        print(f"Compilation failed as expected: {compilation_result.error}")
        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
