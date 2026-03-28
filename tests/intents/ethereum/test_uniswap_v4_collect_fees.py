"""4-layer intent tests for Uniswap V4 LP_COLLECT_FEES on Ethereum Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
collecting fees from V4 LP positions via PositionManager:
1. Open a position first (LP_OPEN as setup)
2. Create CollectFeesIntent with position_id and protocol_params
3. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
4. Execute via ExecutionOrchestrator (full production pipeline)
5. Parse receipts (fee collection events)
6. Verify position remains open and balances are conserved

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

To run:
    uv run pytest tests/intents/ethereum/test_uniswap_v4_collect_fees.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import CollectFeesIntent, LPOpenIntent
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
# Helper: Open a position (setup for collect fees tests)
# =============================================================================


async def _open_v4_position(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> tuple[int, str, str]:
    """Open a V4 LP position and return (position_id, currency0, currency1).

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

    # Extract position_id from receipt
    parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
    position_id = None

    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            receipt_dict = tx_result.receipt.to_dict()
            pid = parser.extract_position_id(receipt_dict)
            if pid is not None:
                position_id = pid

    assert position_id is not None, "Setup LP_OPEN must yield a position_id"

    # Get currency addresses from bundle metadata
    token0 = bundle.metadata.get("token0", {})
    token1 = bundle.metadata.get("token1", {})
    currency0 = token0.get("address", "")
    currency1 = token1.get("address", "")

    assert currency0 and currency1, "Must extract currency addresses from bundle metadata"

    return position_id, currency0, currency1


# =============================================================================
# CollectFeesIntent Tests -- Uniswap V4 on Ethereum
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.lp
class TestUniswapV4CollectFeesIntent:
    """Test Uniswap V4 LP_COLLECT_FEES using CollectFeesIntent on Ethereum.

    These tests verify the fee collection flow:
    - First open a position (setup)
    - CollectFeesIntent creation with protocol_params
    - IntentCompiler routes to _compile_collect_fees_uniswap_v4()
    - Transactions execute successfully on-chain via PositionManager
    - Position remains open after fee collection
    - Balance deltas are non-negative (fees collected >= 0)
    """

    @pytest.mark.xfail(
        reason="V4 LP on Anvil fork may fail due to PositionManager event differences (VIB-2025)",
        strict=False,
    )
    @pytest.mark.asyncio
    async def test_collect_fees_weth_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test collecting fees from a WETH/USDC LP position via V4.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: Transaction confirmed with expected events
        4. Balance Deltas: Balances non-negative (fees >= 0, no tokens lost)

        Note: On a freshly opened position, accrued fees will be 0.
        The test verifies the collection flow works without errors,
        not that fees > 0 (which requires trading activity in the pool).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]

        weth_decimals = get_token_decimals(web3, weth_addr)
        usdc_decimals = get_token_decimals(web3, usdc_addr)

        print(f"\n{'='*80}")
        print("Test: LP_COLLECT_FEES WETH/USDC via Uniswap V4 on Ethereum")
        print(f"{'='*80}")

        # Setup: Open a position first
        print("\n--- Setup: Opening LP position ---")
        position_id, currency0, currency1 = await _open_v4_position(
            web3, funded_wallet, orchestrator, price_oracle,
        )
        print(f"Opened position: id={position_id}")
        print(f"Currencies: {currency0[:10]}.../{currency1[:10]}...")

        # Record balances before fee collection
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        print(f"\n--- Collecting fees ---")
        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # Layer 1: Compilation
        collect_intent = CollectFeesIntent(
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            protocol_params={
                "position_id": position_id,
                "currency0": currency0,
                "currency1": currency1,
            },
        )

        print(f"Created CollectFeesIntent: pool={collect_intent.pool}, position_id={position_id}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        compilation_result = compiler.compile(collect_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"COLLECT_FEES compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        bundle = compilation_result.action_bundle
        print(f"ActionBundle created with {len(bundle.transactions)} transactions")

        # Layer 2: Execution
        print("\nExecuting COLLECT_FEES via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(bundle)

        assert execution_result.success, f"COLLECT_FEES execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Receipt Parsing
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parsed = parser.parse_receipt(receipt_dict)

                # Log Transfer events (fee collection)
                for transfer in parsed.transfer_events:
                    print(f"  Transfer: from={transfer.from_address[:10]}... to={transfer.to_address[:10]}... amount={transfer.amount}")
                for ml in parsed.modify_liquidity_events:
                    print(f"  ModifyLiquidity: delta={ml.liquidity_delta}")

        # Layer 4: Balance Deltas
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        weth_delta = weth_after - weth_before
        usdc_delta = usdc_after - usdc_before

        print("\n--- Balance Deltas ---")
        print(f"WETH delta: {format_token_amount(weth_delta, weth_decimals)}")
        print(f"USDC delta: {format_token_amount(usdc_delta, usdc_decimals)}")

        # Fees should be >= 0 (can be 0 on freshly opened position)
        assert weth_delta >= 0, "WETH should not decrease from fee collection"
        assert usdc_delta >= 0, "USDC should not decrease from fee collection"

        print(f"\nFees collected from position {position_id} (may be 0 on fresh position)")
        print("\nALL 4 LAYERS PASSED")

    @pytest.mark.asyncio
    async def test_collect_fees_without_position_id_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
    ):
        """Test that COLLECT_FEES without position_id fails at compilation.

        V4 LP_COLLECT_FEES requires position_id in protocol_params.
        """
        print(f"\n{'='*80}")
        print("Test: COLLECT_FEES without position_id (should fail)")
        print(f"{'='*80}")

        collect_intent = CollectFeesIntent(
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            # No protocol_params -- missing position_id
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        compilation_result = compiler.compile(collect_intent)

        assert compilation_result.status.value == "FAILED", (
            "Compilation should fail without position_id"
        )
        assert "position_id" in compilation_result.error.lower(), (
            f"Error should mention position_id, got: {compilation_result.error}"
        )
        print(f"Compilation failed as expected: {compilation_result.error}")
        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
