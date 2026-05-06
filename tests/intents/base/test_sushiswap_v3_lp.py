"""Production-grade LP Intent tests for SushiSwap V3 on Base.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Opening concentrated liquidity positions
- LPCloseIntent: Closing positions with various states

LP Close test cases:
  #1: Position has liquidity + fees (normal close)
  #2: Position has no liquidity and no fees (already decreased + collected)
  #3: Position has no liquidity but owed tokens (decreased but not collected)

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/base/test_sushiswap_v3_lp.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.core.contracts import SUSHISWAP_V3, get_address
from almanak.framework.connectors.sushiswap_v3.receipt_parser import SushiSwapV3ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import (
    IntentCompiler,
    LPCloseIntent,
    LPOpenIntent,
)
from tests.intents._lp_setup_helpers import (
    collect_all_tokens,
    decrease_all_liquidity,
    query_position_liquidity,
)
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "base"
POSITION_MANAGER = get_address(SUSHISWAP_V3, CHAIN_NAME, "position_manager")
MAX_UINT128 = 2**128 - 1

# Pool: WETH/USDC 0.3% fee tier
# After sorting by address: token0=WETH (0x4200...), token1=USDC (0x8335...)
# So amount0=WETH, amount1=USDC, range is in USDC-per-WETH terms
POOL = "WETH/USDC/3000"
LP_AMOUNT_WETH = Decimal("0.2")  # amount0 (WETH after sorting)
LP_AMOUNT_USDC = Decimal("500")  # amount1 (USDC after sorting)

# Wide price range in USDC-per-WETH terms to ensure both tokens are deposited
# range_lower=200   -> ETH at $200
# range_upper=20000 -> ETH at $20,000
RANGE_LOWER = Decimal("200")
RANGE_UPPER = Decimal("20000")


# =============================================================================
# Helpers (shared)
# =============================================================================
#
# ``query_position_liquidity``, ``decrease_all_liquidity``, and
# ``collect_all_tokens`` live in ``tests/intents/_lp_setup_helpers.py`` so
# the no-liquidity edge-case tests route their setup tx through whatever
# orchestrator the test holds — EOA-signed under ``ExecutionOrchestrator``,
# ``execTransactionWithRole``-wrapped under ``ZodiacOrchestrator``.


async def _open_position_via_intent(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> int:
    """Open an LP position via LPOpenIntent and return the position token ID."""
    intent = LPOpenIntent(
        pool=POOL,
        amount0=LP_AMOUNT_WETH,
        amount1=LP_AMOUNT_USDC,
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        protocol="sushiswap_v3",
        chain=CHAIN_NAME,
    )

    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    compilation_result = compiler.compile(intent)
    assert compilation_result.status.value == "SUCCESS", f"LP Open compilation failed: {compilation_result.error}"
    assert compilation_result.action_bundle is not None

    execution_result = await orchestrator.execute(compilation_result.action_bundle)
    assert execution_result.success, f"LP Open execution failed: {execution_result.error}"

    # Extract position ID from mint receipt
    parser = SushiSwapV3ReceiptParser(chain=CHAIN_NAME)
    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            pos_id = parser.extract_position_id(tx_result.receipt.to_dict())
            if pos_id is not None:
                return pos_id

    raise AssertionError("Failed to extract position ID from LP Open receipt")


# =============================================================================
# Fixtures
# =============================================================================


# =============================================================================
# LPOpenIntent Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.lp
class TestSushiSwapV3LPOpenIntent:
    """Test SushiSwap V3 LP Open using LPOpenIntent.

    Verifies the full Intent flow:
    - LPOpenIntent creation with pool, amounts, and price range
    - IntentCompiler generates correct NonfungiblePositionManager mint TX
    - Transactions execute successfully on-chain
    - Position NFT is minted and has liquidity
    - Balance changes are correct
    """

    @pytest.mark.asyncio
    async def test_lp_open_weth_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test opening a WETH/USDC LP position using LPOpenIntent.

        Flow:
        1. Record balances BEFORE
        2. Create LPOpenIntent for WETH/USDC pool
        3. Compile to ActionBundle using IntentCompiler
        4. Execute via ExecutionOrchestrator
        5. Parse receipts - extract position ID
        6. Verify on-chain position liquidity
        7. Verify balance changes
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]

        weth_decimals = get_token_decimals(web3, weth_addr)
        usdc_decimals = get_token_decimals(web3, usdc_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Open WETH/USDC via LPOpenIntent (SushiSwap V3)")
        print(f"{'=' * 80}")
        print(f"Pool: {POOL}")
        print(f"Amount WETH (token0): {LP_AMOUNT_WETH}")
        print(f"Amount USDC (token1): {LP_AMOUNT_USDC}")
        print(f"Range: [{RANGE_LOWER} - {RANGE_UPPER}] USDC per WETH")

        # 1. Record balances BEFORE
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # 2. Create LPOpenIntent
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_USDC,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="sushiswap_v3",
            chain=CHAIN_NAME,
        )

        # 3. Compile
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        print("\nCompiling LPOpenIntent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # 4. Execute
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # 5. Parse receipts - extract position ID
        parser = SushiSwapV3ReceiptParser(chain=CHAIN_NAME)
        position_id = None
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                pos_id = parser.extract_position_id(tx_result.receipt.to_dict())
                if pos_id is not None:
                    position_id = pos_id

                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                if parse_result.success:
                    print(f"  Events parsed: {len(parse_result.events)}")

        assert position_id is not None, "Must extract position ID from mint receipt"
        print(f"\nPosition ID: {position_id}")

        # 6. Verify on-chain position has liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have positive liquidity, got {liquidity}"
        print(f"On-chain liquidity: {liquidity}")

        # 7. Verify balance changes
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_spent = usdc_before - usdc_after

        print(f"\nWETH spent: {format_token_amount(weth_spent, weth_decimals)}")
        print(f"USDC spent: {format_token_amount(usdc_spent, usdc_decimals)}")

        # At least one token must have been deposited
        assert weth_spent > 0 or usdc_spent > 0, "Must deposit at least one token into LP"

        # Amounts spent must not exceed desired amounts
        expected_weth_max = int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))
        expected_usdc_max = int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        assert weth_spent <= expected_weth_max, f"WETH spent ({weth_spent}) exceeds desired ({expected_weth_max})"
        assert usdc_spent <= expected_usdc_max, f"USDC spent ({usdc_spent}) exceeds desired ({expected_usdc_max})"

        print("\nALL CHECKS PASSED")


# =============================================================================
# LPCloseIntent Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.lp
class TestSushiSwapV3LPCloseIntent:
    """Test SushiSwap V3 LP Close using LPCloseIntent.

    Test cases:
    #1: Position has liquidity (normal LP close)
    #2: Position has no liquidity and no owed tokens (already decreased + collected)
    #3: Position has no liquidity but has owed tokens (decreased but not collected)
    """

    @pytest.mark.asyncio
    async def test_lp_close_position_with_liquidity(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test #1: Close position that has liquidity.

        Flow:
        1. Open LP position via LPOpenIntent
        2. Verify position has liquidity on-chain
        3. Record balances BEFORE close
        4. Close via LPCloseIntent
        5. Parse receipts (LP close data)
        6. Verify tokens returned to wallet
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]
        weth_decimals = get_token_decimals(web3, weth_addr)
        usdc_decimals = get_token_decimals(web3, usdc_addr)

        print(f"\n{'=' * 80}")
        print("Test #1: LP Close - Position with Liquidity (SushiSwap V3)")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Verify it has liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have liquidity before close, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 3. Record balances BEFORE close
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)

        # 4. Close via LPCloseIntent
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="sushiswap_v3",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        print("\nCompiling LPCloseIntent...")
        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "SUCCESS", f"LP Close compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        print(f"ActionBundle: {len(compilation_result.action_bundle.transactions)} transactions")

        print("Executing LP Close...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"LP Close execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions")

        # 5. Parse receipts
        parser = SushiSwapV3ReceiptParser(chain=CHAIN_NAME)
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                lp_close_data = parser.extract_lp_close_data(tx_result.receipt.to_dict())
                if lp_close_data:
                    print(
                        f"  LP Close data: amount0_collected={lp_close_data.amount0_collected}, "
                        f"amount1_collected={lp_close_data.amount1_collected}"
                    )

        # 6. Verify tokens returned
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)

        weth_returned = weth_after_close - weth_before_close
        usdc_returned = usdc_after_close - usdc_before_close

        print(f"\nWETH returned: {format_token_amount(weth_returned, weth_decimals)}")
        print(f"USDC returned: {format_token_amount(usdc_returned, usdc_decimals)}")

        # At least one token must be returned (was deposited in LP)
        assert weth_returned > 0 or usdc_returned > 0, (
            f"Must receive tokens back when closing position. "
            f"WETH returned: {weth_returned}, USDC returned: {usdc_returned}"
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_lp_close_position_no_liquidity_no_fees(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        test_private_key: str,
    ):
        """Test #2: Close position with no liquidity and no owed tokens.

        This tests the edge case where a position has already had its liquidity
        removed and tokens collected externally (e.g., via direct contract calls).
        The LPCloseIntent should handle this gracefully.

        Flow:
        1. Open LP position via LPOpenIntent
        2. Decrease all liquidity via direct contract call
        3. Collect all owed tokens via direct contract call
        4. Verify position has 0 liquidity
        5. Record balances BEFORE close
        6. Close via LPCloseIntent
        7. Verify ERC-20 balances unchanged (nothing to collect)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]

        print(f"\n{'=' * 80}")
        print("Test #2: LP Close - No Liquidity, No Owed Tokens (SushiSwap V3)")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Decrease all liquidity directly
        await decrease_all_liquidity(
            web3, orchestrator,
            chain=CHAIN_NAME, protocol="sushiswap_v3",
            position_manager=POSITION_MANAGER, token_id=position_id,
        )
        print("Decreased all liquidity via direct call")

        # 3. Collect all owed tokens directly
        await collect_all_tokens(
            web3, orchestrator,
            chain=CHAIN_NAME, protocol="sushiswap_v3",
            position_manager=POSITION_MANAGER, token_id=position_id,
            recipient=funded_wallet,
        )
        print("Collected all owed tokens via direct call")

        # 4. Verify 0 liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity == 0, f"Expected 0 liquidity after decrease, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 5. Record balances BEFORE close
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)

        # 6. Close via LPCloseIntent
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="sushiswap_v3",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        print("\nCompiling LPCloseIntent for empty position...")
        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "SUCCESS", f"LP Close compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        print("Executing LP Close on empty position...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, "LP Close on empty position is a no-op success (VIB-3644)"
        assert compilation_result.action_bundle.metadata.get("no_op") is True, "Empty LP_CLOSE must carry no_op metadata"
        assert compilation_result.action_bundle.transactions == [], "No-op bundle must have 0 transactions"
        assert len(execution_result.transaction_results) == 0, "No-op execution must produce 0 executed transactions"

        # 7. Verify ERC-20 balances unchanged (nothing to collect)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)

        weth_delta = weth_after_close - weth_before_close
        usdc_delta = usdc_after_close - usdc_before_close

        assert weth_delta == 0, f"WETH balance should be unchanged for empty position, got delta: {weth_delta}"
        assert usdc_delta == 0, f"USDC balance should be unchanged for empty position, got delta: {usdc_delta}"

        print(f"WETH delta: {weth_delta}")
        print(f"USDC delta: {usdc_delta}")
        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_lp_close_position_no_liquidity_but_owed_tokens(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        test_private_key: str,
    ):
        """Test #3: Close position with no liquidity but uncollected owed tokens.

        After decreaseLiquidity, the principal tokens become "owed" to the
        position but are not yet transferred. The collect step in LPCloseIntent
        should retrieve these owed tokens.

        Flow:
        1. Open LP position via LPOpenIntent
        2. Decrease all liquidity via direct contract call (principal becomes owed)
        3. Do NOT collect - tokens remain owed
        4. Verify position has 0 liquidity
        5. Record balances BEFORE close
        6. Close via LPCloseIntent (should collect owed tokens and burn)
        7. Verify tokens were collected
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]
        weth_decimals = get_token_decimals(web3, weth_addr)
        usdc_decimals = get_token_decimals(web3, usdc_addr)

        print(f"\n{'=' * 80}")
        print("Test #3: LP Close - No Liquidity, But Owed Tokens (SushiSwap V3)")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # 2. Decrease all liquidity (principal becomes owed but not collected).
        #    decreaseLiquidity itself moves the LP principal into tokensOwed0/1,
        #    so no swap-to-generate-fees step is required to populate owed tokens.
        await decrease_all_liquidity(
            web3, orchestrator,
            chain=CHAIN_NAME, protocol="sushiswap_v3",
            position_manager=POSITION_MANAGER, token_id=position_id,
        )
        print("Decreased all liquidity via direct call (tokens now owed)")

        # 3. Do NOT collect - leave tokens owed

        # 4. Verify 0 liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity == 0, f"Expected 0 liquidity after decrease, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 5. Record balances BEFORE close
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)

        # 6. Close via LPCloseIntent (should collect owed tokens)
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="sushiswap_v3",
            chain=CHAIN_NAME,
        )

        print("\nCompiling LPCloseIntent for position with owed tokens...")
        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "SUCCESS", f"LP Close compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        print("Executing LP Close...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, (
            f"LP Close should succeed for position with owed tokens. Error: {execution_result.error}"
        )

        # Parse receipts
        parser = SushiSwapV3ReceiptParser(chain=CHAIN_NAME)
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                lp_close_data = parser.extract_lp_close_data(tx_result.receipt.to_dict())
                if lp_close_data:
                    print(
                        f"  LP Close data: amount0_collected={lp_close_data.amount0_collected}, "
                        f"amount1_collected={lp_close_data.amount1_collected}"
                    )

        # 7. Verify tokens were collected (owed tokens from decrease)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)

        weth_collected = weth_after_close - weth_before_close
        usdc_collected = usdc_after_close - usdc_before_close

        print(f"\nWETH collected: {format_token_amount(weth_collected, weth_decimals)}")
        print(f"USDC collected: {format_token_amount(usdc_collected, usdc_decimals)}")

        # At least one token must be collected (there were owed tokens from the decrease)
        assert weth_collected > 0 or usdc_collected > 0, (
            f"Must collect owed tokens from decreased position. "
            f"WETH collected: {weth_collected}, USDC collected: {usdc_collected}"
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
