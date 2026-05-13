"""Production-grade LP Intent tests for SushiSwap V3 on BNB Chain.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Opening concentrated liquidity positions
- LPCloseIntent: Closing positions with various states

LP Close test cases:
  #1: Position has liquidity + fees (normal close)
  #2: Position has no liquidity and no fees (already decreased + collected)
  #3: Position has no liquidity but owed tokens (decreased but not collected)

Note: USDT/WBNB is used instead of USDC/WBNB because Uniswap V3 on BNB chain
does not have USDC/WBNB pools with meaningful liquidity. PancakeSwap V3 is the
dominant DEX for USDC swaps on BNB chain.

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/bnb/test_sushiswap_v3_lp.py -v -s
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
    SwapIntent,
)
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType
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

CHAIN_NAME = "bsc"
POSITION_MANAGER = get_address(SUSHISWAP_V3, "bsc", "position_manager")
MAX_UINT128 = 2**128 - 1

# Pool: USDT/WBNB 0.3% fee tier
# After sorting by address: token0=USDT (0x55d3...), token1=WBNB (0xbb4C...)
# So amount0=USDT, amount1=WBNB, range is in WBNB-per-USDT terms
POOL = "USDT/WBNB/3000"
LP_AMOUNT_USDT = Decimal("500")  # amount0 (USDT after sorting)
LP_AMOUNT_WBNB = Decimal("1.0")  # amount1 (WBNB after sorting)

# Wide price range in WBNB-per-USDT terms to ensure both tokens are deposited
# range_lower=0.0005 -> BNB at ~$2,000
# range_upper=0.05   -> BNB at ~$20
RANGE_LOWER = Decimal("0.0005")
RANGE_UPPER = Decimal("0.05")


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
        amount0=LP_AMOUNT_USDT,
        amount1=LP_AMOUNT_WBNB,
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


@pytest.mark.bnb
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

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_usdt_wbnb(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test opening a USDT/WBNB LP position using LPOpenIntent.

        Flow:
        1. Record balances BEFORE
        2. Create LPOpenIntent for USDT/WBNB pool
        3. Compile to ActionBundle using IntentCompiler
        4. Execute via ExecutionOrchestrator
        5. Parse receipts - extract position ID
        6. Verify on-chain position liquidity
        7. Verify balance changes
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]
        wbnb_addr = tokens["WBNB"]

        usdt_decimals = get_token_decimals(web3, usdt_addr)
        wbnb_decimals = get_token_decimals(web3, wbnb_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Open USDT/WBNB via LPOpenIntent")
        print(f"{'=' * 80}")
        print(f"Pool: {POOL}")
        print(f"Amount USDT (token0): {LP_AMOUNT_USDT}")
        print(f"Amount WBNB (token1): {LP_AMOUNT_WBNB}")
        print(f"Range: [{RANGE_LOWER} - {RANGE_UPPER}] WBNB per USDT")

        # 1. Record balances BEFORE
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_before = get_token_balance(web3, wbnb_addr, funded_wallet)

        print(f"USDT before: {format_token_amount(usdt_before, usdt_decimals)}")
        print(f"WBNB before: {format_token_amount(wbnb_before, wbnb_decimals)}")

        # 2. Create LPOpenIntent
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_USDT,
            amount1=LP_AMOUNT_WBNB,
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
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_after = get_token_balance(web3, wbnb_addr, funded_wallet)

        usdt_spent = usdt_before - usdt_after
        wbnb_spent = wbnb_before - wbnb_after

        print(f"\nUSDT spent: {format_token_amount(usdt_spent, usdt_decimals)}")
        print(f"WBNB spent: {format_token_amount(wbnb_spent, wbnb_decimals)}")

        # At least one token must have been deposited
        assert usdt_spent > 0 or wbnb_spent > 0, "Must deposit at least one token into LP"

        # Amounts spent must not exceed desired amounts
        expected_usdt_max = int(LP_AMOUNT_USDT * Decimal(10**usdt_decimals))
        expected_wbnb_max = int(LP_AMOUNT_WBNB * Decimal(10**wbnb_decimals))
        assert usdt_spent <= expected_usdt_max, f"USDT spent ({usdt_spent}) exceeds desired ({expected_usdt_max})"
        assert wbnb_spent <= expected_wbnb_max, f"WBNB spent ({wbnb_spent}) exceeds desired ({expected_wbnb_max})"

        print("\nALL CHECKS PASSED")


# =============================================================================
# LPCloseIntent Tests
# =============================================================================


@pytest.mark.bnb
@pytest.mark.lp
class TestSushiSwapV3LPCloseIntent:
    """Test SushiSwap V3 LP Close using LPCloseIntent.

    Test cases:
    #1: Position has liquidity (normal LP close)
    #2: Position has no liquidity and no owed tokens (already decreased + collected)
    #3: Position has no liquidity but has owed tokens (decreased but not collected)
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
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
        usdt_addr = tokens["USDT"]
        wbnb_addr = tokens["WBNB"]
        usdt_decimals = get_token_decimals(web3, usdt_addr)
        wbnb_decimals = get_token_decimals(web3, wbnb_addr)

        print(f"\n{'=' * 80}")
        print("Test #1: LP Close - Position with Liquidity")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Verify it has liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have liquidity before close, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 3. Record balances BEFORE close
        usdt_before_close = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_before_close = get_token_balance(web3, wbnb_addr, funded_wallet)

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
        usdt_after_close = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_after_close = get_token_balance(web3, wbnb_addr, funded_wallet)

        usdt_returned = usdt_after_close - usdt_before_close
        wbnb_returned = wbnb_after_close - wbnb_before_close

        print(f"\nUSDT returned: {format_token_amount(usdt_returned, usdt_decimals)}")
        print(f"WBNB returned: {format_token_amount(wbnb_returned, wbnb_decimals)}")

        # At least one token must be returned (was deposited in LP)
        assert usdt_returned > 0 or wbnb_returned > 0, (
            f"Must receive tokens back when closing position. "
            f"USDT returned: {usdt_returned}, WBNB returned: {wbnb_returned}"
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
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
        usdt_addr = tokens["USDT"]
        wbnb_addr = tokens["WBNB"]

        print(f"\n{'=' * 80}")
        print("Test #2: LP Close - No Liquidity, No Owed Tokens")
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
        usdt_before_close = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_before_close = get_token_balance(web3, wbnb_addr, funded_wallet)

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
        usdt_after_close = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_after_close = get_token_balance(web3, wbnb_addr, funded_wallet)

        usdt_delta = usdt_after_close - usdt_before_close
        wbnb_delta = wbnb_after_close - wbnb_before_close

        assert usdt_delta == 0, f"USDT balance should be unchanged for empty position, got delta: {usdt_delta}"
        assert wbnb_delta == 0, f"WBNB balance should be unchanged for empty position, got delta: {wbnb_delta}"

        print(f"USDT delta: {usdt_delta}")
        print(f"WBNB delta: {wbnb_delta}")
        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
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
        usdt_addr = tokens["USDT"]
        wbnb_addr = tokens["WBNB"]
        usdt_decimals = get_token_decimals(web3, usdt_addr)
        wbnb_decimals = get_token_decimals(web3, wbnb_addr)

        print(f"\n{'=' * 80}")
        print("Test #3: LP Close - No Liquidity, But Owed Tokens")
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
        usdt_before_close = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_before_close = get_token_balance(web3, wbnb_addr, funded_wallet)

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
        usdt_after_close = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_after_close = get_token_balance(web3, wbnb_addr, funded_wallet)

        usdt_collected = usdt_after_close - usdt_before_close
        wbnb_collected = wbnb_after_close - wbnb_before_close

        print(f"\nUSDT collected: {format_token_amount(usdt_collected, usdt_decimals)}")
        print(f"WBNB collected: {format_token_amount(wbnb_collected, wbnb_decimals)}")

        # At least one token must be collected (there were owed tokens from the decrease)
        assert usdt_collected > 0 or wbnb_collected > 0, (
            f"Must collect owed tokens from decreased position. "
            f"USDT collected: {usdt_collected}, WBNB collected: {wbnb_collected}"
        )

        print("\nALL CHECKS PASSED")


@pytest.mark.bnb
@pytest.mark.lp
class TestSushiSwapV3CollectFeesIntent:
    """SushiSwap V3 LP_COLLECT_FEES on BSC/BNB (4-layer Intent flow).

    Mirrors the Uniswap V3 ``TestUniswapV3CollectFeesIntent`` pattern: open an
    in-range position, trigger a same-pool swap to accrue fees, then issue
    ``CollectFeesIntent`` and verify wallet balances increase while position
    liquidity stays put.
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.SWAP, IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=True,
        reason="VIB-4314: same-pool fee-accrual fixture not yet wired — swap routes to different fee tier than LP position (as of 2026-05-12)",
    )
    async def test_collect_fees_usdt_wbnb(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Collect fees from an in-range USDT/WBNB position after a same-pool swap.

        Asserts:
        * Compilation of ``CollectFeesIntent`` (sushiswap_v3) -> SUCCESS.
        * Execution -> success.
        * Position liquidity unchanged (fee harvest does not remove principal).
        * At least one wallet balance strictly increases (fees transferred back).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]
        wbnb_addr = tokens["WBNB"]
        usdt_decimals = get_token_decimals(web3, usdt_addr)
        wbnb_decimals = get_token_decimals(web3, wbnb_addr)

        print(f"\n{'=' * 80}")
        print(f"Test: LP_COLLECT_FEES USDT/WBNB via SushiSwap V3 on {CHAIN_NAME}")
        print(f"{'=' * 80}")

        # 1. Open an in-range position to accrue fees against.
        position_id = await _open_position_via_intent(
            funded_wallet, orchestrator, price_oracle, anvil_rpc_url,
        )
        print(f"Opened position #{position_id}")

        liquidity_before = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity_before > 0, "Setup LP_OPEN must yield positive liquidity"

        # 2. Execute a same-pool swap to generate trading fees.
        swap_intent = SwapIntent(
            from_token="USDT",
            to_token="WBNB",
            amount=Decimal("1000"),
            max_slippage=Decimal("0.05"),
            protocol="sushiswap_v3",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        swap_compilation = compiler.compile(swap_intent)
        assert swap_compilation.status.value == "SUCCESS", (
            f"Fee-accrual swap must compile to seed LP_COLLECT_FEES coverage. "
            f"Error: {swap_compilation.error}"
        )
        assert swap_compilation.action_bundle is not None
        swap_result = await orchestrator.execute(swap_compilation.action_bundle)
        assert swap_result.success, (
            f"Fee-accrual swap must execute so LP_COLLECT_FEES runs on a fee-accrued "
            f"position. Error: {swap_result.error}"
        )
        print("Executed swap to generate LP fees")

        # 3. Record balances BEFORE fee collection.
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_before = get_token_balance(web3, wbnb_addr, funded_wallet)
        print(f"USDT before: {format_token_amount(usdt_before, usdt_decimals)}")
        print(f"WBNB before: {format_token_amount(wbnb_before, wbnb_decimals)}")

        # 4. Issue the LP_COLLECT_FEES intent.
        collect_intent = CollectFeesIntent(
            pool=POOL,
            protocol="sushiswap_v3",
            chain=CHAIN_NAME,
            protocol_params={"position_id": position_id},
        )

        print("\nCompiling CollectFeesIntent...")
        compilation_result = compiler.compile(collect_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"CollectFees compilation must succeed (sushiswap_v3 LP_COLLECT_FEES). "
            f"Error: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        print(f"ActionBundle: {len(compilation_result.action_bundle.transactions)} transactions")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"CollectFees execution failed: {execution_result.error}"

        # 5. Parse receipts — Layer 3 strict: COLLECT event amounts > 0.
        # The V3-fork collect compiler routes `recipient=wallet` directly (no
        # unwrap), so Collect event amount0/amount1 must equal wallet deltas
        # exactly (see compiler._compile_collect_fees_v3_fork).
        parser = SushiSwapV3ReceiptParser(chain=CHAIN_NAME)
        parsed_amount0_collected = 0
        parsed_amount1_collected = 0
        saw_collect = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parse_result = parser.parse_receipt(receipt_dict)
                assert parse_result.success, (
                    f"Receipt parser must succeed on a confirmed receipt; "
                    f"error={parse_result.error}"
                )
                lp_close_data = parser.extract_lp_close_data(receipt_dict)
                if lp_close_data:
                    parsed_amount0_collected += lp_close_data.amount0_collected
                    parsed_amount1_collected += lp_close_data.amount1_collected
                    saw_collect = True

        assert saw_collect, (
            "Receipt must contain a Collect event from LP_COLLECT_FEES"
        )
        assert parsed_amount0_collected > 0 or parsed_amount1_collected > 0, (
            f"Parser must report positive collected amounts. "
            f"amount0={parsed_amount0_collected}, amount1={parsed_amount1_collected}"
        )

        # 6. Verify principal liquidity is unchanged (fees-only, not LP_CLOSE).
        liquidity_after = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity_after == liquidity_before, (
            f"LP_COLLECT_FEES must NOT remove liquidity. "
            f"before={liquidity_before}, after={liquidity_after}"
        )

        # 7. Layer 4 strict: wallet deltas exactly equal parsed amounts.
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_after = get_token_balance(web3, wbnb_addr, funded_wallet)
        usdt_received = usdt_after - usdt_before
        wbnb_received = wbnb_after - wbnb_before

        print(f"\nUSDT received: {format_token_amount(usdt_received, usdt_decimals)}")
        print(f"WBNB received: {format_token_amount(wbnb_received, wbnb_decimals)}")

        if int(usdt_addr, 16) < int(wbnb_addr, 16):
            parsed_usdt_collected, parsed_wbnb_collected = (
                parsed_amount0_collected,
                parsed_amount1_collected,
            )
        else:
            parsed_wbnb_collected, parsed_usdt_collected = (
                parsed_amount0_collected,
                parsed_amount1_collected,
            )

        assert usdt_received == parsed_usdt_collected, (
            f"USDT wallet delta must exactly equal parsed Collect amount. "
            f"wallet={usdt_received}, parsed={parsed_usdt_collected}"
        )
        assert wbnb_received == parsed_wbnb_collected, (
            f"WBNB wallet delta must exactly equal parsed Collect amount. "
            f"wallet={wbnb_received}, parsed={parsed_wbnb_collected}"
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
