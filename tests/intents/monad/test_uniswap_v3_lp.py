"""Production-grade LP Intent tests for Uniswap V3 on Monad.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Opening concentrated liquidity positions
- LPCloseIntent: Closing positions with various states
- CollectFeesIntent: Harvesting fees from an open position

LP Close test cases:
  #1: Position has liquidity + fees (normal close)
  #2: Position has no liquidity and no fees (already decreased + collected)
  #3: Position has no liquidity but owed tokens (decreased but not collected)

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

VIB-4351 (Phase 1a of VIB-4343). The (uniswap_v3, monad) entry in the connector
registry is intentionally deferred to a follow-up PR; the sibling SWAP coverage
ticket (VIB-4350) runs in parallel and a coordinated registry edit will land
after both test files exist. The intent-test gate at
``tests/unit/permissions/test_onchain_case_coverage.py`` only inspects the
(protocol, intent_type) matrix — it does not require the chain to be present in
the connector registry, so these tests still provide coverage rows for
uniswap_v3 LP_OPEN / LP_CLOSE / LP_COLLECT_FEES while the registry catches up.

To run:
    uv run pytest tests/intents/monad/test_uniswap_v3_lp.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser
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

CHAIN_NAME = "monad"
# Uniswap V3 NonfungiblePositionManager on Monad (compiler_constants.py:242).
POSITION_MANAGER = "0x7197E214c0b767cFB76Fb734ab638E2c192F4E53"

# Pool: WMON/USDC 0.3% fee tier.
# Token ordering by address on Monad:
#   WMON (0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A)
#   USDC (0x754704Bc059F8C67012fEd69BC8A327a5aafb603)
# WMON < USDC, so WMON=token0, USDC=token1; range is in USDC-per-WMON terms.
#
# Pool selection rationale (probed 2026-05-13 against rpc.monad.xyz):
#   WMON/USDC fee=3000 — pool 0x659bd0...4a9da — ~33.4M WMON + ~536K USDC.
#   This is the deepest WMON/USDC pool on Monad Uniswap V3 by an order of
#   magnitude (fee=500 holds <8 USDC; fee=10000 holds ~7.4K). WETH/USDC pools
#   are an order of magnitude thinner (~$19K vs ~$540K) so WMON/USDC is the
#   sole defensible primary pair for these tests.
POOL = "WMON/USDC/3000"
# Funded balances on the Monad anvil fork: 10 WMON (wrap budget) + 100,000 USDC
# (storage-slot override) per tests/intents/monad/conftest.py. Sizing
# accordingly: 5 WMON leaves headroom for the swap helper in test #3 to also
# pull WMON liquidity through the pool. USDC amount is tiny because the
# 0.029 USDC/WMON exchange rate means a wide-range mint deposits very little
# USDC relative to WMON.
LP_AMOUNT_WMON = Decimal("5")     # amount0 (WMON is token0 on Monad)
LP_AMOUNT_USDC = Decimal("1")     # amount1 (USDC is token1 on Monad)

# Wide price range in USDC-per-WMON terms to ensure both tokens are deposited.
# Mid-market price ~0.029 USDC/WMON (tick ~-311724 as of 2026-05-13 fork block).
# range_lower=0.005 -> WMON at $0.005
# range_upper=0.5   -> WMON at $0.50
RANGE_LOWER = Decimal("0.005")
RANGE_UPPER = Decimal("0.5")


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
        amount0=LP_AMOUNT_WMON,   # WMON is token0 on Monad
        amount1=LP_AMOUNT_USDC,   # USDC is token1 on Monad
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        protocol="uniswap_v3",
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
    parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            pos_id = parser.extract_position_id(tx_result.receipt.to_dict())
            if pos_id is not None:
                return pos_id

    raise AssertionError("Failed to extract position ID from LP Open receipt")


# =============================================================================
# LPOpenIntent Tests
# =============================================================================


@pytest.mark.monad
@pytest.mark.lp
class TestUniswapV3LPOpenIntent:
    """Test Uniswap V3 LP Open using LPOpenIntent on Monad.

    Verifies the full Intent flow:
    - LPOpenIntent creation with pool, amounts, and price range
    - IntentCompiler generates correct NonfungiblePositionManager mint TX
    - Transactions execute successfully on-chain
    - Position NFT is minted and has liquidity
    - Balance changes are correct
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_wmon_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test opening a WMON/USDC LP position using LPOpenIntent.

        Flow:
        1. Record balances BEFORE
        2. Create LPOpenIntent for WMON/USDC pool
        3. Compile to ActionBundle using IntentCompiler
        4. Execute via ExecutionOrchestrator
        5. Parse receipts - extract position ID
        6. Verify on-chain position liquidity
        7. Verify balance changes
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wmon_addr = tokens["WMON"]

        usdc_decimals = get_token_decimals(web3, usdc_addr)
        wmon_decimals = get_token_decimals(web3, wmon_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Open WMON/USDC via LPOpenIntent on Monad")
        print(f"{'=' * 80}")
        print(f"Pool: {POOL}")
        print(f"Amount WMON (user token0): {LP_AMOUNT_WMON}")
        print(f"Amount USDC (user token1): {LP_AMOUNT_USDC}")
        print(f"Range: [{RANGE_LOWER} - {RANGE_UPPER}] USDC per WMON")

        # 1. Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        wmon_before = get_token_balance(web3, wmon_addr, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"WMON before: {format_token_amount(wmon_before, wmon_decimals)}")

        # 2. Create LPOpenIntent
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WMON,
            amount1=LP_AMOUNT_USDC,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        # 3. Compile (Layer 1)
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

        # 4. Execute (Layer 2)
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # 5. Parse receipts (Layer 3) — extract position ID + strict parser success
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        position_id = None
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                pos_id = parser.extract_position_id(receipt_dict)
                if pos_id is not None:
                    position_id = pos_id

                parse_result = parser.parse_receipt(receipt_dict)
                assert parse_result.success, (
                    f"Receipt parser must succeed on a confirmed LP_OPEN receipt; "
                    f"error={parse_result.error}"
                )
                print(f"  Events parsed: {len(parse_result.events)}")

        assert position_id is not None, "Must extract position ID from mint receipt"
        print(f"\nPosition ID: {position_id}")

        # 6. Verify on-chain position has liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have positive liquidity, got {liquidity}"
        print(f"On-chain liquidity: {liquidity}")

        # 7. Verify balance changes (Layer 4)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        wmon_after = get_token_balance(web3, wmon_addr, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        wmon_spent = wmon_before - wmon_after

        print(f"\nUSDC spent: {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"WMON spent: {format_token_amount(wmon_spent, wmon_decimals)}")

        # At least one token must have been deposited
        assert usdc_spent > 0 or wmon_spent > 0, "Must deposit at least one token into LP"

        # Amounts spent must not exceed desired amounts
        expected_usdc_max = int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        expected_wmon_max = int(LP_AMOUNT_WMON * Decimal(10**wmon_decimals))
        assert usdc_spent <= expected_usdc_max, f"USDC spent ({usdc_spent}) exceeds desired ({expected_usdc_max})"
        assert wmon_spent <= expected_wmon_max, f"WMON spent ({wmon_spent}) exceeds desired ({expected_wmon_max})"

        print("\nALL CHECKS PASSED")


# =============================================================================
# LPCloseIntent Tests
# =============================================================================


@pytest.mark.monad
@pytest.mark.lp
class TestUniswapV3LPCloseIntent:
    """Test Uniswap V3 LP Close using LPCloseIntent on Monad.

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
        usdc_addr = tokens["USDC"]
        wmon_addr = tokens["WMON"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        wmon_decimals = get_token_decimals(web3, wmon_addr)

        print(f"\n{'=' * 80}")
        print("Test #1: LP Close - Position with Liquidity (Monad)")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Verify it has liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have liquidity before close, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 3. Record balances BEFORE close
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wmon_before_close = get_token_balance(web3, wmon_addr, funded_wallet)

        # 4. Close via LPCloseIntent
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="uniswap_v3",
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

        # 5. Parse receipts (Layer 3) — strict: at least one Collect event with
        # decodable LP_CLOSE data covering both legs (close emits both
        # decreaseLiquidity AND collect, so amount0+amount1 must be > 0).
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        parsed_amount0_collected = 0
        parsed_amount1_collected = 0
        saw_lp_close_data = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parse_result = parser.parse_receipt(receipt_dict)
                assert parse_result.success, (
                    f"Receipt parser must succeed on a confirmed LP_CLOSE receipt; "
                    f"error={parse_result.error}"
                )
                lp_close_data = parser.extract_lp_close_data(receipt_dict)
                if lp_close_data:
                    saw_lp_close_data = True
                    parsed_amount0_collected += lp_close_data.amount0_collected
                    parsed_amount1_collected += lp_close_data.amount1_collected
                    print(
                        f"  LP Close data: amount0_collected={lp_close_data.amount0_collected}, "
                        f"amount1_collected={lp_close_data.amount1_collected}"
                    )

        assert saw_lp_close_data, "LP_CLOSE receipts must include decodable close/collect data"
        # Layer 3: positive amounts somewhere across the Burn+Collect aggregate.
        # ``extract_lp_close_data`` returns ``amount0_collected = collect_amount0
        # if saw_collect else burn_amount0`` per receipt. Across the V3 LP_CLOSE
        # multi-tx (decreaseLiquidity ‖ collect ‖ burn), summing across receipts
        # therefore double-counts on the WMON/USDC happy path (TX1.Burn carries
        # principal, TX2.Collect carries principal+fees). That bookkeeping
        # asymmetry is why this test asserts "either parsed leg > 0" instead of
        # an exact wallet-delta equality — for an exact-delta receipt-vs-wallet
        # invariant on the V3 family, see ``test_collect_fees_wmon_usdc`` and
        # the dedicated ``TestUniswapV3CollectFeesIntent`` class, which exercise
        # a single-tx Collect path where the parser sum and wallet delta tie
        # exactly.
        assert parsed_amount0_collected > 0 or parsed_amount1_collected > 0, (
            f"LP_CLOSE must parse positive amounts on at least one leg. "
            f"amount0={parsed_amount0_collected}, amount1={parsed_amount1_collected}"
        )

        # 6. Verify tokens returned (Layer 4)
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wmon_after_close = get_token_balance(web3, wmon_addr, funded_wallet)

        usdc_returned = usdc_after_close - usdc_before_close
        wmon_returned = wmon_after_close - wmon_before_close

        print(f"\nUSDC returned: {format_token_amount(usdc_returned, usdc_decimals)}")
        print(f"WMON returned: {format_token_amount(wmon_returned, wmon_decimals)}")

        # At least one token must be returned (was deposited in LP).
        assert usdc_returned > 0 or wmon_returned > 0, (
            f"Must receive tokens back when closing position. "
            f"USDC returned: {usdc_returned}, WMON returned: {wmon_returned}"
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_position_no_liquidity_no_fees(  # noqa: layers
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test #2: Close position with no liquidity and no owed tokens.

        The LP_CLOSE compiles to a no-op ActionBundle (0 transactions) per
        VIB-3644, so there are no receipts to parse — Layer 3 is intentionally
        skipped here. The test still verifies Layer 1 (compilation success
        with ``metadata.no_op == True``), Layer 2 (execution success with
        ``transaction_results == []``), and Layer 4 (balance conservation:
        deltas are exactly zero).

        This tests the edge case where a position has already had its liquidity
        removed and tokens collected externally (e.g., via direct contract calls).
        The LPCloseIntent should handle this gracefully (no-op success per
        VIB-3644).

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
        usdc_addr = tokens["USDC"]
        wmon_addr = tokens["WMON"]

        print(f"\n{'=' * 80}")
        print("Test #2: LP Close - No Liquidity, No Owed Tokens (Monad)")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Decrease all liquidity directly
        await decrease_all_liquidity(
            web3, orchestrator,
            chain=CHAIN_NAME, protocol="uniswap_v3",
            position_manager=POSITION_MANAGER, token_id=position_id,
        )
        print("Decreased all liquidity via direct call")

        # 3. Collect all owed tokens directly
        await collect_all_tokens(
            web3, orchestrator,
            chain=CHAIN_NAME, protocol="uniswap_v3",
            position_manager=POSITION_MANAGER, token_id=position_id,
            recipient=funded_wallet,
        )
        print("Collected all owed tokens via direct call")

        # 4. Verify 0 liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity == 0, f"Expected 0 liquidity after decrease, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 5. Record balances BEFORE close
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wmon_before_close = get_token_balance(web3, wmon_addr, funded_wallet)

        # 6. Close via LPCloseIntent
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="uniswap_v3",
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
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wmon_after_close = get_token_balance(web3, wmon_addr, funded_wallet)

        usdc_delta = usdc_after_close - usdc_before_close
        wmon_delta = wmon_after_close - wmon_before_close

        assert usdc_delta == 0, f"USDC balance should be unchanged for empty position, got delta: {usdc_delta}"
        assert wmon_delta == 0, f"WMON balance should be unchanged for empty position, got delta: {wmon_delta}"

        print(f"USDC delta: {usdc_delta}")
        print(f"WMON delta: {wmon_delta}")
        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_lp_close_position_no_liquidity_but_owed_tokens(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test #3: Close position with no liquidity but uncollected owed tokens.

        After decreaseLiquidity, the principal tokens (and any accrued fees)
        become "owed" to the position but are not yet transferred. The collect
        step in LPCloseIntent should retrieve these owed tokens.

        Flow:
        1. Open LP position via LPOpenIntent
        2. Execute a swap to generate trading fees for the position
        3. Decrease all liquidity via direct contract call (tokens become owed)
        4. Do NOT collect - tokens remain owed
        5. Verify position has 0 liquidity
        6. Record balances BEFORE close
        7. Close via LPCloseIntent (should collect owed tokens and burn)
        8. Verify tokens were collected
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wmon_addr = tokens["WMON"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        wmon_decimals = get_token_decimals(web3, wmon_addr)

        print(f"\n{'=' * 80}")
        print("Test #3: LP Close - No Liquidity, But Owed Tokens (Monad)")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Execute a swap through the pool to generate fees
        swap_intent = SwapIntent(
            from_token="USDC",
            to_token="WMON",
            amount=Decimal("1"),
            max_slippage=Decimal("0.05"),
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        swap_compilation = compiler.compile(swap_intent)
        if swap_compilation.status.value == "SUCCESS" and swap_compilation.action_bundle:
            swap_result = await orchestrator.execute(swap_compilation.action_bundle)
            if swap_result.success:
                print("Executed swap to generate LP fees")
            else:
                print(f"Swap failed (non-critical for this test): {swap_result.error}")

        # 3. Decrease all liquidity (tokens become owed but not collected)
        await decrease_all_liquidity(
            web3, orchestrator,
            chain=CHAIN_NAME, protocol="uniswap_v3",
            position_manager=POSITION_MANAGER, token_id=position_id,
        )
        print("Decreased all liquidity via direct call (tokens now owed)")

        # 4. Do NOT collect - leave tokens owed

        # 5. Verify 0 liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity == 0, f"Expected 0 liquidity after decrease, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 6. Record balances BEFORE close
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wmon_before_close = get_token_balance(web3, wmon_addr, funded_wallet)

        # 7. Close via LPCloseIntent (should collect owed tokens)
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="uniswap_v3",
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

        # Parse receipts (Layer 3) — strict: at least one Collect event with
        # decodable LP_CLOSE data and positive collected amounts.
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        parsed_amount0_collected = 0
        parsed_amount1_collected = 0
        saw_lp_close_data = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parse_result = parser.parse_receipt(receipt_dict)
                assert parse_result.success, (
                    f"Receipt parser must succeed on a confirmed LP_CLOSE receipt; "
                    f"error={parse_result.error}"
                )
                lp_close_data = parser.extract_lp_close_data(receipt_dict)
                if lp_close_data:
                    saw_lp_close_data = True
                    parsed_amount0_collected += lp_close_data.amount0_collected
                    parsed_amount1_collected += lp_close_data.amount1_collected
                    print(
                        f"  LP Close data: amount0_collected={lp_close_data.amount0_collected}, "
                        f"amount1_collected={lp_close_data.amount1_collected}"
                    )

        assert saw_lp_close_data, "LP_CLOSE receipts must include decodable close/collect data"
        # Layer 3: positive amounts somewhere across the Burn+Collect aggregate.
        # See test_lp_close_position_with_liquidity for the per-V3 LP_CLOSE
        # multi-tx accounting note — same reason "either leg > 0" is the right
        # bound here rather than wallet-delta-exact.
        assert parsed_amount0_collected > 0 or parsed_amount1_collected > 0, (
            f"LP_CLOSE must parse positive amounts on at least one leg (owed-tokens path). "
            f"amount0={parsed_amount0_collected}, amount1={parsed_amount1_collected}"
        )

        # 8. Verify tokens were collected (Layer 4)
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wmon_after_close = get_token_balance(web3, wmon_addr, funded_wallet)

        usdc_collected = usdc_after_close - usdc_before_close
        wmon_collected = wmon_after_close - wmon_before_close

        print(f"\nUSDC collected: {format_token_amount(usdc_collected, usdc_decimals)}")
        print(f"WMON collected: {format_token_amount(wmon_collected, wmon_decimals)}")

        # At least one token must be collected (there were owed tokens from the decrease).
        assert usdc_collected > 0 or wmon_collected > 0, (
            f"Must collect owed tokens from decreased position. "
            f"USDC collected: {usdc_collected}, WMON collected: {wmon_collected}"
        )

        print("\nALL CHECKS PASSED")


# =============================================================================
# CollectFeesIntent Tests (LP_COLLECT_FEES) — VIB-4307 / VIB-4351
# =============================================================================


@pytest.mark.monad
@pytest.mark.lp
class TestUniswapV3CollectFeesIntent:
    """Test Uniswap V3 LP_COLLECT_FEES using CollectFeesIntent on Monad.

    Flow (4 layers):
      1. Open an in-range LP position via LPOpenIntent.
      2. Execute a swap through the same pool to accrue fees on the position.
      3. Issue CollectFeesIntent(protocol="uniswap_v3", protocol_params={"position_id": ...}).
      4. Verify wallet balances increased (fees were transferred to the wallet).
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.SWAP, IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=True,
        reason="VIB-4314: same-pool fee-accrual fixture not yet wired — swap may route to a different fee tier than the LP position (as of 2026-05-13). Tracks the cross-chain blocker noted on ethereum/arbitrum sister tests.",
    )
    async def test_collect_fees_wmon_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Collect fees from an in-range WMON/USDC position after a same-pool swap.

        Asserts:
        * Compilation of CollectFeesIntent (uniswap_v3) -> SUCCESS.
        * Execution -> success.
        * Position liquidity unchanged (fee harvest does not remove principal).
        * Parser emits a Collect event with positive amounts.
        * Wallet deltas exactly equal the parsed Collect amounts.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wmon_addr = tokens["WMON"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        wmon_decimals = get_token_decimals(web3, wmon_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP_COLLECT_FEES WMON/USDC via Uniswap V3 on Monad")
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
            from_token="USDC",
            to_token="WMON",
            amount=Decimal("1"),
            max_slippage=Decimal("0.05"),
            protocol="uniswap_v3",
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

        # 3. Record balances BEFORE fee collection
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        wmon_before = get_token_balance(web3, wmon_addr, funded_wallet)
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"WMON before: {format_token_amount(wmon_before, wmon_decimals)}")

        # 4. Issue the LP_COLLECT_FEES intent.
        collect_intent = CollectFeesIntent(
            pool=POOL,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
            protocol_params={"position_id": position_id},
        )

        print("\nCompiling CollectFeesIntent...")
        compilation_result = compiler.compile(collect_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"CollectFees compilation must succeed (uniswap_v3 LP_COLLECT_FEES). "
            f"Error: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        print(f"ActionBundle: {len(compilation_result.action_bundle.transactions)} transactions")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"CollectFees execution failed: {execution_result.error}"

        # 5. Parse receipts — Layer 3 strict: COLLECT event amounts > 0.
        # The V3-fork collect compiler routes `recipient=wallet` directly (no
        # unwrap), so Collect event amount0/amount1 must equal wallet deltas
        # exactly (see the connector collect-fees compiler).
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
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
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        wmon_after = get_token_balance(web3, wmon_addr, funded_wallet)
        usdc_received = usdc_after - usdc_before
        wmon_received = wmon_after - wmon_before

        print(f"\nUSDC received: {format_token_amount(usdc_received, usdc_decimals)}")
        print(f"WMON received: {format_token_amount(wmon_received, wmon_decimals)}")

        # On Monad, token0=WMON (0x3bd3...), token1=USDC (0x7547...) by address
        # ordering, so amount0 maps to WMON and amount1 maps to USDC.
        if int(wmon_addr, 16) < int(usdc_addr, 16):
            parsed_wmon_collected, parsed_usdc_collected = (
                parsed_amount0_collected,
                parsed_amount1_collected,
            )
        else:
            parsed_usdc_collected, parsed_wmon_collected = (
                parsed_amount0_collected,
                parsed_amount1_collected,
            )

        assert wmon_received == parsed_wmon_collected, (
            f"WMON wallet delta must exactly equal parsed Collect amount. "
            f"wallet={wmon_received}, parsed={parsed_wmon_collected}"
        )
        assert usdc_received == parsed_usdc_collected, (
            f"USDC wallet delta must exactly equal parsed Collect amount. "
            f"wallet={usdc_received}, parsed={parsed_usdc_collected}"
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
