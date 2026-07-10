"""Production-grade LP Intent tests for Uniswap V3 on Robinhood Chain (4663).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Opening concentrated liquidity positions
- LPCloseIntent: Closing positions with various states

LP Close test cases:
  #1: Position has liquidity + fees (normal close)
  #2: Position has no liquidity and no fees (already decreased + collected)
  #3: Position has no liquidity but owed tokens (decreased but not collected)

Pair: WETH/USDG fee 500 — the chain's primary liquid V3 pool
(0x69BfaF19C9f377BB306a89aEd9F6B07e2c1a8d9a, ~$3.5M TVL @ fork block 5,610,000,
price sanity ~$1,745/WETH). Token ordering by address on Robinhood:
  WETH (0x0Bd7…AD73) < USDG (0x5fc5…d168)
so WETH=token0, USDG=token1; range is in USDG-per-WETH terms.

NO MOCKING. All tests execute real on-chain transactions and verify state
changes. Default-on Zodiac routes each intent through Safe + Roles +
execTransactionWithRole.

To run:
    uv run pytest tests/intents/robinhood/test_uniswap_v3_lp.py -v -s
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

CHAIN_NAME = "robinhood"
# Uniswap V3 NonfungiblePositionManager on Robinhood (addresses.py).
POSITION_MANAGER = "0x73991a25C818Bf1f1128dEAaB1492D45638DE0D3"

# Pool: WETH/USDG 0.05% fee tier (the chain's deepest V3 pool, ~$3.5M TVL).
# WETH < USDG by address, so WETH=token0, USDG=token1; range is USDG-per-WETH.
POOL = "WETH/USDG/500"
# Funded balances on the Robinhood anvil fork: 10 WETH (wrap budget) + 100,000
# USDG (storage-slot override) per tests/intents/robinhood/conftest.py. A wide
# range at ~$1,745/WETH deposits a small fraction of each.
LP_AMOUNT_WETH = Decimal("0.2")   # amount0 (WETH is token0)
LP_AMOUNT_USDG = Decimal("500")   # amount1 (USDG is token1)

# Wide price range in USDG-per-WETH terms to ensure both tokens are deposited.
# range_lower=200   -> WETH at $200
# range_upper=20000 -> WETH at $20,000
RANGE_LOWER = Decimal("200")
RANGE_UPPER = Decimal("20000")


# =============================================================================
# Helpers (shared)
# =============================================================================
#
# ``query_position_liquidity``, ``decrease_all_liquidity``, and
# ``collect_all_tokens`` live in ``tests/intents/_lp_setup_helpers.py`` so the
# no-liquidity edge-case tests route their setup tx through whatever orchestrator
# the test holds — EOA-signed under ``ExecutionOrchestrator``,
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
        amount0=LP_AMOUNT_WETH,   # WETH is token0
        amount1=LP_AMOUNT_USDG,   # USDG is token1
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


@pytest.mark.robinhood
@pytest.mark.lp
class TestUniswapV3LPOpenIntent:
    """Test Uniswap V3 LP Open using LPOpenIntent on Robinhood Chain."""

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_weth_usdg(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test opening a WETH/USDG LP position using LPOpenIntent.

        Flow:
        1. Record balances BEFORE
        2. Create LPOpenIntent for WETH/USDG pool
        3. Compile to ActionBundle using IntentCompiler
        4. Execute via ExecutionOrchestrator
        5. Parse receipts - extract position ID
        6. Verify on-chain position liquidity
        7. Verify balance changes
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdg_addr = tokens["USDG"]
        weth_addr = tokens["WETH"]

        usdg_decimals = get_token_decimals(web3, usdg_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Open WETH/USDG via LPOpenIntent on Robinhood")
        print(f"{'=' * 80}")
        print(f"Pool: {POOL}")
        print(f"Amount WETH (user token0): {LP_AMOUNT_WETH}")
        print(f"Amount USDG (user token1): {LP_AMOUNT_USDG}")
        print(f"Range: [{RANGE_LOWER} - {RANGE_UPPER}] USDG per WETH")

        # 1. Record balances BEFORE
        usdg_before = get_token_balance(web3, usdg_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)

        print(f"USDG before: {format_token_amount(usdg_before, usdg_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")

        # 2. Create LPOpenIntent
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_USDG,
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
                    f"Receipt parser must succeed on a confirmed LP_OPEN receipt; error={parse_result.error}"
                )
                print(f"  Events parsed: {len(parse_result.events)}")

        assert position_id is not None, "Must extract position ID from mint receipt"
        print(f"\nPosition ID: {position_id}")

        # 6. Verify on-chain position has liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have positive liquidity, got {liquidity}"
        print(f"On-chain liquidity: {liquidity}")

        # 7. Verify balance changes (Layer 4)
        usdg_after = get_token_balance(web3, usdg_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)

        usdg_spent = usdg_before - usdg_after
        weth_spent = weth_before - weth_after

        print(f"\nUSDG spent: {format_token_amount(usdg_spent, usdg_decimals)}")
        print(f"WETH spent: {format_token_amount(weth_spent, weth_decimals)}")

        # At least one token must have been deposited
        assert usdg_spent > 0 or weth_spent > 0, "Must deposit at least one token into LP"

        # Amounts spent must not exceed desired amounts
        expected_usdg_max = int(LP_AMOUNT_USDG * Decimal(10**usdg_decimals))
        expected_weth_max = int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))
        assert usdg_spent <= expected_usdg_max, f"USDG spent ({usdg_spent}) exceeds desired ({expected_usdg_max})"
        assert weth_spent <= expected_weth_max, f"WETH spent ({weth_spent}) exceeds desired ({expected_weth_max})"

        print("\nALL CHECKS PASSED")


# =============================================================================
# LPCloseIntent Tests
# =============================================================================


@pytest.mark.robinhood
@pytest.mark.lp
class TestUniswapV3LPCloseIntent:
    """Test Uniswap V3 LP Close using LPCloseIntent on Robinhood Chain.

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
        """Test #1: Close position that has liquidity."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdg_addr = tokens["USDG"]
        weth_addr = tokens["WETH"]
        usdg_decimals = get_token_decimals(web3, usdg_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        print(f"\n{'=' * 80}")
        print("Test #1: LP Close - Position with Liquidity (Robinhood)")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Verify it has liquidity
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have liquidity before close, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 3. Record balances BEFORE close
        usdg_before_close = get_token_balance(web3, usdg_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)

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

        # 5. Parse receipts (Layer 3) — strict: at least one decodable LP_CLOSE
        # leg with positive collected amounts.
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        parsed_amount0_collected = 0
        parsed_amount1_collected = 0
        saw_lp_close_data = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parse_result = parser.parse_receipt(receipt_dict)
                assert parse_result.success, (
                    f"Receipt parser must succeed on a confirmed LP_CLOSE receipt; error={parse_result.error}"
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
        # Positive amounts somewhere across the Burn+Collect aggregate. The V3
        # LP_CLOSE multi-tx double-counts principal across receipts, so "either
        # leg > 0" is the right bound rather than a wallet-delta-exact equality.
        assert parsed_amount0_collected > 0 or parsed_amount1_collected > 0, (
            f"LP_CLOSE must parse positive amounts on at least one leg. "
            f"amount0={parsed_amount0_collected}, amount1={parsed_amount1_collected}"
        )

        # 6. Verify tokens returned (Layer 4)
        usdg_after_close = get_token_balance(web3, usdg_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)

        usdg_returned = usdg_after_close - usdg_before_close
        weth_returned = weth_after_close - weth_before_close

        print(f"\nUSDG returned: {format_token_amount(usdg_returned, usdg_decimals)}")
        print(f"WETH returned: {format_token_amount(weth_returned, weth_decimals)}")

        # At least one token must be returned (was deposited in LP).
        assert usdg_returned > 0 or weth_returned > 0, (
            f"Must receive tokens back when closing position. "
            f"USDG returned: {usdg_returned}, WETH returned: {weth_returned}"
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
        skipped here. The test still verifies Layer 1 (compilation success with
        ``metadata.no_op == True``), Layer 2 (execution success with
        ``transaction_results == []``), and Layer 4 (balance conservation:
        deltas are exactly zero).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdg_addr = tokens["USDG"]
        weth_addr = tokens["WETH"]

        print(f"\n{'=' * 80}")
        print("Test #2: LP Close - No Liquidity, No Owed Tokens (Robinhood)")
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
        usdg_before_close = get_token_balance(web3, usdg_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)

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
        usdg_after_close = get_token_balance(web3, usdg_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)

        usdg_delta = usdg_after_close - usdg_before_close
        weth_delta = weth_after_close - weth_before_close

        assert usdg_delta == 0, f"USDG balance should be unchanged for empty position, got delta: {usdg_delta}"
        assert weth_delta == 0, f"WETH balance should be unchanged for empty position, got delta: {weth_delta}"

        print(f"USDG delta: {usdg_delta}")
        print(f"WETH delta: {weth_delta}")
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
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdg_addr = tokens["USDG"]
        weth_addr = tokens["WETH"]
        usdg_decimals = get_token_decimals(web3, usdg_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        print(f"\n{'=' * 80}")
        print("Test #3: LP Close - No Liquidity, But Owed Tokens (Robinhood)")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Execute a swap through the pool to generate fees
        swap_intent = SwapIntent(
            from_token="USDG",
            to_token="WETH",
            amount=Decimal("100"),
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
        usdg_before_close = get_token_balance(web3, usdg_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)

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

        # Parse receipts (Layer 3) — strict: decodable LP_CLOSE data + positive amounts.
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        parsed_amount0_collected = 0
        parsed_amount1_collected = 0
        saw_lp_close_data = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parse_result = parser.parse_receipt(receipt_dict)
                assert parse_result.success, (
                    f"Receipt parser must succeed on a confirmed LP_CLOSE receipt; error={parse_result.error}"
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
        assert parsed_amount0_collected > 0 or parsed_amount1_collected > 0, (
            f"LP_CLOSE must parse positive amounts on at least one leg (owed-tokens path). "
            f"amount0={parsed_amount0_collected}, amount1={parsed_amount1_collected}"
        )

        # 8. Verify tokens were collected (Layer 4)
        usdg_after_close = get_token_balance(web3, usdg_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)

        usdg_collected = usdg_after_close - usdg_before_close
        weth_collected = weth_after_close - weth_before_close

        print(f"\nUSDG collected: {format_token_amount(usdg_collected, usdg_decimals)}")
        print(f"WETH collected: {format_token_amount(weth_collected, weth_decimals)}")

        # At least one token must be collected (there were owed tokens from the decrease).
        assert usdg_collected > 0 or weth_collected > 0, (
            f"Must collect owed tokens from decreased position. "
            f"USDG collected: {usdg_collected}, WETH collected: {weth_collected}"
        )

        print("\nALL CHECKS PASSED")



# =============================================================================
# CollectFeesIntent Tests (LP_COLLECT_FEES)
# =============================================================================


@pytest.mark.robinhood
@pytest.mark.lp
class TestUniswapV3CollectFeesIntent:
    """Test Uniswap V3 LP_COLLECT_FEES using CollectFeesIntent on Robinhood.

    Flow (4 layers):
      1. Open an in-range LP position via LPOpenIntent.
      2. Execute a swap through the same pool to accrue fees on the position.
      3. Issue CollectFeesIntent(protocol="uniswap_v3", protocol_params={"position_id": ...}).
      4. Verify wallet balances increased and deltas equal parsed Collect amounts.

    Unlike the ethereum/arbitrum/monad sister tests (xfail on VIB-4314's
    cross-fee-tier routing blocker), Robinhood's only pool with real depth IS
    the fee-500 WETH/USDG pool the position sits in — the fee-accrual swap has
    nowhere else to route, so this test asserts the full pass outright.
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.SWAP, IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    async def test_collect_fees_weth_usdg(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Collect fees from an in-range WETH/USDG position after a same-pool swap.

        Asserts:
        * Compilation of CollectFeesIntent (uniswap_v3) -> SUCCESS.
        * Execution -> success.
        * Position liquidity unchanged (fee harvest does not remove principal).
        * Parser emits a Collect event with positive amounts.
        * Wallet deltas exactly equal the parsed Collect amounts.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdg_addr = tokens["USDG"]
        weth_decimals = get_token_decimals(web3, weth_addr)
        usdg_decimals = get_token_decimals(web3, usdg_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP_COLLECT_FEES WETH/USDG via Uniswap V3 on Robinhood")
        print(f"{'=' * 80}")

        # 1. Open an in-range position to accrue fees against.
        position_id = await _open_position_via_intent(
            funded_wallet, orchestrator, price_oracle, anvil_rpc_url,
        )
        print(f"Opened position #{position_id}")

        liquidity_before = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity_before > 0, "Setup LP_OPEN must yield positive liquidity"

        # 2. Execute a same-pool swap to generate trading fees. The swap must be
        # large enough that the router's best-execution fee-tier selection picks
        # the deep fee-500 pool (~$3.5M) the position sits in — a small swap
        # routes to the thin fee-100 pool (~$0.23M) and accrues fees elsewhere
        # (the VIB-4314 cross-tier blocker on the sister-chain tests). 20,000
        # USDG at 0.05% = 10 USDG of fees spread across in-range liquidity.
        swap_intent = SwapIntent(
            from_token="USDG",
            to_token="WETH",
            amount=Decimal("20000"),
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
            f"Fee-accrual swap must execute so LP_COLLECT_FEES runs on a "
            f"fee-accrued position. Error: {swap_result.error}"
        )
        print("Executed swap to generate LP fees")

        # 3. Record balances BEFORE fee collection.
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdg_before = get_token_balance(web3, usdg_addr, funded_wallet)
        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")
        print(f"USDG before: {format_token_amount(usdg_before, usdg_decimals)}")

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

        # 5. Parse receipts — Layer 3 strict: Collect event amounts > 0.
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

        assert saw_collect, "Receipt must contain a Collect event from LP_COLLECT_FEES"
        assert parsed_amount0_collected > 0 or parsed_amount1_collected > 0, (
            f"Parser must report positive collected amounts. "
            f"amount0={parsed_amount0_collected}, amount1={parsed_amount1_collected}"
        )

        # 6. Principal liquidity unchanged (fees-only, not LP_CLOSE).
        liquidity_after = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity_after == liquidity_before, (
            f"LP_COLLECT_FEES must NOT remove liquidity. "
            f"before={liquidity_before}, after={liquidity_after}"
        )

        # 7. Layer 4 strict: wallet deltas exactly equal parsed amounts.
        # WETH (0x0Bd7…) < USDG (0x5fc5…) by address, so token0=WETH, token1=USDG.
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdg_after = get_token_balance(web3, usdg_addr, funded_wallet)
        weth_received = weth_after - weth_before
        usdg_received = usdg_after - usdg_before

        print(f"\nWETH received: {format_token_amount(weth_received, weth_decimals)}")
        print(f"USDG received: {format_token_amount(usdg_received, usdg_decimals)}")

        if int(weth_addr, 16) < int(usdg_addr, 16):
            parsed_weth_collected, parsed_usdg_collected = (
                parsed_amount0_collected,
                parsed_amount1_collected,
            )
        else:
            parsed_usdg_collected, parsed_weth_collected = (
                parsed_amount0_collected,
                parsed_amount1_collected,
            )

        assert weth_received == parsed_weth_collected, (
            f"WETH wallet delta must exactly equal parsed Collect amount. "
            f"wallet={weth_received}, parsed={parsed_weth_collected}"
        )
        assert usdg_received == parsed_usdg_collected, (
            f"USDG wallet delta must exactly equal parsed Collect amount. "
            f"wallet={usdg_received}, parsed={parsed_usdg_collected}"
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
