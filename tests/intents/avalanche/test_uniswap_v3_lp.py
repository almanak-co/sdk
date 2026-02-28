"""Production-grade LP Intent tests for Uniswap V3 on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Opening concentrated liquidity positions
- LPCloseIntent: Closing positions with various states

LP Close test cases:
  #1: Position has liquidity + fees (normal close)
  #2: Position has no liquidity and no fees (already decreased + collected)
  #3: Position has no liquidity but owed tokens (decreased but not collected)

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/avalanche/test_uniswap_v3_lp.py -v -s
"""

import time
from decimal import Decimal

import pytest
from eth_account import Account
from web3 import Web3

from almanak.framework.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import (
    IntentCompiler,
    LPCloseIntent,
    LPOpenIntent,
    SwapIntent,
    UniswapV3LPAdapter,
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

CHAIN_NAME = "avalanche"
POSITION_MANAGER = "0x655C406EBFa14EE2006250925e54ec43AD184f8B"
MAX_UINT128 = 2**128 - 1

# Pool: WAVAX/USDC 0.05% fee tier
# Token ordering by address on Avalanche:
#   WAVAX (0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7)
#   USDC  (0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E)
# WAVAX < USDC, so WAVAX=token0, USDC=token1
# Range is in USDC-per-WAVAX terms
POOL = "WAVAX/USDC/500"
LP_AMOUNT_WAVAX = Decimal("2.0")   # amount0 (WAVAX is token0 on Avalanche)
LP_AMOUNT_USDC = Decimal("50")     # amount1 (USDC is token1 on Avalanche)

# Wide price range in USDC-per-WAVAX terms to ensure both tokens are deposited
# range_lower=5   -> AVAX at $5
# range_upper=500 -> AVAX at $500
RANGE_LOWER = Decimal("5")
RANGE_UPPER = Decimal("500")


# =============================================================================
# Helpers
# =============================================================================


def _send_raw_tx(web3: Web3, private_key: str, to: str, data: bytes, value: int = 0) -> dict:
    """Send a raw transaction on Anvil."""
    account = Account.from_key(private_key)
    nonce = web3.eth.get_transaction_count(account.address)
    tx = {
        "to": Web3.to_checksum_address(to),
        "data": "0x" + data.hex(),
        "value": value,
        "gas": 1_000_000,
        "gasPrice": web3.eth.gas_price,
        "nonce": nonce,
        "chainId": web3.eth.chain_id,
    }
    signed = account.sign_transaction(tx)
    raw_tx = getattr(signed, "rawTransaction", None) or getattr(signed, "raw_transaction", None)
    if raw_tx is None:
        raise AssertionError("Signed transaction is missing raw transaction bytes")
    tx_hash = web3.eth.send_raw_transaction(raw_tx)
    return web3.eth.wait_for_transaction_receipt(tx_hash)


def _query_position_liquidity(web3: Web3, position_manager: str, token_id: int) -> int:
    """Query position liquidity from NonfungiblePositionManager.positions()."""
    selector = "0x99fbab88"  # positions(uint256)
    data = selector + hex(token_id)[2:].zfill(64)
    result = web3.eth.call({"to": Web3.to_checksum_address(position_manager), "data": data})
    # positions() returns: nonce(0), operator(1), token0(2), token1(3),
    # fee(4), tickLower(5), tickUpper(6), liquidity(7), ...
    liquidity_offset = 7 * 32
    return int.from_bytes(result[liquidity_offset : liquidity_offset + 32], byteorder="big")


def _decrease_all_liquidity(web3: Web3, private_key: str, position_manager: str, token_id: int) -> None:
    """Decrease all liquidity from a position via direct contract call."""
    liquidity = _query_position_liquidity(web3, position_manager, token_id)
    if liquidity == 0:
        return

    adapter = UniswapV3LPAdapter(chain=CHAIN_NAME)
    deadline = int(time.time()) + 86400
    calldata = adapter.get_decrease_liquidity_calldata(
        token_id=token_id,
        liquidity=liquidity,
        amount0_min=0,
        amount1_min=0,
        deadline=deadline,
    )
    receipt = _send_raw_tx(web3, private_key, position_manager, calldata)
    assert receipt["status"] == 1, "decreaseLiquidity direct call failed"


def _collect_all_tokens(web3: Web3, private_key: str, position_manager: str, token_id: int, recipient: str) -> None:
    """Collect all owed tokens from a position via direct contract call."""
    adapter = UniswapV3LPAdapter(chain=CHAIN_NAME)
    calldata = adapter.get_collect_calldata(
        token_id=token_id,
        recipient=recipient,
        amount0_max=MAX_UINT128,
        amount1_max=MAX_UINT128,
    )
    receipt = _send_raw_tx(web3, private_key, position_manager, calldata)
    assert receipt["status"] == 1, "collect direct call failed"


async def _open_position_via_intent(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> int:
    """Open an LP position via LPOpenIntent and return the position token ID."""
    intent = LPOpenIntent(
        pool=POOL,
        amount0=LP_AMOUNT_WAVAX,   # WAVAX is token0 on Avalanche
        amount1=LP_AMOUNT_USDC,    # USDC is token1 on Avalanche
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
# Fixtures
# =============================================================================


# =============================================================================
# LPOpenIntent Tests
# =============================================================================


@pytest.mark.xfail(reason="flaky: needs more investigation", strict=False)
@pytest.mark.avalanche
@pytest.mark.lp
class TestUniswapV3LPOpenIntent:
    """Test Uniswap V3 LP Open using LPOpenIntent.

    Verifies the full Intent flow:
    - LPOpenIntent creation with pool, amounts, and price range
    - IntentCompiler generates correct NonfungiblePositionManager mint TX
    - Transactions execute successfully on-chain
    - Position NFT is minted and has liquidity
    - Balance changes are correct
    """

    @pytest.mark.asyncio
    async def test_lp_open_wavax_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test opening a WAVAX/USDC LP position using LPOpenIntent.

        Flow:
        1. Record balances BEFORE
        2. Create LPOpenIntent for WAVAX/USDC pool
        3. Compile to ActionBundle using IntentCompiler
        4. Execute via ExecutionOrchestrator
        5. Parse receipts - extract position ID
        6. Verify on-chain position liquidity
        7. Verify balance changes
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wavax_addr = tokens["WAVAX"]

        usdc_decimals = get_token_decimals(web3, usdc_addr)
        wavax_decimals = get_token_decimals(web3, wavax_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Open WAVAX/USDC via LPOpenIntent")
        print(f"{'=' * 80}")
        print(f"Pool: {POOL}")
        print(f"Amount WAVAX (token0): {LP_AMOUNT_WAVAX}")
        print(f"Amount USDC (token1): {LP_AMOUNT_USDC}")
        print(f"Range: [{RANGE_LOWER} - {RANGE_UPPER}] USDC per WAVAX")

        # 1. Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_before = get_token_balance(web3, wavax_addr, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"WAVAX before: {format_token_amount(wavax_before, wavax_decimals)}")

        # 2. Create LPOpenIntent
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WAVAX,   # WAVAX is token0 on Avalanche
            amount1=LP_AMOUNT_USDC,    # USDC is token1 on Avalanche
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="uniswap_v3",
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
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
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
        liquidity = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have positive liquidity, got {liquidity}"
        print(f"On-chain liquidity: {liquidity}")

        # 7. Verify balance changes
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_after = get_token_balance(web3, wavax_addr, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        wavax_spent = wavax_before - wavax_after

        print(f"\nUSDC spent: {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"WAVAX spent: {format_token_amount(wavax_spent, wavax_decimals)}")

        # At least one token must have been deposited
        assert usdc_spent > 0 or wavax_spent > 0, "Must deposit at least one token into LP"

        # Amounts spent must not exceed desired amounts
        expected_usdc_max = int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        expected_wavax_max = int(LP_AMOUNT_WAVAX * Decimal(10**wavax_decimals))
        assert usdc_spent <= expected_usdc_max, f"USDC spent ({usdc_spent}) exceeds desired ({expected_usdc_max})"
        assert wavax_spent <= expected_wavax_max, f"WAVAX spent ({wavax_spent}) exceeds desired ({expected_wavax_max})"

        print("\nALL CHECKS PASSED")


# =============================================================================
# LPCloseIntent Tests
# =============================================================================


@pytest.mark.xfail(reason="flaky: needs more investigation", strict=False)
@pytest.mark.avalanche
@pytest.mark.lp
class TestUniswapV3LPCloseIntent:
    """Test Uniswap V3 LP Close using LPCloseIntent.

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
        usdc_addr = tokens["USDC"]
        wavax_addr = tokens["WAVAX"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        wavax_decimals = get_token_decimals(web3, wavax_addr)

        print(f"\n{'=' * 80}")
        print("Test #1: LP Close - Position with Liquidity")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Verify it has liquidity
        liquidity = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have liquidity before close, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 3. Record balances BEFORE close
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_before_close = get_token_balance(web3, wavax_addr, funded_wallet)

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

        # 5. Parse receipts
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                lp_close_data = parser.extract_lp_close_data(tx_result.receipt.to_dict())
                if lp_close_data:
                    print(
                        f"  LP Close data: amount0_collected={lp_close_data.amount0_collected}, "
                        f"amount1_collected={lp_close_data.amount1_collected}"
                    )

        # 6. Verify tokens returned
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_after_close = get_token_balance(web3, wavax_addr, funded_wallet)

        usdc_returned = usdc_after_close - usdc_before_close
        wavax_returned = wavax_after_close - wavax_before_close

        print(f"\nUSDC returned: {format_token_amount(usdc_returned, usdc_decimals)}")
        print(f"WAVAX returned: {format_token_amount(wavax_returned, wavax_decimals)}")

        # At least one token must be returned (was deposited in LP)
        assert usdc_returned > 0 or wavax_returned > 0, (
            f"Must receive tokens back when closing position. "
            f"USDC returned: {usdc_returned}, WAVAX returned: {wavax_returned}"
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
        usdc_addr = tokens["USDC"]
        wavax_addr = tokens["WAVAX"]

        print(f"\n{'=' * 80}")
        print("Test #2: LP Close - No Liquidity, No Owed Tokens")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Decrease all liquidity directly
        _decrease_all_liquidity(web3, test_private_key, POSITION_MANAGER, position_id)
        print("Decreased all liquidity via direct call")

        # 3. Collect all owed tokens directly
        _collect_all_tokens(web3, test_private_key, POSITION_MANAGER, position_id, funded_wallet)
        print("Collected all owed tokens via direct call")

        # 4. Verify 0 liquidity
        liquidity = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity == 0, f"Expected 0 liquidity after decrease, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 5. Record balances BEFORE close
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_before_close = get_token_balance(web3, wavax_addr, funded_wallet)

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

        assert execution_result.success, f"LP Close should succeed for empty position. Error: {execution_result.error}"

        # 7. Verify ERC-20 balances unchanged (nothing to collect)
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_after_close = get_token_balance(web3, wavax_addr, funded_wallet)

        usdc_delta = usdc_after_close - usdc_before_close
        wavax_delta = wavax_after_close - wavax_before_close

        assert usdc_delta == 0, f"USDC balance should be unchanged for empty position, got delta: {usdc_delta}"
        assert wavax_delta == 0, f"WAVAX balance should be unchanged for empty position, got delta: {wavax_delta}"

        print(f"USDC delta: {usdc_delta}")
        print(f"WAVAX delta: {wavax_delta}")
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
        wavax_addr = tokens["WAVAX"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        wavax_decimals = get_token_decimals(web3, wavax_addr)

        print(f"\n{'=' * 80}")
        print("Test #3: LP Close - No Liquidity, But Owed Tokens")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Execute a swap through the pool to generate fees
        swap_intent = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
            amount=Decimal("50"),
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
        _decrease_all_liquidity(web3, test_private_key, POSITION_MANAGER, position_id)
        print("Decreased all liquidity via direct call (tokens now owed)")

        # 4. Do NOT collect - leave tokens owed

        # 5. Verify 0 liquidity
        liquidity = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity == 0, f"Expected 0 liquidity after decrease, got {liquidity}"
        print(f"Position liquidity: {liquidity}")

        # 6. Record balances BEFORE close
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_before_close = get_token_balance(web3, wavax_addr, funded_wallet)

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

        # Parse receipts
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                lp_close_data = parser.extract_lp_close_data(tx_result.receipt.to_dict())
                if lp_close_data:
                    print(
                        f"  LP Close data: amount0_collected={lp_close_data.amount0_collected}, "
                        f"amount1_collected={lp_close_data.amount1_collected}"
                    )

        # 8. Verify tokens were collected (owed tokens from decrease + any fees)
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_after_close = get_token_balance(web3, wavax_addr, funded_wallet)

        usdc_collected = usdc_after_close - usdc_before_close
        wavax_collected = wavax_after_close - wavax_before_close

        print(f"\nUSDC collected: {format_token_amount(usdc_collected, usdc_decimals)}")
        print(f"WAVAX collected: {format_token_amount(wavax_collected, wavax_decimals)}")

        # At least one token must be collected (there were owed tokens from the decrease)
        assert usdc_collected > 0 or wavax_collected > 0, (
            f"Must collect owed tokens from decreased position. "
            f"USDC collected: {usdc_collected}, WAVAX collected: {wavax_collected}"
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
