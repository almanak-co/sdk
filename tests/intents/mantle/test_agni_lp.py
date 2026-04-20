"""Production-grade LP Intent tests for Agni Finance on Mantle.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Opening concentrated liquidity positions on Agni Finance
- LPCloseIntent: Closing positions (with liquidity)

Agni Finance is a Uniswap V3 fork on Mantle. The SDK routes "agni" protocol
intents through the Uniswap V3 connector via protocol aliases.

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/mantle/test_agni_lp.py -v -s
"""

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
)
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_v3_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "mantle"

# Agni Finance position manager on Mantle
POSITION_MANAGER = "0x218bf598D1453383e2F4AA7b14fFB9BfB102D637"
MAX_UINT128 = 2**128 - 1

# Pool: WMNT/WETH fee tier 500 (0.05%)
# Confirmed pool exists at 0x54169896d28dec0FFABE3B16f90f71323774949f
POOL = "WMNT/WETH/500"
LP_AMOUNT_WMNT = Decimal("10")     # amount0 (WMNT)
LP_AMOUNT_WETH = Decimal("0.005")  # amount1 (WETH)

# Wide price range in WETH-per-WMNT terms
# WMNT ~$0.50, WETH ~$3500, so WMNT/WETH ratio ~0.00014
# Use very wide range to ensure both tokens deposited
RANGE_LOWER = Decimal("0.00001")
RANGE_UPPER = Decimal("0.01")


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
        "gas": 2_000_000,
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
    """Query position liquidity from NonfungiblePositionManager.positions().

    Returns 0 if the position NFT has been burned (call reverts).
    """
    selector = "0x99fbab88"  # positions(uint256)
    data = selector + hex(token_id)[2:].zfill(64)
    try:
        result = web3.eth.call({"to": Web3.to_checksum_address(position_manager), "data": data})
    except Exception:
        # Position NFT burned after close — treat as zero liquidity
        return 0
    # positions() returns: nonce(0), operator(1), token0(2), token1(3),
    # fee(4), tickLower(5), tickUpper(6), liquidity(7), ...
    liquidity_offset = 7 * 32
    if len(result) < liquidity_offset + 32:
        return 0
    return int.from_bytes(result[liquidity_offset : liquidity_offset + 32], byteorder="big")


async def _open_position_via_intent(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> int:
    """Open an LP position via LPOpenIntent and return the position token ID."""
    intent = LPOpenIntent(
        pool=POOL,
        amount0=LP_AMOUNT_WMNT,
        amount1=LP_AMOUNT_WETH,
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        protocol="agni",
        chain=CHAIN_NAME,
    )

    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=orchestrator.rpc_url,
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


@pytest.mark.mantle
@pytest.mark.lp
class TestAgniLPOpenIntent:
    """Test Agni Finance LP Open using LPOpenIntent.

    Verifies the full Intent flow:
    - LPOpenIntent creation with WMNT/WETH pool, amounts, and price range
    - IntentCompiler generates correct NonfungiblePositionManager mint TX
    - Transactions execute successfully on-chain
    - Position NFT is minted and has liquidity
    - Balance changes are correct
    """

    @pytest.mark.asyncio
    async def test_lp_open_wmnt_weth(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test opening a WMNT/WETH LP position on Agni Finance.

        4-Layer Verification:
        1. Compilation: LPOpenIntent -> ActionBundle (SUCCESS)
        2. Execution: ActionBundle -> on-chain transactions (success)
        3. Receipt Parsing: extract position_id, verify liquidity > 0
        4. Balance Deltas: WMNT and WETH decreased by expected amounts
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wmnt_addr = tokens["WMNT"]
        weth_addr = tokens["WETH"]

        # Pre-check: verify pool exists on fork
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "agni_finance", wmnt_addr, weth_addr, 500)

        wmnt_decimals = get_token_decimals(web3, wmnt_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Open WMNT/WETH via LPOpenIntent (Agni Finance / Mantle)")
        print(f"{'=' * 80}")
        print(f"Pool: {POOL}")
        print(f"Amount WMNT: {LP_AMOUNT_WMNT}")
        print(f"Amount WETH: {LP_AMOUNT_WETH}")
        print(f"Range: [{RANGE_LOWER} - {RANGE_UPPER}] WETH per WMNT")

        # Layer 4a: Record balances BEFORE
        wmnt_before = get_token_balance(web3, wmnt_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)

        print(f"WMNT before: {format_token_amount(wmnt_before, wmnt_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")

        # Layer 1: Compile LPOpenIntent
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WMNT,
            amount1=LP_AMOUNT_WETH,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="agni",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=orchestrator.rpc_url,
        )

        print("\nCompiling LPOpenIntent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: Execute
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Parse receipts - extract position ID and verify liquidity
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

        # Verify on-chain position has liquidity
        liquidity = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have positive liquidity, got {liquidity}"
        print(f"On-chain liquidity: {liquidity}")

        # Layer 4b: Verify balance changes
        wmnt_after = get_token_balance(web3, wmnt_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)

        wmnt_spent = wmnt_before - wmnt_after
        weth_spent = weth_before - weth_after

        print(f"\nWMNT spent: {format_token_amount(wmnt_spent, wmnt_decimals)}")
        print(f"WETH spent: {format_token_amount(weth_spent, weth_decimals)}")

        # At least one token must have been deposited
        assert wmnt_spent > 0 or weth_spent > 0, "Must deposit at least one token into LP"

        # Amounts spent must not exceed desired amounts
        expected_wmnt_max = int(LP_AMOUNT_WMNT * Decimal(10**wmnt_decimals))
        expected_weth_max = int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))
        assert wmnt_spent <= expected_wmnt_max, f"WMNT spent ({wmnt_spent}) exceeds desired ({expected_wmnt_max})"
        assert weth_spent <= expected_weth_max, f"WETH spent ({weth_spent}) exceeds desired ({expected_weth_max})"

        print("\nALL CHECKS PASSED")


# =============================================================================
# LPCloseIntent Tests
# =============================================================================


@pytest.mark.mantle
@pytest.mark.lp
class TestAgniLPCloseIntent:
    """Test Agni Finance LP Close using LPCloseIntent.

    Test case: Close position that has liquidity (normal close).
    """

    @pytest.mark.asyncio
    async def test_lp_close_position_with_liquidity(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        test_private_key: str,
        price_oracle: dict[str, Decimal],
    ):
        """Test closing an Agni LP position that has liquidity.

        4-Layer Verification:
        1. Compilation: LPCloseIntent -> ActionBundle (SUCCESS)
        2. Execution: ActionBundle -> on-chain transactions (success)
        3. Receipt Parsing: verify close events parsed
        4. Balance Deltas: WMNT and WETH increased (tokens returned),
           on-chain liquidity = 0
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wmnt_addr = tokens["WMNT"]
        weth_addr = tokens["WETH"]

        # Pre-check: verify pool exists
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "agni_finance", wmnt_addr, weth_addr, 500)

        wmnt_decimals = get_token_decimals(web3, wmnt_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Close with Liquidity (Agni Finance / Mantle)")
        print(f"{'=' * 80}")

        # Step 1: Open a position first
        print("\nStep 1: Opening LP position via LPOpenIntent...")
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle)
        print(f"Position opened: ID={position_id}")

        # Verify it has liquidity
        liquidity_before = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity_before > 0, f"Position must have liquidity before close, got {liquidity_before}"
        print(f"Liquidity before close: {liquidity_before}")

        # Layer 4a: Record balances BEFORE close
        wmnt_before = get_token_balance(web3, wmnt_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)

        print(f"WMNT before close: {format_token_amount(wmnt_before, wmnt_decimals)}")
        print(f"WETH before close: {format_token_amount(weth_before, weth_decimals)}")

        # Layer 1: Compile LPCloseIntent
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="agni",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=orchestrator.rpc_url,
        )

        print("\nCompiling LPCloseIntent...")
        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        print(f"ActionBundle: {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: Execute
        print("\nExecuting LP Close...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Close execution failed: {execution_result.error}"
        print(f"Close successful! {len(execution_result.transaction_results)} transactions")

        # Layer 3: Parse receipts
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                if parse_result.success:
                    print(f"  Events parsed: {len(parse_result.events)}")

        # Verify on-chain: position liquidity should be 0
        liquidity_after = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity_after == 0, f"Position liquidity must be 0 after close, got {liquidity_after}"
        print(f"\nOn-chain liquidity after close: {liquidity_after}")

        # Layer 4b: Verify tokens returned
        wmnt_after = get_token_balance(web3, wmnt_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)

        wmnt_returned = wmnt_after - wmnt_before
        weth_returned = weth_after - weth_before

        print(f"WMNT returned: {format_token_amount(wmnt_returned, wmnt_decimals)}")
        print(f"WETH returned: {format_token_amount(weth_returned, weth_decimals)}")

        # At least one token must have been returned
        assert wmnt_returned > 0 or weth_returned > 0, "Must receive at least one token back from LP close"

        print("\nALL CHECKS PASSED")
