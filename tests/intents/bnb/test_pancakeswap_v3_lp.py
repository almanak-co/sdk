"""Production-grade LP Intent tests for PancakeSwap V3 on BNB Chain.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Opening concentrated liquidity positions
- LPCloseIntent: Closing positions with various states
- CollectFeesIntent: Harvesting accrued fees without closing

PancakeSwap V3 is the dominant DEX on BNB. The canonical liquid pair
is USDT/WBNB at fee tier 500 (0.05%, PancakeSwap V3 tier).

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/bnb/test_pancakeswap_v3_lp.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.pancakeswap_v3.receipt_parser import PancakeSwapV3ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import (
    LP_POSITION_MANAGERS,
    IntentCompiler,
    LPCloseIntent,
    LPOpenIntent,
    SwapIntent,
)
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "bsc"  # Framework canonical name for BNB Chain
POSITION_MANAGER = LP_POSITION_MANAGERS["bsc"]["pancakeswap_v3"]

# Pool: USDT/WBNB 0.05% fee tier (canonical liquid pair on BNB Chain)
# PancakeSwap V3 fee tiers: 100, 500, 2500, 10000
POOL = "USDT/WBNB/500"
LP_AMOUNT_USDT = Decimal("100")   # amount0 candidate (sorted by address)
LP_AMOUNT_WBNB = Decimal("0.1")   # amount1 candidate
# The compiler auto-sorts tokens and inverts the range when needed.

# Wide price range in WBNB-per-USDT terms (the pool's natural ordering can vary)
RANGE_LOWER = Decimal("0.0005")   # ~ 1 USDT for 0.0005 BNB (BNB at $2000)
RANGE_UPPER = Decimal("0.05")     # ~ 1 USDT for 0.05 BNB (BNB at $20)


# =============================================================================
# Helpers
# =============================================================================


def _query_position_liquidity(web3: Web3, position_manager: str, token_id: int) -> int:
    """Query position liquidity from NonfungiblePositionManager.positions()."""
    selector = "0x99fbab88"  # positions(uint256)
    data = selector + hex(token_id)[2:].zfill(64)
    result = web3.eth.call({"to": Web3.to_checksum_address(position_manager), "data": data})
    # positions() returns: nonce(0), operator(1), token0(2), token1(3),
    # fee(4), tickLower(5), tickUpper(6), liquidity(7), ...
    liquidity_offset = 7 * 32
    return int.from_bytes(result[liquidity_offset : liquidity_offset + 32], byteorder="big")


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
        protocol="pancakeswap_v3",
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

    parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)
    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            pos_id = parser.extract_position_id(tx_result.receipt.to_dict())
            if pos_id is not None:
                return pos_id

    raise AssertionError("Failed to extract position ID from LP Open receipt")


# =============================================================================
# LPOpenIntent Tests
# =============================================================================


@pytest.mark.bsc
@pytest.mark.lp
class TestPancakeSwapV3LPOpenIntent:
    """Test PancakeSwap V3 LP Open using LPOpenIntent on BNB Chain."""

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
        print("Test: LP Open USDT/WBNB via LPOpenIntent (PancakeSwap V3 on BNB)")
        print(f"{'=' * 80}")
        print(f"Pool: {POOL}")
        print(f"Position Manager: {POSITION_MANAGER}")

        # 1. Record balances BEFORE
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_before = get_token_balance(web3, wbnb_addr, funded_wallet)
        assert usdt_before > 0, "funded_wallet has no USDT — fixture funding failed"
        assert wbnb_before > 0, "funded_wallet has no WBNB — fixture funding failed"

        print(f"USDT before: {format_token_amount(usdt_before, usdt_decimals)}")
        print(f"WBNB before: {format_token_amount(wbnb_before, wbnb_decimals)}")

        # 2. Create LPOpenIntent
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_USDT,
            amount1=LP_AMOUNT_WBNB,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="pancakeswap_v3",
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
        assert compilation_result.action_bundle is not None

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # 4. Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # 5. Parse receipts - extract position ID
        parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)
        position_id = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                pos_id = parser.extract_position_id(tx_result.receipt.to_dict())
                if pos_id is not None:
                    position_id = pos_id

        assert position_id is not None, "Must extract position ID from mint receipt"
        print(f"\nPosition ID: {position_id}")

        # 6. Verify on-chain position has liquidity
        liquidity = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
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

        print("\nALL CHECKS PASSED")


# =============================================================================
# LPCloseIntent Tests
# =============================================================================


@pytest.mark.bsc
@pytest.mark.lp
class TestPancakeSwapV3LPCloseIntent:
    """Test PancakeSwap V3 LP Close using LPCloseIntent on BNB Chain."""

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
        """Test closing a PancakeSwap V3 position that has liquidity.

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
        print("Test: LP Close - Position with Liquidity (PancakeSwap V3 on BNB)")
        print(f"{'=' * 80}")

        # 1. Open position
        position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print(f"Opened position #{position_id}")

        # 2. Verify it has liquidity
        liquidity = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
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
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        compilation_result = compiler.compile(close_intent)
        assert compilation_result.status.value == "SUCCESS", f"LP Close compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"LP Close execution failed: {execution_result.error}"

        # 5. Parse receipts
        parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)
        lp_close_data = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                data = parser.extract_lp_close_data(tx_result.receipt.to_dict())
                if data:
                    lp_close_data = data

        assert lp_close_data is not None, "Must extract LP close data from receipt"
        assert lp_close_data.amount0_collected > 0 or lp_close_data.amount1_collected > 0, (
            "At least one collected amount must be positive"
        )

        # 6. Verify tokens returned
        usdt_after_close = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_after_close = get_token_balance(web3, wbnb_addr, funded_wallet)

        usdt_returned = usdt_after_close - usdt_before_close
        wbnb_returned = wbnb_after_close - wbnb_before_close

        print(f"\nUSDT returned: {format_token_amount(usdt_returned, usdt_decimals)}")
        print(f"WBNB returned: {format_token_amount(wbnb_returned, wbnb_decimals)}")

        assert usdt_returned > 0 or wbnb_returned > 0, (
            f"Must receive tokens back when closing position. "
            f"USDT returned: {usdt_returned}, WBNB returned: {wbnb_returned}"
        )

        print("\nALL CHECKS PASSED")


# =============================================================================
# CollectFeesIntent Tests (LP_COLLECT_FEES) — VIB-4307
# =============================================================================


@pytest.mark.bsc
@pytest.mark.lp
class TestPancakeSwapV3CollectFeesIntent:
    """Test PancakeSwap V3 LP_COLLECT_FEES using CollectFeesIntent on BNB Chain.

    Flow (4 layers):
      1. Open an in-range LP position via LPOpenIntent.
      2. Execute a swap through the same pool to accrue fees on the position.
      3. Issue CollectFeesIntent(protocol="pancakeswap_v3", protocol_params={"position_id": ...}).
      4. Verify wallet balances increased (fees were transferred to the wallet).
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
        """Collect fees from an in-range PancakeSwap V3 USDT/WBNB position after a swap.

        Asserts:
        * Compilation of CollectFeesIntent (pancakeswap_v3) -> SUCCESS.
        * Execution -> success.
        * Position liquidity unchanged (fee harvest does not remove principal).
        * At least one wallet balance strictly increases.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]
        wbnb_addr = tokens["WBNB"]
        usdt_decimals = get_token_decimals(web3, usdt_addr)
        wbnb_decimals = get_token_decimals(web3, wbnb_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP_COLLECT_FEES USDT/WBNB via PancakeSwap V3 on BNB")
        print(f"{'=' * 80}")

        # 1. Open an in-range position to accrue fees against.
        position_id = await _open_position_via_intent(
            funded_wallet, orchestrator, price_oracle, anvil_rpc_url,
        )
        print(f"Opened position #{position_id}")

        liquidity_before = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity_before > 0, "Setup LP_OPEN must yield positive liquidity"

        # 2. Execute a same-pool swap to generate trading fees.
        swap_intent = SwapIntent(
            from_token="USDT",
            to_token="WBNB",
            amount=Decimal("100"),
            max_slippage=Decimal("0.20"),
            protocol="pancakeswap_v3",
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

        # 3. Record balances BEFORE fee collection
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_before = get_token_balance(web3, wbnb_addr, funded_wallet)
        print(f"USDT before: {format_token_amount(usdt_before, usdt_decimals)}")
        print(f"WBNB before: {format_token_amount(wbnb_before, wbnb_decimals)}")

        # 4. Issue the LP_COLLECT_FEES intent.
        collect_intent = CollectFeesIntent(
            pool=POOL,
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
            protocol_params={"position_id": position_id},
        )

        print("\nCompiling CollectFeesIntent...")
        compilation_result = compiler.compile(collect_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"CollectFees compilation must succeed (pancakeswap_v3 LP_COLLECT_FEES). "
            f"Error: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"CollectFees execution failed: {execution_result.error}"

        # 5. Parse receipts
        parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                if parse_result.success:
                    print(f"  Swaps parsed: {len(parse_result.swaps)}")

        # 6. Verify principal liquidity is unchanged.
        liquidity_after = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity_after == liquidity_before, (
            f"LP_COLLECT_FEES must NOT remove liquidity. "
            f"before={liquidity_before}, after={liquidity_after}"
        )

        # 7. Verify wallet balances increased (fees were transferred).
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        wbnb_after = get_token_balance(web3, wbnb_addr, funded_wallet)
        usdt_received = usdt_after - usdt_before
        wbnb_received = wbnb_after - wbnb_before

        print(f"\nUSDT received: {format_token_amount(usdt_received, usdt_decimals)}")
        print(f"WBNB received: {format_token_amount(wbnb_received, wbnb_decimals)}")

        assert usdt_received >= 0 and wbnb_received >= 0, (
            "Fee collection must not decrease wallet balances"
        )
        assert usdt_received > 0 or wbnb_received > 0, (
            f"At least one token must increase after fee collection. "
            f"USDT received: {usdt_received}, WBNB received: {wbnb_received}"
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
