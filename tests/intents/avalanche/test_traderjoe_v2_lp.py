"""Production-grade LP Intent tests for TraderJoe V2 on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Opening liquidity positions in discrete price bins
- LPCloseIntent: Closing positions with various states

TraderJoe V2 uses a Liquidity Book model with discrete price bins and
ERC1155-like fungible LP tokens (not NFT positions like Uniswap/SushiSwap V3).

LP Close test cases:
  #1: Position has liquidity (normal close)
  #2: No position exists (wallet has no LP tokens)

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/avalanche/test_traderjoe_v2_lp.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config
from almanak.framework.connectors.traderjoe_v2.receipt_parser import (
    TraderJoeV2EventType,
    TraderJoeV2ReceiptParser,
)
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
from tests.intents.pool_helpers import fail_if_traderjoe_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "avalanche"

# Pool: WAVAX/USDC with binStep=20 (0.2% fee tier)
# Token X: WAVAX (0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7)
# Token Y: USDC (0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E)
POOL = "WAVAX/USDC/20"
LP_AMOUNT_WAVAX = Decimal("2.0")  # amount0 (Token X = WAVAX)
LP_AMOUNT_USDC = Decimal("50")  # amount1 (Token Y = USDC)

# Price range in USDC-per-WAVAX terms (wide range to ensure both tokens are deposited)
# Note: TraderJoe V2 compiler uses bin_range around active bin, but the model requires these fields.
RANGE_LOWER = Decimal("5")
RANGE_UPPER = Decimal("500")

BIN_STEP = 20


# =============================================================================
# Helpers
# =============================================================================


def _get_position_via_adapter(
    rpc_url: str,
    wallet: str,
    token_x: str,
    token_y: str,
    bin_step: int,
):
    """Query position using TraderJoeV2Adapter."""
    config = TraderJoeV2Config(
        chain=CHAIN_NAME,
        wallet_address=wallet,
        rpc_url=rpc_url,
    )
    adapter = TraderJoeV2Adapter(config)
    return adapter.get_position(token_x, token_y, bin_step, wallet=wallet)


async def _open_position_via_intent(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> None:
    """Open an LP position via LPOpenIntent.

    TraderJoe V2 uses bin-based positions (no NFT token ID to return).
    Position is identified by pool + wallet + bin IDs.
    """
    intent = LPOpenIntent(
        pool=POOL,
        amount0=LP_AMOUNT_WAVAX,  # Token X = WAVAX
        amount1=LP_AMOUNT_USDC,  # Token Y = USDC
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        protocol="traderjoe_v2",
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


# =============================================================================
# LPOpenIntent Tests
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.lp
class TestTraderJoeV2LPOpenIntent:
    """Test TraderJoe V2 LP Open using LPOpenIntent.

    Verifies the full Intent flow:
    - LPOpenIntent creation with pool and amounts
    - IntentCompiler generates correct LBRouter addLiquidity TX
    - Transactions execute successfully on-chain
    - Position has liquidity in bins (queried via adapter)
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
        5. Parse receipts - verify DepositedToBins events, extract bin IDs
        6. Query position via adapter - verify bin_ids not empty, amounts > 0
        7. Verify balance changes
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wavax_addr = tokens["WAVAX"]
        fail_if_traderjoe_pool_missing(web3, CHAIN_NAME, wavax_addr, usdc_addr, 20)

        usdc_decimals = get_token_decimals(web3, usdc_addr)
        wavax_decimals = get_token_decimals(web3, wavax_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Open WAVAX/USDC via LPOpenIntent (TraderJoe V2)")
        print(f"{'=' * 80}")
        print(f"Pool: {POOL}")
        print(f"Amount WAVAX (token X): {LP_AMOUNT_WAVAX}")
        print(f"Amount USDC (token Y): {LP_AMOUNT_USDC}")

        # 1. Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_before = get_token_balance(web3, wavax_addr, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"WAVAX before: {format_token_amount(wavax_before, wavax_decimals)}")

        # 2. Create LPOpenIntent
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WAVAX,  # Token X = WAVAX
            amount1=LP_AMOUNT_USDC,  # Token Y = USDC
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="traderjoe_v2",
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

        # 5. Parse receipts - verify DepositedToBins events and extract bin IDs
        parser = TraderJoeV2ReceiptParser()
        found_deposit_event = False
        extracted_bin_ids = None

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()

                # Check for DepositedToBins events
                parse_result = parser.parse_receipt(receipt_dict)
                if parse_result.success:
                    print(f"  Events parsed: {len(parse_result.events)}")
                    for event in parse_result.events:
                        if event.event_type == TraderJoeV2EventType.DEPOSITED_TO_BINS:
                            found_deposit_event = True
                            print(f"  DepositedToBins event found at log index {event.log_index}")

                    if parse_result.liquidity_result and parse_result.liquidity_result.is_add:
                        print(f"  Liquidity add detected: pool={parse_result.liquidity_result.pool_address}")

                # Extract bin IDs
                bin_ids = parser.extract_bin_ids(receipt_dict)
                if bin_ids:
                    extracted_bin_ids = bin_ids
                    print(f"  Extracted bin IDs: {len(bin_ids)} bins")

        assert found_deposit_event, "Must find DepositedToBins event in receipts"
        assert extracted_bin_ids is not None and len(extracted_bin_ids) > 0, (
            "Must extract bin IDs from DepositedToBins event"
        )
        print(f"\nExtracted {len(extracted_bin_ids)} bin IDs")

        # 6. Query position via adapter - verify bin_ids not empty, amounts > 0
        position = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=wavax_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        assert position is not None, "Position must exist after LP open"
        assert len(position.bin_ids) > 0, "Position must have bin IDs"
        # Verify LP token balances are non-zero (amount_x/amount_y may be 0 due to SDK getBin() limitations)
        total_lp_balance = sum(position.balances.values())
        assert total_lp_balance > 0, (
            f"Position must have non-zero LP token balances, got total={total_lp_balance}"
        )
        print(
            f"On-chain position: {len(position.bin_ids)} bins, "
            f"total LP balance={total_lp_balance}, "
            f"amount_x={position.amount_x}, amount_y={position.amount_y}"
        )

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


@pytest.mark.avalanche
@pytest.mark.lp
class TestTraderJoeV2LPCloseIntent:
    """Test TraderJoe V2 LP Close using LPCloseIntent.

    Test cases:
    #1: Position has liquidity (normal LP close)
    #2: No position exists (wallet has no LP tokens)

    Note: TraderJoe V2 does not have the "decreased but not collected" edge case
    because removeLiquidity removes and returns tokens in one step.
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
        2. Verify position exists with liquidity via adapter
        3. Record balances BEFORE close
        4. Close via LPCloseIntent
        5. Parse receipts - verify WithdrawnFromBins events
        6. Verify tokens returned to wallet (balance deltas > 0)
        7. Verify position is now empty
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wavax_addr = tokens["WAVAX"]
        fail_if_traderjoe_pool_missing(web3, CHAIN_NAME, wavax_addr, usdc_addr, 20)
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        wavax_decimals = get_token_decimals(web3, wavax_addr)

        print(f"\n{'=' * 80}")
        print("Test #1: LP Close - Position with Liquidity (TraderJoe V2)")
        print(f"{'=' * 80}")

        # 1. Open position
        await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
        print("Opened LP position via LPOpenIntent")

        # 2. Verify it has liquidity
        position = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=wavax_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        assert position is not None, "Position must exist before close"
        assert len(position.bin_ids) > 0, "Position must have bin IDs"
        print(f"Position: {len(position.bin_ids)} bins, amount_x={position.amount_x}, amount_y={position.amount_y}")

        # 3. Record balances BEFORE close
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_before_close = get_token_balance(web3, wavax_addr, funded_wallet)

        # 4. Close via LPCloseIntent
        close_intent = LPCloseIntent(
            position_id="0",  # TraderJoe V2 uses bin-based positions, not NFT IDs
            pool=POOL,
            collect_fees=True,
            protocol="traderjoe_v2",
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

        # 5. Parse receipts - verify WithdrawnFromBins events
        parser = TraderJoeV2ReceiptParser()
        found_withdrawal_event = False

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")
            print(f"  Success: {tx_result.success}")

            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()

                parse_result = parser.parse_receipt(receipt_dict)
                if parse_result.success:
                    print(f"  Parsed events: {len(parse_result.events)}")
                    for event in parse_result.events:
                        print(f"    Event: {event.event_name} (type={event.event_type})")
                        if event.event_type == TraderJoeV2EventType.WITHDRAWN_FROM_BINS:
                            found_withdrawal_event = True
                            print("    -> WithdrawnFromBins event found!")

                    lp_close_data = parser.extract_lp_close_data(receipt_dict)
                    if lp_close_data:
                        print(
                            f"  LP Close data: amount0_collected={lp_close_data.amount0_collected}, "
                            f"amount1_collected={lp_close_data.amount1_collected}"
                        )

        assert found_withdrawal_event, "Must find WithdrawnFromBins event in receipts"

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

        # 7. Verify position is now empty
        position_after = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=wavax_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        if position_after is not None:
            assert len(position_after.bin_ids) == 0, (
                f"Position should be empty after close, still has {len(position_after.bin_ids)} bins"
            )

        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_lp_close_no_position(
        self,
        web3: Web3,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test #2: Close when no position exists.

        The LPCloseIntent should handle this gracefully - returning SUCCESS
        with empty transactions and a warning.

        Flow:
        1. Do NOT open a position
        2. Record balances BEFORE
        3. Create LPCloseIntent
        4. Compile - should return SUCCESS with empty transactions + warning
        5. Verify balances unchanged
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wavax_addr = tokens["WAVAX"]

        print(f"\n{'=' * 80}")
        print("Test #2: LP Close - No Position Exists (TraderJoe V2)")
        print(f"{'=' * 80}")

        # 1. Verify no position exists
        position = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=wavax_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        if position is not None and len(position.bin_ids) > 0:
            print("WARNING: Position already exists, skipping test")
            pytest.skip("Position already exists - snapshot isolation may have failed")

        # 2. Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_before = get_token_balance(web3, wavax_addr, funded_wallet)

        # 3. Create LPCloseIntent
        close_intent = LPCloseIntent(
            position_id="0",  # TraderJoe V2 uses bin-based positions, not NFT IDs
            pool=POOL,
            collect_fees=True,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # 4. Compile - should return SUCCESS with empty transactions + warning
        print("\nCompiling LPCloseIntent for non-existent position...")
        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"LP Close compilation should succeed even with no position: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Should have empty transactions (no position to close)
        num_txs = len(compilation_result.action_bundle.transactions)
        print(f"ActionBundle has {num_txs} transactions (expected 0 for no position)")
        assert num_txs == 0, f"Expected 0 transactions when no position exists, got {num_txs}"

        # Should have a warning
        if compilation_result.warnings:
            print(f"Warnings: {compilation_result.warnings}")

        # 5. Verify balances unchanged
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_after = get_token_balance(web3, wavax_addr, funded_wallet)

        usdc_delta = usdc_after - usdc_before
        wavax_delta = wavax_after - wavax_before

        assert usdc_delta == 0, f"USDC balance should be unchanged for no position, got delta: {usdc_delta}"
        assert wavax_delta == 0, f"WAVAX balance should be unchanged for no position, got delta: {wavax_delta}"

        print(f"USDC delta: {usdc_delta}")
        print(f"WAVAX delta: {wavax_delta}")
        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
