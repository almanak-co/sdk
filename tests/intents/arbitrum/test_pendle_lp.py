"""Production-grade LP Intent tests for Pendle on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Adding single-sided liquidity to a Pendle market
- LPCloseIntent: Removing liquidity and receiving the output token

Pendle LP mechanics differ from Uniswap V3:
- Single-sided liquidity (amount0 only; range_lower/upper are dummies ignored by compiler)
- The market address IS the LP token (no separate NFT)
- position_id = LP token amount in wei (not a numeric NFT ID)
- Output token for LP_CLOSE must be passed via protocol_params={"token": ...}

NO MOCKING. All tests execute real on-chain transactions on an Arbitrum Anvil fork.

To run:
    uv run pytest tests/intents/arbitrum/test_pendle_lp.py -v -s -n0 --import-mode=importlib
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.pendle.receipt_parser import PendleReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import LPCloseIntent, LPOpenIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"

# PT-wstETH-25JUN2026 market on Arbitrum — most liquid Pendle market on this chain.
# The market contract address is also the LP token address for Pendle positions.
PENDLE_WSTETH_MARKET = "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b"

# Input token: wstETH mints SY directly — no pre-swap routing needed.
WSTETH_ADDRESS = "0x5979D7b546E38E414F7E9822514be443A4800529"
WSTETH_SYMBOL = "wstETH"

# Small LP deposit: 0.005 wstETH (~$12 at ~$2400/wstETH).
LP_DEPOSIT_AMOUNT = Decimal("0.005")

# range_lower/upper are required by LPOpenIntent validation but ignored by the
# Pendle compiler (Pendle uses single-sided liquidity with no tick range).
_DUMMY_RANGE_LOWER = Decimal("0.0001")
_DUMMY_RANGE_UPPER = Decimal("999999")


# =============================================================================
# LP_OPEN Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestPendleLPOpenIntent:
    """4-layer tests for Pendle LP_OPEN on Arbitrum.

    Deposits wstETH into the PT-wstETH-25JUN2026 market and verifies:
    1. Compilation succeeds
    2. Execution lands on-chain
    3. PendleReceiptParser finds a Mint event with net_lp_minted > 0
    4. wstETH balance decreased, LP token balance increased
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_wsteth_into_pendle_market(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Open a wstETH LP position in the PT-wstETH-25JUN2026 Pendle market."""
        wsteth_decimals = get_token_decimals(web3, WSTETH_ADDRESS)

        print(f"\n{'='*80}")
        print("Test: LP_OPEN wstETH -> PT-wstETH-25JUN2026 (Pendle)")
        print(f"{'='*80}")
        print(f"Deposit: {LP_DEPOSIT_AMOUNT} {WSTETH_SYMBOL}")

        # Layer 4 setup: record balances BEFORE
        wsteth_before = get_token_balance(web3, WSTETH_ADDRESS, funded_wallet)
        lp_before = get_token_balance(web3, PENDLE_WSTETH_MARKET, funded_wallet)
        print(f"wstETH before:  {format_token_amount(wsteth_before, wsteth_decimals)}")
        print(f"LP before:      {format_token_amount(lp_before, 18)}")

        # Layer 1: Compile
        intent = LPOpenIntent(
            pool=f"{WSTETH_SYMBOL}/{PENDLE_WSTETH_MARKET}",
            amount0=LP_DEPOSIT_AMOUNT,
            amount1=Decimal("0"),
            range_lower=_DUMMY_RANGE_LOWER,
            range_upper=_DUMMY_RANGE_UPPER,
            protocol="pendle",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        tx_count = len(compilation_result.action_bundle.transactions)
        print(f"ActionBundle: {tx_count} transactions")

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful: {len(execution_result.transaction_results)} txs confirmed")

        # Layer 3: Receipt parsing — expect exactly one Mint event
        parser = PendleReceiptParser(chain=CHAIN_NAME)
        lp_minted_raw: int | None = None
        for i, tx_result in enumerate(execution_result.transaction_results):
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            if parse_result.mint_events:
                mint = parse_result.mint_events[0]
                lp_minted_raw = mint.net_lp_minted
                print(
                    f"\nTx {i+1} Mint event:"
                    f"\n  market:        {mint.market_address}"
                    f"\n  net_lp_minted: {mint.net_lp_minted}"
                    f"\n  net_sy_used:   {mint.net_sy_used}"
                    f"\n  net_pt_used:   {mint.net_pt_used}"
                )

        assert lp_minted_raw is not None, "No Mint event found in any transaction receipt"
        assert lp_minted_raw > 0, f"net_lp_minted must be positive, got {lp_minted_raw}"

        # Verify market address in Mint event matches expected market
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            for mint in parse_result.mint_events:
                assert mint.market_address.lower() == PENDLE_WSTETH_MARKET.lower(), (
                    f"Mint market_address mismatch: got {mint.market_address}"
                )

        # Layer 4: Balance deltas
        wsteth_after = get_token_balance(web3, WSTETH_ADDRESS, funded_wallet)
        lp_after = get_token_balance(web3, PENDLE_WSTETH_MARKET, funded_wallet)

        wsteth_spent = wsteth_before - wsteth_after
        lp_received = lp_after - lp_before

        print("\n--- Results ---")
        print(f"wstETH spent:   {format_token_amount(wsteth_spent, wsteth_decimals)}")
        print(f"LP received:    {format_token_amount(lp_received, 18)}")

        expected_wsteth_wei = int(LP_DEPOSIT_AMOUNT * Decimal(10**wsteth_decimals))
        assert wsteth_spent == expected_wsteth_wei, (
            f"wstETH spent must EXACTLY equal deposit amount. "
            f"Expected: {expected_wsteth_wei}, Got: {wsteth_spent}"
        )
        assert lp_received > 0, "LP token balance must increase after LP_OPEN"
        assert lp_received == lp_minted_raw, (
            f"On-chain LP balance delta must match receipt net_lp_minted. "
            f"Balance delta: {lp_received}, receipt: {lp_minted_raw}"
        )

        # Verify extraction methods (position-key / enrichment path)
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            if not parser.parse_receipt(receipt_dict).mint_events:
                continue
            position_id = parser.extract_position_id(receipt_dict)
            assert position_id is not None, "extract_position_id must return a value for LP_OPEN"
            assert position_id.lower() == PENDLE_WSTETH_MARKET.lower(), (
                f"position_id must equal the market address, got {position_id}"
            )
            lp_open_data = parser.extract_lp_open_data(receipt_dict)
            assert lp_open_data is not None, "extract_lp_open_data must return data"
            assert lp_open_data.liquidity == lp_minted_raw, (
                f"lp_open_data.liquidity must match net_lp_minted. "
                f"Expected: {lp_minted_raw}, Got: {lp_open_data.liquidity}"
            )

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_insufficient_balance_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """LP_OPEN with more wstETH than the wallet holds must fail gracefully."""
        wsteth_balance = get_token_balance(web3, WSTETH_ADDRESS, funded_wallet)
        lp_before = get_token_balance(web3, PENDLE_WSTETH_MARKET, funded_wallet)
        wsteth_decimals = get_token_decimals(web3, WSTETH_ADDRESS)
        balance_decimal = Decimal(wsteth_balance) / Decimal(10**wsteth_decimals)
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'='*80}")
        print("Test: LP_OPEN Insufficient Balance (Pendle)")
        print(f"{'='*80}")
        print(f"wstETH balance: {balance_decimal}")
        print(f"Trying:         {excessive_amount}")

        intent = LPOpenIntent(
            pool=f"{WSTETH_SYMBOL}/{PENDLE_WSTETH_MARKET}",
            amount0=excessive_amount,
            amount1=Decimal("0"),
            range_lower=_DUMMY_RANGE_LOWER,
            range_upper=_DUMMY_RANGE_UPPER,
            protocol="pendle",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        # Bilateral conservation: both wstETH and LP token unchanged after failure
        wsteth_after = get_token_balance(web3, WSTETH_ADDRESS, funded_wallet)
        lp_after = get_token_balance(web3, PENDLE_WSTETH_MARKET, funded_wallet)
        assert wsteth_after == wsteth_balance, "wstETH balance must be unchanged after failed LP_OPEN"
        assert lp_after == lp_before, "LP token balance must be unchanged after failed LP_OPEN"

        print("\nALL CHECKS PASSED")


# =============================================================================
# LP_CLOSE Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestPendleLPCloseIntent:
    """4-layer tests for Pendle LP_CLOSE on Arbitrum.

    Opens a position within each test, then closes it, verifying:
    1. Compilation succeeds
    2. Execution lands on-chain
    3. PendleReceiptParser finds a Burn event with net_sy_out > 0
    4. LP token balance returns to zero, wstETH balance increases
    """

    async def _open_lp_position(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> int:
        """Open a wstETH LP position and return the LP token amount received."""
        intent = LPOpenIntent(
            pool=f"{WSTETH_SYMBOL}/{PENDLE_WSTETH_MARKET}",
            amount0=LP_DEPOSIT_AMOUNT,
            amount1=Decimal("0"),
            range_lower=_DUMMY_RANGE_LOWER,
            range_upper=_DUMMY_RANGE_UPPER,
            protocol="pendle",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        result = compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"LP_OPEN compilation failed: {result.error}"
        exec_result = await orchestrator.execute(result.action_bundle)
        assert exec_result.success, f"LP_OPEN execution failed: {exec_result.error}"

        lp_balance = get_token_balance(web3, PENDLE_WSTETH_MARKET, funded_wallet)
        assert lp_balance > 0, "Expected LP tokens after LP_OPEN"
        return lp_balance

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_returns_wsteth(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Close an open wstETH Pendle LP position and verify wstETH is returned."""
        wsteth_decimals = get_token_decimals(web3, WSTETH_ADDRESS)

        # Setup: open an LP position to close
        lp_amount = await self._open_lp_position(
            web3, funded_wallet, orchestrator, price_oracle, anvil_rpc_url
        )

        print(f"\n{'='*80}")
        print("Test: LP_CLOSE PT-wstETH-25JUN2026 -> wstETH (Pendle)")
        print(f"{'='*80}")
        print(f"LP to burn: {format_token_amount(lp_amount, 18)}")

        # Layer 4 setup: record balances BEFORE close
        wsteth_before = get_token_balance(web3, WSTETH_ADDRESS, funded_wallet)
        lp_before = get_token_balance(web3, PENDLE_WSTETH_MARKET, funded_wallet)
        print(f"wstETH before: {format_token_amount(wsteth_before, wsteth_decimals)}")
        print(f"LP before:     {format_token_amount(lp_before, 18)}")

        # Layer 1: Compile
        # Output token is passed via protocol_params since LPCloseIntent has no token field.
        intent = LPCloseIntent(
            position_id=str(lp_amount),
            pool=PENDLE_WSTETH_MARKET,
            protocol="pendle",
            chain=CHAIN_NAME,
            protocol_params={"token": WSTETH_SYMBOL},
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        tx_count = len(compilation_result.action_bundle.transactions)
        print(f"ActionBundle: {tx_count} transactions")

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful: {len(execution_result.transaction_results)} txs confirmed")

        # Layer 3: Receipt parsing — expect exactly one Burn event
        parser = PendleReceiptParser(chain=CHAIN_NAME)
        lp_burned_raw: int | None = None
        sy_out_raw: int | None = None
        for i, tx_result in enumerate(execution_result.transaction_results):
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            if parse_result.burn_events:
                burn = parse_result.burn_events[0]
                lp_burned_raw = burn.net_lp_burned
                sy_out_raw = burn.net_sy_out
                print(
                    f"\nTx {i+1} Burn event:"
                    f"\n  market:        {burn.market_address}"
                    f"\n  net_lp_burned: {burn.net_lp_burned}"
                    f"\n  net_sy_out:    {burn.net_sy_out}"
                    f"\n  net_pt_out:    {burn.net_pt_out}"
                )

        assert lp_burned_raw is not None, "No Burn event found in any transaction receipt"
        assert lp_burned_raw > 0, f"net_lp_burned must be positive, got {lp_burned_raw}"
        assert sy_out_raw is not None and sy_out_raw > 0, (
            f"net_sy_out must be positive, got {sy_out_raw}"
        )

        # Layer 4: Balance deltas
        wsteth_after = get_token_balance(web3, WSTETH_ADDRESS, funded_wallet)
        lp_after = get_token_balance(web3, PENDLE_WSTETH_MARKET, funded_wallet)

        wsteth_received = wsteth_after - wsteth_before
        lp_spent = lp_before - lp_after

        print("\n--- Results ---")
        print(f"wstETH received: {format_token_amount(wsteth_received, wsteth_decimals)}")
        print(f"LP burned:       {format_token_amount(lp_spent, 18)}")

        assert lp_spent == lp_amount, (
            f"LP tokens burned must equal position_id amount. "
            f"Expected: {lp_amount}, Got: {lp_spent}"
        )
        assert lp_after == 0, f"LP token balance must be zero after full close, got {lp_after}"
        assert wsteth_received > 0, "Must receive positive wstETH after LP_CLOSE"

        # Verify extraction methods (position-key / enrichment path)
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            if not parser.parse_receipt(receipt_dict).burn_events:
                continue
            position_id = parser.extract_position_id(receipt_dict)
            assert position_id is not None, "extract_position_id must return a value for LP_CLOSE"
            assert position_id.lower() == PENDLE_WSTETH_MARKET.lower(), (
                f"position_id must equal the market address, got {position_id}"
            )
            lp_close_data = parser.extract_lp_close_data(receipt_dict)
            assert lp_close_data is not None, "extract_lp_close_data must return data"
            assert lp_close_data.liquidity_removed == lp_burned_raw, (
                f"lp_close_data.liquidity_removed must match net_lp_burned. "
                f"Expected: {lp_burned_raw}, Got: {lp_close_data.liquidity_removed}"
            )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    import pytest as _pytest

    _pytest.main([__file__, "-v", "-s", "-n0", "--import-mode=importlib"])
