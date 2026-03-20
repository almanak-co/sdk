"""Curve WETH/cbETH LP Intent tests for Base chain.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Adding liquidity to Curve WETH/cbETH Twocrypto pool on Base
- LPCloseIntent: Removing liquidity proportionally

The WETH/cbETH pool is an old-style Twocrypto pool (NOT NG) on Base:
- Pool: 0x11C1fBd4b3De66bC0565779b35171a6CF3E71f59
- LP token: 0x98244d93D42b42aB3E3A4D12A5dc0B3e7f8F32f9 (SEPARATE from pool)
- Coins: WETH + cbETH (both ETH derivatives, ~3% APY from LST yield + fees)

NOTE: This pool uses an OLD Twocrypto factory, so the LP token is a separate
ERC20 contract (not the pool address). Verified on-chain 2026-03-19 via pool.token().

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/base/test_curve_lp_base.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from tests.intents.conftest import (
    fund_erc20_token,
    get_token_balance,
    _wrap_native_token,
)

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "base"

# Curve WETH/cbETH Twocrypto pool on Base (old-style, NOT NG)
POOL = "weth_cbeth"
POOL_ADDRESS = "0x11C1fBd4b3De66bC0565779b35171a6CF3E71f59"
# LP token is a SEPARATE contract from the pool — verified on-chain 2026-03-19 via pool.token()
# This pool uses old-style Twocrypto (not NG), so pool address != LP token address
LP_TOKEN = "0x98244d93D42b42aB3E3A4D12A5dc0B3e7f8F32f9"

# Token addresses
WETH_ADDRESS = "0x4200000000000000000000000000000000000006"
CBETH_ADDRESS = "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22"

# cbETH balance slot in storage (verified on Base mainnet via cast storage brute-force)
CBETH_BALANCE_SLOT = 51

# Deposit amounts (small amounts to avoid price impact)
LP_AMOUNT_WETH = Decimal("0.01")   # 0.01 WETH
LP_AMOUNT_CBETH = Decimal("0.01")  # 0.01 cbETH


# =============================================================================
# Helpers
# =============================================================================


def _fund_cbeth(wallet: str, rpc_url: str, amount_cbeth: Decimal = Decimal("1.0")) -> None:
    """Fund test wallet with cbETH via storage slot manipulation."""
    cbeth_decimals = 18
    amount_wei = int(amount_cbeth * Decimal(10**cbeth_decimals))
    fund_erc20_token(wallet, CBETH_ADDRESS, amount_wei, CBETH_BALANCE_SLOT, rpc_url)


def _fund_weth(wallet: str, rpc_url: str, amount_weth: Decimal = Decimal("1.0")) -> None:
    """Fund test wallet with WETH by wrapping ETH."""
    amount_wei = int(amount_weth * Decimal(10**18))
    _wrap_native_token(wallet, WETH_ADDRESS, amount_wei, rpc_url)


def _get_lp_token_balance(web3: Web3, wallet: str) -> int:
    """Get current LP token balance for the wallet."""
    return get_token_balance(web3, LP_TOKEN, wallet)


# =============================================================================
# LP Open Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.lp
class TestCurveWethCbethLPOpen:
    """Test Curve WETH/cbETH LP_OPEN using LPOpenIntent on Base.

    Verifies the full Intent flow:
    - LPOpenIntent with pool=weth_cbeth, WETH + cbETH amounts
    - IntentCompiler generates approve + add_liquidity TXs for Base chain
    - Transactions execute on Anvil fork of Base
    - AddLiquidity event parsed from receipt
    - LP tokens minted and balance delta verified
    """

    @pytest.mark.asyncio
    async def test_lp_open_weth_cbeth(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test adding WETH + cbETH to Curve WETH/cbETH Twocrypto pool on Base.

        Flow:
        1. Fund wallet with WETH (wrap ETH) and cbETH (slot manipulation)
        2. Record balances BEFORE (WETH, cbETH, LP token)
        3. Create LPOpenIntent for Curve weth_cbeth pool
        4. Compile to ActionBundle (approve WETH + approve cbETH + add_liquidity)
        5. Execute on-chain
        6. Parse receipt for AddLiquidity event
        7. Verify LP tokens received and token balance deltas
        """
        # Fund WETH and cbETH
        _fund_weth(funded_wallet, anvil_rpc_url, Decimal("1.0"))
        _fund_cbeth(funded_wallet, anvil_rpc_url, Decimal("1.0"))
        assert get_token_balance(web3, CBETH_ADDRESS, funded_wallet) > 0, (
            "cbETH funding failed — check CBETH_BALANCE_SLOT"
        )

        # --- Layer 4 BEFORE ---
        weth_before = get_token_balance(web3, WETH_ADDRESS, funded_wallet)
        cbeth_before = get_token_balance(web3, CBETH_ADDRESS, funded_wallet)
        lp_before = _get_lp_token_balance(web3, funded_wallet)

        # --- Layer 1: Compile ---
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_CBETH,
            range_lower=Decimal("1"),         # Dummy — Curve uses pool-based positions
            range_upper=Decimal("1000000"),    # Dummy — required by LPOpenIntent validation
            protocol="curve",
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
            f"Curve LP_OPEN compilation failed on Base: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Curve LP_OPEN execution failed on Base: {execution_result.error}"

        # --- Layer 3: Receipt Parsing ---
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        lp_open_receipt_parsed = False
        lp_tokens_from_receipt: int | None = None

        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"

            for event in parse_result.events:
                if event.event_type == CurveEventType.ADD_LIQUIDITY:
                    lp_open_receipt_parsed = True
                    logger.info(
                        f"AddLiquidity event: token_amounts={event.data.get('token_amounts')}, "
                        f"lp_token_supply={event.data.get('token_supply')}"
                    )

            lp_minted = parser.extract_lp_tokens_received(receipt_dict)
            if lp_minted is not None and lp_minted > 0:
                lp_tokens_from_receipt = lp_minted

        assert lp_open_receipt_parsed, (
            "AddLiquidity event must be found in LP_OPEN receipt. "
            "Parser must detect Curve AddLiquidity events for the WETH/cbETH pool."
        )
        assert lp_tokens_from_receipt is not None and lp_tokens_from_receipt > 0, (
            "LP tokens minted must be > 0 and extractable from receipt Transfer event."
        )

        # --- Layer 4 AFTER: Balance Deltas ---
        weth_after = get_token_balance(web3, WETH_ADDRESS, funded_wallet)
        cbeth_after = get_token_balance(web3, CBETH_ADDRESS, funded_wallet)
        lp_after = _get_lp_token_balance(web3, funded_wallet)

        weth_spent = weth_before - weth_after
        cbeth_spent = cbeth_before - cbeth_after
        lp_received = lp_after - lp_before

        expected_weth_spent = int(LP_AMOUNT_WETH * Decimal(10**18))
        expected_cbeth_spent = int(LP_AMOUNT_CBETH * Decimal(10**18))

        assert weth_spent == expected_weth_spent, (
            f"WETH spent must EXACTLY equal LP_OPEN amount. "
            f"Expected: {expected_weth_spent}, Got: {weth_spent}"
        )
        assert cbeth_spent == expected_cbeth_spent, (
            f"cbETH spent must EXACTLY equal LP_OPEN amount. "
            f"Expected: {expected_cbeth_spent}, Got: {cbeth_spent}"
        )
        assert lp_received > 0, f"LP tokens received must be > 0, got {lp_received}"
        assert lp_received == lp_tokens_from_receipt, (
            f"LP tokens from balance delta ({lp_received}) must match receipt ({lp_tokens_from_receipt})"
        )

        logger.info(
            f"LP_OPEN: WETH spent={weth_spent / 10**18:.6f}, "
            f"cbETH spent={cbeth_spent / 10**18:.6f}, "
            f"LP received={lp_received}"
        )


# =============================================================================
# LP Close Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.lp
class TestCurveWethCbethLPClose:
    """Test Curve WETH/cbETH LP_CLOSE using LPCloseIntent on Base.

    Verifies the full Intent flow:
    - LPCloseIntent with position_id (LP token amount)
    - IntentCompiler generates approve + remove_liquidity TXs for Base chain
    - Transactions execute on Anvil fork of Base
    - RemoveLiquidity event parsed from receipt
    - WETH + cbETH returned and LP tokens burned
    """

    @pytest.mark.asyncio
    async def test_lp_close_weth_cbeth(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test removing WETH + cbETH from Curve WETH/cbETH pool on Base.

        Flow:
        1. Fund WETH + cbETH, open LP position to get LP tokens
        2. Record LP token balance as position_id
        3. Create LPCloseIntent with position_id
        4. Execute and verify WETH + cbETH returned, LP tokens burned
        """
        # Setup: first open an LP position
        _fund_weth(funded_wallet, anvil_rpc_url, Decimal("1.0"))
        _fund_cbeth(funded_wallet, anvil_rpc_url, Decimal("1.0"))
        assert get_token_balance(web3, CBETH_ADDRESS, funded_wallet) > 0, (
            "cbETH funding failed — check CBETH_BALANCE_SLOT"
        )

        # Open LP to get LP tokens (use slightly different amounts to avoid caching issues)
        open_intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_CBETH,
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="curve",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        open_result = compiler.compile(open_intent)
        assert open_result.status.value == "SUCCESS", f"LP open compilation failed: {open_result.error}"
        open_execution = await orchestrator.execute(open_result.action_bundle)
        assert open_execution.success, f"LP open execution failed: {open_execution.error}"

        # LP tokens received = position_id for LP_CLOSE
        lp_balance = _get_lp_token_balance(web3, funded_wallet)
        assert lp_balance > 0, "Must have LP tokens before LP_CLOSE test"

        # --- Layer 4 BEFORE ---
        weth_before = get_token_balance(web3, WETH_ADDRESS, funded_wallet)
        cbeth_before = get_token_balance(web3, CBETH_ADDRESS, funded_wallet)
        lp_before_close = lp_balance

        # --- Layer 1: Compile LP_CLOSE ---
        # Curve LP_CLOSE expects position_id as a DECIMAL token amount (e.g., "9.84"),
        # not raw wei. The compiler multiplies by 1e18 internally (see _compile_lp_close_curve).
        # Convert raw wei balance -> decimal token units, consistent with Ethereum Curve test.
        lp_amount_decimal_str = str(Decimal(lp_balance) / Decimal(10**18))
        close_intent = LPCloseIntent(
            pool=POOL,
            position_id=lp_amount_decimal_str,  # LP token amount in decimal units (not wei)
            protocol="curve",
            chain=CHAIN_NAME,
        )
        close_result = compiler.compile(close_intent)
        assert close_result.status.value == "SUCCESS", (
            f"Curve LP_CLOSE compilation failed on Base: {close_result.error}"
        )
        assert close_result.action_bundle is not None

        # --- Layer 2: Execute ---
        close_execution = await orchestrator.execute(close_result.action_bundle)
        assert close_execution.success, f"Curve LP_CLOSE execution failed on Base: {close_execution.error}"

        # --- Layer 3: Receipt Parsing ---
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        lp_close_receipt_parsed = False
        lp_close_data = None

        for tx_result in close_execution.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"

            for event in parse_result.events:
                if event.event_type == CurveEventType.REMOVE_LIQUIDITY:
                    lp_close_receipt_parsed = True
                    logger.info(f"RemoveLiquidity event: {event.data}")

            # Extract decoded LP close data (amounts returned per token)
            extracted = parser.extract_lp_close_data(receipt_dict)
            if extracted is not None:
                lp_close_data = extracted

        assert lp_close_receipt_parsed, (
            "RemoveLiquidity event must be found in LP_CLOSE receipt."
        )

        # --- Layer 4 AFTER: Balance Deltas ---
        weth_after = get_token_balance(web3, WETH_ADDRESS, funded_wallet)
        cbeth_after = get_token_balance(web3, CBETH_ADDRESS, funded_wallet)
        lp_after = _get_lp_token_balance(web3, funded_wallet)

        weth_received = weth_after - weth_before
        cbeth_received = cbeth_after - cbeth_before
        lp_burned = lp_before_close - lp_after

        assert lp_burned == lp_before_close, (
            f"All LP tokens must be burned in LP_CLOSE. Burned: {lp_burned}, Had: {lp_before_close}"
        )
        assert weth_received > 0, f"WETH must be returned after LP_CLOSE, got {weth_received}"
        assert cbeth_received > 0, f"cbETH must be returned after LP_CLOSE, got {cbeth_received}"

        # Layer 3+4 cross-check: parser-decoded amounts must match on-chain balance deltas
        assert lp_close_data is not None, (
            "extract_lp_close_data() must return data from RemoveLiquidity receipt"
        )
        assert lp_close_data.amount0_collected == weth_received, (
            f"Parser WETH amount ({lp_close_data.amount0_collected}) must match balance delta ({weth_received})"
        )
        assert lp_close_data.amount1_collected == cbeth_received, (
            f"Parser cbETH amount ({lp_close_data.amount1_collected}) must match balance delta ({cbeth_received})"
        )

        logger.info(
            f"LP_CLOSE: WETH returned={weth_received / 10**18:.6f}, "
            f"cbETH returned={cbeth_received / 10**18:.6f}, "
            f"LP burned={lp_burned}"
        )
