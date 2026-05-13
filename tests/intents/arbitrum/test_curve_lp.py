"""Curve 2pool LP Intent tests for Arbitrum (VIB-4307).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Adding liquidity to Curve 2pool (USDC.e/USDT) on Arbitrum
- LPCloseIntent: Removing liquidity proportionally

Curve 2pool on Arbitrum is a stableswap pool:
- Pool: 0x7f90122BF0700F9E7e1F688fe926940E8839F353
- LP token: 0x7f90122BF0700F9E7e1F688fe926940E8839F353 (LP = pool for this pool)
- Coins[0]: USDC.e (bridged USDC, 0xFF970A61...)
- Coins[1]: USDT (0xFd086bC7...)

LPOpenIntent uses amount0 (USDC.e) + amount1 (USDT) directly since n_coins=2.

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/arbitrum/test_curve_lp.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.curve.adapter import CURVE_POOLS
from almanak.framework.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    fund_erc20_token,
    get_token_balance,
)

pytestmark = pytest.mark.no_zodiac(reason="curve LP not in _LP_PROTOCOLS; manifest empty for curve LP")

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"

# Curve 2pool on Arbitrum (USDC.e/USDT)
POOL = "2pool"
POOL_ADDRESS = "0x7f90122BF0700F9E7e1F688fe926940E8839F353"
LP_TOKEN = "0x7f90122BF0700F9E7e1F688fe926940E8839F353"  # LP token = pool address

# Token addresses (coin order: USDC.e=0, USDT=1)
USDC_E_ADDRESS = "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8"  # USDC.e (bridged)
USDT_ADDRESS = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"

# Storage slots for funding (USDC.e is FiatTokenProxy → slot 51 like Optimism USDC.e)
USDC_E_BALANCE_SLOT = 51

# LP deposit amounts (small to keep slippage low)
LP_AMOUNT_USDC_E = Decimal("10")  # 10 USDC.e
LP_AMOUNT_USDT = Decimal("10")    # 10 USDT


# =============================================================================
# Helpers
# =============================================================================


def _fund_usdc_e(wallet: str, rpc_url: str, amount: Decimal = Decimal("10000")) -> None:
    """Fund test wallet with USDC.e (bridged) via storage slot manipulation."""
    decimals = 6
    amount_wei = int(amount * Decimal(10**decimals))
    fund_erc20_token(wallet, USDC_E_ADDRESS, amount_wei, USDC_E_BALANCE_SLOT, rpc_url)


def _get_lp_token_balance(web3: Web3, wallet: str) -> int:
    """Get current LP token balance for the wallet."""
    return get_token_balance(web3, LP_TOKEN, wallet)


# =============================================================================
# Pre-test: Pool Existence Check
# =============================================================================


def _verify_pool_exists() -> None:
    """Pool existence check per intent-tests.md rule 8."""
    if "arbitrum" not in CURVE_POOLS or POOL not in CURVE_POOLS["arbitrum"]:
        pytest.skip(f"No curve {POOL} on arbitrum (pool not in CURVE_POOLS registry)")


# =============================================================================
# LP Open Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestCurve2poolLPOpenArbitrum:
    """Test Curve 2pool LP_OPEN using LPOpenIntent on Arbitrum.

    Verifies the full Intent flow:
    - LPOpenIntent with pool=2pool, USDC.e + USDT amounts
    - IntentCompiler generates approve + add_liquidity TXs for Arbitrum chain
    - Transactions execute on Anvil fork of Arbitrum
    - AddLiquidity event parsed from receipt
    - LP tokens minted and balance delta verified
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_usdc_e_usdt(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test adding USDC.e + USDT to Curve 2pool on Arbitrum.

        Flow:
        1. Fund wallet with USDC.e via slot manipulation
        2. Record balances BEFORE (USDC.e, USDT, LP token)
        3. Create LPOpenIntent for Curve 2pool
        4. Compile to ActionBundle (approve USDC.e + approve USDT + add_liquidity)
        5. Execute on-chain
        6. Parse receipt for AddLiquidity event
        7. Verify LP tokens received and token balance deltas
        """
        _verify_pool_exists()

        # Fund USDC.e (not in standard funded_wallet set on arbitrum)
        _fund_usdc_e(funded_wallet, anvil_rpc_url)
        assert get_token_balance(web3, USDC_E_ADDRESS, funded_wallet) > 0, (
            "USDC.e funding failed — check USDC_E_BALANCE_SLOT"
        )

        # --- Layer 4 BEFORE ---
        usdc_e_before = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        usdt_before = get_token_balance(web3, USDT_ADDRESS, funded_wallet)
        lp_before = _get_lp_token_balance(web3, funded_wallet)

        # --- Layer 1: Compile ---
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_USDC_E,
            amount1=LP_AMOUNT_USDT,
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
            f"Curve LP_OPEN compilation failed on Arbitrum: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, (
            f"Curve LP_OPEN execution failed on Arbitrum: {execution_result.error}"
        )

        # --- Layer 3: Receipt Parsing ---
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        lp_open_receipt_parsed = False
        lp_tokens_from_receipt: Decimal | None = None

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
            "Parser must detect Curve AddLiquidity events on Arbitrum 2pool."
        )
        assert lp_tokens_from_receipt is not None and lp_tokens_from_receipt > 0, (
            "LP tokens minted must be > 0 and extractable from receipt Transfer event."
        )

        # --- Layer 4 AFTER: Balance Deltas ---
        usdc_e_after = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        usdt_after = get_token_balance(web3, USDT_ADDRESS, funded_wallet)
        lp_after = _get_lp_token_balance(web3, funded_wallet)

        usdc_e_spent = usdc_e_before - usdc_e_after
        usdt_spent = usdt_before - usdt_after
        lp_received = lp_after - lp_before

        expected_usdc_e_spent = int(LP_AMOUNT_USDC_E * Decimal(10**6))
        expected_usdt_spent = int(LP_AMOUNT_USDT * Decimal(10**6))

        assert usdc_e_spent == expected_usdc_e_spent, (
            f"USDC.e spent must EXACTLY equal LP_OPEN amount. "
            f"Expected: {expected_usdc_e_spent}, Got: {usdc_e_spent}"
        )
        assert usdt_spent == expected_usdt_spent, (
            f"USDT spent must EXACTLY equal LP_OPEN amount. "
            f"Expected: {expected_usdt_spent}, Got: {usdt_spent}"
        )
        assert lp_received > 0, f"LP tokens received must be > 0, got {lp_received}"
        # extract_lp_tokens_received() returns human-readable Decimal (PR #999),
        # so convert raw wei balance delta to match
        lp_received_decimal = Decimal(lp_received) / Decimal(10**18)
        assert lp_received_decimal == lp_tokens_from_receipt, (
            f"LP tokens from balance delta ({lp_received_decimal}) "
            f"must match receipt ({lp_tokens_from_receipt})"
        )

        logger.info(
            f"LP_OPEN: USDC.e spent={usdc_e_spent / 10**6:.6f}, "
            f"USDT spent={usdt_spent / 10**6:.6f}, "
            f"LP received={lp_received}"
        )


# =============================================================================
# LP Lifecycle Tests (Open -> Close)
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestCurve2poolLPLifecycleArbitrum:
    """Test full Curve 2pool LP lifecycle: LP_OPEN then LP_CLOSE on Arbitrum.

    Verifies:
    - LP_OPEN adds liquidity and mints LP tokens
    - LP_CLOSE burns LP tokens and returns USDC.e + USDT
    - RemoveLiquidity event parsed from close receipt
    - Balance conservation: tokens returned >= 0 for each coin
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_open_then_close(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test full Curve 2pool LP lifecycle on Arbitrum: open then close.

        Flow:
        1. Fund wallet with USDC.e
        2. LP_OPEN: deposit USDC.e + USDT into 2pool
        3. Extract LP token balance
        4. LP_CLOSE: burn all LP tokens proportionally
        5. Verify RemoveLiquidity event + balance deltas
        """
        _verify_pool_exists()

        # Fund USDC.e
        _fund_usdc_e(funded_wallet, anvil_rpc_url)
        assert get_token_balance(web3, USDC_E_ADDRESS, funded_wallet) > 0, (
            "USDC.e funding failed — check USDC_E_BALANCE_SLOT"
        )

        # ==================== OPEN ====================
        open_intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_USDC_E,
            amount1=LP_AMOUNT_USDT,
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

        # Layer 1: Compile LP_OPEN
        open_result = compiler.compile(open_intent)
        assert open_result.status.value == "SUCCESS", (
            f"LP_OPEN compile failed: {open_result.error}"
        )
        assert open_result.action_bundle is not None

        # Layer 2: Execute LP_OPEN
        open_exec = await orchestrator.execute(open_result.action_bundle)
        assert open_exec.success, f"LP_OPEN execution failed: {open_exec.error}"

        # Layer 3: Parse LP_OPEN receipt
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        add_liquidity_found = False
        lp_tokens_received: Decimal = Decimal(0)

        for tx_result in open_exec.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result.success

            for event in parse_result.events:
                if event.event_type == CurveEventType.ADD_LIQUIDITY:
                    add_liquidity_found = True

            minted = parser.extract_lp_tokens_received(receipt_dict)
            if minted is not None and minted > 0:
                lp_tokens_received = minted

        assert add_liquidity_found, (
            "AddLiquidity event must be found in LP_OPEN receipt"
        )
        assert lp_tokens_received > 0, "Must extract LP tokens from LP_OPEN receipt"

        # ==================== CLOSE ====================
        lp_balance = _get_lp_token_balance(web3, funded_wallet)
        assert lp_balance > 0, "Must have LP tokens before LP_CLOSE test"

        # --- Layer 4 BEFORE close ---
        usdc_e_before_close = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        usdt_before_close = get_token_balance(web3, USDT_ADDRESS, funded_wallet)

        # Curve LP_CLOSE expects position_id as DECIMAL token amount (not wei).
        # Use the human-readable amount from receipt parser.
        lp_amount_str = str(lp_tokens_received)

        close_intent = LPCloseIntent(
            position_id=lp_amount_str,
            pool=POOL,
            collect_fees=True,
            protocol="curve",
            chain=CHAIN_NAME,
        )

        # Layer 1: Compile LP_CLOSE
        close_result = compiler.compile(close_intent)
        assert close_result.status.value == "SUCCESS", (
            f"LP_CLOSE compile failed: {close_result.error}"
        )
        assert close_result.action_bundle is not None

        # Layer 2: Execute LP_CLOSE
        close_exec = await orchestrator.execute(close_result.action_bundle)
        assert close_exec.success, f"LP_CLOSE execution failed: {close_exec.error}"

        # Layer 3: Parse LP_CLOSE receipt
        remove_liquidity_found = False

        for tx_result in close_exec.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result.success

            for event in parse_result.events:
                if event.event_type == CurveEventType.REMOVE_LIQUIDITY:
                    remove_liquidity_found = True
                    logger.info(
                        f"RemoveLiquidity event: token_amounts={event.data.get('token_amounts')}"
                    )

        assert remove_liquidity_found, (
            "RemoveLiquidity event must be found in LP_CLOSE receipt. "
            "Parser must detect Curve RemoveLiquidity events on Arbitrum 2pool."
        )

        # Layer 4 AFTER close: balance deltas
        lp_after_close = _get_lp_token_balance(web3, funded_wallet)
        usdc_e_after_close = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        usdt_after_close = get_token_balance(web3, USDT_ADDRESS, funded_wallet)

        lp_burned = lp_balance - lp_after_close
        usdc_e_returned = usdc_e_after_close - usdc_e_before_close
        usdt_returned = usdt_after_close - usdt_before_close

        assert lp_burned > 0, "LP tokens must be burned during LP_CLOSE"
        assert usdc_e_returned > 0, "Must receive USDC.e back from LP_CLOSE"
        assert usdt_returned > 0, "Must receive USDT back from LP_CLOSE"

        logger.info(
            f"LP_CLOSE success: burned {lp_burned / 1e18:.6f} LP tokens, "
            f"received {usdc_e_returned / 10**6:.4f} USDC.e + "
            f"{usdt_returned / 10**6:.4f} USDT"
        )
