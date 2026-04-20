"""Curve 3pool LP Intent tests for Ethereum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Adding liquidity to Curve 3pool (DAI/USDC/USDT)
- LPCloseIntent: Removing liquidity proportionally

Curve 3pool is a stableswap pool -- positions are fungible LP tokens,
not NFTs. The LP token amount is passed as position_id for LP_CLOSE.

Note: LPOpenIntent only supports amount0 + amount1 (2-coin limit).
For 3pool, USDT deposit is always 0. DAI + USDC only.

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/ethereum/test_curve_3pool_lp.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    fund_erc20_token,
    get_token_balance,
)

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "ethereum"

# Curve 3pool (DAI/USDC/USDT)
POOL = "3pool"
POOL_ADDRESS = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"
LP_TOKEN = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"

# Deposit amounts (LPOpenIntent 2-coin limit: DAI + USDC only, USDT=0)
LP_AMOUNT_DAI = Decimal("100")
LP_AMOUNT_USDC = Decimal("100")

# Token addresses
DAI_ADDRESS = "0x6B175474E89094C44Da98b954EedeAC495271d0F"

# DAI balance storage slot on Ethereum mainnet (MakerDAO Dai.sol: balanceOf at slot 2)
DAI_BALANCE_SLOT = 2


# =============================================================================
# Helpers
# =============================================================================


def _fund_dai(wallet: str, rpc_url: str, amount_dai: Decimal = Decimal("10000")) -> None:
    """Fund test wallet with DAI via storage slot manipulation."""
    dai_decimals = 18
    amount_wei = int(amount_dai * Decimal(10**dai_decimals))
    fund_erc20_token(wallet, DAI_ADDRESS, amount_wei, DAI_BALANCE_SLOT, rpc_url)


def _get_lp_token_balance(web3: Web3, wallet: str) -> int:
    """Get current LP token (3Crv) balance for the wallet."""
    return get_token_balance(web3, LP_TOKEN, wallet)


# =============================================================================
# LP Open Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.lp
class TestCurve3poolLPOpen:
    """Test Curve 3pool LP_OPEN using LPOpenIntent.

    Verifies the full Intent flow:
    - LPOpenIntent with pool, DAI + USDC amounts, dummy range
    - IntentCompiler generates approve + add_liquidity TXs
    - Transactions execute on Anvil fork
    - AddLiquidity3 event parsed from receipt
    - LP tokens minted and balance delta verified
    """

    @pytest.mark.asyncio
    async def test_lp_open_dai_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test adding DAI + USDC to Curve 3pool.

        Flow:
        1. Fund wallet with DAI
        2. Record balances BEFORE (DAI, USDC, LP token)
        3. Create LPOpenIntent for Curve 3pool
        4. Compile to ActionBundle (approve DAI + approve USDC + add_liquidity)
        5. Execute on-chain
        6. Parse receipt for AddLiquidity3 event
        7. Verify LP tokens received and token balance deltas
        """
        # Fund DAI (not in standard funded_wallet set)
        _fund_dai(funded_wallet, anvil_rpc_url)

        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        # --- Layer 4 BEFORE ---
        dai_before = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        lp_before = _get_lp_token_balance(web3, funded_wallet)

        # --- Layer 1: Compile ---
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_DAI,
            amount1=LP_AMOUNT_USDC,
            range_lower=Decimal("1"),        # Dummy -- Curve uses pool-based positions
            range_upper=Decimal("1000000"),   # Dummy -- required by LPOpenIntent validation
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
            f"Curve LP_OPEN compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Curve LP_OPEN execution failed: {execution_result.error}"

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

            # Try to extract LP tokens from Transfer (mint) event
            lp_minted = parser.extract_lp_tokens_received(receipt_dict)
            if lp_minted is not None and lp_minted > 0:
                lp_tokens_from_receipt = lp_minted

        assert lp_open_receipt_parsed, (
            "AddLiquidity3 event must be found in LP_OPEN receipt. "
            "Parser must detect Curve AddLiquidity events."
        )
        assert lp_tokens_from_receipt is not None and lp_tokens_from_receipt > 0, (
            "LP tokens minted must be > 0 and extractable from receipt Transfer event."
        )

        # --- Layer 4 AFTER: Balance Deltas ---
        dai_after = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        lp_after = _get_lp_token_balance(web3, funded_wallet)

        dai_spent = dai_before - dai_after
        usdc_spent = usdc_before - usdc_after
        lp_received = lp_after - lp_before

        expected_dai_spent = int(LP_AMOUNT_DAI * Decimal(10**18))
        expected_usdc_spent = int(LP_AMOUNT_USDC * Decimal(10**6))

        assert dai_spent == expected_dai_spent, (
            f"DAI spent must EXACTLY equal LP_OPEN amount. "
            f"Expected: {expected_dai_spent}, Got: {dai_spent}"
        )
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal LP_OPEN amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        assert lp_received > 0, "Must receive LP tokens after adding liquidity"
        assert lp_received == lp_tokens_from_receipt, (
            f"LP tokens from receipt ({lp_tokens_from_receipt}) must match balance delta ({lp_received})"
        )

        logger.info(
            f"LP_OPEN success: spent {LP_AMOUNT_DAI} DAI + {LP_AMOUNT_USDC} USDC, "
            f"received {lp_received / 1e18:.6f} 3Crv LP tokens"
        )


# =============================================================================
# LP Lifecycle Tests (Open -> Close)
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.lp
class TestCurve3poolLPLifecycle:
    """Test full Curve 3pool LP lifecycle: LP_OPEN then LP_CLOSE.

    Verifies:
    - LP_OPEN adds liquidity and mints LP tokens
    - LP_CLOSE burns LP tokens and returns DAI + USDC + USDT
    - RemoveLiquidity3 event parsed from close receipt
    - Balance conservation: tokens returned >= 0 for each coin
    """

    @pytest.mark.asyncio
    async def test_lp_open_then_close(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test full Curve 3pool LP lifecycle: open then close.

        Flow:
        1. Fund wallet with DAI
        2. LP_OPEN: deposit DAI + USDC into 3pool
        3. Extract LP token balance from receipt
        4. LP_CLOSE: burn all LP tokens proportionally
        5. Verify RemoveLiquidity3 event + balance deltas
        """
        # Fund DAI
        _fund_dai(funded_wallet, anvil_rpc_url)

        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        # ==================== OPEN ====================
        dai_before_open = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        usdc_before_open = get_token_balance(web3, usdc_addr, funded_wallet)
        lp_before_open = _get_lp_token_balance(web3, funded_wallet)

        open_intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_DAI,
            amount1=LP_AMOUNT_USDC,
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
        assert open_result.status.value == "SUCCESS", f"LP_OPEN compile failed: {open_result.error}"
        assert open_result.action_bundle is not None

        # Layer 2: Execute LP_OPEN
        open_exec = await orchestrator.execute(open_result.action_bundle)
        assert open_exec.success, f"LP_OPEN execution failed: {open_exec.error}"

        # Layer 3: Parse LP_OPEN receipt
        parser = CurveReceiptParser(chain=CHAIN_NAME)
        add_liquidity_found = False
        lp_tokens_received: int = 0

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

        assert add_liquidity_found, "AddLiquidity3 event must be found in LP_OPEN receipt"
        assert lp_tokens_received > 0, "Must extract LP tokens from LP_OPEN receipt"

        # Layer 4: LP_OPEN balance deltas
        lp_after_open = _get_lp_token_balance(web3, funded_wallet)
        lp_delta_open = lp_after_open - lp_before_open
        assert lp_delta_open > 0, "LP token balance must increase after LP_OPEN"
        assert lp_delta_open == lp_tokens_received, (
            f"LP token delta ({lp_delta_open}) must match receipt extraction ({lp_tokens_received})"
        )

        # ==================== CLOSE ====================
        # Use LP token balance (in raw wei) as position_id for Curve LP_CLOSE
        lp_amount_str = str(Decimal(lp_tokens_received) / Decimal(10**18))

        dai_before_close = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)

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
        lp_close_amounts: list[int] = []

        for tx_result in close_exec.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result.success

            for event in parse_result.events:
                if event.event_type == CurveEventType.REMOVE_LIQUIDITY:
                    remove_liquidity_found = True
                    lp_close_amounts = event.data.get("token_amounts", [])
                    logger.info(
                        f"RemoveLiquidity event: token_amounts={lp_close_amounts}"
                    )

            lp_close_data = parser.extract_lp_close_data(receipt_dict)
            if lp_close_data is not None:
                logger.info(
                    f"LP close data: amount0={lp_close_data.amount0_collected}, "
                    f"amount1={lp_close_data.amount1_collected}"
                )

        assert remove_liquidity_found, (
            "RemoveLiquidity3 event must be found in LP_CLOSE receipt. "
            "Parser must detect Curve RemoveLiquidity events."
        )

        # Layer 4: LP_CLOSE balance deltas
        lp_after_close = _get_lp_token_balance(web3, funded_wallet)
        dai_after_close = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)

        lp_burned = lp_after_open - lp_after_close
        dai_returned = dai_after_close - dai_before_close
        usdc_returned = usdc_after_close - usdc_before_close

        assert lp_burned > 0, "LP tokens must be burned during LP_CLOSE"
        assert dai_returned > 0, "Must receive DAI back from LP_CLOSE"
        assert usdc_returned > 0, "Must receive USDC back from LP_CLOSE"

        logger.info(
            f"LP_CLOSE success: burned {lp_burned / 1e18:.6f} 3Crv, "
            f"received {dai_returned / 1e18:.4f} DAI + {usdc_returned / 1e6:.4f} USDC"
        )
