"""Curve crvUSD/USDC LP Intent tests for Optimism (VIB-4307).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Adding liquidity to Curve crvUSD/USDC StableSwap NG pool on Optimism
- LPCloseIntent: Removing liquidity proportionally

Pool: Curve crvUSD/USDC StableSwap NG on Optimism
- Address: 0x03771e24b7c9172d163bf447490b142a15be3485
- LP token: same as pool (StableSwap NG: LP = pool)
- Coins[0]: crvUSD (0xC52D...)
- Coins[1]: USDC (native, 0x0b2C...)
- Type: stableswap

This pool uses native USDC (not USDC.e). crvUSD must be funded via storage slot.

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/optimism/test_curve_lp.py -v -s
"""

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.curve.adapter import CURVE_POOLS
from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    fund_erc20_token,
    get_token_balance,
)

pytestmark = pytest.mark.no_zodiac(reason="curve LP not in _LP_PROTOCOLS; manifest empty for curve LP")

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "optimism"

# Curve crvUSD/USDC StableSwap NG pool on Optimism
POOL = "crvusd_usdc"
POOL_ADDRESS = "0x03771e24b7C9172d163Bf447490B142a15be3485"
LP_TOKEN = "0x03771e24b7C9172d163Bf447490B142a15be3485"  # StableSwap NG: LP = pool

# Token addresses (coin order: crvUSD=0, USDC=1)
CRVUSD_ADDRESS = "0xC52D7F23a2e460248Db6eE192Cb23dD12bDDCbf6"
USDC_ADDRESS = "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85"  # native USDC

# crvUSD on Optimism (0xC52D7F23a2e460248Db6eE192Cb23dD12bDDCbf6) stores its
# `_balances` mapping at slot 0 (Solidity ERC20 base layout, NOT Vyper).
# Verified 2026-05-26 via on-chain probe: eth_getStorageAt against the pool
# (0x03771e24…) for the same holder returns balanceOf() iff base_slot=0.
# The previous value (4) silently no-op'd funding, causing the runtime
# pytest.skip below to fire on every run (VIB-4822).
CRVUSD_BALANCE_SLOT = 0

# LP deposit amounts
LP_AMOUNT_CRVUSD = Decimal("10")  # 10 crvUSD (18 decimals)
LP_AMOUNT_USDC = Decimal("10")    # 10 USDC (6 decimals)


# =============================================================================
# Helpers
# =============================================================================


def _fund_crvusd(wallet: str, rpc_url: str, amount: Decimal = Decimal("10000")) -> None:
    """Fund test wallet with crvUSD via storage slot manipulation."""
    decimals = 18
    amount_wei = int(amount * Decimal(10**decimals))
    fund_erc20_token(wallet, CRVUSD_ADDRESS, amount_wei, CRVUSD_BALANCE_SLOT, rpc_url)


def _get_lp_token_balance(web3: Web3, wallet: str) -> int:
    """Get current LP token balance for the wallet."""
    return get_token_balance(web3, LP_TOKEN, wallet)


# =============================================================================
# Pre-test: Pool Existence Check
# =============================================================================


def _verify_pool_exists() -> None:
    """Pool existence check per intent-tests.md rule 8."""
    if "optimism" not in CURVE_POOLS or POOL not in CURVE_POOLS["optimism"]:
        pytest.skip(f"No curve {POOL} on optimism (pool not in CURVE_POOLS registry)")


# =============================================================================
# LP Open Tests
# =============================================================================


@pytest.mark.optimism
@pytest.mark.lp
class TestCurveCrvUSDUSDCLPOpenOptimism:
    """Test Curve crvUSD/USDC LP_OPEN using LPOpenIntent on Optimism.

    Verifies the full Intent flow:
    - LPOpenIntent with pool=crvusd_usdc, crvUSD + USDC amounts
    - IntentCompiler generates approve + add_liquidity TXs for Optimism chain
    - Transactions execute on Anvil fork of Optimism
    - AddLiquidity event parsed from receipt
    - LP tokens minted and balance delta verified
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_crvusd_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test adding crvUSD + USDC to Curve crvUSD/USDC pool on Optimism.

        Flow:
        1. Fund wallet with crvUSD via slot manipulation
        2. Record balances BEFORE (crvUSD, USDC, LP token)
        3. Create LPOpenIntent for Curve crvusd_usdc pool
        4. Compile to ActionBundle (approve crvUSD + approve USDC + add_liquidity)
        5. Execute on-chain
        6. Parse receipt for AddLiquidity event
        7. Verify LP tokens received and token balance deltas
        """
        _verify_pool_exists()

        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        # Fund crvUSD
        _fund_crvusd(funded_wallet, anvil_rpc_url)
        crvusd_funded_balance = get_token_balance(web3, CRVUSD_ADDRESS, funded_wallet)
        if crvusd_funded_balance == 0:
            pytest.skip(
                f"crvUSD funding failed at slot {CRVUSD_BALANCE_SLOT}. "
                "Token may use a different storage layout — would require slot discovery."
            )

        # --- Layer 4 BEFORE ---
        crvusd_before = get_token_balance(web3, CRVUSD_ADDRESS, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        lp_before = _get_lp_token_balance(web3, funded_wallet)
        assert crvusd_before > 0, "crvUSD funding failed"
        assert usdc_before > 0, "USDC funding failed"

        # --- Layer 1: Compile ---
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_CRVUSD,
            amount1=LP_AMOUNT_USDC,
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
            f"Curve LP_OPEN compilation failed on Optimism: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, (
            f"Curve LP_OPEN execution failed on Optimism: {execution_result.error}"
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
            "Parser must detect Curve AddLiquidity events on Optimism crvusd_usdc pool."
        )
        assert lp_tokens_from_receipt is not None and lp_tokens_from_receipt > 0, (
            "LP tokens minted must be > 0 and extractable from receipt Transfer event."
        )

        # --- Layer 4 AFTER: Balance Deltas ---
        crvusd_after = get_token_balance(web3, CRVUSD_ADDRESS, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        lp_after = _get_lp_token_balance(web3, funded_wallet)

        crvusd_spent = crvusd_before - crvusd_after
        usdc_spent = usdc_before - usdc_after
        lp_received = lp_after - lp_before

        expected_crvusd_spent = int(LP_AMOUNT_CRVUSD * Decimal(10**18))
        expected_usdc_spent = int(LP_AMOUNT_USDC * Decimal(10**6))

        assert crvusd_spent == expected_crvusd_spent, (
            f"crvUSD spent must EXACTLY equal LP_OPEN amount. "
            f"Expected: {expected_crvusd_spent}, Got: {crvusd_spent}"
        )
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal LP_OPEN amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        assert lp_received > 0, f"LP tokens received must be > 0, got {lp_received}"

        # LP token receipt extraction cross-check
        lp_received_decimal = Decimal(lp_received) / Decimal(10**18)
        assert lp_received_decimal == lp_tokens_from_receipt, (
            f"LP tokens from balance delta ({lp_received_decimal}) "
            f"must match receipt ({lp_tokens_from_receipt})"
        )

        logger.info(
            f"LP_OPEN: crvUSD spent={crvusd_spent / 10**18:.6f}, "
            f"USDC spent={usdc_spent / 10**6:.6f}, "
            f"LP received={lp_received}"
        )


# =============================================================================
# LP Lifecycle Tests (Open -> Close)
# =============================================================================


@pytest.mark.optimism
@pytest.mark.lp
class TestCurveCrvUSDUSDCLPLifecycleOptimism:
    """Test full Curve crvUSD/USDC LP lifecycle on Optimism.

    Verifies:
    - LP_OPEN adds liquidity and mints LP tokens
    - LP_CLOSE burns LP tokens and returns crvUSD + USDC
    - RemoveLiquidity event parsed from close receipt
    - Balance conservation: tokens returned > 0 for each coin
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
        """Test full Curve crvUSD/USDC LP lifecycle on Optimism: open then close.

        Flow:
        1. Fund wallet with crvUSD
        2. LP_OPEN: deposit crvUSD + USDC into pool
        3. Extract LP token balance
        4. LP_CLOSE: burn all LP tokens proportionally
        5. Verify RemoveLiquidity event + balance deltas
        """
        _verify_pool_exists()

        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        # Fund crvUSD
        _fund_crvusd(funded_wallet, anvil_rpc_url)
        crvusd_funded_balance = get_token_balance(web3, CRVUSD_ADDRESS, funded_wallet)
        if crvusd_funded_balance == 0:
            pytest.skip(
                f"crvUSD funding failed at slot {CRVUSD_BALANCE_SLOT}. "
                "Token may use a different storage layout."
            )

        # ==================== OPEN ====================
        open_intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_CRVUSD,
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

        assert add_liquidity_found, "AddLiquidity event must be found in LP_OPEN receipt"
        assert lp_tokens_received > 0, "Must extract LP tokens from LP_OPEN receipt"

        # ==================== CLOSE ====================
        lp_balance = _get_lp_token_balance(web3, funded_wallet)
        assert lp_balance > 0, "Must have LP tokens before LP_CLOSE test"

        # --- Layer 4 BEFORE close ---
        crvusd_before_close = get_token_balance(web3, CRVUSD_ADDRESS, funded_wallet)
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)

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
            "RemoveLiquidity event must be found in LP_CLOSE receipt."
        )

        # Layer 4 AFTER close: balance deltas
        lp_after_close = _get_lp_token_balance(web3, funded_wallet)
        crvusd_after_close = get_token_balance(web3, CRVUSD_ADDRESS, funded_wallet)
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)

        lp_burned = lp_balance - lp_after_close
        crvusd_returned = crvusd_after_close - crvusd_before_close
        usdc_returned = usdc_after_close - usdc_before_close

        assert lp_burned > 0, "LP tokens must be burned during LP_CLOSE"
        assert crvusd_returned > 0, "Must receive crvUSD back from LP_CLOSE"
        assert usdc_returned > 0, "Must receive USDC back from LP_CLOSE"

        logger.info(
            f"LP_CLOSE success: burned {lp_burned / 1e18:.6f} LP, "
            f"received {crvusd_returned / 10**18:.4f} crvUSD + "
            f"{usdc_returned / 10**6:.4f} USDC"
        )
