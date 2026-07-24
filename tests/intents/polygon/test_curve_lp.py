"""Curve frxUSD/USDT LP Intent tests for Polygon (VIB-4307, reworked for VIB-5551).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Adding liquidity to the Curve frxUSD/USDT NG pool on Polygon
- LPCloseIntent: Removing liquidity proportionally

Pool: Curve "FrxUSD USDT0 v1" (StableSwap NG) on Polygon
- Address: 0x5BC930b8f81F4cEEE3E3527159C3bDF453BcaAe9
- LP token: the pool address itself (StableSwap NG)
- Coins[0]: USDT (0xc2132D05..., 6 dec)
- Coins[1]: frxUSD (0x80Eede..., 18 dec)
- Type: stableswap, is_ng=True

VIB-5551: this pool replaces the aave-type am3pool
(0x445FE580eF8d70FF569aB36e80c647af338db351), whose deposit flow routed the
underlying into the FROZEN Aave V2 Polygon LendingPool (VL_RESERVE_FROZEN)
and reverted on every current fork. Coin order / liquidity verified on-chain
2026-07-24; real-fork proof: tests/reports/vib-5551-polygon-frxusd-usdt-realfork.md.

LPOpenIntent supports 2-coin deposits (amount0 = coins[0] = USDT,
amount1 = coins[1] = frxUSD). We deposit USDT + frxUSD.

NO MOCKING. All tests execute real on-chain transactions on Anvil fork.

To run:
    uv run pytest tests/intents/polygon/test_curve_lp.py -v -s
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
from tests.intents._curve_lp_layer5_helpers import (
    assert_curve_lp_layer5,
    enrich_for_accounting,
)
from tests.intents.conftest import (
    fund_erc20_token,
    get_token_balance,
)

pytestmark = pytest.mark.no_zodiac(reason="curve LP not in _LP_PROTOCOLS; manifest empty for curve LP")

logger = logging.getLogger(__name__)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "polygon"

# Curve frxUSD/USDT StableSwap NG pool on Polygon (VIB-5551).
POOL = "frxusd_usdt"
POOL_ADDRESS = "0x5BC930b8f81F4cEEE3E3527159C3bDF453BcaAe9"
LP_TOKEN = POOL_ADDRESS  # StableSwap NG: the pool IS its own LP token

# Token addresses (coin order: USDT=0, frxUSD=1)
USDT_ADDRESS = "0xc2132D05D31c914a87C6611C10748AEb04B58e8F"
FRXUSD_ADDRESS = "0x80Eede496655FB9047dd39d9f418d5483ED600df"

# Storage slots for token funding.
# USDT: Polygon PoS-bridged UChildERC20Proxy — OpenZeppelin _balances at slot 0.
# frxUSD: LayerZero OFT behind an ERC1967 proxy (OZ-upgradeable v4 layout:
# Initializable + 50-slot __gaps push ERC20 _balances to slot 101). Probed
# empirically on an Anvil fork 2026-07-24 (write keccak(wallet,slot) for slots
# 0..259 and read balanceOf back).
USDT_BALANCE_SLOT = 0
FRXUSD_BALANCE_SLOT = 101

# LP deposit amounts (small to keep slippage low)
LP_AMOUNT_USDT = Decimal("10")  # 10 USDT (6 decimals)
LP_AMOUNT_FRXUSD = Decimal("10")  # 10 frxUSD (18 decimals)


# =============================================================================
# Helpers
# =============================================================================


def _fund_usdt(wallet: str, rpc_url: str, amount: Decimal = Decimal("10000")) -> None:
    """Fund test wallet with USDT on Polygon via storage slot manipulation."""
    decimals = 6
    amount_wei = int(amount * Decimal(10**decimals))
    fund_erc20_token(wallet, USDT_ADDRESS, amount_wei, USDT_BALANCE_SLOT, rpc_url)


def _fund_frxusd(wallet: str, rpc_url: str, amount: Decimal = Decimal("10000")) -> None:
    """Fund test wallet with frxUSD on Polygon via storage slot manipulation."""
    decimals = 18
    amount_wei = int(amount * Decimal(10**decimals))
    fund_erc20_token(wallet, FRXUSD_ADDRESS, amount_wei, FRXUSD_BALANCE_SLOT, rpc_url)


def _get_lp_token_balance(web3: Web3, wallet: str) -> int:
    """Get current LP token balance for the wallet (NG: pool address)."""
    return get_token_balance(web3, LP_TOKEN, wallet)


# =============================================================================
# Pre-test: Pool Existence Check
# =============================================================================


def _verify_pool_exists() -> None:
    """Pool existence check per intent-tests.md rule 8."""
    if "polygon" not in CURVE_POOLS or POOL not in CURVE_POOLS["polygon"]:
        pytest.skip(f"No curve {POOL} on polygon (pool not in CURVE_POOLS registry)")


# =============================================================================
# LP Open Tests
# =============================================================================


@pytest.mark.polygon
@pytest.mark.lp
class TestCurveFrxusdUsdtLPOpenPolygon:
    """Test Curve frxusd_usdt LP_OPEN using LPOpenIntent on Polygon.

    Verifies the full Intent flow:
    - LPOpenIntent with pool=frxusd_usdt, USDT + frxUSD amounts
    - IntentCompiler generates approve + add_liquidity (NG dynamic-array) TXs
    - Transactions execute on Anvil fork of Polygon
    - AddLiquidity (dynamic-array NG variant) event parsed from receipt
    - LP tokens minted and balance delta verified
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_usdt_frxusd(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test adding USDT + frxUSD to the Curve NG pool on Polygon.

        Flow:
        1. Fund wallet with USDT and frxUSD via slot manipulation
        2. Record balances BEFORE (USDT, frxUSD, LP token)
        3. Create LPOpenIntent for frxusd_usdt
        4. Compile to ActionBundle (approve USDT + approve frxUSD + add_liquidity)
        5. Execute on-chain
        6. Parse receipt for AddLiquidity event
        7. Verify LP tokens received and token balance deltas
        """
        _verify_pool_exists()

        # Fund USDT and frxUSD (frxUSD is not in the standard polygon funded set)
        _fund_usdt(funded_wallet, anvil_rpc_url)
        _fund_frxusd(funded_wallet, anvil_rpc_url)

        usdt_funded = get_token_balance(web3, USDT_ADDRESS, funded_wallet)
        frxusd_funded = get_token_balance(web3, FRXUSD_ADDRESS, funded_wallet)
        # Fail fast on funding regressions (silent skips were masking storage-
        # slot regressions on Polygon — VIB-4307 review).
        assert usdt_funded > 0, (
            f"USDT funding failed at slot {USDT_BALANCE_SLOT}. "
            "Polygon USDT storage layout may have changed — fail fast to "
            "surface the infra regression."
        )
        assert frxusd_funded > 0, (
            f"frxUSD funding failed at slot {FRXUSD_BALANCE_SLOT}. "
            "frxUSD proxy implementation storage layout may have changed — "
            "fail fast to surface the infra regression."
        )

        # --- Layer 4 BEFORE ---
        usdt_before = get_token_balance(web3, USDT_ADDRESS, funded_wallet)
        frxusd_before = get_token_balance(web3, FRXUSD_ADDRESS, funded_wallet)
        lp_before = _get_lp_token_balance(web3, funded_wallet)

        # --- Layer 1: Compile ---
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_USDT,
            amount1=LP_AMOUNT_FRXUSD,
            range_lower=Decimal("1"),  # Dummy — Curve uses pool-based positions
            range_upper=Decimal("1000000"),  # Dummy — required by LPOpenIntent validation
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
            f"Curve LP_OPEN compilation failed on Polygon: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Curve LP_OPEN execution failed on Polygon: {execution_result.error}"
        execution_result = enrich_for_accounting(
            execution_result,
            intent,
            funded_wallet,
            chain=CHAIN_NAME,
            bundle_metadata=compilation_result.action_bundle.metadata,
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
            "Parser must detect the NG dynamic-array AddLiquidity on Polygon frxusd_usdt."
        )
        assert lp_tokens_from_receipt is not None and lp_tokens_from_receipt > 0, (
            "LP tokens minted must be > 0 and extractable from receipt Transfer event."
        )

        # --- Layer 4 AFTER: Balance Deltas ---
        usdt_after = get_token_balance(web3, USDT_ADDRESS, funded_wallet)
        frxusd_after = get_token_balance(web3, FRXUSD_ADDRESS, funded_wallet)
        lp_after = _get_lp_token_balance(web3, funded_wallet)

        usdt_spent = usdt_before - usdt_after
        frxusd_spent = frxusd_before - frxusd_after
        lp_received = lp_after - lp_before

        expected_usdt_spent = int(LP_AMOUNT_USDT * Decimal(10**6))
        expected_frxusd_spent = int(LP_AMOUNT_FRXUSD * Decimal(10**18))

        assert usdt_spent == expected_usdt_spent, (
            f"USDT spent must EXACTLY equal LP_OPEN amount. Expected: {expected_usdt_spent}, Got: {usdt_spent}"
        )
        assert frxusd_spent == expected_frxusd_spent, (
            f"frxUSD spent must EXACTLY equal LP_OPEN amount. Expected: {expected_frxusd_spent}, Got: {frxusd_spent}"
        )
        assert lp_received > 0, f"LP tokens received must be > 0, got {lp_received}"

        # LP token receipt extraction cross-check
        lp_received_decimal = Decimal(lp_received) / Decimal(10**18)
        assert lp_received_decimal == lp_tokens_from_receipt, (
            f"LP tokens from balance delta ({lp_received_decimal}) must match receipt ({lp_tokens_from_receipt})"
        )

        logger.info(
            f"LP_OPEN: USDT spent={usdt_spent / 10**6:.6f}, "
            f"frxUSD spent={frxusd_spent / 10**18:.6f}, "
            f"LP received={lp_received}"
        )

        # --- Layer 5: real accounting pipeline (VIB-4968) ---
        # The parser stamps a canonical 0x pool address so lp_handler books a
        # typed LP_OPEN event. USD aggregates are expected: USDT and frxUSD are
        # both CURVE_USD_STABLE_SYMBOLS members ($1-numeraire peg applies).
        await assert_curve_lp_layer5(
            layer5_accounting_harness,
            intent=intent,
            result=execution_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            event_type="LP_OPEN",
            expect_usd_aggregates=True,
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            expected_pool_address=POOL_ADDRESS,
        )


# =============================================================================
# LP Lifecycle Tests (Open -> Close)
# =============================================================================


@pytest.mark.polygon
@pytest.mark.lp
class TestCurveFrxusdUsdtLPLifecyclePolygon:
    """Test full Curve frxusd_usdt LP lifecycle: LP_OPEN then LP_CLOSE on Polygon.

    Verifies:
    - LP_OPEN adds liquidity and mints NG LP tokens (pool address)
    - LP_CLOSE burns LP tokens and returns USDT + frxUSD
    - RemoveLiquidity (dynamic-array NG variant) event parsed from close receipt
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
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test full Curve frxusd_usdt LP lifecycle on Polygon: open then close.

        Flow:
        1. Fund wallet with USDT and frxUSD
        2. LP_OPEN: deposit USDT + frxUSD into frxusd_usdt
        3. Extract LP token balance
        4. LP_CLOSE: burn all LP tokens proportionally
        5. Verify RemoveLiquidity event + balance deltas
        """
        _verify_pool_exists()

        # Fund USDT and frxUSD
        _fund_usdt(funded_wallet, anvil_rpc_url)
        _fund_frxusd(funded_wallet, anvil_rpc_url)
        usdt_funded = get_token_balance(web3, USDT_ADDRESS, funded_wallet)
        frxusd_funded = get_token_balance(web3, FRXUSD_ADDRESS, funded_wallet)
        # Fail fast on funding regressions (silent skips were masking storage-
        # slot regressions on Polygon — VIB-4307 review).
        assert usdt_funded > 0, (
            "USDT funding failed on Polygon. Storage layout may have changed "
            "— fail fast to surface the infra regression."
        )
        assert frxusd_funded > 0, (
            "frxUSD funding failed on Polygon. Proxy storage layout may have "
            "changed — fail fast to surface the infra regression."
        )

        # ==================== OPEN ====================
        open_intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_USDT,
            amount1=LP_AMOUNT_FRXUSD,
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
        open_exec = enrich_for_accounting(
            open_exec,
            open_intent,
            funded_wallet,
            chain=CHAIN_NAME,
            bundle_metadata=open_result.action_bundle.metadata,
        )

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

        # Layer 5: persist LP_OPEN — VIB-4968 books a typed LP_OPEN event.
        open_accounting_row = await assert_curve_lp_layer5(
            layer5_accounting_harness,
            intent=open_intent,
            result=open_exec,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            event_type="LP_OPEN",
            expect_usd_aggregates=True,
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            expected_pool_address=POOL_ADDRESS,
        )

        # ==================== CLOSE ====================
        lp_balance = _get_lp_token_balance(web3, funded_wallet)
        assert lp_balance > 0, "Must have LP tokens before LP_CLOSE test"

        # --- Layer 4 BEFORE close ---
        usdt_before_close = get_token_balance(web3, USDT_ADDRESS, funded_wallet)
        frxusd_before_close = get_token_balance(web3, FRXUSD_ADDRESS, funded_wallet)

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
        assert close_result.status.value == "SUCCESS", f"LP_CLOSE compile failed: {close_result.error}"
        assert close_result.action_bundle is not None

        # Layer 2: Execute LP_CLOSE
        close_exec = await orchestrator.execute(close_result.action_bundle)
        assert close_exec.success, f"LP_CLOSE execution failed: {close_exec.error}"
        close_exec = enrich_for_accounting(
            close_exec,
            close_intent,
            funded_wallet,
            chain=CHAIN_NAME,
            bundle_metadata=close_result.action_bundle.metadata,
        )

        # Layer 3: Parse LP_CLOSE receipt and capture token_amounts so Layer 4
        # can reconcile exact deltas (not just positivity).
        remove_liquidity_found = False
        parsed_token_amounts: list[int] = []
        lp_close_data = None

        for tx_result in close_exec.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result.success

            for event in parse_result.events:
                if event.event_type == CurveEventType.REMOVE_LIQUIDITY:
                    remove_liquidity_found = True
                    raw_amounts = event.data.get("token_amounts") or []
                    parsed_token_amounts = [int(x) for x in raw_amounts]
                    logger.info(f"RemoveLiquidity event: token_amounts={parsed_token_amounts}")

            extracted = parser.extract_lp_close_data(receipt_dict)
            if extracted is not None:
                lp_close_data = extracted

        assert remove_liquidity_found, "RemoveLiquidity event must be found in LP_CLOSE receipt."
        assert len(parsed_token_amounts) == 2, (
            f"frxusd_usdt RemoveLiquidity must emit token_amounts for 2 coins; got {parsed_token_amounts}"
        )

        # Layer 4 AFTER close: balance deltas reconciled to parsed amounts.
        # frxusd_usdt index order: 0=USDT, 1=frxUSD.
        lp_after_close = _get_lp_token_balance(web3, funded_wallet)
        usdt_after_close = get_token_balance(web3, USDT_ADDRESS, funded_wallet)
        frxusd_after_close = get_token_balance(web3, FRXUSD_ADDRESS, funded_wallet)

        lp_burned = lp_balance - lp_after_close
        usdt_returned = usdt_after_close - usdt_before_close
        frxusd_returned = frxusd_after_close - frxusd_before_close

        # lp_tokens_received is HUMAN units (Decimal); lp_burned is raw wei.
        expected_lp_burned = int(lp_tokens_received * Decimal(10**18))
        assert lp_burned == expected_lp_burned, (
            f"LP burned must exactly equal requested close amount. requested={expected_lp_burned}, burned={lp_burned}"
        )
        assert usdt_returned == parsed_token_amounts[0], (
            f"USDT delta must exactly equal parsed RemoveLiquidity amount. "
            f"wallet={usdt_returned}, parsed={parsed_token_amounts[0]}"
        )
        assert frxusd_returned == parsed_token_amounts[1], (
            f"frxUSD delta must exactly equal parsed RemoveLiquidity amount. "
            f"wallet={frxusd_returned}, parsed={parsed_token_amounts[1]}"
        )

        logger.info(
            f"LP_CLOSE success: burned {lp_burned / 1e18:.6f} LP, "
            f"received {usdt_returned / 10**6:.4f} USDT + "
            f"{frxusd_returned / 10**18:.4f} frxUSD"
        )

        # --- Layer 5: real accounting pipeline LP_CLOSE (VIB-4968) ---
        assert lp_close_data is not None, "Layer-5 assertion needs parsed LPCloseData"
        await assert_curve_lp_layer5(
            layer5_accounting_harness,
            intent=close_intent,
            result=close_exec,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            event_type="LP_CLOSE",
            expect_usd_aggregates=True,
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            prior_open_row=open_accounting_row,
        )
