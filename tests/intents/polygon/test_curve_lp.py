"""Curve am3pool LP Intent tests for Polygon (VIB-4307).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Adding liquidity to Curve am3pool (DAI/USDC.e/USDT) on Polygon
- LPCloseIntent: Removing liquidity proportionally

Pool: Curve am3pool on Polygon (aave-type StableSwap variant)
- Address: 0x445FE580eF8d70FF569aB36e80c647af338db351
- LP token: 0xE7a24EF0C5e95Ffb0f6684b813A78F2a3AD7D171 (am3CRV)
- Coins[0]: DAI (0x8f3Cf7ad...)
- Coins[1]: USDC.e (0x2791Bca1..., bridged USDC)
- Coins[2]: USDT (0xc2132D05...)
- Type: stableswap with use_underlying=True (aave-type)

LPOpenIntent supports 2-coin deposits (amount0 + amount1; remaining padded to 0).
We deposit DAI + USDC.e (USDT=0).

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

# Curve am3pool on Polygon (aave-type StableSwap)
POOL = "3pool"
# VIB-5434: corrected from the dead 0x445Fe580…898ed8631406dB5f literal (no code on
# Polygon) to the real am3pool. Verified on-fork 2026-06-30.
POOL_ADDRESS = "0x445FE580eF8d70FF569aB36e80c647af338db351"
LP_TOKEN = "0xE7a24EF0C5e95Ffb0f6684b813A78F2a3AD7D171"  # am3CRV LP token

# Token addresses (coin order: DAI=0, USDC.e=1, USDT=2)
DAI_ADDRESS = "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063"
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # bridged USDC (USDC.e)
USDT_ADDRESS = "0xc2132D05D31c914a87C6611C10748AEb04B58e8F"

# Storage slots for token funding (Polygon PoS-bridged tokens use slot 0)
DAI_BALANCE_SLOT = 0
USDC_E_BALANCE_SLOT = 0

# LP deposit amounts (small to keep slippage low)
LP_AMOUNT_DAI = Decimal("10")  # 10 DAI (18 decimals)
LP_AMOUNT_USDC_E = Decimal("10")  # 10 USDC.e (6 decimals)


# =============================================================================
# Helpers
# =============================================================================


def _fund_dai(wallet: str, rpc_url: str, amount: Decimal = Decimal("10000")) -> None:
    """Fund test wallet with DAI on Polygon via storage slot manipulation."""
    decimals = 18
    amount_wei = int(amount * Decimal(10**decimals))
    fund_erc20_token(wallet, DAI_ADDRESS, amount_wei, DAI_BALANCE_SLOT, rpc_url)


def _fund_usdc_e(wallet: str, rpc_url: str, amount: Decimal = Decimal("10000")) -> None:
    """Fund test wallet with USDC.e (bridged USDC) on Polygon via storage slot."""
    decimals = 6
    amount_wei = int(amount * Decimal(10**decimals))
    fund_erc20_token(wallet, USDC_E_ADDRESS, amount_wei, USDC_E_BALANCE_SLOT, rpc_url)


def _get_lp_token_balance(web3: Web3, wallet: str) -> int:
    """Get current LP token (am3CRV) balance for the wallet."""
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
class TestCurveAm3poolLPOpenPolygon:
    """Test Curve am3pool LP_OPEN using LPOpenIntent on Polygon.

    Verifies the full Intent flow:
    - LPOpenIntent with pool=3pool, DAI + USDC.e amounts
    - IntentCompiler generates approve + add_liquidity TXs for Polygon chain
    - Transactions execute on Anvil fork of Polygon
    - AddLiquidity event parsed from receipt
    - LP tokens minted and balance delta verified
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=True,
        reason=(
            "VIB-5551: am3pool LP_OPEN reverts on a current Polygon fork — the aave-type "
            "underlying deposit routes through the FROZEN Aave V2 Polygon LendingPool "
            "(VL_RESERVE_FROZEN), and the compiler emits the non-aave "
            "add_liquidity(uint256[3],uint256) selector. The receipt-parser decode itself "
            "is PROVEN by tests/unit/connectors/curve/test_am3pool_real_logs.py against real "
            "on-fork AddLiquidity3/RemoveLiquidity3 logs (the VIB-4307 'missing signatures' "
            "claim was stale). as of 2026-06-30."
        ),
    )
    async def test_lp_open_dai_usdc_e(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test adding DAI + USDC.e to Curve am3pool on Polygon.

        Flow:
        1. Fund wallet with DAI and USDC.e via slot manipulation
        2. Record balances BEFORE (DAI, USDC.e, LP token)
        3. Create LPOpenIntent for Curve am3pool
        4. Compile to ActionBundle (approve DAI + approve USDC.e + add_liquidity)
        5. Execute on-chain
        6. Parse receipt for AddLiquidity event
        7. Verify LP tokens received and token balance deltas
        """
        _verify_pool_exists()

        # Fund DAI and USDC.e (not in standard polygon funded_wallet set)
        _fund_dai(funded_wallet, anvil_rpc_url)
        _fund_usdc_e(funded_wallet, anvil_rpc_url)

        dai_funded = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        usdc_e_funded = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        # Fail fast on funding regressions (silent skips were masking storage-
        # slot regressions on Polygon DAI / USDC.e — VIB-4307 review).
        assert dai_funded > 0, (
            f"DAI funding failed at slot {DAI_BALANCE_SLOT}. "
            "Polygon DAI storage layout may have changed — fail fast to "
            "surface the infra regression."
        )
        assert usdc_e_funded > 0, (
            f"USDC.e funding failed at slot {USDC_E_BALANCE_SLOT}. "
            "Polygon USDC.e storage layout may have changed — fail fast to "
            "surface the infra regression."
        )

        # --- Layer 4 BEFORE ---
        dai_before = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        usdc_e_before = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        lp_before = _get_lp_token_balance(web3, funded_wallet)

        # --- Layer 1: Compile ---
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_DAI,
            amount1=LP_AMOUNT_USDC_E,
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
            "Parser must detect Curve AddLiquidity events on Polygon am3pool."
        )
        assert lp_tokens_from_receipt is not None and lp_tokens_from_receipt > 0, (
            "LP tokens minted must be > 0 and extractable from receipt Transfer event."
        )

        # --- Layer 4 AFTER: Balance Deltas ---
        dai_after = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        usdc_e_after = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        lp_after = _get_lp_token_balance(web3, funded_wallet)

        dai_spent = dai_before - dai_after
        usdc_e_spent = usdc_e_before - usdc_e_after
        lp_received = lp_after - lp_before

        expected_dai_spent = int(LP_AMOUNT_DAI * Decimal(10**18))
        expected_usdc_e_spent = int(LP_AMOUNT_USDC_E * Decimal(10**6))

        assert dai_spent == expected_dai_spent, (
            f"DAI spent must EXACTLY equal LP_OPEN amount. Expected: {expected_dai_spent}, Got: {dai_spent}"
        )
        assert usdc_e_spent == expected_usdc_e_spent, (
            f"USDC.e spent must EXACTLY equal LP_OPEN amount. Expected: {expected_usdc_e_spent}, Got: {usdc_e_spent}"
        )
        assert lp_received > 0, f"LP tokens received must be > 0, got {lp_received}"

        # LP token receipt extraction cross-check
        lp_received_decimal = Decimal(lp_received) / Decimal(10**18)
        assert lp_received_decimal == lp_tokens_from_receipt, (
            f"LP tokens from balance delta ({lp_received_decimal}) must match receipt ({lp_tokens_from_receipt})"
        )

        logger.info(
            f"LP_OPEN: DAI spent={dai_spent / 10**18:.6f}, "
            f"USDC.e spent={usdc_e_spent / 10**6:.6f}, "
            f"LP received={lp_received}"
        )

        # --- Layer 5: real accounting pipeline (VIB-4968) ---
        # Post-VIB-4968 the parser stamps a canonical 0x pool address so
        # lp_handler books a typed LP_OPEN event. NOTE: this test is still
        # xfail-marked, but for VIB-5551 (the aave-type underlying deposit reverts
        # at the FROZEN Aave V2 Polygon LendingPool — the test fails at Layer-2
        # EXECUTION, never reaching here). The receipt parser DOES decode am3pool's
        # AddLiquidity3/RemoveLiquidity3 events (VIB-4307's "missing signatures"
        # claim was stale) — proven by
        # tests/unit/connectors/curve/test_am3pool_real_logs.py.
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
class TestCurveAm3poolLPLifecyclePolygon:
    """Test full Curve am3pool LP lifecycle: LP_OPEN then LP_CLOSE on Polygon.

    Verifies:
    - LP_OPEN adds liquidity and mints am3CRV LP tokens
    - LP_CLOSE burns LP tokens and returns DAI + USDC.e + USDT
    - RemoveLiquidity event parsed from close receipt
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=True,
        reason=(
            "VIB-5551: am3pool LP_OPEN→CLOSE reverts on a current Polygon fork — the aave-type "
            "underlying deposit routes through the FROZEN Aave V2 Polygon LendingPool "
            "(VL_RESERVE_FROZEN), and the compiler emits the non-aave "
            "add_liquidity(uint256[3],uint256) selector. The receipt-parser decode itself "
            "is PROVEN by tests/unit/connectors/curve/test_am3pool_real_logs.py against real "
            "on-fork AddLiquidity3/RemoveLiquidity3 logs (the VIB-4307 'missing signatures' "
            "claim was stale). as of 2026-06-30."
        ),
    )
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
        """Test full Curve am3pool LP lifecycle on Polygon: open then close.

        Flow:
        1. Fund wallet with DAI and USDC.e
        2. LP_OPEN: deposit DAI + USDC.e into am3pool
        3. Extract LP token balance
        4. LP_CLOSE: burn all LP tokens proportionally
        5. Verify RemoveLiquidity event + balance deltas
        """
        _verify_pool_exists()

        # Fund DAI and USDC.e
        _fund_dai(funded_wallet, anvil_rpc_url)
        _fund_usdc_e(funded_wallet, anvil_rpc_url)
        dai_funded = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        usdc_e_funded = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        # Fail fast on funding regressions (silent skips were masking storage-
        # slot regressions on Polygon DAI / USDC.e — VIB-4307 review).
        assert dai_funded > 0, (
            "DAI funding failed on Polygon. Storage layout may have changed "
            "— fail fast to surface the infra regression."
        )
        assert usdc_e_funded > 0, (
            "USDC.e funding failed on Polygon. Storage layout may have changed "
            "— fail fast to surface the infra regression."
        )

        # ==================== OPEN ====================
        open_intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_DAI,
            amount1=LP_AMOUNT_USDC_E,
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
        dai_before_close = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        usdc_e_before_close = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        usdt_before_close = get_token_balance(web3, USDT_ADDRESS, funded_wallet)

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
        assert len(parsed_token_amounts) == 3, (
            f"am3pool RemoveLiquidity must emit token_amounts for 3 coins; got {parsed_token_amounts}"
        )

        # Layer 4 AFTER close: balance deltas reconciled to parsed amounts.
        # am3pool index order: 0=DAI, 1=USDC.e, 2=USDT.
        lp_after_close = _get_lp_token_balance(web3, funded_wallet)
        dai_after_close = get_token_balance(web3, DAI_ADDRESS, funded_wallet)
        usdc_e_after_close = get_token_balance(web3, USDC_E_ADDRESS, funded_wallet)
        usdt_after_close = get_token_balance(web3, USDT_ADDRESS, funded_wallet)

        lp_burned = lp_balance - lp_after_close
        dai_returned = dai_after_close - dai_before_close
        usdc_e_returned = usdc_e_after_close - usdc_e_before_close
        usdt_returned = usdt_after_close - usdt_before_close

        assert lp_burned == lp_tokens_received, (
            f"LP burned must exactly equal requested close amount. requested={lp_tokens_received}, burned={lp_burned}"
        )
        assert dai_returned == parsed_token_amounts[0], (
            f"DAI delta must exactly equal parsed RemoveLiquidity amount. "
            f"wallet={dai_returned}, parsed={parsed_token_amounts[0]}"
        )
        assert usdc_e_returned == parsed_token_amounts[1], (
            f"USDC.e delta must exactly equal parsed RemoveLiquidity amount. "
            f"wallet={usdc_e_returned}, parsed={parsed_token_amounts[1]}"
        )
        assert usdt_returned == parsed_token_amounts[2], (
            f"USDT delta must exactly equal parsed RemoveLiquidity amount. "
            f"wallet={usdt_returned}, parsed={parsed_token_amounts[2]}"
        )

        logger.info(
            f"LP_CLOSE success: burned {lp_burned / 1e18:.6f} am3CRV, "
            f"received {dai_returned / 10**18:.4f} DAI + "
            f"{usdc_e_returned / 10**6:.4f} USDC.e + "
            f"{usdt_returned / 10**6:.4f} USDT"
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
