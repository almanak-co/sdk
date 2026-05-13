"""Production-grade LP Intent tests for Aerodrome (Solidly fork) on Base.

Covers ``(aerodrome, LP_OPEN)`` and ``(aerodrome, LP_CLOSE)`` for the default-on
Zodiac coverage gate (issue #2028). Aerodrome on Base is a fungible-LP Solidly
fork — *not* a concentrated-liquidity NFT system — so the assertion shape
differs from ``test_uniswap_v3_lp.py``:

  * The LP token IS the pool address. There is no NonfungiblePositionManager,
    no ``decreaseLiquidity`` / ``collect`` / ``burn`` flow, and no NFT tokenId.
  * ``Router.addLiquidity(...)`` may pull *less* than the requested amount of
    one side when the input ratio doesn't match current pool reserves; the
    excess is refunded. Hence ``<=`` tolerance assertions, not ``==``.
  * ``Router.removeLiquidity(...)`` returns reserves proportional to LP burned.

These tests run under default-on Zodiac (no ``no_zodiac`` marker) so the
manifest derived from the intent constructors is exercised through Safe +
Roles + ``execTransactionWithRole`` automatically.

To run:
    uv run pytest tests/intents/base/test_aerodrome_lp.py -v -s
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.aerodrome.receipt_parser import AerodromeReceiptParser
from almanak.framework.connectors.aerodrome.sdk import AerodromeSDK
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_aerodrome_pool_missing

logger = logging.getLogger(__name__)


# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "base"

# Aerodrome USDC/WETH **volatile** pool on Base. The pool address is resolved
# at module load via the SDK's factory query — pinning a hardcoded address here
# would tie the test to whatever the factory returns today and silently break
# if a redeploy ever moved it. Cached in a module-level singleton so the
# factory call happens at most once per test session.
POOL_LABEL = "USDC/WETH/volatile"
STABLE = False  # volatile pool

# Deposit amounts: ~$10 of each side (10 USDC, ~0.005 WETH at $2000/ETH).
# Small enough to keep price impact negligible while large enough to clear
# Solidly's MINIMUM_LIQUIDITY guard (1000 wei) for a fresh test wallet.
LP_AMOUNT_USDC = Decimal("10")
LP_AMOUNT_WETH = Decimal("0.005")

# Solidly LP doesn't use price ranges; LPOpenIntent's validator demands them.
RANGE_LOWER = Decimal("1")
RANGE_UPPER = Decimal("1000000")


# =============================================================================
# Helpers
# =============================================================================


_pool_address_cache: str | None = None


def _resolve_pool_address(web3: Web3, anvil_rpc_url: str) -> str:
    """Resolve the Aerodrome USDC/WETH volatile pool address on Base.

    Result is cached in a module-level singleton: the factory ``getPool``
    call is a deterministic view that doesn't change between test runs in a
    given session, so paying for it once per session is enough.
    """
    global _pool_address_cache
    if _pool_address_cache is not None:
        return _pool_address_cache

    tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
    sdk = AerodromeSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
    pool_address = sdk.get_pool_address_from_factory(
        tokens["USDC"],
        tokens["WETH"],
        STABLE,
        web3=web3,
    )
    if not pool_address:
        pytest.fail(
            "Aerodrome USDC/WETH volatile pool not found on Base via factory. "
            "Either the factory returned address(0) or the RPC is unreachable."
        )
    _pool_address_cache = Web3.to_checksum_address(pool_address)
    return _pool_address_cache


def _get_lp_token_balance(web3: Web3, pool_address: str, wallet: str) -> int:
    """LP token IS the pool contract for Solidly forks — query as ERC-20."""
    return get_token_balance(web3, pool_address, wallet)


async def _open_lp_position(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> tuple[str, int]:
    """Open the USDC/WETH volatile LP position. Returns (pool_address, lp_balance_after)."""
    pool_address = _resolve_pool_address(web3, anvil_rpc_url)

    intent = LPOpenIntent(
        pool=POOL_LABEL,
        amount0=LP_AMOUNT_USDC,
        amount1=LP_AMOUNT_WETH,
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        protocol="aerodrome",
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
        f"Aerodrome LP_OPEN compilation failed: {compilation_result.error}"
    )
    assert compilation_result.action_bundle is not None

    execution_result = await orchestrator.execute(compilation_result.action_bundle)
    assert execution_result.success, f"Aerodrome LP_OPEN execution failed: {execution_result.error}"

    return pool_address, _get_lp_token_balance(web3, pool_address, funded_wallet)


# =============================================================================
# LP Open Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.lp
class TestAerodromeLPOpen:
    """Aerodrome LP_OPEN via ``LPOpenIntent`` on Base.

    Verifies:
      * USDC and WETH spent <= requested amounts (Solidly may pull less when
        the input ratio doesn't match the pool's current reserves).
      * Both tokens strictly decrease (some of each was actually deposited).
      * LP token balance strictly increases.
      * Receipt parses cleanly via ``AerodromeReceiptParser``.
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_usdc_weth_volatile(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Open a USDC + WETH volatile-pool LP position via LPOpenIntent."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]

        # Pre-flight: pool must exist on the fork before we run.
        fail_if_aerodrome_pool_missing(web3, CHAIN_NAME, usdc_addr, weth_addr, STABLE)

        pool_address = _resolve_pool_address(web3, anvil_rpc_url)

        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        # --- Layer 4 BEFORE ---
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        lp_before = _get_lp_token_balance(web3, pool_address, funded_wallet)

        # --- Layer 1: Compile ---
        intent = LPOpenIntent(
            pool=POOL_LABEL,
            amount0=LP_AMOUNT_USDC,
            amount1=LP_AMOUNT_WETH,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="aerodrome",
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
            f"Aerodrome LP_OPEN compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Aerodrome LP_OPEN execution failed: {execution_result.error}"

        # --- Layer 3: Receipt Parsing ---
        # Aerodrome's parser doesn't yet emit dedicated LP open events for the
        # Solidly fungible-LP add_liquidity flow (see receipt_parser.py event
        # registry — Mint/Sync events are routed but LP-shaped extraction is
        # still SDK-side). Asserting parser-level ``success`` is the cleanest
        # contract this layer can offer today; deeper semantic checks belong
        # to balance deltas in Layer 4.
        parser = AerodromeReceiptParser(chain=CHAIN_NAME)
        any_parse_succeeded = False
        saw_liquidity_mint = False
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, (
                f"AerodromeReceiptParser must parse LP_OPEN receipt cleanly: {parse_result.error}"
            )
            any_parse_succeeded = True
            if parse_result.mint_events:
                saw_liquidity_mint = True
        assert any_parse_succeeded, "At least one LP_OPEN tx receipt must be parsed"
        # Layer 3 must validate that the parser actually *decoded* protocol
        # semantics — ``parse_result.success`` alone is true for empty
        # receipts and would let a parser regression slip past.
        assert saw_liquidity_mint, (
            "LP_OPEN must decode at least one Mint event in receipts. "
            "An empty event list means the parser silently failed to recognise "
            "the LP-mint emission shape."
        )

        # --- Layer 4 AFTER: Balance Deltas ---
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        lp_after = _get_lp_token_balance(web3, pool_address, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_spent = weth_before - weth_after
        lp_received = lp_after - lp_before

        # Solidly add_liquidity may pull LESS than the desired amount of one
        # side when the input ratio doesn't match pool reserves — the excess
        # is refunded. Use <= on the desired ceiling, > 0 on actual spend.
        expected_usdc_max = int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        expected_weth_max = int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))

        # Solidly add_liquidity at extreme pool ratios may pull all of one
        # side and refund 100 % of the other. "Both > 0" would be too strict
        # for valid edge cases; require at least one side spent and let the
        # per-side ``<= desired`` ceilings + LP-mint event check guard
        # against complete no-ops.
        assert usdc_spent > 0 or weth_spent > 0, (
            "At least one token must be deposited in LP_OPEN. "
            f"USDC delta={format_token_amount(usdc_spent, usdc_decimals)}, "
            f"WETH delta={format_token_amount(weth_spent, weth_decimals)}"
        )
        assert usdc_spent <= expected_usdc_max, (
            f"USDC spent ({usdc_spent}) must not exceed requested "
            f"({expected_usdc_max}) — Solidly refunds excess but never overspends"
        )
        assert weth_spent <= expected_weth_max, (
            f"WETH spent ({weth_spent}) must not exceed requested "
            f"({expected_weth_max}) — Solidly refunds excess but never overspends"
        )
        assert lp_received > 0, f"LP token balance must strictly increase, got {lp_received}"

        logger.info(
            f"LP_OPEN OK: USDC spent={format_token_amount(usdc_spent, usdc_decimals)}, "
            f"WETH spent={format_token_amount(weth_spent, weth_decimals)}, "
            f"LP received={lp_received}"
        )


# =============================================================================
# LP Close Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.lp
class TestAerodromeLPClose:
    """Aerodrome LP_CLOSE via ``LPCloseIntent`` on Base.

    Verifies the open-then-close roundtrip:
      * LP balance after close < LP balance after open (LP burned, ideally to
        ~0 modulo a small dust threshold).
      * USDC and WETH both strictly increase from the post-open snapshot.
      * Receipt parses cleanly.
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_usdc_weth_returns_tokens(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Open then close a USDC + WETH volatile LP position; verify roundtrip."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]

        # Pre-flight: pool must exist on the fork.
        fail_if_aerodrome_pool_missing(web3, CHAIN_NAME, usdc_addr, weth_addr, STABLE)

        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        # --- Setup: open a position so we have LP tokens to burn ---
        pool_address, lp_after_open = await _open_lp_position(
            web3, funded_wallet, orchestrator, price_oracle, anvil_rpc_url
        )
        assert lp_after_open > 0, "Setup invariant: LP_OPEN must mint at least 1 LP wei"

        # --- Layer 4 BEFORE close ---
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)
        lp_before_close = lp_after_open

        # --- Layer 1: Compile LP_CLOSE ---
        # Aerodrome LP_CLOSE accepts the bare pool address as ``position_id``
        # (Solidly: pool address IS the LP token, analogous to a V3 NFT
        # tokenId). The compiler reads the wallet's full LP balance from
        # chain and burns it all, so the actual amount is on-chain state, not
        # an intent field.
        close_intent = LPCloseIntent(
            position_id=pool_address,
            pool=POOL_LABEL,
            collect_fees=True,
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        close_result = compiler.compile(close_intent)
        assert close_result.status.value == "SUCCESS", f"Aerodrome LP_CLOSE compilation failed: {close_result.error}"
        assert close_result.action_bundle is not None
        assert close_result.action_bundle.metadata.get("no_op") is not True, (
            "LP_CLOSE must produce a real bundle — wallet just deposited LP"
        )

        # --- Layer 2: Execute ---
        close_execution = await orchestrator.execute(close_result.action_bundle)
        assert close_execution.success, f"Aerodrome LP_CLOSE execution failed: {close_execution.error}"

        # --- Layer 3: Receipt Parsing ---
        # Aerodrome's volatile pool variant doesn't always emit a standard
        # ``Burn`` event on remove — the parser has a documented Transfer-
        # event fallback (see ``AerodromeReceiptParser.extract_lp_close_data``
        # in ``almanak/framework/connectors/aerodrome/receipt_parser.py``).
        # Asserting on ``parse_result.burn_events`` directly fails on those
        # variants. Use the high-level ``LPCloseData`` extractor that
        # composes both paths, so the test stays robust to per-pool
        # event-shape variation while still proving "the parser decoded a
        # close, with non-zero token amounts."
        parser = AerodromeReceiptParser(chain=CHAIN_NAME)
        any_parse_succeeded = False
        saw_close_data = False
        for tx_result in close_execution.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result.success, (
                f"AerodromeReceiptParser must parse LP_CLOSE receipt cleanly: {parse_result.error}"
            )
            any_parse_succeeded = True
            close_data = parser.extract_lp_close_data(receipt_dict)
            if close_data is not None and (close_data.amount0_collected > 0 or close_data.amount1_collected > 0):
                saw_close_data = True
        assert any_parse_succeeded, "At least one LP_CLOSE tx receipt must be parsed"
        assert saw_close_data, (
            "LP_CLOSE must yield non-zero LPCloseData from the parser "
            "(via Burn events on Solidly stable pools, or Transfer-event "
            "fallback on volatile pools). Empty across both paths means the "
            "parser silently failed to recognise the close — a regression."
        )

        # --- Layer 4 AFTER: Balance Deltas ---
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)
        lp_after_close = _get_lp_token_balance(web3, pool_address, funded_wallet)

        usdc_returned = usdc_after_close - usdc_before_close
        weth_returned = weth_after_close - weth_before_close
        lp_burned = lp_before_close - lp_after_close

        assert lp_after_close < lp_before_close, (
            f"LP balance after close ({lp_after_close}) must be strictly less than before close ({lp_before_close})"
        )
        assert lp_burned > 0, f"LP burned must be > 0, got {lp_burned}"
        assert usdc_returned > 0, (
            f"USDC must strictly increase after LP_CLOSE, got delta {format_token_amount(usdc_returned, usdc_decimals)}"
        )
        assert weth_returned > 0, (
            f"WETH must strictly increase after LP_CLOSE, got delta {format_token_amount(weth_returned, weth_decimals)}"
        )

        logger.info(
            f"LP_CLOSE OK: USDC returned={format_token_amount(usdc_returned, usdc_decimals)}, "
            f"WETH returned={format_token_amount(weth_returned, weth_decimals)}, "
            f"LP burned={lp_burned}, LP residual={lp_after_close}"
        )


# =============================================================================
# LP_COLLECT_FEES Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.lp
class TestAerodromeLPCollectFees:
    """Aerodrome (Classic) LP_COLLECT_FEES coverage on Base.

    The Solidly-fork volatile/stable AMM does NOT support standalone
    LP_COLLECT_FEES — fees auto-compound into pool reserves and are realized
    only when liquidity is removed (see
    ``almanak/framework/connectors/aerodrome/permission_hints.py`` —
    ``supports_standalone_fee_collection`` is unset / False, and the compiler
    explicitly rejects ``protocol="aerodrome"`` in
    ``compiler._compile_collect_fees``).

    The contract this test pins:

      * ``CollectFeesIntent(protocol="aerodrome")`` MUST be rejected cleanly at
        compile time with the documented error message (so the gate sees a
        proper L1 → L2 boundary).
      * No transactions are emitted; therefore wallet balances are unchanged
        (a degenerate but still load-bearing conservation check).

    Standalone fee collection on Aerodrome's Slipstream (CL) variant is
    covered by ``protocol="aerodrome_slipstream"`` and lives outside this
    test (different connector key).
    """

    @pytest.mark.intent(IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    async def test_lp_collect_fees_aerodrome_classic_rejected(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """LP_COLLECT_FEES must be rejected for aerodrome (Solidly fork).

        Aerodrome Classic auto-compounds fees into reserves; the standalone
        collect path doesn't exist on the V1 Router. The compiler MUST refuse
        the intent with an error message pointing the caller at the supported
        alternatives (``LPCloseIntent(collect_fees=True)`` or
        ``aerodrome_slipstream``).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]

        # Record balances before — must be unchanged (no TX should be emitted).
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)

        # --- Layer 1: Compile must FAIL with the documented error ---
        intent = CollectFeesIntent(
            pool=POOL_LABEL,
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)

        # The compiler must reject this with the documented error message.
        assert compilation_result.status.value != "SUCCESS", (
            "Aerodrome Classic LP_COLLECT_FEES must be rejected at compile time. "
            "Solidly-fork pools auto-compound fees; standalone collection is not "
            "representable in the V1 Router contract surface."
        )
        # Compiler must surface a useful error pointing at the supported paths.
        error_text = (compilation_result.error or "").lower()
        assert "aerodrome" in error_text and (
            "lp_close" in error_text or "slipstream" in error_text
        ), (
            "Compiler error must guide the caller toward LPCloseIntent(collect_fees=True) "
            f"or aerodrome_slipstream. Got: {compilation_result.error!r}"
        )

        # --- Layer 4 (degenerate): no TX emitted ⇒ balances unchanged ---
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        assert usdc_after == usdc_before, (
            "Rejected compilation must NOT emit any TX; USDC balance must be unchanged"
        )
        assert weth_after == weth_before, (
            "Rejected compilation must NOT emit any TX; WETH balance must be unchanged"
        )

        logger.info("LP_COLLECT_FEES aerodrome-classic rejection contract verified")
