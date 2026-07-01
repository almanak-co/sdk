"""Real-fork proof of the executed-floor oracle anchor (VIB-5490).

VIB-5439 added a *compile-time* oracle guard that blocks an ALREADY-displaced
pool before the tx is built. It does NOT bind the floor that lands on-chain: the
executed ``min_amount_out`` is still ``pool_quote × (1 − slippage)`` — a
percentage off the pool's *own* number — so an ATOMIC same-block sandwich passes
the clean-pool build-time check and then extracts value up to the operator's
``max_slippage`` at execution.

VIB-5490 anchors the executed floor to the independent oracle:

    min_out = max(pool_floor, min(oracle_fair × (1 − tolerance),
                                  pool_quote × (1 − residual)))

The oracle floor is capped at ``pool_quote × (1 − residual)`` — a benign
inter-block-drift buffer below the clean quote, NOT the raw quote — so the honest
guarantee is bounded-extraction (sandwich capped at ``residual`` below the clean
quote) with benign drift still filling, not "revert-safe by construction". This
test proves, on a REAL Ethereum fork of the Curve 3pool (DAI/USDC/USDT
StableSwap), the atomic-sandwich case the compile-time guard cannot see:

* (c-fires) the victim swap is COMPILED against the CLEAN pool — so the build-
  time VIB-5439 guard passes (this is the exact atomic-sandwich window). A same-
  block ADVERSE move then displaces the pool below the oracle-anchored floor, and
  executing the pre-compiled victim bundle REVERTS on-chain — the executed floor
  bites and value is conserved (no bad fill lands).
* (c-baseline) with NO oracle (pre-VIB-5490 degrade-open) and a wide slippage,
  the SAME victim swap after the SAME adverse move FILLS — proving the loose
  pool-self-referential floor accepted exactly the bad fill the anchor now blocks.

The stable no-false-revert (a) and volatile no-false-revert (b) legs of the proof
are the passing ``tests/intents/base/test_curve_swap.py`` (4pool StableSwap) and
``tests/intents/arbitrum/test_curve_swap.py`` (tricrypto CryptoSwap) full
lifecycles, which now build + execute real swaps THROUGH the clamp with no false
revert. Full evidence:
``tests/reports/vib-5490-executed-floor-oracle-anchor-realfork.md``.

The victim here uses a tight per-intent ``oracle_guard_bps`` tolerance so that a
REALISTIC, cleanly-landing displacement on the very deep 3pool suffices to push
the fill past the anchor — a knife-edge huge dump that the deep pool cannot even
fill is not needed to demonstrate the mechanism. The clamp math itself (floor =
oracle_fair × (1 − tol), max-with-pool-floor, capped at pool quote, degrade-open)
is covered exhaustively by the pure unit tests in
``tests/unit/connectors/_strategy_base/test_swap_oracle_guard.py``.

This test manages its own fork snapshot bracket and raw displacement sequence, so
it opts out of the default Safe+Roles wrap.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.curve.adapter import CURVE_POOLS
from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler
from almanak.framework.intents.compiler import CompilationStatus, IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from tests.intents.conftest import CHAIN_CONFIGS, fund_erc20_token, get_token_balance

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_zodiac(
    reason="VIB-5490: manages its own evm_snapshot/revert bracket + raw adverse-move "
    "displacement sequence to prove the executed floor bites; not a permission-manifest test."
)

CHAIN_NAME = "ethereum"

# Curve 3pool (DAI/USDC/USDT) — coin order [DAI, USDC, USDT].
POOL = CURVE_POOLS["ethereum"]["3pool"]
USDC_ADDR = Web3.to_checksum_address(POOL["coin_addresses"][1])
USDT_ADDR = Web3.to_checksum_address(POOL["coin_addresses"][2])

VICTIM_AMOUNT = Decimal("100")  # 100 USDC -> USDT
# A tight per-intent oracle tolerance (< the 50 bps stable residual). With the
# VIB-5490 residual fix, the EXECUTED stable floor is
# ``max(pool_floor, min(oracle_fair*(1-tol), quote*(1-residual)))``; a tolerance
# below the residual makes the residual the binding floor (quote*(1-50bps)), so a
# displacement must exceed ~50 bps of drift to push the fill past it. The per-intent
# tolerance is an operator knob (VIB-5439); the DEFAULT stable tolerance is 150 bps.
VICTIM_ORACLE_TOL_BPS = 3
# A single-sided USDC->USDT dump sized to drift the deep 3pool by ~80 bps — safely
# ABOVE the 50 bps stable residual floor (so the anchored victim reverts) yet well
# BELOW the victim's 10% baseline slippage (so the no-oracle loose victim still
# fills). Calibrated on the pinned fork: the 3pool holds a flat ~40 bps at 100M then
# steepens sharply (~82 bps at 105M, ~310 bps at 110M, drained past ~112M), so 105M
# sits in the usable window above the residual and below the drain cliff.
DISPLACEMENT_AMOUNT = Decimal("105000000")  # 105M USDC
# Deliberately WIDE: the whole point is that a loose pool-self-referential floor is
# sandwich-exploitable; the anchor caps damage at its residual regardless.
WIDE_SLIPPAGE = Decimal("0.10")  # 10% == 1000 bps, far past the ~80 bps displacement


def _assert_swap_receipt_parsed(exec_result, *, expected_bought_wei: int) -> None:
    """Layer 3 (receipt parse): the swap receipt carries a Curve TokenExchange event
    whose ``tokens_bought`` is positive and matches the observed balance delta."""
    parser = CurveReceiptParser(chain=CHAIN_NAME)
    bought = 0
    found = False
    for tx_result in exec_result.transaction_results:
        if not tx_result.receipt:
            continue
        receipt_dict = tx_result.receipt if isinstance(tx_result.receipt, dict) else tx_result.receipt.to_dict()
        parse_result = parser.parse_receipt(receipt_dict)
        assert parse_result is not None, "CurveReceiptParser returned None"
        if parse_result.success and parse_result.events:
            for event in parse_result.events:
                if event.event_type == CurveEventType.TOKEN_EXCHANGE:
                    found = True
                    assert event.data.get("tokens_bought", 0) > 0, "parsed tokens_bought must be > 0"
                    bought = event.data["tokens_bought"]
    assert found, "CurveReceiptParser did not find a TokenExchange event on the filled swap"
    assert bought == expected_bought_wei, (
        f"parsed tokens_bought {bought} must equal the observed balance delta {expected_bought_wei}"
    )


def _snapshot(web3: Web3) -> str:
    return web3.provider.make_request("evm_snapshot", [])["result"]


def _revert(web3: Web3, snap: str) -> None:
    web3.provider.make_request("evm_revert", [snap])


async def _displace_pool(
    *,
    funded_wallet: str,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
    orchestrator: ExecutionOrchestrator,
) -> None:
    """Simulate the sandwich front-run: a large USDC -> USDT trade that moves the
    3pool against the victim's pending USDC -> USDT swap."""
    # Fund the attacker leg with enough USDC for the heavy displacement (default
    # seeding only gives 100k). Slot from CHAIN_CONFIGS is the single source of
    # truth for the mainnet USDC balance slot.
    usdc_slot = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["USDC"]
    fund_erc20_token(
        funded_wallet,
        USDC_ADDR,
        int(DISPLACEMENT_AMOUNT * Decimal(10**6)) + int(VICTIM_AMOUNT * Decimal(10**6)) * 4,
        usdc_slot,
        anvil_rpc_url,
    )
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    intent = SwapIntent(
        from_token="USDC",
        to_token="USDT",
        amount=DISPLACEMENT_AMOUNT,
        max_slippage=Decimal("0.50"),
        protocol="curve",
        chain=CHAIN_NAME,
        # The attacker does not care about its own floor: widen its detection guard
        # so the heavy dump is not blocked as "displaced" (that is exactly what it
        # is deliberately creating). The victim keeps a tight tolerance.
        swap_params={"oracle_guard_bps": 9000},
    )
    result = compiler.compile(intent)
    assert result.status == CompilationStatus.SUCCESS, f"displacement compile failed: {result.error}"
    exec_result = await orchestrator.execute(result.action_bundle)
    assert exec_result.success, f"displacement swap failed to land: {exec_result.error}"


def _compile_victim(
    *,
    funded_wallet: str,
    price_oracle: dict[str, Decimal] | None,
    anvil_rpc_url: str,
    oracle_guard_bps: int | None,
):
    """Compile the victim USDC -> USDT swap against the CLEAN pool.

    ``price_oracle=None`` models the pre-VIB-5490 degrade-open path (placeholder
    prices → ``oracle_prices_real=False`` → the anchor degrades to the pool-self-
    referential loose floor only). A real oracle + a tolerance engages the anchor.
    """
    config = IntentCompilerConfig(allow_placeholder_prices=True) if price_oracle is None else None
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
        config=config,
    )
    swap_params = {"oracle_guard_bps": oracle_guard_bps} if oracle_guard_bps is not None else None
    intent = SwapIntent(
        from_token="USDC",
        to_token="USDT",
        amount=VICTIM_AMOUNT,
        max_slippage=WIDE_SLIPPAGE,
        protocol="curve",
        chain=CHAIN_NAME,
        swap_params=swap_params,
    )
    return compiler.compile(intent)


@pytest.mark.ethereum
@pytest.mark.swap
class TestExecutedFloorOracleAnchorRealFork:
    """VIB-5490 real-fork: the executed floor is oracle-anchored and bites on an
    in-block adverse move, while the pre-anchor loose floor would have filled."""

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_oracle_anchored_floor_bites_on_adverse_move(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> None:
        """Victim compiled on the CLEAN pool (build-time guard passes); an in-block
        adverse move then pushes the fill below the oracle-anchored floor, so
        executing the pre-compiled victim bundle REVERTS — floor bites, value
        conserved."""
        # Compile against the CLEAN pool — the VIB-5439 build-time guard sees a
        # healthy pool and passes, which is exactly the atomic-sandwich window the
        # EXECUTED anchor must close.
        victim = _compile_victim(
            funded_wallet=funded_wallet,
            price_oracle=price_oracle,
            anvil_rpc_url=anvil_rpc_url,
            oracle_guard_bps=VICTIM_ORACLE_TOL_BPS,
        )
        assert victim.status == CompilationStatus.SUCCESS, (
            f"victim compile against the CLEAN pool should pass the build-time guard: {victim.error}"
        )

        snap = _snapshot(web3)
        try:
            # In-block adverse move: attacker front-runs, displacing the pool.
            await _displace_pool(
                funded_wallet=funded_wallet,
                price_oracle=price_oracle,
                anvil_rpc_url=anvil_rpc_url,
                orchestrator=orchestrator,
            )

            usdc_post_displace = get_token_balance(web3, USDC_ADDR, funded_wallet)
            usdt_post_displace = get_token_balance(web3, USDT_ADDR, funded_wallet)

            # Victim executes against the displaced pool. The oracle-anchored floor
            # is above the sandwiched fill → on-chain revert.
            exec_result = await orchestrator.execute(victim.action_bundle)
            assert not exec_result.success, (
                "VIB-5490 FAILED: victim swap FILLED against the displaced pool — the "
                "oracle-anchored executed floor did not bite. A wide max_slippage let "
                "the sandwich through."
            )

            # Conservation: the reverted victim swap moved none of its own USDC/USDT.
            usdc_after = get_token_balance(web3, USDC_ADDR, funded_wallet)
            usdt_after = get_token_balance(web3, USDT_ADDR, funded_wallet)
            assert usdc_after == usdc_post_displace, "victim USDC changed despite reverted swap"
            assert usdt_after == usdt_post_displace, "victim USDT changed despite reverted swap"
            logger.info(
                "VIB-5490 anchor BIT: victim USDC->USDT reverted on displaced 3pool "
                "(oracle tol %d bps; post-displace USDC=%d USDT=%d, after USDC=%d USDT=%d).",
                VICTIM_ORACLE_TOL_BPS,
                usdc_post_displace,
                usdt_post_displace,
                usdc_after,
                usdt_after,
            )
        finally:
            _revert(web3, snap)

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_loose_floor_without_oracle_fills_the_sandwich(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> None:
        """Baseline: with NO oracle (pre-VIB-5490 degrade-open), the SAME wide-
        slippage victim swap after the SAME adverse move FILLS — proving the loose
        pool-self-referential floor accepted the bad fill the anchor now blocks."""
        # Victim compiled with NO oracle → the executed floor is the loose pool-
        # self-referential ``pool_quote × (1 − 10% slippage)`` only.
        victim = _compile_victim(
            funded_wallet=funded_wallet,
            price_oracle=None,
            anvil_rpc_url=anvil_rpc_url,
            oracle_guard_bps=None,
        )
        assert victim.status == CompilationStatus.SUCCESS, f"victim compile failed: {victim.error}"

        snap = _snapshot(web3)
        try:
            await _displace_pool(
                funded_wallet=funded_wallet,
                price_oracle=price_oracle,
                anvil_rpc_url=anvil_rpc_url,
                orchestrator=orchestrator,
            )
            usdc_post_displace = get_token_balance(web3, USDC_ADDR, funded_wallet)
            usdt_post_displace = get_token_balance(web3, USDT_ADDR, funded_wallet)

            exec_result = await orchestrator.execute(victim.action_bundle)
            assert exec_result.success, (
                f"baseline loose-floor victim swap unexpectedly reverted: {exec_result.error}. "
                "The displacement must be sized so the loose 10% floor still fills — "
                "otherwise the delta this test asserts is not demonstrated."
            )

            usdc_after = get_token_balance(web3, USDC_ADDR, funded_wallet)
            usdt_after = get_token_balance(web3, USDT_ADDR, funded_wallet)
            usdc_spent = usdc_post_displace - usdc_after
            usdt_received = usdt_after - usdt_post_displace
            assert usdc_spent == int(VICTIM_AMOUNT * Decimal(10**6)), "victim did not spend exactly 100 USDC"
            assert usdt_received > 0, "victim received no USDT on the (expected) fill"
            # Layer 3: receipt parse — the TokenExchange event matches the fill.
            _assert_swap_receipt_parsed(exec_result, expected_bought_wei=usdt_received)
            logger.info(
                "VIB-5490 baseline: loose-floor victim FILLED the sandwich "
                "(spent %d USDC, received %d USDT on displaced pool) — the bad fill "
                "the oracle anchor now reverts.",
                usdc_spent,
                usdt_received,
            )
        finally:
            _revert(web3, snap)
