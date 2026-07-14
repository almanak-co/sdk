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
VICTIM_UNITS = int(VICTIM_AMOUNT * Decimal(10**6))
# A tight per-intent oracle tolerance (< the 50 bps stable residual). With the
# VIB-5490 residual fix, the EXECUTED stable floor is
# ``max(pool_floor, min(oracle_fair*(1-tol), quote*(1-residual)))``; a tolerance
# below the residual makes the residual the binding floor (quote*(1-50bps)), so a
# displacement must exceed ~50 bps of drift to push the fill past it. The per-intent
# tolerance is an operator knob (VIB-5439); the DEFAULT stable tolerance is 150 bps.
VICTIM_ORACLE_TOL_BPS = 3
# Deliberately WIDE: the whole point is that a loose pool-self-referential floor is
# sandwich-exploitable; the anchor caps damage at its residual regardless.
WIDE_SLIPPAGE = Decimal("0.10")  # 10% == 1000 bps

# --- Adaptive displacement sizing (VIB-5674) ---------------------------------
# The single-sided USDC->USDT dump that front-runs the victim must drift the
# 3pool into a narrow window: ABOVE the 50 bps stable residual floor (so the
# anchored victim in test A reverts) yet BELOW the victim's 10% baseline slippage
# (so the no-oracle loose victim in test B still fills). This test forks the
# Ethereum "latest" block (NOT a pinned block), and the 3pool is being deprecated
# — its USDT reserve drains over time. A hardcoded USD dump (the original 105M)
# silently crossed the drain cliff once the pool shrank below ~105M USDT,
# over-displacing the pool so BOTH victims reverted and the delta this test proves
# vanished (VIB-5674). Instead of a magic number, we binary-search the dump that
# lands the victim's post-displacement fill inside this ratio band (fill /
# clean_quote), re-calibrated per run against the live pool depth:
#   * anchored floor bites below ~0.995 (50 bps residual) -> need ratio < 0.99
#   * loose 10% floor fills above 0.90                    -> need ratio > 0.905
# A band centred near ~0.965 (~350 bps move) clears BOTH with wide margin.
_TARGET_FILL_RATIO_LO = 0.94
_TARGET_FILL_RATIO_HI = 0.985
# Hard pass band (the actual per-test conditions); the fallback candidate must sit
# strictly inside it even if the search converges before hitting the target band.
_PASS_RATIO_LO = 0.905
_PASS_RATIO_HI = 0.99

_POOL_ADDR = Web3.to_checksum_address(POOL["address"])
_USDC_COIN_IDX = 1  # 3pool coin order [DAI, USDC, USDT]
_USDT_COIN_IDX = 2
_POOL_PROBE_ABI = [
    {
        "name": "get_dy",
        "outputs": [{"type": "uint256"}],
        "inputs": [{"type": "int128"}, {"type": "int128"}, {"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "exchange",
        "outputs": [],
        "inputs": [{"type": "int128"}, {"type": "int128"}, {"type": "uint256"}, {"type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "name": "balances",
        "outputs": [{"type": "uint256"}],
        "inputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]
_ERC20_APPROVE_ABI = [
    {
        "name": "approve",
        "outputs": [{"type": "bool"}],
        "inputs": [{"type": "address"}, {"type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


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


def _send_and_mined_ok(web3: Web3, tx: dict) -> bool:
    """Send a raw ``eth_sendTransaction`` and return True only if it both
    submitted AND mined successfully (receipt status == 1). Anvil auto-mining
    returns a tx hash even for a reverted tx, so the JSON-RPC response alone is
    not proof of success — the mined receipt status is."""
    res = web3.provider.make_request("eth_sendTransaction", [tx])
    if "error" in res or not res.get("result"):
        return False
    receipt = web3.eth.wait_for_transaction_receipt(res["result"], timeout=60)
    return receipt["status"] == 1


def _victim_fill_ratio_after_dump(
    web3: Web3, funded_wallet: str, anvil_rpc_url: str, displacement_units: int, clean_dy: int
) -> float:
    """Displace the 3pool by ``displacement_units`` USDC (raw USDC->USDT exchange
    under a nested snapshot), return the victim's displaced ``get_dy`` as a
    fraction of the clean quote, then revert so the pool is left pristine.

    Uses a raw pool ``exchange`` (not the orchestrator) purely to *measure* the
    pool response cheaply during calibration; the real displacement in
    ``_displace_pool`` still routes through the production compile+execute path.
    This test is module-level ``no_zodiac`` (funded_wallet is the EOA) and, per
    its docstring, manages its own raw displacement sequence."""
    pool = web3.eth.contract(address=_POOL_ADDR, abi=_POOL_PROBE_ABI)
    usdc = web3.eth.contract(address=USDC_ADDR, abi=_ERC20_APPROVE_ABI)
    usdc_slot = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["USDC"]
    snap = _snapshot(web3)
    try:
        fund_erc20_token(funded_wallet, USDC_ADDR, displacement_units + VICTIM_UNITS * 4, usdc_slot, anvil_rpc_url)
        web3.provider.make_request("anvil_impersonateAccount", [funded_wallet])
        web3.provider.make_request("anvil_setBalance", [funded_wallet, hex(10**19)])
        approve_data = usdc.functions.approve(_POOL_ADDR, displacement_units).build_transaction(
            {"from": funded_wallet, "nonce": 0, "gas": 200_000}
        )["data"]
        if not _send_and_mined_ok(
            web3, {"from": funded_wallet, "to": USDC_ADDR, "data": approve_data, "gas": hex(200_000)}
        ):
            return 0.0
        swap_data = pool.functions.exchange(_USDC_COIN_IDX, _USDT_COIN_IDX, displacement_units, 0).build_transaction(
            {"from": funded_wallet, "nonce": 0, "gas": 3_000_000}
        )["data"]
        # A reverted dump (pool can't fill the size) must be read as a MAXIMAL
        # over-displacement, not a no-op: under Anvil auto-mining eth_sendTransaction
        # returns a tx hash even on revert, so we must inspect the mined receipt
        # status — treating a revert as success would leave get_dy reading the CLEAN
        # (un-displaced) pool, yield a ratio ~1.0, and steer the search to a bigger
        # dump (the wrong direction).
        if not _send_and_mined_ok(
            web3, {"from": funded_wallet, "to": _POOL_ADDR, "data": swap_data, "gas": hex(3_000_000)}
        ):
            return 0.0
        displaced_dy = pool.functions.get_dy(_USDC_COIN_IDX, _USDT_COIN_IDX, VICTIM_UNITS).call()
        return displaced_dy / clean_dy
    finally:
        _revert(web3, snap)


def _calibrate_displacement_units(web3: Web3, funded_wallet: str, anvil_rpc_url: str) -> int:
    """Binary-search the USDC displacement (base units) whose post-dump victim
    fill lands inside ``[_TARGET_FILL_RATIO_LO, _TARGET_FILL_RATIO_HI]``.

    Adaptive sizing keeps this real-fork proof robust as the 3pool's USDT reserve
    drains over time on the unpinned "latest" fork (VIB-5674). ``get_dy`` is
    monotonically decreasing in the dump size, so a plain bisection converges."""
    pool = web3.eth.contract(address=_POOL_ADDR, abi=_POOL_PROBE_ABI)
    clean_dy = pool.functions.get_dy(_USDC_COIN_IDX, _USDT_COIN_IDX, VICTIM_UNITS).call()
    assert clean_dy > 0, "clean 3pool USDC->USDT quote must be positive"
    # The pool's USDT reserve caps how much USDT a USDC->USDT dump can pull, so it
    # is a natural ceiling on the displacement magnitude.
    usdt_reserve = pool.functions.balances(_USDT_COIN_IDX).call()
    target = (_TARGET_FILL_RATIO_LO + _TARGET_FILL_RATIO_HI) / 2
    lo, hi = VICTIM_UNITS, usdt_reserve
    best: int | None = None
    best_dist: float | None = None
    for _ in range(48):
        mid = (lo + hi) // 2
        ratio = _victim_fill_ratio_after_dump(web3, funded_wallet, anvil_rpc_url, mid, clean_dy)
        # Track the closest-to-target candidate that still clears the hard pass band,
        # so we never return a value that fails either test even if the search stops
        # on a boundary rather than dead-centre.
        if _PASS_RATIO_LO < ratio < _PASS_RATIO_HI:
            dist = abs(ratio - target)
            if best_dist is None or dist < best_dist:
                best, best_dist = mid, dist
        if ratio > _TARGET_FILL_RATIO_HI:
            lo = mid  # too little displacement -> need a bigger dump
        elif ratio < _TARGET_FILL_RATIO_LO:
            hi = mid  # too much displacement -> back off
        else:
            best = mid  # inside the target band -> good enough
            break
        if hi - lo <= 250_000:  # converged to 0.25 USDC precision
            break
    assert best is not None, (
        f"could not calibrate a 3pool displacement landing the victim fill in the pass band "
        f"[{_PASS_RATIO_LO}, {_PASS_RATIO_HI}] (clean_dy={clean_dy}, usdt_reserve={usdt_reserve}, lo={lo}, hi={hi})"
    )
    logger.info(
        "VIB-5674 calibrated 3pool displacement=%d USDC base-units (clean_dy=%d, usdt_reserve=%d)",
        best,
        clean_dy,
        usdt_reserve,
    )
    return best


async def _displace_pool(
    *,
    funded_wallet: str,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
    orchestrator: ExecutionOrchestrator,
    displacement_units: int,
) -> None:
    """Simulate the sandwich front-run: a large USDC -> USDT trade that moves the
    3pool against the victim's pending USDC -> USDT swap. ``displacement_units``
    is the calibrated USDC base-unit dump from ``_calibrate_displacement_units``."""
    displacement_human = Decimal(displacement_units) / Decimal(10**6)
    # Fund the attacker leg with enough USDC for the heavy displacement (default
    # seeding only gives 100k). Slot from CHAIN_CONFIGS is the single source of
    # truth for the mainnet USDC balance slot.
    usdc_slot = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["USDC"]
    fund_erc20_token(
        funded_wallet,
        USDC_ADDR,
        displacement_units + VICTIM_UNITS * 4,
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
        amount=displacement_human,
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

    @pytest.mark.xfail(
        reason=(
            "VIB-5674: this test forks Ethereum 'latest' (unpinned) and the Curve 3pool "
            "(DAI/USDC/USDT) is being deprecated / draining, so the CLEAN-pool precondition "
            "— the pre-displacement 3pool sits within the 3bps oracle-fair guard tolerance — "
            "is not market-guaranteed; when the real pool is already displaced the build-time "
            "guard correctly refuses and the 'clean pool should pass' assertion fails for a "
            "reason unrelated to what the test proves (as of 2026-07-13). "
            "strict=False because an XPASS is the correct outcome whenever the live pool "
            "happens to be within tolerance — the test genuinely passes then. The root-cause "
            "fix (pin the fork block / precondition-skip) lands on "
            "fix/curve-floor-oracle-robust-vib5674 and will remove this marker."
        ),
        strict=False,
    )
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
        # Calibrate the displacement to the live 3pool depth (VIB-5674) — runs its
        # own snapshot/revert bracket, leaving the pool CLEAN for the victim compile.
        displacement_units = _calibrate_displacement_units(web3, funded_wallet, anvil_rpc_url)

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
                displacement_units=displacement_units,
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
        # Calibrate the displacement to the live 3pool depth (VIB-5674) — same dump
        # magnitude both tests use, so the anchor-reverts vs loose-fills delta is
        # measured against one adverse move. Runs its own snapshot/revert bracket.
        displacement_units = _calibrate_displacement_units(web3, funded_wallet, anvil_rpc_url)

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
                displacement_units=displacement_units,
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
