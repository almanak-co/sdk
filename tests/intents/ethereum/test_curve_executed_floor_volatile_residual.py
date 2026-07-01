"""Real-fork proof of the VIB-5490 residual drift-buffer on a VOLATILE pool.

The first-round executed-floor anchor capped the oracle floor at the RAW pool
quote. On a volatile pool, when a swap's GENUINE price impact exceeds the anchor
tolerance (unbounded — the detection guard is skipped for volatile pools), that
raw-quote cap pins ``min_out`` to the exact clean build-time quote with ZERO
slippage buffer. A volatile pool always drifts between the block the swap is
quoted and the block it executes, so ``realized dy < get_dy`` trips Curve's
``assert dy >= min_dy`` and the swap REVERTS — stranding a legit (often
risk-reducing teardown) swap and burning gas. That is strictly worse than the
pre-anchor floor, which preserved the operator's slippage buffer.

The fix caps the oracle floor at ``pool_quote × (1 − residual)`` (a benign
inter-block-drift buffer), not the raw quote. This module proves it on a REAL
Ethereum fork of the deep Curve tricrypto2 pool (USDT/WBTC/WETH, CryptoSwap):

* (fills) a genuine >tolerance-impact USDT→WETH swap, built through the production
  path (so ``min_out = quote × (1 − 200 bps residual)``), EXECUTES and FILLS
  against a pool that has drifted between build and execution by LESS than the
  residual — where a raw-quote cap (``min_out = quote``) would have reverted. The
  assertion ``min_out ≤ realized < clean_quote`` is the load-bearing proof: the
  pool moved below the clean quote (a zero-buffer floor reverts) yet the swap
  filled (the residual absorbed the drift).
* (reverts) the anchor STILL bites — the same swap against a pool drifted by MORE
  than the residual REVERTS, so sandwich extraction beyond the residual is still
  blocked. Extraction is bounded to the residual, not the operator's wide
  slippage.

Calibration (deep tricrypto2, ~6 bps victim-drift per 1k USDT dumped): a 10k dump
≈ 60 bps (< 200 residual → fills), a 65k dump ≈ 400 bps (> 200 residual →
reverts). Margins are wide (3x / 2x) so the test tolerates fork-block depth
drift. The stable-pool adverse-sandwich bite + extraction-blocking is proven
separately in ``test_curve_executed_floor_oracle_anchor.py``.

Manages its own fork snapshot bracket + raw displacement sequence → opts out of
the default Safe+Roles wrap.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors._strategy_base.swap_oracle_guard import (
    DEFAULT_VOLATILE_ORACLE_FLOOR_RESIDUAL_BPS,
)
from almanak.connectors.curve.adapter import CURVE_POOLS, CurveAdapter, CurveConfig
from almanak.connectors.curve.receipt_parser import CurveEventType, CurveReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler
from almanak.framework.intents.compiler import CompilationStatus
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    _calculate_mapping_slot,
    _retry_rpc_call,
    fund_erc20_token,
    get_token_balance,
    make_intent_test_web3,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_zodiac(
    reason="VIB-5490: manages its own evm_snapshot/revert bracket + raw drift-displacement "
    "sequence on a volatile pool to prove the residual buffer; not a permission-manifest test."
)

CHAIN_NAME = "ethereum"
POOL = CURVE_POOLS["ethereum"]["tricrypto2"]
POOL_ADDRESS = POOL["address"]
USDT_ADDR = Web3.to_checksum_address(POOL["coin_addresses"][0])
WETH_ADDR = Web3.to_checksum_address(POOL["coin_addresses"][2])

# A genuine large WETH position exit: 500k USDT -> WETH on tricrypto2. Its impact
# far exceeds the 500 bps volatile tolerance, so the residual cap is what bounds
# the executed floor (min_out = quote * (1 - 200 bps)).
VICTIM_AMOUNT = Decimal("500000")
# Wider than the 200 bps residual so the residual (not the operator floor) is the
# binding constraint — i.e. the sandwich-exploitable window this anchor closes.
VICTIM_SLIPPAGE = Decimal("0.10")  # 1000 bps

# Drift injected between the victim's build and its execution, as a single USDT->WETH
# dump. Calibrated on the pinned fork (~6 bps victim-drift per 1k USDT):
BENIGN_DUMP = Decimal("10000")  # ≈ 60 bps drift  (< 200 residual → victim fills)
ADVERSE_DUMP = Decimal("65000")  # ≈ 400 bps drift (> 200 residual → victim reverts)

_USDT_FUNDING = Decimal("80000000")  # 80M USDT: covers victim + the largest dump


def _snapshot(web3: Web3) -> str:
    return web3.provider.make_request("evm_snapshot", [])["result"]


def _revert(web3: Web3, snap: str) -> None:
    web3.provider.make_request("evm_revert", [snap])


def _assert_swap_receipt_parsed(exec_result, *, expected_bought_wei: int) -> None:
    """Layer 3 (receipt parse): the swap receipt carries a Curve TokenExchange
    event (tricrypto emits TokenExchangeCrypto, normalised to TOKEN_EXCHANGE) whose
    ``tokens_bought`` is positive and matches the observed WETH balance delta."""
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
        f"parsed tokens_bought {bought} must equal the observed WETH delta {expected_bought_wei}"
    )


# USDT (TetherToken) allowances live in the ``allowed`` mapping at storage slot 5.
_USDT_ALLOWED_SLOT = 5
_MAX_UINT256_HEX = "0x" + "f" * 64


def _fund_usdt(funded_wallet: str, anvil_rpc_url: str) -> None:
    fund_erc20_token(
        funded_wallet,
        USDT_ADDR,
        int(_USDT_FUNDING * Decimal(10**6)),
        CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["USDT"],
        anvil_rpc_url,
    )


def _preapprove_usdt_to_pool(funded_wallet: str, anvil_rpc_url: str) -> None:
    """Seed a MAX USDT->pool allowance directly in storage.

    Test-only: this wallet plays BOTH the attacker (dump) and the victim, which a
    production sandwich never does — they are distinct wallets with independent
    allowances. Sharing one wallet means a victim swap built when allowance==0
    carries a bare ``approve(MAX)`` that then reverts on USDT's non-zero → non-zero
    rule once the dump has already set the allowance. Pre-seeding MAX in storage
    (no tx, no nonce coupling with the orchestrator) makes every swap build with a
    sufficient allowance, so no bundle carries an approve and the sequence isolates
    the min-out behaviour under test — not the USDT approve quirk.
    """
    inner = _calculate_mapping_slot(funded_wallet, _USDT_ALLOWED_SLOT)
    outer = _calculate_mapping_slot(POOL_ADDRESS, int(inner, 16))
    w3 = make_intent_test_web3(anvil_rpc_url)
    _retry_rpc_call(w3, "anvil_setStorageAt", [USDT_ADDR, outer, _MAX_UINT256_HEX])
    _retry_rpc_call(w3, "evm_mine", [])


def _price_ratio(price_oracle: dict[str, Decimal]) -> Decimal:
    return price_oracle["USDT"] / price_oracle["WETH"]


def _build_victim(
    *,
    funded_wallet: str,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
):
    """Compile the high-impact victim USDT->WETH swap through the PRODUCTION path
    (default volatile tolerance 500 bps + 200 bps residual)."""
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    intent = SwapIntent(
        from_token="USDT",
        to_token="WETH",
        amount=VICTIM_AMOUNT,
        max_slippage=VICTIM_SLIPPAGE,
        protocol="curve",
        chain=CHAIN_NAME,
    )
    return compiler.compile(intent)


def _victim_floor_numbers(
    *,
    funded_wallet: str,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> tuple[int, int]:
    """Read the production (min_out, clean_quote) for the victim via the SAME
    adapter path the compiler uses — clean pool, so the numbers are the build-time
    floor the bundle carries."""
    adapter = CurveAdapter(
        CurveConfig(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            default_slippage_bps=int(VICTIM_SLIPPAGE * Decimal(10_000)),
            rpc_url=anvil_rpc_url,
        )
    )
    res = adapter.swap(
        pool_address=POOL_ADDRESS,
        token_in="USDT",
        token_out="WETH",
        amount_in=VICTIM_AMOUNT,
        slippage_bps=int(VICTIM_SLIPPAGE * Decimal(10_000)),
        price_ratio=_price_ratio(price_oracle),
    )
    assert res.success, f"victim floor read failed: {res.error}"
    return res.amount_out_minimum, res.amount_out_estimate


async def _dump(
    *,
    amount: Decimal,
    funded_wallet: str,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
    orchestrator: ExecutionOrchestrator,
) -> None:
    """Execute a single USDT->WETH dump to drift the pool against the victim."""
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    intent = SwapIntent(
        from_token="USDT",
        to_token="WETH",
        amount=amount,
        max_slippage=Decimal("0.60"),
        protocol="curve",
        chain=CHAIN_NAME,
        # The attacker/other-flow does not care about its own floor.
        swap_params={"oracle_guard_bps": 9000},
    )
    result = compiler.compile(intent)
    assert result.status == CompilationStatus.SUCCESS, f"dump compile failed: {result.error}"
    exec_result = await orchestrator.execute(result.action_bundle)
    assert exec_result.success, f"dump failed to land: {exec_result.error}"


@pytest.mark.ethereum
@pytest.mark.swap
class TestVolatileResidualDriftBuffer:
    """VIB-5490 residual fix: a high-impact volatile swap keeps a benign-drift
    buffer (fills under sub-residual drift) yet the anchor still bites beyond it."""

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_high_impact_volatile_fills_under_benign_drift(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> None:
        """A >tolerance-impact volatile swap FILLS against a pool drifted by less
        than the residual — where the old raw-quote cap (min_out == quote) would
        have reverted."""
        snap = _snapshot(web3)
        try:
            _fund_usdt(funded_wallet, anvil_rpc_url)
            _preapprove_usdt_to_pool(funded_wallet, anvil_rpc_url)

            # Production floor numbers on the CLEAN pool.
            min_out, clean_quote = _victim_floor_numbers(
                funded_wallet=funded_wallet,
                price_oracle=price_oracle,
                anvil_rpc_url=anvil_rpc_url,
            )
            # The load-bearing property of the FIX: the executed floor carries a
            # residual buffer BELOW the clean quote — it is NOT pinned to the raw
            # quote (the zero-buffer bug). ~200 bps below, within a rounding band.
            assert min_out < clean_quote, "executed floor was pinned to the raw quote (zero buffer) — the bug"
            expected = clean_quote * (10_000 - DEFAULT_VOLATILE_ORACLE_FLOOR_RESIDUAL_BPS) // 10_000
            assert abs(min_out - expected) <= max(1, clean_quote // 10_000), (
                f"executed floor {min_out} is not ~quote*(1-200bps) {expected}"
            )

            # Build the victim against the CLEAN pool (bundle carries the build-time
            # min_out), then drift the pool by LESS than the residual, then execute.
            victim = _build_victim(
                funded_wallet=funded_wallet,
                price_oracle=price_oracle,
                anvil_rpc_url=anvil_rpc_url,
            )
            assert victim.status == CompilationStatus.SUCCESS, f"victim compile failed: {victim.error}"

            await _dump(
                amount=BENIGN_DUMP,
                funded_wallet=funded_wallet,
                price_oracle=price_oracle,
                anvil_rpc_url=anvil_rpc_url,
                orchestrator=orchestrator,
            )

            weth_before = get_token_balance(web3, WETH_ADDR, funded_wallet)
            exec_result = await orchestrator.execute(victim.action_bundle)
            assert exec_result.success, (
                f"VIB-5490 FAILED: high-impact volatile swap REVERTED under benign (<residual) drift — "
                f"the residual buffer did not absorb it: {exec_result.error}"
            )
            weth_after = get_token_balance(web3, WETH_ADDR, funded_wallet)
            realized = weth_after - weth_before

            # The proof: the pool DID drift below the clean quote (so a zero-buffer
            # floor == clean_quote would have reverted), yet realized cleared the
            # residual-buffered min_out and the swap filled.
            assert realized < clean_quote, (
                "no drift occurred — benign dump must move the pool below the clean quote so that a "
                "zero-buffer floor would have reverted (else the test proves nothing)"
            )
            assert realized >= min_out, "realized below the executed floor but the swap did not revert (impossible)"
            # Layer 3: receipt parse — the TokenExchangeCrypto event matches the fill.
            _assert_swap_receipt_parsed(exec_result, expected_bought_wei=realized)
            logger.info(
                "VIB-5490 residual FILLS: high-impact volatile USDT->WETH filled under benign drift "
                "(clean_quote=%d, min_out=%d [~200bps buffer], realized=%d).",
                clean_quote,
                min_out,
                realized,
            )
        finally:
            _revert(web3, snap)

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_high_impact_volatile_reverts_under_adverse_drift(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> None:
        """The anchor STILL bites: the same swap against a pool drifted by MORE than
        the residual REVERTS — sandwich extraction beyond the residual is blocked."""
        snap = _snapshot(web3)
        try:
            _fund_usdt(funded_wallet, anvil_rpc_url)
            _preapprove_usdt_to_pool(funded_wallet, anvil_rpc_url)

            victim = _build_victim(
                funded_wallet=funded_wallet,
                price_oracle=price_oracle,
                anvil_rpc_url=anvil_rpc_url,
            )
            assert victim.status == CompilationStatus.SUCCESS, f"victim compile failed: {victim.error}"

            await _dump(
                amount=ADVERSE_DUMP,
                funded_wallet=funded_wallet,
                price_oracle=price_oracle,
                anvil_rpc_url=anvil_rpc_url,
                orchestrator=orchestrator,
            )

            usdt_before = get_token_balance(web3, USDT_ADDR, funded_wallet)
            weth_before = get_token_balance(web3, WETH_ADDR, funded_wallet)
            exec_result = await orchestrator.execute(victim.action_bundle)
            assert not exec_result.success, (
                "VIB-5490 FAILED: victim FILLED against a pool drifted beyond the residual — the anchor "
                "did not bite; sandwich extraction beyond the residual is NOT bounded."
            )
            # Value conserved by the reverted swap.
            assert get_token_balance(web3, USDT_ADDR, funded_wallet) == usdt_before, "victim USDT moved on revert"
            assert get_token_balance(web3, WETH_ADDR, funded_wallet) == weth_before, "victim WETH moved on revert"
            logger.info(
                "VIB-5490 anchor BITES on volatile: high-impact USDT->WETH reverted under adverse "
                "(>residual) drift — extraction bounded to the residual."
            )
        finally:
            _revert(web3, snap)
