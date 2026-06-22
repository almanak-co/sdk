"""4-layer intent tests for Uniswap V4 LP_COLLECT_FEES on Polygon Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
collecting fees from V4 LP positions via PositionManager on Polygon:
1. Open a MATIC/USDC LP position (LP_OPEN as setup -- ``MATIC`` symbol so
   currency0 resolves to ``address(0)``, matching the V4 swap router's
   WMATIC -> native MATIC remapping)
2. Generate fees by counter-swapping (USDC -> MATIC) through the SAME
   native-MATIC pool
3. Create CollectFeesIntent with position_id and protocol_params
4. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
5. Execute via ExecutionOrchestrator (full production pipeline)
6. Parse receipts -- fees (Transfer from PoolManager for USDC, native
   delta for MATIC) separate from principal (ModifyLiquidity delta == 0)
7. Verify position liquidity is unchanged on-chain after collection
8. Verify at least one side of the position accrued strictly positive
   fees (bilateral no-op guard)

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

Pool selection: ``MATIC/USDC/3000``. Polygon V4 has an ERC20-keyed
``WETH/USDC/3000`` pool used by VIB-4363 LP_OPEN, but
``UniswapV4SDK.build_swap_tx`` currently reverts on-chain for ERC20<>ERC20
V4 swaps on Polygon via UniversalRouter (VIB-4413 -- see
``tests/intents/polygon/test_uniswap_v4_swap.py`` xfail markers). To
generate fees end-to-end we need a swap path that DOES work, which is
the native-keyed pool: ``UniswapV4SDK.build_swap_tx`` detects WMATIC and
remaps it to native MATIC (address(0)) at the pool layer (exactly like
WETH -> NATIVE_ETH on Base / Optimism), so a swap with ``"MATIC"``
symbols routes through the same ``(NATIVE_MATIC, USDC, fee=3000, ts=60,
hooks=0x0)`` pool key.

The native MATIC/USDC pool at fee=3000 / ts=60 was verified initialized
on Polygon mainnet 2026-05-14 via direct StateView.getSlot0(bytes32)
against PoolManager 0x67366782... with sqrtPriceX96 ~ 2.47e22
(tick=-299649, corresponding to ~$0.0974 USDC/MATIC; math:
raw_ratio=(2.47e22/2**96)**2≈9.74e-14, human=9.74e-14*1e12≈0.0974)
and liquidity ~ 2.09e18 -- ample depth for the small amounts the test
uses. Native
MATIC/USDC pools at fee=500 / ts=10 (liq ~ 1.07e17) and fee=10000 /
ts=200 (liq ~ 1.03e15) are also initialized; the 3000 tier is selected
to match the Base / Optimism sibling pattern (tick spacing 60).

Counter-swap direction: USDC -> MATIC (single leg, not a round-trip
like Base / Optimism). The SDK's WMATIC -> native MATIC remap means
the wallet would need WMATIC for a MATIC -> USDC return leg, but the
polygon conftest only funds native MATIC (not WMATIC). USDC -> MATIC
alone generates USDC-side fees -- the input token is what the LP
accrues fees in, so the COLLECT_FEES surfaces a USDC PoolManager
Transfer that the parser picks up.

The bilateral fee assertion uses OR across the two sides (MATIC fee OR
USDC fee strictly positive). This guards against the V4 no-op bug class
(tx succeeds but 0 tokens move) while accommodating the single-direction
counter-swap above -- USDC fees alone are sufficient evidence of
fee-accrual.

To run:
    uv run pytest tests/intents/polygon/test_uniswap_v4_collect_fees.py -v -s
"""

import json
from collections.abc import Generator
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.connectors.uniswap_v4.sdk import NATIVE_CURRENCY, UniswapV4SDK
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType, LPOpenIntent
from almanak.gateway.utils.rpc_provider import get_rpc_url
from tests.conftest_gateway import AnvilFixture
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_accounting_persisted,
    assert_no_accounting_on_failure,
    format_token_amount,
    fund_native_token,
    get_token_balance,
    get_token_decimals,
    seed_wallet_state_with_recovery,
)
from tests.intents.polygon.conftest import _seed_wallet_state

# VIB-4483: native-keyed V4 pool (currency0=0x0) — its modifyLiquidities calldata
# shape isn't in the ERC20-derived Zodiac synthetic-discovery manifest, so every
# tx fails execTransactionWithRole authz. Native-pool matrix discovery is a
# separate Zodiac follow-up (VIB-4421 family); opt out so these tests validate the
# real 4-layer + Layer-5 accounting path on the EOA.
pytestmark = pytest.mark.no_zodiac(
    reason="VIB-4483: native-keyed V4 pool (currency0=0x0) selector set is not in "
    "the ERC20-derived Zodiac manifest; native-pool matrix discovery is a separate "
    "Zodiac follow-up (VIB-4421 family)."
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "polygon"

# VIB-4427: this test proves same-pool V4 fee accrual by opening an LP position,
# moving the pool with a counter-swap, then collecting fees. Pool depth and
# Polygon gas price drift enough across CI's weekly fork pin that the fixed LP /
# counter-swap sizing can oscillate between passing, slippage aborts, and
# insufficient-gas funding failures. Keep this module on the fork block used to
# calibrate COUNTER_SWAP_USDC / SWAP_MAX_SLIPPAGE / gas headroom instead of the
# workflow's rolling cache pin.
POLYGON_V4_COLLECT_FEES_FORK_BLOCK = 88430000


@pytest.fixture(scope="module")
def anvil_polygon() -> Generator[AnvilFixture, None, None]:
    """Run this module against the calibrated Polygon V4 collect-fees fork."""
    try:
        fork_rpc_url = get_rpc_url(CHAIN_NAME, network="mainnet")
    except ValueError as exc:
        pytest.skip(f"Cannot start pinned Polygon Anvil fork: {exc}")
        return

    anvil = AnvilFixture(
        chain=CHAIN_NAME,
        fork_rpc_url=fork_rpc_url,
        fork_block_number=POLYGON_V4_COLLECT_FEES_FORK_BLOCK,
    )
    try:
        anvil.start()
    except RuntimeError as exc:
        pytest.skip(f"Failed to start pinned Polygon Anvil fork: {exc}")
        return

    try:
        yield anvil
    finally:
        anvil.stop()


@pytest.fixture(scope="module")
def _eoa_funded_wallet(web3, anvil_rpc_url: str, anvil_instance: AnvilFixture) -> str:
    """Seed the wallet on this module's dedicated pinned fork WITHOUT the shared
    cross-module pristine reset.

    Overrides the Polygon conftest's ``_eoa_funded_wallet``. That fixture calls
    ``reset_fork_to_pristine(web3)``, whose ``_session_pristine`` cache is keyed by
    ``chain_id`` only. This module runs its own module-scoped ``anvil_polygon``
    fork (pinned to ``POLYGON_V4_COLLECT_FEES_FORK_BLOCK``) alongside the
    session-scoped Polygon fork the other ~18 polygon modules use. With both forks
    sharing ``_session_pristine[137]`` in the single ``pytest tests/intents/polygon/
    -n0`` process, the shared reset would try to ``evm_revert`` a snapshot captured
    on the *session* fork against this *separate* fork — returning ``False``,
    raising under ``strict=True``, and corrupting the session fork's pristine entry
    for sibling modules (e.g. ``test_uniswap_v4_lp_close``).

    A dedicated fork is fresh at module start and torn down at module end, so
    cross-module pristine reversion is unnecessary here — seed directly. (PR #2963)
    """
    return seed_wallet_state_with_recovery(
        seed_wallet_state=_seed_wallet_state,
        web3=web3,
        rpc_url=anvil_rpc_url,
        anvil_instance=anvil_instance,
        chain_name=CHAIN_NAME,
    )


# MATIC/USDC pool with 0.3% fee tier. V4 pools support native MATIC as
# currency0 (address(0)); ``UniswapV4Adapter._resolve_token(for_v4_pool=True)``
# returns ``NATIVE_CURRENCY`` for the ``MATIC`` symbol (it is in the
# adapter's ``native_symbols = {"ETH", "AVAX", "MATIC", "BNB"}`` set), and
# ``UniswapV4SDK.build_swap_tx`` substitutes WMATIC -> native MATIC for the
# V4 pool key (see ``sdk.py:_is_wrapped_native`` handling). Opening the LP
# with ``MATIC/USDC/3000`` resolves currency0 to ``address(0)``, so the LP
# position and the counter-swap end up on the SAME pool key and the
# position can actually accrue fees. Using ``WMATIC/USDC/3000`` here would
# open a position on a separate ERC20-keyed pool that the V4 swap path
# can't reach (VIB-4413 ERC20<>ERC20 revert).
#
# Why MATIC, not POL or WPOL: the V4 adapter's static ``native_symbols``
# set explicitly contains ``"MATIC"``; ``"POL"`` and ``"WPOL"`` fall
# through to ``resolve_for_swap`` which auto-wraps to WMATIC and yields a
# non-native pool key. ``"MATIC"`` is the only symbol that produces the
# native-keyed pool the test needs.
LP_POOL = "MATIC/USDC/3000"

# Token symbols for the fee-generation counter-swap. ``MATIC`` symbol in
# the SwapIntent path resolves to WMATIC via ``resolve_for_swap``, then
# ``UniswapV4SDK.build_swap_tx`` detects WMATIC as the wrapped native and
# substitutes native MATIC at the pool layer -- matching the LP pool key.
# Wallet balance checks below read native balance for the MATIC side.
SWAP_TOKEN0_SYMBOL = "MATIC"
SWAP_TOKEN1_SYMBOL = "USDC"

# LP amounts for setup. The polygon conftest funds the EOA with native
# MATIC (anvil default ~10000 MATIC) and 100,000 USDC; we deposit
# meaningful slices so the position captures a non-trivial share of
# the counter-swap's fees. The native MATIC/USDC pool at fork time has
# liquidity ~2.09e18, so the position needs enough capital to register
# against the very large pool.
LP_AMOUNT_MATIC = Decimal("500")
LP_AMOUNT_USDC = Decimal("500")
# Narrow tick range that brackets the current pool price (~$0.0974
# USDC/MATIC at the polygon mainnet fork-block time -- on-chain tick
# ~-299649). The range must include the spot price so the position is
# in-range and accrues fees from the counter-swap. A wider range
# (e.g. 0.01 - 100) would shrink the position's effective share at the
# spot tick to below the precision of the fee accrual integers, and the
# parser would see zero fees collected.
LP_RANGE_LOWER = Decimal("0.05")
LP_RANGE_UPPER = Decimal("0.20")

# Counter-swap amount -- a single USDC -> MATIC swap through the same pool
# key as LP_OPEN forces the LP position to accrue USDC-side fees. We
# intentionally do only ONE direction (USDC -> MATIC) so the LP accrues
# fees in USDC (the input token of the swap), which surface as a
# PoolManager -> wallet Transfer event during COLLECT_FEES. The reverse
# leg (MATIC -> USDC) is skipped because (1) it would also work via the
# native-key pool but (2) the test wallet doesn't hold WMATIC and the
# UR/Permit2 path expects wrapped-native; and (3) we already have a
# verifiable fee-accrual signal via the USDC leg alone.
#
# Sizing (VIB-4483, re-probed 2026-06-13 at fork block 88,430,000): the
# native MATIC/USDC fee=3000 pool carries liquidity ~1.07e17 at tick
# ~-302226 (~$0.075 USDC/MATIC). That is ~20x thinner than the ~2.09e18
# the original (2026-05-14) probe recorded, so this module pins the fork
# block above. A 2,000 USDC counter-swap exceeds the 0.10 slippage tolerance
# against this depth and reverts with the V4 router's bare slippage abort
# ("Invalid revert data (too short): 0x"). 200 USDC (~2,670 MATIC out)
# stays comfortably inside slippage at the pinned depth while still charging
# ~0.6 USDC of pool-wide fees, of which our in-range position captures a
# wei-positive USDC Transfer at COLLECT_FEES.
COUNTER_SWAP_USDC = Decimal("200")
SWAP_MAX_SLIPPAGE = Decimal("0.10")
NATIVE_MATIC_GAS_HEADROOM = Decimal("25")
POST_LP_OPEN_NATIVE_TOP_UP_MATIC = Decimal("100")


# =============================================================================
# Helpers
# =============================================================================


def _derive_matic_price_from_slot0(anvil_rpc_url: str) -> Decimal:
    """Derive MATIC/USD price from the V4 pool's sqrtPriceX96 at fork time.

    Reads the on-chain sqrtPriceX96 from the Polygon V4 StateView for
    the ``(NATIVE_MATIC, USDC, 3000, 60, 0x0)`` pool.  The fork block is
    pinned, so this value is constant across CI runs and immune to live-price
    drift — eliminating the VIB-4427 flake.

    Conversion: ``price_usdc_per_matic = (sqrtPriceX96 / 2**96)**2
                 * 10**(matic_decimals - usdc_decimals)``
    (18 - 6 = 12, USDC is currency1 / token1).

    Falls back to a hard-coded fork-block snapshot price (~$0.0974 USDC/MATIC,
    math-consistent with sqrtPriceX96 ~ 2.47e22 verified 2026-05-14 via
    direct StateView.getSlot0 query) if the StateView call reverts or raises.
    """
    tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
    usdc_addr = tokens["USDC"]

    sdk = UniswapV4SDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
    pool_key = sdk.compute_pool_key(
        token0=NATIVE_CURRENCY,  # address(0) for native MATIC (currency0)
        token1=usdc_addr,
        fee=3000,
        tick_spacing=60,
        hooks=NATIVE_CURRENCY,
    )

    try:
        sqrt_price = sdk.get_pool_sqrt_price(pool_key, rpc_url=anvil_rpc_url)
    except Exception:  # noqa: BLE001 — fall through to fork-block snapshot on any RPC/decode failure
        sqrt_price = None
    if sqrt_price is None or sqrt_price == 0:
        # Hard-coded fork-block snapshot (VIB-4427): ~$0.0974 USDC/MATIC
        # Math: sqrtPriceX96~2.47e22 verified 2026-05-14 via StateView.getSlot0
        # (tick=-299649). raw_ratio=(2.47e22/2**96)**2≈9.74e-14; human price
        # =9.74e-14*10**(18-6)≈0.0974 USDC/MATIC.
        return Decimal("0.0974")

    # sqrtPriceX96 represents sqrt(token1/token0) in Q96 fixed-point.
    # token0 = native MATIC (18 dec), token1 = USDC (6 dec).
    # raw_ratio = (sqrtPriceX96 / 2**96)**2  ->  USDC_raw_units / MATIC_raw_units
    # human price (USDC per MATIC) = raw_ratio * 10**(18 - 6)
    raw_ratio = (Decimal(sqrt_price) / Decimal(2**96)) ** 2
    matic_price = raw_ratio * Decimal(10 ** (18 - 6))
    return matic_price


def _build_price_oracle_with_native(
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> dict[str, Decimal]:
    """Return price oracle augmented with MATIC / POL / WMATIC prices.

    Derives the MATIC price from the pinned fork-block sqrtPriceX96
    (VIB-4427) instead of a live CoinGecko fetch to eliminate the
    live-oracle ↔ fork-block coupling that causes the flaky
    V4TooLittleReceived revert.
    """
    matic_price = _derive_matic_price_from_slot0(anvil_rpc_url)
    return {
        **price_oracle,
        "MATIC": matic_price,
        "POL": matic_price,
        "WMATIC": matic_price,
    }


async def _open_v4_position(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> tuple[int, int, str, str]:
    """Open a V4 LP position and return ``(position_id, liquidity, currency0, currency1)``.

    Raises ``AssertionError`` if the setup LP_OPEN fails or yields no
    position id / no positive liquidity.
    """
    intent = LPOpenIntent(
        pool=LP_POOL,
        amount0=LP_AMOUNT_MATIC,
        amount1=LP_AMOUNT_USDC,
        range_lower=LP_RANGE_LOWER,
        range_upper=LP_RANGE_UPPER,
        protocol="uniswap_v4",
        chain=CHAIN_NAME,
        # VIB-2180/VIB-2701: V4 StateView.getSlot0 reverts on the Anvil fork -> estimated price; opt in.
        protocol_params={"allow_estimated_price": True},
    )

    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
    )

    compilation_result = compiler.compile(intent)
    assert compilation_result.status.value == "SUCCESS", (
        f"Setup LP_OPEN compilation failed: {compilation_result.error}"
    )
    bundle = compilation_result.action_bundle
    assert bundle is not None

    execution_result = await orchestrator.execute(bundle)
    assert execution_result.success, f"Setup LP_OPEN execution failed: {execution_result.error}"

    # Extract position_id and liquidity from receipt.
    # Iterate until both are found, then stop -- avoids spamming the
    # "no position ID found" parser warning for approval txs.
    parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
    position_id: int | None = None
    liquidity: int | None = None

    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            receipt_dict = tx_result.receipt.to_dict()
            if position_id is None:
                position_id = parser.extract_position_id(receipt_dict)
            if liquidity is None:
                liquidity = parser.extract_liquidity(receipt_dict)
        if position_id is not None and liquidity is not None:
            break

    assert position_id is not None, "Setup LP_OPEN must yield a position_id"
    assert liquidity is not None and liquidity > 0, (
        "Setup LP_OPEN must yield positive liquidity"
    )

    # Get currency addresses from bundle metadata
    token0 = bundle.metadata.get("token0", {})
    token1 = bundle.metadata.get("token1", {})
    currency0 = token0.get("address", "")
    currency1 = token1.get("address", "")

    assert currency0 and currency1, (
        "Must extract currency addresses from bundle metadata"
    )

    return position_id, liquidity, currency0, currency1


async def _counter_swap_to_generate_fees(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> bool:
    """Run a USDC -> MATIC swap through the V4 connector to accrue fees.

    Single direction USDC -> MATIC SwapIntent routed via the V4
    UniversalRouter. The SDK routes the swap through the
    ``(NATIVE_MATIC, USDC, 3000, 60, 0x0)`` pool key (V4's swap path
    always remaps WMATIC -> native MATIC), which is the SAME pool key
    LP_OPEN used. The position then accrues fees in USDC (the input
    token of the swap), which surface as a PoolManager -> wallet
    Transfer event during the subsequent COLLECT_FEES.

    Single-direction (not round-trip) because the wallet only holds
    USDC and native MATIC, not WMATIC; the SDK's WMATIC -> native
    swap path expects WMATIC in the wallet via Permit2. Doing only
    the USDC-in side avoids that constraint and still produces a
    verifiable fee-accrual signal.

    Returns ``True`` if the swap compiled AND executed successfully,
    ``False`` otherwise.
    """
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
    )

    swap_intent = SwapIntent(
        from_token=SWAP_TOKEN1_SYMBOL,
        to_token=SWAP_TOKEN0_SYMBOL,
        amount=COUNTER_SWAP_USDC,
        max_slippage=SWAP_MAX_SLIPPAGE,
        protocol="uniswap_v4",
        chain=CHAIN_NAME,
    )
    swap_compilation = compiler.compile(swap_intent)
    if swap_compilation.status.value != "SUCCESS" or swap_compilation.action_bundle is None:
        print(f"Counter-swap compilation failed: {swap_compilation.error}")
        return False
    swap_result = await orchestrator.execute(swap_compilation.action_bundle)
    if not swap_result.success:
        wallet_balance = web3.eth.get_balance(funded_wallet)
        print(
            "Counter-swap execution failed: "
            f"error={swap_result.error!r}, wallet_native_balance={wallet_balance}"
        )
        # Compiler-stage transactions only carry to/value/gas_estimate; gas-price
        # and fee fields are populated later by the orchestrator, so they would
        # always print None here. Use safe access over the fields that exist.
        for i, tx in enumerate(swap_compilation.action_bundle.transactions):
            if isinstance(tx, dict):
                tx_to = tx.get("to")
                tx_value = tx.get("value")
                tx_gas_estimate = tx.get("gas_estimate") or tx.get("gas") or tx.get("gas_limit")
            else:
                tx_to = getattr(tx, "to", None)
                tx_value = getattr(tx, "value", None)
                tx_gas_estimate = getattr(tx, "gas_estimate", None) or getattr(tx, "gas_limit", None)
            print(
                f"  counter-swap tx[{i}]: to={tx_to} value={tx_value} gas_estimate={tx_gas_estimate}"
            )
    return swap_result.success


# =============================================================================
# Layer-5 accounting helpers (mirrors tests/intents/ethereum/test_uniswap_v3_lp.py;
# V4-specific position_hash directional contract per epic VIB-4591 / VIB-4594)
# =============================================================================


def _execution_context(wallet: str) -> ExecutionContext:
    return ExecutionContext(
        deployment_id="layer5-uniswap-v4-lp",
        chain=CHAIN_NAME,
        wallet_address=wallet,
        protocol="uniswap_v4",
    )


def _enrich_for_accounting(execution_result, intent, wallet: str, bundle_metadata: dict | None = None):
    return enrich_result(
        execution_result,
        intent,
        _execution_context(wallet),
        live_mode=False,
        bundle_metadata=bundle_metadata,
    )


def _payload(row: dict) -> dict:
    return json.loads(row["payload_json"])


def _to_human(raw: int | None, decimals: int) -> Decimal | None:
    if raw is None:
        return None
    return Decimal(int(raw)) / Decimal(10**decimals)


def _assert_identity(row: dict, *, event_type: str, wallet: str) -> None:
    assert row["deployment_id"] == "layer5-intent-test"
    assert row["cycle_id"] == "layer5-cycle"
    assert row["execution_mode"] == "paper"
    assert row["event_type"] == event_type
    assert row["tx_hash"], "accounting row must link to an on-chain tx_hash"
    assert row["ledger_entry_id"], "accounting row must link to transaction_ledger"
    assert row["wallet_address"].lower() == wallet.lower()
    # Identity sextuple has no agent_id (Morpho precedent VIB-4604).
    assert "agent_id" not in row


def _assert_no_lot_id(row: dict, payload: dict) -> None:
    assert "lot_id" not in row
    assert "lot_id" not in payload


def _assert_v4_close_position_hash(payload: dict) -> None:
    """V4 LP_CLOSE / LP_COLLECT_FEES leave ``position_hash`` ``None``.

    The close leg matches against the prior OPEN payload by ``position_key``
    (not by re-reading the hash off the burn receipt), so the handler
    forwards ``position_hash=None`` for the close-like events even on V4.
    See ``lp_accounting.py`` VIB-4473 comment.
    """
    assert payload["position_hash"] is None, (
        "V4 LP_CLOSE/LP_COLLECT_FEES match by position_key; position_hash "
        "must stay None (not re-read off the burn receipt)"
    )


def _payload_fee(raw) -> Decimal | None:
    """Decode a persisted ``fees*_collected`` cell honoring Empty≠Zero≠None.

    ``None`` = unmeasured (the parser did not separately measure fees).
    ``""`` = the parser did not emit the field. Both stay ``None`` here so
    the caller can apply the directional null-contract; any concrete value
    (``"0"`` measured-zero or a positive amount) becomes a ``Decimal``.
    """
    if raw is None or raw == "":
        return None
    return Decimal(raw)


def _assert_fee_contract(payload_raw, parser_human: Decimal | None, *, field: str) -> None:
    """Directional null-contract for a single ``fees*_collected`` leg.

    Per epic VIB-4591 decision #5 / docs/internal/blueprints/27 Empty≠Zero≠None. The V4
    receipt parser sets ``LPCloseData.fees0/fees1 = None`` (Empty): V4
    bundles fees into the withdrawal Transfer, fee separation is V1 work
    (VIB-4482). The LP handler correctly persists an unmeasured ``None``
    (it does NOT fabricate a measured-zero):

    * parser reading is concrete  -> payload MUST equal it exactly.
    * parser reading is ``None`` (Empty) -> payload may be ``None``
      (unmeasured) or measured-zero ``Decimal('0')``; it must NEVER
      fabricate a non-zero fee.
    """
    payload_fee = _payload_fee(payload_raw)
    if parser_human is not None:
        assert payload_fee == parser_human, (
            f"{field}: payload {payload_fee!r} must equal parser reading {parser_human!r}"
        )
        return
    assert payload_fee is None or payload_fee == Decimal("0"), (
        f"{field}: parser did not measure fees (Empty); payload must be unmeasured "
        f"(None) or measured-zero (0), never a fabricated {payload_fee!r}"
    )


# =============================================================================
# CollectFeesIntent Tests -- Uniswap V4 on Polygon
# =============================================================================


@pytest.mark.polygon
@pytest.mark.lp
class TestUniswapV4CollectFeesIntent:
    """Test Uniswap V4 LP_COLLECT_FEES using CollectFeesIntent on Polygon.

    These tests verify the fee collection flow:
    - First open a position (setup)
    - Generate trading fees via counter-swap through the same pool key
    - CollectFeesIntent creation with protocol_params
    - UniswapV4Compiler compiles ``LP_COLLECT_FEES``
    - Transactions execute successfully on-chain via PositionManager
    - Position liquidity is unchanged after fee collection (fees-only)
    - Wallet gains ONLY the fee amounts; principal stays locked in the pool
    """

    @pytest.mark.intent(
        IntentType.LP_OPEN, IntentType.SWAP, IntentType.LP_COLLECT_FEES
    )
    @pytest.mark.asyncio
    async def test_collect_fees_matic_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Collect fees from a MATIC/USDC LP position via V4 on Polygon.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle.
        2. Execution: ExecutionOrchestrator -> success.
        3. Receipt Parsing: ModifyLiquidity event with ``liquidity_delta == 0``
           (fees-only path; principal is NOT removed). USDC fee amount
           surfaces as a PoolManager -> wallet Transfer; native MATIC fee
           is the native balance delta (TAKE flows native MATIC directly
           with no Transfer event).
        4. Balance Deltas: Wallet gains ONLY the fee amounts (USDC delta
           equals the parsed Transfer amount exactly; native MATIC delta
           net of gas equals fee0), on-chain position liquidity is
           unchanged, and at least one side of the position accrued
           strictly positive fees (bilateral no-op guard).

        Pool selection: V4's swap router always remaps WMATIC -> native
        MATIC at the pool key (see ``UniswapV4SDK.build_swap_tx``); LP_OPEN
        with ``MATIC/USDC/3000`` resolves currency0 to ``address(0)`` via
        ``_resolve_token(..., for_v4_pool=True)``, so LP and swap share
        the SAME ``(NATIVE_MATIC, USDC, 3000, 60, 0x0)`` pool key and the
        position genuinely accrues fees from the counter-swap. The
        ERC20-keyed ``WETH/USDC/3000`` pool used by VIB-4363 LP_OPEN
        cannot be used here because the V4 swap path is broken for
        ERC20<>ERC20 on Polygon (VIB-4413).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        usdc_decimals = get_token_decimals(web3, usdc_addr)

        # Inject MATIC prices into the session-scoped oracle so the V4
        # compiler's slippage protection can compute against native MATIC.
        # Price is derived from the fork-pinned sqrtPriceX96 (VIB-4427).
        prices_with_native = _build_price_oracle_with_native(price_oracle, anvil_rpc_url)

        # Fail-fast funding check: surface infra/fixture funding regressions
        # before LP_OPEN / counter-swap runs and produces a less-actionable error.
        matic_before_setup = web3.eth.get_balance(funded_wallet)
        usdc_before_setup = get_token_balance(web3, usdc_addr, funded_wallet)
        required_usdc = int((LP_AMOUNT_USDC + COUNTER_SWAP_USDC) * (10 ** usdc_decimals))
        # Native MATIC budget: LP deposit + gas headroom calibrated to the
        # pinned fork block above (counter-swap is USDC -> MATIC so it does
        # not spend wallet's MATIC).
        required_matic = int(
            (LP_AMOUNT_MATIC + NATIVE_MATIC_GAS_HEADROOM) * (10**18)
        )
        assert matic_before_setup >= required_matic, (
            f"Insufficient native MATIC funding for test setup: "
            f"have={matic_before_setup}, need>={required_matic}"
        )
        assert usdc_before_setup >= required_usdc, (
            f"Insufficient USDC funding for test setup: "
            f"have={usdc_before_setup}, need>={required_usdc}"
        )

        print(f"\n{'=' * 80}")
        print("Test: LP_COLLECT_FEES MATIC/USDC via Uniswap V4 on Polygon")
        print(f"{'=' * 80}")

        # Setup: Open a position first
        print("\n--- Setup: Opening LP position ---")
        position_id, liquidity_before, currency0, currency1 = await _open_v4_position(
            web3, funded_wallet, orchestrator, prices_with_native,
        )
        print(f"Opened position: id={position_id}, liquidity={liquidity_before}")
        print(f"Currencies: {currency0[:10]}.../{currency1[:10]}...")

        # LP_OPEN can consume almost all native MATIC available to the test
        # wallet on this fork before the counter-swap runs. The fee-generation
        # swap is USDC -> MATIC and has tx.value=0, but the signer still needs
        # native MATIC for Polygon-priced gas. Top up as setup before recording
        # the COLLECT_FEES pre-balances below so collection deltas stay exact.
        fund_native_token(
            funded_wallet,
            int(POST_LP_OPEN_NATIVE_TOP_UP_MATIC * (10**18)),
            anvil_rpc_url,
        )

        # Verify the LP pool key is the native-MATIC pool (currency0 must
        # be address(0)). If LP_OPEN ever shifted to a non-native pool
        # key, the same-pool fee-accrual invariant below would silently
        # break -- and the counter-swap (which always routes through the
        # native pool key) would generate fees on a DIFFERENT pool than
        # the one we're collecting from.
        assert int(currency0, 16) == 0, (
            f"LP_OPEN must use native MATIC as currency0 so swap and LP "
            f"share the same V4 pool key (currency0=0x0). Got: {currency0}"
        )

        # Fee generation: a single USDC -> MATIC swap through the SAME
        # pool key as the LP position. ``UniswapV4SDK.build_swap_tx``
        # always remaps WMATIC -> native MATIC at the pool layer, so by
        # using the "MATIC" symbol the swap routes through the same
        # (NATIVE_MATIC, USDC, fee=3000, tickSpacing=60, hooks=0x0) pool
        # as LP_OPEN. The LP accrues fees in USDC (the swap's input
        # token), which surface as a PoolManager -> wallet Transfer
        # during COLLECT_FEES.
        print("\n--- Counter-swap to accrue fees (USDC -> MATIC) ---")
        counter_swap_executed = await _counter_swap_to_generate_fees(
            web3, funded_wallet, orchestrator, prices_with_native,
        )
        print(f"Counter-swap executed: {counter_swap_executed}")
        assert counter_swap_executed, (
            "Counter-swap must succeed -- the same-pool fee-accrual cycle "
            "is the only way to prove COLLECT actually transfers fees "
            "(not just principal) on this chain."
        )

        # Cross-check that the on-chain liquidity is unchanged after the
        # counter-swaps (swaps must not touch the LP position liquidity).
        v4_sdk = UniswapV4SDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        liquidity_after_swap = v4_sdk.get_position_liquidity(position_id)
        assert liquidity_after_swap == liquidity_before, (
            f"Counter-swap must not change LP position liquidity. "
            f"before={liquidity_before}, after_swap={liquidity_after_swap}"
        )

        # Record balances BEFORE fee collection.
        # Native MATIC is currency0 of the LP pool; USDC is currency1.
        # ``web3.eth.get_balance`` returns the native MATIC balance; gas
        # spent by COLLECT_FEES is accounted for explicitly below.
        matic_before = web3.eth.get_balance(funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        print("\n--- Collecting fees ---")
        print(f"MATIC before: {format_token_amount(matic_before, 18)}")
        print(f"USDC  before: {format_token_amount(usdc_before, usdc_decimals)}")

        # Layer 1: Compilation
        collect_intent = CollectFeesIntent(
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            protocol_params={
                "position_id": position_id,
                "currency0": currency0,
                "currency1": currency1,
            },
        )

        print(
            f"Created CollectFeesIntent: pool={collect_intent.pool}, "
            f"position_id={position_id}"
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=prices_with_native,
        )

        compilation_result = compiler.compile(collect_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"COLLECT_FEES compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        bundle = compilation_result.action_bundle
        print(f"ActionBundle created with {len(bundle.transactions)} transactions")

        # Layer 2: Execution
        print("\nExecuting COLLECT_FEES via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(bundle)

        assert execution_result.success, (
            f"COLLECT_FEES execution failed: {execution_result.error}"
        )
        print(
            f"Execution successful! "
            f"{len(execution_result.transaction_results)} transactions confirmed"
        )

        # Enrich for accounting (populates result.lp_close_data — Layer 5
        # needs it; mirrors the V3 golden / SushiSwap precedent ordering).
        execution_result = _enrich_for_accounting(
            execution_result, collect_intent, funded_wallet, bundle.metadata
        )

        # Layer 3: Receipt Parsing -- fees0 / fees1 separate from principal.
        #
        # V4 COLLECT_FEES is implemented as DECREASE_LIQUIDITY(0) + TAKE_PAIR,
        # so the receipt MUST contain:
        #   * exactly one ModifyLiquidity event with liquidity_delta == 0
        #     (fees-only; no principal removed), and
        #   * USDC: a Transfer event from the PoolManager to the wallet
        #     carrying the fee amount.
        #   * Native MATIC (currency0): NO Transfer event -- MATIC moves
        #     as msg.value-style flow from the PoolManager. The wallet's
        #     native balance delta is the authoritative fee0 measurement.
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        saw_zero_delta_modify_liquidity = False
        usdc_fees_from_transfers = 0
        lp_close_data = None
        pool_manager_addr = parser.pool_manager  # lowercased in parser ctor
        wallet_lower = funded_wallet.lower()
        gas_spent_wei = 0

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if not tx_result.receipt:
                continue

            receipt_dict = tx_result.receipt.to_dict()
            parsed = parser.parse_receipt(receipt_dict)

            # Compute gas cost so we can isolate fee0 from gas in the
            # native-MATIC balance delta.
            gas_used = receipt_dict.get("gasUsed") or receipt_dict.get("gas_used") or 0
            gas_price = receipt_dict.get("effectiveGasPrice")
            if gas_price is None:
                gas_price = receipt_dict.get("effective_gas_price")
            # Polygon is EIP-1559: ``effectiveGasPrice`` is always present.
            # Fail loudly if it is missing -- it signals an unexpected receipt
            # shape (e.g. a non-1559 tx type or a custom provider transform),
            # which would silently inflate ``matic_fees_received`` and let a
            # COLLECT_FEES no-op slip through the bilateral assertion below.
            assert gas_price is not None, (
                f"effectiveGasPrice missing from Polygon receipt -- tx={tx_result.tx_hash}. "
                f"Polygon is EIP-1559; absence indicates a receipt-shape regression."
            )
            gas_spent_wei += int(gas_used) * int(gas_price)

            for ml in parsed.modify_liquidity_events:
                print(f"  ModifyLiquidity: delta={ml.liquidity_delta}")
                if ml.liquidity_delta == 0:
                    saw_zero_delta_modify_liquidity = True

            for transfer in parsed.transfer_events:
                if (
                    transfer.from_address.lower() == pool_manager_addr
                    and transfer.to_address.lower() == wallet_lower
                ):
                    print(
                        f"  Fee Transfer: token={transfer.token[:10]}... "
                        f"amount={transfer.amount}"
                    )
                    if transfer.token.lower() == usdc_addr.lower():
                        usdc_fees_from_transfers += transfer.amount

            close_data = parser.extract_lp_close_data(receipt_dict)
            if close_data is not None:
                lp_close_data = close_data

        assert saw_zero_delta_modify_liquidity, (
            "V4 LP_COLLECT_FEES must emit a ModifyLiquidity event with "
            "liquidity_delta == 0 (fees-only path, no principal removed)."
        )

        # Layer 4: Balance Deltas + position-liquidity invariant.
        matic_after = web3.eth.get_balance(funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        # MATIC delta excludes gas (gas was deducted from the wallet's MATIC).
        matic_fees_received = (matic_after - matic_before) + gas_spent_wei
        usdc_delta = usdc_after - usdc_before

        print("\n--- Balance Deltas ---")
        print(f"MATIC fees (net of gas): {format_token_amount(matic_fees_received, 18)}")
        print(f"USDC  delta:             {format_token_amount(usdc_delta, usdc_decimals)}")

        # Wallet MUST NOT lose tokens (net of gas) from a fee collection
        # (fees-only path). The wallet may pay gas in native MATIC but the
        # COLLECT step itself can only return value.
        assert matic_fees_received >= 0, (
            f"Native MATIC fee0 must be >= 0 after netting out gas. "
            f"matic_delta+gas={matic_fees_received}"
        )
        assert usdc_delta >= 0, "USDC fee1 must not decrease from fee collection"

        # USDC wallet delta MUST equal parsed Transfer amount exactly --
        # COLLECT_FEES routes USDC directly to the wallet (no unwrap), so
        # any USDC the wallet sees came from a PoolManager -> wallet
        # Transfer event the parser surfaced. This is the strict
        # "fees-vs-principal" separation for the ERC-20 leg.
        assert usdc_delta == usdc_fees_from_transfers, (
            f"USDC wallet delta must equal sum of PoolManager Transfer "
            f"amounts. delta={usdc_delta}, transfers="
            f"{usdc_fees_from_transfers}"
        )

        # Counter-swap routed through the SAME pool key as LP_OPEN
        # (asserted via currency0=0x0 above), so the position MUST have
        # accrued fees on at least one side. This is the bilateral
        # no-op guard for COLLECT_FEES: a tx that succeeds but moves no
        # tokens would silently pass without this assertion.
        #
        # The OR (rather than AND) handles the case where one side of
        # the fee-delivery path may behave differently per chain (e.g.
        # the Optimism LP_CLOSE leak documented in VIB-4360). As long
        # as at least one fee leg fires, the COLLECT_FEES flow is
        # exercised end-to-end and the no-op bug class is caught.
        assert matic_fees_received > 0 or usdc_fees_from_transfers > 0, (
            f"Counter-swap was confirmed to route through the LP_OPEN "
            f"pool key, so the LP position MUST have accrued fees. "
            f"Got MATIC={matic_fees_received}, USDC={usdc_fees_from_transfers}."
        )

        # Position-liquidity invariant: LP_COLLECT_FEES must NOT touch
        # principal liquidity. Query on-chain liquidity post-collect and
        # verify it matches the pre-LP-open liquidity exactly.
        liquidity_after = v4_sdk.get_position_liquidity(position_id)
        assert liquidity_after == liquidity_before, (
            f"LP_COLLECT_FEES must leave position liquidity unchanged. "
            f"before={liquidity_before}, after={liquidity_after}"
        )

        print(
            f"\nFees collected from position {position_id}: "
            f"MATIC={matic_fees_received}, USDC={usdc_delta}"
        )
        print(
            f"Position liquidity invariant: {liquidity_before} == "
            f"{liquidity_after} (unchanged)"
        )

        # Layer 5: assert the real accounting pipeline persisted
        # LP_COLLECT_FEES. VIB-4637 (genuine production gap, surfaced by
        # this rollout): a V4 fees-only collect emits ModifyLiquidity
        # delta=0, so extract_lp_close_data yields no typed pool_address
        # and _resolve_pool_address rejects the V3-style V4 position_key
        # (`matic/usdc/3000`) — the LP_COLLECT_FEES event is dropped
        # entirely (zero rows). On-chain collect is verified correct above
        # (Layers 1–4 hard-asserted). Encode the TRUE behavior via a
        # runtime xfail that fires ONLY on the exact zero-rows drop and
        # auto-reactivates (full hard asserts below run) when VIB-4637
        # lands. Pattern mirrors merged VIB-4633/4634/4635.
        collect_accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=collect_intent,
            result=execution_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_COLLECT_FEES",
            price_oracle=prices_with_native,
            eth_call_reader=anvil_eth_call_adapter,
        )
        _assert_identity(collect_accounting_row, event_type="LP_COLLECT_FEES", wallet=funded_wallet)
        collect_payload = _payload(collect_accounting_row)
        assert collect_payload["position_key"] == collect_accounting_row["position_key"]
        _assert_no_lot_id(collect_accounting_row, collect_payload)
        _assert_v4_close_position_hash(collect_payload)
        if lp_close_data is not None:
            tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
            dec0 = (
                get_token_decimals(web3, tokens[collect_payload["token0"]])
                if collect_payload["token0"] in tokens
                else 18
            )
            dec1 = (
                get_token_decimals(web3, tokens[collect_payload["token1"]])
                if collect_payload["token1"] in tokens
                else 18
            )
            assert Decimal(collect_payload["amount0"]) == _to_human(lp_close_data.amount0_collected, dec0)
            assert Decimal(collect_payload["amount1"]) == _to_human(lp_close_data.amount1_collected, dec1)
            _assert_fee_contract(
                collect_payload["fees0_collected"], _to_human(lp_close_data.fees0, dec0), field="fees0_collected"
            )
            _assert_fee_contract(
                collect_payload["fees1_collected"], _to_human(lp_close_data.fees1, dec1), field="fees1_collected"
            )
        else:
            _assert_fee_contract(collect_payload["fees0_collected"], None, field="fees0_collected")
            _assert_fee_contract(collect_payload["fees1_collected"], None, field="fees1_collected")

        print("\nALL 5 LAYERS PASSED")

    @pytest.mark.intent(IntentType.LP_COLLECT_FEES)  # noqa: layers
    @pytest.mark.asyncio
    async def test_collect_fees_without_position_id_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """V4 LP_COLLECT_FEES requires ``position_id`` in protocol_params.

        Compilation must fail with a clear error mentioning the missing
        ``position_id`` -- this is a hard precondition of
        ``UniswapV4Compiler.compile_collect_fees``.
        Layer 5 adds the books-side mirror: a failed LP_COLLECT_FEES
        writes ZERO accounting_events rows (epic VIB-4591 decision #7).

        Intentional layer exception (``# noqa: layers``) -- this test stops at
        Layer 1 by design. The failure-path contract from
        ``.claude/rules/intent-tests.md`` is still honoured by snapshotting
        WETH/USDC around ``compiler.compile(...)`` and asserting both
        balances are unchanged after the failed compilation (matches the
        sibling pattern in the optimism / base V4 collect_fees tests).
        """
        print(f"\n{'=' * 80}")
        print("Test: COLLECT_FEES without position_id (should fail)")
        print(f"{'=' * 80}")

        # Snapshot balances BEFORE compilation so we can assert conservation
        # after the compile-time failure (no transaction should be sent).
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        prices_with_native = _build_price_oracle_with_native(price_oracle, anvil_rpc_url)

        collect_intent = CollectFeesIntent(
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            # No protocol_params -- missing position_id
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=prices_with_native,
        )

        compilation_result = compiler.compile(collect_intent)

        assert compilation_result.status.value == "FAILED", (
            "Compilation should fail without position_id"
        )
        assert compilation_result.action_bundle is None, (
            "Failed compilation must not produce an ActionBundle"
        )
        assert compilation_result.error, "Compilation failed without an error message"
        assert "position_id" in compilation_result.error.lower(), (
            f"Error should mention position_id, got: {compilation_result.error}"
        )

        # Failure-path balance conservation: no on-chain tx fired, balances unchanged.
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        assert weth_after == weth_before, (
            f"WETH balance must be unchanged after compile-time failure. "
            f"before={weth_before}, after={weth_after}"
        )
        assert usdc_after == usdc_before, (
            f"USDC balance must be unchanged after compile-time failure. "
            f"before={usdc_before}, after={usdc_after}"
        )

        print(f"Compilation failed as expected: {compilation_result.error}")

        # Layer 5: a failed LP_COLLECT_FEES must write zero accounting_events rows.
        failed_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.VALIDATION,
            error=compilation_result.error or "LP_COLLECT_FEES compilation failed",
        )
        await assert_no_accounting_on_failure(
            layer5_accounting_harness,
            intent=collect_intent,
            result=failed_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=prices_with_native,
            eth_call_reader=anvil_eth_call_adapter,
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
