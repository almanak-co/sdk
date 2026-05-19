"""4-layer intent tests for Uniswap V4 LP_COLLECT_FEES on Avalanche Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
collecting fees from V4 LP positions via PositionManager on Avalanche:
1. Open an AVAX/USDC LP position (LP_OPEN as setup -- ``AVAX`` symbol so
   currency0 resolves to ``address(0)``, matching the V4 swap router's
   WAVAX -> native AVAX remapping)
2. Generate fees by counter-swapping (USDC -> AVAX) through the SAME
   native-AVAX pool
3. Create CollectFeesIntent with position_id and protocol_params
4. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
5. Execute via ExecutionOrchestrator (full production pipeline)
6. Parse receipts -- fees (Transfer from PoolManager for USDC, native
   delta for AVAX) separate from principal (ModifyLiquidity delta == 0)
7. Verify position liquidity is unchanged on-chain after collection
8. Verify at least one side of the position accrued strictly positive
   fees (bilateral no-op guard)

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

VIB-4369 / VIB-4343: registry edit (adding "avalanche" to uniswap_v4 declared
chains) is OUT OF SCOPE for this ticket. The ``no_zodiac`` marker is
required because uniswap_v4 is not in the synthetic_intents manifest matrix.

Pool selection: ``AVAX/USDC/3000``. Avalanche V4 has the native-keyed
``(NATIVE_AVAX, USDC, 3000, 60, 0x0)`` pool initialized at fork time
(verified 2026-05-14 via the VIB-4366 sibling swap test:
sqrtPriceX96=2.477e23, tick=-253527, liquidity=1.47e13 — sufficient for
the small LP / counter-swap amounts the test uses; corresponds to
~9.77 USDC per AVAX). Using ``AVAX`` symbol means
``UniswapV4Adapter._resolve_token(for_v4_pool=True)`` resolves currency0
to ``address(0)`` at LP_OPEN time (AVAX is in the adapter's
``native_symbols = {"ETH", "AVAX", "MATIC", "BNB"}`` set), and the V4
SwapIntent path already remaps WAVAX -> native AVAX at the pool layer
(``UniswapV4SDK.build_swap_tx``). So the LP and the counter-swap
genuinely share the SAME pool key. This is the same VIB-4413 workaround
used in the Base (VIB-4357), Optimism (VIB-4361), and Polygon (VIB-4365)
siblings — picking the wrapped-native side avoids the ERC20<>ERC20 V4
swap revert.

Counter-swap direction: USDC -> AVAX (single leg, mirroring the Polygon
VIB-4365 sibling pattern). The SDK's WAVAX -> native AVAX remap means
the wallet would need WAVAX for a return-leg AVAX -> USDC swap. The
avalanche conftest does wrap 10 WAVAX for the wallet, so a round-trip
would technically work, but a single USDC -> AVAX leg is sufficient to
prove fee-accrual end-to-end (the LP accrues fees in USDC — the input
token of the swap — which surface as a PoolManager -> wallet Transfer
during COLLECT_FEES), and keeps the test minimal and aligned with the
Polygon sibling.

The bilateral fee assertion uses OR across the two sides (AVAX fee OR
USDC fee strictly positive) — matches the Optimism VIB-4361 sibling
pattern where the native-fee leg may not reach the wallet on every V4
deployment (PositionManager edge case noted in VIB-4360). As long as
at least one fee leg fires, the COLLECT_FEES flow is exercised
end-to-end and the no-op bug class is caught.

To run:
    uv run pytest tests/intents/avalanche/test_uniswap_v4_collect_fees.py -v -s
"""

import json
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.connectors.uniswap_v4.sdk import NATIVE_CURRENCY, UniswapV4SDK
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
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_accounting_persisted_or_gap,
    assert_no_accounting_on_failure,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

pytestmark = pytest.mark.no_zodiac(
    reason="VIB-4343: uniswap_v4 not yet in synthetic_intents matrix"
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "avalanche"

# AVAX/USDC pool with 0.3% fee tier. V4 pools support native AVAX as
# currency0 (address(0)); ``UniswapV4Adapter._resolve_token(for_v4_pool=True)``
# returns ``NATIVE_CURRENCY`` for the ``AVAX`` symbol (it is in the
# adapter's ``native_symbols = {"ETH", "AVAX", "MATIC", "BNB"}`` set), and
# ``UniswapV4SDK.build_swap_tx`` substitutes WAVAX -> native AVAX for the
# V4 pool key (see ``sdk.py:_is_wrapped_native`` handling). Opening the LP
# with ``AVAX/USDC/3000`` resolves currency0 to ``address(0)``, so the LP
# position and the counter-swap end up on the SAME pool key and the
# position can actually accrue fees. Using ``WAVAX/USDC/3000`` here would
# open a position on a separate ERC20-keyed pool that the V4 swap path
# can't reach (VIB-4413 ERC20<>ERC20 revert).
#
# Fee tier 3000 matches the Base / Optimism / Polygon siblings (tick
# spacing 60). The native AVAX/USDC pool at fee=3000 was verified
# initialized at fork time by the VIB-4366 swap test:
# liquidity=1.47e13 at tick=-253527 (~9.77 USDC per AVAX).
LP_POOL = "AVAX/USDC/3000"

# Token symbols for the fee-generation counter-swap. ``AVAX`` symbol in
# the SwapIntent path resolves to WAVAX via ``resolve_for_swap``, then
# ``UniswapV4SDK.build_swap_tx`` detects WAVAX as the wrapped native and
# substitutes native AVAX at the pool layer -- matching the LP pool key.
# Wallet balance checks below read native balance for the AVAX side.
SWAP_TOKEN0_SYMBOL = "AVAX"
SWAP_TOKEN1_SYMBOL = "USDC"

# LP amounts for setup. The avalanche conftest funds the EOA with native
# AVAX (100 AVAX via fund_native_token), 10 WAVAX (via wrap), and 100,000
# USDC; we deposit meaningful slices so the position captures a non-trivial
# share of the counter-swap's fees. The native AVAX/USDC pool at fork time
# has liquidity ~1.47e13, so the position needs enough capital to register
# against the pool.
LP_AMOUNT_AVAX = Decimal("5")
LP_AMOUNT_USDC = Decimal("50")
# Tick range that brackets the current pool price (~9.77 USDC/AVAX at
# avalanche mainnet fork-block time -- on-chain tick ~-253527). The range
# must include the spot price so the position is in-range and accrues
# fees from the counter-swap. A wider range (e.g. 0.01 - 1000) would
# shrink the position's effective share at the spot tick to below the
# precision of the fee accrual integers, and the parser would see zero
# fees collected.
LP_RANGE_LOWER = Decimal("1")
LP_RANGE_UPPER = Decimal("100")

# Counter-swap amount -- a single USDC -> AVAX swap through the same pool
# key as LP_OPEN forces the LP position to accrue USDC-side fees. We
# intentionally do only ONE direction (USDC -> AVAX) so the LP accrues
# fees in USDC (the input token of the swap), which surface as a
# PoolManager -> wallet Transfer event during COLLECT_FEES. Matches the
# Polygon VIB-4365 sibling pattern -- the reverse leg is unnecessary for
# proving fee-accrual end-to-end.
COUNTER_SWAP_USDC = Decimal("500")
SWAP_MAX_SLIPPAGE = Decimal("0.10")


# =============================================================================
# Helpers
# =============================================================================


def _derive_avax_price_from_slot0(anvil_rpc_url: str) -> Decimal:
    """Derive AVAX/USD price from the V4 pool's sqrtPriceX96 at fork time.

    Reads the on-chain sqrtPriceX96 from the Avalanche V4 StateView for
    the ``(NATIVE_AVAX, USDC, 3000, 60, 0x0)`` pool.  The fork block is
    pinned (``ANVIL_FORK_BLOCK_AVALANCHE``), so this value is constant
    across CI runs and immune to live-price drift — eliminating the
    VIB-4427 flake.

    Conversion: ``price_usdc_per_avax = (sqrtPriceX96 / 2**96)**2
                 * 10**(avax_decimals - usdc_decimals)``
    (18 - 6 = 12, USDC is currency1 / token1).

    Falls back to a hard-coded fork-block snapshot price (~9.77 USDC/AVAX,
    verified 2026-05-14 by the VIB-4366 swap test) if the StateView call
    reverts — this makes the test fail-safe on infra issues while keeping
    the price anchored to the fork block rather than the live market.
    """
    tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
    usdc_addr = tokens["USDC"]

    sdk = UniswapV4SDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
    pool_key = sdk.compute_pool_key(
        token0=NATIVE_CURRENCY,  # address(0) for native AVAX (currency0)
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
        # Hard-coded fork-block snapshot (VIB-4427): ~9.77 USDC/AVAX
        # verified 2026-05-14 via the VIB-4366 swap test (sqrtPriceX96=2.477e23).
        return Decimal("9.77")

    # sqrtPriceX96 represents sqrt(token1/token0) in Q96 fixed-point.
    # token0 = native AVAX (18 dec), token1 = USDC (6 dec).
    # raw_ratio = (sqrtPriceX96 / 2**96)**2  ->  USDC_raw_units / AVAX_raw_units
    # human price (USDC per AVAX) = raw_ratio * 10**(18 - 6)
    raw_ratio = (Decimal(sqrt_price) / Decimal(2**96)) ** 2
    avax_price = raw_ratio * Decimal(10 ** (18 - 6))
    return avax_price


def _build_price_oracle_with_native(
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> dict[str, Decimal]:
    """Return price oracle augmented with AVAX / WAVAX prices.

    Derives the AVAX price from the pinned fork-block sqrtPriceX96
    (VIB-4427) instead of a live CoinGecko fetch to eliminate the
    live-oracle ↔ fork-block coupling that causes the flaky
    V4TooLittleReceived revert.

    The session-scoped oracle already prices WAVAX (it's in
    CHAIN_CONFIGS), so we honor that for WAVAX and only add an explicit
    AVAX entry. Doing both makes the test resilient to either symbol
    being looked up by the LP_OPEN / SwapIntent compile paths.
    """
    avax_price = _derive_avax_price_from_slot0(anvil_rpc_url)
    return {
        **price_oracle,
        "AVAX": avax_price,
        "WAVAX": price_oracle.get("WAVAX", avax_price),
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
        amount0=LP_AMOUNT_AVAX,
        amount1=LP_AMOUNT_USDC,
        range_lower=LP_RANGE_LOWER,
        range_upper=LP_RANGE_UPPER,
        protocol="uniswap_v4",
        chain=CHAIN_NAME,
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
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> bool:
    """Run a USDC -> AVAX swap through the V4 connector to accrue fees.

    Single direction USDC -> AVAX SwapIntent routed via the V4
    UniversalRouter. The SDK routes the swap through the
    ``(NATIVE_AVAX, USDC, 3000, 60, 0x0)`` pool key (V4's swap path
    always remaps WAVAX -> native AVAX), which is the SAME pool key
    LP_OPEN used. The position then accrues fees in USDC (the input
    token of the swap), which surface as a PoolManager -> wallet
    Transfer event during the subsequent COLLECT_FEES.

    Single-direction (not round-trip) mirrors the Polygon VIB-4365
    sibling pattern -- the USDC-side fee leg alone produces a verifiable
    fee-accrual signal end-to-end.

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
        return False
    swap_result = await orchestrator.execute(swap_compilation.action_bundle)
    return swap_result.success


# =============================================================================
# Layer-5 accounting helpers (mirrors tests/intents/ethereum/test_uniswap_v3_lp.py;
# V4-specific position_hash directional contract per epic VIB-4591 / VIB-4594)
# =============================================================================


def _execution_context(wallet: str) -> ExecutionContext:
    return ExecutionContext(
        strategy_id="layer5-uniswap-v4-lp",
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
    assert row["strategy_id"] == "layer5-intent-test"
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

    Per epic VIB-4591 decision #5 / blueprints/27 Empty≠Zero≠None. The V4
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
# CollectFeesIntent Tests -- Uniswap V4 on Avalanche
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.lp
class TestUniswapV4CollectFeesIntent:
    """Test Uniswap V4 LP_COLLECT_FEES using CollectFeesIntent on Avalanche.

    These tests verify the fee collection flow:
    - First open a position (setup)
    - Generate trading fees via counter-swap through the same pool key
    - CollectFeesIntent creation with protocol_params
    - IntentCompiler routes to ``_compile_collect_fees_uniswap_v4()``
    - Transactions execute successfully on-chain via PositionManager
    - Position liquidity is unchanged after fee collection (fees-only)
    - Wallet gains ONLY the fee amounts; principal stays locked in the pool
    """

    @pytest.mark.intent(
        IntentType.LP_OPEN, IntentType.SWAP, IntentType.LP_COLLECT_FEES
    )
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-4426 V0 (PR #2335) rejects native-ETH V4 pools via the T06 adapter guard at test setup; native-ETH currency0 support is V1 work (VIB-4483 / P-V1-B). as of 2026-05-17.",
        strict=True,
    )
    async def test_collect_fees_avax_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Collect fees from an AVAX/USDC LP position via V4 on Avalanche.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle.
        2. Execution: ExecutionOrchestrator -> success.
        3. Receipt Parsing: ModifyLiquidity event with ``liquidity_delta == 0``
           (fees-only path; principal is NOT removed). USDC fee amount
           surfaces as a PoolManager -> wallet Transfer; native AVAX fee
           is the native balance delta (TAKE flows native AVAX directly
           with no Transfer event).
        4. Balance Deltas: Wallet gains ONLY the fee amounts (USDC delta
           equals the parsed Transfer amount exactly; native AVAX delta
           net of gas equals fee0), on-chain position liquidity is
           unchanged, and at least one side of the position accrued
           strictly positive fees (bilateral no-op guard).

        Pool selection: V4's swap router always remaps WAVAX -> native
        AVAX at the pool key (see ``UniswapV4SDK.build_swap_tx``); LP_OPEN
        with ``AVAX/USDC/3000`` resolves currency0 to ``address(0)`` via
        ``_resolve_token(..., for_v4_pool=True)``, so LP and swap share
        the SAME ``(NATIVE_AVAX, USDC, 3000, 60, 0x0)`` pool key and the
        position genuinely accrues fees from the counter-swap. The
        ERC20-keyed ``WAVAX/USDC/3000`` pool path would fall foul of the
        VIB-4413 ERC20<>ERC20 revert.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        usdc_decimals = get_token_decimals(web3, usdc_addr)

        # Inject AVAX prices into the session-scoped oracle so the V4
        # compiler's slippage protection can compute against native AVAX.
        # Price is derived from the fork-pinned sqrtPriceX96 (VIB-4427).
        prices_with_native = _build_price_oracle_with_native(price_oracle, anvil_rpc_url)

        # Fail-fast funding check: surface infra/fixture funding regressions
        # before LP_OPEN / counter-swap runs and produces a less-actionable error.
        avax_before_setup = web3.eth.get_balance(funded_wallet)
        usdc_before_setup = get_token_balance(web3, usdc_addr, funded_wallet)
        required_usdc = int((LP_AMOUNT_USDC + COUNTER_SWAP_USDC) * (10 ** usdc_decimals))
        # Native AVAX budget: LP deposit + gas headroom (counter-swap
        # is USDC -> AVAX so it does not spend wallet's AVAX).
        required_avax = int(
            (LP_AMOUNT_AVAX + Decimal("2")) * (10**18)
        )
        assert avax_before_setup >= required_avax, (
            f"Insufficient native AVAX funding for test setup: "
            f"have={avax_before_setup}, need>={required_avax}"
        )
        assert usdc_before_setup >= required_usdc, (
            f"Insufficient USDC funding for test setup: "
            f"have={usdc_before_setup}, need>={required_usdc}"
        )

        print(f"\n{'=' * 80}")
        print("Test: LP_COLLECT_FEES AVAX/USDC via Uniswap V4 on Avalanche")
        print(f"{'=' * 80}")

        # Setup: Open a position first
        print("\n--- Setup: Opening LP position ---")
        position_id, liquidity_before, currency0, currency1 = await _open_v4_position(
            web3, funded_wallet, orchestrator, prices_with_native,
        )
        print(f"Opened position: id={position_id}, liquidity={liquidity_before}")
        print(f"Currencies: {currency0[:10]}.../{currency1[:10]}...")

        # Verify the LP pool key is the native-AVAX pool (currency0 must
        # be address(0)). If LP_OPEN ever shifted to a non-native pool
        # key, the same-pool fee-accrual invariant below would silently
        # break -- and the counter-swap (which always routes through the
        # native pool key) would generate fees on a DIFFERENT pool than
        # the one we're collecting from.
        assert int(currency0, 16) == 0, (
            f"LP_OPEN must use native AVAX as currency0 so swap and LP "
            f"share the same V4 pool key (currency0=0x0). Got: {currency0}"
        )

        # Fee generation: a single USDC -> AVAX swap through the SAME
        # pool key as the LP position. ``UniswapV4SDK.build_swap_tx``
        # always remaps WAVAX -> native AVAX at the pool layer, so by
        # using the "AVAX" symbol the swap routes through the same
        # (NATIVE_AVAX, USDC, fee=3000, tickSpacing=60, hooks=0x0) pool
        # as LP_OPEN. The LP accrues fees in USDC (the swap's input
        # token), which surface as a PoolManager -> wallet Transfer
        # during COLLECT_FEES.
        print("\n--- Counter-swap to accrue fees (USDC -> AVAX) ---")
        counter_swap_executed = await _counter_swap_to_generate_fees(
            funded_wallet, orchestrator, prices_with_native,
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
        # Native AVAX is currency0 of the LP pool; USDC is currency1.
        # ``web3.eth.get_balance`` returns the native AVAX balance; gas
        # spent by COLLECT_FEES is accounted for explicitly below.
        avax_before = web3.eth.get_balance(funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        print("\n--- Collecting fees ---")
        print(f"AVAX before: {format_token_amount(avax_before, 18)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

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
        #   * Native AVAX (currency0): NO Transfer event -- AVAX moves
        #     as msg.value-style flow from the PoolManager. The wallet's
        #     native balance delta is the authoritative fee0 measurement.
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        saw_zero_delta_modify_liquidity = False
        usdc_fees_from_transfers = 0
        parsed_fee_amount_total = 0
        pool_manager_addr = parser.pool_manager  # lowercased in parser ctor
        wallet_lower = funded_wallet.lower()
        gas_spent_wei = 0
        lp_close_data = None

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if not tx_result.receipt:
                continue

            receipt_dict = tx_result.receipt.to_dict()
            parsed = parser.parse_receipt(receipt_dict)

            # Compute gas cost so we can isolate fee0 from gas in the
            # native-AVAX balance delta.
            gas_used = receipt_dict.get("gasUsed") or receipt_dict.get("gas_used") or 0
            gas_price = receipt_dict.get("effectiveGasPrice")
            if gas_price is None:
                gas_price = receipt_dict.get("effective_gas_price")
            # Avalanche C-Chain is EIP-1559: ``effectiveGasPrice`` is always present.
            # Fail loudly if it is missing -- it signals an unexpected receipt
            # shape (e.g. a non-1559 tx type or a custom provider transform),
            # which would silently inflate ``avax_fees_received`` and let a
            # COLLECT_FEES no-op slip through the bilateral assertion below.
            assert gas_price is not None, (
                f"effectiveGasPrice missing from Avalanche receipt -- tx={tx_result.tx_hash}. "
                f"Avalanche C-Chain is EIP-1559; absence indicates a receipt-shape regression."
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
                    if transfer.amount > 0:
                        parsed_fee_amount_total += transfer.amount
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

        # Layer 3 fee-positivity assertion: the parsed receipt must independently
        # prove fee movement on the USDC side (the AVAX side has no Transfer
        # event since native value flows from PoolManager without an ERC-20
        # log — that side is asserted via the native balance delta below).
        # This catches receipt-parser regressions before Layer 4 has a chance
        # to mask them via the OR-bilateral guard.
        assert parsed_fee_amount_total > 0, (
            "V4 LP_COLLECT_FEES receipt parser must surface at least one "
            "positive PoolManager -> wallet Transfer event for the USDC fee "
            "leg. Got 0 — parser regression or no fees accrued in the "
            "counter-swap setup."
        )

        # Layer 4: Balance Deltas + position-liquidity invariant.
        avax_after = web3.eth.get_balance(funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        # AVAX delta excludes gas (gas was deducted from the wallet's AVAX).
        avax_fees_received = (avax_after - avax_before) + gas_spent_wei
        usdc_delta = usdc_after - usdc_before

        print("\n--- Balance Deltas ---")
        print(f"AVAX fees (net of gas): {format_token_amount(avax_fees_received, 18)}")
        print(f"USDC delta:             {format_token_amount(usdc_delta, usdc_decimals)}")

        # Wallet MUST NOT lose tokens (net of gas) from a fee collection
        # (fees-only path). The wallet may pay gas in native AVAX but the
        # COLLECT step itself can only return value.
        assert avax_fees_received >= 0, (
            f"Native AVAX fee0 must be >= 0 after netting out gas. "
            f"avax_delta+gas={avax_fees_received}"
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
        # the Optimism LP_CLOSE leak documented in VIB-4360, where the
        # native-fee leg may not reach the wallet on some deployed V4
        # PositionManagers). As long as at least one fee leg fires, the
        # COLLECT_FEES flow is exercised end-to-end and the no-op bug
        # class is caught.
        assert avax_fees_received > 0 or usdc_fees_from_transfers > 0, (
            f"Counter-swap was confirmed to route through the LP_OPEN "
            f"pool key, so the LP position MUST have accrued fees. "
            f"Got AVAX={avax_fees_received}, USDC={usdc_fees_from_transfers}."
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
            f"AVAX={avax_fees_received}, USDC={usdc_delta}"
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
        # (`avax/usdc/3000`) — the LP_COLLECT_FEES event is dropped
        # entirely (zero rows). On-chain collect is verified correct above
        # (Layers 1–4 hard-asserted). Encode the TRUE behavior via a
        # runtime xfail that fires ONLY on the exact zero-rows drop and
        # auto-reactivates (full hard asserts below run) when VIB-4637
        # lands. Pattern mirrors merged VIB-4633/4634/4635.
        collect_accounting_row = await assert_accounting_persisted_or_gap(
            layer5_accounting_harness,
            intent=collect_intent,
            result=execution_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_COLLECT_FEES",
            gap_xfail_reason=(
                "VIB-4637: V4 LP_COLLECT_FEES accounting event dropped — "
                "fees-only collect (ModifyLiquidity delta=0) yields no typed "
                "pool_address and _resolve_pool_address rejects the V3-style "
                "V4 position_key. On-chain collect verified correct above."
            ),
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
            # No lp_close_data — still pin amount0/1 to the Layer-4 signals
            # so a zero or mis-scaled persisted amount cannot pass unchecked
            # (CodeRabbit PR #2369). token0 = native AVAX (18), token1 = USDC.
            assert Decimal(collect_payload["amount0"]) == _to_human(avax_fees_received, 18)
            assert Decimal(collect_payload["amount1"]) == _to_human(usdc_delta, usdc_decimals)
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
        ``_compile_collect_fees_uniswap_v4``. Layer 5 adds the books-side
        mirror: a failed LP_COLLECT_FEES writes ZERO accounting_events
        rows (epic VIB-4591 decision #7).

        Intentional layer exception (``# noqa: layers``) -- this test stops at
        Layer 1 by design. The failure-path contract from
        ``.claude/rules/intent-tests.md`` is still honoured by snapshotting
        WAVAX/USDC around ``compiler.compile(...)`` and asserting both
        balances are unchanged after the failed compilation (matches the
        sibling pattern in the optimism / polygon V4 collect_fees tests).
        """
        print(f"\n{'=' * 80}")
        print("Test: COLLECT_FEES without position_id (should fail)")
        print(f"{'=' * 80}")

        # Snapshot balances BEFORE compilation so we can assert conservation
        # after the compile-time failure (no transaction should be sent).
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wavax_addr = tokens["WAVAX"]
        usdc_addr = tokens["USDC"]
        wavax_before = get_token_balance(web3, wavax_addr, funded_wallet)
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
        wavax_after = get_token_balance(web3, wavax_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        assert wavax_after == wavax_before, (
            f"WAVAX balance must be unchanged after compile-time failure. "
            f"before={wavax_before}, after={wavax_after}"
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
