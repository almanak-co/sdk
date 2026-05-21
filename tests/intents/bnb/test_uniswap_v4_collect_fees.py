"""4-layer intent tests for Uniswap V4 LP_COLLECT_FEES on BNB Chain Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
collecting fees from V4 LP positions via PositionManager on BNB Chain:
1. Open a BNB/USDT LP position (LP_OPEN as setup -- ``BNB`` symbol so
   currency0 resolves to ``address(0)``, matching the V4 swap router's
   WBNB -> native BNB remapping)
2. Generate fees by counter-swapping (USDT -> WBNB) through the SAME
   native-BNB pool (WBNB resolves to NATIVE at the pool key layer)
3. Create CollectFeesIntent with position_id and protocol_params
4. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
5. Execute via ExecutionOrchestrator (full production pipeline)
6. Parse receipts -- fees (Transfer from PoolManager for USDT, native
   delta for BNB) separate from principal (ModifyLiquidity delta == 0)
7. Verify position liquidity is unchanged on-chain after collection
8. Verify at least one side of the position accrued strictly positive
   fees (bilateral no-op guard)

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

VIB-4373 / VIB-4343: registry edit (adding "bsc" to uniswap_v4 declared
chains) is OUT OF SCOPE for this ticket. The ``no_zodiac`` marker is
required because uniswap_v4 is not in the synthetic_intents manifest matrix.

Pool selection: ``BNB/USDT/3000``. The native-keyed
``(NATIVE_BNB, USDT, 3000, 60, 0x0)`` pool was probed against BSC
mainnet on 2026-05-14 with sqrtPriceX96 ~= 2.057e30, tick=65133 (~673
USDT per BNB) and liquidity ~= 5.587e21. Same tier the LP_CLOSE
sibling (VIB-4372) uses.

WHY fee=3000 not fee=500: the ``UniswapV4Adapter.default_fee_tier`` is
3000, and ``SwapIntent`` does NOT carry a fee-tier parameter — the
counter-swap below routes through the fee=3000 pool regardless of
what tier LP_OPEN uses. Opening the LP at fee=500 (the deepest
native-keyed BNB pool, ~38x the fee=3000 tier, used by the swap
VIB-4370 and LP_OPEN VIB-4371 siblings that don't need a same-pool
round-trip) would put LP and swap on DIFFERENT pool keys, the
position would accrue zero fees from the counter-swap, and the
parser's ``usdt_fees_from_transfers > 0`` assertion would fail with
a misleading "no fee transfer" error rather than the actual
wrong-pool-key cause. fee=3000 matches the Avalanche (VIB-4369) and
Base (VIB-4357) collect_fees siblings' convention.

Using ``BNB`` symbol means
``UniswapV4Adapter._resolve_token(for_v4_pool=True)`` resolves
currency0 to ``address(0)`` at LP_OPEN time (BNB is in the adapter's
``native_symbols = {"ETH", "AVAX", "MATIC", "BNB"}`` set), and the V4
SwapIntent path remaps WBNB -> native BNB at the pool layer
(``UniswapV4SDK.build_swap_tx``). This is the same VIB-4413
workaround used in the Base (VIB-4357), Optimism (VIB-4361), Polygon
(VIB-4365) and Avalanche (VIB-4369) siblings — picking the
wrapped-native side avoids the ERC20<>ERC20 V4 swap revert.

Counter-swap direction: USDT -> WBNB (single leg, mirroring the
Avalanche VIB-4369 and Polygon VIB-4365 sibling pattern). ``WBNB`` is
the wrapped-native side; the V4 SDK substitutes NATIVE for the pool
key at swap-build time so the swap routes through the same
native-keyed pool as LP_OPEN. The reverse leg (BNB -> USDT) is
skipped because the LP only needs the input-side fees to be
verifiable: a USDT -> WBNB swap charges fees on USDT (the input
token), which surface as a PoolManager -> wallet Transfer during
COLLECT_FEES. Using "WBNB" rather than the bare "BNB" symbol mirrors
the BNB swap (VIB-4370) sibling and avoids the native-output
UNWRAP+SWEEP code path entirely.

The bilateral fee assertion uses OR across the two sides (BNB fee OR
USDT fee strictly positive) — matches the Avalanche VIB-4369 and
Optimism VIB-4361 sibling pattern where the native-fee leg may not
reach the wallet on every V4 deployment (PositionManager edge case
noted in VIB-4360). As long as at least one fee leg fires, the
COLLECT_FEES flow is exercised end-to-end and the no-op bug class is
caught.

BNB-mainnet-state quirk (EIP-7702): ``TEST_WALLET = 0xf39F...`` (Anvil
account #0) has signed an EIP-7702 SetCode delegation
(``0xef0100<delegate>``) on BNB mainnet that auto-forwards incoming
native BNB to an external address. Inherited by the Anvil fork, this
swallows the COLLECT_FEES TAKE_PAIR native-fee payout silently — the
wallet's ``eth.get_balance`` does not change. The test clears that
delegation via ``anvil_setCode`` before LP_OPEN; this matches the
LP_CLOSE sibling (VIB-4372) and is consistent with production
user-wallet behaviour (no delegation set).

BSC USDT (and USDC) are 18-decimal Binance-Peg tokens — unlike most
chains where USDT/USDC are 6-decimal. The adapter resolves decimals
via the token resolver so the wei math works without any test-side
override.

To run:
    uv run pytest tests/intents/bnb/test_uniswap_v4_collect_fees.py -v -s
"""

import json
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.connectors.uniswap_v4.sdk import UniswapV4SDK
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

# Framework canonical chain name for BNB Chain. The conftest exposes both
# "bsc" and "bnb" aliases in CHAIN_CONFIGS, but the V4 adapter / SDK route
# off the chain name passed to IntentCompiler, and the BNB conftest uses
# "bsc" (matches chain_id=56 lookup order). The intent's ``chain=`` param
# is normalised to "bsc" by ``resolve_chain_name`` regardless of whether
# the caller writes "bnb" or "bsc".
CHAIN_NAME = "bsc"

# BNB/USDT pool with 0.3% fee tier (3000), tick spacing 60.
# This is the native-keyed V4 pool — pool key currency0=address(0). Probed
# against BSC mainnet on 2026-05-14: sqrtPriceX96≈2.057e30 (mid-price ~673
# USDT per BNB) and liquidity≈5.587e21.
#
# WHY fee=3000 not fee=500: the ``UniswapV4Adapter`` ``default_fee_tier``
# is 3000, and ``SwapIntent`` does NOT carry a fee-tier parameter — the
# counter-swap below would route through the fee=3000 pool regardless
# of what tier LP_OPEN used. Opening the LP at fee=500 (the deepest
# native-keyed BNB pool, used by the swap (VIB-4370) and LP_OPEN
# (VIB-4371) siblings that don't need a same-pool round-trip) would
# put LP and swap on DIFFERENT pool keys, the position would accrue
# zero fees from the counter-swap, and the parser's
# ``usdt_fees_from_transfers > 0`` assertion would fail with a
# misleading "no fee transfer" error rather than the actual
# wrong-pool-key cause. fee=3000 is also the tier the BNB LP_CLOSE
# sibling (VIB-4372) uses, and it matches the Avalanche (VIB-4369)
# and Base (VIB-4357) collect_fees siblings' fee=3000 LP convention.
# Picking BNB over WBNB forces the adapter into ``for_v4_pool=True``
# -> NATIVE_CURRENCY substitution, matching the native-key path used
# by the avalanche / polygon / base / optimism siblings and avoiding
# the VIB-4413 ERC20<>ERC20 revert.
LP_POOL = "BNB/USDT/3000"

# Token symbols for the fee-generation counter-swap. ``WBNB`` symbol in
# the SwapIntent path resolves directly to the WBNB ERC-20 address;
# ``UniswapV4SDK.build_swap_tx`` then detects WBNB as the wrapped native
# and substitutes NATIVE for the pool key at swap-build time -- so the
# swap routes through the SAME ``(NATIVE_BNB, USDT, 3000, 60, 0x0)``
# pool key as LP_OPEN. Using ``WBNB`` (not the bare ``BNB`` symbol)
# mirrors the BNB swap (VIB-4370) sibling exactly, avoiding the
# native-output UNWRAP+SWEEP code path (the SETTLE leg returns the
# wallet to base state cleanly when the output stays as WBNB).
SWAP_TOKEN_NATIVE_SYMBOL = "WBNB"
SWAP_TOKEN_STABLE_SYMBOL = "USDT"

# LP amounts and price range for setup. The bnb conftest funds the EOA
# with 100 native BNB and 100,000 USDT (Binance-Peg is 18 decimals on
# BSC), so 2 BNB / 1,400 USDT is ~2% of the native and ~1.4% of the
# USDT seed — well inside the funding envelope. At fork price ~673
# USDT/BNB the in-range token ratio is roughly 1:673, so 1,400 USDT /
# 2 BNB = 700 USDT per BNB lands near the spot ratio.
#
# Why this size: the native-keyed BNB/USDT fee=3000 pool has liquidity
# ~5.587e21 (fee=3000 is ~38x less liquid than fee=500 on BSC, but
# fee=3000 is the only tier the V4 SwapIntent counter-swap reaches —
# the adapter's ``default_fee_tier=3000`` is hard-coded and SwapIntent
# has no fee parameter). At 2 BNB the position contributes ~1e21 to
# the active liquidity at the spot tick, giving the LP a meaningful
# share (~0.13%) of pool-wide fee accrual. The 2,000 USDT counter-swap
# below charges ~6 USDT in pool fees (0.3% of swap); ~0.13% × 6 USDT
# ≈ 7.8 mUSDT, well above the integer-wei rounding floor.
LP_AMOUNT_BNB = Decimal("2")
LP_AMOUNT_USDT = Decimal("1400")
# Wide tick range bracketing the current pool price (~$673 USDT/BNB
# at fork-block time, on-chain tick ~65133). The range must include
# the spot price so the position is in-range and accrues fees from the
# counter-swap. Matches the bnb LP_CLOSE (VIB-4372) sibling range
# [200, 2000] — wide enough that small price drifts during the
# counter-swap keep the position unambiguously in-range, and the
# fee=3000 pool's smaller absolute liquidity (~5.587e21 vs fee=500's
# 2.1e23) keeps the position's share at the spot tick large enough
# for fees to surface above the integer-wei rounding floor.
LP_RANGE_LOWER = Decimal("200")  # 200 USDT per BNB
LP_RANGE_UPPER = Decimal("2000")  # 2000 USDT per BNB

# Counter-swap amount -- a single USDT -> WBNB swap through the same
# pool key as LP_OPEN forces the LP position to accrue USDT-side fees.
# We intentionally do only ONE direction (USDT -> WBNB; WBNB is the
# wrapped native -- SDK substitutes NATIVE for the pool key, so this
# routes through the same (NATIVE, USDT, 3000, 60, 0x0) pool as
# LP_OPEN) so the LP accrues fees in USDT (the input token of the
# swap), which surface as a PoolManager -> wallet Transfer event
# during COLLECT_FEES. Matches the Avalanche VIB-4369 and Polygon
# VIB-4365 sibling pattern -- the reverse leg is unnecessary for
# proving fee-accrual end-to-end.
#
# Size at 2,000 USDT (~3 BNB at fork price) -- this is the largest
# single-direction counter-swap that consistently lands under the
# 0.10 slippage tolerance configured below; a 5,000 USDT swap reverted
# on Anvil with "Invalid revert data: 0x" (V4 router slippage abort
# with no bubbled selector). Combined with the LP position above
# (2 BNB / 1,400 USDT in a fee=3000 pool with ~5.587e21 liquidity),
# 2,000 USDT generates ~7.8 mUSDT of fees for our position — a
# positive integer-wei USDT Transfer from PoolManager to the wallet
# during COLLECT_FEES, well above the rounding floor.
COUNTER_SWAP_USDT = Decimal("2000")
SWAP_MAX_SLIPPAGE = Decimal("0.10")


# =============================================================================
# Helpers
# =============================================================================


def _augment_oracle_with_bnb(
    price_oracle: dict[str, Decimal],
) -> dict[str, Decimal]:
    """Return a copy of the session oracle with a ``BNB`` entry.

    The session-scoped oracle is built from
    ``CHAIN_CONFIGS["bsc"]["tokens"]`` which lists ``WBNB`` (the ERC-20
    wrapper) but NOT the bare ``BNB`` symbol. ``LP_POOL =
    "BNB/USDT/3000"`` triggers the V4 adapter to substitute
    ``address(0)`` for the wrapped-native WBNB address at the pool key
    layer, but it still reads ``price_oracle.get(token0_symbol.upper())``
    (= ``"BNB"``) when the on-chain ``StateView.getSlot0`` query reverts
    (BNB's deployed StateView only exposes the ``bytes32`` overload;
    the SDK calls the tuple overload). Without a ``BNB`` price the
    fallback degrades to the tick-range midpoint, which is wildly off
    from the real ~$673 USDT/BNB price and produces a one-sided
    position that fails the bilateral COLLECT_FEES delta check.

    BNB and WBNB have the same USD price by construction (1:1 wrap),
    so reusing the WBNB price is correct and preserves the
    session-scoped invariant. Matches the LP_CLOSE sibling (VIB-4372)
    helper.
    """
    augmented = dict(price_oracle)
    if "BNB" not in augmented and "WBNB" in augmented:
        augmented["BNB"] = augmented["WBNB"]
    return augmented


def _clear_eip7702_delegation_if_present(web3: Web3, wallet: str) -> None:
    """Clear the EIP-7702 SetCode delegation on the Anvil account #0.

    BNB-mainnet-state artifact: ``TEST_WALLET = 0xf39F...`` is Anvil's
    first account, which on BNB mainnet has signed an EIP-7702 SetCode
    delegation (``0xef0100<delegate>``) pointing at a forwarder contract
    that transfers all incoming native BNB out to an external address.
    The Anvil fork inherits this code, so any native BNB returned by
    ``TAKE_PAIR`` during COLLECT_FEES lands in the wallet and is
    immediately forwarded — ``web3.eth.get_balance(funded_wallet)``
    shows 0 delta and the bilateral assertion below silently fails.
    Clearing the code on the fork is the correct test-time fix: an EOA
    without delegation is exactly what user wallets look like in
    production.

    No effect on Avalanche / Polygon / Optimism / Base / Arbitrum
    because ``0xf39F...`` has no code on those chains. Mirrors the
    LP_CLOSE sibling (VIB-4372) helper.
    """
    if Web3.to_checksum_address(wallet) != Web3.to_checksum_address(
        "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    ):
        return
    existing_code = web3.eth.get_code(Web3.to_checksum_address(wallet))
    # Compare the raw byte prefix (``b"\xef\x01\x00"``) rather than
    # ``.hex().startswith("ef0100")``: ``HexBytes.hex()`` returns the
    # unprefixed hex string on hexbytes>=1.0 but the 0x-prefixed form
    # on older releases, so a string-prefix check is version-fragile.
    # Bytes-level comparison is unambiguous and prefix-independent.
    if bytes(existing_code[:3]) == b"\xef\x01\x00":
        web3.provider.make_request(
            "anvil_setCode",
            [wallet, "0x"],
        )
        print(
            f"Cleared EIP-7702 delegation on {wallet}: "
            f"0x{existing_code.hex().removeprefix('0x')} -> 0x"
        )


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
        amount0=LP_AMOUNT_BNB,
        amount1=LP_AMOUNT_USDT,
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
    """Run a USDT -> WBNB swap through the V4 connector to accrue fees.

    Single direction USDT -> WBNB SwapIntent routed via the V4
    UniversalRouter. The SDK routes the swap through the
    ``(NATIVE_BNB, USDT, 3000, 60, 0x0)`` pool key (V4's swap path
    always remaps WBNB -> native BNB), which is the SAME pool key
    LP_OPEN used (the adapter's ``default_fee_tier = 3000`` matches
    our LP's fee=3000). The position then accrues fees in USDT (the
    input token of the swap), which surface as a PoolManager -> wallet
    Transfer event during the subsequent COLLECT_FEES.

    Single-direction (not round-trip) mirrors the Avalanche VIB-4369
    and Polygon VIB-4365 sibling pattern -- the USDT-side fee leg
    alone produces a verifiable fee-accrual signal end-to-end.

    Returns ``True`` if the swap compiled AND executed successfully,
    ``False`` otherwise.
    """
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
    )

    swap_intent = SwapIntent(
        from_token=SWAP_TOKEN_STABLE_SYMBOL,
        to_token=SWAP_TOKEN_NATIVE_SYMBOL,
        amount=COUNTER_SWAP_USDT,
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
# CollectFeesIntent Tests -- Uniswap V4 on BNB Chain
# =============================================================================


@pytest.mark.bsc
@pytest.mark.lp
class TestUniswapV4CollectFeesIntent:
    """Test Uniswap V4 LP_COLLECT_FEES using CollectFeesIntent on BNB Chain.

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
    @pytest.mark.xfail(
        reason="VIB-4426 V0 (PR #2335) rejects native-ETH V4 pools via the T06 adapter guard at test setup; native-BNB currency0 support is V1 work (VIB-4483 / P-V1-B). as of 2026-05-17.",
        strict=True,
    )
    async def test_collect_fees_bnb_usdt(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Collect fees from a BNB/USDT LP position via V4 on BNB Chain.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle.
        2. Execution: ExecutionOrchestrator -> success.
        3. Receipt Parsing: ModifyLiquidity event with ``liquidity_delta == 0``
           (fees-only path; principal is NOT removed). USDT fee amount
           surfaces as a PoolManager -> wallet Transfer; native BNB fee
           is the native balance delta (TAKE flows native BNB directly
           with no Transfer event). The parser MUST surface at least
           one positive PoolManager -> wallet Transfer for the USDT leg
           specifically (``usdt_fees_from_transfers > 0``).
        4. Balance Deltas: Wallet gains ONLY the fee amounts (USDT delta
           equals the parsed Transfer amount exactly; native BNB delta
           net of gas equals fee0), on-chain position liquidity is
           unchanged, and at least one side of the position accrued
           strictly positive fees (bilateral no-op guard).

        Pool selection: V4's swap router always remaps WBNB -> native
        BNB at the pool key (see ``UniswapV4SDK.build_swap_tx``); LP_OPEN
        with ``BNB/USDT/3000`` resolves currency0 to ``address(0)`` via
        ``_resolve_token(..., for_v4_pool=True)``, so LP and swap share
        the SAME ``(NATIVE_BNB, USDT, 3000, 60, 0x0)`` pool key and the
        position genuinely accrues fees from the counter-swap. The
        ERC20-keyed ``WBNB/USDT/3000`` pool path would fall foul of the
        VIB-4413 ERC20<>ERC20 revert.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]

        usdt_decimals = get_token_decimals(web3, usdt_addr)
        # Native BNB has 18 decimals (matches WBNB). ``web3.eth.get_balance``
        # returns the raw native balance in wei.
        bnb_decimals = 18

        # Augment the session oracle with a BNB -> price entry derived
        # from WBNB. See ``_augment_oracle_with_bnb`` for the full
        # rationale (BNB StateView only exposes the bytes32 form of
        # getSlot0, so the V4 adapter degrades to oracle fallback).
        prices_with_native = _augment_oracle_with_bnb(price_oracle)

        # EIP-7702 delegation cleanup (BNB-mainnet-state artifact). See
        # ``_clear_eip7702_delegation_if_present`` and the module docstring
        # for full context. Must run BEFORE LP_OPEN so that the
        # COLLECT_FEES native-BNB payout actually lands in the wallet's
        # balance instead of being silently forwarded.
        _clear_eip7702_delegation_if_present(web3, funded_wallet)

        # Fail-fast funding check: surface infra/fixture funding regressions
        # before LP_OPEN / counter-swap runs and produces a less-actionable error.
        bnb_before_setup = web3.eth.get_balance(funded_wallet)
        usdt_before_setup = get_token_balance(web3, usdt_addr, funded_wallet)
        required_usdt = int((LP_AMOUNT_USDT + COUNTER_SWAP_USDT) * (10 ** usdt_decimals))
        # Native BNB budget: LP deposit + gas headroom (counter-swap
        # is USDT -> WBNB so it does not spend wallet's native BNB;
        # the wallet receives WBNB output, not native, since the swap
        # symbol is WBNB).
        required_bnb = int(
            (LP_AMOUNT_BNB + Decimal("2")) * (10**bnb_decimals)
        )
        assert bnb_before_setup >= required_bnb, (
            f"Insufficient native BNB funding for test setup: "
            f"have={bnb_before_setup}, need>={required_bnb}"
        )
        assert usdt_before_setup >= required_usdt, (
            f"Insufficient USDT funding for test setup: "
            f"have={usdt_before_setup}, need>={required_usdt}"
        )

        print(f"\n{'=' * 80}")
        print("Test: LP_COLLECT_FEES BNB/USDT via Uniswap V4 on BNB Chain")
        print(f"{'=' * 80}")

        # Setup: Open a position first
        print("\n--- Setup: Opening LP position ---")
        position_id, liquidity_before, currency0, currency1 = await _open_v4_position(
            web3, funded_wallet, orchestrator, prices_with_native,
        )
        print(f"Opened position: id={position_id}, liquidity={liquidity_before}")
        print(f"Currencies: {currency0[:10]}.../{currency1[:10]}...")

        # Verify the LP pool key is the native-BNB pool (currency0 must
        # be address(0)). If LP_OPEN ever shifted to a non-native pool
        # key, the same-pool fee-accrual invariant below would silently
        # break -- and the counter-swap (which always routes through the
        # native pool key) would generate fees on a DIFFERENT pool than
        # the one we're collecting from.
        assert int(currency0, 16) == 0, (
            f"LP_OPEN must use native BNB as currency0 so swap and LP "
            f"share the same V4 pool key (currency0=0x0). Got: {currency0}"
        )
        assert currency1.lower() == usdt_addr.lower(), (
            f"LP_OPEN currency1 must be USDT. Got: {currency1}"
        )

        # Fee generation: a single USDT -> WBNB swap through the SAME
        # pool key as the LP position. ``UniswapV4SDK.build_swap_tx``
        # always remaps WBNB -> native BNB at the pool layer, so by
        # using the "WBNB" symbol (the wrapped native), the swap routes
        # through the same (NATIVE_BNB, USDT, fee=3000, tickSpacing=60,
        # hooks=0x0) pool as LP_OPEN (the adapter's default_fee_tier
        # is 3000, matching our LP). The LP accrues fees in USDT (the
        # swap's input token), which surface as a PoolManager -> wallet
        # Transfer during COLLECT_FEES.
        print("\n--- Counter-swap to accrue fees (USDT -> WBNB) ---")
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
        # Native BNB is currency0 of the LP pool; USDT is currency1.
        # ``web3.eth.get_balance`` returns the native BNB balance; gas
        # spent by COLLECT_FEES is accounted for explicitly below.
        bnb_before = web3.eth.get_balance(funded_wallet)
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)

        print("\n--- Collecting fees ---")
        print(f"BNB before:  {format_token_amount(bnb_before, bnb_decimals)}")
        print(f"USDT before: {format_token_amount(usdt_before, usdt_decimals)}")

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
        #   * USDT: a Transfer event from the PoolManager to the wallet
        #     carrying the fee amount.
        #   * Native BNB (currency0): NO Transfer event -- BNB moves
        #     as msg.value-style flow from the PoolManager. The wallet's
        #     native balance delta is the authoritative fee0 measurement.
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        saw_zero_delta_modify_liquidity = False
        usdt_fees_from_transfers = 0
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
            # native-BNB balance delta.
            gas_used = receipt_dict.get("gasUsed") or receipt_dict.get("gas_used") or 0
            gas_price = receipt_dict.get("effectiveGasPrice")
            if gas_price is None:
                gas_price = receipt_dict.get("effective_gas_price")
            # BNB Chain supports EIP-1559 since the Pascal hardfork (2024).
            # ``effectiveGasPrice`` is always present. Fail loudly if it
            # is missing -- it signals an unexpected receipt shape (e.g.
            # a non-1559 tx type or a custom provider transform), which
            # would silently inflate ``bnb_fees_received`` and let a
            # COLLECT_FEES no-op slip through the bilateral assertion
            # below.
            assert gas_price is not None, (
                f"effectiveGasPrice missing from BNB Chain receipt -- "
                f"tx={tx_result.tx_hash}. BNB Chain supports EIP-1559 "
                f"since the Pascal hardfork; absence indicates a "
                f"receipt-shape regression."
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
                    if transfer.token.lower() == usdt_addr.lower():
                        usdt_fees_from_transfers += transfer.amount

            close_data = parser.extract_lp_close_data(receipt_dict)
            if close_data is not None:
                lp_close_data = close_data

        assert saw_zero_delta_modify_liquidity, (
            "V4 LP_COLLECT_FEES must emit a ModifyLiquidity event with "
            "liquidity_delta == 0 (fees-only path, no principal removed)."
        )

        # Layer 3 fee-positivity assertion: the parsed receipt must
        # independently prove fee movement on the USDT side specifically
        # (the BNB side has no Transfer event since native value flows
        # from PoolManager without an ERC-20 log — that side is asserted
        # via the native balance delta below). Asserting on
        # ``usdt_fees_from_transfers`` rather than a token-agnostic
        # aggregate of all PoolManager -> wallet transfers prevents a
        # stray non-USDT transfer from satisfying the check when the
        # intended USDT fee leg is absent. Catches receipt-parser
        # regressions before Layer 4 has a chance to mask them via the
        # OR-bilateral guard. Matches the Avalanche VIB-4369 final form
        # referenced in the ticket.
        assert usdt_fees_from_transfers > 0, (
            "V4 LP_COLLECT_FEES receipt parser must surface at least one "
            "positive PoolManager -> wallet Transfer event for the USDT "
            "fee leg. Got 0 — parser regression or no fees accrued in "
            "the counter-swap setup."
        )

        # Layer 4: Balance Deltas + position-liquidity invariant.
        bnb_after = web3.eth.get_balance(funded_wallet)
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)

        # BNB delta excludes gas (gas was deducted from the wallet's BNB).
        bnb_fees_received = (bnb_after - bnb_before) + gas_spent_wei
        usdt_delta = usdt_after - usdt_before

        print("\n--- Balance Deltas ---")
        print(f"BNB fees (net of gas): {format_token_amount(bnb_fees_received, bnb_decimals)}")
        print(f"USDT delta:            {format_token_amount(usdt_delta, usdt_decimals)}")

        # Wallet MUST NOT lose tokens (net of gas) from a fee collection
        # (fees-only path). The wallet may pay gas in native BNB but the
        # COLLECT step itself can only return value.
        assert bnb_fees_received >= 0, (
            f"Native BNB fee0 must be >= 0 after netting out gas. "
            f"bnb_delta+gas={bnb_fees_received}"
        )
        assert usdt_delta >= 0, "USDT fee1 must not decrease from fee collection"

        # USDT wallet delta MUST equal parsed Transfer amount exactly --
        # COLLECT_FEES routes USDT directly to the wallet (no unwrap), so
        # any USDT the wallet sees came from a PoolManager -> wallet
        # Transfer event the parser surfaced. This is the strict
        # "fees-vs-principal" separation for the ERC-20 leg.
        assert usdt_delta == usdt_fees_from_transfers, (
            f"USDT wallet delta must equal sum of PoolManager Transfer "
            f"amounts. delta={usdt_delta}, transfers="
            f"{usdt_fees_from_transfers}"
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
        assert bnb_fees_received > 0 or usdt_fees_from_transfers > 0, (
            f"Counter-swap was confirmed to route through the LP_OPEN "
            f"pool key, so the LP position MUST have accrued fees. "
            f"Got BNB={bnb_fees_received}, USDT={usdt_fees_from_transfers}."
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
            f"BNB={bnb_fees_received}, USDT={usdt_delta}"
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
        # (`bnb/usdt/3000`) — the LP_COLLECT_FEES event is dropped
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
            # (CodeRabbit PR #2369). token0 = native BNB, token1 = USDT.
            assert Decimal(collect_payload["amount0"]) == (Decimal(bnb_fees_received) / Decimal(10**bnb_decimals))
            assert Decimal(collect_payload["amount1"]) == (Decimal(usdt_delta) / Decimal(10**usdt_decimals))
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
        native BNB and USDT around ``compiler.compile(...)`` and asserting
        both balances are unchanged after the failed compilation (matches the
        sibling pattern in the optimism / polygon / avalanche V4 collect_fees
        tests).
        """
        print(f"\n{'=' * 80}")
        print("Test: COLLECT_FEES without position_id (should fail)")
        print(f"{'=' * 80}")

        # Snapshot balances BEFORE compilation so we can assert conservation
        # after the compile-time failure (no transaction should be sent).
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdt_addr = tokens["USDT"]
        bnb_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)

        prices_with_native = _augment_oracle_with_bnb(price_oracle)

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
        bnb_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        assert bnb_after == bnb_before, (
            f"Native BNB balance must be unchanged after compile-time failure. "
            f"before={bnb_before}, after={bnb_after}"
        )
        assert usdt_after == usdt_before, (
            f"USDT balance must be unchanged after compile-time failure. "
            f"before={usdt_before}, after={usdt_after}"
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
