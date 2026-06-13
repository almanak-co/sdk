"""4-layer intent tests for Uniswap V4 LP_CLOSE on Avalanche Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
closing V4 LP positions via PositionManager on Avalanche:
1. Open an AVAX/USDC LP position (LP_OPEN as setup -- ``AVAX`` symbol so
   currency0 resolves to ``address(0)`` via
   ``UniswapV4Adapter._resolve_token(for_v4_pool=True)``, matching the
   single initialized V4 pool key on Avalanche)
2. Create LPCloseIntent with position_id and protocol_params
3. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
4. Execute via ExecutionOrchestrator (full production pipeline)
5. Parse receipts using UniswapV4ReceiptParser (liquidity removed,
   tokens returned)
6. Verify bilateral balance deltas: native AVAX (net of gas) and USDC
   both strictly positive

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

Pool selection: ``AVAX/USDC/3000``. VIB-4366's on-chain probe (verified
2026-05-14 against PoolManager 0x06380C0e... on Avalanche) confirmed that
the ``(NATIVE_AVAX, USDC, 3000, 60, 0x0)`` pool is initialized with
sqrtPriceX96 ~= 2.477e23 (tick=-253527, price ~$9.77 USDC/AVAX) and
liquidity ~= 1.47e13 — sufficient for a small two-sided LP position.
The WAVAX-keyed pool is NOT a separate venue here; using ``AVAX`` makes
``_resolve_token(for_v4_pool=True)`` substitute the wrapped native to
``address(0)`` so the LP routes through the only initialized pool.
Also avoids the VIB-4413 UR-mediated ERC-20<>ERC-20 revert that affects
the WAVAX/USDC swap path (LP uses PositionManager, not UniversalRouter,
but matching the swap test's pool key keeps the cross-test invariant
intact).

To run:
    uv run pytest tests/intents/avalanche/test_uniswap_v4_lp_close.py -v -s
"""

import json
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType, LPCloseIntent, LPOpenIntent
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_accounting_persisted,
    assert_no_accounting_on_failure,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "avalanche"

# AVAX/USDC pool with 0.3% fee tier. Token-ordering by address on Avalanche
# resolves the wrapped/native-native pair as:
#   NATIVE  (0x0000000000000000000000000000000000000000)  -- currency0
#   USDC    (0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E)  -- currency1
# The ``AVAX`` symbol triggers ``_resolve_token(for_v4_pool=True)`` to
# substitute ``address(0)`` for the wrapped-native AVAX address, matching
# the only initialized V4 venue on the fork (see module docstring).
LP_POOL = "AVAX/USDC/3000"

# Small amounts for setup LP_OPEN. AVAX trades ~$9.77 at fork time so
# 1 AVAX ≈ 10 USDC; using 1 AVAX / 10 USDC keeps capital small while
# being well above pool dust thresholds for the 1.47e13 liquidity venue.
LP_AMOUNT_AVAX = Decimal("1")
LP_AMOUNT_USDC = Decimal("10")

# Wide price range in USDC-per-AVAX terms to ensure both tokens are
# deposited at the current ~$9.77 price.
# range_lower=5   -> AVAX at $5
# range_upper=500 -> AVAX at $500 (matches the avalanche V3 LP golden)
LP_RANGE_LOWER = Decimal("5")
LP_RANGE_UPPER = Decimal("500")


# =============================================================================
# Helper: oracle augmentation
# =============================================================================


def _augment_oracle_with_avax(
    price_oracle: dict[str, Decimal],
) -> dict[str, Decimal]:
    """Return a copy of the session oracle with an ``AVAX`` entry.

    The session-scoped oracle is built from
    ``CHAIN_CONFIGS["avalanche"]["tokens"]`` which lists ``WAVAX`` (the
    ERC-20 wrapper) but NOT the bare ``AVAX`` symbol. ``LP_POOL =
    "AVAX/USDC/3000"`` triggers the V4 adapter to substitute
    ``address(0)`` for the wrapped-native AVAX address at the pool key
    layer, but it still reads ``price_oracle.get(token0_symbol.upper())``
    (= ``"AVAX"``) when the on-chain StateView.getSlot0 query reverts
    (Avalanche's deployed StateView only exposes the ``bytes32`` overload;
    the SDK calls the tuple overload). Without an ``AVAX`` price the
    fallback degrades to the tick-range midpoint, which is wildly off
    from the real ~$9.77 USDC/AVAX price and produces a one-sided
    position that fails the bilateral LP_CLOSE delta check.

    AVAX and WAVAX have the same USD price by construction (1:1 wrap),
    so reusing the WAVAX price is correct and preserves the
    session-scoped invariant.
    """
    augmented = dict(price_oracle)
    if "AVAX" not in augmented and "WAVAX" in augmented:
        augmented["AVAX"] = augmented["WAVAX"]
    return augmented


# =============================================================================
# Helper: Open a position (setup for close tests)
# =============================================================================


async def _open_v4_position(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> tuple[int, int, str, str]:
    """Open a V4 LP position and return ``(position_id, liquidity, currency0, currency1)``.

    Self-sufficient setup that mirrors the polygon / optimism / base / arbitrum
    / ethereum LP_CLOSE goldens so VIB-4368 can land without depending on
    VIB-4367's parallel LP_OPEN file. Uses the ``AVAX/USDC/3000`` native-key
    pool (see ``LP_POOL`` comment) which matches the only initialized V4
    venue on Avalanche.

    Raises AssertionError if the setup LP_OPEN fails.
    """
    intent = LPOpenIntent(
        pool=LP_POOL,
        amount0=LP_AMOUNT_AVAX,
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
    assert liquidity is not None and liquidity > 0, "Setup LP_OPEN must yield positive liquidity"

    # Get currency addresses from bundle metadata
    token0 = bundle.metadata.get("token0", {})
    token1 = bundle.metadata.get("token1", {})
    currency0 = token0.get("address", "")
    currency1 = token1.get("address", "")

    assert currency0 and currency1, "Must extract currency addresses from bundle metadata"

    return position_id, liquidity, currency0, currency1


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


def _assert_v4_open_position_hash(payload: dict) -> None:
    """The reused close-basis LP_OPEN row must carry the V4 anchor (VIB-4473).

    This close path reuses the setup ``LP_OPEN`` accounting row as the
    lot-matching basis, so it must verify that row actually persisted the
    V4 ``position_hash`` anchor — otherwise the close test could pass
    without ever covering the LP_OPEN-side accounting regression once the
    outer xfail is lifted. Gap-aware: encodes the TRUE current behavior via
    a runtime xfail that fires ONLY on the exact ``position_hash is None``
    signature and auto-reactivates the hard asserts the moment VIB-4636
    lands (same pattern as the merged VIB-4633/4634/4635 gap encodings).
    """
    ph = payload["position_hash"]
    if ph is None:
        pytest.xfail(
            "VIB-4636: V4 LP_OPEN position_hash anchor (VIB-4473) is not "
            "persisted onto the accounting_events payload — enrichment path "
            "drops the mint-sourced lp_open_data. On-chain LP_OPEN verified "
            "correct above (amounts/pool/ticks/confidence hard-asserted)."
        )
    # Reactivates automatically once VIB-4636 wires position_hash through.
    assert isinstance(ph, str) and ph.startswith("0x"), (
        f"V4 position_hash must be 0x-prefixed hex, got {ph!r}"
    )
    assert len(ph) == 66, f"V4 position_hash must be a 32-byte keccak hash, got {ph!r}"


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


async def _open_v4_position_with_accounting(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    *,
    harness,
    eth_call_reader,
) -> tuple[int, int, str, str, dict]:
    """Open a V4 LP position AND persist the LP_OPEN through Layer 5.

    Returns ``(position_id, liquidity, currency0, currency1,
    open_accounting_row)``. The persisted OPEN seeds the cost basis the
    subsequent LP_CLOSE links against (epic VIB-4591 decisions #4/#5).
    """
    intent = LPOpenIntent(
        pool=LP_POOL,
        amount0=LP_AMOUNT_AVAX,
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
    assert liquidity is not None and liquidity > 0, "Setup LP_OPEN must yield positive liquidity"

    token0 = bundle.metadata.get("token0", {})
    token1 = bundle.metadata.get("token1", {})
    currency0 = token0.get("address", "")
    currency1 = token1.get("address", "")
    assert currency0 and currency1, "Must extract currency addresses from bundle metadata"

    open_accounting_row = await assert_accounting_persisted(
        harness,
        intent=intent,
        result=execution_result,
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        expected_event_type="LP_OPEN",
        price_oracle=price_oracle,
        eth_call_reader=eth_call_reader,
    )
    _assert_identity(open_accounting_row, event_type="LP_OPEN", wallet=funded_wallet)
    # Verify the reused close-basis row carries the V4 lot-matching anchor
    # (gap-aware: xfails on the VIB-4636 signature, auto-reactivates on fix).
    _assert_v4_open_position_hash(_payload(open_accounting_row))

    return position_id, liquidity, currency0, currency1, open_accounting_row


# =============================================================================
# LPCloseIntent Tests -- Uniswap V4 on Avalanche
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.lp
class TestUniswapV4LPCloseIntent:
    """Test Uniswap V4 LP_CLOSE using LPCloseIntent on Avalanche.

    These tests verify the full LP close flow:
    - First open a position (setup) on the native AVAX/USDC pool
    - LPCloseIntent creation with position_id and protocol_params
    - IntentCompiler routes to UniswapV4Adapter.compile_lp_close_intent()
    - Transactions execute successfully on-chain via PositionManager
    - UniswapV4ReceiptParser correctly extracts close data
    - Balance changes match expected token returns (native AVAX net of gas,
      plus USDC), both strictly positive
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-4426 V0 (PR #2335) rejects native-ETH V4 pools via the T06 adapter guard; native-ETH currency0 support is V1 work (VIB-4483 / P-V1-B). as of 2026-05-17.",
        strict=True,
    )
    async def test_lp_close_avax_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test full LP_OPEN -> LP_CLOSE lifecycle for AVAX/USDC via V4 on Avalanche.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> lp_close_data extracted
        4. Balance Deltas: native AVAX (net of gas) and USDC both returned
           from the pool (principal + fees)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        usdc_decimals = get_token_decimals(web3, usdc_addr)
        # Native AVAX has 18 decimals (matches WAVAX). ``web3.eth.get_balance``
        # returns the raw native balance in wei.
        avax_decimals = 18

        # Fail-fast funding check: surface infra/fixture funding regressions
        # before LP_OPEN runs and produces a less-actionable error.
        # The wallet pays gas in native AVAX, so we need headroom above the
        # raw LP amount. The avalanche conftest seeds the EOA with 100 AVAX
        # which is well above this floor.
        avax_available = web3.eth.get_balance(funded_wallet)
        usdc_available = get_token_balance(web3, usdc_addr, funded_wallet)
        avax_required = int(
            (LP_AMOUNT_AVAX + Decimal("0.5")) * (Decimal(10) ** avax_decimals)
        )
        usdc_required = int(LP_AMOUNT_USDC * (Decimal(10) ** usdc_decimals))
        assert avax_available >= avax_required, (
            f"Insufficient native AVAX funding for setup LP_OPEN: "
            f"have={avax_available}, need>={avax_required}"
        )
        assert usdc_available >= usdc_required, (
            f"Insufficient USDC funding for setup LP_OPEN: "
            f"have={usdc_available}, need>={usdc_required}"
        )

        print(f"\n{'=' * 80}")
        print("Test: LP_CLOSE AVAX/USDC via Uniswap V4 on Avalanche")
        print(f"{'=' * 80}")

        # Augment the session oracle with an AVAX -> price entry derived
        # from WAVAX. See ``_augment_oracle_with_avax`` for the full
        # rationale (Avalanche StateView only exposes the bytes32 form
        # of getSlot0, so the V4 adapter degrades to oracle fallback).
        augmented_oracle = _augment_oracle_with_avax(price_oracle)

        # Setup: Open a position first (and persist its LP_OPEN through
        # Layer 5 so the LP_CLOSE below has a prior OPEN to link basis to).
        print("\n--- Setup: Opening LP position ---")
        (
            position_id,
            liquidity,
            currency0,
            currency1,
            open_accounting_row,
        ) = await _open_v4_position_with_accounting(
            web3,
            funded_wallet,
            orchestrator,
            augmented_oracle,
            harness=layer5_accounting_harness,
            eth_call_reader=anvil_eth_call_adapter,
        )
        print(f"Opened position: id={position_id}, liquidity={liquidity}")
        print(f"Currencies: {currency0[:10]}.../{currency1[:10]}...")

        # Verify the LP pool key is the native-AVAX pool (currency0 must be
        # address(0)). If LP_OPEN ever shifted to a non-native pool key, the
        # bilateral close-side delta check below would silently break.
        assert int(currency0, 16) == 0, (
            f"LP_OPEN must use native AVAX as currency0 so the LP routes "
            f"through the only initialized V4 venue on Avalanche "
            f"(currency0=0x0). Got: {currency0}"
        )
        assert currency1.lower() == usdc_addr.lower(), (
            f"LP_OPEN currency1 must be USDC. Got: {currency1}"
        )

        # Record balances BEFORE the close.
        # Native AVAX is currency0 of the LP pool; USDC is currency1.
        # ``web3.eth.get_balance`` returns the native AVAX balance; the gas
        # spent on the LP_CLOSE tx itself is accounted for explicitly below.
        avax_before = web3.eth.get_balance(funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        print("\n--- Closing LP position ---")
        print(f"AVAX before close: {format_token_amount(avax_before, avax_decimals)}")
        print(f"USDC before close: {format_token_amount(usdc_before, usdc_decimals)}")

        # Layer 1: Compilation
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            protocol_params={
                "liquidity": liquidity,
                "currency0": currency0,
                "currency1": currency1,
            },
        )

        print(f"Created LPCloseIntent: position_id={close_intent.position_id}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=augmented_oracle,
        )

        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"LP_CLOSE compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        bundle = compilation_result.action_bundle
        print(f"ActionBundle created with {len(bundle.transactions)} transactions")

        # Layer 2: Execution
        print("\nExecuting LP_CLOSE via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(bundle)

        assert execution_result.success, f"LP_CLOSE execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Receipt Parsing
        # Track gas spent on the close txs so we can isolate the native AVAX
        # principal/fees from the native gas burn in the Layer 4 delta check.
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        lp_close_data = None
        gas_spent_wei = 0

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if not tx_result.receipt:
                continue

            receipt_dict = tx_result.receipt.to_dict()

            # Compute gas cost so we can isolate native AVAX returns from
            # the native gas burn in the Layer 4 delta below.
            gas_used = receipt_dict.get("gasUsed") or receipt_dict.get("gas_used") or 0
            gas_price = receipt_dict.get("effectiveGasPrice")
            if gas_price is None:
                gas_price = receipt_dict.get("effective_gas_price")
            # Avalanche C-Chain is EIP-1559: ``effectiveGasPrice`` is always
            # present. Fail loudly if it is missing -- it signals an
            # unexpected receipt shape, which would silently inflate the
            # ``avax_received`` net-of-gas value below and let a no-op
            # LP_CLOSE slip through the bilateral assertion.
            assert gas_price is not None, (
                f"effectiveGasPrice missing from Avalanche receipt -- "
                f"tx={tx_result.tx_hash}. Avalanche C-Chain is EIP-1559; "
                f"absence indicates a receipt-shape regression."
            )
            gas_spent_wei += int(gas_used) * int(gas_price)

            # Exercise parse_receipt() entrypoint -- this is the surface
            # ResultEnricher consumes in production via extract_lp_amounts,
            # so the intent-test contract requires calling it here
            # (.claude/rules/intent-tests.md Layer 3).
            parser.parse_receipt(receipt_dict)
            close_data = parser.extract_lp_close_data(receipt_dict)
            if close_data is not None:
                lp_close_data = close_data
                print("  LP Close Data:")
                print(f"    amount0_collected: {close_data.amount0_collected}")
                print(f"    amount1_collected: {close_data.amount1_collected}")
                print(f"    liquidity_removed: {close_data.liquidity_removed}")

        assert lp_close_data is not None, "Must extract LP close data from receipt"
        assert lp_close_data.liquidity_removed is not None and lp_close_data.liquidity_removed > 0, (
            "Must remove positive liquidity"
        )
        # Parser MUST report a positive amount on the ERC-20 side (USDC).
        # On a native-key V4 pool (currency0 = address(0)), the native AVAX
        # leg flows out of the PoolManager WITHOUT a Transfer event -- the
        # parser sums tokens by walking Transfer events from the
        # PoolManager, so only the USDC transfer surfaces here. Native AVAX
        # is therefore measured via the eth.get_balance delta in Layer 4
        # below, not via the parser. The parser assigns the single ERC-20
        # transfer to ``amount0_collected`` because it sorts by token
        # address and USDC is the only key present; this is a parser-naming
        # artefact, not a semantic claim about pool currency0.
        assert lp_close_data.amount0_collected is not None and lp_close_data.amount0_collected > 0, (
            "Parser must extract positive USDC collection from LP_CLOSE receipt "
            "(surfaces as amount0_collected on native-key V4 pools because USDC "
            "is the only ERC-20 transfer the parser walks)"
        )

        # Layer 4: Balance Deltas -- wallet gains native AVAX (net of gas)
        # AND USDC (principal + any fees).
        avax_after = web3.eth.get_balance(funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        # Native AVAX delta excludes gas (gas was deducted from the wallet's
        # native balance). Adding ``gas_spent_wei`` back isolates the
        # AVAX returned by TAKE_PAIR for currency0.
        avax_received = (avax_after - avax_before) + gas_spent_wei
        usdc_received = usdc_after - usdc_before

        print("\n--- Balance Deltas ---")
        print(f"AVAX received (net of gas): {format_token_amount(avax_received, avax_decimals)}")
        print(f"USDC received:              {format_token_amount(usdc_received, usdc_decimals)}")

        # MANDATORY bilateral delta (see .claude/rules/intent-tests.md and
        # #1691): the position was opened with both tokens, so closing it
        # MUST return both. Permitting `or` here would let a V4
        # one-sided-close bug pass.
        assert avax_received > 0 and usdc_received > 0, (
            f"LP_CLOSE on a two-token position must return BOTH tokens "
            f"(no-op guard). avax_received={avax_received} (net of gas), "
            f"usdc_received={usdc_received}"
        )

        # Layer 5: assert the real accounting pipeline persisted LP_CLOSE.
        close_accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=close_intent,
            result=execution_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_CLOSE",
            price_oracle=augmented_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        _assert_identity(close_accounting_row, event_type="LP_CLOSE", wallet=funded_wallet)
        close_payload = _payload(close_accounting_row)
        open_payload = _payload(open_accounting_row)
        # #4 linkage: LP_CLOSE.position_key == LP_OPEN.position_key + basis from prior OPEN.
        assert close_payload["position_key"] == open_payload["position_key"]
        _assert_no_lot_id(close_accounting_row, close_payload)
        # #2 directional null-contract: V4 close matches by position_key, so
        # position_hash stays None on LP_CLOSE (the anchor lives on LP_OPEN).
        _assert_v4_close_position_hash(close_payload)
        assert close_payload["realized_pnl_usd"] is not None, (
            "open-then-close must compute realized PnL"
        )

        # #3 parser ↔ event exact equality, matched by token IDENTITY
        # (Empty≠Zero≠None on the fee legs: V4 LPCloseData.fees0/fees1 are
        # None — fee separation is V1 VIB-4482).
        #
        # Native-key V4 pool: native AVAX (currency0=0x0) flows out of the
        # PoolManager WITHOUT a Transfer event, so the parser observes only
        # the USDC leg. ``extract_lp_close_data`` keys collected amounts by
        # PoolKey currency (``collected_by_token.get(currency0/1, 0)``), so
        # the native leg is a measured-zero and the USDC value lands on
        # whichever of amount0/1_collected corresponds to the USDC currency.
        # A positional ``payload.amount0 == parser.amount0_collected`` would
        # validate the wrong leg with the wrong decimals once the V4
        # LP_CLOSE xfail lifts (CodeRabbit, PR #2369). Resolve BOTH sides by
        # the USDC currency address and assert that single ERC-20 leg; the
        # native AVAX leg is already validated by the Layer-4 balance delta.
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_decimals_p = get_token_decimals(web3, usdc_addr)

        # Parser side: pick the (amount, fee) for the USDC currency.
        p_cur0 = (lp_close_data.currency0 or "").lower()
        p_cur1 = (lp_close_data.currency1 or "").lower()
        if p_cur0 == usdc_addr.lower():
            parser_erc20_amount = lp_close_data.amount0_collected
            parser_erc20_fee = lp_close_data.fees0
        else:
            assert p_cur1 == usdc_addr.lower(), (
                f"expected USDC on a parser currency leg, got {p_cur0}/{p_cur1}"
            )
            parser_erc20_amount = lp_close_data.amount1_collected
            parser_erc20_fee = lp_close_data.fees1

        # Payload side: pick the leg whose token symbol resolves to USDC.
        if close_payload["token0"] in tokens and tokens[close_payload["token0"]].lower() == usdc_addr.lower():
            erc20_payload_amount = Decimal(close_payload["amount0"])
            erc20_fee_raw = close_payload["fees0_collected"]
        else:
            assert (
                close_payload["token1"] in tokens
                and tokens[close_payload["token1"]].lower() == usdc_addr.lower()
            ), f"expected USDC on one payload leg, got {close_payload['token0']}/{close_payload['token1']}"
            erc20_payload_amount = Decimal(close_payload["amount1"])
            erc20_fee_raw = close_payload["fees1_collected"]

        assert erc20_payload_amount == _to_human(parser_erc20_amount, usdc_decimals_p)
        # Fee legs: V4 LPCloseData.fees{0,1} are None (Empty) — directional
        # null-contract on the ERC-20 leg the parser actually measured.
        _assert_fee_contract(
            erc20_fee_raw, _to_human(parser_erc20_fee, usdc_decimals_p), field="fees(erc20-leg)"
        )

        print(f"\nPosition {position_id} successfully closed")
        print("\nALL 5 LAYERS PASSED")

    @pytest.mark.intent(IntentType.LP_CLOSE)  # noqa: layers
    @pytest.mark.asyncio
    async def test_lp_close_without_liquidity_fails_compilation(
        self,
        web3: Web3,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test that LP_CLOSE without liquidity in protocol_params fails at compilation.

        V4 LP_CLOSE requires on-chain position data (liquidity, currencies).

        Intentional layer exception (``# noqa: layers``) -- this test stops at
        Layer 1 by design. The failure-path contract from
        ``.claude/rules/intent-tests.md`` is still honoured by snapshotting
        native AVAX and USDC around ``compiler.compile(...)`` and asserting
        both balances are unchanged after the failed compilation. Layer 5
        adds the books-side mirror: a failed LP_CLOSE writes ZERO
        accounting_events rows (epic VIB-4591 decision #7).
        """
        print(f"\n{'=' * 80}")
        print("Test: LP_CLOSE without liquidity (should fail compilation)")
        print(f"{'=' * 80}")

        # Snapshot balances BEFORE compilation so we can assert conservation
        # after the compile-time failure (no transaction should be sent).
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        avax_before = web3.eth.get_balance(funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        # Use a token id well above any minted position on Avalanche V4 at
        # fork time so the on-chain ``get_position_liquidity`` query returns
        # 0 and the compiler must fall back to the protocol_params-required
        # error path. (Matches the deliberately out-of-range value used in
        # ``tests/intents/polygon/test_uniswap_v4_lp_close.py`` and the
        # optimism / base siblings.)
        close_intent = LPCloseIntent(
            position_id="999999999999",
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            # No protocol_params -- missing liquidity
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_augment_oracle_with_avax(price_oracle),
        )

        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "FAILED", (
            "Compilation should fail without liquidity in protocol_params"
        )
        assert compilation_result.action_bundle is None, (
            "Failed compilation must not produce an ActionBundle"
        )
        assert compilation_result.error is not None, (
            "FAILED compilation must surface an error message; missing one would "
            "mask the actual failure and trip an obscure AttributeError below."
        )
        assert "liquidity" in compilation_result.error.lower(), (
            f"Error should mention liquidity requirement, got: {compilation_result.error}"
        )

        # Failure-path balance conservation: no on-chain tx fired, balances
        # unchanged. Note: native AVAX strictly equality here because no tx
        # was submitted at all -- compile-time failure means no gas was
        # spent.
        avax_after = web3.eth.get_balance(funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        assert avax_after == avax_before, (
            f"Native AVAX balance must be unchanged after compile-time "
            f"failure. before={avax_before}, after={avax_after}"
        )
        assert usdc_after == usdc_before, (
            f"USDC balance must be unchanged after compile-time failure. "
            f"before={usdc_before}, after={usdc_after}"
        )

        print(f"Compilation failed as expected: {compilation_result.error}")

        # Layer 5: a failed LP_CLOSE must write zero accounting_events rows.
        failed_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.VALIDATION,
            error=compilation_result.error or "LP_CLOSE compilation failed",
        )
        await assert_no_accounting_on_failure(
            layer5_accounting_harness,
            intent=close_intent,
            result=failed_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_augment_oracle_with_avax(price_oracle),
            eth_call_reader=anvil_eth_call_adapter,
        )
        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
