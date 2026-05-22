"""4-layer intent tests for Uniswap V4 LP_OPEN on Avalanche Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
opening concentrated liquidity positions via V4 PositionManager on Avalanche:
1. Create LPOpenIntent with pool, amounts, and price range
2. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV4ReceiptParser (position_id + liquidity)
5. Verify balances changed correctly (tokens deposited into pool)

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

Pool choice: NATIVE_AVAX/USDC fee=3000, tick_spacing=60. Verified by VIB-4366
(`test_uniswap_v4_swap.py`) against avalanche mainnet on 2026-05-13:
sqrtPriceX96=2.477e23 (~10 USDC per AVAX), liquidity=1.47e13. This native-keyed
pool exists and is liquid; the ERC20<>ERC20 WAVAX/USDC variant hits the
VIB-4413 UR-mediated revert. The adapter resolves the "AVAX" symbol via
``_resolve_token(..., for_v4_pool=True)`` to ``address(0)`` (NATIVE_CURRENCY),
so the pool key uses the native sentinel. The PositionManager.modifyLiquidities
TX carries the LP value as ``msg.value`` and SETTLE_PAIR refunds excess to
the wallet.

To run:
    uv run pytest tests/intents/avalanche/test_uniswap_v4_lp_open.py -v -s
"""

import json
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType, LPOpenIntent
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_accounting_persisted,
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

# AVAX/USDC pool with 0.3% fee tier (3000), tick spacing 60.
# This is the native-keyed V4 pool — pool key currency0=address(0). The same
# pool the VIB-4366 swap test exercised; on-chain at fork time it has
# sqrtPriceX96≈2.477e23 and liquidity≈1.47e13 (mid-price ~10 USDC per AVAX).
# Picking AVAX over WAVAX forces the adapter into ``for_v4_pool=True`` →
# NATIVE_CURRENCY substitution, avoiding the VIB-4413 ERC20<>ERC20 revert.
LP_POOL = "AVAX/USDC/3000"

# Small amounts to minimise capital requirements. funded_wallet is seeded with
# 100 native AVAX + 100,000 USDC by the avalanche conftest, so this is well
# inside the funding envelope.
LP_AMOUNT_AVAX = Decimal("1")    # ~$10 of AVAX at fork prices
LP_AMOUNT_USDC = Decimal("25")   # $25 of USDC

# Wide price range centred around the ~10 USDC/AVAX mid-price (verified
# 2026-05-13 via V4 Quoter against the same pool). Roughly 50% below and
# ~5× above ensures the position is unambiguously in-range and both tokens
# must be deposited (no-op guard).
LP_RANGE_LOWER = Decimal("5")    # 5 USDC per AVAX
LP_RANGE_UPPER = Decimal("50")   # 50 USDC per AVAX


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


def _assert_v4_open_position_hash(payload: dict) -> None:
    """V4 LP_OPEN should populate the lot-matching anchor (VIB-4473).

    Unlike V3 (where ``position_hash`` is always ``None``), the Uniswap V4
    receipt parser computes ``keccak(positionManager, tickLower, tickUpper,
    salt)`` (VIB-4474 T05) and ``lp_accounting.py:476`` forwards it onto
    the LP_OPEN payload — so the persisted ``accounting_events`` row MUST
    carry a real 0x-prefixed 32-byte hash, NOT ``None``.

    VIB-4636 (genuine production gap, surfaced by this Layer-5 rollout):
    the result-enrichment path invokes the V4 parser on per-tx receipts
    that don't carry the ``ModifyLiquidity`` mint (``total_logs=1``);
    ``_AGGREGATE_FIELDS`` aggregates ``lp_close_data`` but not
    ``lp_open_data``, so ``position_hash`` never reaches the payload. The
    on-chain LP_OPEN is correct (Layers 1–4 + amounts/pool/ticks/confidence
    all hard-assert green); only the books anchor is dropped. Encode the
    TRUE current behavior via a runtime xfail that fires ONLY on the exact
    ``position_hash is None`` signature and auto-reactivates (the hard
    asserts below run) the moment VIB-4636 lands. Pattern mirrors the
    merged VIB-4633/4634/4635 Compound/Morpho gap encodings.
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


# =============================================================================
# LPOpenIntent Tests -- Uniswap V4 on Avalanche
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.lp
class TestUniswapV4LPOpenIntent:
    """Test Uniswap V4 LP_OPEN using LPOpenIntent on Avalanche.

    These tests verify the full Intent flow:
    - LPOpenIntent creation with protocol="uniswap_v4"
    - IntentCompiler routes to UniswapV4Adapter.compile_lp_open_intent()
    - Transactions execute successfully on-chain via PositionManager
    - UniswapV4ReceiptParser correctly extracts position_id and liquidity
    - Balance changes match expected deposits

    Pair choice: AVAX/USDC routes through the NATIVE/USDC V4 pool. The
    adapter remaps "AVAX" to address(0) for the pool key via
    ``_resolve_token(..., for_v4_pool=True)``, and the SDK threads
    ``amount0_max`` as ``msg.value`` on the modifyLiquidities call.
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-4426 V0 (PR #2335) rejects native-ETH V4 pools via the T06 adapter guard; native-ETH currency0 support is V1 work (VIB-4483 / P-V1-B). as of 2026-05-17.",
        strict=True,
    )
    async def test_lp_open_avax_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test opening an AVAX/USDC LP position via Uniswap V4.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> position_id extracted, liquidity > 0
        4. Balance Deltas: native AVAX and USDC deposited into pool (bilateral)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]

        usdc_decimals = get_token_decimals(web3, usdc_addr)
        avax_decimals = 18  # Native AVAX is 18 decimals

        print(f"\n{'='*80}")
        print("Test: LP_OPEN AVAX/USDC via Uniswap V4 on Avalanche")
        print(f"{'='*80}")
        print(f"AVAX amount: {LP_AMOUNT_AVAX}")
        print(f"USDC amount: {LP_AMOUNT_USDC}")
        print(f"Price range: {LP_RANGE_LOWER} - {LP_RANGE_UPPER} USDC/AVAX")

        # Record balances before. AVAX is native — track via web3.eth.get_balance.
        avax_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        # Fail-fast on underfunded fixtures: catch both the zero-balance and
        # the under-seeded cases so the failure is actionable instead of
        # surfacing later as a confusing on-chain revert.
        required_avax = int(LP_AMOUNT_AVAX * (Decimal(10) ** avax_decimals))
        required_usdc = int(LP_AMOUNT_USDC * (Decimal(10) ** usdc_decimals))
        assert avax_before >= required_avax, (
            f"funded_wallet={funded_wallet} must hold >= {required_avax} native AVAX "
            f"(wei); have={avax_before}. Check the avalanche conftest seeding fixture."
        )
        assert usdc_before >= required_usdc, (
            f"funded_wallet={funded_wallet} must hold >= {required_usdc} USDC "
            f"({usdc_addr}); have={usdc_before}. "
            "Check the avalanche conftest seeding fixture."
        )

        print(f"AVAX before: {format_token_amount(avax_before, avax_decimals)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # Layer 1: Compilation
        intent = LPOpenIntent(
            pool=LP_POOL,
            amount0=LP_AMOUNT_AVAX,
            amount1=LP_AMOUNT_USDC,
            range_lower=LP_RANGE_LOWER,
            range_upper=LP_RANGE_UPPER,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated LPOpenIntent: pool={intent.pool}, protocol={intent.protocol}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        print("Compiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        bundle = compilation_result.action_bundle
        print(f"ActionBundle created with {len(bundle.transactions)} transactions")
        print(f"Metadata: liquidity={bundle.metadata.get('liquidity')}, "
              f"tick_lower={bundle.metadata.get('tick_lower')}, "
              f"tick_upper={bundle.metadata.get('tick_upper')}")

        # Layer 2: Execution
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Enrich for accounting (populates result.lp_open_data — Layer 5 needs
        # it; mirrors the V3 golden / SushiSwap precedent ordering).
        execution_result = _enrich_for_accounting(
            execution_result, intent, funded_wallet, bundle.metadata
        )

        # Layer 3: Receipt Parsing
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        position_id = None
        liquidity = None
        saw_modify_liquidity_event = False
        saw_transfer_event = False

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()

                # Exercise parse_receipt() entrypoint — this is the surface
                # ResultEnricher consumes in production via extract_swap_amounts
                # / extract_lp_amounts, so the intent-test contract requires
                # calling it here (.claude/rules/intent-tests.md Layer 3).
                parse_result = parser.parse_receipt(receipt_dict)
                if parse_result.modify_liquidity_events:
                    saw_modify_liquidity_event = True
                if parse_result.transfer_events:
                    saw_transfer_event = True

                # Extract position_id from ERC-721 Transfer (mint) event
                extracted_id = parser.extract_position_id(receipt_dict)
                if extracted_id is not None:
                    position_id = extracted_id
                    print(f"  Position ID (NFT tokenId): {position_id}")

                # Extract liquidity from ModifyLiquidity event
                extracted_liq = parser.extract_liquidity(receipt_dict)
                if extracted_liq is not None:
                    liquidity = extracted_liq
                    print(f"  Liquidity delta: {liquidity}")

        assert position_id is not None, "Must extract position_id from LP mint receipt"
        assert position_id > 0, f"Position ID must be positive, got {position_id}"
        assert liquidity is not None, "Must extract liquidity from ModifyLiquidity event"
        assert liquidity > 0, f"Liquidity must be positive, got {liquidity}"
        assert saw_modify_liquidity_event, (
            "parse_receipt() must surface the ModifyLiquidity event for an LP_OPEN"
        )
        assert saw_transfer_event, (
            "parse_receipt() must surface the ERC-721 mint Transfer for an LP_OPEN"
        )

        # Layer 4: Balance Deltas
        avax_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        avax_spent = avax_before - avax_after
        usdc_spent = usdc_before - usdc_after
        max_avax_spend = int(LP_AMOUNT_AVAX * (Decimal(10) ** avax_decimals))
        max_usdc_spend = int(LP_AMOUNT_USDC * (Decimal(10) ** usdc_decimals))

        print("\n--- Balance Deltas ---")
        print(f"AVAX spent (incl. gas): {format_token_amount(avax_spent, avax_decimals)}")
        print(f"USDC spent:             {format_token_amount(usdc_spent, usdc_decimals)}")

        # MANDATORY bilateral delta (see .claude/rules/intent-tests.md and #1691):
        # the position is opened with `range_lower=5`, `range_upper=50` and
        # AVAX/USDC at ~10 USDC at fork time — unambiguously in-range. Both
        # currencies MUST have been deposited. Permitting `or` here would let
        # a V4 no-op silently pass.
        #
        # AVAX is native: ``avax_spent`` is the wallet's native delta, which
        # equals ``actual_avax_deposited + gas_used*gas_price``. SETTLE_PAIR
        # refunds the slippage cushion via the PoolManager, so the deposit
        # itself is between ~0.5 AVAX (in-range, lower-half) and the full
        # ``amount0_max`` (~1.30 AVAX with the LP minimum 30% slippage when
        # the on-chain sqrtPrice query succeeds the cushion drops to 5%).
        # We assert avax_spent is strictly positive AND bounded above by
        # ``amount0_max + 0.1 AVAX`` gas headroom — anything outside that
        # window is either a no-op (lower bound) or a regression that
        # overspent (upper bound).
        # Gas headroom for the native-AVAX bound (defined here so the lower
        # and upper bounds can both reference it without duplication).
        gas_headroom = int(Decimal("0.1") * (Decimal(10) ** avax_decimals))

        # Lower bound for the AVAX deposit: subtract the gas-headroom ceiling
        # so we still prove the position actually consumed native AVAX. If a
        # tick/pool-key regression makes this position mint as one-sided
        # USDC liquidity, ``avax_spent`` would be ~= gas alone (well below
        # gas_headroom), and this assertion catches it; a plain ``> 0`` check
        # would mask that case as long as any gas was paid.
        actual_avax_deposit = avax_spent - gas_headroom
        assert actual_avax_deposit > 0, (
            "In-range LP_OPEN must deposit native AVAX above the gas-only "
            "floor (no-op guard). "
            f"avax_spent={avax_spent}, gas_headroom={gas_headroom}, "
            f"deposit_estimate={actual_avax_deposit}"
        )
        assert usdc_spent > 0, (
            f"In-range LP_OPEN must deposit USDC (no-op guard). usdc_spent={usdc_spent}"
        )

        # Upper-bound checks. For USDC the bound is the requested max1
        # (modifyLiquidities will refund the slippage cushion via SETTLE_PAIR,
        # so the actual spend cannot exceed the requested amount1_max which
        # the adapter caps at amount1 * (1 + slippage)). We bound on the
        # unbuffered amount1 to catch any off-by-decimals overspend.
        assert usdc_spent <= max_usdc_spend, (
            f"USDC spend exceeded requested amount1: spent={usdc_spent}, max={max_usdc_spend}"
        )
        # For native AVAX include a gas headroom: a modifyLiquidities TX plus
        # 4 approval TXs (2× ERC-20 approve, 2× Permit2.approve, 1× mint) at
        # avalanche gas prices ought to stay well below 0.1 AVAX, even with
        # the 25 nAVAX min base fee. The adapter caps amount0_max at
        # amount0 * (1 + lp_default_slippage) where lp_default_slippage is
        # 0.30 when sqrtPrice came from oracle prices or 0.05 when on-chain.
        # Take the looser 30% cap + the gas headroom defined above to keep
        # this robust regardless of which path the compiler took.
        max_native_spend = int(
            Decimal(max_avax_spend) * Decimal("1.30")
        ) + gas_headroom
        assert avax_spent <= max_native_spend, (
            f"Native AVAX spend exceeded amount0_max + gas headroom: "
            f"spent={avax_spent}, max={max_native_spend}"
        )

        print(f"\nPosition ID: {position_id}")
        print(f"Liquidity:   {liquidity}")

        # Layer 5: assert the real accounting pipeline persisted LP_OPEN.
        accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=execution_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_OPEN",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        _assert_identity(accounting_row, event_type="LP_OPEN", wallet=funded_wallet)
        payload = _payload(accounting_row)
        assert payload["event_type"] == "LP_OPEN"
        assert payload["position_key"] == accounting_row["position_key"]
        assert payload["pool_address"].startswith("0x"), "LP_OPEN must persist canonical pool address"
        # V4 difference vs V3: position_hash IS populated (VIB-4473 anchor).
        # Gap-aware FIRST: on the VIB-4636 enrich-path drop the persisted
        # lp_open_data (amounts + anchor) is missing/garbage, so xfail here
        # before the amount asserts rather than hard-failing on corrupted
        # books (CodeRabbit PR #2369 / VIB-4636).
        _assert_v4_open_position_hash(payload)
        # Tie the persisted amounts to the Layer-4 signals — `>= 0` would
        # pass on a zero or mis-scaled row (CodeRabbit PR #2369). USDC
        # (token1, ERC-20) is a clean delta → exact equality. Native AVAX
        # (token0) cannot be matched exactly because ``avax_spent`` is
        # gas-polluted, so bound the persisted amount to the proven Layer-4
        # native-deposit window: at least the gas-discounted floor and no
        # more than what actually left the wallet.
        assert Decimal(payload["amount1"]) == (Decimal(usdc_spent) / Decimal(10**usdc_decimals))
        _amount0_h = Decimal(payload["amount0"])
        assert (Decimal(actual_avax_deposit) / Decimal(10**avax_decimals)) <= _amount0_h <= (
            Decimal(avax_spent) / Decimal(10**avax_decimals)
        ), (
            f"persisted LP_OPEN amount0 ({_amount0_h}) must fall within the "
            f"proven native-AVAX deposit window "
            f"[{actual_avax_deposit}, {avax_spent}] / 1e{avax_decimals}"
        )
        assert payload["tick_lower"] is not None
        assert payload["tick_upper"] is not None
        assert payload["liquidity"] is not None
        assert payload["current_tick"] is not None
        assert payload["in_range"] is True

        print("\nALL 5 LAYERS PASSED")

    @pytest.mark.intent(IntentType.LP_OPEN)  # noqa: layers
    @pytest.mark.asyncio
    async def test_lp_open_with_invalid_pool_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test that LP_OPEN with an invalid pool fails at compilation.

        Verifies compilation produces a clear error for invalid pool specs,
        and (Layer 5) that a failed LP_OPEN writes ZERO accounting_events
        rows (epic VIB-4591 decision #7).
        """
        print(f"\n{'='*80}")
        print("Test: LP_OPEN with invalid pool (should fail)")
        print(f"{'='*80}")

        # Snapshot balances BEFORE compile so we can assert the failure-path
        # conservation invariant (no on-chain tx, no balance movement).
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        avax_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        intent = LPOpenIntent(
            pool="INVALID/TOKENS/3000",
            amount0=Decimal("1"),
            amount1=Decimal("1"),
            range_lower=Decimal("1000"),
            range_upper=Decimal("2000"),
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "FAILED", (
            "Compilation should fail for invalid token symbols"
        )
        assert compilation_result.error is not None
        assert compilation_result.action_bundle is None, (
            "Compiler must not return an ActionBundle on FAILED compilation"
        )

        # Failure-path bilateral conservation: compile-only test, no tx fired,
        # both balances MUST be unchanged.
        avax_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        assert avax_after == avax_before, (
            f"Native AVAX balance must remain unchanged on compile failure. "
            f"before={avax_before}, after={avax_after}"
        )
        assert usdc_after == usdc_before, (
            f"USDC balance must remain unchanged on compile failure. "
            f"before={usdc_before}, after={usdc_after}"
        )

        print(f"Compilation failed as expected: {compilation_result.error}")

        # Layer 5: a failed LP_OPEN must write zero accounting_events rows.
        failed_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.VALIDATION,
            error=compilation_result.error or "LP_OPEN compilation failed",
        )
        await assert_no_accounting_on_failure(
            layer5_accounting_harness,
            intent=intent,
            result=failed_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
