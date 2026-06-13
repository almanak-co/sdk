"""4-layer intent tests for Uniswap V4 LP_COLLECT_FEES on Arbitrum Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
collecting fees from V4 LP positions via PositionManager on Arbitrum:
1. Open a position first (LP_OPEN as setup)
2. Create CollectFeesIntent with position_id and protocol_params
3. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
4. Execute via ExecutionOrchestrator (full production pipeline)
5. Parse receipts (fee collection events)
6. Verify position remains open and balances are conserved

NO MOCKING. All tests execute real on-chain LP operations and verify state changes.

To run:
    uv run pytest tests/intents/arbitrum/test_uniswap_v4_collect_fees.py -v -s
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
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType, LPOpenIntent
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

CHAIN_NAME = "arbitrum"

# WETH/USDC pool with 0.3% fee tier
LP_POOL = "WETH/USDC/3000"

# Small amounts for setup LP_OPEN
LP_AMOUNT_WETH = Decimal("0.01")
LP_AMOUNT_USDC = Decimal("25")
LP_RANGE_LOWER = Decimal("1000")
LP_RANGE_UPPER = Decimal("10000")

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
# Helper: Open a position (setup for collect fees tests)
# =============================================================================


async def _open_v4_position(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
) -> tuple[int, str, str]:
    """Open a V4 LP position and return (position_id, currency0, currency1).

    Raises AssertionError if the setup LP_OPEN fails.
    """
    intent = LPOpenIntent(
        pool=LP_POOL,
        amount0=LP_AMOUNT_WETH,
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

    # Extract position_id from receipt
    parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
    position_id = None

    for tx_result in execution_result.transaction_results:
        if tx_result.receipt:
            receipt_dict = tx_result.receipt.to_dict()
            pid = parser.extract_position_id(receipt_dict)
            if pid is not None:
                position_id = pid

    assert position_id is not None, "Setup LP_OPEN must yield a position_id"

    # Get currency addresses from bundle metadata
    token0 = bundle.metadata.get("token0", {})
    token1 = bundle.metadata.get("token1", {})
    currency0 = token0.get("address", "")
    currency1 = token1.get("address", "")

    assert currency0 and currency1, "Must extract currency addresses from bundle metadata"

    return position_id, currency0, currency1


# =============================================================================
# CollectFeesIntent Tests -- Uniswap V4 on Arbitrum
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestUniswapV4CollectFeesIntent:
    """Test Uniswap V4 LP_COLLECT_FEES using CollectFeesIntent on Arbitrum.

    These tests verify the fee collection flow:
    - First open a position (setup)
    - CollectFeesIntent creation with protocol_params
    - UniswapV4Compiler compiles LP_COLLECT_FEES
    - Transactions execute successfully on-chain via PositionManager
    - Position remains open after fee collection
    - Balance deltas are non-negative (fees collected >= 0)
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    async def test_collect_fees_weth_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test collecting fees from a WETH/USDC LP position via V4 on Arbitrum.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: Transaction confirmed with expected events
        4. Balance Deltas: Balances non-negative (fees >= 0, no tokens lost)

        Note: On a freshly opened position, accrued fees will be 0.
        The test verifies the collection flow works without errors,
        not that fees > 0 (which requires trading activity in the pool).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_addr = tokens["WETH"]
        usdc_addr = tokens["USDC"]

        weth_decimals = get_token_decimals(web3, weth_addr)
        usdc_decimals = get_token_decimals(web3, usdc_addr)

        print(f"\n{'='*80}")
        print("Test: LP_COLLECT_FEES WETH/USDC via Uniswap V4 on Arbitrum")
        print(f"{'='*80}")

        # Setup: Open a position first
        print("\n--- Setup: Opening LP position ---")
        position_id, currency0, currency1 = await _open_v4_position(
            web3, funded_wallet, orchestrator, price_oracle,
        )
        print(f"Opened position: id={position_id}")
        print(f"Currencies: {currency0[:10]}.../{currency1[:10]}...")

        # Record balances before fee collection
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        print("\n--- Collecting fees ---")
        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")
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

        print(f"Created CollectFeesIntent: pool={collect_intent.pool}, position_id={position_id}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
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

        assert execution_result.success, f"COLLECT_FEES execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Enrich for accounting (populates result.lp_close_data — Layer 5
        # needs it; mirrors the V3 golden / SushiSwap precedent ordering).
        execution_result = _enrich_for_accounting(
            execution_result, collect_intent, funded_wallet, bundle.metadata
        )

        # Layer 3: Receipt Parsing
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        lp_close_data = None

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parsed = parser.parse_receipt(receipt_dict)

                # Log Transfer events (fee collection)
                for transfer in parsed.transfer_events:
                    print(f"  Transfer: from={transfer.from_address[:10]}... to={transfer.to_address[:10]}... amount={transfer.amount}")
                for ml in parsed.modify_liquidity_events:
                    print(f"  ModifyLiquidity: delta={ml.liquidity_delta}")

                close_data = parser.extract_lp_close_data(receipt_dict)
                if close_data is not None:
                    lp_close_data = close_data

        # Layer 4: Balance Deltas
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)

        weth_delta = weth_after - weth_before
        usdc_delta = usdc_after - usdc_before

        print("\n--- Balance Deltas ---")
        print(f"WETH delta: {format_token_amount(weth_delta, weth_decimals)}")
        print(f"USDC delta: {format_token_amount(usdc_delta, usdc_decimals)}")

        # Fees should be >= 0 (can be 0 on freshly opened position)
        assert weth_delta >= 0, "WETH should not decrease from fee collection"
        assert usdc_delta >= 0, "USDC should not decrease from fee collection"

        # Layer 5: assert the real accounting pipeline persisted
        # LP_COLLECT_FEES. VIB-4637 (genuine production gap, surfaced by
        # this rollout): a V4 fees-only collect emits ModifyLiquidity
        # delta=0, so extract_lp_close_data yields no typed pool_address
        # and _resolve_pool_address rejects the V3-style V4 position_key
        # (`weth/usdc/3000`) — the LP_COLLECT_FEES event is dropped
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
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        _assert_identity(collect_accounting_row, event_type="LP_COLLECT_FEES", wallet=funded_wallet)
        collect_payload = _payload(collect_accounting_row)
        assert collect_payload["position_key"] == collect_accounting_row["position_key"]
        _assert_no_lot_id(collect_accounting_row, collect_payload)
        # #2 directional null-contract: LP_COLLECT_FEES matches by
        # position_key, so position_hash stays None (anchor lives on OPEN).
        _assert_v4_close_position_hash(collect_payload)
        # #3 parser ↔ event exact equality, honoring Empty≠Zero≠None.
        if lp_close_data is not None:
            dec0 = get_token_decimals(web3, tokens[collect_payload["token0"]])
            dec1 = get_token_decimals(web3, tokens[collect_payload["token1"]])
            assert Decimal(collect_payload["amount0"]) == _to_human(lp_close_data.amount0_collected, dec0)
            assert Decimal(collect_payload["amount1"]) == _to_human(lp_close_data.amount1_collected, dec1)
            _assert_fee_contract(
                collect_payload["fees0_collected"], _to_human(lp_close_data.fees0, dec0), field="fees0_collected"
            )
            _assert_fee_contract(
                collect_payload["fees1_collected"], _to_human(lp_close_data.fees1, dec1), field="fees1_collected"
            )
        else:
            # No lp_close_data — still pin amount0/1 to the Layer-4 wallet
            # deltas so a zero or mis-scaled persisted amount cannot pass
            # unchecked (CodeRabbit PR #2369).
            assert Decimal(collect_payload["amount0"]) == _to_human(weth_delta, weth_decimals)
            assert Decimal(collect_payload["amount1"]) == _to_human(usdc_delta, usdc_decimals)
            _assert_fee_contract(collect_payload["fees0_collected"], None, field="fees0_collected")
            _assert_fee_contract(collect_payload["fees1_collected"], None, field="fees1_collected")

        print(f"\nFees collected from position {position_id} (may be 0 on fresh position)")
        print("\nALL 5 LAYERS PASSED")

    @pytest.mark.intent(IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    async def test_collect_fees_without_position_id_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test that COLLECT_FEES without position_id fails at compilation.

        V4 LP_COLLECT_FEES requires position_id in protocol_params.
        Layer 5: a failed LP_COLLECT_FEES writes ZERO accounting_events
        rows (epic VIB-4591 decision #7).
        """
        print(f"\n{'='*80}")
        print("Test: COLLECT_FEES without position_id (should fail)")
        print(f"{'='*80}")

        collect_intent = CollectFeesIntent(
            pool=LP_POOL,
            protocol="uniswap_v4",
            chain=CHAIN_NAME,
            # No protocol_params -- missing position_id
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        compilation_result = compiler.compile(collect_intent)

        assert compilation_result.status.value == "FAILED", (
            "Compilation should fail without position_id"
        )
        assert "position_id" in compilation_result.error.lower(), (
            f"Error should mention position_id, got: {compilation_result.error}"
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
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
