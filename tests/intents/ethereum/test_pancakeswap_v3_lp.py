"""Production-grade LP Intent tests for PancakeSwap V3 on Ethereum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Opening concentrated liquidity positions
- LPCloseIntent: Closing positions with liquidity
- CollectFeesIntent: Harvesting accrued fees without closing

PancakeSwap V3 on Ethereum supports the canonical WETH/USDC pair.
Uses fee tier 500 (0.05%) which is the canonical liquid tier.

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/ethereum/test_pancakeswap_v3_lp.py -v -s
"""

import json
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.pancakeswap_v3.receipt_parser import PancakeSwapV3ReceiptParser
from almanak.framework.execution.extracted_data import LPCloseData
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
)
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents import (
    LP_POSITION_MANAGERS,
    IntentCompiler,
    LPCloseIntent,
    LPOpenIntent,
    SwapIntent,
)
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_accounting_persisted,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "ethereum"
POSITION_MANAGER = LP_POSITION_MANAGERS["ethereum"]["pancakeswap_v3"]

# Pool: WETH/USDC 0.05% fee tier
# PancakeSwap V3 fee tiers: 100, 500, 2500, 10000
POOL = "WETH/USDC/500"
LP_AMOUNT_WETH = Decimal("0.1")    # amount0 candidate
LP_AMOUNT_USDC = Decimal("250")    # amount1 candidate

# Wide price range in USDC-per-WETH terms
RANGE_LOWER = Decimal("200")
RANGE_UPPER = Decimal("20000")


# =============================================================================
# Helpers
# =============================================================================


def _query_position_liquidity(web3: Web3, position_manager: str, token_id: int) -> int:
    """Query position liquidity from NonfungiblePositionManager.positions()."""
    selector = "0x99fbab88"  # positions(uint256)
    data = selector + hex(token_id)[2:].zfill(64)
    result = web3.eth.call({"to": Web3.to_checksum_address(position_manager), "data": data})
    liquidity_offset = 7 * 32
    return int.from_bytes(result[liquidity_offset : liquidity_offset + 32], byteorder="big")


# -----------------------------------------------------------------------------
# Layer-5 accounting helpers (mirrors tests/intents/ethereum/test_uniswap_v3_lp.py)
# -----------------------------------------------------------------------------


def _execution_context(wallet: str) -> ExecutionContext:
    return ExecutionContext(
        deployment_id="layer5-pancakeswap-v3-lp",
        chain=CHAIN_NAME,
        wallet_address=wallet,
        protocol="pancakeswap_v3",
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


def _assert_no_lot_id(row: dict, payload: dict) -> None:
    assert "lot_id" not in row
    assert "lot_id" not in payload


def _assert_parser_event_equality(payload: dict, lp_close_data, *, dec0: int, dec1: int) -> None:
    """Parser ↔ event exact equality, honoring the Empty≠Zero≠None contract.

    ``LPCloseData.fees{0,1}`` default to ``None`` when the parser did not
    measure fees separately (Empty). The LP handler persists measured-zero
    ``Decimal('0')`` in that no-fee state per epic decision #5, so a ``None``
    parser reading must reconcile against a ``"0"`` payload, not crash.
    """
    assert Decimal(payload["amount0"]) == _to_human(lp_close_data.amount0_collected, dec0)
    assert Decimal(payload["amount1"]) == _to_human(lp_close_data.amount1_collected, dec1)
    expected_fees0 = _to_human(lp_close_data.fees0, dec0)
    expected_fees1 = _to_human(lp_close_data.fees1, dec1)
    assert Decimal(payload["fees0_collected"]) == (expected_fees0 if expected_fees0 is not None else Decimal("0"))
    assert Decimal(payload["fees1_collected"]) == (expected_fees1 if expected_fees1 is not None else Decimal("0"))


async def _open_position_for_accounting(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
):
    """Open an LP position via LPOpenIntent; return (position_id, intent, enriched_result)."""
    intent = LPOpenIntent(
        pool=POOL,
        amount0=LP_AMOUNT_WETH,
        amount1=LP_AMOUNT_USDC,
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        protocol="pancakeswap_v3",
        chain=CHAIN_NAME,
    )

    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    compilation_result = compiler.compile(intent)
    assert compilation_result.status.value == "SUCCESS", f"LP Open compilation failed: {compilation_result.error}"
    assert compilation_result.action_bundle is not None

    execution_result = await orchestrator.execute(compilation_result.action_bundle)
    assert execution_result.success, f"LP Open execution failed: {execution_result.error}"
    enriched = _enrich_for_accounting(
        execution_result,
        intent,
        funded_wallet,
        compilation_result.action_bundle.metadata,
    )

    parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)
    position_id = None
    for tx_result in enriched.transaction_results:
        if tx_result.receipt:
            pos_id = parser.extract_position_id(tx_result.receipt.to_dict())
            if pos_id is not None:
                position_id = pos_id
    assert position_id is not None, "Failed to extract position ID from LP Open receipt"
    return position_id, intent, enriched


async def _open_position_via_intent(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> int:
    """Open an LP position via LPOpenIntent and return the position token ID."""
    position_id, _, _ = await _open_position_for_accounting(
        funded_wallet,
        orchestrator,
        price_oracle,
        anvil_rpc_url,
    )
    return position_id


# =============================================================================
# LPOpenIntent Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.lp
class TestPancakeSwapV3LPOpenIntent:
    """Test PancakeSwap V3 LP Open using LPOpenIntent on Ethereum."""

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_weth_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test opening a WETH/USDC LP position using LPOpenIntent."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]

        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Open WETH/USDC via LPOpenIntent (PancakeSwap V3 on Ethereum)")
        print(f"{'=' * 80}")
        print(f"Pool: {POOL}")
        print(f"Position Manager: {POSITION_MANAGER}")

        # 1. Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        assert usdc_before > 0, "funded_wallet has no USDC — fixture funding failed"
        assert weth_before > 0, "funded_wallet has no WETH — fixture funding failed"

        # 2. Create LPOpenIntent
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_USDC,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
        )

        # 3. Compile
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        # 4. Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        execution_result = _enrich_for_accounting(
            execution_result,
            intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
        )

        # 5. Parse receipts - extract position ID
        parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)
        position_id = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                pos_id = parser.extract_position_id(tx_result.receipt.to_dict())
                if pos_id is not None:
                    position_id = pos_id

        assert position_id is not None, "Must extract position ID from mint receipt"
        print(f"\nPosition ID: {position_id}")

        # 6. Verify on-chain position has liquidity
        liquidity = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have positive liquidity, got {liquidity}"

        # 7. Verify balance changes
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_spent = weth_before - weth_after

        print(f"USDC spent: {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"WETH spent: {format_token_amount(weth_spent, weth_decimals)}")

        assert usdc_spent > 0 or weth_spent > 0, "Must deposit at least one token into LP"

        # 8. Layer 5 — assert the real accounting pipeline persisted LP_OPEN.
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
        assert Decimal(payload["amount0"]) >= 0
        assert Decimal(payload["amount1"]) >= 0
        assert payload["position_hash"] is None, "PancakeSwap V3 LP_OPEN must not fabricate a V4 position_hash"
        assert payload["tick_lower"] is not None
        assert payload["tick_upper"] is not None
        assert payload["liquidity"] is not None
        assert payload["current_tick"] is not None
        assert payload["in_range"] is True

        print("\nALL CHECKS PASSED")


# =============================================================================
# LPCloseIntent Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.lp
class TestPancakeSwapV3LPCloseIntent:
    """Test PancakeSwap V3 LP Close using LPCloseIntent on Ethereum."""

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_position_with_liquidity(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test closing a PancakeSwap V3 position that has liquidity."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Close - Position with Liquidity (PancakeSwap V3 on Ethereum)")
        print(f"{'=' * 80}")

        # 1. Open position (Layer 5 needs the prior OPEN for linkage + basis)
        position_id, open_intent, open_result = await _open_position_for_accounting(
            funded_wallet,
            orchestrator,
            price_oracle,
            anvil_rpc_url,
        )
        open_accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=open_intent,
            result=open_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_OPEN",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        print(f"Opened position #{position_id}")

        # 2. Verify it has liquidity
        liquidity = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have liquidity before close, got {liquidity}"

        # 3. Record balances BEFORE close
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)

        # 4. Close via LPCloseIntent
        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        compilation_result = compiler.compile(close_intent)
        assert compilation_result.status.value == "SUCCESS", f"LP Close compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"LP Close execution failed: {execution_result.error}"
        execution_result = _enrich_for_accounting(
            execution_result,
            close_intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
        )

        # 5. Parse receipts
        parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)
        lp_close_data = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                data = parser.extract_lp_close_data(tx_result.receipt.to_dict())
                if data:
                    lp_close_data = data

        assert lp_close_data is not None, "Must extract LP close data from receipt"
        assert lp_close_data.amount0_collected > 0 or lp_close_data.amount1_collected > 0, (
            "At least one collected amount must be positive"
        )

        # 6. Verify tokens returned
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)

        usdc_returned = usdc_after_close - usdc_before_close
        weth_returned = weth_after_close - weth_before_close

        print(f"\nUSDC returned: {format_token_amount(usdc_returned, usdc_decimals)}")
        print(f"WETH returned: {format_token_amount(weth_returned, weth_decimals)}")

        assert usdc_returned > 0 or weth_returned > 0, (
            f"Must receive tokens back when closing position. "
            f"USDC returned: {usdc_returned}, WETH returned: {weth_returned}"
        )

        # 7. Layer 5 — assert the real accounting pipeline persisted LP_CLOSE.
        close_accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=close_intent,
            result=execution_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_CLOSE",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        _assert_identity(close_accounting_row, event_type="LP_CLOSE", wallet=funded_wallet)
        close_payload = _payload(close_accounting_row)
        open_payload = _payload(open_accounting_row)
        # #4 linkage: LP_CLOSE.position_key == LP_OPEN.position_key + basis from prior OPEN.
        assert close_payload["position_key"] == open_payload["position_key"]
        _assert_no_lot_id(close_accounting_row, close_payload)
        # #2 directional null-contract on LP_CLOSE (V3: no fabricated V4 hash).
        assert close_payload["position_hash"] is None, "PancakeSwap V3 LP_CLOSE must not fabricate a V4 position_hash"
        assert close_payload["realized_pnl_usd"] is not None, "open-then-close must compute realized PnL"

        # #3 parser ↔ event exact equality.
        dec0 = get_token_decimals(web3, tokens[close_payload["token0"]])
        dec1 = get_token_decimals(web3, tokens[close_payload["token1"]])
        # lp-close-may20.md §6.3: read aggregated value (matches persisted payload).
        # Per-receipt parser loop above is preserved for diagnostic prints.
        lp_close_data = execution_result.lp_close_data
        _assert_parser_event_equality(close_payload, lp_close_data, dec0=dec0, dec1=dec1)

        print("\nALL CHECKS PASSED")


# =============================================================================
# CollectFeesIntent Tests (LP_COLLECT_FEES) — VIB-4307
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.lp
class TestPancakeSwapV3CollectFeesIntent:
    """Test PancakeSwap V3 LP_COLLECT_FEES using CollectFeesIntent on Ethereum.

    Flow (4 layers):
      1. Open an in-range LP position via LPOpenIntent.
      2. Execute a swap through the same pool to accrue fees on the position.
      3. Issue CollectFeesIntent(protocol="pancakeswap_v3", protocol_params={"position_id": ...}).
      4. Verify wallet balances increased (fees were transferred to the wallet).
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.SWAP, IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    async def test_collect_fees_weth_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Collect fees from an in-range PancakeSwap V3 WETH/USDC position after a swap.

        Asserts:
        * Compilation of CollectFeesIntent (pancakeswap_v3) -> SUCCESS.
        * Execution -> success.
        * Position liquidity unchanged (fee harvest does not remove principal).
        * At least one wallet balance strictly increases.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP_COLLECT_FEES WETH/USDC via PancakeSwap V3 on Ethereum")
        print(f"{'=' * 80}")

        # 1. Open an in-range position to accrue fees against.
        position_id, open_intent, open_result = await _open_position_for_accounting(
            funded_wallet, orchestrator, price_oracle, anvil_rpc_url,
        )
        open_accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=open_intent,
            result=open_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_OPEN",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        print(f"Opened position #{position_id}")

        liquidity_before = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity_before > 0, "Setup LP_OPEN must yield positive liquidity"

        # 2. Execute a same-pool swap to generate trading fees.
        swap_intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            max_slippage=Decimal("0.05"),
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        swap_compilation = compiler.compile(swap_intent)
        if swap_compilation.status.value == "SUCCESS" and swap_compilation.action_bundle:
            swap_result = await orchestrator.execute(swap_compilation.action_bundle)
            if swap_result.success:
                print("Executed swap to generate LP fees")

        # 3. Record balances BEFORE fee collection
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")

        # 4. Issue the LP_COLLECT_FEES intent.
        collect_intent = CollectFeesIntent(
            pool=POOL,
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
            protocol_params={"position_id": position_id},
        )

        print("\nCompiling CollectFeesIntent...")
        compilation_result = compiler.compile(collect_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"CollectFees compilation must succeed (pancakeswap_v3 LP_COLLECT_FEES). "
            f"Error: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"CollectFees execution failed: {execution_result.error}"
        execution_result = _enrich_for_accounting(
            execution_result,
            collect_intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
        )

        # 5. Parse receipts — accumulate the Collect amounts (fees-only harvest).
        parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)
        parsed_amount0_collected = 0
        parsed_amount1_collected = 0
        saw_collect = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parse_result = parser.parse_receipt(receipt_dict)
                assert parse_result.success, (
                    f"Receipt parser must succeed on a confirmed receipt; error={parse_result.error}"
                )
                lp_close_data = parser.extract_lp_close_data(receipt_dict)
                if lp_close_data:
                    parsed_amount0_collected += lp_close_data.amount0_collected
                    parsed_amount1_collected += lp_close_data.amount1_collected
                    saw_collect = True

        assert saw_collect, "Receipt must contain a Collect event from LP_COLLECT_FEES"
        assert parsed_amount0_collected > 0 or parsed_amount1_collected > 0, (
            f"Parser must report positive collected amounts. "
            f"amount0={parsed_amount0_collected}, amount1={parsed_amount1_collected}"
        )
        execution_result.lp_close_data = LPCloseData(
            amount0_collected=0,
            amount1_collected=0,
            fees0=parsed_amount0_collected,
            fees1=parsed_amount1_collected,
            liquidity_removed=0,
            source="collect",
        )
        execution_result.extracted_data["lp_close_data"] = execution_result.lp_close_data

        # 6. Verify principal liquidity is unchanged.
        liquidity_after = _query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity_after == liquidity_before, (
            f"LP_COLLECT_FEES must NOT remove liquidity. "
            f"before={liquidity_before}, after={liquidity_after}"
        )

        # 7. Verify wallet balances increased (fees were transferred).
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_received = usdc_after - usdc_before
        weth_received = weth_after - weth_before

        print(f"\nUSDC received: {format_token_amount(usdc_received, usdc_decimals)}")
        print(f"WETH received: {format_token_amount(weth_received, weth_decimals)}")

        assert usdc_received >= 0 and weth_received >= 0, (
            "Fee collection must not decrease wallet balances"
        )
        assert usdc_received > 0 or weth_received > 0, (
            f"At least one token must increase after fee collection. "
            f"USDC received: {usdc_received}, WETH received: {weth_received}"
        )

        # 8. Layer 5 — assert the real accounting pipeline persisted LP_COLLECT_FEES.
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
        assert collect_payload["position_key"] == _payload(open_accounting_row)["position_key"]
        _assert_no_lot_id(collect_accounting_row, collect_payload)
        assert collect_payload["position_hash"] is None
        # Fees-only harvest: principal amounts are measured-zero, fees carry the value.
        dec0 = get_token_decimals(web3, tokens[collect_payload["token0"]])
        dec1 = get_token_decimals(web3, tokens[collect_payload["token1"]])
        assert Decimal(collect_payload["amount0"]) == Decimal("0")
        assert Decimal(collect_payload["amount1"]) == Decimal("0")
        assert Decimal(collect_payload["fees0_collected"]) == _to_human(parsed_amount0_collected, dec0)
        assert Decimal(collect_payload["fees1_collected"]) == _to_human(parsed_amount1_collected, dec1)

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
