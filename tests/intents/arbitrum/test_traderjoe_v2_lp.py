"""Production-grade LP Intent tests for TraderJoe V2 on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Opening liquidity positions in discrete price bins
- LPCloseIntent: Closing positions with various states

TraderJoe V2 uses a Liquidity Book model with discrete price bins and
ERC1155-like fungible LP tokens (not NFT positions like Uniswap/SushiSwap V3).
``removeLiquidity`` auto-collects fees on close -- there is no separate
LP_COLLECT_FEES verb in this single-file flow.

LP Close test cases:
  #1: Position has liquidity (normal close)
  #2: No position exists (wallet has no LP tokens)

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/arbitrum/test_traderjoe_v2_lp.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config
from almanak.framework.connectors.traderjoe_v2.receipt_parser import (
    TraderJoeV2EventType,
    TraderJoeV2ReceiptParser,
)
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import (
    IntentCompiler,
    LPCloseIntent,
    LPOpenIntent,
)
from almanak.framework.intents.vocabulary import IntentType
from tests.intents import _traderjoe_v2_layer5 as _l5
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_traderjoe_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"

# Pool: WETH/USDC with binStep=15 (the only WETH/USDC LBPair on Arbitrum
# carrying meaningful reserves -- bin steps 20, 25, 50, 100 all return zero
# reserves via ``LBPair.getReserves()`` as of 2026-05-14).
# Token X: WETH (0x82aF49447D8a07e3bd95BD0d56f35241523fBab1)
# Token Y: USDC (0xaf88d065e77c8cC2239327C5EDb3A432268e5831)
POOL = "WETH/USDC/15"
LP_AMOUNT_WETH = Decimal("0.05")  # amount0 (Token X = WETH)
LP_AMOUNT_USDC = Decimal("150")  # amount1 (Token Y = USDC)

# Price range in USDC-per-WETH terms (wide range to ensure both tokens are deposited).
# TraderJoe V2's compiler places liquidity around the active bin using
# ``protocol_params.bin_range`` (default 5); the LPOpenIntent ``range_lower`` /
# ``range_upper`` fields are required by the intent model but not bin-mapped.
RANGE_LOWER = Decimal("200")
RANGE_UPPER = Decimal("20000")

BIN_STEP = 15


# =============================================================================
# Layer-5 accounting helpers (epic VIB-4591, ticket VIB-4598)
# =============================================================================
#
# Shared across all five TraderJoe V2 LP intent-test files via
# ``tests/intents/_traderjoe_v2_layer5.py`` (gemini PR #2366: de-duplicate
# the ~180-line-per-file block). The thin chain-bound wrappers below bind
# this file's ``CHAIN_NAME`` so call sites stay one-liners. The module
# docstring documents the bin-model directional null-contract in full.


def _enrich_for_accounting(execution_result, intent, wallet: str, bundle_metadata: dict | None = None):
    return _l5.enrich_for_accounting(
        execution_result,
        intent,
        chain=CHAIN_NAME,
        wallet=wallet,
        bundle_metadata=bundle_metadata,
    )


_payload = _l5.payload
_to_human = _l5.to_human
_assert_identity = _l5.assert_identity
_assert_no_lot_id = _l5.assert_no_lot_id
_assert_accounting_persisted_or_xfail = _l5.assert_accounting_persisted_or_xfail
_assert_bin_model_null_contract = _l5.assert_bin_model_null_contract
_assert_close_parser_event_equality = _l5.assert_close_parser_event_equality


# =============================================================================
# Helpers
# =============================================================================


def _get_position_via_adapter(
    rpc_url: str,
    wallet: str,
    token_x: str,
    token_y: str,
    bin_step: int,
):
    """Query position using TraderJoeV2Adapter."""
    config = TraderJoeV2Config(
        chain=CHAIN_NAME,
        wallet_address=wallet,
        rpc_url=rpc_url,
    )
    adapter = TraderJoeV2Adapter(config)
    return adapter.get_position(token_x, token_y, bin_step, wallet=wallet)


async def _open_position_for_accounting(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
):
    """Open an LP position via LPOpenIntent; return (intent, enriched_result).

    TraderJoe V2 uses bin-based positions (no NFT token ID to return).
    Position is identified by pool + wallet + bin IDs. The enrichment runs
    with ``live_mode=False`` (paper) exactly as the runner would in
    non-live mode, so Layer-5 callers can persist the LP_OPEN through the
    real accounting pipeline.
    """
    intent = LPOpenIntent(
        pool=POOL,
        amount0=LP_AMOUNT_WETH,  # Token X = WETH
        amount1=LP_AMOUNT_USDC,  # Token Y = USDC
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        protocol="traderjoe_v2",
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
    # Return the RAW result — the caller enriches for accounting only just
    # before the Layer-5 persistence call, so an enricher regression cannot
    # mask the close-path Layer-3/4 hard asserts (CodeRabbit PR #2366).
    return intent, execution_result, compilation_result.action_bundle.metadata


async def _open_position_via_intent(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> None:
    """Open an LP position via LPOpenIntent (no accounting return)."""
    await _open_position_for_accounting(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)


# =============================================================================
# LPOpenIntent Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestTraderJoeV2LPOpenIntent:
    """Test TraderJoe V2 LP Open using LPOpenIntent.

    Verifies the full Intent flow:
    - LPOpenIntent creation with pool and amounts
    - IntentCompiler generates correct LBRouter addLiquidity TX
    - Transactions execute successfully on-chain
    - Position has liquidity in bins (queried via adapter)
    - Balance changes are correct (bilateral: tokens spent + LBPair shares received)
    """

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
        """Test opening a WETH/USDC LP position using LPOpenIntent.

        Flow:
        1. Record balances BEFORE
        2. Create LPOpenIntent for WETH/USDC pool
        3. Compile to ActionBundle using IntentCompiler
        4. Execute via ExecutionOrchestrator
        5. Parse receipts - verify DepositedToBins event, extract bin IDs
        6. Query position via adapter - verify bin_ids non-empty, LP shares > 0
        7. Verify balance changes (bilateral: tokens out + shares in)
        8. Layer 5 - assert the real accounting pipeline persisted LP_OPEN
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        fail_if_traderjoe_pool_missing(web3, CHAIN_NAME, weth_addr, usdc_addr, BIN_STEP)

        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Open WETH/USDC via LPOpenIntent (TraderJoe V2)")
        print(f"{'=' * 80}")
        print(f"Pool: {POOL}")
        print(f"Amount WETH (token X): {LP_AMOUNT_WETH}")
        print(f"Amount USDC (token Y): {LP_AMOUNT_USDC}")

        # 1. Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")

        # 2. Create LPOpenIntent
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WETH,  # Token X = WETH
            amount1=LP_AMOUNT_USDC,  # Token Y = USDC
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )

        # 3. Compile
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        print("\nCompiling LPOpenIntent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # 4. Execute
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # 5. Parse receipts - verify DepositedToBins events and extract bin IDs
        parser = TraderJoeV2ReceiptParser()
        found_deposit_event = False
        extracted_bin_ids = None

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()

                # Check for DepositedToBins events
                parse_result = parser.parse_receipt(receipt_dict)
                if parse_result.success:
                    print(f"  Events parsed: {len(parse_result.events)}")
                    for event in parse_result.events:
                        if event.event_type == TraderJoeV2EventType.DEPOSITED_TO_BINS:
                            found_deposit_event = True
                            print(f"  DepositedToBins event found at log index {event.log_index}")

                    if parse_result.liquidity_result and parse_result.liquidity_result.is_add:
                        print(f"  Liquidity add detected: pool={parse_result.liquidity_result.pool_address}")

                # Extract bin IDs
                bin_ids = parser.extract_bin_ids(receipt_dict)
                if bin_ids:
                    extracted_bin_ids = bin_ids
                    print(f"  Extracted bin IDs: {len(bin_ids)} bins")

        assert found_deposit_event, "Must find DepositedToBins event in receipts"
        assert extracted_bin_ids is not None and len(extracted_bin_ids) > 0, (
            "Must extract bin IDs from DepositedToBins event"
        )
        print(f"\nExtracted {len(extracted_bin_ids)} bin IDs")

        # 6. Query position via adapter - verify bin_ids not empty, LBPair shares > 0
        position = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=weth_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        assert position is not None, "Position must exist after LP open"
        assert len(position.bin_ids) > 0, "Position must have bin IDs"
        # LBPair ERC-1155 shares are tracked via ``position.balances`` (one
        # balance per bin). A non-zero total is the receipt-of-shares proof
        # the bilateral-delta layer requires for LP_OPEN.
        total_lp_balance = sum(position.balances.values())
        assert total_lp_balance > 0, (
            f"Position must have non-zero LBPair ERC-1155 share balance, got total={total_lp_balance}"
        )
        print(
            f"On-chain position: {len(position.bin_ids)} bins, "
            f"total LBPair shares={total_lp_balance}, "
            f"amount_x={position.amount_x}, amount_y={position.amount_y}"
        )

        # 7. Verify balance changes (bilateral: tokens out + shares in covered above)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_spent = weth_before - weth_after

        print(f"\nUSDC spent: {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"WETH spent: {format_token_amount(weth_spent, weth_decimals)}")

        # At least one token must have been deposited
        assert usdc_spent > 0 or weth_spent > 0, "Must deposit at least one token into LP"

        # Amounts spent must not exceed desired amounts
        expected_usdc_max = int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        expected_weth_max = int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))
        assert usdc_spent <= expected_usdc_max, f"USDC spent ({usdc_spent}) exceeds desired ({expected_usdc_max})"
        assert weth_spent <= expected_weth_max, f"WETH spent ({weth_spent}) exceeds desired ({expected_weth_max})"

        # 8. Layer 5 — assert the real accounting pipeline persisted LP_OPEN.
        # Enrichment runs HERE (after Layers 1–4 hard asserts), so an
        # enricher regression cannot mask receipt-parse/balance coverage
        # (CodeRabbit PR #2366).
        accounting_result = _enrich_for_accounting(
            execution_result,
            intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
        )
        accounting_row = await _assert_accounting_persisted_or_xfail(
            layer5_accounting_harness,
            intent=intent,
            result=accounting_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_OPEN",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        _assert_identity(accounting_row, event_type="LP_OPEN", wallet=funded_wallet)
        payload = _payload(accounting_row)
        assert payload["position_key"] == accounting_row["position_key"]
        # Bin-model directional null-contract: no fabricated NFT/tick fields,
        # pool_address is the descriptor (not a 0x address).
        _assert_bin_model_null_contract(payload, event_type="LP_OPEN")
        assert Decimal(payload["amount0"]) >= 0
        assert Decimal(payload["amount1"]) >= 0

        print("\nALL CHECKS PASSED")


# =============================================================================
# LPCloseIntent Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestTraderJoeV2LPCloseIntent:
    """Test TraderJoe V2 LP Close using LPCloseIntent.

    Test cases:
    #1: Position has liquidity (normal LP close, auto-collects fees)
    #2: No position exists (wallet has no LP tokens)

    Note: TraderJoe V2 does not have the "decreased but not collected" edge case
    because ``removeLiquidity`` removes and returns tokens (including any
    accrued fees) in a single step.
    """

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
        """Test #1: Close position that has liquidity.

        Flow:
        1. Open LP position via LPOpenIntent
        2. Verify position exists with liquidity via adapter
        3. Record balances BEFORE close
        4. Close via LPCloseIntent
        5. Parse receipts - verify WithdrawnFromBins events
        6. Verify tokens returned to wallet (bilateral deltas > 0)
        7. Verify position is now empty (LBPair shares burned)
        8. Layer 5 - assert the real accounting pipeline persisted LP_OPEN + LP_CLOSE
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        fail_if_traderjoe_pool_missing(web3, CHAIN_NAME, weth_addr, usdc_addr, BIN_STEP)
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        print(f"\n{'=' * 80}")
        print("Test #1: LP Close - Position with Liquidity (TraderJoe V2)")
        print(f"{'=' * 80}")

        # 1. Open position. Capture the enriched OPEN result + intent now,
        # but DEFER its Layer-5 persistence to the end of the test: the
        # VIB-4634 production gap makes that helper raise pytest.xfail, and
        # xfailing here would skip the LP_CLOSE compile/execute/parse/balance
        # hard asserts below (gemini PR #2366). Layers 1–4 for the close run
        # first; all Layer-5 persistence (OPEN then CLOSE) happens at step 8.
        open_intent, open_result, open_meta = await _open_position_for_accounting(
            funded_wallet, orchestrator, price_oracle, anvil_rpc_url
        )
        print("Opened LP position via LPOpenIntent")

        # 2. Verify it has liquidity
        position = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=weth_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        assert position is not None, "Position must exist before close"
        assert len(position.bin_ids) > 0, "Position must have bin IDs"
        total_shares_before_close = sum(position.balances.values())
        assert total_shares_before_close > 0, (
            "Position must hold non-zero LBPair shares before close"
        )
        print(
            f"Position: {len(position.bin_ids)} bins, "
            f"shares={total_shares_before_close}, "
            f"amount_x={position.amount_x}, amount_y={position.amount_y}"
        )

        # 3. Record balances BEFORE close
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)

        # 4. Close via LPCloseIntent
        close_intent = LPCloseIntent(
            position_id="0",  # TraderJoe V2 uses bin-based positions, not NFT IDs
            pool=POOL,
            collect_fees=True,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        print("\nCompiling LPCloseIntent...")
        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "SUCCESS", f"LP Close compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        print(f"ActionBundle: {len(compilation_result.action_bundle.transactions)} transactions")

        print("Executing LP Close...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"LP Close execution failed: {execution_result.error}"
        close_meta = compilation_result.action_bundle.metadata
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions")

        # 5. Parse receipts - verify WithdrawnFromBins events
        parser = TraderJoeV2ReceiptParser()
        found_withdrawal_event = False
        lp_close_data = None

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")
            print(f"  Success: {tx_result.success}")

            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()

                parse_result = parser.parse_receipt(receipt_dict)
                if parse_result.success:
                    print(f"  Parsed events: {len(parse_result.events)}")
                    for event in parse_result.events:
                        print(f"    Event: {event.event_name} (type={event.event_type})")
                        if event.event_type == TraderJoeV2EventType.WITHDRAWN_FROM_BINS:
                            found_withdrawal_event = True
                            print("    -> WithdrawnFromBins event found!")

                    lp_close_data = parser.extract_lp_close_data(receipt_dict)
                    if lp_close_data:
                        print(
                            f"  LP Close data: amount0_collected={lp_close_data.amount0_collected}, "
                            f"amount1_collected={lp_close_data.amount1_collected}"
                        )

        assert found_withdrawal_event, "Must find WithdrawnFromBins event in receipts"

        # 6. Verify tokens returned (bilateral: both tokens should come back from
        # a two-sided LP position in the active bin range).
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)

        usdc_returned = usdc_after_close - usdc_before_close
        weth_returned = weth_after_close - weth_before_close

        print(f"\nUSDC returned: {format_token_amount(usdc_returned, usdc_decimals)}")
        print(f"WETH returned: {format_token_amount(weth_returned, weth_decimals)}")

        # Bilateral assertion: balances must not decrease, and at least one token
        # must have been returned (no-op guard).
        assert usdc_returned >= 0 and weth_returned >= 0, (
            f"Wallet balances must not decrease on a successful LP_CLOSE "
            f"(usdc_delta={usdc_returned}, weth_delta={weth_returned})"
        )
        assert usdc_returned > 0 or weth_returned > 0, (
            f"Must receive tokens back when closing position. "
            f"USDC returned: {usdc_returned}, WETH returned: {weth_returned}"
        )

        # 7. Verify position is now empty (LBPair shares fully burned)
        position_after = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=weth_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        if position_after is not None:
            residual_shares = sum(position_after.balances.values())
            assert residual_shares == 0, (
                f"All LBPair shares must be burned after close, "
                f"got residual={residual_shares} across bins={sorted(position_after.bin_ids)}"
            )
            assert len(position_after.bin_ids) == 0, (
                f"Position should be empty after close, still has {len(position_after.bin_ids)} bins"
            )

        # 8. Layer 5 — assert the real accounting pipeline persisted both
        # legs. Runs ONLY after every LP_CLOSE Layer-1–4 hard assert above,
        # so neither the VIB-4634 xfail nor an enricher regression can mask
        # core close logic. Enrichment also happens HERE, not at execute time
        # (CodeRabbit PR #2366). Persist the prior OPEN first (linkage + cost
        # basis), then CLOSE.
        assert lp_close_data is not None, "Layer-5 assertion needs parsed LPCloseData"
        open_accounting_result = _enrich_for_accounting(
            open_result, open_intent, funded_wallet, open_meta
        )
        close_accounting_result = _enrich_for_accounting(
            execution_result, close_intent, funded_wallet, close_meta
        )
        open_accounting_row = await _assert_accounting_persisted_or_xfail(
            layer5_accounting_harness,
            intent=open_intent,
            result=open_accounting_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_OPEN",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        close_accounting_row = await _assert_accounting_persisted_or_xfail(
            layer5_accounting_harness,
            intent=close_intent,
            result=close_accounting_result,
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
        # #2 bin-model directional null-contract on LP_CLOSE.
        _assert_bin_model_null_contract(close_payload, event_type="LP_CLOSE")
        assert close_payload["realized_pnl_usd"] is not None, (
            "open-then-close must compute realized PnL"
        )
        # #3 parser ↔ event exact scaled-int equality.
        dec0 = get_token_decimals(web3, tokens[close_payload["token0"]])
        dec1 = get_token_decimals(web3, tokens[close_payload["token1"]])
        _assert_close_parser_event_equality(close_payload, lp_close_data, dec0=dec0, dec1=dec1)

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_no_position(
        self,
        web3: Web3,
        funded_wallet: str,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Test #2: Close when no position exists.

        The LPCloseIntent should handle this gracefully - returning SUCCESS
        with empty transactions and a warning.

        Flow:
        1. Do NOT open a position
        2. Record balances BEFORE
        3. Create LPCloseIntent
        4. Compile - should return SUCCESS with empty transactions + warning
        5. Verify balances unchanged
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]

        print(f"\n{'=' * 80}")
        print("Test #2: LP Close - No Position Exists (TraderJoe V2)")
        print(f"{'=' * 80}")

        # 1. Verify no position exists
        position = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=weth_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        if position is not None and len(position.bin_ids) > 0:
            print("WARNING: Position already exists, skipping test")
            pytest.skip("Position already exists - snapshot isolation may have failed")

        # 2. Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)

        # 3. Create LPCloseIntent
        close_intent = LPCloseIntent(
            position_id="0",  # TraderJoe V2 uses bin-based positions, not NFT IDs
            pool=POOL,
            collect_fees=True,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # 4. Compile - should return SUCCESS with empty transactions + warning
        print("\nCompiling LPCloseIntent for non-existent position...")
        compilation_result = compiler.compile(close_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"LP Close compilation should succeed even with no position: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Should have empty transactions (no position to close)
        num_txs = len(compilation_result.action_bundle.transactions)
        print(f"ActionBundle has {num_txs} transactions (expected 0 for no position)")
        assert num_txs == 0, f"Expected 0 transactions when no position exists, got {num_txs}"

        # Should have a warning
        if compilation_result.warnings:
            print(f"Warnings: {compilation_result.warnings}")

        # 5. Verify balances unchanged
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)

        usdc_delta = usdc_after - usdc_before
        weth_delta = weth_after - weth_before

        assert usdc_delta == 0, f"USDC balance should be unchanged for no position, got delta: {usdc_delta}"
        assert weth_delta == 0, f"WETH balance should be unchanged for no position, got delta: {weth_delta}"

        print(f"USDC delta: {usdc_delta}")
        print(f"WETH delta: {weth_delta}")

        # Layer 5 — N/A by construction.
        # This is a compile-time (L1) no-op: the compiler returns SUCCESS with
        # an EMPTY ActionBundle (0 transactions), so the test never reaches
        # ExecutionOrchestrator and there is no ExecutionResult. The Layer-5
        # helpers operate on a real ExecutionResult (success →
        # assert_accounting_persisted; failed execution →
        # assert_no_accounting_on_failure). With nothing executed there is no
        # ledger/outbox/accounting surface to assert against — synthesising a
        # fake result here would test the helper, not this protocol. The
        # "0 TX emitted + balances unchanged" conservation checks above are
        # the books-side mirror for the no-op path (epic VIB-4591 #7).
        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
