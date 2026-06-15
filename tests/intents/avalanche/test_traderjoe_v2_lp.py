"""Production-grade LP Intent tests for TraderJoe V2 on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Opening liquidity positions in discrete price bins
- LPCloseIntent: Closing positions with various states

TraderJoe V2 uses a Liquidity Book model with discrete price bins and
ERC1155-like fungible LP tokens (not NFT positions like Uniswap/SushiSwap V3).

LP Close test cases:
  #1: Position has liquidity (normal close)
  #2: No position exists (wallet has no LP tokens)

NO MOCKING. All tests execute real on-chain transactions and verify state changes.

To run:
    uv run pytest tests/intents/avalanche/test_traderjoe_v2_lp.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config
from almanak.connectors.traderjoe_v2.receipt_parser import (
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

CHAIN_NAME = "avalanche"

# Pool: WAVAX/USDC with binStep=20 (0.2% fee tier)
# Token X: WAVAX (0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7)
# Token Y: USDC (0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E)
POOL = "WAVAX/USDC/20"
LP_AMOUNT_WAVAX = Decimal("2.0")  # amount0 (Token X = WAVAX)
LP_AMOUNT_USDC = Decimal("50")  # amount1 (Token Y = USDC)

# Price range in USDC-per-WAVAX terms (wide range to ensure both tokens are deposited)
# Note: TraderJoe V2 compiler uses bin_range around active bin, but the model requires these fields.
RANGE_LOWER = Decimal("5")
RANGE_UPPER = Decimal("500")

BIN_STEP = 20


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
_assert_accounting_persisted = _l5.assert_accounting_persisted
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
        amount0=LP_AMOUNT_WAVAX,  # Token X = WAVAX
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


@pytest.mark.avalanche
@pytest.mark.lp
class TestTraderJoeV2LPOpenIntent:
    """Test TraderJoe V2 LP Open using LPOpenIntent.

    Verifies the full Intent flow:
    - LPOpenIntent creation with pool and amounts
    - IntentCompiler generates correct LBRouter addLiquidity TX
    - Transactions execute successfully on-chain
    - Position has liquidity in bins (queried via adapter)
    - Balance changes are correct
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_wavax_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Test opening a WAVAX/USDC LP position using LPOpenIntent.

        Flow:
        1. Record balances BEFORE
        2. Create LPOpenIntent for WAVAX/USDC pool
        3. Compile to ActionBundle using IntentCompiler
        4. Execute via ExecutionOrchestrator
        5. Parse receipts - verify DepositedToBins events, extract bin IDs
        6. Query position via adapter - verify bin_ids not empty, amounts > 0
        7. Verify balance changes
        8. Layer 5 - assert the real accounting pipeline persisted LP_OPEN
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wavax_addr = tokens["WAVAX"]
        fail_if_traderjoe_pool_missing(web3, CHAIN_NAME, wavax_addr, usdc_addr, 20)

        usdc_decimals = get_token_decimals(web3, usdc_addr)
        wavax_decimals = get_token_decimals(web3, wavax_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Open WAVAX/USDC via LPOpenIntent (TraderJoe V2)")
        print(f"{'=' * 80}")
        print(f"Pool: {POOL}")
        print(f"Amount WAVAX (token X): {LP_AMOUNT_WAVAX}")
        print(f"Amount USDC (token Y): {LP_AMOUNT_USDC}")

        # 1. Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_before = get_token_balance(web3, wavax_addr, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"WAVAX before: {format_token_amount(wavax_before, wavax_decimals)}")

        # 2. Create LPOpenIntent
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WAVAX,  # Token X = WAVAX
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

        # 6. Query position via adapter - verify bin_ids not empty, amounts > 0
        position = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=wavax_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        assert position is not None, "Position must exist after LP open"
        assert len(position.bin_ids) > 0, "Position must have bin IDs"
        # Verify LP token balances are non-zero (amount_x/amount_y may be 0 due to SDK getBin() limitations)
        total_lp_balance = sum(position.balances.values())
        assert total_lp_balance > 0, (
            f"Position must have non-zero LP token balances, got total={total_lp_balance}"
        )
        print(
            f"On-chain position: {len(position.bin_ids)} bins, "
            f"total LP balance={total_lp_balance}, "
            f"amount_x={position.amount_x}, amount_y={position.amount_y}"
        )

        # 7. Verify balance changes
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_after = get_token_balance(web3, wavax_addr, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        wavax_spent = wavax_before - wavax_after

        print(f"\nUSDC spent: {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"WAVAX spent: {format_token_amount(wavax_spent, wavax_decimals)}")

        # At least one token must have been deposited
        assert usdc_spent > 0 or wavax_spent > 0, "Must deposit at least one token into LP"

        # Amounts spent must not exceed desired amounts
        expected_usdc_max = int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        expected_wavax_max = int(LP_AMOUNT_WAVAX * Decimal(10**wavax_decimals))
        assert usdc_spent <= expected_usdc_max, f"USDC spent ({usdc_spent}) exceeds desired ({expected_usdc_max})"
        assert wavax_spent <= expected_wavax_max, f"WAVAX spent ({wavax_spent}) exceeds desired ({expected_wavax_max})"

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
        accounting_row = await _assert_accounting_persisted(
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
        _assert_bin_model_null_contract(payload, event_type="LP_OPEN")
        assert Decimal(payload["amount0"]) >= 0
        assert Decimal(payload["amount1"]) >= 0

        print("\nALL CHECKS PASSED")


# =============================================================================
# LPCloseIntent Tests
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.lp
class TestTraderJoeV2LPCloseIntent:
    """Test TraderJoe V2 LP Close using LPCloseIntent.

    Test cases:
    #1: Position has liquidity (normal LP close)
    #2: No position exists (wallet has no LP tokens)

    Note: TraderJoe V2 does not have the "decreased but not collected" edge case
    because removeLiquidity removes and returns tokens in one step.
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
        6. Verify tokens returned to wallet (balance deltas > 0)
        7. Verify position is now empty
        8. Layer 5 - assert the real accounting pipeline persisted LP_OPEN + LP_CLOSE
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wavax_addr = tokens["WAVAX"]
        fail_if_traderjoe_pool_missing(web3, CHAIN_NAME, wavax_addr, usdc_addr, 20)
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        wavax_decimals = get_token_decimals(web3, wavax_addr)

        print(f"\n{'=' * 80}")
        print("Test #1: LP Close - Position with Liquidity (TraderJoe V2)")
        print(f"{'=' * 80}")

        # 1. Open position. Capture the RAW OPEN result + intent + bundle
        # metadata now, but DEFER both enrichment and Layer-5 persistence to
        # the end of the test so the LP_CLOSE compile/execute/parse/balance
        # hard asserts below run first and an enrichment/persistence
        # regression cannot mask them (gemini + CodeRabbit PR #2366). All
        # enrichment + Layer-5 persistence (OPEN then CLOSE) happens at step 8
        # as live assertions (VIB-4634 fixed — pool_address is now the
        # canonical LBPair address stamped by the receipt parser).
        open_intent, open_result, open_meta = await _open_position_for_accounting(
            funded_wallet, orchestrator, price_oracle, anvil_rpc_url
        )
        print("Opened LP position via LPOpenIntent")

        # 2. Verify it has liquidity
        position = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=wavax_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        assert position is not None, "Position must exist before close"
        assert len(position.bin_ids) > 0, "Position must have bin IDs"
        print(f"Position: {len(position.bin_ids)} bins, amount_x={position.amount_x}, amount_y={position.amount_y}")

        # 3. Record balances BEFORE close
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_before_close = get_token_balance(web3, wavax_addr, funded_wallet)

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

        # 6. Verify tokens returned
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_after_close = get_token_balance(web3, wavax_addr, funded_wallet)

        usdc_returned = usdc_after_close - usdc_before_close
        wavax_returned = wavax_after_close - wavax_before_close

        print(f"\nUSDC returned: {format_token_amount(usdc_returned, usdc_decimals)}")
        print(f"WAVAX returned: {format_token_amount(wavax_returned, wavax_decimals)}")

        # At least one token must be returned (was deposited in LP)
        assert usdc_returned > 0 or wavax_returned > 0, (
            f"Must receive tokens back when closing position. "
            f"USDC returned: {usdc_returned}, WAVAX returned: {wavax_returned}"
        )

        # Layer 3 (parser amounts) + Layer 4 (exact wallet deltas): the
        # parser must decode a non-zero close, and the wallet deltas must
        # equal the parser-extracted amounts exactly — token X = WAVAX
        # (amount0), token Y = USDC (amount1). Mirrors the explicit-bin_ids
        # close test's hard asserts (CodeRabbit PR #2366).
        assert lp_close_data is not None, "Receipt parser must decode an LP_CLOSE"
        parsed_amount0 = int(lp_close_data.amount0_collected or 0)
        parsed_amount1 = int(lp_close_data.amount1_collected or 0)
        assert parsed_amount0 > 0 or parsed_amount1 > 0, (
            f"Parser-extracted close amounts must be non-zero "
            f"(amount0={parsed_amount0}, amount1={parsed_amount1})"
        )
        assert wavax_returned == parsed_amount0, (
            f"WAVAX wallet delta ({wavax_returned}) must equal parser "
            f"amount0_collected ({parsed_amount0})"
        )
        assert usdc_returned == parsed_amount1, (
            f"USDC wallet delta ({usdc_returned}) must equal parser "
            f"amount1_collected ({parsed_amount1})"
        )

        # 7. Verify position is now empty
        position_after = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=wavax_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        if position_after is not None:
            assert len(position_after.bin_ids) == 0, (
                f"Position should be empty after close, still has {len(position_after.bin_ids)} bins"
            )

        # 8. Layer 5 — assert the real accounting pipeline persisted both
        # legs. Runs ONLY after every LP_CLOSE Layer-1–4 hard assert above,
        # so an enricher/persistence regression cannot mask
        # core close logic. Enrichment also happens HERE, not at execute
        # time (CodeRabbit PR #2366). Persist the prior OPEN first
        # (linkage + cost basis), then CLOSE.
        open_accounting_result = _enrich_for_accounting(
            open_result, open_intent, funded_wallet, open_meta
        )
        close_accounting_result = _enrich_for_accounting(
            execution_result, close_intent, funded_wallet, close_meta
        )
        open_accounting_row = await _assert_accounting_persisted(
            layer5_accounting_harness,
            intent=open_intent,
            result=open_accounting_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_OPEN",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        close_accounting_row = await _assert_accounting_persisted(
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
        wavax_addr = tokens["WAVAX"]

        print(f"\n{'=' * 80}")
        print("Test #2: LP Close - No Position Exists (TraderJoe V2)")
        print(f"{'=' * 80}")

        # 1. Assert no position exists at test start (isolation invariant).
        #
        # VIB-4828: this used to ``pytest.skip`` when a position was found,
        # defensively masking suspected Anvil snapshot/revert leakage of
        # TraderJoe V2 LBPair bin state. That hid a real isolation failure
        # instead of surfacing it. Under the default-on Zodiac model
        # (`.claude/rules/intent-tests.md`) every test runs against a fresh
        # per-test Safe (`zodiac_safe`, function-scoped), so ``funded_wallet``
        # is a brand-new owner each test and cannot inherit a sibling's
        # position — the precondition holds structurally. Assert it so any
        # future regression (e.g. reverting to a shared module-scoped wallet
        # without working snapshot isolation) fails loudly here rather than
        # silently skipping.
        position = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=wavax_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        assert position is None or len(position.bin_ids) == 0, (
            "VIB-4828: a TraderJoe V2 position already exists at the start of the "
            "no-position test — test isolation has leaked LBPair bin state across "
            f"tests (bins={sorted(position.bin_ids) if position else []}). "
            "The no-position close path cannot be verified against a dirty wallet."
        )

        # 2. Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_before = get_token_balance(web3, wavax_addr, funded_wallet)

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
        wavax_after = get_token_balance(web3, wavax_addr, funded_wallet)

        usdc_delta = usdc_after - usdc_before
        wavax_delta = wavax_after - wavax_before

        assert usdc_delta == 0, f"USDC balance should be unchanged for no position, got delta: {usdc_delta}"
        assert wavax_delta == 0, f"WAVAX balance should be unchanged for no position, got delta: {wavax_delta}"

        print(f"USDC delta: {usdc_delta}")
        print(f"WAVAX delta: {wavax_delta}")

        # Layer 5 — N/A by construction.
        # This is a compile-time (L1) no-op: the compiler returns SUCCESS with
        # an EMPTY ActionBundle (0 transactions), so the test never reaches
        # ExecutionOrchestrator and there is no ExecutionResult. The Layer-5
        # helpers operate on a real ExecutionResult — with nothing executed
        # there is no ledger/outbox/accounting surface to assert against, and
        # synthesising a fake result here would test the helper, not this
        # protocol. The "0 TX emitted + balances unchanged" conservation
        # checks above are the books-side mirror for the no-op path
        # (epic VIB-4591 #7).
        print("\nALL CHECKS PASSED")


@pytest.mark.avalanche
@pytest.mark.lp
class TestTraderJoeV2LPCloseWithBinIds:
    """Test the VIB-3741 fix: passing bin_ids to LP_CLOSE protocol_params.

    The strategy-side fix (sweep across 13 TraderJoe V2 LP strategies) is
    only useful if the compiler honours protocol_params["bin_ids"] and the
    resulting close burns liquidity in exactly those bins. This test
    exercises the contract end-to-end:

        1. LP_OPEN — capture bin_ids from the on-chain receipt.
        2. LP_CLOSE with protocol_params={"bin_ids": [...]} — drive the
           targeted withdrawal path (NOT the heuristic ±50 scan).
        3. Assert on-chain liquidity is exactly zero across every bin the
           position ever held — i.e., no liquidity stranded.
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_with_bin_ids_zeroes_all_bins(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """VIB-3741: explicit bin_ids close must zero on-chain liquidity in all bins."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wavax_addr = tokens["WAVAX"]
        fail_if_traderjoe_pool_missing(web3, CHAIN_NAME, wavax_addr, usdc_addr, 20)

        print(f"\n{'=' * 80}")
        print("VIB-3741 Test: LP Close with explicit bin_ids zeroes all bins")
        print(f"{'=' * 80}")

        # 1. Open LP position
        open_intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WAVAX,
            amount1=LP_AMOUNT_USDC,
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
        open_compilation = compiler.compile(open_intent)
        assert open_compilation.status.value == "SUCCESS", f"LP Open compilation failed: {open_compilation.error}"
        assert open_compilation.action_bundle is not None, "LP Open ActionBundle must be created"
        open_execution = await orchestrator.execute(open_compilation.action_bundle)
        assert open_execution.success, f"LP Open execution failed: {open_execution.error}"
        open_meta = open_compilation.action_bundle.metadata

        # 2. Extract bin_ids from receipts
        parser = TraderJoeV2ReceiptParser()
        bin_ids: list[int] = []
        for tx_result in open_execution.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                extracted = parser.extract_bin_ids(receipt_dict)
                if extracted:
                    bin_ids = list(extracted)
                    break
        assert bin_ids, "Receipt must surface bin_ids from DepositedToBins event"
        print(f"Captured {len(bin_ids)} bin_ids from LP_OPEN receipt: {bin_ids[:5]}...")

        # 3. Sanity: position is live across exactly those bins
        position_before = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=wavax_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        assert position_before is not None
        assert set(position_before.bin_ids) == set(bin_ids), (
            f"Adapter bin_ids ({sorted(position_before.bin_ids)}) must match "
            f"receipt bin_ids ({sorted(bin_ids)})"
        )
        total_before = sum(position_before.balances.values())
        assert total_before > 0, "Position must hold non-zero LP tokens before close"

        # 4. Close LP — explicitly pass bin_ids in protocol_params (the VIB-3741 fix)
        # Capture balances BEFORE the close so we can verify the returned-token deltas.
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        wavax_decimals = get_token_decimals(web3, wavax_addr)
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_before_close = get_token_balance(web3, wavax_addr, funded_wallet)

        close_intent = LPCloseIntent(
            position_id=POOL,
            pool=POOL,
            collect_fees=True,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
            protocol_params={"bin_ids": list(bin_ids)},
        )
        close_compilation = compiler.compile(close_intent)
        assert close_compilation.status.value == "SUCCESS", (
            f"LP Close compilation failed: {close_compilation.error}"
        )
        assert close_compilation.action_bundle is not None
        print(
            f"LP Close compiled with explicit bin_ids "
            f"({len(close_compilation.action_bundle.transactions)} transactions)"
        )

        close_execution = await orchestrator.execute(close_compilation.action_bundle)
        assert close_execution.success, f"LP Close execution failed: {close_execution.error}"
        close_meta = close_compilation.action_bundle.metadata

        # 5. Receipt parsing layer — verify the close emitted WithdrawnFromBins
        # and the parser extracts non-zero collected amounts.
        found_withdrawal_event = False
        parsed_amount0 = 0
        parsed_amount1 = 0
        layer5_close_data = None
        for tx_result in close_execution.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result.success, (
                f"Close receipt parsing must succeed, got: {parse_result.error}"
            )
            for event in parse_result.events:
                if event.event_type == TraderJoeV2EventType.WITHDRAWN_FROM_BINS:
                    found_withdrawal_event = True
            lp_close_data = parser.extract_lp_close_data(receipt_dict)
            if lp_close_data:
                parsed_amount0 += int(lp_close_data.amount0_collected or 0)
                parsed_amount1 += int(lp_close_data.amount1_collected or 0)
                if layer5_close_data is None:
                    layer5_close_data = lp_close_data
        assert found_withdrawal_event, (
            "Close must emit a WithdrawnFromBins event (receipt-parsing layer)"
        )
        assert parsed_amount0 > 0 or parsed_amount1 > 0, (
            "Parser-extracted close amounts must be non-zero "
            f"(amount0={parsed_amount0}, amount1={parsed_amount1})"
        )

        # 6. Balance-delta layer — returned-token deltas must match parser output and
        #    must move on at least one side (no-op guard).
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_after_close = get_token_balance(web3, wavax_addr, funded_wallet)
        usdc_returned = usdc_after_close - usdc_before_close
        wavax_returned = wavax_after_close - wavax_before_close
        print(f"USDC returned: {format_token_amount(usdc_returned, usdc_decimals)}")
        print(f"WAVAX returned: {format_token_amount(wavax_returned, wavax_decimals)}")
        assert usdc_returned >= 0 and wavax_returned >= 0, (
            "Wallet balances must not decrease on a successful LP_CLOSE "
            f"(usdc_delta={usdc_returned}, wavax_delta={wavax_returned})"
        )
        assert usdc_returned > 0 or wavax_returned > 0, (
            "At least one token must be returned to the wallet on close (no-op guard)"
        )
        # Parser-extracted amounts (token X = WAVAX, token Y = USDC) should match
        # what the wallet actually received, modulo gas and rounding noise.
        assert wavax_returned == parsed_amount0, (
            f"WAVAX wallet delta ({wavax_returned}) must equal parser amount0 "
            f"({parsed_amount0})"
        )
        assert usdc_returned == parsed_amount1, (
            f"USDC wallet delta ({usdc_returned}) must equal parser amount1 "
            f"({parsed_amount1})"
        )

        # 7. Verify on-chain liquidity is exactly zero across every bin
        position_after = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=wavax_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        if position_after is not None:
            residual_total = sum(position_after.balances.values())
            assert residual_total == 0, (
                f"Position must hold zero LP tokens across all bins after close, "
                f"got residual={residual_total} across bins={sorted(position_after.bin_ids)}"
            )
            assert len(position_after.bin_ids) == 0, (
                f"All bin IDs must be cleared after close, "
                f"still holding bins={sorted(position_after.bin_ids)}"
            )

        # 8. Layer 5 — assert the real accounting pipeline persisted
        # LP_OPEN then LP_CLOSE (explicit-bin_ids close path). Enrichment
        # runs HERE, after all Layer-1–4 hard asserts (CodeRabbit PR #2366).
        open_accounting_result = _enrich_for_accounting(
            open_execution, open_intent, funded_wallet, open_meta
        )
        close_accounting_result = _enrich_for_accounting(
            close_execution, close_intent, funded_wallet, close_meta
        )
        open_accounting_row = await _assert_accounting_persisted(
            layer5_accounting_harness,
            intent=open_intent,
            result=open_accounting_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_OPEN",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        _assert_identity(open_accounting_row, event_type="LP_OPEN", wallet=funded_wallet)
        _assert_bin_model_null_contract(_payload(open_accounting_row), event_type="LP_OPEN")

        assert layer5_close_data is not None, "Layer-5 assertion needs parsed LPCloseData"
        close_accounting_row = await _assert_accounting_persisted(
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
        _assert_bin_model_null_contract(close_payload, event_type="LP_CLOSE")
        assert close_payload["realized_pnl_usd"] is not None, (
            "open-then-close must compute realized PnL"
        )
        # #3 parser ↔ event exact scaled-int equality.
        dec0 = get_token_decimals(web3, tokens[close_payload["token0"]])
        dec1 = get_token_decimals(web3, tokens[close_payload["token1"]])
        _assert_close_parser_event_equality(close_payload, layer5_close_data, dec0=dec0, dec1=dec1)

        print("VIB-3741 PASS: explicit bin_ids close zeroed all bins")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
