"""Production-grade LP intent tests for Aerodrome Slipstream on Base (VIB-4434 W3).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for the
Slipstream concentrated-liquidity surface (Uniswap V3-style NFT positions via
the Slipstream NonfungiblePositionManager at ``AERODROME["base"]["cl_nft"]``):

- LPOpenIntent (``protocol="aerodrome_slipstream"``)            — single LP_OPEN
- LPCloseIntent (``protocol="aerodrome_slipstream"``)           — three position-state cases
- CollectFeesIntent (``protocol="aerodrome_slipstream"``)       — standalone fee harvest

LP_CLOSE compiles to a two-tx bundle on Slipstream (``decreaseLiquidity`` then
``collect``) — there is no ``burn`` step (audit B6 / compiler
``compile_lp_close_aerodrome_slipstream`` / adapter ``remove_cl_liquidity``).

NO MOCKING. All tests execute real on-chain transactions on a Base Anvil fork
and verify state changes. Mirrors the rigour of
``tests/intents/base/test_pancakeswap_v3_lp.py``.

Default-on Zodiac applies per the per-chain conftest — the test body is
unchanged and routes through Safe + Roles + ``execTransactionWithRole``.

To run:
    uv run pytest tests/intents/base/test_aerodrome_slipstream_lp.py -v -s
"""

import json
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.core.contracts import AERODROME
from almanak.framework.connectors.aerodrome.receipt_parser import (
    AerodromeSlipstreamReceiptParser,
)
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
)
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents import (
    IntentCompiler,
    LPCloseIntent,
    LPOpenIntent,
    SwapIntent,
)
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType
from tests.intents._lp_setup_helpers import (
    collect_all_tokens,
    decrease_all_liquidity,
    query_position_liquidity,
)
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

CHAIN_NAME = "base"
PROTOCOL = "aerodrome_slipstream"

# Slipstream NonfungiblePositionManager (cl_nft) on Base.
POSITION_MANAGER = AERODROME["base"]["cl_nft"]

# Pool: WETH/USDC, tick_spacing=200 (the canonical Slipstream WETH/USDC volatile
# pool on Base — same tick_spacing the ``demo_aerodrome_slipstream_lp`` demo
# strategy uses by default; verified on-chain via the cl_factory).
#
# Token order: Base WETH (0x4200…) < USDC (0x8335…) by address, so
# token0=WETH, token1=USDC. Ticks therefore measure log(USDC_raw/WETH_raw),
# which is negative for any realistic ETH price (USDC has 6 decimals vs WETH's
# 18, so the raw ratio is ~1e-9 even at ETH=$3000).
POOL = "WETH/USDC/200"
LP_AMOUNT_WETH = Decimal("0.1")  # amount0 (WETH, token0 on Base)
LP_AMOUNT_USDC = Decimal("250")  # amount1 (USDC, token1 on Base)

# Wide tick range that covers any realistic ETH price across forks.
# Slipstream V3 ticks max out at ±887272; snapped to tick_spacing=200 these
# round to ±887200. -300000 → +200000 covers an ETH price range from below $1
# to far above any historical high, so the position deposits both tokens
# regardless of the fork-block ETH price.
RANGE_LOWER = Decimal("-300000")  # must be tick-integer-valued
RANGE_UPPER = Decimal("200000")


# =============================================================================
# Layer-5 accounting helpers (mirrors tests/intents/base/test_pancakeswap_v3_lp.py)
# =============================================================================
#
# Aerodrome Slipstream is concentrated-liquidity (Uniswap-V3-shaped): same
# ``lp:{protocol}:{chain}:{wallet}:{pool}`` position key, same ``LPCloseData``,
# same ``lp_handler.py`` path, and ``AerodromeSlipstreamReceiptParser`` extracts
# the V3-style ``lp_open_data`` (with ticks). The Layer-5 contract is therefore
# the V3 directional null-contract — identical to PancakeSwap V3 / SushiSwap V3.


def _execution_context(wallet: str) -> ExecutionContext:
    return ExecutionContext(
        strategy_id="layer5-aerodrome-slipstream-lp",
        chain=CHAIN_NAME,
        wallet_address=wallet,
        protocol=PROTOCOL,
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


def _assert_no_lot_id(row: dict, payload: dict) -> None:
    assert "lot_id" not in row
    assert "lot_id" not in payload


def _assert_parser_event_equality(payload: dict, lp_close_data, *, dec0: int, dec1: int) -> None:
    """Parser ↔ event exact equality, honoring the Empty≠Zero≠None contract.

    ``LPCloseData.fees{0,1}`` default to ``None`` when the parser did not
    measure fees separately (Empty). Aerodrome Slipstream's clean-close path
    takes that branch, and the LP handler correctly persists an *unmeasured*
    ``None`` (it does NOT fabricate a measured-zero — verified on a real
    Anvil-fork run: ``lp_close_data.fees0/1 is None`` →
    ``payload["fees{0,1}_collected"] is None``). This is the merged
    **SushiSwap V3** directional fee-contract (the better precedent per
    VIB-4597), NOT the PancakeSwap-style ``Decimal(payload[...]) == "0"``
    reconciliation which crashes on the legitimately-``None`` payload.

    Directional contract per epic VIB-4591 decision #5 / blueprints/27:

    * parser reading concrete  → payload MUST equal it exactly.
    * parser reading ``None`` (Empty) → payload may be ``None`` (unmeasured)
      or measured-zero ``Decimal('0')``; it must NEVER fabricate a non-zero
      fee.
    """
    assert Decimal(payload["amount0"]) == _to_human(lp_close_data.amount0_collected, dec0)
    assert Decimal(payload["amount1"]) == _to_human(lp_close_data.amount1_collected, dec1)
    for field, raw in (
        ("fees0_collected", lp_close_data.fees0),
        ("fees1_collected", lp_close_data.fees1),
    ):
        dec = dec0 if field == "fees0_collected" else dec1
        parser_human = _to_human(raw, dec)
        payload_raw = payload[field]
        payload_fee = None if payload_raw is None or payload_raw == "" else Decimal(payload_raw)
        if parser_human is not None:
            assert payload_fee == parser_human, (
                f"{field}: payload {payload_fee!r} must equal parser reading {parser_human!r}"
            )
        else:
            assert payload_fee is None or payload_fee == Decimal("0"), (
                f"{field}: parser did not measure fees (Empty); payload must be "
                f"unmeasured (None) or measured-zero (0), never a fabricated {payload_fee!r}"
            )


async def _open_position_for_accounting(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
):
    """Open a Slipstream LP position; return (position_id, intent, enriched_result)."""
    intent = LPOpenIntent(
        pool=POOL,
        amount0=LP_AMOUNT_WETH,
        amount1=LP_AMOUNT_USDC,
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        protocol="aerodrome_slipstream",
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

    parser = AerodromeSlipstreamReceiptParser(chain=CHAIN_NAME)
    position_id: int | None = None
    for tx_result in enriched.transaction_results:
        if tx_result.receipt:
            pos_id = parser.extract_position_id(tx_result.receipt.to_dict())
            if pos_id is not None:
                position_id = int(pos_id)
    assert position_id is not None, "Failed to extract Slipstream position ID from LP Open receipt"
    return position_id, intent, enriched


# =============================================================================
# Helpers
# =============================================================================
#
# ``query_position_liquidity``, ``decrease_all_liquidity``, and
# ``collect_all_tokens`` live in ``tests/intents/_lp_setup_helpers.py``.
# Slipstream's NPM is byte-compatible with the Uniswap V3 NPM for the
# ``positions(uint256)`` / ``decreaseLiquidity(...)`` / ``collect(...)``
# selectors (only ``mint`` differs because Slipstream uses ``tickSpacing``
# instead of ``fee``), so the same helpers work unchanged here when the
# Slipstream cl_nft is passed as the ``position_manager`` argument.


async def _open_position_via_intent(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> int:
    """Open a Slipstream LP position via LPOpenIntent and return the NFT tokenId."""
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


@pytest.mark.base
@pytest.mark.lp
class TestAerodromeSlipstreamLPOpenIntent:
    """LP Open on Aerodrome Slipstream CL — full 4-layer flow."""

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
        """Open a WETH/USDC Slipstream LP position via LPOpenIntent.

        Layers:
        1. Compile     — LPOpenIntent → ActionBundle (single ``mint`` tx).
        2. Execute     — ExecutionOrchestrator submits the bundle.
        3. Parse       — AerodromeSlipstreamReceiptParser.extract_position_id.
        4. Balance     — token0/token1 wallet deltas match LP_OPEN amounts.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP Open WETH/USDC via LPOpenIntent (Aerodrome Slipstream)")
        print(f"{'=' * 80}")
        print(f"Pool: {POOL}")
        print(f"Amount WETH (token0): {LP_AMOUNT_WETH}")
        print(f"Amount USDC (token1): {LP_AMOUNT_USDC}")
        print(f"Tick range: [{RANGE_LOWER} - {RANGE_UPPER}]")
        print(f"Position Manager (cl_nft): {POSITION_MANAGER}")

        # 1. Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        assert usdc_before > 0, "funded_wallet has no USDC — fixture funding failed"
        assert weth_before > 0, "funded_wallet has no WETH — fixture funding failed"

        # 2. Create the intent
        intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WETH,
            amount1=LP_AMOUNT_USDC,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="aerodrome_slipstream",
            chain=CHAIN_NAME,
        )

        # Layer 1 — Compile
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None
        # Slipstream LP_OPEN compiles to N ERC20 approves + one mint tx; the
        # canonical case is at least one tx (mint itself), with optional
        # approves preceding.
        assert len(compilation_result.action_bundle.transactions) >= 1

        # Layer 2 — Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        execution_result = _enrich_for_accounting(
            execution_result,
            intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
        )

        # Layer 3 — Parse: extract position ID from the Mint/Transfer receipt
        parser = AerodromeSlipstreamReceiptParser(chain=CHAIN_NAME)
        position_id: int | None = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                pos_id_str = parser.extract_position_id(tx_result.receipt.to_dict())
                if pos_id_str is not None:
                    position_id = int(pos_id_str)
        assert position_id is not None, (
            "Must extract position ID from Slipstream mint receipt"
        )
        print(f"\nPosition tokenId: {position_id}")

        # Position must have on-chain liquidity after mint.
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have positive liquidity, got {liquidity}"

        # Layer 4 — Balance deltas
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        weth_spent = weth_before - weth_after

        print(f"USDC spent: {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"WETH spent: {format_token_amount(weth_spent, weth_decimals)}")

        assert usdc_spent > 0 or weth_spent > 0, "Must deposit at least one token into LP"

        expected_usdc_max = int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        expected_weth_max = int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))
        assert usdc_spent <= expected_usdc_max, (
            f"USDC spent ({usdc_spent}) exceeds desired ({expected_usdc_max})"
        )
        assert weth_spent <= expected_weth_max, (
            f"WETH spent ({weth_spent}) exceeds desired ({expected_weth_max})"
        )

        # Layer 5 — assert the real accounting pipeline persisted LP_OPEN.
        # Slipstream is V3-concentrated: ticks/liquidity ship via the
        # structured ``lp_open_data`` struct, so the V3 OPEN bracket must be
        # populated and the V4 ``position_hash`` must NOT be fabricated.
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
        assert payload["pool_address"].startswith("0x"), (
            "Slipstream LP_OPEN must persist the canonical on-chain pool address"
        )
        assert Decimal(payload["amount0"]) >= 0
        assert Decimal(payload["amount1"]) >= 0
        assert payload["position_hash"] is None, (
            "Aerodrome Slipstream LP_OPEN must not fabricate a V4 position_hash"
        )
        assert payload["tick_lower"] is not None
        assert payload["tick_upper"] is not None
        assert payload["liquidity"] is not None
        assert payload["current_tick"] is not None
        assert payload["in_range"] is True

        print("\nALL CHECKS PASSED")


# =============================================================================
# LPCloseIntent Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.lp
class TestAerodromeSlipstreamLPCloseIntent:
    """LP Close on Aerodrome Slipstream CL — three position-state cases.

    Slipstream LP_CLOSE compiles to a two-tx bundle: ``decreaseLiquidity`` then
    ``collect`` (no ``burn`` — audit B6). The cases mirror the standard
    Uniswap-V3-family LP-close edge matrix.
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
        """Case #1: Close a position that has liquidity (normal close).

        Layers:
        1. Compile     — LPCloseIntent → ActionBundle (decreaseLiquidity+collect).
        2. Execute     — both txs land on-chain.
        3. Parse       — extract_lp_close_data reports collected amounts > 0.
        4. Balance     — token0/token1 wallet deltas positive (principal returned).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        # Open via the accounting helper so Layer 5 has the prior LP_OPEN row
        # for linkage + cost basis.
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
        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity > 0, f"Position must have liquidity before close, got {liquidity}"

        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)

        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="aerodrome_slipstream",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(close_intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"LP Close compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None
        # Slipstream LP_CLOSE = decreaseLiquidity + collect = 2 txs.
        assert len(compilation_result.action_bundle.transactions) == 2, (
            "Slipstream LP_CLOSE must compile to exactly two transactions "
            "(decreaseLiquidity + collect, no burn)"
        )

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"LP Close execution failed: {execution_result.error}"
        execution_result = _enrich_for_accounting(
            execution_result,
            close_intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
        )

        # Layer 3 strict: parse_receipt success on every receipt + extract
        # lp_close_data on at least one. Mirrors the PancakeSwap V3 LP_CLOSE
        # pattern (CodeRabbit review on PR #2331 — pin LP_CLOSE receipts to
        # exact wallet deltas via parse_receipt + extract_lp_close_data).
        parser = AerodromeSlipstreamReceiptParser(chain=CHAIN_NAME)
        lp_close_data = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parse_result = parser.parse_receipt(receipt_dict)
                assert parse_result.success, (
                    f"Receipt parser must succeed on a confirmed receipt; "
                    f"error={parse_result.error}"
                )
                data = parser.extract_lp_close_data(receipt_dict)
                if data:
                    lp_close_data = data

        assert lp_close_data is not None, "Must extract LP close data from a receipt"
        assert (
            lp_close_data.amount0_collected > 0 or lp_close_data.amount1_collected > 0
        ), "At least one collected amount must be positive"

        # Layer 4 strict: wallet deltas EXACTLY equal parsed Collect amounts.
        # POOL = "WETH/USDC/200" on Base → token0=WETH (0x4200…), token1=USDC
        # (0x8335…) by address ordering. The Slipstream NPM collect() routes
        # tokens directly to ``recipient=wallet`` (no unwrap), so the parsed
        # amount0_collected/amount1_collected MUST equal the wallet deltas
        # to the wei (CodeRabbit "pin LP_CLOSE receipts to exact wallet
        # deltas" — replaces the loose `> 0` asserts).
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_returned = usdc_after_close - usdc_before_close
        weth_returned = weth_after_close - weth_before_close

        print(f"USDC returned: {format_token_amount(usdc_returned, usdc_decimals)}")
        print(f"WETH returned: {format_token_amount(weth_returned, weth_decimals)}")

        if int(weth_addr, 16) < int(usdc_addr, 16):
            parsed_weth, parsed_usdc = (
                lp_close_data.amount0_collected,
                lp_close_data.amount1_collected,
            )
        else:
            parsed_usdc, parsed_weth = (
                lp_close_data.amount0_collected,
                lp_close_data.amount1_collected,
            )
        assert weth_returned == parsed_weth, (
            f"WETH wallet delta must equal parsed Collect amount exactly. "
            f"wallet={weth_returned}, parsed={parsed_weth}"
        )
        assert usdc_returned == parsed_usdc, (
            f"USDC wallet delta must equal parsed Collect amount exactly. "
            f"wallet={usdc_returned}, parsed={parsed_usdc}"
        )

        # Layer 5 — assert the real accounting pipeline persisted LP_CLOSE.
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
        # #2 directional null-contract on LP_CLOSE (V3-shaped: no fabricated V4 hash).
        assert close_payload["position_hash"] is None, (
            "Aerodrome Slipstream LP_CLOSE must not fabricate a V4 position_hash"
        )
        assert close_payload["realized_pnl_usd"] is not None, (
            "open-then-close must compute realized PnL"
        )
        # #3 parser ↔ event exact scaled-int equality.
        dec0 = get_token_decimals(web3, tokens[close_payload["token0"]])
        dec1 = get_token_decimals(web3, tokens[close_payload["token1"]])
        _assert_parser_event_equality(close_payload, lp_close_data, dec0=dec0, dec1=dec1)

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_position_no_liquidity_no_fees(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Case #2: Close a position with no liquidity and no owed tokens.

        After externally decreasing and collecting, LPCloseIntent must be a
        no-op (compiles to an empty ActionBundle) and ERC-20 balances stay
        untouched (mirrors VIB-3644 for UniV3 / PancakeSwap V3 / SushiSwap V3).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]

        # Open via the accounting helper so Layer 5 persists the OPEN — the
        # no-op close assertion is then not vacuous against a fresh harness.
        position_id, open_intent, open_result = await _open_position_for_accounting(
            funded_wallet, orchestrator, price_oracle, anvil_rpc_url,
        )
        await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=open_intent,
            result=open_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_OPEN",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )

        # The Slipstream NPM exposes the same ``decreaseLiquidity`` /
        # ``collect`` selectors as the V3 NPM (only ``mint`` differs), so the
        # shared V3 setup helpers work for Slipstream by passing the cl_nft as
        # ``position_manager`` and the Slipstream protocol literal so the
        # late-binding manifest covers the right selectors.
        await decrease_all_liquidity(
            web3, orchestrator,
            chain=CHAIN_NAME, protocol=PROTOCOL,
            position_manager=POSITION_MANAGER, token_id=position_id,
        )
        await collect_all_tokens(
            web3, orchestrator,
            chain=CHAIN_NAME, protocol=PROTOCOL,
            position_manager=POSITION_MANAGER, token_id=position_id,
            recipient=funded_wallet,
        )

        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity == 0, f"Expected 0 liquidity after decrease, got {liquidity}"

        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)

        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="aerodrome_slipstream",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(close_intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"LP Close compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Layer 5 — snapshot the LP_CLOSE row set BEFORE the no-op close so the
        # post-check is scoped to this run, not worker-shared store history.
        close_rows_before = await layer5_accounting_harness.store.get_accounting_events(
            "layer5-intent-test",
            event_type="LP_CLOSE",
            limit=20,
        )

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, (
            "LP Close on empty Slipstream position must succeed as a no-op"
        )
        assert compilation_result.action_bundle.metadata.get("no_op") is True, (
            "Empty Slipstream LP_CLOSE must carry no_op metadata"
        )
        assert compilation_result.action_bundle.transactions == [], (
            "No-op bundle must have 0 transactions"
        )
        assert len(execution_result.transaction_results) == 0, (
            "No-op execution must produce 0 executed transactions"
        )

        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)
        assert usdc_after_close == usdc_before_close, (
            f"USDC balance must be unchanged for empty position close; "
            f"delta={usdc_after_close - usdc_before_close}"
        )
        assert weth_after_close == weth_before_close, (
            f"WETH balance must be unchanged for empty position close; "
            f"delta={weth_after_close - weth_before_close}"
        )

        # Layer 5 — a no-op empty LP_CLOSE must NOT fabricate an accounting
        # event (epic VIB-4591 decision #7: failure/no-op → zero rows). The
        # LP_CLOSE row set must be unchanged from the pre-close snapshot.
        close_rows_after = await layer5_accounting_harness.store.get_accounting_events(
            "layer5-intent-test",
            event_type="LP_CLOSE",
            limit=20,
        )
        assert close_rows_after == close_rows_before, (
            "No-op empty Slipstream LP_CLOSE must not fabricate an accounting event "
            f"(before={len(close_rows_before)} rows, after={len(close_rows_after)} rows)"
        )

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_position_no_liquidity_but_owed_tokens(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Case #3: Close a position with no liquidity but uncollected owed tokens.

        After decreaseLiquidity (but no external collect), principal sits in
        ``tokensOwed0/1``. LPCloseIntent must collect them via its single
        ``collect`` tx (the compile path short-circuits the decreaseLiquidity
        leg when liquidity == 0 — adapter ``remove_cl_liquidity``).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        # Open via the accounting helper for the prior LP_OPEN (linkage + basis).
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

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        await decrease_all_liquidity(
            web3, orchestrator,
            chain=CHAIN_NAME, protocol=PROTOCOL,
            position_manager=POSITION_MANAGER, token_id=position_id,
        )

        liquidity = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity == 0, f"Expected 0 liquidity after decrease, got {liquidity}"

        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)

        close_intent = LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="aerodrome_slipstream",
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(close_intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"LP Close compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None
        # No liquidity → decreaseLiquidity is skipped, only collect emitted.
        assert len(compilation_result.action_bundle.transactions) == 1, (
            "Slipstream LP_CLOSE on a zero-liquidity-with-owed-tokens position "
            "must emit only the collect tx (decreaseLiquidity short-circuited)"
        )

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, (
            f"LP Close on owed-tokens position must succeed. Error: {execution_result.error}"
        )
        execution_result = _enrich_for_accounting(
            execution_result,
            close_intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
        )

        # Layer 3 strict: parse_receipt success + extract_lp_close_data,
        # per CodeRabbit "pin LP_CLOSE receipts to exact wallet deltas".
        parser = AerodromeSlipstreamReceiptParser(chain=CHAIN_NAME)
        lp_close_data = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parse_result = parser.parse_receipt(receipt_dict)
                assert parse_result.success, (
                    f"Receipt parser must succeed on a confirmed receipt; "
                    f"error={parse_result.error}"
                )
                data = parser.extract_lp_close_data(receipt_dict)
                if data:
                    lp_close_data = data

        assert lp_close_data is not None, "Must extract LP close data from receipt"
        assert (
            lp_close_data.amount0_collected > 0 or lp_close_data.amount1_collected > 0
        ), "At least one collected amount must be positive (owed tokens from decrease)"

        # Layer 4 strict: wallet deltas EXACTLY equal parsed Collect amounts.
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)
        usdc_collected = usdc_after_close - usdc_before_close
        weth_collected = weth_after_close - weth_before_close

        print(f"USDC collected: {format_token_amount(usdc_collected, usdc_decimals)}")
        print(f"WETH collected: {format_token_amount(weth_collected, weth_decimals)}")

        if int(weth_addr, 16) < int(usdc_addr, 16):
            parsed_weth, parsed_usdc = (
                lp_close_data.amount0_collected,
                lp_close_data.amount1_collected,
            )
        else:
            parsed_usdc, parsed_weth = (
                lp_close_data.amount0_collected,
                lp_close_data.amount1_collected,
            )
        assert weth_collected == parsed_weth, (
            f"WETH wallet delta must equal parsed Collect amount exactly. "
            f"wallet={weth_collected}, parsed={parsed_weth}"
        )
        assert usdc_collected == parsed_usdc, (
            f"USDC wallet delta must equal parsed Collect amount exactly. "
            f"wallet={usdc_collected}, parsed={parsed_usdc}"
        )

        # Layer 5 — assert the real accounting pipeline persisted LP_CLOSE.
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
        # #4 linkage: LP_CLOSE.position_key == LP_OPEN.position_key + basis from prior OPEN.
        assert close_payload["position_key"] == _payload(open_accounting_row)["position_key"]
        _assert_no_lot_id(close_accounting_row, close_payload)
        # #2 directional null-contract on LP_CLOSE (V3-shaped: no fabricated V4 hash).
        assert close_payload["position_hash"] is None, (
            "Aerodrome Slipstream LP_CLOSE must not fabricate a V4 position_hash"
        )
        # #3 parser ↔ event exact scaled-int equality.
        dec0 = get_token_decimals(web3, tokens[close_payload["token0"]])
        dec1 = get_token_decimals(web3, tokens[close_payload["token1"]])
        _assert_parser_event_equality(close_payload, lp_close_data, dec0=dec0, dec1=dec1)


# =============================================================================
# CollectFeesIntent Tests (LP_COLLECT_FEES)
# =============================================================================


@pytest.mark.base
@pytest.mark.lp
class TestAerodromeSlipstreamCollectFeesIntent:
    """LP_COLLECT_FEES on Aerodrome Slipstream — standalone fee harvest.

    Slipstream's NPM ``collect()`` harvests accrued fees + any unlocked
    principal without burning the position (audit / connector docstring on
    ``collect_cl_fees``). Calling it on a position with no owed tokens is a
    contract-level no-op but the intent still emits the tx so the runner sees
    a deterministic outcome.
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.SWAP, IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason=(
            "VIB-4434: fee-accrual swap (auto-routed Aerodrome) reverts "
            "with selector=0xd27b44a9 on the base CI pinned fork block "
            "(as of 2026-05-16). Test passes locally against latest block; "
            "same fork-block flake class as VIB-4465 on the sibling V4 "
            "collect_fees test. VIB-4434 owns clearing this once the "
            "pinned fork block has stable pool state for the auto-routed "
            "swap path. strict=False because xpass = pool state recovered, "
            "not a code fix."
        ),
        strict=False,
    )
    async def test_collect_fees_zero_accrual_conservation(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """LP_COLLECT_FEES on a Slipstream position with no accrued fees is a
        clean no-op that preserves balance conservation.

        Background: a same-pool fee-accrual fixture for Slipstream is not yet
        wired — the ``SwapIntent(protocol="aerodrome")`` auto-router may
        route through a different pool than the LP position's
        ``WETH/USDC/200`` Slipstream pool, so no fees accrue on the position
        at this fork block. Until a same-pool fixture lands (separate
        follow-up ticket, mirrors VIB-4314 for pancakeswap_v3), this test
        verifies the *no-fee* path is a structurally clean no-op:

          * The compile / execute / parse pipeline runs to completion.
          * Slipstream's NPM ``collect()`` lands on-chain as a contract-level
            no-op with zero ``amount0_collected`` / ``amount1_collected``.
          * The position's liquidity is unchanged (fee harvest does not
            touch principal).
          * **Bilateral balance conservation**: both WETH and USDC wallet
            deltas BEFORE→AFTER the LP_COLLECT_FEES intent are zero — no
            tokens move when no fees accrue. Replaces the previous
            ``xfail(strict=True)`` body that asserted positive deltas, per
            CodeRabbit Major #2 on PR #2331 + ``.claude/rules/intent-tests.md``
            §"Missing conservation checks on failures".

        **When the same-pool fee-accrual fixture lands** (follow-up ticket),
        the conservation assertions below will start failing (positive
        deltas) — that's the signal to update this test (rename, switch to
        positive-delta assertions, and pair with the parsed-equals-wallet-
        delta checks the LP_CLOSE cases already use).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]

        # Open via the accounting helper so Layer 5 covers the LP_OPEN even
        # though the downstream fee-accrual swap is the xfail-prone step.
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
        _assert_identity(open_accounting_row, event_type="LP_OPEN", wallet=funded_wallet)
        assert _payload(open_accounting_row)["position_hash"] is None, (
            "Aerodrome Slipstream LP_OPEN must not fabricate a V4 position_hash"
        )
        liquidity_before = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity_before > 0, "Setup LP_OPEN must yield positive liquidity"

        # Drive a swap to attempt fee accrual. With the current setup the
        # SwapIntent(protocol="aerodrome") may route through a non-LP pool,
        # so this step exercises the SWAP pipeline but does NOT accrue fees
        # on the position. Compile + execute MUST still succeed regardless
        # (Layer 1 / Layer 2 invariants on the SWAP intent).
        swap_intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.05"),
            protocol="aerodrome",  # auto-routed; may NOT hit POOL
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        swap_compilation = compiler.compile(swap_intent)
        assert swap_compilation.status.value == "SUCCESS", (
            f"Fee-accrual swap must compile. Error: {swap_compilation.error}"
        )
        assert swap_compilation.action_bundle is not None
        swap_result = await orchestrator.execute(swap_compilation.action_bundle)
        assert swap_result.success, (
            f"Fee-accrual swap must execute. Error: {swap_result.error}"
        )

        # Record balances AFTER the swap (the load-bearing reference for the
        # conservation check below — the LP_COLLECT_FEES intent must not
        # change either token balance when no fees have accrued).
        usdc_before_collect = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before_collect = get_token_balance(web3, weth_addr, funded_wallet)

        collect_intent = CollectFeesIntent(
            pool=POOL,
            protocol="aerodrome_slipstream",
            chain=CHAIN_NAME,
            protocol_params={"position_id": position_id},
        )
        compilation_result = compiler.compile(collect_intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"CollectFees compilation must succeed. Error: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, (
            f"CollectFees execution failed: {execution_result.error}"
        )

        # Layer 3: receipt parser must succeed on the confirmed receipt and
        # surface a Collect event with zero collected amounts (the on-chain
        # contract-level no-op shape).
        parser = AerodromeSlipstreamReceiptParser(chain=CHAIN_NAME)
        parsed_amount0_collected = 0
        parsed_amount1_collected = 0
        saw_collect = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parse_result = parser.parse_receipt(receipt_dict)
                assert parse_result.success, (
                    f"Receipt parser must succeed on a confirmed receipt; "
                    f"error={parse_result.error}"
                )
                lp_close_data = parser.extract_lp_close_data(receipt_dict)
                if lp_close_data:
                    parsed_amount0_collected += lp_close_data.amount0_collected
                    parsed_amount1_collected += lp_close_data.amount1_collected
                    saw_collect = True

        assert saw_collect, "Receipt must contain a Collect event from LP_COLLECT_FEES"
        assert parsed_amount0_collected == 0, (
            f"Parsed amount0_collected must be zero on no-fee accrual path; "
            f"got {parsed_amount0_collected}. If this fires positive, a same-"
            f"pool fee-accrual fixture has been wired — rewrite this test to "
            f"assert positive collected amounts + wallet deltas matching the "
            f"LP_CLOSE pattern in this file."
        )
        assert parsed_amount1_collected == 0, (
            f"Parsed amount1_collected must be zero on no-fee accrual path; "
            f"got {parsed_amount1_collected}. Same signal as amount0 above."
        )

        # Layer 4 — bilateral balance conservation: BOTH WETH and USDC wallet
        # deltas across the LP_COLLECT_FEES intent are zero. No tokens move
        # when no fees accrue. CodeRabbit #2: enforce conservation in the
        # failure-path body, not positive-delta assertions.
        usdc_after_collect = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_collect = get_token_balance(web3, weth_addr, funded_wallet)
        assert usdc_after_collect == usdc_before_collect, (
            f"USDC wallet balance must be UNCHANGED across no-fee-accrual "
            f"LP_COLLECT_FEES (delta={usdc_after_collect - usdc_before_collect}). "
            f"A non-zero delta means fees DID accrue — see assertion above."
        )
        assert weth_after_collect == weth_before_collect, (
            f"WETH wallet balance must be UNCHANGED across no-fee-accrual "
            f"LP_COLLECT_FEES (delta={weth_after_collect - weth_before_collect}). "
            f"A non-zero delta means fees DID accrue — see assertion above."
        )

        # Position liquidity is unchanged regardless of fee accrual (fee
        # harvest does not touch principal — load-bearing invariant of the
        # NPM collect() entry point).
        liquidity_after = query_position_liquidity(web3, POSITION_MANAGER, position_id)
        assert liquidity_after == liquidity_before, (
            f"LP_COLLECT_FEES must NOT remove liquidity. "
            f"before={liquidity_before}, after={liquidity_after}"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
