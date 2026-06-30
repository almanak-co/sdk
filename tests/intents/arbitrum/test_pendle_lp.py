"""Production-grade LP Intent tests for Pendle on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for:
- LPOpenIntent: Adding single-sided liquidity to a Pendle market
- LPCloseIntent: Removing liquidity and receiving the output token

Pendle LP mechanics differ from Uniswap V3:
- Single-sided liquidity (amount0 only; range_lower/upper are dummies ignored by compiler)
- The market address IS the LP token (no separate NFT)
- position_id = LP token amount in wei (not a numeric NFT ID)
- Output token for LP_CLOSE must be passed via protocol_params={"token": ...}

NO MOCKING. All tests execute real on-chain transactions on an Arbitrum Anvil fork.

Layer 5 (accounting-persistence correctness, epic VIB-4591 / ticket VIB-4599):
Pendle LP routes through the dedicated ``pendle_handler.py`` (NOT the generic
``lp_handler.py``), so the typed record is a ``PendleAccountingEvent`` with
``event_type`` in {``PENDLE_LP_OPEN``, ``PENDLE_LP_CLOSE``} — NOT the generic
``LP_OPEN`` / ``LP_CLOSE`` shape. Pendle LP events carry ``sy_amount`` /
``pt_amount`` (scaled by an assumed 18-decimal precision) and are ALWAYS
``confidence=ESTIMATED`` by design (no USD price, no pt_token on the LP leg;
see ``almanak/framework/accounting/category_handlers/pendle_handler.py::handle_pendle_lp``).
The conftest ``_default_compute_position_key`` deliberately special-cases
pendle OUT of the generic ``lp:`` keyed branch, so the persisted Pendle LP
event carries an empty ``position_key`` / ``market_id`` — a real contract
divergence vs Uniswap V3 LP, asserted here as such.

To run:
    uv run pytest tests/intents/arbitrum/test_pendle_lp.py -v -s -n0 --import-mode=importlib
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.pendle.receipt_parser import PendleReceiptParser
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionResult,
)
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents import LPCloseIntent, LPOpenIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
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

# PT-sUSDai-15OCT2026 market on Arbitrum — a live, deeply-liquid Pendle market
# (~$13M TVL on-chain as of 2026-06-29; expiry() = 1792022400 = 2026-10-15 UTC).
# This replaced the former PT-wstETH-25JUN2026 market, which expired 2026-06-25
# (expiry() = 1782345600) and now reverts every LP/swap into it — the wstETH
# Pendle markets on Arbitrum are all gone (Pendle's active-markets API lists only
# sUSDai / USDai markets here now). The market contract address is also the LP
# token address for Pendle positions.
PENDLE_SUSDAI_MARKET = "0xcbf629c8d396b1261f81f55175afa010e94787d8"

# Input token: sUSDai mints SY directly (it is the market's tokenMintSy, verified
# via SY.getTokensIn()), so no pre-swap routing is needed.
SUSDAI_ADDRESS = "0x0B2b2B2076d95dda7817e785989fE353fe955ef9"
SUSDAI_SYMBOL = "sUSDai"

# Small LP deposit: 10 sUSDai (~$10.5 at ~$1.05/sUSDai). The market's SY reserve
# is ~10.8M, so this is a negligible-impact deposit.
LP_DEPOSIT_AMOUNT = Decimal("10")


def _enrich_oracle_with_susdai(price_oracle: dict[str, Decimal]) -> dict[str, Decimal]:
    """Add a SUSDAI price to the oracle if missing (needed for compile estimation).

    sUSDai has no CoinGecko id, so the session price-oracle fixture never carries
    it. sUSDai is staked USDai and trades near $1; we anchor it to USDC (the
    market's compile-time slippage/min-out math is loose enough that the exact
    figure is immaterial — the on-chain SDK computes the binding min-out).
    """
    enriched = dict(price_oracle)
    if "SUSDAI" not in enriched:
        enriched["SUSDAI"] = enriched.get("USDC", Decimal("1"))
    return enriched


# range_lower/upper are required by LPOpenIntent validation but ignored by the
# Pendle compiler (Pendle uses single-sided liquidity with no tick range).
_DUMMY_RANGE_LOWER = Decimal("0.0001")
_DUMMY_RANGE_UPPER = Decimal("999999")

# Pendle scales SY/PT amounts by an assumed 18-decimal precision
# (handle_pendle_lp: ``Decimal(str(raw)) / 10**18``).
_PENDLE_SCALE_18 = Decimal(10**18)


# =============================================================================
# Layer 5 — accounting-persistence helpers (VIB-4599)
# =============================================================================


def _execution_context(wallet: str) -> ExecutionContext:
    # This deployment_id labels the ExecutionContext for enrichment only; it is
    # NOT what lands in the persisted row. ``assert_accounting_persisted``
    # stamps the row deployment_id from its own ``layer5-intent-test`` default
    # (the descriptive-enrichment-id vs canonical-persisted-identity split that
    # mirrors the merged Uniswap V3 / Spark goldens).
    return ExecutionContext(
        deployment_id="layer5-pendle-lp",
        chain=CHAIN_NAME,
        wallet_address=wallet,
        protocol="pendle",
    )


def _enrich_for_accounting(
    execution_result: ExecutionResult,
    intent: Any,
    wallet: str,
    bundle_metadata: dict | None = None,
) -> ExecutionResult:
    return enrich_result(
        execution_result,
        intent,
        _execution_context(wallet),
        live_mode=False,
        bundle_metadata=bundle_metadata,
    )


def _payload(row: dict) -> dict:
    return json.loads(row["payload_json"])


def _to_human_18(raw: int | None) -> Decimal | None:
    if raw is None:
        return None
    return Decimal(int(raw)) / _PENDLE_SCALE_18


def _assert_pendle_lp_identity(row: dict, *, event_type: str, wallet: str) -> None:
    """Identity contract shared by PENDLE_LP_OPEN / PENDLE_LP_CLOSE rows."""
    assert row["deployment_id"] == "layer5-intent-test"
    assert row["cycle_id"] == "layer5-cycle"
    assert row["execution_mode"] == "paper"
    assert row["event_type"] == event_type
    assert row["tx_hash"], "accounting row must link to an on-chain tx_hash"
    assert row["ledger_entry_id"], "accounting row must link to transaction_ledger"
    assert row["wallet_address"].lower() == wallet.lower()
    # Pendle LP is ALWAYS ESTIMATED on the LP leg (no USD price / pt_token).
    assert row["confidence"] == "ESTIMATED"


def _assert_pendle_lp_payload(
    payload: dict,
    *,
    event_type: str,
    sy_amount: Decimal | None,
    pt_amount: Decimal | None,
) -> None:
    """Assert the actual PendleAccountingEvent contract (NOT the generic LP shape)."""
    assert payload["event_type"] == event_type
    # SY/PT amounts come from the parser's lp_open_data / lp_close_data, scaled 1e18.
    assert sy_amount is not None
    assert pt_amount is not None
    assert Decimal(str(payload["sy_amount"])) == sy_amount
    assert Decimal(str(payload["pt_amount"])) == pt_amount
    # Pendle LP leg never carries USD price / PT token / yield / APR / maturity.
    assert payload["pt_token"] == "", "Pendle LP leg must not fabricate a pt_token"
    assert payload["pt_price"] is None
    assert payload["sy_price"] is None
    assert payload["implied_apr_bps"] is None
    assert payload["days_to_maturity"] is None
    assert payload["realized_yield_usd"] is None
    assert payload["maturity_timestamp"] is None
    assert payload["confidence"] == "ESTIMATED"
    assert payload["unavailable_reason"], "Pendle LP must document why it is ESTIMATED (Empty != None discipline)"
    # Pendle's conftest position-key special-case yields an empty key/market.
    assert payload["position_key"] == "", "Pendle LP position_key is empty by design (see conftest)"
    assert payload["market_id"] == "", "Pendle LP market_id is empty by design (see conftest)"


# =============================================================================
# LP_OPEN Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestPendleLPOpenIntent:
    """4-layer tests for Pendle LP_OPEN on Arbitrum.

    Deposits sUSDai into the PT-sUSDai-15OCT2026 market and verifies:
    1. Compilation succeeds
    2. Execution lands on-chain
    3. PendleReceiptParser finds a Mint event with net_lp_minted > 0
    4. sUSDai balance decreased, LP token balance increased
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_susdai_into_pendle_market(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Open a sUSDai LP position in the PT-sUSDai-15OCT2026 Pendle market."""
        susdai_decimals = get_token_decimals(web3, SUSDAI_ADDRESS)

        print(f"\n{'=' * 80}")
        print("Test: LP_OPEN sUSDai -> PT-sUSDai-15OCT2026 (Pendle)")
        print(f"{'=' * 80}")
        print(f"Deposit: {LP_DEPOSIT_AMOUNT} {SUSDAI_SYMBOL}")

        # Layer 4 setup: record balances BEFORE
        susdai_before = get_token_balance(web3, SUSDAI_ADDRESS, funded_wallet)
        lp_before = get_token_balance(web3, PENDLE_SUSDAI_MARKET, funded_wallet)
        print(f"sUSDai before:  {format_token_amount(susdai_before, susdai_decimals)}")
        print(f"LP before:      {format_token_amount(lp_before, 18)}")

        # Layer 1: Compile
        intent = LPOpenIntent(
            pool=f"{SUSDAI_SYMBOL}/{PENDLE_SUSDAI_MARKET}",
            amount0=LP_DEPOSIT_AMOUNT,
            amount1=Decimal("0"),
            range_lower=_DUMMY_RANGE_LOWER,
            range_upper=_DUMMY_RANGE_UPPER,
            protocol="pendle",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_susdai(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        tx_count = len(compilation_result.action_bundle.transactions)
        print(f"ActionBundle: {tx_count} transactions")

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful: {len(execution_result.transaction_results)} txs confirmed")

        # Layer 5 enrichment: populate execution_result.extracted_data
        # (lp_open_data) so the accounting handler can read SY/PT amounts.
        execution_result = _enrich_for_accounting(
            execution_result,
            intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
        )

        # Layer 3: Receipt parsing — expect exactly one Mint event
        parser = PendleReceiptParser(chain=CHAIN_NAME)
        lp_minted_raw: int | None = None
        net_sy_used_raw: int | None = None
        net_pt_used_raw: int | None = None
        for i, tx_result in enumerate(execution_result.transaction_results):
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            if parse_result.mint_events:
                mint = parse_result.mint_events[0]
                lp_minted_raw = mint.net_lp_minted
                net_sy_used_raw = mint.net_sy_used
                net_pt_used_raw = mint.net_pt_used
                print(
                    f"\nTx {i + 1} Mint event:"
                    f"\n  market:        {mint.market_address}"
                    f"\n  net_lp_minted: {mint.net_lp_minted}"
                    f"\n  net_sy_used:   {mint.net_sy_used}"
                    f"\n  net_pt_used:   {mint.net_pt_used}"
                )

        assert lp_minted_raw is not None, "No Mint event found in any transaction receipt"
        assert lp_minted_raw > 0, f"net_lp_minted must be positive, got {lp_minted_raw}"

        # Verify market address in Mint event matches expected market
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            for mint in parse_result.mint_events:
                assert mint.market_address.lower() == PENDLE_SUSDAI_MARKET.lower(), (
                    f"Mint market_address mismatch: got {mint.market_address}"
                )

        # Layer 4: Balance deltas
        susdai_after = get_token_balance(web3, SUSDAI_ADDRESS, funded_wallet)
        lp_after = get_token_balance(web3, PENDLE_SUSDAI_MARKET, funded_wallet)

        susdai_spent = susdai_before - susdai_after
        lp_received = lp_after - lp_before

        print("\n--- Results ---")
        print(f"sUSDai spent:   {format_token_amount(susdai_spent, susdai_decimals)}")
        print(f"LP received:    {format_token_amount(lp_received, 18)}")

        expected_susdai_wei = int(LP_DEPOSIT_AMOUNT * Decimal(10**susdai_decimals))
        assert susdai_spent == expected_susdai_wei, (
            f"sUSDai spent must EXACTLY equal deposit amount. Expected: {expected_susdai_wei}, Got: {susdai_spent}"
        )
        assert lp_received > 0, "LP token balance must increase after LP_OPEN"
        assert lp_received == lp_minted_raw, (
            f"On-chain LP balance delta must match receipt net_lp_minted. "
            f"Balance delta: {lp_received}, receipt: {lp_minted_raw}"
        )

        # Verify extraction methods (position-key / enrichment path)
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            if not parser.parse_receipt(receipt_dict).mint_events:
                continue
            position_id = parser.extract_position_id(receipt_dict)
            assert position_id is not None, "extract_position_id must return a value for LP_OPEN"
            assert position_id.lower() == PENDLE_SUSDAI_MARKET.lower(), (
                f"position_id must equal the market address, got {position_id}"
            )
            lp_open_data = parser.extract_lp_open_data(receipt_dict)
            assert lp_open_data is not None, "extract_lp_open_data must return data"
            assert lp_open_data.liquidity == lp_minted_raw, (
                f"lp_open_data.liquidity must match net_lp_minted. "
                f"Expected: {lp_minted_raw}, Got: {lp_open_data.liquidity}"
            )

        # Layer 5: accounting persistence — PendleAccountingEvent(PENDLE_LP_OPEN)
        accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=execution_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="PENDLE_LP_OPEN",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        _assert_pendle_lp_identity(accounting_row, event_type="PENDLE_LP_OPEN", wallet=funded_wallet)
        # handle_pendle_lp scales lp_open_data.amount0/amount1 (net_sy_used /
        # net_pt_used) by 1e18 into sy_amount / pt_amount.
        _assert_pendle_lp_payload(
            _payload(accounting_row),
            event_type="PENDLE_LP_OPEN",
            sy_amount=_to_human_18(net_sy_used_raw),
            pt_amount=_to_human_18(net_pt_used_raw),
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_insufficient_balance_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """LP_OPEN with more sUSDai than the wallet holds must fail gracefully."""
        susdai_balance = get_token_balance(web3, SUSDAI_ADDRESS, funded_wallet)
        lp_before = get_token_balance(web3, PENDLE_SUSDAI_MARKET, funded_wallet)
        assert susdai_balance > 0, "Funded wallet must have positive sUSDai balance for this test"
        susdai_decimals = get_token_decimals(web3, SUSDAI_ADDRESS)
        balance_decimal = Decimal(susdai_balance) / Decimal(10**susdai_decimals)
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'=' * 80}")
        print("Test: LP_OPEN Insufficient Balance (Pendle)")
        print(f"{'=' * 80}")
        print(f"sUSDai balance: {balance_decimal}")
        print(f"Trying:         {excessive_amount}")

        intent = LPOpenIntent(
            pool=f"{SUSDAI_SYMBOL}/{PENDLE_SUSDAI_MARKET}",
            amount0=excessive_amount,
            amount1=Decimal("0"),
            range_lower=_DUMMY_RANGE_LOWER,
            range_upper=_DUMMY_RANGE_UPPER,
            protocol="pendle",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_susdai(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        # Bilateral conservation: both sUSDai and LP token unchanged after failure
        susdai_after = get_token_balance(web3, SUSDAI_ADDRESS, funded_wallet)
        lp_after = get_token_balance(web3, PENDLE_SUSDAI_MARKET, funded_wallet)
        assert susdai_after == susdai_balance, "sUSDai balance must be unchanged after failed LP_OPEN"
        assert lp_after == lp_before, "LP token balance must be unchanged after failed LP_OPEN"

        # Layer 5: a failed LP_OPEN must write NO typed PendleAccountingEvent.
        failed_result = _enrich_for_accounting(
            execution_result,
            intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
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


# =============================================================================
# LP_CLOSE Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestPendleLPCloseIntent:
    """4-layer tests for Pendle LP_CLOSE on Arbitrum.

    Opens a position within each test, then closes it, verifying:
    1. Compilation succeeds
    2. Execution lands on-chain
    3. PendleReceiptParser finds a Burn event with net_sy_out > 0
    4. LP token balance returns to zero, sUSDai balance increases
    """

    async def _open_lp_position(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> int:
        """Open a sUSDai LP position and return the LP token amount received."""
        intent = LPOpenIntent(
            pool=f"{SUSDAI_SYMBOL}/{PENDLE_SUSDAI_MARKET}",
            amount0=LP_DEPOSIT_AMOUNT,
            amount1=Decimal("0"),
            range_lower=_DUMMY_RANGE_LOWER,
            range_upper=_DUMMY_RANGE_UPPER,
            protocol="pendle",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_susdai(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        result = compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"LP_OPEN compilation failed: {result.error}"
        exec_result = await orchestrator.execute(result.action_bundle)
        assert exec_result.success, f"LP_OPEN execution failed: {exec_result.error}"

        lp_balance = get_token_balance(web3, PENDLE_SUSDAI_MARKET, funded_wallet)
        assert lp_balance > 0, "Expected LP tokens after LP_OPEN"
        return lp_balance

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_returns_susdai(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Close an open sUSDai Pendle LP position and verify sUSDai is returned."""
        susdai_decimals = get_token_decimals(web3, SUSDAI_ADDRESS)

        # Setup: open an LP position to close
        lp_amount = await self._open_lp_position(web3, funded_wallet, orchestrator, price_oracle, anvil_rpc_url)

        print(f"\n{'=' * 80}")
        print("Test: LP_CLOSE PT-sUSDai-15OCT2026 -> sUSDai (Pendle)")
        print(f"{'=' * 80}")
        print(f"LP to burn: {format_token_amount(lp_amount, 18)}")

        # Layer 4 setup: record balances BEFORE close
        susdai_before = get_token_balance(web3, SUSDAI_ADDRESS, funded_wallet)
        lp_before = get_token_balance(web3, PENDLE_SUSDAI_MARKET, funded_wallet)
        print(f"sUSDai before: {format_token_amount(susdai_before, susdai_decimals)}")
        print(f"LP before:     {format_token_amount(lp_before, 18)}")

        # Layer 1: Compile
        # Output token is passed via protocol_params since LPCloseIntent has no token field.
        intent = LPCloseIntent(
            position_id=str(lp_amount),
            pool=PENDLE_SUSDAI_MARKET,
            protocol="pendle",
            chain=CHAIN_NAME,
            protocol_params={"token": SUSDAI_SYMBOL},
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_susdai(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        tx_count = len(compilation_result.action_bundle.transactions)
        print(f"ActionBundle: {tx_count} transactions")

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful: {len(execution_result.transaction_results)} txs confirmed")

        # Layer 5 enrichment: populate execution_result.extracted_data
        # (lp_close_data) so the accounting handler can read SY/PT amounts.
        execution_result = _enrich_for_accounting(
            execution_result,
            intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
        )

        # Layer 3: Receipt parsing — expect exactly one Burn event
        parser = PendleReceiptParser(chain=CHAIN_NAME)
        lp_burned_raw: int | None = None
        sy_out_raw: int | None = None
        pt_out_raw: int | None = None
        for i, tx_result in enumerate(execution_result.transaction_results):
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            if parse_result.burn_events:
                burn = parse_result.burn_events[0]
                lp_burned_raw = burn.net_lp_burned
                sy_out_raw = burn.net_sy_out
                pt_out_raw = burn.net_pt_out
                print(
                    f"\nTx {i + 1} Burn event:"
                    f"\n  market:        {burn.market_address}"
                    f"\n  net_lp_burned: {burn.net_lp_burned}"
                    f"\n  net_sy_out:    {burn.net_sy_out}"
                    f"\n  net_pt_out:    {burn.net_pt_out}"
                )

        assert lp_burned_raw is not None, "No Burn event found in any transaction receipt"
        assert lp_burned_raw > 0, f"net_lp_burned must be positive, got {lp_burned_raw}"
        assert sy_out_raw is not None and sy_out_raw > 0, f"net_sy_out must be positive, got {sy_out_raw}"

        # Layer 4: Balance deltas
        susdai_after = get_token_balance(web3, SUSDAI_ADDRESS, funded_wallet)
        lp_after = get_token_balance(web3, PENDLE_SUSDAI_MARKET, funded_wallet)

        susdai_received = susdai_after - susdai_before
        lp_spent = lp_before - lp_after

        print("\n--- Results ---")
        print(f"sUSDai received: {format_token_amount(susdai_received, susdai_decimals)}")
        print(f"LP burned:       {format_token_amount(lp_spent, 18)}")

        assert lp_spent == lp_amount, (
            f"LP tokens burned must equal position_id amount. Expected: {lp_amount}, Got: {lp_spent}"
        )
        assert lp_after == 0, f"LP token balance must be zero after full close, got {lp_after}"
        assert susdai_received > 0, "Must receive positive sUSDai after LP_CLOSE"

        # Verify extraction methods (position-key / enrichment path)
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            if not parser.parse_receipt(receipt_dict).burn_events:
                continue
            position_id = parser.extract_position_id(receipt_dict)
            assert position_id is not None, "extract_position_id must return a value for LP_CLOSE"
            assert position_id.lower() == PENDLE_SUSDAI_MARKET.lower(), (
                f"position_id must equal the market address, got {position_id}"
            )
            lp_close_data = parser.extract_lp_close_data(receipt_dict)
            assert lp_close_data is not None, "extract_lp_close_data must return data"
            assert lp_close_data.liquidity_removed == lp_burned_raw, (
                f"lp_close_data.liquidity_removed must match net_lp_burned. "
                f"Expected: {lp_burned_raw}, Got: {lp_close_data.liquidity_removed}"
            )

        # VIB-5302: LP_CLOSE must emit the RECEIVED-UNDERLYING amount (the realized
        # proceeds the wallet gets back), not just the intermediate SY/PT from the
        # Burn event. The single-sided removal redeems SY -> sUSDai; the SY Redeem
        # event's amount_token_out is the sUSDai the wallet received. Cross-check
        # Layer 3 (parser) against Layer 4 (on-chain balance delta): byte-identical
        # (no MEV on Anvil), and the declared OUTPUT money-leg must carry that same
        # measured amount in human units. amount="all" LP_CLOSE chaining off this
        # output is deferred to VIB-5346 (LPCloseIntent has no chained-amount
        # concept today); this test uses an explicit wei position_id.
        underlying_out_raw: int | None = None
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            pr = parser.parse_receipt(receipt_dict)
            if not pr.burn_events:
                continue
            assert pr.redeem_sy_events, (
                "Pendle single-sided LP_CLOSE must emit an SY Redeem event carrying "
                "the underlying paid to the wallet (received-underlying source)"
            )
            underlying_out_raw = pr.redeem_sy_events[0].amount_token_out
            assert underlying_out_raw == susdai_received, (
                f"SY Redeem amount_token_out must equal the on-chain sUSDai balance delta. "
                f"Receipt: {underlying_out_raw}, balance delta: {susdai_received}"
            )
            # Declared money-leg surface (US-009): the received underlying is the
            # OUTPUT leg the ledger dispatcher records — measured, in human units.
            legs = parser.extract_primitive_money_legs(
                receipt_dict,
                out_token_symbol=SUSDAI_SYMBOL,
                out_token_address=SUSDAI_ADDRESS,
                out_token_decimals=susdai_decimals,
            )
            assert legs is not None, "LP_CLOSE must declare its received-underlying OUTPUT leg"
            assert len(legs.output_legs) == 1
            out_leg = legs.output_legs[0]
            assert out_leg.token == SUSDAI_SYMBOL
            assert out_leg.amount.is_measured
            expected_human = Decimal(susdai_received) / Decimal(10**susdai_decimals)
            assert out_leg.amount.value == expected_human, (
                f"OUTPUT leg must carry the received underlying in human units. "
                f"Expected: {expected_human}, Got: {out_leg.amount.value}"
            )
        assert underlying_out_raw is not None, "No Burn receipt found to verify received-underlying extraction"

        # Layer 5: accounting persistence — PendleAccountingEvent(PENDLE_LP_CLOSE)
        accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=execution_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="PENDLE_LP_CLOSE",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        _assert_pendle_lp_identity(accounting_row, event_type="PENDLE_LP_CLOSE", wallet=funded_wallet)
        # handle_pendle_lp scales lp_close_data.amount0_collected /
        # amount1_collected (net_sy_out / net_pt_out) by 1e18.
        _assert_pendle_lp_payload(
            _payload(accounting_row),
            event_type="PENDLE_LP_CLOSE",
            sy_amount=_to_human_18(sy_out_raw),
            pt_amount=_to_human_18(pt_out_raw),
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    import pytest as _pytest

    _pytest.main([__file__, "-v", "-s", "-n0", "--import-mode=importlib"])
