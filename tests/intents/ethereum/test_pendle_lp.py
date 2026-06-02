"""Intent tests for Pendle LP_OPEN / LP_CLOSE / WITHDRAW on Ethereum (VIB-4307).

This file covers the (pendle, LP_OPEN, ethereum), (pendle, LP_CLOSE,
ethereum), and (pendle, WITHDRAW, ethereum) triples from ConnectorRegistry.

Pendle's WITHDRAW intent path is **PT/YT redemption at maturity** (NOT
Aave-style lending withdraw). The compiler routes ``WithdrawIntent`` with
``protocol="pendle"`` to ``compile_pendle_redeem``, which builds a
PT-to-token redemption via the Pendle Router. See
``almanak/connectors/pendle/compiler.py::compile_pendle_redeem``.

Active Ethereum market (as of 2026-05-12):

* PT-sUSDe-13AUG2026 (market ``0x177768...``)
* underlying SY mint: sUSDe (``0x9D39A5DE30e57443BfF2A8307A4256c8797A3497``)
* LP pair token: sUSDe (single-sided liquidity)

Pendle **is** in ``_LP_PROTOCOLS`` and ``_SWAP_PROTOCOLS`` in
``synthetic_intents.py``, so this module does NOT carry the
``no_zodiac`` marker — the default-on Zodiac wrap applies.

Layer 5 (accounting-persistence correctness, epic VIB-4591 / ticket
VIB-4599): Pendle LP routes through the dedicated ``pendle_handler.py`` (NOT
the generic ``lp_handler.py``), so the typed record is a
``PendleAccountingEvent`` with ``event_type`` in {``PENDLE_LP_OPEN``,
``PENDLE_LP_CLOSE``} — NOT the generic ``LP_OPEN`` / ``LP_CLOSE`` shape.
Pendle LP events carry ``sy_amount`` / ``pt_amount`` (scaled by an assumed
18-decimal precision) and are ALWAYS ``confidence=ESTIMATED`` by design (no
USD price, no pt_token on the LP leg; see
``almanak/framework/accounting/category_handlers/pendle_handler.py::handle_pendle_lp``).
The conftest ``_default_compute_position_key`` deliberately special-cases
pendle OUT of the generic ``lp:`` keyed branch, so the persisted Pendle LP
event carries an empty ``position_key`` / ``market_id`` — a real contract
divergence vs Uniswap V3 LP, asserted here as such. The WITHDRAW (PT
redemption) test stays xfail-grandfathered and is NOT rolled into Layer 5.

To run::

    uv run pytest tests/intents/ethereum/test_pendle_lp.py -v -s
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
from almanak.framework.intents import (
    LPCloseIntent,
    LPOpenIntent,
    SwapIntent,
    WithdrawIntent,
)
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    assert_accounting_persisted,
    assert_no_accounting_on_failure,
    format_token_amount,
    fund_erc20_token,
    get_token_balance,
    get_token_decimals,
)

pytestmark = pytest.mark.intent(
    IntentType.LP_OPEN,
    IntentType.LP_CLOSE,
    IntentType.WITHDRAW,
    IntentType.SWAP,
)


# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "ethereum"

# Active PT-sUSDe-13AUG2026 market on Ethereum.
# The market (LP token) and PT token are distinct addresses in Pendle V2:
# market = the AMM pool (also the LP token minted on add-liquidity).
# PT = the principal token minted from SY (separate ERC20).
# See almanak/connectors/pendle/sdk.py: PENDLE_MARKETS vs PT_TOKENS.
PENDLE_SUSDE_MARKET = "0x177768caf9d0e036725a51d3f60d7e20f2d4d194"
PT_SUSDE_ADDRESS = "0x5a19fa369f2895dcd8d2cee62e4ceae58ef92bbb"
# YT pair (required for WithdrawIntent PT/YT redemption pre-maturity).
YT_SUSDE_ADDRESS = "0x45a699a11a4a17fe0931ef3cea4bfc3235e659f2"

# Underlying / SY mint token for this market.
SUSDE_ADDRESS = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
SUSDE_SYMBOL = "sUSDe"
# StakedUSDeV2 _balances storage slot (verified live 2026-04-30 against
# the live contract; NOT slot 0 because slot 0 is OZ ERC20Votes' _checkpoints).
SUSDE_BALANCE_SLOT = 4

LP_DEPOSIT_AMOUNT = Decimal("50")  # 50 sUSDe (~$50)

# Pendle LP requires range_lower/upper for the LPOpenIntent schema, but
# the Pendle compiler ignores them (single-sided liquidity, no ticks).
_DUMMY_RANGE_LOWER = Decimal("0.0001")
_DUMMY_RANGE_UPPER = Decimal("999999")

# Maturity is 13 Aug 2026; advance ~95 days from a 12 May 2026 fork block
# to be safely past maturity for the WITHDRAW (PT redemption) test.
_SECONDS_PAST_MATURITY = 95 * 24 * 60 * 60


def _enrich_oracle_with_susde(
    price_oracle: dict[str, Decimal],
) -> dict[str, Decimal]:
    """sUSDe isn't in the default Ethereum oracle — seed a near-USDC price."""
    enriched = dict(price_oracle)
    if "SUSDE" not in enriched:
        enriched["SUSDE"] = Decimal("1.10")
    if "sUSDe" not in enriched:
        enriched["sUSDe"] = enriched["SUSDE"]
    return enriched


# =============================================================================
# Layer 5 — accounting-persistence helpers (VIB-4599)
# =============================================================================

# Pendle scales SY/PT amounts by an assumed 18-decimal precision
# (handle_pendle_lp: ``Decimal(str(raw)) / 10**18``).
_PENDLE_SCALE_18 = Decimal(10**18)


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
# LP_OPEN
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.lp
class TestPendleLPOpenEthereum:
    """4-layer test for Pendle LP_OPEN on Ethereum (PT-sUSDe-13AUG2026)."""

    @pytest.mark.asyncio
    async def test_lp_open_susde_into_pendle_market(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Open a single-sided sUSDe LP position in PT-sUSDe-13AUG2026."""
        # Seed sUSDe (10x deposit amount for headroom)
        deposit_wei = int(LP_DEPOSIT_AMOUNT * Decimal(10**18))
        fund_erc20_token(
            funded_wallet,
            SUSDE_ADDRESS,
            deposit_wei * 10,
            SUSDE_BALANCE_SLOT,
            anvil_rpc_url,
        )

        susde_decimals = get_token_decimals(web3, SUSDE_ADDRESS)
        assert susde_decimals == 18, "sUSDe must have 18 decimals"

        # Layer 4 setup
        susde_before = get_token_balance(web3, SUSDE_ADDRESS, funded_wallet)
        lp_before = get_token_balance(
            web3, PENDLE_SUSDE_MARKET, funded_wallet
        )
        assert susde_before >= deposit_wei, "sUSDe seeding failed"

        # Layer 1: Compile
        intent = LPOpenIntent(
            pool=f"{SUSDE_SYMBOL}/{PENDLE_SUSDE_MARKET}",
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
            price_oracle=_enrich_oracle_with_susde(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Layer 2: Execute
        execution_result = await orchestrator.execute(
            compilation_result.action_bundle
        )
        assert execution_result.success, (
            f"Execution failed: {execution_result.error}"
        )

        # Layer 5 enrichment: populate execution_result.extracted_data
        # (lp_open_data) so the accounting handler can read SY/PT amounts.
        execution_result = _enrich_for_accounting(
            execution_result,
            intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
        )

        # Layer 3: Receipt parsing — Pendle Mint event
        parser = PendleReceiptParser(chain=CHAIN_NAME)
        lp_minted_raw: int | None = None
        net_sy_used_raw: int | None = None
        net_pt_used_raw: int | None = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            if parse_result.mint_events:
                mint = parse_result.mint_events[0]
                lp_minted_raw = mint.net_lp_minted
                net_sy_used_raw = mint.net_sy_used
                net_pt_used_raw = mint.net_pt_used
                assert mint.market_address.lower() == PENDLE_SUSDE_MARKET.lower()
                break
        assert lp_minted_raw is not None, (
            "Expected exactly one Pendle Mint event for LP_OPEN"
        )
        assert lp_minted_raw > 0, (
            f"net_lp_minted must be positive, got {lp_minted_raw}"
        )

        # Layer 4: Balance deltas
        susde_after = get_token_balance(web3, SUSDE_ADDRESS, funded_wallet)
        lp_after = get_token_balance(web3, PENDLE_SUSDE_MARKET, funded_wallet)

        susde_spent = susde_before - susde_after
        lp_received = lp_after - lp_before

        assert susde_spent == deposit_wei, (
            f"sUSDe spent must equal deposit amount exactly. "
            f"Expected: {deposit_wei}, Got: {susde_spent}"
        )
        assert lp_received > 0, "LP token balance must increase"
        assert lp_received == lp_minted_raw, (
            f"On-chain LP delta ({lp_received}) must match Mint event "
            f"net_lp_minted ({lp_minted_raw})"
        )

        # Layer 5: accounting persistence — PendleAccountingEvent(PENDLE_LP_OPEN)
        accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=execution_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="PENDLE_LP_OPEN",
            price_oracle=_enrich_oracle_with_susde(price_oracle),
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
    ) -> None:
        """LP_OPEN with more sUSDe than the wallet holds must fail gracefully.

        Failure-path Layer 5 contract: a reverted LP_OPEN writes a ledger row
        but NO typed PendleAccountingEvent (mirrors the arbitrum failure test).
        """
        # Do NOT seed sUSDe — wallet should hold ~0; request a large amount.
        susde_decimals = get_token_decimals(web3, SUSDE_ADDRESS)
        susde_before = get_token_balance(web3, SUSDE_ADDRESS, funded_wallet)
        lp_before = get_token_balance(web3, PENDLE_SUSDE_MARKET, funded_wallet)
        balance_decimal = Decimal(susde_before) / Decimal(10**susde_decimals)
        excessive_amount = (balance_decimal * Decimal("100")) + LP_DEPOSIT_AMOUNT

        intent = LPOpenIntent(
            pool=f"{SUSDE_SYMBOL}/{PENDLE_SUSDE_MARKET}",
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
            price_oracle=_enrich_oracle_with_susde(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert not execution_result.success, "Execution should fail with insufficient balance"

        # Bilateral conservation: both sUSDe and LP token unchanged after failure.
        susde_after = get_token_balance(web3, SUSDE_ADDRESS, funded_wallet)
        lp_after = get_token_balance(web3, PENDLE_SUSDE_MARKET, funded_wallet)
        assert susde_after == susde_before, "sUSDe balance must be unchanged after failed LP_OPEN"
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
            price_oracle=_enrich_oracle_with_susde(price_oracle),
            eth_call_reader=anvil_eth_call_adapter,
        )


# =============================================================================
# LP_CLOSE
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.lp
class TestPendleLPCloseEthereum:
    """4-layer test for Pendle LP_CLOSE on Ethereum (PT-sUSDe-13AUG2026)."""

    async def _open_lp_position(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> int:
        """Helper: open an LP position and return the LP balance."""
        # Seed sUSDe
        deposit_wei = int(LP_DEPOSIT_AMOUNT * Decimal(10**18))
        fund_erc20_token(
            funded_wallet,
            SUSDE_ADDRESS,
            deposit_wei * 10,
            SUSDE_BALANCE_SLOT,
            anvil_rpc_url,
        )

        intent = LPOpenIntent(
            pool=f"{SUSDE_SYMBOL}/{PENDLE_SUSDE_MARKET}",
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
            price_oracle=_enrich_oracle_with_susde(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        result = compiler.compile(intent)
        assert result.status.value == "SUCCESS", (
            f"LP_OPEN compile failed: {result.error}"
        )
        exec_result = await orchestrator.execute(result.action_bundle)
        assert exec_result.success, (
            f"LP_OPEN execution failed: {exec_result.error}"
        )

        lp_balance = get_token_balance(
            web3, PENDLE_SUSDE_MARKET, funded_wallet
        )
        assert lp_balance > 0, "Expected LP tokens after LP_OPEN"
        return lp_balance

    @pytest.mark.asyncio
    async def test_lp_close_returns_susde(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Open then close an sUSDe Pendle LP position; verify sUSDe returned."""
        susde_decimals = get_token_decimals(web3, SUSDE_ADDRESS)

        # Setup: open
        lp_amount = await self._open_lp_position(
            web3, funded_wallet, orchestrator, price_oracle, anvil_rpc_url
        )

        # Layer 4 setup
        susde_before = get_token_balance(web3, SUSDE_ADDRESS, funded_wallet)
        lp_before = get_token_balance(
            web3, PENDLE_SUSDE_MARKET, funded_wallet
        )

        # Layer 1: Compile LP_CLOSE
        intent = LPCloseIntent(
            position_id=str(lp_amount),
            pool=PENDLE_SUSDE_MARKET,
            protocol="pendle",
            chain=CHAIN_NAME,
            protocol_params={"token": SUSDE_SYMBOL},
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_susde(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"LP_CLOSE compile failed: {compilation_result.error}"
        )

        # Layer 2: Execute
        execution_result = await orchestrator.execute(
            compilation_result.action_bundle
        )
        assert execution_result.success, (
            f"LP_CLOSE execution failed: {execution_result.error}"
        )

        # Layer 5 enrichment: populate execution_result.extracted_data
        # (lp_close_data) so the accounting handler can read SY/PT amounts.
        execution_result = _enrich_for_accounting(
            execution_result,
            intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
        )

        # Layer 3: Receipt parsing — Pendle Burn event
        parser = PendleReceiptParser(chain=CHAIN_NAME)
        lp_burned_raw: int | None = None
        sy_out_raw: int | None = None
        pt_out_raw: int | None = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            if parse_result.burn_events:
                burn = parse_result.burn_events[0]
                lp_burned_raw = burn.net_lp_burned
                sy_out_raw = burn.net_sy_out
                pt_out_raw = burn.net_pt_out
                break
        assert lp_burned_raw is not None, (
            "Expected exactly one Pendle Burn event for LP_CLOSE"
        )
        assert lp_burned_raw > 0
        assert sy_out_raw is not None and sy_out_raw > 0

        # Layer 4: Balance deltas
        susde_after = get_token_balance(web3, SUSDE_ADDRESS, funded_wallet)
        lp_after = get_token_balance(web3, PENDLE_SUSDE_MARKET, funded_wallet)

        susde_received = susde_after - susde_before
        lp_spent = lp_before - lp_after

        assert lp_spent == lp_amount, (
            f"All LP must be burned. Expected: {lp_amount}, Got: {lp_spent}"
        )
        assert susde_received > 0, (
            f"sUSDe must be returned on close (no-op guard). "
            f"Got delta: "
            f"{format_token_amount(susde_received, susde_decimals)}"
        )

        # Layer 5: accounting persistence — PendleAccountingEvent(PENDLE_LP_CLOSE)
        accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=execution_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="PENDLE_LP_CLOSE",
            price_oracle=_enrich_oracle_with_susde(price_oracle),
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


# =============================================================================
# WITHDRAW (PT redemption at maturity)
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.withdraw
class TestPendleWithdrawEthereum:
    """4-layer test for Pendle WITHDRAW on Ethereum (PT redemption).

    Pendle's WITHDRAW path is PT-to-token redemption. Pre-maturity it
    needs both PT and YT (PT+YT burn together for SY); post-maturity
    only PT is needed. We use the post-maturity path: buy PT, advance
    time past maturity, then redeem.

    Note: this test's pattern mirrors
    ``tests/intents/arbitrum/test_pendle_redeem.py`` (which is currently
    marked xfail-grandfathered for a fork-block-pinning flake). The
    arbitrum test is the reference for the receipt-parsing assertions.
    """

    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=False,
        reason=(
            "VIB-4307: PT redemption pattern has a known Anvil fork-block "
            "pinning flake (mirrors arbitrum test_pendle_redeem.py: the "
            "PT approval tx mines but its state may not be visible to the "
            "redeem tx during simulation under cached fork state). "
            "Tracked under #1694 / VIB-3xxx — keep strict=False so a clean "
            "fork-block passes do not surface as XPASS noise. "
            "as of 2026-05-12."
        ),
    )
    async def test_redeem_pt_susde_at_maturity(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Buy PT via sUSDe -> PT swap, advance past maturity, redeem."""
        # ── Step 1: Seed sUSDe and buy PT ────────────────────────────────
        buy_amount = Decimal("10")
        buy_wei = int(buy_amount * Decimal(10**18))
        fund_erc20_token(
            funded_wallet,
            SUSDE_ADDRESS,
            buy_wei * 10,
            SUSDE_BALANCE_SLOT,
            anvil_rpc_url,
        )

        buy_intent = SwapIntent(
            from_token="sUSDe",
            to_token="PT-sUSDe-13AUG2026",
            amount=buy_amount,
            max_slippage=Decimal("0.20"),
            protocol="pendle",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_susde(price_oracle),
            rpc_url=anvil_rpc_url,
        )
        buy_result = compiler.compile(buy_intent)
        assert buy_result.status.value == "SUCCESS", (
            f"PT buy compile failed: {buy_result.error}"
        )
        buy_exec = await orchestrator.execute(buy_result.action_bundle)
        assert buy_exec.success, f"PT buy execution failed: {buy_exec.error}"

        pt_balance = get_token_balance(
            web3, PT_SUSDE_ADDRESS, funded_wallet
        )
        assert pt_balance > 0, "PT buy yielded no PT balance"
        pt_amount_decimal = Decimal(pt_balance) / Decimal(10**18)

        # ── Step 2: Advance past maturity ─────────────────────────────────
        web3.provider.make_request(
            "evm_increaseTime", [_SECONDS_PAST_MATURITY]
        )  # type: ignore[attr-defined]
        web3.provider.make_request("evm_mine", [])  # type: ignore[attr-defined]

        # ── Layer 4 setup ─────────────────────────────────────────────────
        susde_before = get_token_balance(web3, SUSDE_ADDRESS, funded_wallet)
        pt_before_redeem = get_token_balance(
            web3, PT_SUSDE_ADDRESS, funded_wallet
        )

        # ── Layer 1: Compile WithdrawIntent ───────────────────────────────
        # WithdrawIntent.market_id = YT address (required by compile_pendle_redeem)
        redeem_intent = WithdrawIntent(
            token="sUSDe",
            amount=pt_amount_decimal,
            market_id=YT_SUSDE_ADDRESS,
            protocol="pendle",
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(redeem_intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"WithdrawIntent compile failed: {compilation_result.error}"
        )

        # ── Layer 2: Execute ──────────────────────────────────────────────
        execution_result = await orchestrator.execute(
            compilation_result.action_bundle
        )
        assert execution_result.success, (
            f"WITHDRAW execution failed: {execution_result.error}"
        )

        # ── Layer 3: Receipt parsing — RedeemPY or RedeemSY ──────────────
        parser = PendleReceiptParser(chain=CHAIN_NAME)
        py_redeemed_raw: int | None = None
        sy_received_raw: int | None = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            # Pre-maturity: RedeemPY
            if parse_result.redeem_events:
                r = parse_result.redeem_events[0]
                py_redeemed_raw = r.net_py_redeemed
                sy_received_raw = r.net_sy_redeemed
                break
            # Post-maturity: RedeemSY
            if parse_result.redeem_sy_events:
                r2 = parse_result.redeem_sy_events[0]
                sy_received_raw = r2.amount_sy_to_redeem
                py_redeemed_raw = r2.amount_sy_to_redeem
                break
        assert sy_received_raw is not None and sy_received_raw > 0
        assert py_redeemed_raw is not None and py_redeemed_raw > 0

        # ── Layer 4: Balance deltas ───────────────────────────────────────
        susde_after = get_token_balance(web3, SUSDE_ADDRESS, funded_wallet)
        pt_after_redeem = get_token_balance(
            web3, PT_SUSDE_ADDRESS, funded_wallet
        )

        susde_received = susde_after - susde_before
        pt_spent = pt_before_redeem - pt_after_redeem

        assert pt_spent == pt_balance, (
            f"All PT must be burned. Expected: {pt_balance}, Got: {pt_spent}"
        )
        assert susde_received > 0, (
            "sUSDe must be received on PT redemption (no-op guard)"
        )
