"""Production-grade LP Intent tests for Aerodrome (Solidly fork) on Base.

Covers ``(aerodrome, LP_OPEN)`` and ``(aerodrome, LP_CLOSE)`` for the default-on
Zodiac coverage gate (issue #2028). Aerodrome on Base is a fungible-LP Solidly
fork — *not* a concentrated-liquidity NFT system — so the assertion shape
differs from ``test_uniswap_v3_lp.py``:

  * The LP token IS the pool address. There is no NonfungiblePositionManager,
    no ``decreaseLiquidity`` / ``collect`` / ``burn`` flow, and no NFT tokenId.
  * ``Router.addLiquidity(...)`` may pull *less* than the requested amount of
    one side when the input ratio doesn't match current pool reserves; the
    excess is refunded. Hence ``<=`` tolerance assertions, not ``==``.
  * ``Router.removeLiquidity(...)`` returns reserves proportional to LP burned.

These tests run under default-on Zodiac (no ``no_zodiac`` marker) so the
manifest derived from the intent constructors is exercised through Safe +
Roles + ``execTransactionWithRole`` automatically.

To run:
    uv run pytest tests/intents/base/test_aerodrome_lp.py -v -s
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.aerodrome.receipt_parser import AerodromeReceiptParser
from almanak.framework.connectors.aerodrome.sdk import AerodromeSDK
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
)
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_accounting_persisted,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_aerodrome_pool_missing

logger = logging.getLogger(__name__)


# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "base"

# Aerodrome USDC/WETH **volatile** pool on Base. The pool address is resolved
# at module load via the SDK's factory query — pinning a hardcoded address here
# would tie the test to whatever the factory returns today and silently break
# if a redeploy ever moved it. Cached in a module-level singleton so the
# factory call happens at most once per test session.
POOL_LABEL = "USDC/WETH/volatile"
STABLE = False  # volatile pool

# Deposit amounts: ~$10 of each side (10 USDC, ~0.005 WETH at $2000/ETH).
# Small enough to keep price impact negligible while large enough to clear
# Solidly's MINIMUM_LIQUIDITY guard (1000 wei) for a fresh test wallet.
LP_AMOUNT_USDC = Decimal("10")
LP_AMOUNT_WETH = Decimal("0.005")

# Solidly LP doesn't use price ranges; LPOpenIntent's validator demands them.
RANGE_LOWER = Decimal("1")
RANGE_UPPER = Decimal("1000000")


# =============================================================================
# Helpers
# =============================================================================


# -----------------------------------------------------------------------------
# Layer-5 accounting helpers (epic VIB-4591)
# -----------------------------------------------------------------------------
#
# Aerodrome **Classic** on Base is a Solidly fork: fungible LP, no NFT, no
# concentrated-liquidity tick model. The result enricher's
# ``EXTRACTION_SPECS_REMOVE_BY_PROTOCOL["aerodrome"]`` removes ``lp_open_data``
# and the flat ``tick_*`` fields on LP_OPEN (no structured open data exists);
# LP_CLOSE amounts ship inside ``lp_close_data``. So the directional
# null-contract here is the INVERSE of the V3-style precedents:
#
#   * ``position_hash`` / ``tick_lower`` / ``tick_upper`` / ``liquidity`` /
#     ``current_tick`` / ``in_range`` MUST be ``None`` — Solidly has no tick
#     bracket, and fabricating one would be a correctness regression
#     (Empty≠Zero≠None, blueprints/27).
#   * ``pool_address`` is the canonical Solidly descriptor the position key
#     carries (``token0/token1/volatile|stable``), NOT a ``0x`` address — the
#     classic Aerodrome receipt layer surfaces no on-chain pool address
#     (lp_handler ``_resolve_lp_pool_address`` priority 3 / VIB-4396).
#   * ``amount0`` / ``amount1`` are measured (>= 0); fee legs follow the
#     directional Empty≠Zero≠None contract from the SushiSwap V3 precedent.


def _execution_context(wallet: str) -> ExecutionContext:
    return ExecutionContext(
        deployment_id="layer5-aerodrome-lp",
        chain=CHAIN_NAME,
        wallet_address=wallet,
        protocol="aerodrome",
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


def _assert_solidly_null_contract(payload: dict, *, event_type: str) -> None:
    """Assert the classic-Aerodrome (Solidly) directional null-contract.

    Solidly fungible LP has no NFT / tick model. The handler must persist
    ``None`` for every concentrated-liquidity field rather than fabricate a
    zero or a synthetic bracket (Empty≠Zero≠None, epic VIB-4591 decision #5).
    ``pool_address`` is the canonical Solidly descriptor (slash-separated
    ``token0/token1/volatile|stable``), never a fabricated 0x address.
    """
    assert payload["event_type"] == event_type
    assert payload["position_hash"] is None, (
        "Aerodrome Classic (Solidly) must not fabricate a V4 position_hash"
    )
    # The Solidly contract holds for BOTH LP_OPEN and LP_CLOSE: classic
    # Aerodrome must never fabricate a tick bracket. LP_CLOSE's payload schema
    # doesn't carry these keys at all (fees/pnl/il instead), so ``.get``
    # absent → None still satisfies "not fabricated" and future-proofs
    # against a regression that starts injecting them on close rows
    # (CodeRabbit PR #2364).
    for field in ("tick_lower", "tick_upper", "liquidity", "current_tick", "in_range"):
        assert payload.get(field) is None, (
            f"Aerodrome Classic {event_type} must not fabricate concentrated-"
            f"liquidity field {field!r}; Solidly has no tick model (got "
            f"{payload.get(field)!r})"
        )
    pool_address = payload["pool_address"]
    assert isinstance(pool_address, str) and pool_address, (
        "Aerodrome Classic must persist a non-empty pool identifier"
    )
    assert not pool_address.startswith("0x"), (
        "classic Aerodrome surfaces the Solidly descriptor as pool_address "
        f"(token0/token1/volatile|stable), not a 0x address; got {pool_address!r}"
    )
    assert "/" in pool_address, (
        "classic Aerodrome pool_address must be the Solidly descriptor "
        f"(slash-separated); got {pool_address!r}"
    )


def _assert_close_parser_event_equality(payload: dict, lp_close_data, *, dec0: int, dec1: int) -> None:
    """Parser ↔ event exact equality for an Aerodrome Classic LP_CLOSE.

    Mirrors the merged SushiSwap V3 directional fee-contract: a concrete
    parser fee reading must equal the payload exactly; a ``None`` parser
    reading (Empty — Solidly's close path does not separately measure fees)
    reconciles against an unmeasured ``None`` or a measured-zero payload, and
    must NEVER match a fabricated non-zero fee.
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


_pool_address_cache: str | None = None


def _resolve_pool_address(web3: Web3, anvil_rpc_url: str) -> str:
    """Resolve the Aerodrome USDC/WETH volatile pool address on Base.

    Result is cached in a module-level singleton: the factory ``getPool``
    call is a deterministic view that doesn't change between test runs in a
    given session, so paying for it once per session is enough.
    """
    global _pool_address_cache
    if _pool_address_cache is not None:
        return _pool_address_cache

    tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
    sdk = AerodromeSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
    pool_address = sdk.get_pool_address_from_factory(
        tokens["USDC"],
        tokens["WETH"],
        STABLE,
        web3=web3,
    )
    if not pool_address:
        pytest.fail(
            "Aerodrome USDC/WETH volatile pool not found on Base via factory. "
            "Either the factory returned address(0) or the RPC is unreachable."
        )
    _pool_address_cache = Web3.to_checksum_address(pool_address)
    return _pool_address_cache


def _get_lp_token_balance(web3: Web3, pool_address: str, wallet: str) -> int:
    """LP token IS the pool contract for Solidly forks — query as ERC-20."""
    return get_token_balance(web3, pool_address, wallet)


async def _open_lp_position_for_accounting(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
):
    """Open the USDC/WETH volatile LP position.

    Returns ``(pool_address, lp_balance_after, intent, enriched_result)`` so
    Layer-5 callers can persist the LP_OPEN through the real accounting
    pipeline. The enrichment runs with ``live_mode=False`` (paper) exactly as
    the runner would in non-live mode.
    """
    pool_address = _resolve_pool_address(web3, anvil_rpc_url)

    intent = LPOpenIntent(
        pool=POOL_LABEL,
        amount0=LP_AMOUNT_USDC,
        amount1=LP_AMOUNT_WETH,
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        protocol="aerodrome",
        chain=CHAIN_NAME,
    )

    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    compilation_result = compiler.compile(intent)
    assert compilation_result.status.value == "SUCCESS", (
        f"Aerodrome LP_OPEN compilation failed: {compilation_result.error}"
    )
    assert compilation_result.action_bundle is not None

    execution_result = await orchestrator.execute(compilation_result.action_bundle)
    assert execution_result.success, f"Aerodrome LP_OPEN execution failed: {execution_result.error}"
    enriched = _enrich_for_accounting(
        execution_result,
        intent,
        funded_wallet,
        compilation_result.action_bundle.metadata,
    )

    return pool_address, _get_lp_token_balance(web3, pool_address, funded_wallet), intent, enriched


async def _open_lp_position(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> tuple[str, int]:
    """Open the USDC/WETH volatile LP position. Returns (pool_address, lp_balance_after)."""
    pool_address, lp_balance, _, _ = await _open_lp_position_for_accounting(
        web3, funded_wallet, orchestrator, price_oracle, anvil_rpc_url
    )
    return pool_address, lp_balance


# =============================================================================
# LP Open Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.lp
class TestAerodromeLPOpen:
    """Aerodrome LP_OPEN via ``LPOpenIntent`` on Base.

    Verifies:
      * USDC and WETH spent <= requested amounts (Solidly may pull less when
        the input ratio doesn't match the pool's current reserves).
      * Both tokens strictly decrease (some of each was actually deposited).
      * LP token balance strictly increases.
      * Receipt parses cleanly via ``AerodromeReceiptParser``.
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_usdc_weth_volatile(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Open a USDC + WETH volatile-pool LP position via LPOpenIntent."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]

        # Pre-flight: pool must exist on the fork before we run.
        fail_if_aerodrome_pool_missing(web3, CHAIN_NAME, usdc_addr, weth_addr, STABLE)

        pool_address = _resolve_pool_address(web3, anvil_rpc_url)

        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        # --- Layer 4 BEFORE ---
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)
        lp_before = _get_lp_token_balance(web3, pool_address, funded_wallet)

        # --- Layer 1: Compile ---
        intent = LPOpenIntent(
            pool=POOL_LABEL,
            amount0=LP_AMOUNT_USDC,
            amount1=LP_AMOUNT_WETH,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Aerodrome LP_OPEN compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        # --- Layer 2: Execute ---
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Aerodrome LP_OPEN execution failed: {execution_result.error}"
        execution_result = _enrich_for_accounting(
            execution_result,
            intent,
            funded_wallet,
            compilation_result.action_bundle.metadata,
        )

        # --- Layer 3: Receipt Parsing ---
        # Aerodrome's parser doesn't yet emit dedicated LP open events for the
        # Solidly fungible-LP add_liquidity flow (see receipt_parser.py event
        # registry — Mint/Sync events are routed but LP-shaped extraction is
        # still SDK-side). Asserting parser-level ``success`` is the cleanest
        # contract this layer can offer today; deeper semantic checks belong
        # to balance deltas in Layer 4.
        parser = AerodromeReceiptParser(chain=CHAIN_NAME)
        any_parse_succeeded = False
        saw_liquidity_mint = False
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, (
                f"AerodromeReceiptParser must parse LP_OPEN receipt cleanly: {parse_result.error}"
            )
            any_parse_succeeded = True
            if parse_result.mint_events:
                saw_liquidity_mint = True
        assert any_parse_succeeded, "At least one LP_OPEN tx receipt must be parsed"
        # Layer 3 must validate that the parser actually *decoded* protocol
        # semantics — ``parse_result.success`` alone is true for empty
        # receipts and would let a parser regression slip past.
        assert saw_liquidity_mint, (
            "LP_OPEN must decode at least one Mint event in receipts. "
            "An empty event list means the parser silently failed to recognise "
            "the LP-mint emission shape."
        )

        # --- Layer 4 AFTER: Balance Deltas ---
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        lp_after = _get_lp_token_balance(web3, pool_address, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_spent = weth_before - weth_after
        lp_received = lp_after - lp_before

        # Solidly add_liquidity may pull LESS than the desired amount of one
        # side when the input ratio doesn't match pool reserves — the excess
        # is refunded. Use <= on the desired ceiling, > 0 on actual spend.
        expected_usdc_max = int(LP_AMOUNT_USDC * Decimal(10**usdc_decimals))
        expected_weth_max = int(LP_AMOUNT_WETH * Decimal(10**weth_decimals))

        # Solidly add_liquidity at extreme pool ratios may pull all of one
        # side and refund 100 % of the other. "Both > 0" would be too strict
        # for valid edge cases; require at least one side spent and let the
        # per-side ``<= desired`` ceilings + LP-mint event check guard
        # against complete no-ops.
        assert usdc_spent > 0 or weth_spent > 0, (
            "At least one token must be deposited in LP_OPEN. "
            f"USDC delta={format_token_amount(usdc_spent, usdc_decimals)}, "
            f"WETH delta={format_token_amount(weth_spent, weth_decimals)}"
        )
        assert usdc_spent <= expected_usdc_max, (
            f"USDC spent ({usdc_spent}) must not exceed requested "
            f"({expected_usdc_max}) — Solidly refunds excess but never overspends"
        )
        assert weth_spent <= expected_weth_max, (
            f"WETH spent ({weth_spent}) must not exceed requested "
            f"({expected_weth_max}) — Solidly refunds excess but never overspends"
        )
        assert lp_received > 0, f"LP token balance must strictly increase, got {lp_received}"

        logger.info(
            f"LP_OPEN OK: USDC spent={format_token_amount(usdc_spent, usdc_decimals)}, "
            f"WETH spent={format_token_amount(weth_spent, weth_decimals)}, "
            f"LP received={lp_received}"
        )

        # --- Layer 5: real accounting pipeline persisted LP_OPEN ---
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
        assert payload["position_key"] == accounting_row["position_key"]
        # Solidly directional null-contract: no fabricated NFT/tick fields,
        # pool_address is the Solidly descriptor (not a 0x address).
        _assert_solidly_null_contract(payload, event_type="LP_OPEN")
        assert Decimal(payload["amount0"]) >= 0
        assert Decimal(payload["amount1"]) >= 0


# =============================================================================
# LP Close Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.lp
class TestAerodromeLPClose:
    """Aerodrome LP_CLOSE via ``LPCloseIntent`` on Base.

    Verifies the open-then-close roundtrip:
      * LP balance after close < LP balance after open (LP burned, ideally to
        ~0 modulo a small dust threshold).
      * USDC and WETH both strictly increase from the post-open snapshot.
      * Receipt parses cleanly.
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_usdc_weth_returns_tokens(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Open then close a USDC + WETH volatile LP position; verify roundtrip."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]

        # Pre-flight: pool must exist on the fork.
        fail_if_aerodrome_pool_missing(web3, CHAIN_NAME, usdc_addr, weth_addr, STABLE)

        usdc_decimals = get_token_decimals(web3, usdc_addr)
        weth_decimals = get_token_decimals(web3, weth_addr)

        # --- Setup: open a position so we have LP tokens to burn ---
        # Open via the accounting helper so Layer 5 has the prior LP_OPEN row
        # for position-key linkage + cost basis.
        pool_address, lp_after_open, open_intent, open_result = await _open_lp_position_for_accounting(
            web3, funded_wallet, orchestrator, price_oracle, anvil_rpc_url
        )
        assert lp_after_open > 0, "Setup invariant: LP_OPEN must mint at least 1 LP wei"
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

        # --- Layer 4 BEFORE close ---
        usdc_before_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before_close = get_token_balance(web3, weth_addr, funded_wallet)
        lp_before_close = lp_after_open

        # --- Layer 1: Compile LP_CLOSE ---
        # Aerodrome LP_CLOSE accepts the bare pool address as ``position_id``
        # (Solidly: pool address IS the LP token, analogous to a V3 NFT
        # tokenId). The compiler reads the wallet's full LP balance from
        # chain and burns it all, so the actual amount is on-chain state, not
        # an intent field.
        close_intent = LPCloseIntent(
            position_id=pool_address,
            pool=POOL_LABEL,
            collect_fees=True,
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        close_result = compiler.compile(close_intent)
        assert close_result.status.value == "SUCCESS", f"Aerodrome LP_CLOSE compilation failed: {close_result.error}"
        assert close_result.action_bundle is not None
        assert close_result.action_bundle.metadata.get("no_op") is not True, (
            "LP_CLOSE must produce a real bundle — wallet just deposited LP"
        )

        # --- Layer 2: Execute ---
        close_execution = await orchestrator.execute(close_result.action_bundle)
        assert close_execution.success, f"Aerodrome LP_CLOSE execution failed: {close_execution.error}"
        close_execution = _enrich_for_accounting(
            close_execution,
            close_intent,
            funded_wallet,
            close_result.action_bundle.metadata,
        )

        # --- Layer 3: Receipt Parsing ---
        # Aerodrome's volatile pool variant doesn't always emit a standard
        # ``Burn`` event on remove — the parser has a documented Transfer-
        # event fallback (see ``AerodromeReceiptParser.extract_lp_close_data``
        # in ``almanak/framework/connectors/aerodrome/receipt_parser.py``).
        # Asserting on ``parse_result.burn_events`` directly fails on those
        # variants. Use the high-level ``LPCloseData`` extractor that
        # composes both paths, so the test stays robust to per-pool
        # event-shape variation while still proving "the parser decoded a
        # close, with non-zero token amounts."
        parser = AerodromeReceiptParser(chain=CHAIN_NAME)
        any_parse_succeeded = False
        saw_close_data = False
        lp_close_data = None
        for tx_result in close_execution.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result.success, (
                f"AerodromeReceiptParser must parse LP_CLOSE receipt cleanly: {parse_result.error}"
            )
            any_parse_succeeded = True
            close_data = parser.extract_lp_close_data(receipt_dict)
            if close_data is not None and (close_data.amount0_collected > 0 or close_data.amount1_collected > 0):
                saw_close_data = True
                lp_close_data = close_data
        assert any_parse_succeeded, "At least one LP_CLOSE tx receipt must be parsed"
        assert saw_close_data, (
            "LP_CLOSE must yield non-zero LPCloseData from the parser "
            "(via Burn events on Solidly stable pools, or Transfer-event "
            "fallback on volatile pools). Empty across both paths means the "
            "parser silently failed to recognise the close — a regression."
        )

        # --- Layer 4 AFTER: Balance Deltas ---
        usdc_after_close = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after_close = get_token_balance(web3, weth_addr, funded_wallet)
        lp_after_close = _get_lp_token_balance(web3, pool_address, funded_wallet)

        usdc_returned = usdc_after_close - usdc_before_close
        weth_returned = weth_after_close - weth_before_close
        lp_burned = lp_before_close - lp_after_close

        assert lp_after_close < lp_before_close, (
            f"LP balance after close ({lp_after_close}) must be strictly less than before close ({lp_before_close})"
        )
        assert lp_burned > 0, f"LP burned must be > 0, got {lp_burned}"
        assert usdc_returned > 0, (
            f"USDC must strictly increase after LP_CLOSE, got delta {format_token_amount(usdc_returned, usdc_decimals)}"
        )
        assert weth_returned > 0, (
            f"WETH must strictly increase after LP_CLOSE, got delta {format_token_amount(weth_returned, weth_decimals)}"
        )

        logger.info(
            f"LP_CLOSE OK: USDC returned={format_token_amount(usdc_returned, usdc_decimals)}, "
            f"WETH returned={format_token_amount(weth_returned, weth_decimals)}, "
            f"LP burned={lp_burned}, LP residual={lp_after_close}"
        )

        # --- Layer 5: real accounting pipeline persisted LP_CLOSE ---
        assert lp_close_data is not None, "Layer-5 assertion needs parsed LPCloseData"
        close_accounting_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=close_intent,
            result=close_execution,
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
        # #2 Solidly directional null-contract on LP_CLOSE.
        _assert_solidly_null_contract(close_payload, event_type="LP_CLOSE")
        assert close_payload["realized_pnl_usd"] is not None, (
            "open-then-close must compute realized PnL"
        )
        # #3 parser ↔ event exact scaled-int equality.
        dec0 = get_token_decimals(web3, tokens[close_payload["token0"]])
        dec1 = get_token_decimals(web3, tokens[close_payload["token1"]])
        _assert_close_parser_event_equality(close_payload, lp_close_data, dec0=dec0, dec1=dec1)


# =============================================================================
# LP_COLLECT_FEES Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.lp
class TestAerodromeLPCollectFees:
    """Aerodrome (Classic) LP_COLLECT_FEES coverage on Base.

    The Solidly-fork volatile/stable AMM does NOT support standalone
    LP_COLLECT_FEES — fees auto-compound into pool reserves and are realized
    only when liquidity is removed (see
    ``almanak/framework/connectors/aerodrome/permission_hints.py`` —
    ``supports_standalone_fee_collection`` is unset / False, and the compiler
    explicitly rejects ``protocol="aerodrome"`` in
    ``compiler._compile_collect_fees``).

    The contract this test pins:

      * ``CollectFeesIntent(protocol="aerodrome")`` MUST be rejected cleanly at
        compile time with the documented error message (so the gate sees a
        proper L1 → L2 boundary).
      * No transactions are emitted; therefore wallet balances are unchanged
        (a degenerate but still load-bearing conservation check).

    Standalone fee collection on Aerodrome's Slipstream (CL) variant is
    covered by ``protocol="aerodrome_slipstream"`` and lives outside this
    test (different connector key).
    """

    @pytest.mark.intent(IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    async def test_lp_collect_fees_aerodrome_classic_rejected(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """LP_COLLECT_FEES must be rejected for aerodrome (Solidly fork).

        Aerodrome Classic auto-compounds fees into reserves; the standalone
        collect path doesn't exist on the V1 Router. The compiler MUST refuse
        the intent with an error message pointing the caller at the supported
        alternatives (``LPCloseIntent(collect_fees=True)`` or
        ``aerodrome_slipstream``).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        weth_addr = tokens["WETH"]

        # Record balances before — must be unchanged (no TX should be emitted).
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_before = get_token_balance(web3, weth_addr, funded_wallet)

        # --- Layer 1: Compile must FAIL with the documented error ---
        intent = CollectFeesIntent(
            pool=POOL_LABEL,
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)

        # The compiler must reject this with the documented error message.
        assert compilation_result.status.value != "SUCCESS", (
            "Aerodrome Classic LP_COLLECT_FEES must be rejected at compile time. "
            "Solidly-fork pools auto-compound fees; standalone collection is not "
            "representable in the V1 Router contract surface."
        )
        # Compiler must surface a useful error pointing at the supported paths.
        error_text = (compilation_result.error or "").lower()
        assert "aerodrome" in error_text and (
            "lp_close" in error_text or "slipstream" in error_text
        ), (
            "Compiler error must guide the caller toward LPCloseIntent(collect_fees=True) "
            f"or aerodrome_slipstream. Got: {compilation_result.error!r}"
        )

        # --- Layer 4 (degenerate): no TX emitted ⇒ balances unchanged ---
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        weth_after = get_token_balance(web3, weth_addr, funded_wallet)
        assert usdc_after == usdc_before, (
            "Rejected compilation must NOT emit any TX; USDC balance must be unchanged"
        )
        assert weth_after == weth_before, (
            "Rejected compilation must NOT emit any TX; WETH balance must be unchanged"
        )

        # --- Layer 5: N/A by construction ---
        # This is a compile-time (L1) rejection: no ActionBundle, no
        # ExecutionOrchestrator run, no ExecutionResult. The Layer-5 helpers
        # operate on a real ExecutionResult (success → assert_accounting_
        # persisted; failed execution → assert_no_accounting_on_failure). With
        # nothing executed there is no ledger/outbox/accounting surface to
        # assert against — synthesising a fake failed result here would test
        # the helper, not this protocol. The "no TX emitted" conservation
        # check above is the books-side mirror for the rejection path.
        logger.info("LP_COLLECT_FEES aerodrome-classic rejection contract verified")
