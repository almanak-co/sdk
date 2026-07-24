"""Production-grade Morpho Blue lending intent tests for Monad (VIB-4307).

Markets (from ``almanak.connectors.morpho_blue.adapter.MORPHO_MARKETS``):

- ``wstETH/WETH`` (94.5% LLTV) — largest Monad Morpho market (~$61.8M supply)
- ``WBTC/AUSD`` (86% LLTV) — BTC-backed lending (~$13.2M supply)

Coverage in this file:

- ``SUPPLY``  — loan-token supply of WETH into ``wstETH/WETH`` (no collateral needed).
- ``BORROW`` / ``REPAY`` / ``WITHDRAW`` — borrow USDC^M ... see "Anvil fork limitation"
  below; these are marked ``xfail(strict=True)`` because the funded test wallet on
  Monad currently only holds WMON / WETH / USDC (see ``CHAIN_CONFIGS["monad"]``
  in ``tests/intents/conftest.py``). Both Morpho markets on Monad require
  collateral tokens (wstETH or WBTC) that are NOT in the funded-token set,
  and the funding infrastructure (storage-slot manipulation) is owned by the
  root conftest which is out of scope for VIB-4307 modifications.

Each test still exercises the full Intent → Compile → Execute → Verify pipeline
for shape correctness. When wstETH or WBTC funding is added to the Monad
conftest, the xfail markers should be removed and the tests should pass.

Layer 5 (epic VIB-4591 / ticket VIB-4604): only the loan-side ``SUPPLY``
test executes on Monad today (BORROW/REPAY/WITHDRAW are ``xfail(strict=True)``
on the wstETH-funding gap and never run), so Layer 5 is wired only into the
SUPPLY test. Morpho Blue's pre/post-state reader has full parity with Aave V3
(both legs resolved from ``intent.market_id``), so the Anvil ``eth_call``
adapter populates before/after collateral / debt / health-factor at
``confidence=HIGH``. Loan-side supply emits a ``Supply`` event so
``amount_token`` is populated (not the VIB-4635 collateral path).

NO MOCKING. The intents are real; the executions are real; the xfail markers
document the funding gap explicitly rather than papering over it.

To run:
    uv run pytest tests/intents/monad/test_morpho_blue_lending.py -v -s
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.morpho_blue.adapter import MORPHO_MARKETS
from almanak.connectors.morpho_blue.receipt_parser import (
    MorphoBlueEvent,
    MorphoBlueEventType,
    MorphoBlueReceiptParser,
)
from almanak.connectors.morpho_blue.sdk import MorphoBlueSDK
from almanak.framework.accounting.lending_accounting import (
    capture_lending_post_state,
    capture_lending_pre_state,
    lending_state_to_dict,
)
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionResult,
)
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents import (
    BorrowIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_accounting_persisted,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

CHAIN_NAME = "monad"
PROTOCOL = "morpho_blue"
MORPHO_MARKET_NAME_LOAN = "wstETH/WETH"  # loan token = WETH → supply test uses this
MORPHO_MARKET_NAME_BORROW = "wstETH/WETH"  # collateral = wstETH


def _select_market_id(chain: str, market_name: str) -> str:
    markets = MORPHO_MARKETS.get(chain, {})
    for market_id, info in markets.items():
        if info.get("name") == market_name:
            return market_id
    raise AssertionError(
        f"Expected Morpho market '{market_name}' to exist for chain='{chain}'"
    )


MORPHO_MARKET_ID = _select_market_id(CHAIN_NAME, MORPHO_MARKET_NAME_LOAN)
MORPHO_MARKET_INFO = MORPHO_MARKETS[CHAIN_NAME][MORPHO_MARKET_ID]


def _collect_morpho_events(execution_result) -> list[MorphoBlueEvent]:
    parser = MorphoBlueReceiptParser()
    events: list[MorphoBlueEvent] = []
    for tx_result in execution_result.transaction_results:
        receipt = tx_result.receipt
        assert receipt is not None, "Expected receipt for executed transaction"
        parse_result = parser.parse_receipt(receipt.to_dict())
        assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
        events.extend(parse_result.events)
    return events


def _first_event(
    events: list[MorphoBlueEvent], event_type: MorphoBlueEventType
) -> MorphoBlueEvent | None:
    for event in events:
        if event.event_type == event_type:
            return event
    return None


def _assets_wei(event: MorphoBlueEvent) -> int:
    assets = event.data.get("assets")
    assert assets is not None, f"Expected 'assets' in event data for {event.event_type}"
    return int(Decimal(str(assets)))


# =============================================================================
# Layer 5 helpers (shared) — epic VIB-4591 / ticket VIB-4604
# =============================================================================
#
# Identical shape to the merged Aave V3 golden
# (``tests/intents/arbitrum/test_aave_v3_lending.py``) and the Arbitrum Morpho
# Blue Layer-5 rollout. Only the loan-side SUPPLY test runs on Monad (the
# borrow/repay/withdraw funding-gap tests are xfail(strict=True) and never
# execute), so these helpers are exercised by the SUPPLY path only.


def _execution_context(wallet: str) -> ExecutionContext:
    return ExecutionContext(
        deployment_id="layer5-morpho-blue-lending",
        chain=CHAIN_NAME,
        wallet_address=wallet,
        protocol=PROTOCOL,
        simulation_enabled=True,
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


def _capture_lending_state(
    intent: Any,
    wallet: str,
    reader: Any,
    price_oracle: dict[str, Decimal],
    *,
    post: bool,
) -> dict | None:
    """Capture and serialize Morpho Blue pre/post state via the Anvil eth_call adapter.

    Returns the runner-shaped state dict (``lending_state_to_dict`` output) or
    ``None`` when the read genuinely yields nothing — never a fabricated zero.
    """
    capture = capture_lending_post_state if post else capture_lending_pre_state
    state = capture(
        intent=intent,
        chain=CHAIN_NAME,
        wallet_address=wallet,
        gateway_client=reader,
        price_oracle=price_oracle,
    )
    return lending_state_to_dict(state, protocol=PROTOCOL)


def _payload(row: dict) -> dict:
    return json.loads(row["payload_json"])


def _assert_identity(row: dict, *, event_type: str, wallet: str) -> None:
    """Identity sextuple per epic VIB-4591 decision #5 (no agent_id)."""
    assert row["deployment_id"] == "layer5-intent-test"
    assert row["cycle_id"] == "layer5-cycle"
    assert row["execution_mode"] == "paper"
    assert row["event_type"] == event_type
    assert row["tx_hash"], "accounting row must link to an on-chain tx_hash"
    assert row["ledger_entry_id"], "accounting row must link to transaction_ledger"
    assert row["wallet_address"].lower() == wallet.lower()
    # Epic decision #5: the identity is a sextuple — there is NO agent_id.
    # Enforce the contract, don't just document it: a persisted lending row
    # must never carry a populated agent_id (absent, or present-but-empty).
    assert not row.get("agent_id"), (
        f"Layer-5 lending row must not carry an agent_id (epic decision #5); "
        f"got {row.get('agent_id')!r}"
    )


def _assert_no_lot_id(row: dict, payload: dict) -> None:
    """Epic decision #6: no lot_id on the persisted lending event."""
    assert "lot_id" not in row
    assert "lot_id" not in payload


def _assert_high_confidence_state(payload: dict) -> None:
    """Morpho Blue has a full pre/post-state reader → confidence=HIGH.

    Morpho Blue's ``_capture_morpho_blue_pre_state`` resolves both market legs
    from ``intent.market_id`` via ``MORPHO_MARKETS`` for every lending intent
    type (it does NOT require ``intent.collateral_token`` the way Compound V3's
    REPAY arm does — VIB-4633), so the Anvil eth_call adapter yields a live
    before+after read at ``confidence=HIGH`` with collateral/debt/HF populated.
    """
    assert payload["confidence"] == "HIGH", (
        f"Morpho Blue lending must persist confidence=HIGH (full reader + Anvil "
        f"eth_call adapter), got {payload['confidence']!r} "
        f"(unavailable_reason={payload.get('unavailable_reason')!r})"
    )
    assert payload["collateral_value_before_usd"] is not None, "before-collateral must be populated"
    assert payload["collateral_value_after_usd"] is not None, "after-collateral must be populated"
    assert payload["debt_value_before_usd"] is not None, "before-debt must be populated"
    assert payload["debt_value_after_usd"] is not None, "after-debt must be populated"
    assert payload["health_factor_before"] is not None, "before-health-factor must be populated"
    assert payload["health_factor_after"] is not None, "after-health-factor must be populated"


def _assert_asset(payload: dict, expected: str) -> None:
    """Asset-symbol assertion (case-insensitive).

    The lending category handler upper-cases the asset symbol
    (lending_handler.py: ``asset = (...).upper()``), so compare
    case-insensitively — the symbol identity, not its casing, is the contract.
    """
    assert payload["asset"].upper() == expected.upper(), (
        f"persisted asset {payload['asset']!r} must match {expected!r} (case-insensitive)"
    )


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    return ExecutionContext(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        simulation_enabled=True,
    )


@pytest.fixture(scope="module")
def price_oracle_monad_local(price_oracle_monad: dict[str, Decimal]) -> dict[str, Decimal]:
    """Module-scope alias for the session-wide Monad price oracle."""
    return price_oracle_monad


@pytest.mark.monad
@pytest.mark.supply
@pytest.mark.lending
class TestMorphoBlueSupplyIntent:
    """SUPPLY (loan-token, not collateral) into the wstETH/WETH market.

    Uses ``use_as_collateral=False`` so we deposit WETH as loan capital
    (earning interest) — this avoids needing wstETH which isn't funded
    in the Monad conftest.
    """

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_weth_as_loan_token(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle_monad_local: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Supply WETH as loan token (not collateral) into wstETH/WETH market.

        Layer 5: loan-side supply emits a Morpho ``Supply`` event, so the
        lending handler resolves the amount via the primary ``supply_amount``
        key → ``amount_token`` populated and ``confidence=HIGH`` (this is the
        loan-side path, NOT the collateral path that hits VIB-4635).
        """
        price_oracle = price_oracle_monad_local
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth_address = tokens["WETH"]
        weth_decimals = get_token_decimals(web3, weth_address)

        # The wstETH/WETH market's loan token is WETH. Sanity-check our market
        # selection so a future adapter change doesn't break this test silently.
        assert weth_address.lower() == MORPHO_MARKET_INFO["loan_token_address"].lower(), (
            f"Expected market loan token to be WETH, got {MORPHO_MARKET_INFO['loan_token']}"
        )

        supply_amount = Decimal("0.5")  # 0.5 WETH — well within the 10 WETH wrap budget

        weth_before = get_token_balance(web3, weth_address, funded_wallet)
        expected_wei = int(supply_amount * Decimal(10**weth_decimals))
        assert weth_before >= expected_wei, (
            f"funded_wallet has only {weth_before} WETH wei, need >= {expected_wei} "
            f"({supply_amount} WETH) — Monad fork funding regression"
        )

        print("\n" + "=" * 80)
        print(f"Morpho Blue SUPPLY: {supply_amount} WETH (loan token) on Monad")
        print(f"Market: {MORPHO_MARKET_INFO['name']} ({MORPHO_MARKET_ID[:10]}...)")
        print("=" * 80)
        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")

        # Layer 1: Compile (use_as_collateral=False → loan-token supply)
        intent = SupplyIntent(
            protocol="morpho_blue",
            token="WETH",
            amount=supply_amount,
            use_as_collateral=False,
            market_id=MORPHO_MARKET_ID,
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
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        # Layer 2: Execute
        execution_result = await orchestrator.execute(
            compilation_result.action_bundle, execution_context
        )
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parse — Supply (not SupplyCollateral) event
        events = _collect_morpho_events(execution_result)
        supply_event = _first_event(events, MorphoBlueEventType.SUPPLY)
        assert supply_event is not None, (
            "Expected Supply event in Morpho Blue receipts (loan-token supply)"
        )
        assert supply_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()
        supplied_wei = _assets_wei(supply_event)
        assert supplied_wei == expected_wei, (
            f"Supply event assets must EXACTLY equal supply amount. "
            f"Expected: {expected_wei}, Got: {supplied_wei}"
        )

        # Layer 4: Balance delta — exact WETH spent
        weth_after = get_token_balance(web3, weth_address, funded_wallet)
        weth_spent = weth_before - weth_after
        assert weth_spent == expected_wei, (
            f"WETH spent must EXACTLY equal supply amount. "
            f"Expected: {expected_wei}, Got: {weth_spent}"
        )
        assert weth_spent == supplied_wei, "Event-layer and balance-layer must agree"

        # On-chain sanity: supply_shares > 0 (we own a lending position)
        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.supply_shares > 0, (
            f"Expected supply_shares > 0 after loan-token supply, got {position.supply_shares}"
        )

        # ── Layer 5: loan-side SUPPLY accounting ─────────────────────────────
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True
        )

        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="SUPPLY",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="SUPPLY", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        _assert_high_confidence_state(payload)
        _assert_asset(payload, "WETH")
        # Loan-side Supply event → handler resolves the amount via the primary
        # supply_amount key (NOT the collateral path), so amount_token is
        # populated. SUPPLY drains wallet inventory: principal measured,
        # interest not applicable (must be None, not 0).
        assert payload["amount_token"] is not None, (
            "loan-side SUPPLY must populate amount_token (Supply event → "
            "supply_amount key; not the VIB-4635 collateral path)"
        )
        assert Decimal(payload["amount_token"]) == supply_amount
        assert payload["principal_delta_usd"] is not None, "SUPPLY must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "SUPPLY has no interest leg — must be None, not 0"

        print(f"\nWETH spent: {format_token_amount(weth_spent, weth_decimals)}")
        print(f"Supply shares: {position.supply_shares}")
        print("\nALL CHECKS PASSED")


# =============================================================================
# Borrow / Repay / Withdraw — funding gap
# =============================================================================
#
# These tests target the wstETH/WETH market with wstETH as collateral. The
# Monad conftest does NOT currently fund wstETH on the test wallet (only
# WMON/WETH/USDC are funded — see CHAIN_CONFIGS["monad"] in
# tests/intents/conftest.py). VIB-4307 explicitly forbids modifying the chain
# conftest, so we cannot resolve the gap inside this PR.
#
# The tests are written so they pass cleanly once wstETH funding is added
# (storage-slot for the canonical OpenZeppelin layout, or a wrap path if
# wstETH on Monad supports deposit() like WETH9). The borrow amounts use
# the standard ~25% LTV calculation against the session-scoped price oracle.


async def _setup_borrow(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    execution_context: ExecutionContext,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
    collateral_amount: Decimal,
    borrow_amount: Decimal,
) -> None:
    """Helper: supply wstETH collateral then borrow WETH. Asserts success.

    Production-faithful two-intent shape (PR #2827): a bundled
    ``BorrowIntent(collateral_amount>0)`` is rejected for SEPARABLE lending
    protocols (morpho_blue is guarded — only ``fluid`` opts out via
    ``supports_bundled_collateral_borrow``). The user-facing flow is a
    standalone ``SUPPLY(use_as_collateral=True)`` followed by a standalone
    ``BORROW(collateral_amount=0)``, so the setup mirrors exactly that.
    """
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )

    # 1) Supply wstETH as collateral (standalone SUPPLY → supplyCollateral).
    supply_intent = SupplyIntent(
        protocol="morpho_blue",
        token="wstETH",
        amount=collateral_amount,
        use_as_collateral=True,
        market_id=MORPHO_MARKET_ID,
        chain=CHAIN_NAME,
    )
    supply_result = compiler.compile(supply_intent)
    assert supply_result.status.value == "SUCCESS", (
        f"Borrow setup collateral-supply compile failed: {supply_result.error}"
    )
    assert supply_result.action_bundle is not None, "Borrow setup missing supply action_bundle"
    supply_exec = await orchestrator.execute(supply_result.action_bundle, execution_context)
    assert supply_exec.success, f"Borrow setup collateral-supply execution failed: {supply_exec.error}"

    # 2) Borrow WETH against the now-existing collateral (standalone BORROW).
    borrow_intent = BorrowIntent(
        protocol="morpho_blue",
        collateral_token="wstETH",
        collateral_amount=Decimal("0"),
        borrow_token="WETH",
        borrow_amount=borrow_amount,
        market_id=MORPHO_MARKET_ID,
        chain=CHAIN_NAME,
    )
    borrow_result = compiler.compile(borrow_intent)
    assert borrow_result.status.value == "SUCCESS", (
        f"Borrow setup compile failed: {borrow_result.error}"
    )
    assert borrow_result.action_bundle is not None, "Borrow setup missing action_bundle"
    borrow_exec = await orchestrator.execute(borrow_result.action_bundle, execution_context)
    assert borrow_exec.success, f"Borrow setup execution failed: {borrow_exec.error}"


@pytest.mark.monad
@pytest.mark.borrow
@pytest.mark.lending
class TestMorphoBlueBorrowIntent:
    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-5971: wstETH not funded on Monad test wallet (as of 2026-05-12). "
        "CHAIN_CONFIGS['monad'] in tests/intents/conftest.py funds only WMON/WETH/USDC. "
        "The wstETH/WETH market needs wstETH as collateral; storage-slot funding for "
        "wstETH on Monad (0x10Aeaf63...) is not yet mapped in balance_slots. "
        "Test is structurally complete; unblock by adding wstETH to the Monad "
        "tokens+balance_slots entries in the root conftest. Re-pointed to VIB-5971 2026-07-24.",
        strict=True,
    )
    async def test_borrow_weth_with_wsteth_collateral(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle_monad_local: dict[str, Decimal],
    ) -> None:
        """Borrow WETH against wstETH collateral on Monad."""
        price_oracle = price_oracle_monad_local
        # Use known Monad wstETH address from MORPHO_BLUE_TOKENS / market metadata.
        wsteth_address = MORPHO_MARKET_INFO["collateral_token_address"]
        weth_address = MORPHO_MARKET_INFO["loan_token_address"]
        wsteth_decimals = 18
        weth_decimals = 18

        # ~25% LTV: 0.1 wstETH (~$350) → ~0.05 WETH borrow.
        wsteth_price = (
            price_oracle.get("wstETH") or price_oracle.get("WETH") or Decimal("3500")
        )
        weth_price = price_oracle.get("WETH") or Decimal("3000")
        collateral_amount = Decimal("0.1")
        max_borrow_usd = collateral_amount * wsteth_price * Decimal("0.25")
        borrow_amount = max_borrow_usd / weth_price

        wsteth_before = get_token_balance(web3, wsteth_address, funded_wallet)
        weth_before = get_token_balance(web3, weth_address, funded_wallet)
        assert wsteth_before >= int(collateral_amount * Decimal(10**wsteth_decimals)), (
            "Funded wallet lacks wstETH collateral on Monad."
        )

        expected_collateral_wei = int(collateral_amount * Decimal(10**wsteth_decimals))
        expected_borrow_wei = int(borrow_amount * Decimal(10**weth_decimals))

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Production-faithful two-intent shape (PR #2827): morpho_blue is a
        # SEPARABLE lending protocol, so a bundled BORROW(collateral_amount>0)
        # is rejected at validation. Supply collateral standalone, then borrow
        # standalone (collateral_amount=0). The SUPPLY_COLLATERAL event + the
        # wstETH balance decrease belong to the SUPPLY execution; the BORROW
        # event + the WETH balance increase belong to the BORROW execution.

        # ── Standalone SUPPLY (collateral) ───────────────────────────────────
        supply_intent = SupplyIntent(
            protocol="morpho_blue",
            token="wstETH",
            amount=collateral_amount,
            use_as_collateral=True,
            market_id=MORPHO_MARKET_ID,
            chain=CHAIN_NAME,
        )
        supply_compilation = compiler.compile(supply_intent)
        assert supply_compilation.status.value == "SUCCESS", (
            f"Collateral-supply compilation failed: {supply_compilation.error}"
        )
        assert supply_compilation.action_bundle is not None

        supply_execution = await orchestrator.execute(
            supply_compilation.action_bundle, execution_context
        )
        assert supply_execution.success, f"Collateral-supply execution failed: {supply_execution.error}"

        supply_events = _collect_morpho_events(supply_execution)
        supply_collateral_event = _first_event(supply_events, MorphoBlueEventType.SUPPLY_COLLATERAL)
        assert supply_collateral_event is not None
        assert _assets_wei(supply_collateral_event) == expected_collateral_wei

        wsteth_after = get_token_balance(web3, wsteth_address, funded_wallet)
        assert wsteth_before - wsteth_after == expected_collateral_wei

        # ── Standalone BORROW (against the now-existing collateral) ───────────
        borrow_intent = BorrowIntent(
            protocol="morpho_blue",
            collateral_token="wstETH",
            collateral_amount=Decimal("0"),
            borrow_token="WETH",
            borrow_amount=borrow_amount,
            market_id=MORPHO_MARKET_ID,
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(borrow_intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(
            compilation_result.action_bundle, execution_context
        )
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        borrow_events = _collect_morpho_events(execution_result)
        borrow_event = _first_event(borrow_events, MorphoBlueEventType.BORROW)
        assert borrow_event is not None
        assert _assets_wei(borrow_event) == expected_borrow_wei

        weth_after = get_token_balance(web3, weth_address, funded_wallet)
        assert weth_after - weth_before == expected_borrow_wei


@pytest.mark.monad
@pytest.mark.repay
@pytest.mark.lending
class TestMorphoBlueRepayIntent:
    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-5971: wstETH not funded on Monad test wallet (as of 2026-05-12). "
        "Depends on the borrow setup which is blocked by the same wstETH funding "
        "gap. See test_borrow_weth_with_wsteth_collateral. Re-pointed to VIB-5971 2026-07-24.",
        strict=True,
    )
    async def test_repay_weth_full_after_borrow(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle_monad_local: dict[str, Decimal],
    ) -> None:
        """Repay full WETH debt with repay_full=True after borrowing against wstETH."""
        price_oracle = price_oracle_monad_local
        wsteth_address = MORPHO_MARKET_INFO["collateral_token_address"]
        weth_address = MORPHO_MARKET_INFO["loan_token_address"]
        weth_decimals = 18

        wsteth_price = (
            price_oracle.get("wstETH") or price_oracle.get("WETH") or Decimal("3500")
        )
        weth_price = price_oracle.get("WETH") or Decimal("3000")
        collateral_amount = Decimal("0.1")
        max_borrow_usd = collateral_amount * wsteth_price * Decimal("0.25")
        borrow_amount = max_borrow_usd / weth_price

        await _setup_borrow(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            anvil_rpc_url=anvil_rpc_url,
            collateral_amount=collateral_amount,
            borrow_amount=borrow_amount,
        )

        wsteth_before = get_token_balance(web3, wsteth_address, funded_wallet)
        weth_before = get_token_balance(web3, weth_address, funded_wallet)

        intent = RepayIntent(
            protocol="morpho_blue",
            token="WETH",
            amount=borrow_amount,
            repay_full=True,
            market_id=MORPHO_MARKET_ID,
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
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(
            compilation_result.action_bundle, execution_context
        )
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        events = _collect_morpho_events(execution_result)
        repay_event = _first_event(events, MorphoBlueEventType.REPAY)
        assert repay_event is not None, "Expected Repay event"
        repaid_assets_wei = _assets_wei(repay_event)
        assert repaid_assets_wei > 0

        wsteth_after = get_token_balance(web3, wsteth_address, funded_wallet)
        weth_after = get_token_balance(web3, weth_address, funded_wallet)
        weth_spent = weth_before - weth_after
        expected_weth_wei = int(borrow_amount * Decimal(10**weth_decimals))
        assert weth_spent >= expected_weth_wei
        assert weth_spent == repaid_assets_wei
        assert wsteth_after == wsteth_before, "Collateral must stay locked during repay"

        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.borrow_shares == 0
        assert position.collateral > 0


@pytest.mark.monad
@pytest.mark.withdraw
@pytest.mark.lending
class TestMorphoBlueWithdrawCollateralIntent:
    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-5971: wstETH not funded on Monad test wallet (as of 2026-05-12). "
        "Depends on the borrow+repay setup which is blocked by the same wstETH funding "
        "gap. See test_borrow_weth_with_wsteth_collateral. Re-pointed to VIB-5971 2026-07-24.",
        strict=True,
    )
    async def test_withdraw_wsteth_collateral_after_repay(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle_monad_local: dict[str, Decimal],
    ) -> None:
        """Withdraw wstETH collateral after a full borrow-repay cycle on Monad."""
        price_oracle = price_oracle_monad_local
        wsteth_address = MORPHO_MARKET_INFO["collateral_token_address"]
        wsteth_decimals = 18

        wsteth_price = (
            price_oracle.get("wstETH") or price_oracle.get("WETH") or Decimal("3500")
        )
        weth_price = price_oracle.get("WETH") or Decimal("3000")
        collateral_amount = Decimal("0.1")
        max_borrow_usd = collateral_amount * wsteth_price * Decimal("0.25")
        borrow_amount = max_borrow_usd / weth_price

        await _setup_borrow(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            anvil_rpc_url=anvil_rpc_url,
            collateral_amount=collateral_amount,
            borrow_amount=borrow_amount,
        )

        repay_intent = RepayIntent(
            protocol="morpho_blue",
            token="WETH",
            amount=borrow_amount,
            repay_full=True,
            market_id=MORPHO_MARKET_ID,
            chain=CHAIN_NAME,
        )
        repay_compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        repay_result = repay_compiler.compile(repay_intent)
        assert repay_result.status.value == "SUCCESS"
        assert repay_result.action_bundle is not None
        repay_exec = await orchestrator.execute(repay_result.action_bundle, execution_context)
        assert repay_exec.success

        wsteth_before = get_token_balance(web3, wsteth_address, funded_wallet)

        intent = WithdrawIntent(
            protocol="morpho_blue",
            token="wstETH",
            amount=collateral_amount,
            withdraw_all=False,
            market_id=MORPHO_MARKET_ID,
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(
            compilation_result.action_bundle, execution_context
        )
        assert execution_result.success

        events = _collect_morpho_events(execution_result)
        withdraw_event = _first_event(events, MorphoBlueEventType.WITHDRAW_COLLATERAL)
        assert withdraw_event is not None
        withdrawn_assets_wei = _assets_wei(withdraw_event)
        expected_collateral_wei = int(collateral_amount * Decimal(10**wsteth_decimals))
        assert withdrawn_assets_wei == expected_collateral_wei

        wsteth_after = get_token_balance(web3, wsteth_address, funded_wallet)
        wsteth_received = wsteth_after - wsteth_before
        assert wsteth_received == expected_collateral_wei

        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.collateral == 0
        assert position.borrow_shares == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
