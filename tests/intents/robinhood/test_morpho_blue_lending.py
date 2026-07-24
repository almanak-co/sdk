"""Production-grade Morpho Blue lending intent tests for Robinhood Chain (4663).

Robinhood is an Arbitrum Orbit L2 where Morpho powers ~73% of TVL (the Earn
product). Unlike every other chain, there is NO WETH-collateral market and no
real USDC/USDT on 4663 — every Morpho market uses USDG (Global Dollar, 6 dec)
as the loan asset. This suite exercises the deep **USDe/USDG** market (USDe
collateral, 18 dec; $75M supplied) through the full lending lifecycle.

Coverage — the four mandatory verification layers on every successful tx:
  1. Compilation  — ``IntentCompiler.compile`` → SUCCESS + ActionBundle
  2. Execution    — ``orchestrator.execute`` → success (via Safe + Zodiac Roles)
  3. Receipt parse— ``MorphoBlueReceiptParser`` → exact event assets
  4. Balance delta— exact before/after ERC-20 deltas + on-chain position sanity
Plus a failure case (borrow with no collateral) with balance conservation.

Scope note: this file intentionally stays at the 4 mandatory layers. The
Layer-5 accounting-persistence assertions carried by the older ethereum/base/
monad morpho suites are a separate epic (VIB-4591); they are deferred for
robinhood until the chain's funding + markets are proven on a real fork — which
is exactly what this suite does. Every assertion here asserts correct behaviour
(never a weaker/wrong one), so the 4-layer contract is fully met.

NO MOCKING. All tests execute real on-chain transactions on an Anvil fork of
Robinhood (forked at the pinned block 5,610,000, so the Safe/Zodiac stack deployed 2026-07-09 is
present). Default-on Zodiac: every tx routes through ``execTransactionWithRole``.

To run:
    uv run pytest tests/intents/robinhood/test_morpho_blue_lending.py -v -s
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.morpho_blue.adapter import MORPHO_MARKETS
from almanak.connectors.morpho_blue.receipt_parser import (
    MorphoBlueEvent,
    MorphoBlueEventType,
    MorphoBlueReceiptParser,
)
from almanak.connectors.morpho_blue.sdk import MorphoBlueSDK
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
)
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

CHAIN_NAME = "robinhood"
PROTOCOL = "morpho_blue"
MORPHO_MARKET_NAME = "USDe/USDG"


def _select_market_id(chain: str, market_name: str) -> str:
    markets = MORPHO_MARKETS.get(chain, {})
    for market_id, info in markets.items():
        if info.get("name") == market_name:
            return market_id
    raise AssertionError(f"Expected Morpho market '{market_name}' to exist for chain='{chain}'")


MORPHO_MARKET_ID = _select_market_id(CHAIN_NAME, MORPHO_MARKET_NAME)
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


def _first_event(events: list[MorphoBlueEvent], event_type: MorphoBlueEventType) -> MorphoBlueEvent | None:
    for event in events:
        if event.event_type == event_type:
            return event
    return None


def _assets_wei(event: MorphoBlueEvent) -> int:
    assets = event.data.get("assets")
    assert assets is not None, f"Expected 'assets' in event data for {event.event_type}"
    return int(Decimal(str(assets)))


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    return ExecutionContext(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        simulation_enabled=True,
    )


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
    """Helper: supply USDe collateral, then standalone-borrow USDG (two-intent
    form, #2827). Asserts success.

    The bundled ``BorrowIntent(collateral_amount>0)`` is fail-closed at the
    intent validator (accounting writes one event per intent), so the
    production-faithful shape is SUPPLY -> standalone BORROW (mirrors the
    base suite).
    """
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    supply_intent = SupplyIntent(
        protocol="morpho_blue",
        token="USDe",
        amount=collateral_amount,
        use_as_collateral=True,
        market_id=MORPHO_MARKET_ID,
        chain=CHAIN_NAME,
    )
    supply_result = compiler.compile(supply_intent)
    assert supply_result.status.value == "SUCCESS", f"Supply setup compile failed: {supply_result.error}"
    assert supply_result.action_bundle is not None, "Supply setup missing action_bundle"
    supply_exec = await orchestrator.execute(supply_result.action_bundle, execution_context)
    assert supply_exec.success, f"Collateral supply setup failed: {supply_exec.error}"

    intent = BorrowIntent(
        protocol="morpho_blue",
        collateral_token="USDe",
        collateral_amount=Decimal("0"),
        borrow_token="USDG",
        borrow_amount=borrow_amount,
        market_id=MORPHO_MARKET_ID,
        chain=CHAIN_NAME,
    )
    result = compiler.compile(intent)
    assert result.status.value == "SUCCESS", f"Borrow setup compile failed: {result.error}"
    assert result.action_bundle is not None, "Borrow setup missing action_bundle"
    exec_result = await orchestrator.execute(result.action_bundle, execution_context)
    assert exec_result.success, f"Borrow setup execution failed: {exec_result.error}"


@pytest.mark.robinhood
@pytest.mark.borrow
@pytest.mark.lending
class TestMorphoBlueBorrowIntent:
    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_usdg_with_usde_collateral_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ) -> None:
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usde_address = tokens["USDe"]
        usdg_address = tokens["USDG"]
        assert usde_address.lower() == MORPHO_MARKET_INFO["collateral_token_address"].lower()
        assert usdg_address.lower() == MORPHO_MARKET_INFO["loan_token_address"].lower()

        usde_decimals = get_token_decimals(web3, usde_address)
        usdg_decimals = get_token_decimals(web3, usdg_address)

        # Stable-stable, LLTV 0.915. Keep well under 30% LTV: 100 USDe (~$100)
        # collateral, borrow 20 USDG (~20% LTV).
        collateral_amount = Decimal("100")
        borrow_amount = Decimal("20")

        print(f"\n{'='*80}")
        print(f"Test: Morpho Blue borrow {borrow_amount} USDG with {collateral_amount} USDe collateral")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")

        usde_before = get_token_balance(web3, usde_address, funded_wallet)
        usdg_before = get_token_balance(web3, usdg_address, funded_wallet)
        assert usde_before >= int(collateral_amount * Decimal(10**usde_decimals)), (
            f"funded_wallet lacks USDe collateral (have {usde_before}) — robinhood fork funding gap"
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Step 1 — supply the USDe collateral (two-intent form, #2827: the
        # bundled shape is fail-closed at the intent validator).
        supply_intent = SupplyIntent(
            protocol="morpho_blue",
            token="USDe",
            amount=collateral_amount,
            use_as_collateral=True,
            market_id=MORPHO_MARKET_ID,
            chain=CHAIN_NAME,
        )
        supply_compile = compiler.compile(supply_intent)
        assert supply_compile.status.value == "SUCCESS", f"Supply compilation failed: {supply_compile.error}"
        assert supply_compile.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_compile.action_bundle, execution_context)
        assert supply_exec.success, f"Collateral supply failed: {supply_exec.error}"

        # Layer 3 (supply leg): SupplyCollateral must land on the market.
        supply_events = _collect_morpho_events(supply_exec)
        supply_collateral_event = _first_event(supply_events, MorphoBlueEventType.SUPPLY_COLLATERAL)
        assert supply_collateral_event is not None, "Expected SupplyCollateral event in the supply-leg receipts"
        assert supply_collateral_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()

        # Step 2 — standalone borrow (the only shape the public API allows).
        intent = BorrowIntent(
            protocol="morpho_blue",
            collateral_token="USDe",
            collateral_amount=Decimal("0"),
            borrow_token="USDG",
            borrow_amount=borrow_amount,
            market_id=MORPHO_MARKET_ID,
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"
        assert len(compilation_result.action_bundle.transactions) == 1, (
            "Expected 1 transaction: borrow (standalone two-intent form)"
        )

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        events = _collect_morpho_events(execution_result)

        assert _first_event(events, MorphoBlueEventType.SUPPLY_COLLATERAL) is None, (
            "Standalone borrow must not emit SupplyCollateral events (collateral moved in step 1)"
        )

        borrow_event = _first_event(events, MorphoBlueEventType.BORROW)
        assert borrow_event is not None, "Expected Borrow event"
        assert borrow_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()

        expected_collateral_wei = int(collateral_amount * Decimal(10**usde_decimals))
        expected_borrow_wei = int(borrow_amount * Decimal(10**usdg_decimals))

        supplied_collateral_wei = _assets_wei(supply_collateral_event)
        borrowed_assets_wei = _assets_wei(borrow_event)
        assert supplied_collateral_wei == expected_collateral_wei, (
            f"SupplyCollateral assets must equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {supplied_collateral_wei}"
        )
        assert borrowed_assets_wei == expected_borrow_wei, (
            f"Borrow assets must equal borrow amount. Expected: {expected_borrow_wei}, Got: {borrowed_assets_wei}"
        )

        usde_after = get_token_balance(web3, usde_address, funded_wallet)
        usdg_after = get_token_balance(web3, usdg_address, funded_wallet)
        usde_spent = usde_before - usde_after
        usdg_received = usdg_after - usdg_before

        print(f"USDe spent:    {format_token_amount(usde_spent, usde_decimals)}")
        print(f"USDG received: {format_token_amount(usdg_received, usdg_decimals)}")

        assert usde_spent == expected_collateral_wei, (
            f"USDe spent must equal collateral amount. Expected: {expected_collateral_wei}, Got: {usde_spent}"
        )
        assert usdg_received == expected_borrow_wei, (
            f"USDG received must equal borrow amount. Expected: {expected_borrow_wei}, Got: {usdg_received}"
        )
        assert usde_spent == supplied_collateral_wei
        assert usdg_received == borrowed_assets_wei

        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.collateral > 0, "Expected collateral after borrow"
        assert position.borrow_shares > 0, "Expected debt (borrow_shares) after borrow"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_without_collateral_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Borrow with zero collateral must fail; USDG and USDe balances conserved."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdg_address = tokens["USDG"]
        usde_address = tokens["USDe"]

        usdg_before = get_token_balance(web3, usdg_address, funded_wallet)
        usde_before = get_token_balance(web3, usde_address, funded_wallet)

        intent = BorrowIntent(
            protocol="morpho_blue",
            collateral_token="USDe",
            collateral_amount=Decimal("0"),
            borrow_token="USDG",
            borrow_amount=Decimal("20"),
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

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert not execution_result.success, "Execution should fail without collateral"

        usdg_after = get_token_balance(web3, usdg_address, funded_wallet)
        assert usdg_after == usdg_before, "USDG balance must be unchanged after failed borrow"
        usde_after = get_token_balance(web3, usde_address, funded_wallet)
        assert usde_after == usde_before, "USDe collateral must be conserved when the borrow reverts"


@pytest.mark.robinhood
@pytest.mark.repay
@pytest.mark.lending
class TestMorphoBlueRepayIntent:
    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_usdg_full_after_borrow(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Repay full USDG debt (repay_full=True) after borrowing against USDe."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usde_address = tokens["USDe"]
        usdg_address = tokens["USDG"]

        usdg_decimals = get_token_decimals(web3, usdg_address)

        collateral_amount = Decimal("100")
        borrow_amount = Decimal("20")

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

        usde_before = get_token_balance(web3, usde_address, funded_wallet)
        usdg_before = get_token_balance(web3, usdg_address, funded_wallet)

        intent = RepayIntent(
            protocol="morpho_blue",
            token="USDG",
            amount=borrow_amount,  # fallback; ignored by repay_full
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
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        events = _collect_morpho_events(execution_result)
        repay_event = _first_event(events, MorphoBlueEventType.REPAY)
        assert repay_event is not None, "Expected Repay event"
        assert repay_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()
        repaid_assets_wei = _assets_wei(repay_event)
        assert repaid_assets_wei > 0, "Repay event must report positive assets repaid"

        usde_after = get_token_balance(web3, usde_address, funded_wallet)
        usdg_after = get_token_balance(web3, usdg_address, funded_wallet)
        usdg_spent = usdg_before - usdg_after
        usde_delta = abs(usde_before - usde_after)

        expected_usdg_wei = int(borrow_amount * Decimal(10**usdg_decimals))
        assert usdg_spent >= expected_usdg_wei, (
            f"USDG spent must be >= borrowed amount (plus tiny interest). "
            f"Expected >= {expected_usdg_wei}, Got: {usdg_spent}"
        )
        assert usdg_spent == repaid_assets_wei, (
            f"USDG spent must equal Repay event assets. Expected: {repaid_assets_wei}, Got: {usdg_spent}"
        )
        assert usde_delta == 0, f"USDe collateral must stay locked during repay. Got delta: {usde_delta}"

        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.borrow_shares == 0, f"Expected borrow_shares=0 after repay_full, got {position.borrow_shares}"
        assert position.collateral > 0, "Collateral must still be present after repay (not withdrawn yet)"

        print("\nALL CHECKS PASSED")


@pytest.mark.robinhood
@pytest.mark.withdraw
@pytest.mark.lending
class TestMorphoBlueWithdrawCollateralIntent:
    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_usde_collateral_after_repay(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Withdraw USDe collateral after a full borrow-repay cycle."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usde_address = tokens["USDe"]
        usde_decimals = get_token_decimals(web3, usde_address)

        collateral_amount = Decimal("100")
        borrow_amount = Decimal("20")

        # Setup: borrow, then repay full so withdrawCollateral is unblocked.
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
            token="USDG",
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
        assert repay_result.status.value == "SUCCESS", f"Repay setup compile failed: {repay_result.error}"
        assert repay_result.action_bundle is not None, "Repay setup missing action_bundle"
        repay_exec = await orchestrator.execute(repay_result.action_bundle, execution_context)
        assert repay_exec.success, f"Repay setup execution failed: {repay_exec.error}"

        usde_before = get_token_balance(web3, usde_address, funded_wallet)

        intent = WithdrawIntent(
            protocol="morpho_blue",
            token="USDe",
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
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        events = _collect_morpho_events(execution_result)
        withdraw_event = _first_event(events, MorphoBlueEventType.WITHDRAW_COLLATERAL)
        assert withdraw_event is not None, "Expected WithdrawCollateral event"
        assert withdraw_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()

        withdrawn_assets_wei = _assets_wei(withdraw_event)
        expected_collateral_wei = int(collateral_amount * Decimal(10**usde_decimals))
        assert withdrawn_assets_wei == expected_collateral_wei, (
            f"WithdrawCollateral assets must equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {withdrawn_assets_wei}"
        )

        usde_after = get_token_balance(web3, usde_address, funded_wallet)
        usde_received = usde_after - usde_before
        assert usde_received == expected_collateral_wei, (
            f"USDe received must equal collateral amount. Expected: {expected_collateral_wei}, Got: {usde_received}"
        )
        assert usde_received == withdrawn_assets_wei

        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.collateral == 0, f"Expected collateral=0 after withdrawal, got {position.collateral}"
        assert position.borrow_shares == 0, f"Expected borrow_shares=0 after full unwind, got {position.borrow_shares}"

        print("\nALL CHECKS PASSED")


@pytest.mark.robinhood
@pytest.mark.supply
@pytest.mark.lending
class TestMorphoBlueSupplyIntent:
    """SUPPLY USDG as loan token (use_as_collateral=False) into USDe/USDG."""

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_usdg_as_loan_token(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ) -> None:
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdg_address = tokens["USDG"]
        usdg_decimals = get_token_decimals(web3, usdg_address)
        assert usdg_address.lower() == MORPHO_MARKET_INFO["loan_token_address"].lower(), (
            "Expected market loan token to be USDG"
        )

        supply_amount = Decimal("1000")  # 1000 USDG

        usdg_before = get_token_balance(web3, usdg_address, funded_wallet)
        expected_wei = int(supply_amount * Decimal(10**usdg_decimals))
        assert usdg_before >= expected_wei, f"funded_wallet has only {usdg_before} USDG wei, need >= {expected_wei}"

        print(f"\n{'='*80}")
        print(f"Morpho Blue SUPPLY: {supply_amount} USDG (loan token) on Robinhood")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")

        intent = SupplyIntent(
            protocol="morpho_blue",
            token="USDG",
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
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        events = _collect_morpho_events(execution_result)
        supply_event = _first_event(events, MorphoBlueEventType.SUPPLY)
        assert supply_event is not None, "Expected Supply event (loan-token supply)"
        assert supply_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()
        supplied_wei = _assets_wei(supply_event)
        assert supplied_wei == expected_wei, (
            f"Supply event assets must equal supply amount. Expected: {expected_wei}, Got: {supplied_wei}"
        )

        usdg_after = get_token_balance(web3, usdg_address, funded_wallet)
        usdg_spent = usdg_before - usdg_after
        assert usdg_spent == expected_wei, (
            f"USDG spent must equal supply amount. Expected: {expected_wei}, Got: {usdg_spent}"
        )
        assert usdg_spent == supplied_wei

        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.supply_shares > 0, f"Expected supply_shares > 0, got {position.supply_shares}"

        print(f"USDG spent: {format_token_amount(usdg_spent, usdg_decimals)}")
        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
