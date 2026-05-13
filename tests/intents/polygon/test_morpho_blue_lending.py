"""Production-grade Morpho Blue lending intent tests for Polygon.

Mirrors the coverage shape of the Ethereum/Base/Arbitrum Morpho Blue tests:
- Exact token balance deltas
- Receipt parser integration
- On-chain position sanity checks
- Failure case with conservation

Market: WBTC/USDC (0x1cfe584a...) — second-highest-TVL Polygon Morpho Blue
market (~$1.7M supply). Uses the chain-specific AdaptiveCurveIRM at
0xe675A2161D4a6E2de2eeD70ac98EEBf257FBF0B0.

Note: Polygon Morpho Blue is deployed at 0x1bF0c2541F820E775182832f06c0B7Fc27A25f67
(not the universal 0xBBBB...FFCb vanity address used on Ethereum/Base). Registry
populated 2026-04-17 after on-chain verification — same pattern as Arbitrum in
VIB-2969 where the vanity address has 0 bytes of code on this chain.

To run:
    uv run pytest tests/intents/polygon/test_morpho_blue_lending.py -v -s
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.morpho_blue.adapter import MORPHO_MARKETS
from almanak.framework.connectors.morpho_blue.receipt_parser import (
    MorphoBlueEvent,
    MorphoBlueEventType,
    MorphoBlueReceiptParser,
)
from almanak.framework.connectors.morpho_blue.sdk import MorphoBlueSDK
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator, ExecutionResult
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

CHAIN_NAME = "polygon"
MORPHO_MARKET_NAME = "WBTC/USDC"


def _select_market_id(chain: str, market_name: str) -> str:
    markets = MORPHO_MARKETS.get(chain, {})
    for market_id, info in markets.items():
        if info.get("name") == market_name:
            return market_id
    raise AssertionError(f"Expected Morpho market '{market_name}' to exist for chain='{chain}'")


MORPHO_MARKET_ID = _select_market_id(CHAIN_NAME, MORPHO_MARKET_NAME)
MORPHO_MARKET_INFO = MORPHO_MARKETS[CHAIN_NAME][MORPHO_MARKET_ID]


def _collect_morpho_events(execution_result: ExecutionResult) -> list[MorphoBlueEvent]:
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


@pytest.mark.polygon
@pytest.mark.borrow
@pytest.mark.lending
class TestMorphoBlueBorrowIntent:
    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_usdc_with_wbtc_collateral_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ) -> None:
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]

        wbtc_address = tokens["WBTC"]
        usdc_address = tokens["USDC"]
        assert wbtc_address.lower() == MORPHO_MARKET_INFO["collateral_token_address"].lower()
        assert usdc_address.lower() == MORPHO_MARKET_INFO["loan_token_address"].lower()

        wbtc_decimals = get_token_decimals(web3, wbtc_address)
        usdc_decimals = get_token_decimals(web3, usdc_address)

        # LTV headroom: 0.01 WBTC (~$600) collateral with 100 USDC borrow = ~17% LTV,
        # well below the 30% cap in .claude/rules/intent-tests.md §10. Prevents
        # flakiness if WBTC price swings during CI (session-scoped CoinGecko prices).
        collateral_amount = Decimal("0.01")
        borrow_amount = Decimal("100")

        print(f"\n{'='*80}")
        print(f"Test: Morpho Blue borrow {borrow_amount} USDC with {collateral_amount} WBTC collateral")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")

        wbtc_before = get_token_balance(web3, wbtc_address, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)

        print(f"WBTC before: {format_token_amount(wbtc_before, wbtc_decimals)}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        intent = BorrowIntent(
            protocol="morpho_blue",
            collateral_token="WBTC",
            collateral_amount=collateral_amount,
            borrow_token="USDC",
            borrow_amount=borrow_amount,
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
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"
        assert len(compilation_result.action_bundle.transactions) == 3, (
            "Expected 3 transactions: approve(WBTC) + supplyCollateral + borrow"
        )

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        events = _collect_morpho_events(execution_result)

        supply_collateral_event = _first_event(events, MorphoBlueEventType.SUPPLY_COLLATERAL)
        assert supply_collateral_event is not None, "Expected SupplyCollateral event in Morpho Blue receipts"
        assert supply_collateral_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()

        borrow_event = _first_event(events, MorphoBlueEventType.BORROW)
        assert borrow_event is not None, "Expected Borrow event in Morpho Blue receipts"
        assert borrow_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()

        expected_collateral_wei = int(collateral_amount * Decimal(10**wbtc_decimals))
        expected_borrow_wei = int(borrow_amount * Decimal(10**usdc_decimals))

        supplied_collateral_wei = _assets_wei(supply_collateral_event)
        borrowed_assets_wei = _assets_wei(borrow_event)

        assert supplied_collateral_wei == expected_collateral_wei, (
            "SupplyCollateral assets must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {supplied_collateral_wei}"
        )
        assert borrowed_assets_wei == expected_borrow_wei, (
            "Borrow assets must EXACTLY equal borrow amount. "
            f"Expected: {expected_borrow_wei}, Got: {borrowed_assets_wei}"
        )

        wbtc_after = get_token_balance(web3, wbtc_address, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)

        wbtc_spent = wbtc_before - wbtc_after
        usdc_received = usdc_after - usdc_before

        print("\n--- Results ---")
        print(f"WBTC spent:    {format_token_amount(wbtc_spent, wbtc_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, usdc_decimals)}")

        assert wbtc_spent == expected_collateral_wei, (
            "WBTC spent must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {wbtc_spent}"
        )
        assert usdc_received == expected_borrow_wei, (
            "USDC received must EXACTLY equal borrow amount. "
            f"Expected: {expected_borrow_wei}, Got: {usdc_received}"
        )

        assert wbtc_spent == supplied_collateral_wei, (
            "WBTC spent must EXACTLY equal SupplyCollateral event assets. "
            f"Expected: {supplied_collateral_wei}, Got: {wbtc_spent}"
        )
        assert usdc_received == borrowed_assets_wei, (
            "USDC received must EXACTLY equal Borrow event assets. "
            f"Expected: {borrowed_assets_wei}, Got: {usdc_received}"
        )

        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.collateral > 0, "Expected collateral to be present after borrow"
        assert position.borrow_shares > 0, "Expected debt (borrow_shares) to be present after borrow"

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
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_address = tokens["USDC"]
        wbtc_address = tokens["WBTC"]

        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)
        wbtc_before = get_token_balance(web3, wbtc_address, funded_wallet)

        intent = BorrowIntent(
            protocol="morpho_blue",
            collateral_token="WBTC",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("100"),
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

        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)
        wbtc_after = get_token_balance(web3, wbtc_address, funded_wallet)
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed borrow"
        assert wbtc_after == wbtc_before, "WBTC balance must be unchanged after failed borrow"


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
    """Helper: supply WBTC collateral and borrow USDC. Asserts success."""
    intent = BorrowIntent(
        protocol="morpho_blue",
        collateral_token="WBTC",
        collateral_amount=collateral_amount,
        borrow_token="USDC",
        borrow_amount=borrow_amount,
        market_id=MORPHO_MARKET_ID,
        chain=CHAIN_NAME,
    )
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    result = compiler.compile(intent)
    assert result.status.value == "SUCCESS", f"Borrow setup compile failed: {result.error}"
    assert result.action_bundle is not None, "Borrow setup missing action_bundle"
    exec_result = await orchestrator.execute(result.action_bundle, execution_context)
    assert exec_result.success, f"Borrow setup execution failed: {exec_result.error}"


@pytest.mark.polygon
@pytest.mark.repay
@pytest.mark.lending
class TestMorphoBlueRepayIntent:
    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_usdc_full_after_borrow(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Repay full USDC debt with repay_full=True after borrowing against WBTC."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wbtc_address = tokens["WBTC"]
        usdc_address = tokens["USDC"]

        wbtc_decimals = get_token_decimals(web3, wbtc_address)
        usdc_decimals = get_token_decimals(web3, usdc_address)

        # LTV headroom: 0.01 WBTC (~$600) collateral with 100 USDC borrow = ~17% LTV,
        # well below the 30% cap in .claude/rules/intent-tests.md §10.
        collateral_amount = Decimal("0.01")
        borrow_amount = Decimal("100")

        print(f"\n{'='*80}")
        print(f"Test: Morpho Blue repay_full=True after borrowing {borrow_amount} USDC")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")

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

        wbtc_before = get_token_balance(web3, wbtc_address, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)

        print(f"WBTC before repay: {format_token_amount(wbtc_before, wbtc_decimals)}")
        print(f"USDC before repay: {format_token_amount(usdc_before, usdc_decimals)}")

        intent = RepayIntent(
            protocol="morpho_blue",
            token="USDC",
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
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        parser = MorphoBlueReceiptParser()
        all_events: list[MorphoBlueEvent] = []
        for tx_result in execution_result.transaction_results:
            assert tx_result.receipt is not None, "Expected receipt for executed transaction"
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
            all_events.extend(parse_result.events)

        repay_event = _first_event(all_events, MorphoBlueEventType.REPAY)
        assert repay_event is not None, "Expected Repay event in Morpho Blue receipts"
        assert repay_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()

        repaid_assets_wei = _assets_wei(repay_event)
        assert repaid_assets_wei > 0, "Repay event must report positive assets repaid"

        wbtc_after = get_token_balance(web3, wbtc_address, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        wbtc_delta = abs(wbtc_before - wbtc_after)

        print("\n--- Results ---")
        print(f"USDC spent (repaid): {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"WBTC change:         {format_token_amount(wbtc_delta, wbtc_decimals)} (expect 0)")

        expected_usdc_wei = int(borrow_amount * Decimal(10**usdc_decimals))
        assert usdc_spent >= expected_usdc_wei, (
            "USDC spent must be at least the borrowed amount (includes tiny interest). "
            f"Expected >= {expected_usdc_wei}, Got: {usdc_spent}"
        )
        assert usdc_spent == repaid_assets_wei, (
            "USDC spent must EXACTLY equal Repay event assets. "
            f"Expected: {repaid_assets_wei}, Got: {usdc_spent}"
        )
        assert wbtc_delta == 0, (
            "WBTC balance must not change during repay (collateral stays locked). "
            f"Got WBTC delta: {wbtc_delta}"
        )

        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.borrow_shares == 0, (
            f"Expected borrow_shares=0 after repay_full=True, got {position.borrow_shares}"
        )
        assert position.collateral > 0, "Collateral must still be present after repay (not withdrawn yet)"

        print("\nALL CHECKS PASSED")


@pytest.mark.polygon
@pytest.mark.withdraw
@pytest.mark.lending
class TestMorphoBlueWithdrawCollateralIntent:
    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_wbtc_collateral_after_repay(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Withdraw WBTC collateral after a full borrow-repay cycle."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wbtc_address = tokens["WBTC"]

        wbtc_decimals = get_token_decimals(web3, wbtc_address)

        # LTV headroom: 0.01 WBTC (~$600) collateral with 100 USDC borrow = ~17% LTV.
        collateral_amount = Decimal("0.01")
        borrow_amount = Decimal("100")

        print(f"\n{'='*80}")
        print(f"Test: Morpho Blue withdraw {collateral_amount} WBTC collateral after borrow-repay")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")

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
            token="USDC",
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

        wbtc_before = get_token_balance(web3, wbtc_address, funded_wallet)
        print(f"WBTC before withdraw: {format_token_amount(wbtc_before, wbtc_decimals)}")

        intent = WithdrawIntent(
            protocol="morpho_blue",
            token="WBTC",
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

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        parser = MorphoBlueReceiptParser()
        all_events: list[MorphoBlueEvent] = []
        for tx_result in execution_result.transaction_results:
            assert tx_result.receipt is not None, "Expected receipt for executed transaction"
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
            all_events.extend(parse_result.events)

        withdraw_event = _first_event(all_events, MorphoBlueEventType.WITHDRAW_COLLATERAL)
        assert withdraw_event is not None, "Expected WithdrawCollateral event in Morpho Blue receipts"
        assert withdraw_event.data["market_id"].lower() == MORPHO_MARKET_ID.lower()

        withdrawn_assets_wei = _assets_wei(withdraw_event)
        expected_collateral_wei = int(collateral_amount * Decimal(10**wbtc_decimals))
        assert withdrawn_assets_wei == expected_collateral_wei, (
            "WithdrawCollateral event assets must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {withdrawn_assets_wei}"
        )

        wbtc_after = get_token_balance(web3, wbtc_address, funded_wallet)
        wbtc_received = wbtc_after - wbtc_before

        print("\n--- Results ---")
        print(f"WBTC received: {format_token_amount(wbtc_received, wbtc_decimals)}")

        assert wbtc_received == expected_collateral_wei, (
            "WBTC received must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {wbtc_received}"
        )
        assert wbtc_received == withdrawn_assets_wei, (
            "WBTC received must EXACTLY equal WithdrawCollateral event assets. "
            f"Expected: {withdrawn_assets_wei}, Got: {wbtc_received}"
        )

        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.collateral == 0, (
            f"Expected collateral=0 after withdrawal, got {position.collateral}"
        )
        assert position.borrow_shares == 0, (
            f"Expected borrow_shares=0 after full repay+withdraw, got {position.borrow_shares}"
        )

        print("\nALL CHECKS PASSED")


@pytest.mark.polygon
@pytest.mark.supply
@pytest.mark.lending
class TestMorphoBlueSupplyIntent:
    """SUPPLY USDC as loan token into the WBTC/USDC market (VIB-4307)."""

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_usdc_as_loan_token(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Supply USDC as loan token (not collateral) into WBTC/USDC market.

        Verifies the lending side of the market: an approve(USDC, MorphoBlue)
        + supply(market_params, assets, ...) pair that mints supply_shares
        without requiring any collateral.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_address = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc_address)
        assert usdc_address.lower() == MORPHO_MARKET_INFO["loan_token_address"].lower(), (
            "Expected market loan token to be USDC"
        )

        supply_amount = Decimal("1000")  # 1000 USDC

        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)
        expected_wei = int(supply_amount * Decimal(10**usdc_decimals))
        assert usdc_before >= expected_wei, (
            f"funded_wallet has only {usdc_before} USDC wei, need >= {expected_wei}"
        )

        print(f"\n{'='*80}")
        print(f"Morpho Blue SUPPLY: {supply_amount} USDC (loan token) on Polygon")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")
        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")

        # Layer 1: Compile
        intent = SupplyIntent(
            protocol="morpho_blue",
            token="USDC",
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

        # Layer 2: Execute
        execution_result = await orchestrator.execute(
            compilation_result.action_bundle, execution_context
        )
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parse — Supply (loan-token) event
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

        # Layer 4: Balance delta — exact USDC spent
        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        assert usdc_spent == expected_wei, (
            f"USDC spent must EXACTLY equal supply amount. "
            f"Expected: {expected_wei}, Got: {usdc_spent}"
        )
        assert usdc_spent == supplied_wei

        # On-chain sanity
        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.supply_shares > 0, (
            f"Expected supply_shares > 0 after loan-token supply, got {position.supply_shares}"
        )

        print(f"\nUSDC spent: {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"Supply shares: {position.supply_shares}")
        print("\nALL CHECKS PASSED")
