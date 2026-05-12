"""Production-grade Morpho Blue lending intent tests for Base.

Mirrors the coverage shape of the Ethereum Morpho Blue tests:
- Exact token balance deltas
- Receipt parser integration
- On-chain position sanity checks
- Failure case with conservation

To run:
    uv run pytest tests/intents/base/test_morpho_blue_lending.py -v -s
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
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import BorrowIntent, RepayIntent, WithdrawIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

CHAIN_NAME = "base"
MORPHO_MARKET_NAME = "wstETH/USDC"


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


@pytest.mark.base
@pytest.mark.borrow
@pytest.mark.lending
class TestMorphoBlueBorrowIntent:
    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_usdc_with_wsteth_collateral_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ) -> None:
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]

        wsteth_address = tokens["wstETH"]
        usdc_address = tokens["USDC"]
        assert wsteth_address.lower() == MORPHO_MARKET_INFO["collateral_token_address"].lower()
        assert usdc_address.lower() == MORPHO_MARKET_INFO["loan_token_address"].lower()

        wsteth_decimals = get_token_decimals(web3, wsteth_address)
        usdc_decimals = get_token_decimals(web3, usdc_address)

        collateral_amount = Decimal("0.1")
        borrow_amount = Decimal("100")

        print(f"\n{'='*80}")
        print(f"Test: Morpho Blue borrow {borrow_amount} USDC with {collateral_amount} wstETH collateral")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")

        wsteth_before = get_token_balance(web3, wsteth_address, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)

        print(f"wstETH before: {format_token_amount(wsteth_before, wsteth_decimals)}")
        print(f"USDC before:   {format_token_amount(usdc_before, usdc_decimals)}")

        intent = BorrowIntent(
            protocol="morpho_blue",
            collateral_token="wstETH",
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
            "Expected 3 transactions: approve(wstETH) + supplyCollateral + borrow"
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

        expected_collateral_wei = int(collateral_amount * Decimal(10**wsteth_decimals))
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

        wsteth_after = get_token_balance(web3, wsteth_address, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)

        wsteth_spent = wsteth_before - wsteth_after
        usdc_received = usdc_after - usdc_before

        print("\n--- Results ---")
        print(f"wstETH spent:  {format_token_amount(wsteth_spent, wsteth_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, usdc_decimals)}")

        assert wsteth_spent == expected_collateral_wei, (
            "wstETH spent must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {wsteth_spent}"
        )
        assert usdc_received == expected_borrow_wei, (
            "USDC received must EXACTLY equal borrow amount. "
            f"Expected: {expected_borrow_wei}, Got: {usdc_received}"
        )

        assert wsteth_spent == supplied_collateral_wei, (
            "wstETH spent must EXACTLY equal SupplyCollateral event assets. "
            f"Expected: {supplied_collateral_wei}, Got: {wsteth_spent}"
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

        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)

        intent = BorrowIntent(
            protocol="morpho_blue",
            collateral_token="wstETH",
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
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed borrow"


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
    """Helper: supply wstETH collateral and borrow USDC. Asserts success."""
    intent = BorrowIntent(
        protocol="morpho_blue",
        collateral_token="wstETH",
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
    exec_result = await orchestrator.execute(result.action_bundle, execution_context)
    assert exec_result.success, f"Borrow setup execution failed: {exec_result.error}"


@pytest.mark.base
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
        """Repay full USDC debt with repay_full=True after borrowing against wstETH.

        Verifies the VIB-587 fix: repay_full=True correctly queries borrow_shares via
        Anvil fork RPC (not Alchemy mainnet), so all shares including residuals are repaid.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wsteth_address = tokens["wstETH"]
        usdc_address = tokens["USDC"]

        wsteth_decimals = get_token_decimals(web3, wsteth_address)
        usdc_decimals = get_token_decimals(web3, usdc_address)

        collateral_amount = Decimal("0.1")
        borrow_amount = Decimal("100")

        print(f"\n{'='*80}")
        print(f"Test: Morpho Blue repay_full=True after borrowing {borrow_amount} USDC")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")

        # Setup: borrow first
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

        # Record balances before repay
        wsteth_before = get_token_balance(web3, wsteth_address, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)

        print(f"wstETH before repay: {format_token_amount(wsteth_before, wsteth_decimals)}")
        print(f"USDC before repay:   {format_token_amount(usdc_before, usdc_decimals)}")

        # Layer 1: Compile RepayIntent with repay_full=True
        intent = RepayIntent(
            protocol="morpho_blue",
            token="USDC",
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

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parsing -- expect Repay event
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

        # Layer 4: Balance deltas -- USDC decreases, wstETH unchanged
        wsteth_after = get_token_balance(web3, wsteth_address, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        wsteth_delta = abs(wsteth_before - wsteth_after)

        print(f"\n--- Results ---")
        print(f"USDC spent (repaid):  {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"wstETH change:        {format_token_amount(wsteth_delta, wsteth_decimals)} (expect 0)")

        expected_usdc_wei = int(borrow_amount * Decimal(10**usdc_decimals))
        assert usdc_spent >= expected_usdc_wei, (
            "USDC spent must be at least the borrowed amount (includes tiny interest). "
            f"Expected >= {expected_usdc_wei}, Got: {usdc_spent}"
        )
        assert usdc_spent == repaid_assets_wei, (
            "USDC spent must EXACTLY equal Repay event assets. "
            f"Expected: {repaid_assets_wei}, Got: {usdc_spent}"
        )
        assert wsteth_delta == 0, (
            "wstETH balance must not change during repay (collateral stays locked). "
            f"Got wstETH delta: {wsteth_delta}"
        )

        # On-chain sanity: borrow_shares must be 0 after full repay
        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.borrow_shares == 0, (
            f"Expected borrow_shares=0 after repay_full=True, got {position.borrow_shares}"
        )
        assert position.collateral > 0, "Collateral must still be present after repay (not withdrawn yet)"

        print("\nALL CHECKS PASSED")


@pytest.mark.base
@pytest.mark.withdraw
@pytest.mark.lending
class TestMorphoBlueWithdrawCollateralIntent:
    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_wsteth_collateral_after_repay(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """Withdraw wstETH collateral after a full borrow-repay cycle.

        Verifies amount-based withdraw (withdraw_all=False) recovers exact collateral.
        This is the final step of the iter-146 lifecycle.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wsteth_address = tokens["wstETH"]

        wsteth_decimals = get_token_decimals(web3, wsteth_address)

        collateral_amount = Decimal("0.1")
        borrow_amount = Decimal("100")

        print(f"\n{'='*80}")
        print(f"Test: Morpho Blue withdraw {collateral_amount} wstETH collateral after borrow-repay")
        print(f"Market: {MORPHO_MARKET_NAME} ({MORPHO_MARKET_ID[:10]}...)")
        print(f"{'='*80}")

        # Setup step 1: borrow
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

        # Setup step 2: repay full debt so withdrawCollateral is unblocked
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
        repay_exec = await orchestrator.execute(repay_result.action_bundle, execution_context)
        assert repay_exec.success, f"Repay setup execution failed: {repay_exec.error}"

        # Record balances before withdraw
        wsteth_before = get_token_balance(web3, wsteth_address, funded_wallet)
        print(f"wstETH before withdraw: {format_token_amount(wsteth_before, wsteth_decimals)}")

        # Layer 1: Compile WithdrawIntent (amount-based, withdraw_all=False)
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

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parsing -- expect WithdrawCollateral event
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
        expected_collateral_wei = int(collateral_amount * Decimal(10**wsteth_decimals))
        assert withdrawn_assets_wei == expected_collateral_wei, (
            "WithdrawCollateral event assets must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {withdrawn_assets_wei}"
        )

        # Layer 4: Balance delta -- wstETH must return to wallet
        wsteth_after = get_token_balance(web3, wsteth_address, funded_wallet)
        wsteth_received = wsteth_after - wsteth_before

        print(f"\n--- Results ---")
        print(f"wstETH received: {format_token_amount(wsteth_received, wsteth_decimals)}")

        assert wsteth_received == expected_collateral_wei, (
            "wstETH received must EXACTLY equal collateral amount. "
            f"Expected: {expected_collateral_wei}, Got: {wsteth_received}"
        )
        assert wsteth_received == withdrawn_assets_wei, (
            "wstETH received must EXACTLY equal WithdrawCollateral event assets. "
            f"Expected: {withdrawn_assets_wei}, Got: {wsteth_received}"
        )

        # On-chain sanity: position should be empty after full unwind
        sdk = MorphoBlueSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        position = sdk.get_position(MORPHO_MARKET_ID, funded_wallet)
        assert position.collateral == 0, (
            f"Expected collateral=0 after withdrawal, got {position.collateral}"
        )
        assert position.borrow_shares == 0, (
            f"Expected borrow_shares=0 after full repay+withdraw, got {position.borrow_shares}"
        )

        print("\nALL CHECKS PASSED")
