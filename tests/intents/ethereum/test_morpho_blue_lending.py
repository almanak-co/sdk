"""Production-grade Morpho Blue lending intent tests for Ethereum.

Mirrors the coverage shape of Aave V3 intent tests:
- Exact token balance deltas
- Receipt parser integration
- On-chain position sanity checks
- Failure case with conservation

To run:
    uv run pytest tests/intents/ethereum/test_morpho_blue_lending.py -v -s
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
from almanak.framework.intents import BorrowIntent
from almanak.framework.intents.compiler import IntentCompiler
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

CHAIN_NAME = "ethereum"
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


@pytest.mark.ethereum
@pytest.mark.borrow
@pytest.mark.lending
class TestMorphoBlueBorrowIntent:
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

        print("\nALL CHECKS PASSED ✓")

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

