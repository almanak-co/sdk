"""Curvance lending intent tests for Monad.

Covers the SUPPLY intent through all four verification layers per
``.claude/rules/intent-tests.md``:

    1. Compilation:     IntentCompiler.compile(intent) -> SUCCESS, ActionBundle present.
    2. Execution:       ExecutionOrchestrator.execute(bundle) -> success=True.
    3. Receipt parsing: CurvanceReceiptParser returns the Deposit event.
    4. Balance deltas:  get_token_balance() before/after matches the supplied amount.

**Anvil fork limitation (documented, not a connector bug)**

Curvance's ``MarketManager._canBorrow`` and ``_canRedeem`` paths call
``OracleManager.getPrice(token, isMint, getLower=true)`` with a CAUTION error
breakpoint. On an Anvil fork of Monad the Redstone / Chainlink adaptor feeds
look "stale" because the fork snapshots price-feed contracts at a single block
while ``block.timestamp`` continues to advance as test transactions are mined —
any drift past the adaptor's freshness window yields ``errorCode=1 (CAUTION)``
on read, and ``_canBorrow`` reverts with ``MarketManager__InsufficientCollateral()``.
Reproduced 2026-04-18 against `https://rpc.monad.xyz` at the tip block.

This is a **fork-state artefact, not a connector defect** — the same calldata
executes successfully against live mainnet (verified via the mainnet-test
evidence in the PR that introduced this connector). Supply does not consult
the oracle in a way that trips the CAUTION breakpoint, so only SUPPLY is
exercised on Anvil. BORROW / REPAY / WITHDRAW are validated by:

- ``cast`` manual calls against an Anvil fork (see PR description).
- Real on-chain execution on Monad mainnet (see PR evidence: $4 supply, $2
  borrow, repay, withdraw + a $10 loop).

Usage:
    uv run pytest tests/intents/monad/test_curvance_lending.py -v -s
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.curvance import (
    CURVANCE_MARKETS,
    CurvanceReceiptParser,
)
from almanak.framework.connectors.curvance.receipt_parser import (
    CurvanceEvent,
    CurvanceEventType,
)
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import SupplyIntent
from almanak.framework.intents.compiler import IntentCompiler
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

CHAIN_NAME = "monad"
CURVANCE_MARKET_NAME = "WMON-USDC"


def _select_market_id(chain: str, market_name: str) -> str:
    for info in CURVANCE_MARKETS.get(chain, {}).values():
        if info.name == market_name:
            return info.market_manager
    raise AssertionError(f"Expected Curvance market '{market_name}' on chain='{chain}'")


MARKET_ID = _select_market_id(CHAIN_NAME, CURVANCE_MARKET_NAME)
MARKET = CURVANCE_MARKETS[CHAIN_NAME][MARKET_ID.lower()]


def _collect_events(execution_result) -> list[CurvanceEvent]:
    parser = CurvanceReceiptParser()
    events: list[CurvanceEvent] = []
    for tx_result in execution_result.transaction_results:
        receipt = tx_result.receipt
        assert receipt is not None, "Expected receipt for executed transaction"
        parsed = parser.parse_receipt(receipt.to_dict())
        events.extend(parsed.events)
    return events


def _first_event(events: list[CurvanceEvent], event_type: CurvanceEventType) -> CurvanceEvent | None:
    return next((e for e in events if e.event_type == event_type), None)


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
@pytest.mark.lending
class TestCurvanceSupplyIntent:
    """4-layer verification of SUPPLY against the Curvance WMON->USDC market."""

    @pytest.mark.asyncio
    async def test_supply_wmon_as_collateral(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle_monad_local: dict[str, Decimal],
    ) -> None:
        price_oracle = price_oracle_monad_local
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wmon_address = tokens["WMON"]
        wmon_decimals = get_token_decimals(web3, wmon_address)

        collateral_amount = Decimal("1.0")
        wmon_before = get_token_balance(web3, wmon_address, funded_wallet)
        # Fail fast if the funded_wallet fixture did not actually fund WMON —
        # otherwise the test would later fail as a protocol/exec error and
        # disguise the real cause (broken Monad fork funding).
        expected_wei_min = int(collateral_amount * Decimal(10**wmon_decimals))
        assert wmon_before >= expected_wei_min, (
            f"funded_wallet has only {wmon_before} WMON wei, need >= {expected_wei_min} "
            f"({collateral_amount} WMON) — Monad fork funding regression"
        )

        print("\n" + "=" * 80)
        print(f"Curvance SUPPLY: {collateral_amount} WMON as collateral")
        print(f"Market: {MARKET.name}  MarketManager={MARKET.market_manager}")
        print(f"Collateral cToken: {MARKET.collateral_ctoken}")
        print("=" * 80)
        print(f"WMON before: {format_token_amount(wmon_before, wmon_decimals)}")

        # Layer 1: Compile
        intent = SupplyIntent(
            protocol="curvance",
            token="WMON",
            amount=collateral_amount,
            use_as_collateral=True,
            market_id=MARKET.market_manager,
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation = compiler.compile(intent)
        assert compilation.status.value == "SUCCESS", f"Compile failed: {compilation.error}"
        assert compilation.action_bundle is not None
        # Expect approve(WMON, cWMON) + depositAsCollateral
        assert len(compilation.action_bundle.transactions) == 2, (
            f"Expected approve + depositAsCollateral, got {len(compilation.action_bundle.transactions)}"
        )
        approve_tx, supply_tx = compilation.action_bundle.transactions
        assert approve_tx["to"].lower() == wmon_address.lower(), "approve tx must target WMON"
        assert supply_tx["to"].lower() == MARKET.collateral_ctoken.lower(), (
            "supply tx must target the collateral cToken"
        )
        assert supply_tx["data"].startswith("0x2f4a61d9"), (
            "supply tx must call depositAsCollateral(uint256,address) — selector 0x2f4a61d9"
        )

        # Layer 2: Execute
        execution_result = await orchestrator.execute(
            compilation.action_bundle, execution_context
        )
        assert execution_result.success, f"Execute failed: {execution_result.error}"

        # Layer 3: Receipt parse — Deposit event emitted by the collateral cToken
        events = _collect_events(execution_result)
        deposit_event = _first_event(events, CurvanceEventType.DEPOSIT)
        assert deposit_event is not None, "Missing Curvance Deposit event"
        assert deposit_event.contract.lower() == MARKET.collateral_ctoken.lower(), (
            f"Deposit event must originate from collateral cToken; got {deposit_event.contract}"
        )
        supplied_wei = int(deposit_event.data["assets"])
        expected_wei = int(collateral_amount * Decimal(10**wmon_decimals))
        assert supplied_wei == expected_wei, (
            f"Deposit event assets mismatch: {supplied_wei} vs expected {expected_wei}"
        )

        # Layer 4: Balance deltas — WMON spent exactly matches supply amount
        wmon_after = get_token_balance(web3, wmon_address, funded_wallet)
        wmon_spent = wmon_before - wmon_after

        print(f"WMON spent: {format_token_amount(wmon_spent, wmon_decimals)}")
        print(f"Deposit event assets: {supplied_wei}")

        assert wmon_spent == expected_wei, (
            f"WMON spent must equal supply amount. Got {wmon_spent} expected {expected_wei}"
        )
        # Event-layer and balance-layer must agree
        assert wmon_spent == supplied_wei

        print("\nALL SUPPLY CHECKS PASSED")
