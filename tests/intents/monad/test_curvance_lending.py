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

from almanak.connectors.curvance import (
    CURVANCE_MARKETS,
    CurvanceReceiptParser,
)
from almanak.connectors.curvance.receipt_parser import (
    CurvanceEvent,
    CurvanceEventType,
)
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
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

    @pytest.mark.intent(IntentType.SUPPLY)
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


# =============================================================================
# BORROW / REPAY / WITHDRAW — xfail on Anvil fork (VIB-4307)
# =============================================================================
#
# The same OracleManager CAUTION-breakpoint that prevents the SUPPLY test
# from exercising borrow paths (see module docstring) also blocks any
# attempted BORROW/REPAY/WITHDRAW intent test on Anvil. The exact failure
# surface is:
#
# - BORROW: ``_canBorrow`` reverts with ``MarketManager__InsufficientCollateral()``
#   because ``OracleManager.getPrice`` returns errorCode=1 (CAUTION) due to
#   adaptor freshness drift on the fork.
# - REPAY:  same path — the protocol must read the borrow position which
#   transitively consults the oracle on Monad.
# - WITHDRAW: ``_canRedeem`` consults the oracle to ensure the user is not
#   under-collateralised, hitting the same CAUTION breakpoint.
#
# The compile / receipt-parse / balance-delta scaffolding below is fully
# structural — these tests will pass cleanly the day the oracle-freshness
# workaround lands (e.g. ``anvil_setTime`` aligned to the fork block, or
# adaptor mocking at the gateway boundary). Until then ``strict=True``
# ensures we surface the fix immediately via xpass-as-CI-failure.


@pytest.mark.monad
@pytest.mark.borrow
@pytest.mark.lending
class TestCurvanceBorrowIntent:
    """4-layer verification of BORROW against the Curvance WMON-USDC market."""

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-4307: Curvance MarketManager._canBorrow reverts with "
        "InsufficientCollateral() on Monad Anvil forks because OracleManager.getPrice "
        "returns errorCode=1 (CAUTION) due to adaptor freshness drift (as of 2026-05-12). "
        "Same fork-state artefact documented for SUPPLY-only coverage in the module "
        "docstring; verified live on Monad mainnet. Unblock by aligning Anvil "
        "block.timestamp with the fork block via anvil_setTime, or by mocking the "
        "oracle at the gateway boundary.",
        strict=True,
    )
    async def test_borrow_usdc_with_wmon_collateral(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle_monad_local: dict[str, Decimal],
    ) -> None:
        """Borrow USDC against WMON collateral on Curvance WMON-USDC market."""
        price_oracle = price_oracle_monad_local
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wmon_address = tokens["WMON"]
        usdc_address = tokens["USDC"]
        wmon_decimals = get_token_decimals(web3, wmon_address)
        usdc_decimals = get_token_decimals(web3, usdc_address)

        # ~25% LTV using session-scoped oracle prices.
        wmon_price = price_oracle.get("WMON") or price_oracle.get("MON") or Decimal("2")
        usdc_price = price_oracle.get("USDC") or Decimal("1")
        collateral_amount = Decimal("1.0")  # 1 WMON
        max_borrow_usd = collateral_amount * wmon_price * Decimal("0.25")
        borrow_amount = max_borrow_usd / usdc_price

        wmon_before = get_token_balance(web3, wmon_address, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)
        expected_collateral_wei = int(collateral_amount * Decimal(10**wmon_decimals))
        assert wmon_before >= expected_collateral_wei, (
            f"funded_wallet has only {wmon_before} WMON wei, need >= {expected_collateral_wei}"
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Step 1 — supply the WMON collateral (two-intent form, #2827: the
        # bundled shape is fail-closed at the intent validator; supply-first is
        # also the more robust Curvance ordering — depositAsCollateral succeeds
        # in the post-deploy oracle window that reverts BORROW).
        supply_intent = SupplyIntent(
            protocol="curvance",
            token="WMON",
            amount=collateral_amount,
            use_as_collateral=True,
            market_id=MARKET.market_manager,
            chain=CHAIN_NAME,
        )
        supply_compile = compiler.compile(supply_intent)
        assert supply_compile.status.value == "SUCCESS", f"Supply compile failed: {supply_compile.error}"
        assert supply_compile.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_compile.action_bundle, execution_context)
        assert supply_exec.success, f"Collateral supply failed: {supply_exec.error}"

        # Layer 3 (supply leg): the collateral Deposit event must be present
        # with the exact collateral amount.
        supply_events = _collect_events(supply_exec)
        supply_deposit = _first_event(supply_events, CurvanceEventType.DEPOSIT)
        assert supply_deposit is not None, "Missing collateral Deposit event on the supply leg"
        assert int(supply_deposit.data["assets"]) == int(collateral_amount * Decimal(10**wmon_decimals))

        # Step 2 — standalone borrow (the only shape the public API allows).
        intent = BorrowIntent(
            protocol="curvance",
            collateral_token="WMON",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            market_id=MARKET.market_manager,
            chain=CHAIN_NAME,
        )
        compilation = compiler.compile(intent)
        assert compilation.status.value == "SUCCESS", f"Compile failed: {compilation.error}"
        assert compilation.action_bundle is not None

        execution_result = await orchestrator.execute(
            compilation.action_bundle, execution_context
        )
        assert execution_result.success, f"Execute failed: {execution_result.error}"

        events = _collect_events(execution_result)
        borrow_event = _first_event(events, CurvanceEventType.BORROW)
        assert borrow_event is not None, "Missing Curvance Borrow event"
        assert _first_event(events, CurvanceEventType.DEPOSIT) is None, (
            "Standalone borrow must not emit a collateral Deposit event"
        )
        borrowed_assets_wei = int(borrow_event.data["assets"])
        expected_borrow_wei = int(borrow_amount * Decimal(10**usdc_decimals))
        assert borrowed_assets_wei == expected_borrow_wei

        wmon_after = get_token_balance(web3, wmon_address, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)
        assert wmon_before - wmon_after == expected_collateral_wei
        assert usdc_after - usdc_before == expected_borrow_wei


@pytest.mark.monad
@pytest.mark.repay
@pytest.mark.lending
class TestCurvanceRepayIntent:
    """4-layer verification of REPAY against the Curvance WMON-USDC market."""

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-4307: REPAY depends on a successful BORROW which is blocked by "
        "the OracleManager CAUTION-breakpoint on Monad Anvil forks (as of 2026-05-12). "
        "See test_borrow_usdc_with_wmon_collateral for the full rationale; same "
        "fork-state artefact, same unblock path.",
        strict=True,
    )
    async def test_repay_usdc_after_borrow(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle_monad_local: dict[str, Decimal],
    ) -> None:
        """Repay USDC debt with RepayIntent after borrow setup."""
        price_oracle = price_oracle_monad_local
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_address = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc_address)

        wmon_price = price_oracle.get("WMON") or price_oracle.get("MON") or Decimal("2")
        usdc_price = price_oracle.get("USDC") or Decimal("1")
        collateral_amount = Decimal("1.0")
        max_borrow_usd = collateral_amount * wmon_price * Decimal("0.25")
        borrow_amount = max_borrow_usd / usdc_price

        # Setup: supply, then a standalone borrow (two-intent form, #2827).
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        setup_supply_intent = SupplyIntent(
            protocol="curvance",
            token="WMON",
            amount=collateral_amount,
            use_as_collateral=True,
            market_id=MARKET.market_manager,
            chain=CHAIN_NAME,
        )
        setup_supply_result = compiler.compile(setup_supply_intent)
        assert setup_supply_result.status.value == "SUCCESS"
        assert setup_supply_result.action_bundle is not None
        setup_supply_exec = await orchestrator.execute(setup_supply_result.action_bundle, execution_context)
        assert setup_supply_exec.success, f"Collateral supply setup failed: {setup_supply_exec.error}"

        borrow_intent = BorrowIntent(
            protocol="curvance",
            collateral_token="WMON",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            market_id=MARKET.market_manager,
            chain=CHAIN_NAME,
        )
        borrow_compile = compiler.compile(borrow_intent)
        assert borrow_compile.status.value == "SUCCESS"
        assert borrow_compile.action_bundle is not None
        borrow_exec = await orchestrator.execute(borrow_compile.action_bundle, execution_context)
        assert borrow_exec.success, f"Borrow setup failed: {borrow_exec.error}"

        usdc_before = get_token_balance(web3, usdc_address, funded_wallet)
        repay_amount = borrow_amount / Decimal("2")

        intent = RepayIntent(
            protocol="curvance",
            token="USDC",
            amount=repay_amount,
            market_id=MARKET.market_manager,
            chain=CHAIN_NAME,
        )
        repay_compile = compiler.compile(intent)
        assert repay_compile.status.value == "SUCCESS", (
            f"Compile failed: {repay_compile.error}"
        )
        assert repay_compile.action_bundle is not None

        repay_exec = await orchestrator.execute(repay_compile.action_bundle, execution_context)
        assert repay_exec.success, f"Execute failed: {repay_exec.error}"

        events = _collect_events(repay_exec)
        repay_event = _first_event(events, CurvanceEventType.REPAY)
        assert repay_event is not None, "Missing Curvance Repay event"
        repaid_wei = int(repay_event.data["assets"])
        assert repaid_wei > 0

        usdc_after = get_token_balance(web3, usdc_address, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        expected_repay_wei = int(repay_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_repay_wei
        assert usdc_spent == repaid_wei


@pytest.mark.monad
@pytest.mark.withdraw
@pytest.mark.lending
class TestCurvanceWithdrawIntent:
    """4-layer verification of WITHDRAW against the Curvance WMON-USDC market."""

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason="VIB-4307: Curvance MarketManager._canRedeem reverts on Monad Anvil "
        "forks because OracleManager.getPrice returns errorCode=1 (CAUTION) due to "
        "adaptor freshness drift (as of 2026-05-12). Same fork-state artefact "
        "documented for BORROW. Unblock by aligning Anvil block.timestamp with the "
        "fork block via anvil_setTime, or by mocking the oracle at the gateway "
        "boundary.",
        strict=True,
    )
    async def test_withdraw_wmon_after_supply(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle_monad_local: dict[str, Decimal],
    ) -> None:
        """Withdraw a portion of WMON collateral via WithdrawIntent after supply."""
        price_oracle = price_oracle_monad_local
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        wmon_address = tokens["WMON"]
        wmon_decimals = get_token_decimals(web3, wmon_address)

        collateral_amount = Decimal("1.0")
        withdraw_amount = Decimal("0.5")

        # Setup: supply WMON as collateral.
        supply_intent = SupplyIntent(
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
        supply_compile = compiler.compile(supply_intent)
        assert supply_compile.status.value == "SUCCESS"
        assert supply_compile.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_compile.action_bundle, execution_context)
        assert supply_exec.success, f"Supply setup failed: {supply_exec.error}"

        wmon_before = get_token_balance(web3, wmon_address, funded_wallet)

        intent = WithdrawIntent(
            protocol="curvance",
            token="WMON",
            amount=withdraw_amount,
            market_id=MARKET.market_manager,
            chain=CHAIN_NAME,
        )
        withdraw_compile = compiler.compile(intent)
        assert withdraw_compile.status.value == "SUCCESS", (
            f"Compile failed: {withdraw_compile.error}"
        )
        assert withdraw_compile.action_bundle is not None

        withdraw_exec = await orchestrator.execute(withdraw_compile.action_bundle, execution_context)
        assert withdraw_exec.success, f"Execute failed: {withdraw_exec.error}"

        events = _collect_events(withdraw_exec)
        withdraw_event = _first_event(events, CurvanceEventType.WITHDRAW)
        assert withdraw_event is not None, "Missing Curvance Withdraw event"
        withdrawn_wei = int(withdraw_event.data["assets"])
        expected_withdraw_wei = int(withdraw_amount * Decimal(10**wmon_decimals))
        assert withdrawn_wei == expected_withdraw_wei

        wmon_after = get_token_balance(web3, wmon_address, funded_wallet)
        wmon_received = wmon_after - wmon_before
        assert wmon_received == expected_withdraw_wei
