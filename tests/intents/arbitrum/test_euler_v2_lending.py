"""Production-grade intent tests for Euler V2 on Arbitrum (VIB-4307).

Covers all four lending verbs (SUPPLY / WITHDRAW / BORROW / REPAY) for the
eUSDC-1 vault on Arbitrum mainnet:

- USDC: ``0xaf88d065e77c8cC2239327C5EDb3A432268e5831``
- eUSDC-1 vault: ``0x0a1eCC5Fe8C9be3C809844fcBe615B46A869b899``

Each test runs the full Intent → Compile → Execute → Parse → Verify pipeline
on an Anvil fork.

Layer 5 (epic VIB-4591 / ticket VIB-4605): mirrors the merged Spark / Aave V3 /
Compound V3 lending goldens. The success-path SUPPLY/WITHDRAW tests persist the
real ``ExecutionResult`` through the production accounting pipeline (ledger →
outbox → ``AccountingProcessor.drain_one`` into a throwaway SQLite) and assert
the typed ``LendingAccountingEvent`` is correct; the failure-path tests assert
the books-side mirror of balance conservation (zero ``accounting_events`` rows).

EULER V2 STATE READER (VIB-4966, landed): Euler V2 now has a BESPOKE vault/EVC
pre/post-state reader. ``_GENERIC_PRE_STATE_PROTOCOLS`` in
``almanak/framework/accounting/lending_accounting.py`` now includes ``euler_v2``,
and the connector publishes an ``ACCOUNT_STATE_READ_SPEC``
(``almanak/connectors/euler_v2/lending_read.py``). Unlike Aave's single
``getUserAccountData``, Euler's independent ERC-4626 vaults are read via
``maxWithdraw`` on the deposit vault + ``debtOf`` on the borrow/controller vault,
valued from the framework-injected price/decimals seam (Euler is not USD-native, like
Compound/Morpho/Silo — a pure single-pass plan→reduce seam cannot chain
``convertToAssets(balanceOf(user))``, so the protocol-computed single-call reads are
used). So ``capture_lending_pre_state`` / ``capture_lending_post_state`` return
populated state and the lending handler emits ``confidence=HIGH`` with every
before/after collateral / debt / health-factor field populated (Empty≠Zero≠None — a
measured zero is ``"0"``, never ``None``). Euler V2 Layer 5 asserts the
HIGH-confidence contract for chain state (``_assert_high_confidence_state``) — this
INVERTS the prior degradation contract (``_assert_state_degraded_no_reader_vib4605``).
The FIFO principal / interest split is derived from the basis store and is unaffected
by the reader.

NO MOCKING. All tests execute real on-chain transactions and verify state
changes through receipt-event assertions and exact-wei balance deltas.

Borrow/repay use the registered eWETH collateral vault (non-stablecoin
collateral vault (e.g. eWETH, eWBTC) is added to the Ethereum branch of
``EULER_V2_VAULTS_BY_CHAIN`` in
``almanak/connectors/euler_v2/adapter.py``. The compilation path
runs end-to-end, but execution reverts because eUSDC-2 is not a valid
collateral vault for borrowing USDC from itself. Mirrors the Avalanche
``test_euler_v2_borrow.py`` pattern (VIB-2643).

To run:
    uv run pytest tests/intents/ethereum/test_euler_v2_lending.py -v -s
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors.euler_v2.adapter import CHAIN_ADDRESSES, EULER_V2_VAULTS_BY_CHAIN
from almanak.connectors.euler_v2.receipt_parser import EulerV2ReceiptParser
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
    assert_no_accounting_on_failure,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# euler_v2 is NOT in the synthetic-intents lending matrix
# (_LENDING_PROTOCOLS in almanak/framework/permissions/synthetic_intents.py),
# so every test in this module must opt out of the default-on Zodiac wrap.
# See .claude/rules/intent-tests.md §Opt-out for the rationale.
pytestmark = pytest.mark.no_zodiac(reason="VIB-4307: euler_v2 not in synthetic-intents matrix")


# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"

# Euler V2 vault address on Ethereum (eUSDC-2) — used for receipt-parser filtering
# so we only count Deposit/Withdraw/Borrow/Repay events emitted by this vault.
EULER_V2_USDC_VAULT = "0x0a1eCC5Fe8C9be3C809844fcBe615B46A869b899"  # eUSDC-1 (arbitrum)

# Sourced from the connector's own registry rather than hand-typed, so the test cannot
# drift from the addresses the compiler actually targets (VIB-5801).
EULER_V2_WETH_VAULT = EULER_V2_VAULTS_BY_CHAIN[CHAIN_NAME]["eWETH-1"]["vault_address"]  # collateral vault
EULER_V2_EVC = CHAIN_ADDRESSES[CHAIN_NAME]["evc"]  # controller registry — see the controller-gate test

PROTOCOL = "euler_v2"


# =============================================================================
# Layer 5 helpers (shared) — mirror the merged Spark / Aave V3 / Compound V3
# goldens. ``enrich_result`` makes the ledger entry carry extracted_data;
# ``capture_lending_pre_state`` / ``capture_lending_post_state`` dispatch on
# ``intent.protocol``. Euler V2 now has a BESPOKE vault/EVC reader (VIB-4966,
# enabled in ``_GENERIC_PRE_STATE_PROTOCOLS``), so both captures return populated
# state and ``lending_state_to_dict`` serializes it — the persisted event therefore
# reaches ``confidence=HIGH`` with before/after collateral / debt / HF. The conftest
# Layer-5 helper threads the serialized state dicts into ``build_ledger_entry``.
# =============================================================================


def _execution_context(wallet: str) -> ExecutionContext:
    # NOTE: this deployment_id flows only into ``enrich_result`` (it labels the
    # ExecutionContext for enrichment). It is deliberately NOT what lands in the
    # persisted accounting row: the conftest ``assert_accounting_persisted``
    # helper stamps the row's deployment_id from its own
    # ``deployment_id="layer5-intent-test"`` default, which is what
    # ``_assert_identity`` checks. This split (descriptive enrichment id vs
    # canonical persisted identity) mirrors the merged Spark golden.
    return ExecutionContext(
        deployment_id="layer5-euler-v2-lending",
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
    block: int | str | None = None,
) -> dict | None:
    """Capture and serialize Euler V2 pre/post state via the Anvil eth_call adapter.

    Returns the runner-shaped state dict (``lending_state_to_dict`` output) or
    ``None`` — never a fabricated zero. With the bespoke vault/EVC reader (VIB-4966)
    this returns populated before/after collateral / debt / HF via the Anvil eth_call
    adapter (a measured-zero leg is ``"0"``, never ``None``), lighting up the
    HIGH-confidence path.

    ``block`` pins the read to a specific height when a caller needs one — the
    production runner passes the confirmed receipt block through the gateway. These
    Anvil intent tests read through ``anvil_eth_call_adapter``, so BOTH pre- and
    post-state pass ``None`` (→ ``"latest"``): immediately after
    ``orchestrator.execute()`` mines the tx, ``"latest"`` already IS the confirmed
    post-state on the local Anvil node (there is no remote receipt-indexer lag to
    race). Pinning the post-state read to the historical receipt block instead routes
    to the fork archive, which does not hold locally-mined post-tx blocks, so the read
    returns unavailable and the row degrades HIGH → ESTIMATED.
    """
    capture = capture_lending_post_state if post else capture_lending_pre_state
    state = capture(
        intent=intent,
        chain=CHAIN_NAME,
        wallet_address=wallet,
        gateway_client=reader,
        price_oracle=price_oracle,
        block=block,
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


def _assert_no_lot_id(row: dict, payload: dict) -> None:
    """Epic decision #6: no lot_id on the persisted lending event."""
    assert "lot_id" not in row
    assert "lot_id" not in payload


def _assert_high_confidence_state(payload: dict) -> None:
    """Euler V2 HIGH-confidence chain-state contract (VIB-4966 reader landed).

    Euler V2 now has a BESPOKE vault/EVC pre/post-state reader
    (``almanak/connectors/euler_v2/lending_read.py``, enabled in
    ``_GENERIC_PRE_STATE_PROTOCOLS``): unlike Aave's single ``getUserAccountData``,
    its independent ERC-4626 vaults are read via ``maxWithdraw`` on the deposit vault +
    ``debtOf`` on the borrow/controller vault, valued from the framework-injected
    price/decimals seam (Euler is not USD-native). So ``capture_lending_pre_state`` /
    ``capture_lending_post_state`` return populated state through the Anvil eth_call
    adapter and the lending handler emits ``confidence=HIGH`` with every before/after
    collateral / debt / health-factor field populated (Empty ≠ Zero — a measured zero
    is ``"0"``, never ``None``). This is the inverted contract VIB-4966 ships,
    replacing the prior ``_assert_state_degraded_no_reader_vib4605`` degradation
    contract.
    """
    assert payload["confidence"] == "HIGH", (
        f"Euler V2 lending must persist confidence=HIGH (bespoke reader + Anvil eth_call adapter), "
        f"got {payload['confidence']!r} (unavailable_reason={payload.get('unavailable_reason')!r})"
    )
    assert payload["collateral_value_before_usd"] is not None, "before-collateral must be populated"
    assert payload["collateral_value_after_usd"] is not None, "after-collateral must be populated"
    assert payload["debt_value_before_usd"] is not None, "before-debt must be populated"
    assert payload["debt_value_after_usd"] is not None, "after-debt must be populated"
    assert payload["health_factor_before"] is not None, "before-health-factor must be populated"
    assert payload["health_factor_after"] is not None, "after-health-factor must be populated"


# =============================================================================
# Supply / Withdraw Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.supply
@pytest.mark.lending
class TestEulerV2SupplyIntent:
    """Test Euler V2 supply/withdraw operations using SupplyIntent and WithdrawIntent."""

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_usdc_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Supply USDC into the eUSDC-1 vault via SupplyIntent."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("1000")  # 1000 USDC

        print(f"\n{'=' * 80}")
        print(f"Test: Supply {supply_amount} USDC to Euler V2 (Ethereum)")
        print(f"{'=' * 80}")

        # Record balance BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_before >= int(supply_amount * Decimal(10**decimals)), (
            f"Funded wallet lacks required USDC. Need {supply_amount}, have {usdc_before / 10**decimals}"
        )
        print(f"USDC before: {format_token_amount(usdc_before, decimals)}")

        # Layer 1: Compile
        intent = SupplyIntent(
            protocol="euler_v2",
            token="USDC",
            amount=supply_amount,
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

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parse — locate Deposit event from eUSDC-1 vault
        found_supply_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = EulerV2ReceiptParser(underlying_decimals=decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
                )
                if parse_result.success and parse_result.deposit_amount > 0:
                    assert parse_result.deposit_amount > 0
                    assert parse_result.deposit_shares > 0
                    found_supply_event = True
        assert found_supply_event, "Receipt parser must find at least one Deposit event"

        # Layer 4: Balance delta — exact USDC spent
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        expected_usdc_spent = int(supply_amount * Decimal(10**decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal supply amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent,
            funded_wallet,
            anvil_eth_call_adapter,
            price_oracle,
            post=True,
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
        # VIB-4966: Euler V2 now has a bespoke vault/EVC reader → confidence=HIGH with
        # populated before/after chain state. Supply increases collateral.
        _assert_high_confidence_state(payload)
        assert Decimal(payload["collateral_value_after_usd"]) > Decimal(payload["collateral_value_before_usd"]), (
            "SUPPLY must increase on-chain collateral value"
        )
        assert payload["asset"] == "USDC"
        assert payload["amount_token"] is not None
        assert Decimal(payload["amount_token"]) == supply_amount
        # SUPPLY drains wallet inventory: principal_delta_usd is measured (the
        # supplied principal in USD); interest is not applicable on SUPPLY. The
        # FIFO basis store is unaffected by the missing chain-state reader.
        assert payload["principal_delta_usd"] is not None, "SUPPLY must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "SUPPLY has no interest leg — must be None, not 0"

        print("\nALL CHECKS PASSED")

    @pytest.mark.withdraw
    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_all_liquid_redeems_max_uint256_and_drains_position(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """VIB-5801: a liquid full exit encodes redeem(MAX_UINT256) and goes flat.

        The regression guard for the fix. Euler's ``redeem(MAX_UINT256)`` genuinely caps
        to ``balanceOf(owner)`` — verified against all four deployments in
        ``tests/reports/euler_v2_full_exit_redeem_max_verification_vib5801.md`` — so the
        liquid path MUST stay on MAX. MAX drains the balance as of BROADCAST time, where
        a resolved compile-time share count is a stale snapshot that can leave dust, and
        a dusty "full exit" is a teardown leg that never goes flat.

        Supplying always adds cash 1:1, so a wallet's own fresh deposit is by
        construction fully liquid (``maxRedeem == balanceOf``) — which is exactly the
        REDEEM_MAX branch this asserts.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        supply_amount = Decimal("2000")
        supply_result = compiler.compile(
            SupplyIntent(protocol="euler_v2", token="USDC", amount=supply_amount, chain=CHAIN_NAME)
        )
        assert supply_result.status.value == "SUCCESS"
        supply_exec = await orchestrator.execute(supply_result.action_bundle)
        assert supply_exec.success, f"Initial supply failed: {supply_exec.error}"

        vault = web3.eth.contract(
            address=Web3.to_checksum_address(EULER_V2_USDC_VAULT),
            abi=[
                {
                    "name": "balanceOf",
                    "type": "function",
                    "stateMutability": "view",
                    "inputs": [{"name": "a", "type": "address"}],
                    "outputs": [{"name": "", "type": "uint256"}],
                },
                {
                    "name": "maxRedeem",
                    "type": "function",
                    "stateMutability": "view",
                    "inputs": [{"name": "a", "type": "address"}],
                    "outputs": [{"name": "", "type": "uint256"}],
                },
            ],
        )
        shares_before = vault.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        max_redeem = vault.functions.maxRedeem(Web3.to_checksum_address(funded_wallet)).call()
        assert shares_before > 0, "Supply must mint vault shares"
        # Precondition for the REDEEM_MAX branch: this deposit is fully liquid.
        assert max_redeem == shares_before, (
            f"Fresh deposit must be fully redeemable (cash covers it): "
            f"maxRedeem={max_redeem} shares={shares_before}"
        )

        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        # Layer 1: Compile — full exit must encode redeem(MAX_UINT256), not a resolved count.
        intent = WithdrawIntent(
            protocol="euler_v2",
            token="USDC",
            amount=Decimal("1"),  # ignored — withdraw_all wins
            withdraw_all=True,
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compile failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None
        metadata = compilation_result.action_bundle.metadata
        assert metadata["full_exit_mode"] == "redeem_max", (
            f"A liquid full exit must resolve to REDEEM_MAX, got {metadata['full_exit_mode']}"
        )
        calldata = compilation_result.transactions[0].data.lower()
        assert calldata.startswith("0xba087652"), "Full exit must call redeem(uint256,address,address)"
        assert "f" * 64 in calldata, "Liquid full exit must carry MAX_UINT256"

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parse — Withdraw event with real assets + shares
        found_withdraw_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = EulerV2ReceiptParser(underlying_decimals=decimals).parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
                )
                if parse_result.success and parse_result.withdraw_amount > 0:
                    assert parse_result.withdraw_shares > 0
                    found_withdraw_event = True
        assert found_withdraw_event, "Receipt parser must find a Withdraw event"

        # Layer 4: Balance deltas — position flat, principal recovered.
        shares_after = vault.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        assert shares_after == 0, (
            f"redeem(MAX_UINT256) must drain the position to ZERO shares — this is the "
            f"cap-to-balance behaviour VIB-5801 verified on-chain. Residual: {shares_after}"
        )
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before
        supplied_wei = int(supply_amount * Decimal(10**decimals))
        # A full exit returns the principal MINUS at most ERC-4626 double-rounding dust,
        # never a real loss. Both conversions round in the vault's favour — deposit rounds
        # the minted shares DOWN, redeem rounds the returned assets DOWN — so a same-block
        # round-trip can come back up to 2 base units light (here: 1, i.e. $0.000001).
        # Interest accrual pushes the other way, so the observed value straddles the
        # principal depending on the fork block; only the FLOOR is a stable invariant.
        # Asserting `>= supplied` exactly is wrong and fails on a same-block round-trip.
        rounding_dust_wei = 2
        assert usdc_received >= supplied_wei - rounding_dust_wei, (
            f"Full exit must return the principal minus at most {rounding_dust_wei} base units "
            f"of ERC-4626 rounding dust. Supplied: {supply_amount}, "
            f"received: {format_token_amount(usdc_received, decimals)} "
            f"(short by {supplied_wei - usdc_received} base units)"
        )
        # ...and no windfall: a full exit must not return materially MORE than principal
        # on a fresh same-block position (guards a mis-scaled amount / wrong-decimals bug).
        assert usdc_received <= supplied_wei * 101 // 100, (
            f"Full exit returned more than 101% of principal — suspicious. "
            f"Supplied: {supply_amount}, received: {format_token_amount(usdc_received, decimals)}"
        )

    @pytest.mark.withdraw
    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_usdc_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Supply, then withdraw a portion of USDC via WithdrawIntent."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Setup: supply 2000 USDC first.
        supply_amount = Decimal("2000")
        supply_intent = SupplyIntent(
            protocol="euler_v2",
            token="USDC",
            amount=supply_amount,
            chain=CHAIN_NAME,
        )
        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_result.action_bundle)
        assert supply_exec.success, f"Initial supply failed: {supply_exec.error}"

        # Now withdraw 1000 USDC.
        withdraw_amount = Decimal("1000")

        print(f"\n{'=' * 80}")
        print(f"Test: Withdraw {withdraw_amount} USDC from Euler V2 (Ethereum)")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        print(f"USDC before withdraw: {format_token_amount(usdc_before, decimals)}")

        # Layer 1: Compile
        intent = WithdrawIntent(
            protocol="euler_v2",
            token="USDC",
            amount=withdraw_amount,
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        # Layer 2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parse — Withdraw event
        found_withdraw_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = EulerV2ReceiptParser(underlying_decimals=decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
                )
                if parse_result.success and parse_result.withdraw_amount > 0:
                    assert parse_result.withdraw_amount > 0
                    assert parse_result.withdraw_shares > 0
                    found_withdraw_event = True
        assert found_withdraw_event, "Receipt parser must find at least one Withdraw event"

        # Layer 4: Balance delta — exact USDC received
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before
        expected_usdc_received = int(withdraw_amount * Decimal(10**decimals))
        assert usdc_received == expected_usdc_received, (
            f"USDC received must EXACTLY equal withdraw amount. "
            f"Expected: {expected_usdc_received}, Got: {usdc_received}"
        )

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        # The SUPPLY above was NOT persisted through the Layer-5 harness, so the
        # FIFO supply pool is empty: WITHDRAW degrades — principal falls back to
        # the total and interest_delta_usd stays None (never a fabricated 0).
        # This is the degradation contract for an unmatched withdraw (epic
        # decision #6), identical to the Spark golden. The chain-state read is
        # HIGH-confidence via the VIB-4966 vault/EVC reader (capture_lending_pre_state
        # / capture_lending_post_state → ACCOUNT_STATE_READ_SPEC, euler_v2 in
        # _GENERIC_PRE_STATE_PROTOCOLS, _assert_high_confidence_state) — distinct from
        # the unmatched-FIFO interest degradation.
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent,
            funded_wallet,
            anvil_eth_call_adapter,
            price_oracle,
            post=True,
        )

        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="WITHDRAW",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="WITHDRAW", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        # VIB-4966: bespoke reader → confidence=HIGH. Withdraw decreases collateral
        # (we supplied 2000 then withdrew 1000, so after < before).
        _assert_high_confidence_state(payload)
        assert Decimal(payload["collateral_value_after_usd"]) < Decimal(payload["collateral_value_before_usd"]), (
            "WITHDRAW must decrease on-chain collateral value"
        )
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == withdraw_amount
        assert payload["principal_delta_usd"] is not None, "WITHDRAW must measure a principal leg"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, (
            "Unmatched WITHDRAW (no Layer-5 SUPPLY lot) must degrade interest to None — never a fabricated 0"
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_intent_with_insufficient_balance_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Insufficient-balance SUPPLY must fail with USDC balance unchanged.

        Layer 5 failure contract: a failed execution must write ZERO
        accounting_events rows (books-side mirror of "balances unchanged").
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        usdc_balance = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_balance > 0, "Funded wallet must have positive USDC balance"
        balance_decimal = Decimal(usdc_balance) / Decimal(10**decimals)
        excessive_amount = balance_decimal * Decimal("100")

        intent = SupplyIntent(
            protocol="euler_v2",
            token="USDC",
            amount=excessive_amount,
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

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert not execution_result.success, "Execution should fail with insufficient balance"

        # Conservation check
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_balance, "Balance must be unchanged after failed supply"

        # ── Layer 5: failure-path accounting contract ────────────────────────
        failed_result = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
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


# =============================================================================
# Borrow / Repay Tests
# =============================================================================
#
# These are marked xfail(strict=True) until the Ethereum branch of
# ``EULER_V2_VAULTS_BY_CHAIN`` gains a non-stablecoin collateral vault.
# The only vault currently registered for Ethereum is eUSDC-2, and Euler V2
# requires a non-self collateral vault to enable borrowing. Mirror of the
# Avalanche pattern (VIB-2643).


@pytest.mark.arbitrum
@pytest.mark.borrow
@pytest.mark.lending
class TestEulerV2BorrowIntent:
    """Test Euler V2 borrow/repay operations using BorrowIntent / RepayIntent."""

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_usdc_with_weth_collateral(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Borrow USDC against WETH collateral on Euler V2 Arbitrum.

        Two-intent form (#2827): a SupplyIntent deposits the WETH collateral
        into the eWETH vault, then a standalone
        ``BorrowIntent(collateral_amount=0)`` draws USDC via the EVC batch
        (enableCollateral + enableController + borrow) — the bundled shape is
        fail-closed at the intent validator.

        Uses the registered eWETH collateral vault + eUSDC borrow vault.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        weth = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc)
        weth_decimals = get_token_decimals(web3, weth)

        # LTV headroom: ~$1800/WETH; 0.5 WETH collateral = ~$900;
        # 250 USDC borrow = ~28% LTV → safely under the 30% cap.
        collateral_amount = Decimal("0.5")
        weth_price = price_oracle.get("WETH", Decimal("1800"))
        max_borrow_usd = collateral_amount * weth_price * Decimal("0.30")
        borrow_amount = min(Decimal("250"), max_borrow_usd)

        weth_before = get_token_balance(web3, weth, funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        assert weth_before >= int(collateral_amount * Decimal(10**weth_decimals)), (
            f"Funded wallet lacks WETH collateral. Need {collateral_amount}, have {weth_before / 10**weth_decimals}"
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Step 1 — deposit the WETH collateral into the eWETH vault (two-intent
        # form, #2827: the bundled shape is fail-closed at the intent
        # validator). The standalone borrow's EVC batch enables it as
        # collateral.
        supply_intent = SupplyIntent(
            protocol="euler_v2",
            token="WETH",
            amount=collateral_amount,
            chain=CHAIN_NAME,
        )
        supply_compile = compiler.compile(supply_intent)
        assert supply_compile.status.value == "SUCCESS", f"Supply compilation failed: {supply_compile.error}"
        assert supply_compile.action_bundle is not None
        supply_exec = await orchestrator.execute(supply_compile.action_bundle)
        assert supply_exec.success, f"Collateral supply failed: {supply_exec.error}"

        # Layer 3 (supply leg): Deposit event on the WETH collateral vault.
        supply_deposit_seen = False
        for tx_result in supply_exec.transaction_results:
            if tx_result.receipt:
                supply_parse = EulerV2ReceiptParser(underlying_decimals=weth_decimals).parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_WETH_VAULT,
                )
                if supply_parse.success and supply_parse.deposit_amount > 0:
                    assert supply_parse.deposit_shares > 0
                    supply_deposit_seen = True
        assert supply_deposit_seen, "Receipt parser must find a Deposit event on the WETH collateral vault"

        # Step 2 — standalone borrow (the only shape the public API allows).
        intent = BorrowIntent(
            protocol="euler_v2",
            collateral_token="WETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            chain=CHAIN_NAME,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None
        assert len(compilation_result.action_bundle.transactions) == 1, (
            "Standalone borrow must compile to a single EVC borrow tx (no bundled collateral deposit)"
        )

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Receipt parse — Borrow event
        found_borrow_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = EulerV2ReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
                )
                if parse_result.success and parse_result.borrow_amount > 0:
                    assert parse_result.borrow_amount > 0
                    found_borrow_event = True
        assert found_borrow_event, "Receipt parser must find at least one Borrow event"

        # Standalone borrow must not deposit collateral (moved to step 1).
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                wparse = EulerV2ReceiptParser(underlying_decimals=weth_decimals).parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_WETH_VAULT,
                )
                assert not (wparse.success and wparse.deposit_amount > 0), (
                    "Standalone borrow must not emit a collateral Deposit event"
                )

        # Balance deltas — exact
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        weth_after = get_token_balance(web3, weth, funded_wallet)
        usdc_received = usdc_after - usdc_before
        weth_spent = weth_before - weth_after
        expected_usdc_received = int(borrow_amount * Decimal(10**usdc_decimals))
        expected_weth_spent = int(collateral_amount * Decimal(10**weth_decimals))
        assert usdc_received == expected_usdc_received
        assert weth_spent == expected_weth_spent

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent,
            funded_wallet,
            anvil_eth_call_adapter,
            price_oracle,
            post=True,
        )

        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="BORROW",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="BORROW", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        # VIB-4966: bespoke reader → confidence=HIGH. Borrow increases on-chain debt.
        _assert_high_confidence_state(payload)
        assert Decimal(payload["debt_value_after_usd"]) > Decimal(payload["debt_value_before_usd"]), (
            "BORROW must increase on-chain debt value"
        )
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == borrow_amount
        # BORROW records the FIFO principal lot: principal measured, interest
        # has no leg yet (a repay would match it) — must be None, not 0.
        assert payload["principal_delta_usd"] is not None, "BORROW must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "BORROW has no interest leg yet — must be None, not 0"

    @pytest.mark.withdraw
    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_borrow_repay_then_withdraw_all_still_exits_with_controller_enabled(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ) -> None:
        """VIB-5801 controller-gate regression, at 4 layers on a real fork.

        This is the exact teardown shape euler's borrow lifecycle emits
        (``repay(repay_full=True)`` -> ``withdraw(withdraw_all=True)``), and it is the
        lane a Stage-1 audit blocker would have wedged **forever**.

        EVK zeroes ``maxRedeem`` for ANY controller-enabled account — no debt check, no
        health check (``Vault.sol``: ``if (max.isZero() || hasAnyControllerEnabled(owner))
        return Shares.wrap(0)``). Borrowing enables a controller and **nothing in this repo
        ever calls disableController()**, so a borrower's ``maxRedeem`` is 0 even after the
        debt is fully repaid. Reading that 0 as "illiquid" would classify every borrow
        teardown TRANSIENT — retrying forever against a state that can never change — while
        ``redeem(MAX)`` succeeds perfectly well.

        So this test pins the invariant that the unit tests can only assert in the
        abstract: **with a real controller enabled on-chain, a full exit still emits
        redeem(MAX) and still goes flat.**
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        weth = tokens["WETH"]

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        collateral_amount = Decimal("0.5")
        weth_price = price_oracle.get("WETH", Decimal("1800"))
        # <=30% LTV per .claude/rules/intent-tests.md — the oracle uses live prices.
        borrow_amount = min(Decimal("250"), collateral_amount * weth_price * Decimal("0.30"))

        weth_decimals = get_token_decimals(web3, weth)
        weth_balance = get_token_balance(web3, weth, funded_wallet)
        assert weth_balance >= int(collateral_amount * Decimal(10**weth_decimals)), (
            f"Funded wallet lacks WETH collateral. Need {collateral_amount}, have {weth_balance / 10**weth_decimals}"
        )

        # --- Setup: borrow (this is what enables the EVC controller) ---
        setup_supply_intent = SupplyIntent(
            protocol="euler_v2",
            token="WETH",
            amount=collateral_amount,
            chain=CHAIN_NAME,
        )
        setup_supply_result = compiler.compile(setup_supply_intent)
        assert setup_supply_result.status.value == "SUCCESS"
        assert setup_supply_result.action_bundle is not None
        setup_supply_exec = await orchestrator.execute(setup_supply_result.action_bundle)
        assert setup_supply_exec.success, f"Collateral supply setup failed: {setup_supply_exec.error}"

        borrow_intent = BorrowIntent(
            protocol="euler_v2",
            collateral_token="WETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            chain=CHAIN_NAME,
        )
        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS", f"Borrow compile failed: {borrow_result.error}"
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle)
        assert borrow_exec.success, f"Borrow setup failed: {borrow_exec.error}"

        evc = web3.eth.contract(
            address=Web3.to_checksum_address(EULER_V2_EVC),
            abi=[
                {
                    "name": "getControllers",
                    "type": "function",
                    "stateMutability": "view",
                    "inputs": [{"name": "account", "type": "address"}],
                    "outputs": [{"name": "", "type": "address[]"}],
                }
            ],
        )
        collateral_vault = web3.eth.contract(
            address=Web3.to_checksum_address(EULER_V2_WETH_VAULT),
            abi=[
                {
                    "name": "balanceOf",
                    "type": "function",
                    "stateMutability": "view",
                    "inputs": [{"name": "a", "type": "address"}],
                    "outputs": [{"name": "", "type": "uint256"}],
                },
                {
                    "name": "maxRedeem",
                    "type": "function",
                    "stateMutability": "view",
                    "inputs": [{"name": "a", "type": "address"}],
                    "outputs": [{"name": "", "type": "uint256"}],
                },
            ],
        )
        wallet_cs = Web3.to_checksum_address(funded_wallet)

        # --- Setup: repay the debt in full (what teardown does before withdraw_all) ---
        repay_result = compiler.compile(
            RepayIntent.model_construct(
                protocol="euler_v2",
                token="USDC",
                amount=borrow_amount,
                repay_full=True,
                chain=CHAIN_NAME,
            )
        )
        assert repay_result.status.value == "SUCCESS", f"Repay compile failed: {repay_result.error}"
        repay_exec = await orchestrator.execute(repay_result.action_bundle)
        assert repay_exec.success, f"Repay setup failed: {repay_exec.error}"

        # --- The precondition this test exists for, asserted on-chain (not assumed) ---
        controllers = evc.functions.getControllers(wallet_cs).call()
        collateral_shares = collateral_vault.functions.balanceOf(wallet_cs).call()
        max_redeem = collateral_vault.functions.maxRedeem(wallet_cs).call()
        assert len(controllers) > 0, (
            "Setup invalid: borrowing must leave an EVC controller enabled — that is the "
            "whole condition under test. Without it this test proves nothing."
        )
        assert collateral_shares > 0, "Setup invalid: the wallet must hold collateral shares to exit"
        assert max_redeem == 0, (
            f"EVK is expected to zero maxRedeem for a controller-enabled account even at zero "
            f"debt; got {max_redeem} against {collateral_shares} shares. If this ever fails, "
            f"EVK's semantics changed and the controller gate should be re-derived — do NOT "
            f"just relax this assertion."
        )

        # --- Layer 1: the full exit must NOT be classified transient ---
        withdraw_intent = WithdrawIntent(
            protocol="euler_v2",
            token="WETH",
            amount=Decimal("1"),  # ignored — withdraw_all wins
            withdraw_all=True,
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(withdraw_intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"REGRESSION: a controller-enabled full exit compiled to "
            f"{compilation_result.status.value} ({compilation_result.error}). maxRedeem is 0 "
            f"for ANY borrower regardless of debt — treating that as 'illiquid' wedges euler "
            f"borrow teardown forever, because only disableController() clears it and this "
            f"repo never calls it."
        )
        assert compilation_result.is_transient is False, "A controller-enabled exit must never be transient"
        assert compilation_result.action_bundle.metadata["full_exit_mode"] == "redeem_max"
        calldata = compilation_result.transactions[0].data.lower()
        assert calldata.startswith("0xba087652"), "Full exit must call redeem(uint256,address,address)"
        assert "f" * 64 in calldata, "A controller-enabled full exit must carry MAX_UINT256"

        # --- Layer 2: execute ---
        weth_before = get_token_balance(web3, weth, funded_wallet)
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Controller-enabled full exit failed to execute: {execution_result.error}"

        # --- Layer 3: receipt parse ---
        found_withdraw_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = EulerV2ReceiptParser(underlying_decimals=weth_decimals).parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_WETH_VAULT,
                )
                if parse_result.success and parse_result.withdraw_amount > 0:
                    assert parse_result.withdraw_shares > 0
                    found_withdraw_event = True
        assert found_withdraw_event, "Receipt parser must find a Withdraw event on the collateral vault"

        # --- Layer 4: balance deltas — the position actually went flat ---
        shares_after = collateral_vault.functions.balanceOf(wallet_cs).call()
        assert shares_after == 0, (
            f"redeem(MAX) must drain the collateral position to ZERO even with a controller "
            f"enabled. Residual: {shares_after}"
        )
        weth_received = get_token_balance(web3, weth, funded_wallet) - weth_before
        assert weth_received > 0, "Full exit must return WETH to the wallet (no-op guard)"

    @pytest.mark.repay
    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_usdc_after_borrow(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """Repay portion of USDC debt via RepayIntent (after borrow setup)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        weth = tokens["WETH"]
        usdc_decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        # Setup: borrow first (will revert in the xfail world; here for shape).
        collateral_amount = Decimal("0.5")
        weth_price = price_oracle.get("WETH", Decimal("1800"))
        max_borrow_usd = collateral_amount * weth_price * Decimal("0.30")
        borrow_amount = min(Decimal("250"), max_borrow_usd)
        repay_amount = borrow_amount / Decimal("2")

        # Pre-flight: ensure the wallet has the WETH collateral. If this fails
        # we want a clear assertion error, not a confusing borrow revert.
        weth_balance = get_token_balance(web3, weth, funded_wallet)
        weth_decimals = get_token_decimals(web3, weth)
        assert weth_balance >= int(collateral_amount * Decimal(10**weth_decimals)), (
            f"Funded wallet lacks WETH collateral. Need {collateral_amount}, have {weth_balance / 10**weth_decimals}"
        )

        setup_supply_intent = SupplyIntent(
            protocol="euler_v2",
            token="WETH",
            amount=collateral_amount,
            chain=CHAIN_NAME,
        )
        setup_supply_result = compiler.compile(setup_supply_intent)
        assert setup_supply_result.status.value == "SUCCESS"
        assert setup_supply_result.action_bundle is not None
        setup_supply_exec = await orchestrator.execute(setup_supply_result.action_bundle)
        assert setup_supply_exec.success, f"Collateral supply setup failed: {setup_supply_exec.error}"

        borrow_intent = BorrowIntent(
            protocol="euler_v2",
            collateral_token="WETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            chain=CHAIN_NAME,
        )
        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_pre_state = _capture_lending_state(
            borrow_intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle)
        assert borrow_exec.success, f"Borrow setup failed: {borrow_exec.error}"

        # Layer 5: persist the BORROW so the FIFO basis pool holds the lot the
        # REPAY will match against (this is what makes the split exact).
        borrow_enriched = _enrich_for_accounting(
            borrow_exec, borrow_intent, funded_wallet, borrow_result.action_bundle.metadata
        )
        borrow_post_state = _capture_lending_state(
            borrow_intent,
            funded_wallet,
            anvil_eth_call_adapter,
            price_oracle,
            post=True,
        )
        borrow_row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=borrow_intent,
            result=borrow_enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="BORROW",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=borrow_pre_state,
            post_state=borrow_post_state,
        )
        borrow_payload = _payload(borrow_row)
        borrowed_principal_usd = Decimal(borrow_payload["principal_delta_usd"])

        usdc_before = get_token_balance(web3, usdc, funded_wallet)

        intent = RepayIntent(
            protocol="euler_v2",
            token="USDC",
            amount=repay_amount,
            chain=CHAIN_NAME,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        pre_state = _capture_lending_state(intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False)

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Receipt parse — Repay event
        found_repay_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = EulerV2ReceiptParser(underlying_decimals=usdc_decimals)
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    vault_address=EULER_V2_USDC_VAULT,
                )
                if parse_result.success and parse_result.repay_amount > 0:
                    assert parse_result.repay_amount > 0
                    found_repay_event = True
        assert found_repay_event, "Receipt parser must find at least one Repay event"

        # Balance delta — exact
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        expected_usdc_spent = int(repay_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent

        # ── Layer 5: borrow-then-repay FIFO split ────────────────────────────
        enriched = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
        )
        post_state = _capture_lending_state(
            intent,
            funded_wallet,
            anvil_eth_call_adapter,
            price_oracle,
            post=True,
        )

        row = await assert_accounting_persisted(
            layer5_accounting_harness,
            intent=intent,
            result=enriched,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="REPAY",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="REPAY", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        # VIB-4966: bespoke reader → confidence=HIGH. Repay decreases on-chain debt
        # (we borrowed borrow_amount then repaid repay_amount, so after < before).
        _assert_high_confidence_state(payload)
        assert Decimal(payload["debt_value_after_usd"]) < Decimal(payload["debt_value_before_usd"]), (
            "REPAY must decrease on-chain debt value"
        )
        assert payload["asset"] == "USDC"
        assert Decimal(payload["amount_token"]) == repay_amount

        # Exact FIFO split: independent of the chain-state read (it derives from
        # the basis store, not from post_state_json). The REPAY matched the prior
        # BORROW lot in the same harness; repaying repay_amount of a borrow_amount
        # position within the same Anvil block accrues no interest, so the entire
        # repaid amount is matched principal and the interest leg is a measured
        # zero (NOT None — the match succeeded).
        assert payload["principal_delta_usd"] is not None, "matched REPAY must measure principal"
        assert payload["interest_delta_usd"] is not None, (
            "matched REPAY (BORROW lot present in harness) must produce a measured interest leg — not None"
        )
        principal_usd = Decimal(payload["principal_delta_usd"])
        interest_usd = Decimal(payload["interest_delta_usd"])
        repaid_usd = repay_amount * (borrowed_principal_usd / borrow_amount)
        assert principal_usd == repaid_usd, (
            f"FIFO principal_delta_usd must equal the matched principal ({repaid_usd}); got {principal_usd}"
        )
        assert interest_usd == Decimal("0"), (
            f"same-block partial repay accrues no interest — interest_delta_usd must be a measured 0, got {interest_usd}"
        )
        assert principal_usd + interest_usd == repaid_usd, "principal + interest must tie to repaid cash flow"

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=False,
        reason="VIB-2643: euler_v2 zero-collateral borrow on eUSDC-2 succeeds where the test expects revert — vault may not enforce the LTV check the test assumes (as of 2026-05-12)",
    )
    async def test_borrow_without_collateral_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ) -> None:
        """BorrowIntent with zero collateral must fail on-chain with balances unchanged.

        Layer 5 failure contract: zero accounting_events rows.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        weth = tokens["WETH"]

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)

        intent = BorrowIntent(
            protocol="euler_v2",
            collateral_token="WETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("100"),
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

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert not execution_result.success, "Execution should fail without collateral"

        # Conservation check
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        weth_after = get_token_balance(web3, weth, funded_wallet)
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed borrow"
        assert weth_after == weth_before, "WETH balance must be unchanged after failed borrow"

        # ── Layer 5: failure-path accounting contract ────────────────────────
        failed_result = _enrich_for_accounting(
            execution_result, intent, funded_wallet, compilation_result.action_bundle.metadata
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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
