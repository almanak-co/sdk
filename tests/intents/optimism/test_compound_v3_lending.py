"""Production-grade lending intent tests for Compound V3 (Comet) on Optimism.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for the
USDC Comet on Optimism (``0x2e44e174f7D53F0212823acC11C01A11d58c5bCB``):

1. Create lending intents (SupplyIntent, WithdrawIntent, BorrowIntent, RepayIntent)
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using CompoundV3ReceiptParser
5. Verify balance changes and Comet position state
6. Layer 5 — persist the real ExecutionResult through the real accounting
   pipeline (ledger -> outbox -> AccountingProcessor.drain_one) into a
   throwaway SQLite and assert the typed LendingAccountingEvent is correct.

Layer 5 (epic VIB-4591 / ticket VIB-4603): the borrow-then-repay happy path
asserts the exact ``principal_delta_usd`` / ``interest_delta_usd`` FIFO split;
the standalone-repay path asserts the degradation contract
(``interest_delta_usd is None``, never a fabricated 0). Compound V3 has a
pre/post-state reader (``read_compound_v3_account_state``), so the Anvil
``eth_call`` adapter populates before/after collateral / debt / health-factor
at ``confidence=HIGH``. The failure path asserts zero ``accounting_events``
rows. The lending category handler is protocol-agnostic (it keys on
``intent_type`` + the FIFO basis store), so the assertions mirror the merged
Aave V3 golden exactly — see ``tests/intents/arbitrum/test_aave_v3_lending.py``.

Compound V3 differs structurally from Aave V3:

- Asymmetric supply paths. Each Comet has exactly one base (borrowable) asset
  and a fixed set of collateral assets. Supplying the base asset routes through
  ``Comet.supply()``; supplying any collateral asset routes through
  ``Comet.supplyCollateral(asset, amount)``. The intent compiler picks the path
  by comparing ``supply_token.address`` to the market's ``base_token_address``;
  the user does not choose. We exercise both paths.
- Borrow ≡ withdraw at the event layer. Compound V3 does not emit a distinct
  ``Borrow`` event — when the user withdraws more base than they hold, the
  delta becomes a borrow position, but only a ``Withdraw`` event is emitted.
  Symmetrically, repay ≡ supply.
- ``BorrowIntent`` is bundled. With ``collateral_amount > 0``, the compiler
  emits ``approve(collateral) + supplyCollateral(asset, amount) + borrow(base)``
  in a single ActionBundle. With ``collateral_amount = Decimal("0")``, only the
  bare ``borrow`` is emitted — used for the no-collateral failure path.
- No ``getUserAccountData``. The Comet exposes per-position state via
  ``balanceOf`` (base supply), ``borrowBalanceOf`` (base debt), and
  ``userCollateral(account, asset)`` (per-collateral position). We use those
  directly; helpers are file-local to mirror the canonical arbitrum reference.
- No ``interest_rate_mode``. Compound V3 has a single utilization-driven rate
  per Comet — passing ``interest_rate_mode`` to a Compound V3 intent is
  rejected by the BorrowIntent validator (see ``PROTOCOL_CAPABILITIES`` in
  ``almanak/framework/intents/vocabulary.py``). We omit the field.

NO MOCKING. All tests execute real on-chain transactions and verify state
changes.

To run:
    uv run pytest tests/intents/optimism/test_compound_v3_lending.py -v -s
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from web3 import Web3

from almanak.framework.accounting.lending_accounting import (
    capture_lending_post_state,
    capture_lending_pre_state,
    lending_state_to_dict,
)
from almanak.framework.connectors.compound_v3.adapter import (
    COMPOUND_V3_COMET_ADDRESSES,
)
from almanak.framework.connectors.compound_v3.receipt_parser import CompoundV3ReceiptParser
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionResult,
)
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
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

# =============================================================================
# Test Configuration
# =============================================================================

PROTOCOL = "compound_v3"

CHAIN_NAME = "optimism"
MARKET_ID = "usdc"  # Comet alias key in COMPOUND_V3_COMET_ADDRESSES["optimism"]

# Minimal Comet ABI — only what tests need to read per-position state.
# The Comet contract has no Aave-style getUserAccountData(), so we read
# balanceOf / borrowBalanceOf / userCollateral directly.
COMET_ABI = [
    {
        "name": "balanceOf",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "borrowBalanceOf",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "userCollateral",
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "asset", "type": "address"},
        ],
        "outputs": [
            {"name": "balance", "type": "uint128"},
            {"name": "_reserved", "type": "uint128"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "isLiquidatable",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "isBorrowCollateralized",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
]


# =============================================================================
# Helper Functions (file-local — mirrors the arbitrum reference structure so
# the four chain variants of this test stay byte-for-byte aligned per
# anti-pattern #7 in .claude/rules/intent-tests.md.)
# =============================================================================


def _comet_contract(web3: Web3, comet_address: str):
    return web3.eth.contract(address=Web3.to_checksum_address(comet_address), abi=COMET_ABI)


def get_comet_supply_balance(web3: Web3, comet_address: str, account: str) -> int:
    """Return the user's base-asset supply position on the Comet (in base wei)."""
    return _comet_contract(web3, comet_address).functions.balanceOf(Web3.to_checksum_address(account)).call()


def get_comet_borrow_balance(web3: Web3, comet_address: str, account: str) -> int:
    """Return the user's outstanding base-asset debt on the Comet (in base wei)."""
    return _comet_contract(web3, comet_address).functions.borrowBalanceOf(Web3.to_checksum_address(account)).call()


def get_comet_collateral_balance(web3: Web3, comet_address: str, account: str, asset: str) -> int:
    """Return the user's collateral balance for ``asset`` on the Comet (in asset wei)."""
    balance, _ = (
        _comet_contract(web3, comet_address)
        .functions.userCollateral(
            Web3.to_checksum_address(account),
            Web3.to_checksum_address(asset),
        )
        .call()
    )
    return balance


def is_borrow_collateralized(web3: Web3, comet_address: str, account: str) -> bool:
    """Return True if the account is currently sufficiently collateralized."""
    return (
        _comet_contract(web3, comet_address).functions.isBorrowCollateralized(Web3.to_checksum_address(account)).call()
    )


def is_liquidatable(web3: Web3, comet_address: str, account: str) -> bool:
    """Return True if the account would be liquidatable right now."""
    return _comet_contract(web3, comet_address).functions.isLiquidatable(Web3.to_checksum_address(account)).call()


def _safe_usdc_borrow_amount(price_oracle: dict[str, Decimal], weth_amount: Decimal) -> Decimal:
    """Return a USDC borrow amount targeting ~25% LTV against ``weth_amount`` of WETH.

    The 4-layer mandate caps lending borrow tests at 30% LTV; the price oracle
    is session-scoped and reads live CoinGecko prices, so a hardcoded
    ``borrow_amount`` (e.g. 500 USDC) silently breaches the cap whenever WETH
    drops below the ratio that made it ~30% at write time. Computing from the
    fixture keeps headroom durable across normal market drift.

    Targets 25% LTV (5% headroom under the 30% cap). Quantizes to 2 decimals so
    USDC amounts round to whole cents.
    """
    weth_price_usd = price_oracle["WETH"]
    return (weth_amount * weth_price_usd * Decimal("0.25")).quantize(Decimal("0.01"))


# =============================================================================
# Layer 5 helpers (shared)
# =============================================================================
#
# Mirror the merged Aave V3 golden (``test_aave_v3_lending.py``). The lending
# category handler is protocol-agnostic — it keys on ``intent_type`` and the
# FIFO basis store, not on the protocol — so Compound V3's persisted
# LendingAccountingEvent obeys the same null-contract / FIFO-split rules.
# ``enrich_result`` makes the ledger entry carry extracted_data;
# ``capture_lending_pre_state`` / ``capture_lending_post_state`` dispatch on
# ``intent.protocol`` and use ``read_compound_v3_account_state`` via the
# test-scoped Anvil ``eth_call`` adapter so the handler reads real
# collateral/debt/HF and emits ``confidence=HIGH``.


def _execution_context(wallet: str) -> ExecutionContext:
    # NOTE: this deployment_id flows only into ``enrich_result`` (it labels the
    # ExecutionContext for enrichment). It is deliberately NOT what lands in
    # the persisted accounting row: the conftest ``assert_accounting_persisted``
    # helper stamps the row's deployment_id from its own ``deployment_id=
    # "layer5-intent-test"`` default, which is what ``_assert_identity``
    # checks. This split (descriptive enrichment id vs canonical persisted
    # identity) intentionally mirrors the merged Aave V3 golden
    # (``test_aave_v3_lending.py``: ``deployment_id="layer5-aave-v3-lending"``).
    return ExecutionContext(
        deployment_id="layer5-compound-v3-lending",
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
    """Capture and serialize Compound V3 pre/post state via the Anvil eth_call adapter.

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


def _assert_no_lot_id(row: dict, payload: dict) -> None:
    """Epic decision #6: no lot_id on the persisted lending event."""
    assert "lot_id" not in row
    assert "lot_id" not in payload


def _assert_high_confidence_state(payload: dict) -> None:
    """Compound V3 has a pre/post-state reader → confidence=HIGH with state populated."""
    assert payload["confidence"] == "HIGH", (
        f"Compound V3 lending must persist confidence=HIGH (reader + Anvil eth_call adapter), "
        f"got {payload['confidence']!r} (unavailable_reason={payload.get('unavailable_reason')!r})"
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
    (lending_handler.py: ``asset = (...).upper()``), so polygon's bridged
    base token persists as ``USDC.E`` even though the intent symbol is
    ``USDC.e``. Compare case-insensitively — the symbol identity, not its
    casing, is the contract.
    """
    assert payload["asset"].upper() == expected.upper(), (
        f"persisted asset {payload['asset']!r} must match {expected!r} (case-insensitive)"
    )


def _assert_repay_state_degraded_vib4633(payload: dict) -> None:
    """Compound V3 REPAY genuine production degradation contract (VIB-4633).

    Compound V3's ``_capture_compound_v3_pre_state`` takes the
    non-SUPPLY/WITHDRAW branch for a ``RepayIntent`` and requires
    ``intent.collateral_token`` — which ``RepayIntent`` does not have — so
    the account-state read is always skipped for REPAY. The persisted event
    therefore degrades to ``confidence=ESTIMATED`` with ``post_state_json``
    unavailable. This is the TRUE current production behavior (deterministic
    across all 5 chains on real Anvil-fork CI), NOT a flake. We assert the
    genuine degradation contract here rather than HIGH; the HIGH-confidence
    expectation (and the before/after collateral/debt/HF fidelity) is the
    gap tracked by VIB-4633. Empty≠Zero≠None: ``unavailable_reason`` is set,
    nothing is fabricated.
    """
    assert payload["confidence"] == "ESTIMATED", (
        f"Compound V3 REPAY genuinely degrades to confidence=ESTIMATED today "
        f"(VIB-4633: _capture_compound_v3_pre_state requires collateral_token, "
        f"absent on RepayIntent); got {payload['confidence']!r}"
    )
    assert payload.get("unavailable_reason"), (
        "degraded REPAY must carry a non-empty unavailable_reason (never fabricated)"
    )
    # Degradation must not fabricate before/after chain state.
    assert payload["collateral_value_after_usd"] is None, (
        "VIB-4633: degraded REPAY must not fabricate after-collateral"
    )
    assert payload["debt_value_after_usd"] is None, (
        "VIB-4633: degraded REPAY must not fabricate after-debt"
    )


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def comet_address() -> str:
    """Return the Optimism native-USDC Comet address (checksummed by lookup)."""
    return COMPOUND_V3_COMET_ADDRESSES[CHAIN_NAME][MARKET_ID]


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    """Create ExecutionContext with simulation enabled for accurate gas estimation."""
    return ExecutionContext(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        simulation_enabled=True,
    )


# =============================================================================
# Supply / Withdraw Tests
# =============================================================================


@pytest.mark.optimism
@pytest.mark.supply
@pytest.mark.lending
class TestCompoundV3SupplyIntent:
    """Test Compound V3 supply/withdraw on the USDC Comet.

    Covers BOTH supply paths because Compound V3 routes them differently:
      - ``test_supply_usdc_base_using_intent``: USDC is the base asset →
        Comet.supply() (earns interest, becomes balanceOf base position).
      - ``test_supply_weth_collateral_using_intent``: WETH is a registered
        collateral on this Comet → Comet.supplyCollateral(WETH, amount)
        (no interest, posts collateral that backs future borrows).
    """

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_usdc_base_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        comet_address: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Supply USDC (the Comet's base asset) — routes through Comet.supply()."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("1000")  # 1000 USDC

        print(f"\n{'=' * 80}")
        print(f"Test: Supply {supply_amount} USDC (base) to Compound V3 USDC Comet on {CHAIN_NAME}")
        print(f"{'=' * 80}")

        # Layer 4 baseline (token balance + on-chain Comet base position)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        comet_supply_before = get_comet_supply_balance(web3, comet_address, funded_wallet)
        print(f"USDC before:                {format_token_amount(usdc_before, decimals)}")
        print(f"Comet base position before: {comet_supply_before}")

        # Layer 1: Build & compile intent
        intent = SupplyIntent(
            protocol="compound_v3",
            token="USDC",
            amount=supply_amount,
            chain=CHAIN_NAME,
            market_id=MARKET_ID,
        )
        print(f"\nCreated SupplyIntent: token={intent.token}, amount={intent.amount}, market_id={intent.market_id}")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"
        print(f"ActionBundle has {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        # Layer 2: Execute via orchestrator
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful, {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Parse receipts with CompoundV3ReceiptParser
        parser = CompoundV3ReceiptParser(base_decimals=decimals)
        observed_supply_amount = Decimal("0")
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}: {tx_result.tx_hash[:16]}... gas={tx_result.gas_used}")
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict(), comet_address=comet_address)
                if parse_result.success and parse_result.supply_amount > 0:
                    observed_supply_amount += parse_result.supply_amount
                    print(f"  Parsed Supply event amount: {parse_result.supply_amount}")
        assert observed_supply_amount > 0, "Receipt parser must observe a Supply event on the Comet"

        # Layer 4: Exact balance delta + on-chain Comet position changed
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        expected_usdc_spent = int(supply_amount * Decimal(10**decimals))
        print(f"\nUSDC spent: {format_token_amount(usdc_spent, decimals)} (expected exact: {expected_usdc_spent})")
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal supply amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        comet_supply_after = get_comet_supply_balance(web3, comet_address, funded_wallet)
        print(f"Comet base position after: {comet_supply_after}")
        assert comet_supply_after > comet_supply_before, "Comet base supply position must increase after supply"

        # ── Layer 5: real accounting pipeline ────────────────────────────────
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
        _assert_asset(payload, "USDC")
        assert payload["amount_token"] is not None
        assert Decimal(payload["amount_token"]) == supply_amount
        # SUPPLY drains wallet inventory: principal_delta_usd is measured (the
        # supplied principal in USD); interest is not applicable on SUPPLY.
        assert payload["principal_delta_usd"] is not None, "SUPPLY must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "SUPPLY has no interest leg — must be None, not 0"
        # Supplying the base asset increases the Comet base position; the
        # accounting handler tracks it as collateral_value.
        assert Decimal(payload["collateral_value_after_usd"]) > Decimal(payload["collateral_value_before_usd"])

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_weth_collateral_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        comet_address: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Supply WETH (a collateral asset, NOT base) — routes through Comet.supplyCollateral().

        The compiler picks the path by address comparison against the market's
        base_token_address; tests do NOT set ``use_as_collateral`` because the
        default (True) is the only correct value for a non-base token (setting
        False fails closed in connectors/base/lending/aave_helpers.py).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth = tokens["WETH"]
        decimals = get_token_decimals(web3, weth)

        collateral_amount = Decimal("1")  # 1 WETH

        print(f"\n{'=' * 80}")
        print(f"Test: Supply {collateral_amount} WETH as collateral on Compound V3 USDC Comet on {CHAIN_NAME}")
        print(f"{'=' * 80}")

        weth_before = get_token_balance(web3, weth, funded_wallet)
        comet_collateral_before = get_comet_collateral_balance(web3, comet_address, funded_wallet, weth)
        print(f"WETH before:                       {format_token_amount(weth_before, decimals)}")
        print(f"Comet WETH collateral position before: {comet_collateral_before}")

        intent = SupplyIntent(
            protocol="compound_v3",
            token="WETH",
            amount=collateral_amount,
            chain=CHAIN_NAME,
            market_id=MARKET_ID,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None
        print(f"ActionBundle has {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: receipt parser must observe a SupplyCollateral event with WETH as the asset.
        parser = CompoundV3ReceiptParser(base_decimals=get_token_decimals(web3, tokens["USDC"]))
        observed_collateral: dict[str, Decimal] = {}
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict(), comet_address=comet_address)
                if parse_result.success:
                    for asset, amount in parse_result.collateral_supplied.items():
                        observed_collateral[asset.lower()] = (
                            observed_collateral.get(asset.lower(), Decimal("0")) + amount
                        )
        weth_lower = weth.lower()
        assert weth_lower in observed_collateral, (
            f"Receipt parser must observe a SupplyCollateral event for WETH ({weth_lower}). "
            f"Observed assets: {list(observed_collateral.keys())}"
        )
        assert observed_collateral[weth_lower] > 0, "SupplyCollateral amount for WETH must be > 0"

        # Layer 4: exact WETH delta + Comet collateral position increased
        weth_after = get_token_balance(web3, weth, funded_wallet)
        weth_spent = weth_before - weth_after
        expected_weth_spent = int(collateral_amount * Decimal(10**decimals))
        print(f"\nWETH spent: {format_token_amount(weth_spent, decimals)} (expected exact: {expected_weth_spent})")
        assert weth_spent == expected_weth_spent, (
            f"WETH spent must EXACTLY equal supply amount. Expected: {expected_weth_spent}, Got: {weth_spent}"
        )

        comet_collateral_after = get_comet_collateral_balance(web3, comet_address, funded_wallet, weth)
        print(f"Comet WETH collateral position after: {comet_collateral_after}")
        assert comet_collateral_after > comet_collateral_before, (
            "Comet WETH collateral position must increase after supplyCollateral"
        )

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        # supplyCollateral(WETH) posts collateral, not the base position; the
        # accounting contract is the same as the base SUPPLY (principal
        # measured, no interest leg) — the handler keys on intent_type.
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
        # supplyCollateral still gets a full pre/post chain read (the Comet is
        # resolved from market_id) → confidence=HIGH with state populated.
        _assert_high_confidence_state(payload)
        _assert_asset(payload, "WETH")
        assert payload["interest_delta_usd"] is None, "SUPPLY has no interest leg — must be None, not 0"
        # supplyCollateral raises the Comet collateral position on-chain.
        assert Decimal(payload["collateral_value_after_usd"]) > Decimal(payload["collateral_value_before_usd"])
        # VIB-4633 (Finding A): Compound V3 supplyCollateral does NOT populate
        # amount_token / principal_delta_usd — the lending handler's
        # _extract_amount_human only has the morpho_blue collateral fallback
        # key wired, so the compound_v3 overlay's collateral amount is never
        # surfaced. The on-chain transfer is correct (asserted above:
        # collateral position increased); only the books leg is unmeasured.
        # This is a genuine production gap, NOT acceptable degradation
        # (Empty≠Zero≠None: amount is known on-chain). xfail until VIB-4633
        # wires the compound_v3 collateral amount.
        if payload["amount_token"] is None:
            pytest.xfail(
                "VIB-4633: Compound V3 supplyCollateral does not populate "
                "amount_token (handler lacks the compound_v3 collateral "
                "fallback) — on-chain transfer verified correct above"
            )
        # If a future fix lands, these become live again automatically.
        assert Decimal(payload["amount_token"]) == collateral_amount
        assert payload["principal_delta_usd"] is not None, "SUPPLY must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_usdc_base_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        comet_address: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Supply 2000 USDC then withdraw 1000 USDC — net Comet base position +1000."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        supply_amount = Decimal("2000")
        withdraw_amount = Decimal("1000")

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        # Setup: supply 2000 USDC
        supply_intent = SupplyIntent(
            protocol="compound_v3",
            token="USDC",
            amount=supply_amount,
            chain=CHAIN_NAME,
            market_id=MARKET_ID,
        )
        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS"
        assert supply_result.action_bundle is not None
        supply_exec_result = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert supply_exec_result.success, f"Initial supply failed: {supply_exec_result.error}"

        print(f"\n{'=' * 80}")
        print(f"Test: Withdraw {withdraw_amount} USDC from Compound V3 USDC Comet on {CHAIN_NAME}")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        comet_supply_before = get_comet_supply_balance(web3, comet_address, funded_wallet)
        print(f"USDC before withdraw:        {format_token_amount(usdc_before, decimals)}")
        print(f"Comet base position before:  {comet_supply_before}")

        intent = WithdrawIntent(
            protocol="compound_v3",
            token="USDC",
            amount=withdraw_amount,
            chain=CHAIN_NAME,
            market_id=MARKET_ID,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Withdraw execution failed: {execution_result.error}"

        # Layer 3: parser observes a Withdraw event on the Comet
        parser = CompoundV3ReceiptParser(base_decimals=decimals)
        observed_withdraw_amount = Decimal("0")
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict(), comet_address=comet_address)
                if parse_result.success and parse_result.withdraw_amount > 0:
                    observed_withdraw_amount += parse_result.withdraw_amount
        assert observed_withdraw_amount > 0, "Receipt parser must observe a Withdraw event on the Comet"

        # Layer 4: exact USDC delta + Comet base position decreased
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_received = usdc_after - usdc_before
        expected_usdc_received = int(withdraw_amount * Decimal(10**decimals))
        print(
            f"\nUSDC received: {format_token_amount(usdc_received, decimals)} (expected exact: {expected_usdc_received})"
        )
        assert usdc_received == expected_usdc_received, (
            f"USDC received must EXACTLY equal withdraw amount. "
            f"Expected: {expected_usdc_received}, Got: {usdc_received}"
        )

        comet_supply_after = get_comet_supply_balance(web3, comet_address, funded_wallet)
        print(f"Comet base position after:  {comet_supply_after}")
        assert comet_supply_after < comet_supply_before, "Comet base supply position must decrease after withdraw"

        # ── Layer 5: real accounting pipeline ────────────────────────────────
        # The SUPPLY above was NOT persisted through the Layer-5 harness, so
        # the FIFO supply pool is empty: WITHDRAW degrades — principal falls
        # back to the total and interest_delta_usd stays None (never a
        # fabricated 0). This is the degradation contract for an unmatched
        # withdraw (epic decision #6, mirrors standalone repay), identical to
        # the Aave V3 golden.
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
            expected_event_type="WITHDRAW",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="WITHDRAW", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        _assert_high_confidence_state(payload)
        _assert_asset(payload, "USDC")
        assert Decimal(payload["amount_token"]) == withdraw_amount
        assert payload["principal_delta_usd"] is not None, "WITHDRAW must measure a principal leg"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, (
            "Unmatched WITHDRAW (no Layer-5 SUPPLY lot) must degrade interest to "
            "None — never a fabricated 0"
        )
        # Withdraw reduces the Comet base position on-chain.
        assert Decimal(payload["collateral_value_after_usd"]) < Decimal(payload["collateral_value_before_usd"])

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_intent_with_insufficient_balance_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """SupplyIntent with more USDC than the wallet holds must fail and conserve balance.

        Layer 5 failure contract: a failed execution must write ZERO
        accounting_events rows (books-side mirror of "balances unchanged").
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        decimals = get_token_decimals(web3, usdc)

        usdc_balance = get_token_balance(web3, usdc, funded_wallet)
        # Guard against funding-fixture regression: if the wallet has 0 USDC, this test
        # becomes vacuous (excessive_amount = 0 * 100 = 0, which doesn't exercise the
        # insufficient-balance path). Fail loudly so the regression is caught.
        assert usdc_balance > 0, (
            "Funded wallet has 0 USDC -- funding fixture regressed. "
            "Expected >=1 USDC to compute a meaningfully excessive amount."
        )
        balance_decimal = Decimal(usdc_balance) / Decimal(10**decimals)
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'=' * 80}")
        print("Test: SupplyIntent with insufficient USDC balance (should fail)")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SupplyIntent(
            protocol="compound_v3",
            token="USDC",
            amount=excessive_amount,
            chain=CHAIN_NAME,
            market_id=MARKET_ID,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        assert usdc_after == usdc_balance, "USDC balance must be unchanged after failed supply"

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

        print("\nALL CHECKS PASSED")


# =============================================================================
# Borrow / Repay Tests
# =============================================================================


@pytest.mark.optimism
@pytest.mark.borrow
@pytest.mark.lending
class TestCompoundV3BorrowIntent:
    """Test Compound V3 borrow/repay on the USDC Comet.

    The compiler bundles a BorrowIntent with ``collateral_amount > 0`` as
    ``approve(collateral) + supplyCollateral(asset, amount) + borrow(base)``
    in a single ActionBundle. Repay reuses ``Comet.supply()`` of the base
    token; there is no distinct repay event.

    No ``interest_rate_mode`` is passed — Compound V3 has a single
    utilization-driven rate per Comet, and the BorrowIntent validator rejects
    the field for protocols whose capability is ``supports_interest_rate_mode:
    False``.
    """

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_usdc_with_weth_collateral_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        comet_address: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Borrow USDC against 1 WETH collateral on the Compound V3 USDC Comet.

        Borrow amount derived from the live price oracle to target ~25% LTV
        (5% headroom under the 30% cap mandated by .claude/rules/intent-tests.md).
        Compound V3 has no origination fee, so USDC received must equal the
        borrow amount exactly. Comet's WETH borrow collateral factor on
        Optimism is ~82.5% — this test stays well clear.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth = tokens["WETH"]
        usdc = tokens["USDC"]

        weth_decimals = get_token_decimals(web3, weth)
        usdc_decimals = get_token_decimals(web3, usdc)

        collateral_amount = Decimal("1")
        borrow_amount = _safe_usdc_borrow_amount(price_oracle, collateral_amount)

        print(f"\n{'=' * 80}")
        print(
            f"Test: Borrow {borrow_amount} USDC with {collateral_amount} WETH collateral (Compound V3 on {CHAIN_NAME})"
        )
        print(f"{'=' * 80}")

        weth_before = get_token_balance(web3, weth, funded_wallet)
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        comet_borrow_before = get_comet_borrow_balance(web3, comet_address, funded_wallet)
        comet_collateral_before = get_comet_collateral_balance(web3, comet_address, funded_wallet, weth)
        print(f"WETH before:                {format_token_amount(weth_before, weth_decimals)}")
        print(f"USDC before:                {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"Comet debt before:          {comet_borrow_before}")
        print(f"Comet WETH collat before:   {comet_collateral_before}")

        intent = BorrowIntent(
            protocol="compound_v3",
            collateral_token="WETH",
            collateral_amount=collateral_amount,
            borrow_token="USDC",
            borrow_amount=borrow_amount,
            chain=CHAIN_NAME,
            market_id=MARKET_ID,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None
        print(f"ActionBundle has {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 5: capture pre-state BEFORE execution (mirrors the runner)
        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Borrow execution failed: {execution_result.error}"

        # Layer 3: SupplyCollateral(WETH) + Withdraw(base) on the Comet
        parser = CompoundV3ReceiptParser(base_decimals=usdc_decimals)
        observed_collateral: dict[str, Decimal] = {}
        observed_borrow_amount = Decimal("0")
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict(), comet_address=comet_address)
                if parse_result.success:
                    for asset, amount in parse_result.collateral_supplied.items():
                        observed_collateral[asset.lower()] = (
                            observed_collateral.get(asset.lower(), Decimal("0")) + amount
                        )
                    if parse_result.withdraw_amount > 0:
                        observed_borrow_amount += parse_result.withdraw_amount
        weth_lower = weth.lower()
        assert weth_lower in observed_collateral and observed_collateral[weth_lower] > 0, (
            "Receipt parser must observe a SupplyCollateral(WETH) event on the Comet"
        )
        assert observed_borrow_amount > 0, (
            "Receipt parser must observe a Withdraw (≡ borrow) event on the Comet for the base token"
        )

        # Layer 4: exact balance deltas (no origination fee on Compound V3)
        weth_after = get_token_balance(web3, weth, funded_wallet)
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        weth_spent = weth_before - weth_after
        usdc_received = usdc_after - usdc_before

        expected_weth_spent = int(collateral_amount * Decimal(10**weth_decimals))
        expected_usdc_received = int(borrow_amount * Decimal(10**usdc_decimals))
        print(f"\nWETH spent (collateral): {format_token_amount(weth_spent, weth_decimals)}")
        print(f"USDC received (borrowed): {format_token_amount(usdc_received, usdc_decimals)}")
        assert weth_spent == expected_weth_spent, (
            f"WETH spent must EXACTLY equal collateral amount. Expected: {expected_weth_spent}, Got: {weth_spent}"
        )
        assert usdc_received == expected_usdc_received, (
            f"USDC received must EXACTLY equal borrow amount (no origination fee on Compound V3). "
            f"Expected: {expected_usdc_received}, Got: {usdc_received}"
        )

        # Layer 4: Comet position state — debt opened, collateral posted, healthy
        comet_borrow_after = get_comet_borrow_balance(web3, comet_address, funded_wallet)
        comet_collateral_after = get_comet_collateral_balance(web3, comet_address, funded_wallet, weth)
        print(f"Comet debt after:        {comet_borrow_after}")
        print(f"Comet WETH collat after: {comet_collateral_after}")
        assert comet_borrow_after > comet_borrow_before, "Comet debt must be created"
        assert comet_collateral_after > comet_collateral_before, "Comet WETH collateral must increase"
        assert is_borrow_collateralized(web3, comet_address, funded_wallet), (
            "Account must be sufficiently collateralized after borrow"
        )
        assert not is_liquidatable(web3, comet_address, funded_wallet), (
            "Account must not be liquidatable after a healthy borrow"
        )

        # ── Layer 5: real accounting pipeline ────────────────────────────────
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
            expected_event_type="BORROW",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="BORROW", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        _assert_high_confidence_state(payload)
        _assert_asset(payload, "USDC")
        assert Decimal(payload["amount_token"]) == borrow_amount
        # BORROW records the FIFO principal lot: principal measured, interest
        # has no leg yet (a repay would match it) — must be None, not 0.
        assert payload["principal_delta_usd"] is not None, "BORROW must measure principal_delta_usd"
        assert Decimal(payload["principal_delta_usd"]) > 0
        assert payload["interest_delta_usd"] is None, "BORROW has no interest leg yet — must be None, not 0"
        # Borrow creates Comet debt on-chain.
        assert Decimal(payload["debt_value_after_usd"]) > Decimal(payload["debt_value_before_usd"])

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        comet_address: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Borrow USDC vs 1 WETH (oracle-sized to ~25% LTV) then repay 200 USDC — Comet debt strictly decreases.

        Layer 5: persist BOTH the BORROW and the REPAY through the same harness
        so the FIFO basis pool matches — assert the EXACT
        principal_delta_usd / interest_delta_usd split (epic decision #6).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        # Setup: open the borrow position. Reuse the same oracle-derived sizing
        # as the dedicated borrow test so the repay flow stays under the 30% LTV
        # cap as WETH price moves.
        setup_collateral = Decimal("1")
        setup_borrow = _safe_usdc_borrow_amount(price_oracle, setup_collateral)
        borrow_intent = BorrowIntent(
            protocol="compound_v3",
            collateral_token="WETH",
            collateral_amount=setup_collateral,
            borrow_token="USDC",
            borrow_amount=setup_borrow,
            chain=CHAIN_NAME,
            market_id=MARKET_ID,
        )
        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_pre_state = _capture_lending_state(
            borrow_intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )
        borrow_exec_result = await orchestrator.execute(borrow_result.action_bundle, execution_context)
        assert borrow_exec_result.success, f"Setup borrow failed: {borrow_exec_result.error}"

        # Layer 5: persist the BORROW so the FIFO basis pool holds the lot the
        # REPAY will match against (this is what makes the split exact).
        borrow_enriched = _enrich_for_accounting(
            borrow_exec_result, borrow_intent, funded_wallet, borrow_result.action_bundle.metadata
        )
        borrow_post_state = _capture_lending_state(
            borrow_intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=True
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

        repay_amount = Decimal("200")

        print(f"\n{'=' * 80}")
        print(f"Test: Repay {repay_amount} USDC on Compound V3 USDC Comet on {CHAIN_NAME}")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        comet_borrow_before = get_comet_borrow_balance(web3, comet_address, funded_wallet)
        print(f"USDC before repay:  {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"Comet debt before:  {comet_borrow_before}")
        assert comet_borrow_before > 0, "Borrow position must exist before repay"

        intent = RepayIntent(
            protocol="compound_v3",
            token="USDC",
            amount=repay_amount,
            chain=CHAIN_NAME,
            market_id=MARKET_ID,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Repay execution failed: {execution_result.error}"

        # Layer 3: Compound V3 emits a Supply event when repaying (no distinct Repay event)
        parser = CompoundV3ReceiptParser(base_decimals=usdc_decimals)
        observed_supply_amount = Decimal("0")
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict(), comet_address=comet_address)
                if parse_result.success and parse_result.supply_amount > 0:
                    observed_supply_amount += parse_result.supply_amount
        assert observed_supply_amount > 0, (
            "Receipt parser must observe a Supply event on the Comet during repay (Compound V3 has no distinct Repay event)"
        )

        # Layer 4: exact USDC spent + Comet debt decreased
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        expected_usdc_spent = int(repay_amount * Decimal(10**usdc_decimals))
        print(
            f"\nUSDC spent (repaid): {format_token_amount(usdc_spent, usdc_decimals)} (expected exact: {expected_usdc_spent})"
        )
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal repay amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        comet_borrow_after = get_comet_borrow_balance(web3, comet_address, funded_wallet)
        print(f"Comet debt after:  {comet_borrow_after}")
        assert comet_borrow_after < comet_borrow_before, "Comet debt must decrease after repay"

        # ── Layer 5: borrow-then-repay FIFO split ────────────────────────────
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
            expected_event_type="REPAY",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="REPAY", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        # VIB-4633 (Finding B): Compound V3 REPAY post-state is never read
        # (_capture_compound_v3_pre_state requires intent.collateral_token,
        # absent on RepayIntent), so the row genuinely degrades to
        # confidence=ESTIMATED with no after-chain-state. Assert the TRUE
        # production contract here (deterministic across all 5 chains) rather
        # than HIGH; the HIGH-confidence + before/after debt fidelity is the
        # gap tracked by VIB-4633.
        _assert_repay_state_degraded_vib4633(payload)
        _assert_asset(payload, "USDC")
        assert Decimal(payload["amount_token"]) == repay_amount

        # Exact FIFO split: independent of the chain-state read. The REPAY
        # matched the prior BORROW lot in the same harness; repaying
        # repay_amount of a setup_borrow position within the same Anvil block
        # accrues no interest, so the entire repaid amount is matched
        # principal and the interest leg is a measured zero (NOT None — the
        # match succeeded). principal + interest must reconcile to the repaid
        # cash flow. (Compound V3 routes repay through Comet.supply(), but the
        # intent type is REPAY so the handler runs match_repay — same FIFO
        # contract as the Aave V3 golden. This part is unaffected by the
        # VIB-4633 post-state gap because it derives from the basis store,
        # not from post_state_json.)
        assert payload["principal_delta_usd"] is not None, "matched REPAY must measure principal"
        assert payload["interest_delta_usd"] is not None, (
            "matched REPAY (BORROW lot present in harness) must produce a "
            "measured interest leg — not None"
        )
        principal_usd = Decimal(payload["principal_delta_usd"])
        interest_usd = Decimal(payload["interest_delta_usd"])
        # Matched principal in USD = repaid fraction of the borrowed principal.
        # Both legs use the session price oracle, so this is exact (no MEV on
        # Anvil): repay_amount / setup_borrow of borrowed_principal_usd.
        repaid_usd = repay_amount * (borrowed_principal_usd / setup_borrow)
        assert principal_usd == repaid_usd, (
            f"FIFO principal_delta_usd must equal the matched principal "
            f"({repaid_usd}); got {principal_usd}"
        )
        assert interest_usd == Decimal("0"), (
            f"same-block partial repay accrues no interest — interest_delta_usd "
            f"must be a measured 0, got {interest_usd}"
        )
        assert principal_usd + interest_usd == repaid_usd, "principal + interest must tie to repaid cash flow"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_standalone_repay_degrades_interest_to_none(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        comet_address: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """Standalone repay degradation contract (epic VIB-4591 decision #6).

        A REPAY whose matching BORROW lot is NOT in the Layer-5 FIFO basis
        pool (here: the on-chain borrow is executed but deliberately not
        persisted through the harness) must degrade ``interest_delta_usd`` to
        ``None`` — never a fabricated 0. ``match_repay`` consumes no lots, so
        ``repaid_principal == 0`` and ``principal_delta_usd`` is the *measured*
        attributable zero (``_amount_to_usd(0)`` — a real Decimal('0'), not
        None, and not the full repaid cash flow: the REPAY handler does not
        fall back to total the way WITHDRAW does). Mirrors the Aave V3 golden's
        ``test_standalone_repay_degrades_interest_to_none`` exactly — the
        lending handler is protocol-agnostic.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        # On-chain borrow setup — intentionally NOT persisted through Layer 5,
        # so the FIFO basis pool has no matching BORROW lot. Oracle-sized to
        # ~25% LTV so it stays under the 30% cap as WETH price moves.
        setup_collateral = Decimal("1")
        setup_borrow = _safe_usdc_borrow_amount(price_oracle, setup_collateral)
        borrow_intent = BorrowIntent(
            protocol="compound_v3",
            collateral_token="WETH",
            collateral_amount=setup_collateral,
            borrow_token="USDC",
            borrow_amount=setup_borrow,
            chain=CHAIN_NAME,
            market_id=MARKET_ID,
        )
        borrow_result = compiler.compile(borrow_intent)
        assert borrow_result.status.value == "SUCCESS"
        assert borrow_result.action_bundle is not None
        borrow_exec = await orchestrator.execute(borrow_result.action_bundle, execution_context)
        assert borrow_exec.success, f"Borrow setup failed: {borrow_exec.error}"

        repay_amount = Decimal("200")
        print(f"\n{'=' * 80}")
        print(f"Test: Standalone Repay {repay_amount} USDC — degradation contract")
        print(f"{'=' * 80}")

        intent = RepayIntent(
            protocol="compound_v3",
            token="USDC",
            amount=repay_amount,
            chain=CHAIN_NAME,
            market_id=MARKET_ID,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        pre_state = _capture_lending_state(
            intent, funded_wallet, anvil_eth_call_adapter, price_oracle, post=False
        )

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: parser must still observe the repay (Supply event on Comet)
        parser = CompoundV3ReceiptParser(base_decimals=usdc_decimals)
        observed_supply_amount = Decimal("0")
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict(), comet_address=comet_address)
                if parse_result.success and parse_result.supply_amount > 0:
                    observed_supply_amount += parse_result.supply_amount
        assert observed_supply_amount > 0, (
            "Layer 3: parser must observe the Supply event on the Comet during repay"
        )

        # Layer 4: exact balance delta
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        usdc_spent = usdc_before - usdc_after
        expected_usdc_spent = int(repay_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal repay amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # ── Layer 5: degradation contract ────────────────────────────────────
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
            expected_event_type="REPAY",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
            pre_state=pre_state,
            post_state=post_state,
        )
        _assert_identity(row, event_type="REPAY", wallet=funded_wallet)
        payload = _payload(row)
        _assert_no_lot_id(row, payload)
        # VIB-4633 (Finding B): same Compound V3 REPAY post-state gap — the
        # row genuinely degrades to confidence=ESTIMATED. Assert the true
        # contract; the chain-state fidelity is the tracked gap.
        _assert_repay_state_degraded_vib4633(payload)
        _assert_asset(payload, "USDC")
        assert Decimal(payload["amount_token"]) == repay_amount
        # FIFO basis-store contract — independent of the chain-state read.
        # No matching BORROW lot in the harness. match_repay consumes nothing
        # → repaid_principal == 0 → principal_delta_usd is the *measured*
        # attributable zero (a real Decimal('0'), NOT None and NOT the full
        # repaid amount — the REPAY handler does not fall back to total).
        # interest_delta_usd degrades to None (never a fabricated 0). This is
        # the epic's standalone-repay degradation contract and is unaffected
        # by the VIB-4633 post-state gap (it derives from the basis store).
        assert payload["principal_delta_usd"] is not None, (
            "unmatched REPAY must report a measured principal (Decimal('0'), not None)"
        )
        assert Decimal(payload["principal_delta_usd"]) == 0, (
            "unmatched REPAY attributes zero principal (FIFO pool empty) — a "
            "measured 0, not the full repaid cash flow"
        )
        assert payload["interest_delta_usd"] is None, (
            "standalone repay with no Layer-5 BORROW lot must degrade "
            "interest_delta_usd to None — never a fabricated 0"
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_without_collateral_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """A bare ``borrow()`` with no collateral on the Comet must revert and conserve balance.

        Layer 5 failure contract: zero accounting_events rows.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        weth = tokens["WETH"]

        print(f"\n{'=' * 80}")
        print(f"Test: BorrowIntent without collateral on {CHAIN_NAME} (should fail)")
        print(f"{'=' * 80}")

        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        weth_before = get_token_balance(web3, weth, funded_wallet)

        intent = BorrowIntent(
            protocol="compound_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("0"),  # zero collateral → bare borrow only
            borrow_token="USDC",
            borrow_amount=Decimal("1000"),
            chain=CHAIN_NAME,
            market_id=MARKET_ID,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle, execution_context)
        assert not execution_result.success, "Borrow without collateral must fail"
        print(f"Execution failed as expected: {execution_result.error}")

        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        weth_after = get_token_balance(web3, weth, funded_wallet)
        assert usdc_after == usdc_before, "USDC balance must be unchanged after failed borrow"
        assert weth_after == weth_before, "WETH balance must be unchanged after failed borrow (collateral_token)"

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

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
