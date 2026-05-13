"""Production-grade lending intent tests for Compound V3 (Comet) on Base.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for the
USDC Comet on Base (``0xb125E6687d4313864e53df431d5425969c15Eb2F``):

1. Create lending intents (SupplyIntent, WithdrawIntent, BorrowIntent, RepayIntent)
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using CompoundV3ReceiptParser
5. Verify balance changes and Comet position state

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
    uv run pytest tests/intents/base/test_compound_v3_lending.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.compound_v3.adapter import (
    COMPOUND_V3_COMET_ADDRESSES,
)
from almanak.framework.connectors.compound_v3.receipt_parser import CompoundV3ReceiptParser
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

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "base"
MARKET_ID = "usdc"  # Comet alias key in COMPOUND_V3_COMET_ADDRESSES["base"]

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
# Fixtures
# =============================================================================


@pytest.fixture
def comet_address() -> str:
    """Return the Base native-USDC Comet address (checksummed by lookup)."""
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


@pytest.mark.base
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
    ):
        """Supply WETH (a collateral asset, NOT base) — routes through Comet.supplyCollateral().

        The compiler picks the path by address comparison against the market's
        base_token_address; tests do NOT set ``use_as_collateral`` because the
        default (True) is the only correct value for a non-base token (setting
        False fails closed in compiler_lending.py).
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
    ):
        """SupplyIntent with more USDC than the wallet holds must fail and conserve balance."""
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

        print("\nALL CHECKS PASSED")


# =============================================================================
# Borrow / Repay Tests
# =============================================================================


@pytest.mark.base
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
    ):
        """Borrow USDC against 1 WETH collateral on the Compound V3 USDC Comet.

        Borrow amount derived from the live price oracle to target ~25% LTV
        (5% headroom under the 30% cap mandated by .claude/rules/intent-tests.md).
        Compound V3 has no origination fee, so USDC received must equal the
        borrow amount exactly. Comet's WETH borrow collateral factor on Base
        is ~80% — this test stays well clear.
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
    ):
        """Borrow USDC vs 1 WETH (oracle-sized to ~25% LTV) then repay 200 USDC — Comet debt strictly decreases."""
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
        borrow_exec_result = await orchestrator.execute(borrow_result.action_bundle, execution_context)
        assert borrow_exec_result.success, f"Setup borrow failed: {borrow_exec_result.error}"

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
    ):
        """A bare ``borrow()`` with no collateral on the Comet must revert and conserve balance."""
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

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
