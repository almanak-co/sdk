"""Tests for amount='all' resolution in withdraw intents.

Validates that Intent.withdraw(amount="all") compiles and executes correctly
across multiple lending protocols, using the amount resolver introduced in VIB-2537.

The test pattern for each protocol:
1. Supply tokens to the protocol
2. Create WithdrawIntent(amount="all")
3. Compile (the amount resolver should resolve the amount or set withdraw_all=True)
4. Execute
5. Verify balance returned (allowing for small dust from rounding)

To run:
    uv run pytest tests/intents/arbitrum/test_withdraw_amount_all.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents import SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

CHAIN_NAME = "arbitrum"


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    return ExecutionContext(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        simulation_enabled=True,
    )


# =============================================================================
# Helper: Supply tokens to a protocol, then withdraw with amount="all"
# =============================================================================


async def _supply_then_withdraw_all(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    execution_context: ExecutionContext,
    price_oracle: dict[str, Decimal],
    protocol: str,
    token: str,
    supply_amount: Decimal,
    market_id: str | None = None,
    rpc_url: str | None = None,
):
    """Supply tokens to a protocol, then withdraw with amount='all'.

    Returns (supply_success, withdraw_success, usdc_recovered, usdc_supplied).
    """
    tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
    token_address = tokens[token]
    decimals = get_token_decimals(web3, token_address)

    # Record balance BEFORE supply
    balance_before_supply = get_token_balance(web3, token_address, funded_wallet)
    print(f"\n  {token} before supply: {format_token_amount(balance_before_supply, decimals)}")

    # --- STEP 1: Supply tokens ---
    supply_intent = SupplyIntent(
        protocol=protocol,
        token=token,
        amount=supply_amount,
        chain=CHAIN_NAME,
        **({"market_id": market_id} if market_id else {}),
    )

    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=rpc_url,
    )

    supply_result = compiler.compile(supply_intent)
    assert supply_result.status.value == "SUCCESS", f"Supply compilation failed: {supply_result.error}"
    assert supply_result.action_bundle is not None

    exec_result = await orchestrator.execute(supply_result.action_bundle, execution_context)
    assert exec_result.success, f"Supply execution failed: {exec_result.error}"
    print(f"  Supply of {supply_amount} {token} to {protocol}: SUCCESS")

    # Verify supply: balance should have decreased
    balance_after_supply = get_token_balance(web3, token_address, funded_wallet)
    supplied_wei = balance_before_supply - balance_after_supply
    assert supplied_wei > 0, "Token balance must decrease after supply"
    print(f"  {token} after supply: {format_token_amount(balance_after_supply, decimals)}")

    # --- STEP 2: Withdraw with amount="all" ---
    withdraw_intent = WithdrawIntent(
        protocol=protocol,
        token=token,
        amount="all",  # THE KEY TEST: amount="all" must work!
        chain=CHAIN_NAME,
        **({"market_id": market_id} if market_id else {}),
    )

    # Compile — this is where the amount resolver kicks in
    withdraw_result = compiler.compile(withdraw_intent)
    assert withdraw_result.status.value == "SUCCESS", (
        f"Withdraw(amount='all') compilation failed for {protocol}: {withdraw_result.error}"
    )
    assert withdraw_result.action_bundle is not None
    print(f"  Withdraw(amount='all') from {protocol}: COMPILED SUCCESSFULLY")

    # Execute
    exec_result = await orchestrator.execute(withdraw_result.action_bundle, execution_context)
    assert exec_result.success, f"Withdraw execution failed: {exec_result.error}"
    print(f"  Withdraw(amount='all') from {protocol}: EXECUTED SUCCESSFULLY")

    # Verify balance recovery
    balance_after_withdraw = get_token_balance(web3, token_address, funded_wallet)
    recovered_wei = balance_after_withdraw - balance_after_supply
    print(f"  {token} after withdraw: {format_token_amount(balance_after_withdraw, decimals)}")
    print(f"  Recovered: {format_token_amount(recovered_wei, decimals)}")

    # Allow small dust (1 wei difference from rounding) but most should be recovered
    assert recovered_wei > 0, "Must recover tokens after withdraw(amount='all')"

    # Check that we recovered at least 99.9% of what we supplied (allowing for rounding)
    recovery_ratio = Decimal(recovered_wei) / Decimal(supplied_wei) if supplied_wei > 0 else Decimal("0")
    print(f"  Recovery ratio: {recovery_ratio:.6f}")
    assert recovery_ratio >= Decimal("0.999"), (
        f"Must recover at least 99.9% of supplied amount. Got {recovery_ratio:.6f}"
    )

    return True


# =============================================================================
# Test: Aave V3 withdraw(amount="all")
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lending
class TestWithdrawAmountAllAaveV3:
    """Test withdraw(amount='all') for Aave V3."""

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_aave_v3_withdraw_amount_all(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Supply USDC to Aave V3, then withdraw with amount='all'.

        This is the core test for VIB-2537: the amount resolver should query
        the Aave V3 PoolDataProvider for the current aToken balance and resolve
        amount='all' to a concrete amount.
        """
        print(f"\n{'='*80}")
        print("Test: Aave V3 withdraw(amount='all')")
        print(f"{'='*80}")

        await _supply_then_withdraw_all(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            protocol="aave_v3",
            token="USDC",
            supply_amount=Decimal("100"),
        )
        print("\nAave V3 withdraw(amount='all'): ALL CHECKS PASSED")


# =============================================================================
# Test: Compound V3 withdraw(amount="all")
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lending
class TestWithdrawAmountAllCompoundV3:
    """Test withdraw(amount='all') for Compound V3."""

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_compound_v3_withdraw_amount_all(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Supply USDC to Compound V3, then withdraw with amount='all'.

        Compound V3 is the protocol that caused the uint128 overflow with
        MAX_UINT256. The resolver should query Comet.balanceOf() for the
        actual supply balance.
        """
        print(f"\n{'='*80}")
        print("Test: Compound V3 withdraw(amount='all')")
        print(f"{'='*80}")

        await _supply_then_withdraw_all(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            protocol="compound_v3",
            token="USDC",
            supply_amount=Decimal("100"),
            market_id="usdc",
            rpc_url=anvil_rpc_url,
        )
        print("\nCompound V3 withdraw(amount='all'): ALL CHECKS PASSED")


# =============================================================================
# Test: Spark withdraw(amount="all") [Aave V3 fork]
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lending
class TestWithdrawAmountAllSpark:
    """Test withdraw(amount='all') for Spark (Aave V3 fork)."""

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_spark_withdraw_amount_all(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
    ):
        """Supply USDC to Spark, then withdraw with amount='all'.

        Spark is an Aave V3 fork — the AaveV3BalanceReader should work
        for it via the shared LendingPositionReader infrastructure.
        """
        from almanak.framework.connectors.spark import SPARK_POOL_ADDRESSES

        if CHAIN_NAME not in SPARK_POOL_ADDRESSES:
            pytest.skip(f"Spark not available on {CHAIN_NAME}")

        print(f"\n{'='*80}")
        print("Test: Spark withdraw(amount='all')")
        print(f"{'='*80}")

        await _supply_then_withdraw_all(
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            protocol="spark",
            token="USDC",
            supply_amount=Decimal("100"),
        )
        print("\nSpark withdraw(amount='all'): ALL CHECKS PASSED")
