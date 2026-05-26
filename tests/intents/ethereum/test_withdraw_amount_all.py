"""Tests for amount='all' resolution in withdraw intents on Ethereum.

Tests Morpho Blue withdraw(amount="all") to validate the amount resolver
works for protocols that use shares-based withdrawal.

Ticket: VIB-2537

To run:
    uv run pytest tests/intents/ethereum/test_withdraw_amount_all.py -v -s
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.morpho_blue.adapter import MORPHO_MARKETS
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

CHAIN_NAME = "ethereum"
MORPHO_MARKET_NAME = "wstETH/USDC"


def _select_market_id(chain: str, market_name: str) -> str:
    markets = MORPHO_MARKETS.get(chain, {})
    for market_id, info in markets.items():
        if info.get("name") == market_name:
            return market_id
    raise AssertionError(f"Expected Morpho market '{market_name}' to exist for chain='{chain}'")


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    return ExecutionContext(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        simulation_enabled=True,
    )


@pytest.mark.ethereum
@pytest.mark.lending
class TestWithdrawAmountAllMorphoBlue:
    """Test withdraw(amount='all') for Morpho Blue on Ethereum.

    Morpho Blue is unique because it uses shares-based withdrawal:
    MAX_UINT256 overflows Morpho's internal mulDiv, so the adapter
    queries on-chain position and withdraws exact shares.

    The amount resolver should detect Morpho and set withdraw_all=True,
    delegating to the adapter's existing shares-based path.
    """

    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_morpho_blue_withdraw_amount_all(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ):
        """Supply wstETH collateral to Morpho Blue, then withdraw with amount='all'."""
        market_id = _select_market_id(CHAIN_NAME, MORPHO_MARKET_NAME)
        market_info = MORPHO_MARKETS[CHAIN_NAME][market_id]

        # Morpho Blue wstETH/USDC: collateral is wstETH
        collateral_token = market_info["collateral_token"]  # wstETH
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]

        # Find the collateral token address. wstETH is in CHAIN_CONFIGS for
        # ethereum at slot 0 (verified 2026-05-26 via eth_getStorageAt probe);
        # missing entries or zero balance here mean the funded_wallet seeding
        # silently failed — fail loudly instead of skipping (VIB-4824).
        token_address = tokens.get(collateral_token) or tokens.get("wstETH")
        assert token_address, (
            f"Token {collateral_token} missing from CHAIN_CONFIGS[{CHAIN_NAME!r}]['tokens'] — "
            "add the address before running this test"
        )

        decimals = get_token_decimals(web3, token_address)

        print(f"\n{'='*80}")
        print(f"Test: Morpho Blue withdraw(amount='all') — {MORPHO_MARKET_NAME}")
        print(f"{'='*80}")

        # Record balance BEFORE supply
        balance_before_supply = get_token_balance(web3, token_address, funded_wallet)
        print(f"  {collateral_token} before supply: {format_token_amount(balance_before_supply, decimals)}")

        assert balance_before_supply > 0, (
            f"{collateral_token} funding produced zero balance — investigate "
            f"CHAIN_CONFIGS[{CHAIN_NAME!r}]['balance_slots'][{collateral_token!r}] "
            "or anvil_setStorageAt"
        )

        supply_amount = Decimal("0.01")  # Small amount of wstETH

        # --- STEP 1: Supply collateral ---
        supply_intent = SupplyIntent(
            protocol="morpho_blue",
            token=collateral_token,
            amount=supply_amount,
            chain=CHAIN_NAME,
            market_id=market_id,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        supply_result = compiler.compile(supply_intent)
        assert supply_result.status.value == "SUCCESS", f"Supply compilation failed: {supply_result.error}"
        assert supply_result.action_bundle is not None

        exec_result = await orchestrator.execute(supply_result.action_bundle, execution_context)
        assert exec_result.success, f"Supply execution failed: {exec_result.error}"
        print(f"  Supply of {supply_amount} {collateral_token} to Morpho Blue: SUCCESS")

        balance_after_supply = get_token_balance(web3, token_address, funded_wallet)
        supplied_wei = balance_before_supply - balance_after_supply
        assert supplied_wei > 0, "Token balance must decrease after supply"

        # --- STEP 2: Withdraw with amount="all" ---
        withdraw_intent = WithdrawIntent(
            protocol="morpho_blue",
            token=collateral_token,
            amount="all",  # THE KEY TEST
            chain=CHAIN_NAME,
            market_id=market_id,
        )

        withdraw_result = compiler.compile(withdraw_intent)
        assert withdraw_result.status.value == "SUCCESS", (
            f"Withdraw(amount='all') compilation failed for Morpho Blue: {withdraw_result.error}"
        )
        assert withdraw_result.action_bundle is not None
        print("  Withdraw(amount='all') from Morpho Blue: COMPILED SUCCESSFULLY")

        exec_result = await orchestrator.execute(withdraw_result.action_bundle, execution_context)
        assert exec_result.success, f"Withdraw execution failed: {exec_result.error}"
        print("  Withdraw(amount='all') from Morpho Blue: EXECUTED SUCCESSFULLY")

        # Verify balance recovery
        balance_after_withdraw = get_token_balance(web3, token_address, funded_wallet)
        recovered_wei = balance_after_withdraw - balance_after_supply
        print(f"  {collateral_token} after withdraw: {format_token_amount(balance_after_withdraw, decimals)}")
        print(f"  Recovered: {format_token_amount(recovered_wei, decimals)}")

        assert recovered_wei > 0, "Must recover tokens after withdraw(amount='all')"

        # Morpho uses shares, so recovery should be very close to exact
        recovery_ratio = Decimal(recovered_wei) / Decimal(supplied_wei)
        print(f"  Recovery ratio: {recovery_ratio:.6f}")
        assert recovery_ratio >= Decimal("0.999"), (
            f"Must recover at least 99.9% of supplied amount. Got {recovery_ratio:.6f}"
        )

        print("\nMorpho Blue withdraw(amount='all'): ALL CHECKS PASSED")
