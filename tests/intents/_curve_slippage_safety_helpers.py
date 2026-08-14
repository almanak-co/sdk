"""Shared affected-chain assertions for Curve slippage safety refusals."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from web3 import Web3

from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent, SwapIntent
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.state_machine import IntentStateMachine, RetryConfig, StateMachineConfig
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.runner.strategy_runner import RunnerConfig, SingleChainExecutionState, StrategyRunner
from tests.intents.conftest import get_token_balance
from tests.support.curve_pool_catalog import CURVE_TEST_POOLS

CURVE_SLIPPAGE_SAFETY_POOLS = {
    "arbitrum": "2pool",
    "base": "4pool",
    "ethereum": "3pool",
    "optimism": "crvusd_usdc",
    "polygon": "frxusd_usdt",
}

INVALID_SLIPPAGE_CASES = (
    ("0.005", "must be a Decimal"),
    (Decimal("1"), "must be in [0, 1)"),
)

_CURVE_MONEY_PATH_INTENTS = (IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE)


def _pool_balances(web3: Web3, wallet: str, chain: str, pool_name: str) -> dict[str, int]:
    pool = CURVE_TEST_POOLS[chain][pool_name]
    addresses = [*pool["coin_addresses"], pool["lp_token"]]
    unique_addresses = dict.fromkeys(address.lower() for address in addresses)
    return {address: get_token_balance(web3, address, wallet) for address in unique_addresses}


def _intent_with_invalid_slippage(
    chain: str,
    pool_name: str,
    intent_type: IntentType,
    invalid_slippage: object,
) -> SwapIntent | LPOpenIntent | LPCloseIntent:
    pool = CURVE_TEST_POOLS[chain][pool_name]
    if intent_type == IntentType.SWAP:
        intent = SwapIntent(
            from_token=pool["coin_addresses"][0],
            to_token=pool["coin_addresses"][1],
            amount=Decimal("1"),
            max_slippage=Decimal("0.005"),
            protocol="curve",
            chain=chain,
            swap_params={"pool": pool["address"]},
        )
    elif intent_type == IntentType.LP_OPEN:
        intent = LPOpenIntent(
            pool=pool_name,
            amount0=Decimal("1"),
            amount1=Decimal("1"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            max_slippage=Decimal("0.005"),
            protocol="curve",
            chain=chain,
        )
    elif intent_type == IntentType.LP_CLOSE:
        intent = LPCloseIntent(
            position_id="1",
            pool=pool_name,
            max_slippage=Decimal("0.005"),
            protocol="curve",
            chain=chain,
        )
    else:  # pragma: no cover - the shared test matrix owns the closed set
        raise AssertionError(f"Unsupported Curve safety-test intent type: {intent_type}")

    return intent.model_copy(update={"max_slippage": invalid_slippage})


async def assert_curve_invalid_slippage_refusals(
    *,
    web3: Web3,
    funded_wallet: str,
    anvil_rpc_url: str,
    chain: str,
    orchestrator: Any,
) -> None:
    """Assert malformed Curve intents are terminal before orchestration can submit."""
    pool_name = CURVE_SLIPPAGE_SAFETY_POOLS[chain]
    balances_before = _pool_balances(web3, funded_wallet, chain, pool_name)
    compiler = IntentCompiler(
        chain=chain,
        wallet_address=funded_wallet,
        # Every selected pool is stablecoin-only. Fixed parity prices keep this
        # malformed-input safety test deterministic and avoid an irrelevant
        # external price-service dependency.
        price_oracle={symbol: Decimal("1") for symbol in CURVE_TEST_POOLS[chain][pool_name]["coins"]},
        rpc_url=anvil_rpc_url,
    )
    unused_dependency = SimpleNamespace()
    runner = StrategyRunner(
        price_oracle=unused_dependency,
        balance_provider=unused_dependency,
        execution_orchestrator=orchestrator,
        state_manager=unused_dependency,
        config=RunnerConfig(max_retries=0),
    )

    for invalid_slippage, expected_error in INVALID_SLIPPAGE_CASES:
        for intent_type in _CURVE_MONEY_PATH_INTENTS:
            intent = _intent_with_invalid_slippage(chain, pool_name, intent_type, invalid_slippage)
            result = compiler.compile(intent)
            context = f"{chain} {intent_type.value} invalid_slippage={invalid_slippage!r}"

            assert result.status == CompilationStatus.FAILED, context
            assert result.is_safety_refusal is True, context
            assert result.is_transient is False, context
            assert result.transactions == [], context
            # Layer 2 for a compile-time refusal: no executable bundle may
            # escape the compiler, so the orchestrator cannot submit an
            # approval or money-path transaction.
            assert result.action_bundle is None, context
            assert expected_error in (result.error or ""), context

            # Exercise the production single-chain orchestration driver with the
            # real fork-test orchestrator. A compile-time refusal must terminate
            # without ever producing the ActionBundle required to enter
            # ``orchestrator.execute``. Anvil auto-mines every submission, so an
            # unchanged head independently proves that neither an approval nor a
            # money-path transaction escaped this negative path.
            state_machine = IntentStateMachine(
                intent=intent,
                compiler=compiler,
                config=StateMachineConfig(
                    retry_config=RetryConfig(max_retries=0),
                    emit_metrics=False,
                ),
            )
            state = SingleChainExecutionState(
                strategy=SimpleNamespace(
                    chain=chain,
                    wallet_address=funded_wallet,
                    deployment_id=f"curve-slippage-safety:{chain}",
                ),
                intent=intent,
                start_time=datetime.now(UTC),
                deployment_id=f"curve-slippage-safety:{chain}",
                compiler=compiler,
                state_machine=state_machine,
            )
            block_before = web3.eth.block_number

            assert await runner._single_chain_state_machine_loop(state) is None, context

            assert state_machine.is_complete, context
            assert state_machine.success is False, context
            assert state_machine.refused_by_safety_guard is True, context
            assert state_machine.retry_count == 0, context
            assert state_machine.action_bundle is None, context
            assert state.last_execution_result is None, context
            assert expected_error in (state_machine.error or ""), context
            assert web3.eth.block_number == block_before, context

    assert _pool_balances(web3, funded_wallet, chain, pool_name) == balances_before
