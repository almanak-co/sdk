"""Runner-level regression test for VIB-3223.

Pushes a real `BridgeIntent` through `_execute_with_bridge_waiting` and asserts
that the bridge-wait path forwards the intent's `to_chain`/`token` fields to
`state_provider.register_bridge_transfer()`. Pre-fix, the runner read
`destination_chain`/`to_token` (only present on Enso `SwapIntent`), so native
bridges silently skipped the wait — this test pins the fixed behavior at the
call site where the bug actually lived.
"""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.intents import BridgeIntent
from almanak.framework.runner.strategy_runner import (
    RunnerConfig,
    StrategyRunner,
)


def _make_runner() -> StrategyRunner:
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=MagicMock(),
        config=RunnerConfig(
            default_interval_seconds=0,
            enable_state_persistence=False,
        ),
    )


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.strategy_id = "test-bridge-dest-fields"
    strategy.chain = "base"
    strategy.wallet_address = "0x" + "a1" * 20
    strategy.on_intent_executed = MagicMock()
    strategy.save_state = MagicMock()
    return strategy


def _make_orchestrator(tx_hash: str = "0x" + "ab" * 32) -> MagicMock:
    orch = MagicMock()
    orch.wallet_address = "0x" + "a1" * 20
    orch.primary_chain = "base"
    # Provide rpc_urls so _execute_with_bridge_waiting's Web3 fallback doesn't
    # short-circuit with "No RPC URL configured".
    orch._config = SimpleNamespace(rpc_urls={"base": "http://mock.rpc"})

    result = MagicMock()
    result.success = True
    result.error = None
    result.tx_result = SimpleNamespace(
        tx_hash=tx_hash,
        actual_amount_received=Decimal("100"),
    )
    orch.execute = AsyncMock(return_value=result)
    return orch


class TestBridgeIntentDestinationFields:
    """VIB-3223: bridge-wait path must resolve BridgeIntent's to_chain/token."""

    @pytest.mark.asyncio
    async def test_register_bridge_transfer_uses_to_chain_not_destination_chain(self):
        runner = _make_runner()
        strategy = _make_strategy()
        orchestrator = _make_orchestrator()

        intent = BridgeIntent(
            token="USDC",
            amount=Decimal("100"),
            from_chain="base",
            to_chain="arbitrum",
        )

        mock_state_provider = MagicMock()
        mock_state_provider.register_bridge_transfer = MagicMock(return_value="deposit-id-1")
        mock_state_provider.wait_for_bridge_completion = AsyncMock(
            return_value={"status": "completed", "balance_increase": 0}
        )

        # Mock Web3 so source-TX verification succeeds immediately with status=1.
        mock_web3_instance = MagicMock()
        mock_web3_instance.eth.get_transaction_receipt.return_value = {
            "status": 1,
            "blockNumber": 123,
        }
        mock_web3_class = MagicMock(return_value=mock_web3_instance)
        mock_web3_class.HTTPProvider = MagicMock()

        with (
            patch.object(runner, "_load_execution_progress", new_callable=AsyncMock, return_value=None),
            patch.object(runner, "_save_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_clear_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_get_gateway_client", return_value=None),
            patch.object(runner, "_record_success"),
            patch.object(runner, "_calculate_duration_ms", return_value=100),
            patch(
                "almanak.framework.runner.strategy_runner.EnsoStateProvider",
                return_value=mock_state_provider,
            ),
            patch("web3.Web3", mock_web3_class),
        ):
            await runner._execute_with_bridge_waiting(
                strategy=strategy,
                intents=[intent],
                orchestrator=orchestrator,
                start_time=datetime.now(UTC),
            )

        # The regression assertion: the runner must resolve the destination
        # from BridgeIntent.to_chain (not destination_chain) and the token from
        # BridgeIntent.token (not to_token — there is no to_token on a bridge).
        assert mock_state_provider.register_bridge_transfer.called, (
            "Bridge-wait path never registered a transfer for BridgeIntent — "
            "the VIB-3223 bug (silently skipped when destination fields missing)."
        )
        call_kwargs = mock_state_provider.register_bridge_transfer.call_args.kwargs
        assert call_kwargs["source_chain"] == "base"
        assert call_kwargs["destination_chain"] == "arbitrum", (
            "destination_chain should come from BridgeIntent.to_chain"
        )
        assert call_kwargs["token_symbol"] == "USDC", (
            "token_symbol should come from BridgeIntent.token (not to_token)"
        )
