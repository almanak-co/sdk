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
from almanak.gateway.proto import gateway_pb2


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
    # rpc_urls is retained only for EnsoStateProvider wiring — the runner no
    # longer falls back to direct Web3 for source-TX verification; the gateway
    # client is mocked into the verification path instead.
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


def _make_gateway_client_confirmed() -> MagicMock:
    """Return a mock gateway client whose GetTransactionStatus reports confirmed."""
    client = MagicMock()
    status_response = SimpleNamespace(status="confirmed", block_number=123)
    client.execution.GetTransactionStatus = MagicMock(return_value=status_response)
    return client


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

        # Gateway-only boundary: source-TX verification goes through
        # gateway.execution.GetTransactionStatus. No direct Web3 is permitted.
        gateway_client = _make_gateway_client_confirmed()

        with (
            patch.object(runner, "_load_execution_progress", new_callable=AsyncMock, return_value=None),
            patch.object(runner, "_save_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_clear_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_get_gateway_client", return_value=gateway_client),
            patch.object(runner, "_record_success"),
            patch.object(runner, "_calculate_duration_ms", return_value=100),
            patch(
                "almanak.framework.runner.strategy_runner.EnsoStateProvider",
                return_value=mock_state_provider,
            ),
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

        # Gateway-only boundary pin: source-TX verification MUST go through the
        # gateway's GetTransactionStatus RPC with the correct tx_hash and source
        # chain. If this call ever disappears (e.g. the verification becomes a
        # no-op), the register_bridge_transfer assertion above would still pass
        # but we would have silently dropped the trust boundary check.
        assert gateway_client.execution.GetTransactionStatus.called, (
            "Source-TX verification must call gateway.execution.GetTransactionStatus; "
            "without it, the gateway-only boundary is not actually exercised."
        )
        status_request = gateway_client.execution.GetTransactionStatus.call_args.args[0]
        assert isinstance(status_request, gateway_pb2.TxStatusRequest)
        assert status_request.tx_hash == "0x" + "ab" * 32
        assert status_request.chain == "base"


class TestBridgeWaitFailsFastWithoutGateway:
    """Fix #1647: missing gateway_client must fail-fast BEFORE any TX is submitted.

    Prior behaviour constructed a raw ``Web3(HTTPProvider(url))`` and polled
    directly when ``state.gateway_client`` was ``None``. That bypassed the
    gateway-only trust boundary documented in
    ``blueprints/20-gateway-security-architecture.md``. The fix raises
    ``RuntimeError`` at ``_init_bridge_wait_state`` time so the runner never
    submits a cross-chain source transaction it cannot later verify.
    """

    @pytest.mark.asyncio
    async def test_missing_gateway_client_raises_before_execute(self):
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

        with (
            patch.object(runner, "_load_execution_progress", new_callable=AsyncMock, return_value=None),
            patch.object(runner, "_save_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_clear_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_get_gateway_client", return_value=None),
            patch(
                "almanak.framework.runner.strategy_runner.EnsoStateProvider",
                return_value=mock_state_provider,
            ),
        ):
            with pytest.raises(RuntimeError, match="Gateway client required"):
                await runner._execute_with_bridge_waiting(
                    strategy=strategy,
                    intents=[intent],
                    orchestrator=orchestrator,
                    start_time=datetime.now(UTC),
                )

        # Critical: no source transaction may be submitted when the gateway
        # client is missing. The fail-fast guard must run BEFORE any call to
        # ``orchestrator.execute``, otherwise we would broadcast funds we
        # cannot verify.
        orchestrator.execute.assert_not_called()
        # And the bridge tracker must never be wired up either.
        mock_state_provider.register_bridge_transfer.assert_not_called()
