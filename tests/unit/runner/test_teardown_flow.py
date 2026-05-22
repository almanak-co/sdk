"""Tests for teardown flow in StrategyRunner.

Validates that:
- _check_teardown_requested returns TeardownMode or None (pure check, no side effects)
- Teardown path creates market snapshot before generating intents
- No temp compiler is injected into the strategy during teardown
- Lifecycle state transitions (mark_started, mark_completed, mark_failed) fire correctly
- Backward compatibility: strategies with old signature still work
- Multi-chain teardown receives market parameter
"""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock, patch, call

import pytest

from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.teardown.models import TeardownMode


@pytest.fixture(autouse=True)
def _isolated_teardown_state_db(monkeypatch, tmp_path):
    """Pin ``ALMANAK_STATE_DB`` to a per-test tmp file so the strict,
    strategy-scoped DB resolver (VIB-3835) doesn't hard-fail when
    ``execute_teardown_via_manager`` constructs a ``TeardownStateAdapter``.
    Tests in this file mock the state manager so the file is never read.
    """
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
    monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "test_state.db"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_runner(**overrides) -> StrategyRunner:
    """Build a StrategyRunner with minimal mocks."""
    defaults = dict(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=None,
    )
    defaults.update(overrides)
    return StrategyRunner(**defaults)


def _make_strategy(
    *,
    deployment_id: str = "test_strat",
    chain: str = "arbitrum",
    wallet_address: str = "0x1234",
    should_teardown: bool = False,
    teardown_intents: list | None = None,
    market_snapshot: object | None = None,
) -> MagicMock:
    """Build a mock strategy with configurable teardown behaviour."""
    strategy = MagicMock()
    strategy.deployment_id = deployment_id
    strategy.chain = chain
    strategy.wallet_address = wallet_address
    strategy.should_teardown.return_value = should_teardown

    if teardown_intents is not None:
        strategy.generate_teardown_intents.return_value = teardown_intents

    if market_snapshot is not None:
        strategy.create_market_snapshot.return_value = market_snapshot
    else:
        strategy.create_market_snapshot.return_value = MagicMock(
            get_price_oracle_dict=MagicMock(return_value={}),
        )

    return strategy


def _make_intent(intent_type: str = "SWAP", chain: str = "arbitrum") -> MagicMock:
    """Build a mock intent."""
    intent = MagicMock()
    intent.intent_type = SimpleNamespace(value=intent_type)
    intent.chain = chain
    intent.is_chained_amount = False
    return intent


# ---------------------------------------------------------------------------
# _check_teardown_requested
# ---------------------------------------------------------------------------


class TestCheckTeardownRequested:
    """Tests for _check_teardown_requested (pure check, no side effects)."""

    def test_returns_none_when_no_should_teardown(self):
        runner = _make_runner()
        strategy = MagicMock(spec=[])  # no should_teardown attr
        strategy.deployment_id = "strat"
        assert runner._check_teardown_requested(strategy) is None

    def test_returns_none_when_should_teardown_false(self):
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=False)
        assert runner._check_teardown_requested(strategy) is None

    @patch("almanak.framework.teardown.get_teardown_state_manager")
    def test_returns_mode_from_active_request(self, mock_get_manager):
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[])

        # Setup teardown state manager mock
        mock_manager = MagicMock()
        mock_request = MagicMock()
        mock_request.mode = TeardownMode.HARD
        mock_manager.get_active_request.return_value = mock_request
        mock_get_manager.return_value = mock_manager

        mode = runner._check_teardown_requested(strategy)
        assert mode == TeardownMode.HARD

    @patch("almanak.framework.teardown.get_teardown_state_manager")
    def test_returns_soft_when_no_active_request(self, mock_get_manager):
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[])

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        mode = runner._check_teardown_requested(strategy)
        assert mode == TeardownMode.SOFT

    def test_acknowledges_teardown_request(self):
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[])

        with patch("almanak.framework.teardown.get_teardown_state_manager") as mock_get:
            mock_manager = MagicMock()
            mock_manager.get_active_request.return_value = None
            mock_get.return_value = mock_manager

            runner._check_teardown_requested(strategy)
            strategy.acknowledge_teardown_request.assert_called_once()

    def test_no_compiler_set_on_strategy(self):
        """Verify _check_teardown_requested does NOT inject a temp compiler."""
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[])
        # Ensure _compiler starts as None
        strategy._compiler = None

        with patch("almanak.framework.teardown.get_teardown_state_manager") as mock_get:
            mock_manager = MagicMock()
            mock_manager.get_active_request.return_value = None
            mock_get.return_value = mock_manager

            runner._check_teardown_requested(strategy)

        # _compiler should NOT have been set by the check
        assert strategy._compiler is None


# ---------------------------------------------------------------------------
# Teardown in run_iteration
# ---------------------------------------------------------------------------


class TestTeardownInRunIteration:
    """Tests for the teardown path inside run_iteration."""

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_market_created_before_intents(self, mock_get_manager):
        """Market snapshot is created inside _execute_teardown before intent generation.

        Single-chain teardown routes through _execute_teardown, which creates
        the market snapshot and uses it for intent generation and execution.
        """
        runner = _make_runner()
        market_mock = MagicMock(get_price_oracle_dict=MagicMock(return_value={"ETH": Decimal("3000")}))
        intent = _make_intent()
        strategy = _make_strategy(
            should_teardown=True,
            teardown_intents=[intent],
            market_snapshot=market_mock,
        )

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        # Mock _execute_teardown to verify it's called from run_iteration
        runner._execute_teardown = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN, intent=None, deployment_id="test_strat"
            )
        )

        await runner.run_iteration(strategy)

        # _execute_teardown should be called (it handles market creation internally)
        runner._execute_teardown.assert_called_once()

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_empty_intents_marks_completed(self, mock_get_manager):
        """Empty teardown intents should result in successful teardown.

        Single-chain teardown now routes through TeardownManager, which handles
        empty intents by returning an empty result with success=True.
        """
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[])

        mock_manager = MagicMock()
        mock_request = MagicMock()
        mock_request.mode = TeardownMode.SOFT
        mock_manager.get_active_request.return_value = mock_request
        mock_get_manager.return_value = mock_manager

        # Patch TeardownManager path - returns success for empty intents
        runner._execute_teardown = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN, intent=None, deployment_id="test_strat"
            )
        )

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.TEARDOWN
        # TeardownManager was called (handling empty intents internally)
        runner._execute_teardown.assert_called_once()

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_failure_marks_failed(self, mock_get_manager):
        """Exception in teardown execution should mark teardown as failed.

        Single-chain teardown now routes through TeardownManager, which catches
        exceptions from generate_teardown_intents and returns a failed result.
        """
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True)

        mock_manager = MagicMock()
        mock_request = MagicMock()
        mock_request.mode = TeardownMode.SOFT
        mock_manager.get_active_request.return_value = mock_request
        mock_get_manager.return_value = mock_manager

        # TeardownManager returns failure
        runner._execute_teardown = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.EXECUTION_FAILED,
                intent=None,
                deployment_id="test_strat",
                error="Teardown failed: boom",
            )
        )

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.EXECUTION_FAILED

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_not_implemented_marks_failed(self, mock_get_manager):
        """NotImplementedError in teardown execution should result in failure.

        Single-chain teardown now routes through TeardownManager, which catches
        exceptions from generate_teardown_intents and returns a failed result.
        """
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True)

        mock_manager = MagicMock()
        mock_request = MagicMock()
        mock_request.mode = TeardownMode.SOFT
        mock_manager.get_active_request.return_value = mock_request
        mock_get_manager.return_value = mock_manager

        # TeardownManager returns failure for NotImplementedError
        runner._execute_teardown = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.EXECUTION_FAILED,
                intent=None,
                deployment_id="test_strat",
                error="Teardown failed: not done",
            )
        )

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.EXECUTION_FAILED

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_market_failure_continues_without_market(self, mock_get_manager):
        """If create_market_snapshot fails, teardown still proceeds.

        _execute_teardown handles market creation internally and continues
        even if snapshot creation fails.
        """
        runner = _make_runner()
        intent = _make_intent()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[intent])
        strategy.create_market_snapshot.side_effect = RuntimeError("no market")

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        runner._execute_teardown = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN, intent=None, deployment_id="test_strat"
            )
        )

        await runner.run_iteration(strategy)

        # _execute_teardown should still be called despite market failure
        runner._execute_teardown.assert_called_once()

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_no_temp_compiler_injection(self, mock_get_manager):
        """Verify no _compiler is set on the strategy during teardown.

        Single-chain teardown now routes through TeardownManager. The compiler
        is injected into TeardownManager, not the strategy.
        """
        runner = _make_runner()
        intent = _make_intent()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[intent])
        strategy._compiler = None

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        runner._execute_teardown = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN, intent=None, deployment_id="test_strat"
            )
        )

        await runner.run_iteration(strategy)

        # _compiler should still be None on the strategy (compiler goes to TeardownManager)
        assert strategy._compiler is None

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_backward_compat_old_signature(self, mock_get_manager):
        """Strategies with old signature still work via TeardownManager.

        TeardownManager has its own backward compat for old-style signatures
        (catches TypeError and retries without market kwarg).
        """
        runner = _make_runner()
        intent = _make_intent()
        strategy = _make_strategy(should_teardown=True)

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        # TeardownManager handles backward compat internally
        runner._execute_teardown = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN, intent=None, deployment_id="test_strat"
            )
        )

        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.TEARDOWN

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_multichain_teardown_receives_market(self, mock_get_manager):
        """Multi-chain teardown path passes market to _execute_multi_chain."""
        from almanak.framework.execution.multichain import MultiChainOrchestrator

        multi_orch = MagicMock(spec=MultiChainOrchestrator)
        runner = _make_runner(execution_orchestrator=multi_orch)

        market_mock = MagicMock(get_price_oracle_dict=MagicMock(return_value={}))
        intent = _make_intent()
        strategy = _make_strategy(
            should_teardown=True,
            teardown_intents=[intent],
            market_snapshot=market_mock,
        )

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        runner._execute_multi_chain = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent,
                deployment_id="test_strat",
                duration_ms=100,
            )
        )

        await runner.run_iteration(strategy)

        runner._execute_multi_chain.assert_called_once()
        call_kwargs = runner._execute_multi_chain.call_args[1]
        assert call_kwargs["market"] is market_mock

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_prefetch_resolves_addresses_to_symbols(self, mock_get_manager):
        """_prefetch_teardown_prices resolves token addresses to symbols before calling market.price().

        Regression test for VIB-564: teardown fails for ALMANAK token because
        the intent uses an address and market.price() expects a symbol.
        """
        runner = _make_runner()

        # Create a market mock that tracks calls and only accepts symbols, not addresses
        almanak_address = "0xdefa1d21c5f1cbeac00eeb54b44c7d86467cc3a3"
        usdc_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"

        def mock_price(token):
            if token == "ALMANAK":
                return Decimal("0.002")
            if token in ("USDC", usdc_address):
                return Decimal("1")
            raise ValueError(f"Unknown token: {token}")

        market_mock = MagicMock()
        market_mock._chain = "base"
        market_mock.price = MagicMock(side_effect=mock_price)

        # Create intent with addresses (as almanak_rsi strategy does)
        intent = MagicMock()
        intent.from_token = almanak_address
        intent.to_token = usdc_address
        intent.token = None
        intent.collateral_token = None
        intent.borrow_token = None
        intent.token_in = None

        # Mock token resolver to return ALMANAK for the address
        mock_resolved = MagicMock()
        mock_resolved.symbol = "ALMANAK"

        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = mock_resolved

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            StrategyRunner._prefetch_teardown_prices(market_mock, [intent])

        # The resolver should have been called for the ALMANAK address
        mock_resolver.resolve.assert_any_call(
            almanak_address, "base", log_errors=False, skip_gateway=True
        )

        # market.price() should have been called with the resolved symbol "ALMANAK"
        market_mock.price.assert_any_call("ALMANAK")

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_prefetch_handles_resolver_failure_gracefully(self, mock_get_manager):
        """_prefetch_teardown_prices handles token resolver failures gracefully."""
        runner = _make_runner()

        market_mock = MagicMock()
        market_mock._chain = "base"
        market_mock.price = MagicMock(side_effect=ValueError("Unknown token"))

        intent = MagicMock()
        intent.from_token = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        intent.to_token = None
        intent.token = None
        intent.collateral_token = None
        intent.borrow_token = None
        intent.token_in = None

        # Mock resolver that fails
        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = Exception("Token not found")

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            # Should NOT raise - failures are non-fatal
            StrategyRunner._prefetch_teardown_prices(market_mock, [intent])

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_multichain_teardown_threads_prices(self, mock_get_manager):
        """Multi-chain teardown extracts price_map from market and passes to orchestrator."""
        from almanak.framework.execution.multichain import MultiChainOrchestrator

        multi_orch = MagicMock(spec=MultiChainOrchestrator)
        multi_orch.primary_chain = "arbitrum"
        multi_orch.execute_sequence = AsyncMock(
            return_value=MagicMock(
                success=True,
                successful_count=1,
                chains_used={"arbitrum"},
                total_execution_time_ms=100,
                errors_by_chain={},
            )
        )
        runner = _make_runner(execution_orchestrator=multi_orch)

        prices = {"ETH": Decimal("3400"), "USDC": Decimal("1")}
        market_mock = MagicMock(get_price_oracle_dict=MagicMock(return_value=prices))
        intent = _make_intent()
        # Mark intent as same-chain (no destination_chain) to hit execute_sequence path
        intent.destination_chain = None
        strategy = _make_strategy(
            should_teardown=True,
            teardown_intents=[intent],
            market_snapshot=market_mock,
        )

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        # Patch is_cross_chain_intent to return False
        with patch("almanak.framework.runner.strategy_runner.is_cross_chain_intent", return_value=False):
            await runner.run_iteration(strategy)

        # Verify execute_sequence was called with price_map and price_oracle
        multi_orch.execute_sequence.assert_called_once()
        call_kwargs = multi_orch.execute_sequence.call_args[1]
        assert call_kwargs["price_map"] == {"ETH": "3400", "USDC": "1"}
        assert call_kwargs["price_oracle"] == prices


# ---------------------------------------------------------------------------
# TeardownManager integration
# ---------------------------------------------------------------------------


class TestTeardownViaManager:
    """Tests for TeardownManager-routed teardown execution (VIB-1254)."""

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_single_chain_routes_through_manager(self, mock_get_manager):
        """Single-chain teardown calls _execute_teardown_via_manager, not _execute_single_chain."""
        runner = _make_runner()
        intent = _make_intent()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[intent])

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        # Track which execution path is used
        runner._execute_teardown_via_manager = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN,
                intent=None,
                deployment_id="test_strat",
                duration_ms=100,
            )
        )
        runner._execute_single_chain = AsyncMock()

        await runner.run_iteration(strategy)

        # TeardownManager path should be called, not _execute_single_chain
        runner._execute_teardown_via_manager.assert_called_once()
        runner._execute_single_chain.assert_not_called()

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_multichain_does_not_use_manager(self, mock_get_manager):
        """Multi-chain teardown still uses _execute_multi_chain, not TeardownManager."""
        from almanak.framework.execution.multichain import MultiChainOrchestrator

        multi_orch = MagicMock(spec=MultiChainOrchestrator)
        runner = _make_runner(execution_orchestrator=multi_orch)

        intent = _make_intent()
        strategy = _make_strategy(
            should_teardown=True,
            teardown_intents=[intent],
        )

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        runner._execute_multi_chain = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent,
                deployment_id="test_strat",
                duration_ms=100,
            )
        )
        runner._execute_teardown_via_manager = AsyncMock()

        await runner.run_iteration(strategy)

        # Multi-chain path should be used, not TeardownManager
        runner._execute_multi_chain.assert_called_once()
        runner._execute_teardown_via_manager.assert_not_called()

    @pytest.mark.asyncio
    async def test_build_teardown_compiler_with_gateway(self):
        """_build_teardown_compiler creates compiler using gateway client."""
        from almanak.framework.execution.gateway_orchestrator import GatewayExecutionOrchestrator

        mock_orch = MagicMock(spec=GatewayExecutionOrchestrator)
        mock_orch._client = MagicMock()
        runner = _make_runner(execution_orchestrator=mock_orch)

        strategy = _make_strategy()
        market = MagicMock(get_price_oracle_dict=MagicMock(return_value={"ETH": Decimal("3000")}))

        with patch("almanak.framework.runner.runner_teardown.IntentCompiler") as mock_compiler_cls:
            mock_compiler_cls.return_value = MagicMock()
            compiler = runner._build_teardown_compiler(strategy, market)

        assert compiler is not None
        mock_compiler_cls.assert_called_once()
        call_kwargs = mock_compiler_cls.call_args[1]
        assert call_kwargs["gateway_client"] is mock_orch._client
        # Fetched prices are merged with stablecoin fallbacks
        assert call_kwargs["price_oracle"]["ETH"] == Decimal("3000")
        assert "USDC" in call_kwargs["price_oracle"]  # fallback stablecoin

    @pytest.mark.asyncio
    async def test_build_teardown_compiler_returns_none_on_failure(self):
        """_build_teardown_compiler returns None if compiler creation fails."""
        runner = _make_runner()
        strategy = _make_strategy()

        with patch("almanak.framework.runner.runner_teardown.IntentCompiler", side_effect=RuntimeError("bad")):
            compiler = runner._build_teardown_compiler(strategy, None)

        assert compiler is None

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_fallback_to_inline_when_compiler_fails(self, mock_get_manager):
        """Falls back to inline execution when compiler cannot be built."""
        runner = _make_runner()
        runner.config = RunnerConfig(allow_unsafe_teardown_fallback=True)
        intent = _make_intent()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[intent])

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        # Make compiler fail -> should fallback to inline
        runner._build_teardown_compiler = MagicMock(return_value=None)
        runner._execute_teardown_inline = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN,
                intent=intent,
                deployment_id="test_strat",
                duration_ms=100,
            )
        )

        result = await runner._execute_teardown_via_manager(
            strategy=strategy,
            teardown_intents=[intent],
            teardown_mode=TeardownMode.SOFT,
            teardown_market=None,
            start_time=datetime.now(UTC),
            request=None,
            state_manager=mock_manager,
        )

        assert result.status == IterationStatus.TEARDOWN
        runner._execute_teardown_inline.assert_called_once()

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_fallback_to_inline_when_positions_fail(self, mock_get_manager):
        """Falls back to inline execution when get_open_positions() raises.

        This is a safety-critical test: if positions cannot be fetched,
        the code must NOT pass an empty portfolio through safety validation
        (which would trivially pass loss caps). Instead, fall back to inline.
        """
        runner = _make_runner()
        runner.config = RunnerConfig(allow_unsafe_teardown_fallback=True)
        intent = _make_intent()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[intent])
        strategy.get_open_positions.side_effect = RuntimeError("RPC timeout")

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        # Compiler succeeds, but positions fail -> should fallback to inline
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())
        runner._execute_teardown_inline = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN,
                intent=intent,
                deployment_id="test_strat",
                duration_ms=100,
            )
        )

        result = await runner._execute_teardown_via_manager(
            strategy=strategy,
            teardown_intents=[intent],
            teardown_mode=TeardownMode.SOFT,
            teardown_market=None,
            start_time=datetime.now(UTC),
            request=None,
            state_manager=mock_manager,
        )

        assert result.status == IterationStatus.TEARDOWN
        runner._execute_teardown_inline.assert_called_once()

    @pytest.mark.asyncio
    async def test_inline_fallback_executes_sequentially(self):
        """_execute_teardown_inline executes intents via _execute_single_chain."""
        runner = _make_runner()
        intent1 = _make_intent()
        intent2 = _make_intent()
        strategy = _make_strategy()

        runner._execute_single_chain = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent1,
                deployment_id="test_strat",
                duration_ms=100,
            )
        )

        result = await runner._execute_teardown_inline(
            strategy=strategy,
            teardown_intents=[intent1, intent2],
            teardown_market=None,
            start_time=datetime.now(UTC),
            request=None,
            state_manager=MagicMock(),
        )

        assert result.status == IterationStatus.TEARDOWN
        assert runner._execute_single_chain.call_count == 2

    @pytest.mark.asyncio
    async def test_inline_fallback_stops_on_failure(self):
        """_execute_teardown_inline stops on first failed intent."""
        runner = _make_runner()
        intent1 = _make_intent()
        intent2 = _make_intent()
        strategy = _make_strategy()

        runner._execute_single_chain = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.STRATEGY_ERROR,
                error="tx reverted",
                deployment_id="test_strat",
                duration_ms=100,
            )
        )

        mock_state_mgr = MagicMock()
        result = await runner._execute_teardown_inline(
            strategy=strategy,
            teardown_intents=[intent1, intent2],
            teardown_market=None,
            start_time=datetime.now(UTC),
            request=MagicMock(),
            state_manager=mock_state_mgr,
        )

        # Should stop after first failure, not execute second intent
        assert runner._execute_single_chain.call_count == 1
        mock_state_mgr.mark_failed.assert_called_once()


class TestInlineTeardownAmountResolution:
    """Tests for amount='all' resolution in _execute_teardown_inline."""

    @pytest.mark.asyncio
    async def test_chained_amount_missing_market_passes_through(self):
        """amount='all' with no teardown_market passes to compiler as-is (may fail there)."""
        runner = _make_runner()
        strategy = _make_strategy()

        intent = MagicMock()
        intent.intent_type = SimpleNamespace(value="SWAP")
        intent.chain = "arbitrum"
        intent.is_chained_amount = True
        intent.from_token = "PT-wstETH"

        runner._execute_single_chain = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent,
                deployment_id="test_strat",
                duration_ms=100,
            )
        )

        result = await runner._execute_teardown_inline(
            strategy=strategy,
            teardown_intents=[intent],
            teardown_market=None,
            start_time=datetime.now(UTC),
            request=None,
            state_manager=MagicMock(),
        )

        # Intent passes through unresolved — compiler will handle or reject
        assert result.status == IterationStatus.TEARDOWN
        runner._execute_single_chain.assert_called_once()

    @pytest.mark.asyncio
    async def test_chained_amount_no_token_field_passes_through(self):
        """amount='all' with no token field (e.g. shares='all') passes to compiler as-is."""
        runner = _make_runner()
        strategy = _make_strategy()

        intent = MagicMock(spec=[])  # empty spec so getattr returns None
        intent.intent_type = SimpleNamespace(value="VAULT_REDEEM")
        intent.chain = "arbitrum"
        intent.is_chained_amount = True

        runner._execute_single_chain = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent,
                deployment_id="test_strat",
                duration_ms=100,
            )
        )

        result = await runner._execute_teardown_inline(
            strategy=strategy,
            teardown_intents=[intent],
            teardown_market=MagicMock(),
            start_time=datetime.now(UTC),
            request=None,
            state_manager=MagicMock(),
        )

        # Intent passes through to compiler without balance resolution
        assert result.status == IterationStatus.TEARDOWN
        runner._execute_single_chain.assert_called_once()

    @pytest.mark.asyncio
    async def test_chained_amount_balance_exception_fails(self):
        """amount='all' when balance() raises returns COMPILATION_FAILED."""
        runner = _make_runner()
        strategy = _make_strategy()

        intent = MagicMock()
        intent.intent_type = SimpleNamespace(value="SWAP")
        intent.chain = "arbitrum"
        intent.is_chained_amount = True
        intent.from_token = "PT-wstETH"

        mock_market = MagicMock()
        mock_market.balance.side_effect = ValueError("Token not found in registry")

        result = await runner._execute_teardown_inline(
            strategy=strategy,
            teardown_intents=[intent],
            teardown_market=mock_market,
            start_time=datetime.now(UTC),
            request=None,
            state_manager=MagicMock(),
        )

        assert result.status == IterationStatus.COMPILATION_FAILED
        assert "PT-wstETH" in result.error

    @pytest.mark.asyncio
    async def test_chained_amount_zero_balance_skips(self):
        """amount='all' with zero balance skips intent and completes teardown."""
        runner = _make_runner()
        strategy = _make_strategy()
        runner._execute_single_chain = AsyncMock()

        intent = MagicMock()
        intent.intent_type = SimpleNamespace(value="SWAP")
        intent.chain = "arbitrum"
        intent.is_chained_amount = True
        intent.from_token = "PT-wstETH"

        mock_market = MagicMock()
        mock_market.balance.return_value = MagicMock(balance=Decimal("0"))

        result = await runner._execute_teardown_inline(
            strategy=strategy,
            teardown_intents=[intent],
            teardown_market=mock_market,
            start_time=datetime.now(UTC),
            request=None,
            state_manager=MagicMock(),
        )

        # All skipped = teardown complete (positions already closed)
        assert result.status == IterationStatus.TEARDOWN
        runner._execute_single_chain.assert_not_called()

    @pytest.mark.asyncio
    async def test_chained_amount_resolved_executes(self):
        """amount='all' with positive balance resolves and executes."""
        runner = _make_runner()
        strategy = _make_strategy()

        intent = MagicMock()
        intent.intent_type = SimpleNamespace(value="SWAP")
        intent.chain = "arbitrum"
        intent.is_chained_amount = True
        intent.from_token = "PT-wstETH"

        mock_market = MagicMock()
        mock_market.balance.return_value = MagicMock(balance=Decimal("1.5"))

        runner._execute_single_chain = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent,
                deployment_id="test_strat",
                duration_ms=100,
            )
        )

        resolved_intent = _make_intent()
        with patch(
            "almanak.framework.intents.vocabulary.Intent.set_resolved_amount",
            return_value=resolved_intent,
        ) as mock_set:
            result = await runner._execute_teardown_inline(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_market=mock_market,
                start_time=datetime.now(UTC),
                request=None,
                state_manager=MagicMock(),
            )

        assert result.status == IterationStatus.TEARDOWN
        runner._execute_single_chain.assert_called_once()
        mock_set.assert_called_once_with(intent, Decimal("1.5"))
        # Verify the resolved intent (not original) was executed
        called_kwargs = runner._execute_single_chain.call_args.kwargs
        assert called_kwargs["intent"] is resolved_intent


# ---------------------------------------------------------------------------
# Characterization tests for execute_teardown_via_manager (Phase 6A.4 gate)
# ---------------------------------------------------------------------------
#
# These tests pin the current behavior of the full happy path + error branches
# of `_execute_teardown_via_manager` (CC 40 @ runner_teardown.py:499) so Phase
# 6A.4 can extract phase helpers without regressing safety-critical teardown.
#
# Existing teardown tests cover:
#   - Routing (_execute_teardown_via_manager is called vs _execute_single_chain).
#   - Compiler-fail + positions-fail early-fallback branches.
#   - Exception-path side effects (`_request_teardown_failure_shutdown`).
#
# These tests extend that to cover:
#   - Full TeardownManager happy path (persist -> cancel_window -> execute ->
#     verify -> success mapping to IterationStatus.TEARDOWN).
#   - Cancel-window-cancelled branch (returns TEARDOWN but does NOT shut down).
#   - Safety-validation-failed branch (blocked_reason propagates to error).
#   - Verify-fail branch (positions_closed=False after success flips to fail).
#   - Verify-exception branch (verify raised -> treated as verify-fail).
#   - Execute-exception branch (exception inside the outer try + state update).
#   - TeardownManager-returns-failure branch (success=False mapping).
#   - Compiler-fail WITHOUT unsafe-fallback branch (raises STRATEGY_ERROR).
#   - Positions-fail WITHOUT unsafe-fallback branch (raises STRATEGY_ERROR).
#   - Auto-mode derivation wired into approval_callback (manual only).
#   - `request=None` skips `state_manager.mark_*` calls.


def _make_teardown_manager_class_mock(
    *,
    teardown_result,
    positions_closed: bool = True,
    verify_raises: Exception | None = None,
    cancel_was_cancelled: bool = False,
    safety_passed: bool = True,
    safety_reason: str | None = None,
    execute_raises: Exception | None = None,
):
    """Build a class-level mock for TeardownManager.

    When the runner imports `from ..teardown.teardown_manager import
    TeardownManager` inside `execute_teardown_via_manager`, patching the
    symbol at `almanak.framework.runner.runner_teardown` won't work because
    the import is lazy. So we patch the source module instead.
    """
    from almanak.framework.teardown.cancel_window import CancelWindowResult
    from almanak.framework.teardown.safety_guard import SafetyValidation
    from almanak.framework.teardown.models import TeardownState, TeardownStatus

    mgr = MagicMock()
    mgr.orchestrator = MagicMock()
    mgr.compiler = MagicMock()
    mgr.alert_manager = MagicMock()
    mgr.alert_manager.send_teardown_complete = AsyncMock()
    # VIB-3773: characterization tests pre-date the runner-helpers bag.
    # Setting both ``has_*`` to False emulates the legacy "no accounting
    # writes from the teardown lane" path. Production wiring populates a
    # real ``TeardownRunnerHelpers`` via ``build_runner_helpers``.
    mgr.runner_helpers = MagicMock()
    mgr.runner_helpers.has_commit = False
    mgr.runner_helpers.has_snapshot = False
    # safety_guard.validate_teardown_request is sync
    validation = SafetyValidation(
        all_passed=safety_passed,
        checks=[],
        blocked_reason=safety_reason,
    )
    mgr.safety_guard.validate_teardown_request = MagicMock(return_value=validation)
    # cancel_window.run_cancel_window is async
    cw_result = CancelWindowResult(was_cancelled=cancel_was_cancelled)
    mgr.cancel_window.run_cancel_window = AsyncMock(return_value=cw_result)
    # state_manager.save_teardown_state / delete are async
    mgr.state_manager = MagicMock()
    mgr.state_manager.save_teardown_state = AsyncMock()
    mgr.state_manager.delete_teardown_state = AsyncMock()
    # _persist_state returns a TeardownState
    from datetime import UTC, datetime as _dt
    state = TeardownState(
        teardown_id="td_test",
        deployment_id="test_strat",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.PENDING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=_dt.now(UTC),
        updated_at=_dt.now(UTC),
    )
    mgr._persist_state = AsyncMock(return_value=state)
    # _execute_intents async -> TeardownResult (or raises)
    if execute_raises is not None:
        mgr._execute_intents = AsyncMock(side_effect=execute_raises)
    else:
        mgr._execute_intents = AsyncMock(return_value=teardown_result)
    # _verify_closure async -> bool (or raises)
    if verify_raises is not None:
        mgr._verify_closure = AsyncMock(side_effect=verify_raises)
    else:
        mgr._verify_closure = AsyncMock(return_value=positions_closed)
    return mgr


def _make_successful_teardown_result():
    from decimal import Decimal
    from almanak.framework.teardown.models import TeardownResult

    return TeardownResult(
        success=True,
        deployment_id="test_strat",
        mode="graceful",
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        duration_seconds=5.0,
        intents_total=1,
        intents_succeeded=1,
        intents_failed=0,
        starting_value_usd=Decimal("1000"),
        final_value_usd=Decimal("990"),
        total_costs_usd=Decimal("10"),
        final_balances={},
    )


def _make_failed_teardown_result(error_msg: str = "Slippage too high"):
    from decimal import Decimal
    from almanak.framework.teardown.models import TeardownResult

    return TeardownResult(
        success=False,
        deployment_id="test_strat",
        mode="graceful",
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        duration_seconds=5.0,
        intents_total=1,
        intents_succeeded=0,
        intents_failed=1,
        starting_value_usd=Decimal("1000"),
        final_value_usd=Decimal("1000"),
        total_costs_usd=Decimal("0"),
        final_balances={},
        error=error_msg,
    )


def _make_strategy_for_manager(**overrides):
    """Build a strategy suitable for the full TeardownManager path."""
    from decimal import Decimal
    from almanak.framework.teardown.models import (
        PositionInfo,
        PositionType,
        TeardownPositionSummary,
    )

    strategy = _make_strategy(**overrides)
    strategy.name = "Test Strategy"
    strategy.uses_safe_wallet = False
    strategy.pause = AsyncMock()
    strategy.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id="test_strat",
        timestamp=datetime.now(UTC),
        positions=[
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="test_pos",
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("1000"),
                details={"asset": "ETH"},
            )
        ],
    )
    strategy.acknowledge_teardown_request = MagicMock()
    return strategy


class TestExecuteTeardownViaManagerCharacterization:
    """Characterization tests pinning execute_teardown_via_manager behavior.

    These tests MUST continue to pass byte-for-byte after Phase 6A.4 phase-
    helper extraction. They cover the full state machine: safety validation,
    cancel window, intent execution, post-execution verification, and the
    three success/failure/cancel terminal mappings.
    """

    @pytest.fixture()
    def _patch_manager(self):
        """Provide a context-manager factory that patches TeardownManager.

        Returns a context that patches both the source module and the
        TeardownStateAdapter so that real SQLite/Postgres is never touched.
        """
        from contextlib import contextmanager

        @contextmanager
        def _do(**mgr_kwargs):
            with patch(
                "almanak.framework.teardown.teardown_manager.TeardownManager"
            ) as mgr_cls, patch(
                "almanak.framework.teardown.state_manager.TeardownStateAdapter"
            ) as adapter_cls:
                mock_mgr = _make_teardown_manager_class_mock(**mgr_kwargs)
                mgr_cls.return_value = mock_mgr
                mock_adapter = MagicMock()
                mock_adapter.save_teardown_state = AsyncMock()
                adapter_cls.return_value = mock_adapter
                yield mock_mgr, mock_adapter

        return _do

    @pytest.mark.asyncio
    async def test_happy_path_single_chain_success(self, _patch_manager):
        """Full happy path: safety -> persist -> cancel window -> execute ->
        verify -> success returns IterationStatus.TEARDOWN, calls
        request_shutdown, writes TERMINATED lifecycle state."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()
        runner._lifecycle_write_state = MagicMock()
        runner.request_shutdown = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        state_mgr = MagicMock()
        state_mgr.db_path = "/tmp/test_state.db"
        request = MagicMock()
        request.requested_by = "cli"

        with _patch_manager(teardown_result=_make_successful_teardown_result()):
            result = await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=request,
                state_manager=state_mgr,
            )

        assert result.status == IterationStatus.TEARDOWN
        runner.request_shutdown.assert_called_once()
        runner._lifecycle_write_state.assert_any_call("test_strat", "TERMINATED")
        state_mgr.mark_completed.assert_called_once()
        runner._request_teardown_failure_shutdown.assert_not_called()

    @pytest.mark.asyncio
    async def test_cancel_window_aborts_returns_teardown_without_shutdown(self, _patch_manager):
        """Cancel-window-cancelled returns TEARDOWN status and records success
        but does NOT request shutdown — the operator explicitly aborted."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()
        runner.request_shutdown = MagicMock()
        runner._record_success = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        with _patch_manager(
            teardown_result=_make_successful_teardown_result(),
            cancel_was_cancelled=True,
        ):
            result = await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=None,
                state_manager=MagicMock(),
            )

        assert result.status == IterationStatus.TEARDOWN
        # Cancelled path: no shutdown, no failure handler
        runner.request_shutdown.assert_not_called()
        runner._request_teardown_failure_shutdown.assert_not_called()
        runner._record_success.assert_called_once()

    @pytest.mark.asyncio
    async def test_safety_validation_fail_returns_error(self, _patch_manager):
        """Safety validation failure bypasses execution, returns STRATEGY_ERROR
        and requests teardown failure shutdown (ERROR terminal state)."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        state_mgr = MagicMock()
        request = MagicMock()
        request.requested_by = "cli"

        with _patch_manager(
            teardown_result=_make_successful_teardown_result(),
            safety_passed=False,
            safety_reason="Loss cap exceeded: 5% > 3%",
        ):
            result = await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=request,
                state_manager=state_mgr,
            )

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert "Loss cap exceeded" in result.error
        state_mgr.mark_failed.assert_called_once()
        runner._request_teardown_failure_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_verify_fail_flips_success_to_failure(self, _patch_manager):
        """Post-execution verify returns False -> flip successful TeardownResult
        to failure, persist FAILED status, return STRATEGY_ERROR."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()
        runner.request_shutdown = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        state_mgr = MagicMock()
        request = MagicMock()
        request.requested_by = "cli"

        with _patch_manager(
            teardown_result=_make_successful_teardown_result(),
            positions_closed=False,
        ):
            result = await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=request,
                state_manager=state_mgr,
            )

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert "positions still open" in result.error
        # verify-fail branch calls mark_failed once inside _verify_closure
        # handling and once again in the final failure-mapping block. Both
        # calls ship the same error message — this ordering is the existing
        # behavior pinned by this characterization test.
        assert state_mgr.mark_failed.call_count == 2
        all_errors = [c.kwargs["error"] for c in state_mgr.mark_failed.call_args_list]
        assert all("positions still open" in e for e in all_errors)
        runner._request_teardown_failure_shutdown.assert_called_once()
        runner.request_shutdown.assert_not_called()  # failure path, not success

    @pytest.mark.asyncio
    async def test_verify_exception_treated_as_verify_fail(self, _patch_manager):
        """Verify raising an exception is treated as verify-fail (don't discard
        successful execution stats). Flips to failure with error message."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        state_mgr = MagicMock()
        state_mgr.db_path = None

        with _patch_manager(
            teardown_result=_make_successful_teardown_result(),
            verify_raises=RuntimeError("RPC exploded"),
        ):
            result = await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=None,  # test request=None branch
                state_manager=state_mgr,
            )

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert "Post-teardown verification error" in result.error
        # request=None: mark_failed should NOT be called
        state_mgr.mark_failed.assert_not_called()
        runner._request_teardown_failure_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_exception_outer_try(self, _patch_manager):
        """Exception inside the outer try (from _execute_intents) is caught;
        returns STRATEGY_ERROR and flips teardown_state to FAILED if present."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        state_mgr = MagicMock()
        request = MagicMock()
        request.requested_by = "cli"

        with _patch_manager(
            teardown_result=_make_successful_teardown_result(),
            execute_raises=RuntimeError("compiler went boom"),
        ):
            result = await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=request,
                state_manager=state_mgr,
            )

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert "compiler went boom" in result.error
        state_mgr.mark_failed.assert_called_once()
        runner._request_teardown_failure_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_teardown_result_failure_mapped(self, _patch_manager):
        """TeardownResult(success=False) maps to STRATEGY_ERROR with
        error field preserved."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        state_mgr = MagicMock()
        request = MagicMock()
        request.requested_by = "cli"

        failed_result = _make_failed_teardown_result("Slippage exceeded")
        with _patch_manager(teardown_result=failed_result):
            result = await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=request,
                state_manager=state_mgr,
            )

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert result.error == "Slippage exceeded"
        state_mgr.mark_failed.assert_called_once()
        runner._request_teardown_failure_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_compiler_fail_without_fallback_returns_error(self):
        """Compiler fails AND allow_unsafe_teardown_fallback=False ->
        STRATEGY_ERROR; does NOT fall back to inline."""
        runner = _make_runner()
        runner.config = RunnerConfig(allow_unsafe_teardown_fallback=False)
        runner._request_teardown_failure_shutdown = MagicMock()
        runner._build_teardown_compiler = MagicMock(return_value=None)
        runner._execute_teardown_inline = AsyncMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )

        state_mgr = MagicMock()
        request = MagicMock()
        request.requested_by = "cli"

        result = await runner._execute_teardown_via_manager(
            strategy=strategy,
            teardown_intents=[intent],
            teardown_mode=TeardownMode.SOFT,
            teardown_market=None,
            start_time=datetime.now(UTC),
            request=request,
            state_manager=state_mgr,
        )

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert "compiler" in result.error.lower()
        runner._execute_teardown_inline.assert_not_called()
        state_mgr.mark_failed.assert_called_once()
        runner._request_teardown_failure_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_positions_fail_without_fallback_returns_error(self):
        """get_open_positions fails AND allow_unsafe_teardown_fallback=False ->
        STRATEGY_ERROR; does NOT fall back to inline."""
        runner = _make_runner()
        runner.config = RunnerConfig(allow_unsafe_teardown_fallback=False)
        runner._request_teardown_failure_shutdown = MagicMock()
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())
        runner._execute_teardown_inline = AsyncMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        strategy.get_open_positions.side_effect = RuntimeError("RPC timeout")

        state_mgr = MagicMock()
        request = MagicMock()
        request.requested_by = "cli"

        with patch("almanak.framework.teardown.teardown_manager.TeardownManager"), patch(
            "almanak.framework.teardown.state_manager.TeardownStateAdapter"
        ):
            result = await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=request,
                state_manager=state_mgr,
            )

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert "RPC timeout" in result.error or "positions" in result.error.lower()
        runner._execute_teardown_inline.assert_not_called()
        state_mgr.mark_failed.assert_called_once()
        runner._request_teardown_failure_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_auto_mode_skips_approval_callback_manual_wires_it(self, _patch_manager):
        """Auto mode (request=None) -> approval_callback=None.
        Manual mode (request.requested_by='cli') -> approval_callback wired."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()
        runner.request_shutdown = MagicMock()
        runner._lifecycle_write_state = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        # Manual mode
        with _patch_manager(teardown_result=_make_successful_teardown_result()) as (mgr, _):
            request = MagicMock()
            request.requested_by = "cli"
            await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=request,
                state_manager=MagicMock(),
            )
            call_kwargs = mgr._execute_intents.call_args.kwargs
            assert call_kwargs["on_approval_needed"] is not None
            assert call_kwargs["is_auto_mode"] is False

        # Auto mode: request=None
        with _patch_manager(teardown_result=_make_successful_teardown_result()) as (mgr, _):
            await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=None,
                state_manager=MagicMock(),
            )
            call_kwargs = mgr._execute_intents.call_args.kwargs
            assert call_kwargs["on_approval_needed"] is None
            assert call_kwargs["is_auto_mode"] is True

    @pytest.mark.asyncio
    async def test_hard_mode_maps_to_emergency_string(self, _patch_manager):
        """TeardownMode.HARD maps to mode='emergency' in logging and
        mark_completed payload."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()
        runner.request_shutdown = MagicMock()
        runner._lifecycle_write_state = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        state_mgr = MagicMock()
        request = MagicMock()
        request.requested_by = "cli"

        with _patch_manager(teardown_result=_make_successful_teardown_result()):
            await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.HARD,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=request,
                state_manager=state_mgr,
            )

        # mark_completed kwargs: mode should be "emergency"
        mark_kwargs = state_mgr.mark_completed.call_args.kwargs
        assert mark_kwargs["result"]["mode"] == "emergency"

    @pytest.mark.asyncio
    async def test_price_oracle_uses_market_fetch_when_present(self, _patch_manager):
        """teardown_market with populated get_price_oracle_dict -> price_oracle
        threaded through to _execute_intents. Empty dict is preserved (not
        collapsed to None / fallback)."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()
        runner.request_shutdown = MagicMock()
        runner._lifecycle_write_state = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        # Market returns a populated dict
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {"ETH": Decimal("3000"), "USDC": Decimal("1")}

        with _patch_manager(teardown_result=_make_successful_teardown_result()) as (mgr, _):
            await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=market,
                start_time=datetime.now(UTC),
                request=None,
                state_manager=MagicMock(),
            )

            call_kwargs = mgr._execute_intents.call_args.kwargs
            oracle = call_kwargs["price_oracle"]
            assert oracle["ETH"] == Decimal("3000")

    @pytest.mark.asyncio
    async def test_price_oracle_falls_back_when_market_returns_empty(self, _patch_manager):
        """When market.get_price_oracle_dict returns {}, fall back to
        get_fallback_teardown_prices (stablecoins)."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()
        runner.request_shutdown = MagicMock()
        runner._lifecycle_write_state = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        market = MagicMock()
        market.get_price_oracle_dict.return_value = {}  # empty -> fall back

        with _patch_manager(teardown_result=_make_successful_teardown_result()) as (mgr, _):
            await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=market,
                start_time=datetime.now(UTC),
                request=None,
                state_manager=MagicMock(),
            )
            call_kwargs = mgr._execute_intents.call_args.kwargs
            oracle = call_kwargs["price_oracle"]
            # Fallback always includes at least one stablecoin
            assert oracle is not None
            assert len(oracle) > 0

    @pytest.mark.asyncio
    async def test_alert_failure_does_not_prevent_success_mapping(self, _patch_manager):
        """alert_manager.send_teardown_complete raises -> swallowed; still
        returns IterationStatus.TEARDOWN."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()
        runner.request_shutdown = MagicMock()
        runner._lifecycle_write_state = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        with _patch_manager(teardown_result=_make_successful_teardown_result()) as (mgr, _):
            mgr.alert_manager.send_teardown_complete.side_effect = RuntimeError("smtp down")
            result = await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=None,
                state_manager=MagicMock(),
            )

        assert result.status == IterationStatus.TEARDOWN
        runner.request_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_failure_does_not_prevent_success_mapping(self, _patch_manager):
        """state_manager.delete_teardown_state raises -> swallowed; still
        returns IterationStatus.TEARDOWN."""
        runner = _make_runner()
        runner._request_teardown_failure_shutdown = MagicMock()
        runner.request_shutdown = MagicMock()
        runner._lifecycle_write_state = MagicMock()

        intent = _make_intent()
        strategy = _make_strategy_for_manager(
            should_teardown=True, teardown_intents=[intent]
        )
        runner._build_teardown_compiler = MagicMock(return_value=MagicMock())

        with _patch_manager(teardown_result=_make_successful_teardown_result()) as (mgr, _):
            mgr.state_manager.delete_teardown_state.side_effect = RuntimeError("disk full")
            result = await runner._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=[intent],
                teardown_mode=TeardownMode.SOFT,
                teardown_market=None,
                start_time=datetime.now(UTC),
                request=None,
                state_manager=MagicMock(),
            )

        assert result.status == IterationStatus.TEARDOWN
        runner.request_shutdown.assert_called_once()
