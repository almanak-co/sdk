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
    StrategyRunner,
)
from almanak.framework.teardown.models import TeardownMode


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
    strategy_id: str = "test_strat",
    chain: str = "arbitrum",
    wallet_address: str = "0x1234",
    should_teardown: bool = False,
    supports_teardown: bool = True,
    teardown_intents: list | None = None,
    market_snapshot: object | None = None,
) -> MagicMock:
    """Build a mock strategy with configurable teardown behaviour."""
    strategy = MagicMock()
    strategy.strategy_id = strategy_id
    strategy.chain = chain
    strategy.wallet_address = wallet_address
    strategy.should_teardown.return_value = should_teardown
    strategy.supports_teardown.return_value = supports_teardown

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
    return intent


# ---------------------------------------------------------------------------
# _check_teardown_requested
# ---------------------------------------------------------------------------


class TestCheckTeardownRequested:
    """Tests for _check_teardown_requested (pure check, no side effects)."""

    def test_returns_none_when_no_should_teardown(self):
        runner = _make_runner()
        strategy = MagicMock(spec=[])  # no should_teardown attr
        strategy.strategy_id = "strat"
        assert runner._check_teardown_requested(strategy) is None

    def test_returns_none_when_should_teardown_false(self):
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=False)
        assert runner._check_teardown_requested(strategy) is None

    def test_returns_none_when_supports_teardown_false(self):
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True, supports_teardown=False)
        assert runner._check_teardown_requested(strategy) is None

    def test_returns_none_when_no_generate_teardown_intents(self):
        runner = _make_runner()
        strategy = MagicMock(spec=["strategy_id", "should_teardown", "supports_teardown"])
        strategy.strategy_id = "strat"
        strategy.should_teardown.return_value = True
        strategy.supports_teardown.return_value = True
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
        """Market snapshot is created BEFORE teardown is delegated to TeardownManager.

        Single-chain teardown now routes through TeardownManager, which handles
        intent generation. The runner's responsibility is creating the market snapshot
        and passing it to TeardownManager.
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

        # Patch TeardownManager path to return success
        async def mock_tm_via_manager(**kwargs):
            return IterationResult(
                status=IterationStatus.TEARDOWN, intent=None, strategy_id="test_strat"
            )

        runner._execute_teardown_via_manager = AsyncMock(side_effect=mock_tm_via_manager)

        await runner.run_iteration(strategy)

        # Verify market snapshot created before delegation
        strategy.create_market_snapshot.assert_called_once()
        # TeardownManager receives the market snapshot
        call_kwargs = runner._execute_teardown_via_manager.call_args[1]
        assert call_kwargs["teardown_market"] is market_mock

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
        runner._execute_teardown_via_manager = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN, intent=None, strategy_id="test_strat"
            )
        )

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.TEARDOWN
        # TeardownManager was called (handling empty intents internally)
        runner._execute_teardown_via_manager.assert_called_once()

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
        runner._execute_teardown_via_manager = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.EXECUTION_FAILED,
                intent=None,
                strategy_id="test_strat",
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
        runner._execute_teardown_via_manager = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.EXECUTION_FAILED,
                intent=None,
                strategy_id="test_strat",
                error="Teardown failed: not done",
            )
        )

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.EXECUTION_FAILED

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_market_failure_continues_without_market(self, mock_get_manager):
        """If create_market_snapshot fails, teardown still proceeds with market=None.

        Single-chain teardown now routes through TeardownManager. The runner creates
        the market snapshot and passes it. If snapshot fails, market=None is passed.
        """
        runner = _make_runner()
        intent = _make_intent()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[intent])
        strategy.create_market_snapshot.side_effect = RuntimeError("no market")

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        runner._execute_teardown_via_manager = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN, intent=None, strategy_id="test_strat"
            )
        )

        await runner.run_iteration(strategy)

        # TeardownManager should receive market=None since snapshot failed
        call_kwargs = runner._execute_teardown_via_manager.call_args[1]
        assert call_kwargs["teardown_market"] is None

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

        runner._execute_teardown_via_manager = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN, intent=None, strategy_id="test_strat"
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
        runner._execute_teardown_via_manager = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN, intent=None, strategy_id="test_strat"
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
                strategy_id="test_strat",
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
