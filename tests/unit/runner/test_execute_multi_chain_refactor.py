"""Behavioral coverage for multi-chain preparation and lane dispatch."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from almanak.framework.execution.multichain import MultiChainOrchestrator
from almanak.framework.intents.bridge import BridgeIntent
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.runner.runner_models import IterationStatus
from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner


class _IntentStub:
    def __init__(
        self,
        *,
        chain: str | None = "arbitrum",
        destination_chain: str | None = None,
        from_token: str = "USDC",
        to_token: str = "ETH",
    ) -> None:
        self.chain = chain
        self.destination_chain = destination_chain
        self.from_token = from_token
        self.to_token = to_token
        self.intent_type = IntentType.SWAP
        self.intent_id = "intent-0000000000"


class _OracleOnlyMarket:
    def __init__(self, prices: dict[str, Decimal]) -> None:
        self.prices = prices

    def get_price_oracle_dict(self) -> dict[str, Decimal]:
        return self.prices


def _make_runner(*, dry_run: bool = False) -> tuple[StrategyRunner, MagicMock]:
    orchestrator = MagicMock(spec=MultiChainOrchestrator)
    orchestrator.primary_chain = "arbitrum"
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=orchestrator,
        state_manager=AsyncMock(),
        alert_manager=MagicMock(),
        config=RunnerConfig(
            default_interval_seconds=0,
            enable_state_persistence=False,
            enable_alerting=False,
            dry_run=dry_run,
        ),
    )
    runner._record_success = MagicMock()  # type: ignore[method-assign]
    runner._calculate_duration_ms = MagicMock(return_value=17)  # type: ignore[method-assign]
    return runner, orchestrator


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "dep-multichain"
    return strategy


class TestMultiChainPreparation:
    def test_chain_scope_keeps_first_seen_source_and_destination_order(self):
        runner, orchestrator = _make_runner()
        intents = [
            _IntentStub(chain="arbitrum", destination_chain="base"),
            _IntentStub(chain="optimism", destination_chain="base"),
            _IntentStub(chain=None),
        ]

        prepared = runner._prepare_multi_chain_execution(
            deployment_id="dep-multichain",
            intents=intents,
            orchestrator=orchestrator,
            market=None,
        )

        assert prepared.chains_involved == ("arbitrum", "base", "optimism")
        assert prepared.has_cross_chain is True
        assert prepared.price_map is None
        assert prepared.price_oracle is None

    def test_market_without_oracle_export_is_ignored(self):
        runner, orchestrator = _make_runner()

        prepared = runner._prepare_multi_chain_execution(
            deployment_id="dep-multichain",
            intents=[_IntentStub()],
            orchestrator=orchestrator,
            market=object(),
        )

        assert prepared.price_map is None
        assert prepared.price_oracle is None

    def test_exported_prices_are_forwarded_without_a_warmup_api(self):
        runner, orchestrator = _make_runner()
        prices = {"USDC": Decimal("1")}

        prepared = runner._prepare_multi_chain_execution(
            deployment_id="dep-multichain",
            intents=[_IntentStub()],
            orchestrator=orchestrator,
            market=_OracleOnlyMarket(prices),
        )

        assert prepared.price_map == {"USDC": "1"}
        assert prepared.price_oracle is prices

    def test_existing_price_is_not_warmed_again(self):
        runner, orchestrator = _make_runner()
        prices = {"USDC": Decimal("1"), "ETH": Decimal("2500")}
        market = MagicMock()
        market.get_price_oracle_dict.return_value = prices

        prepared = runner._prepare_multi_chain_execution(
            deployment_id="dep-multichain",
            intents=[_IntentStub()],
            orchestrator=orchestrator,
            market=market,
        )

        market.price.assert_not_called()
        market.get_price_oracle_dict.assert_called_once_with()
        assert prepared.price_map == {"USDC": "1", "ETH": "2500"}
        assert prepared.price_oracle is prices

    def test_bridge_intent_warms_source_and_to_chain_then_refreshes_oracle(self):
        runner, orchestrator = _make_runner()
        refreshed = {"USDC": Decimal("1")}
        market = MagicMock()
        market.get_price_oracle_dict.side_effect = [{}, refreshed]
        bridge = BridgeIntent(
            token="USDC",
            amount=Decimal("10"),
            from_chain="arbitrum",
            to_chain="base",
        )

        prepared = runner._prepare_multi_chain_execution(
            deployment_id="dep-multichain",
            intents=[bridge],
            orchestrator=orchestrator,
            market=market,
        )

        assert market.price.call_args_list == [
            call("USDC", chain="arbitrum"),
            call("USDC", chain="base"),
        ]
        assert market.get_price_oracle_dict.call_count == 2
        assert prepared.price_map == {"USDC": "1"}
        assert prepared.price_oracle is refreshed

    def test_warmup_failure_is_warning_only_and_keeps_empty_prices(self, caplog):
        runner, orchestrator = _make_runner()
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {}
        market.price.side_effect = RuntimeError("provider unavailable")

        prepared = runner._prepare_multi_chain_execution(
            deployment_id="dep-multichain",
            intents=[_IntentStub(from_token="USDC", to_token="USDC")],
            orchestrator=orchestrator,
            market=market,
        )

        assert "Failed to pre-fetch price for USDC on arbitrum: provider unavailable" in caplog.text
        market.get_price_oracle_dict.assert_called_once_with()
        assert prepared.price_map is None
        assert prepared.price_oracle is None

    def test_oracle_export_failure_still_propagates(self):
        runner, orchestrator = _make_runner()
        market = MagicMock()
        market.get_price_oracle_dict.side_effect = RuntimeError("oracle export failed")

        with pytest.raises(RuntimeError, match="oracle export failed"):
            runner._prepare_multi_chain_execution(
                deployment_id="dep-multichain",
                intents=[_IntentStub()],
                orchestrator=orchestrator,
                market=market,
            )


class TestMultiChainDispatch:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("intents", [[], [_IntentStub()]])
    async def test_dry_run_returns_identity_and_skips_both_execution_lanes(self, intents):
        runner, _orchestrator = _make_runner(dry_run=True)
        runner._execute_with_bridge_waiting = AsyncMock()  # type: ignore[method-assign]
        runner._execute_same_chain_legs = AsyncMock()  # type: ignore[method-assign]

        result = await runner._execute_multi_chain(
            strategy=_make_strategy(),
            intents=intents,
            start_time=datetime.now(UTC),
        )

        assert result.status is IterationStatus.DRY_RUN
        assert result.intent is (intents[0] if intents else None)
        assert result.deployment_id == "dep-multichain"
        assert result.duration_ms == 17
        runner._record_success.assert_called_once_with()
        runner._execute_with_bridge_waiting.assert_not_awaited()
        runner._execute_same_chain_legs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_same_chain_lane_receives_prepared_inputs_and_resume_marker(self):
        runner, orchestrator = _make_runner()
        strategy = _make_strategy()
        intent = _IntentStub()
        resume_progress = MagicMock()
        sentinel = MagicMock()
        start_time = datetime.now(UTC)
        runner._execute_same_chain_legs = AsyncMock(return_value=sentinel)  # type: ignore[method-assign]

        result = await runner._execute_multi_chain(
            strategy=strategy,
            intents=[intent],
            start_time=start_time,
            market=None,
            resume_progress=resume_progress,
        )

        assert result is sentinel
        assert runner._execute_same_chain_legs.await_args.kwargs == {
            "strategy": strategy,
            "intents": [intent],
            "orchestrator": orchestrator,
            "start_time": start_time,
            "price_map": None,
            "price_oracle": None,
            "resume_progress": resume_progress,
        }

    @pytest.mark.asyncio
    async def test_bridge_lane_receives_refreshed_prices_and_resume_marker(self):
        runner, orchestrator = _make_runner()
        strategy = _make_strategy()
        bridge = BridgeIntent(
            token="USDC",
            amount=Decimal("10"),
            from_chain="arbitrum",
            to_chain="base",
        )
        prices = {"USDC": Decimal("1")}
        market = MagicMock()
        market.get_price_oracle_dict.return_value = prices
        resume_progress = MagicMock()
        sentinel = MagicMock()
        runner._execute_with_bridge_waiting = AsyncMock(return_value=sentinel)  # type: ignore[method-assign]

        result = await runner._execute_multi_chain(
            strategy=strategy,
            intents=[bridge],
            start_time=datetime.now(UTC),
            market=market,
            resume_progress=resume_progress,
        )

        assert result is sentinel
        kwargs = runner._execute_with_bridge_waiting.await_args.kwargs
        assert kwargs["strategy"] is strategy
        assert kwargs["intents"] == [bridge]
        assert kwargs["orchestrator"] is orchestrator
        assert kwargs["price_map"] == {"USDC": "1"}
        assert kwargs["price_oracle"] is prices
        assert kwargs["resume_progress"] is resume_progress

    @pytest.mark.asyncio
    async def test_execution_lane_exception_still_propagates(self):
        runner, _orchestrator = _make_runner()
        runner._execute_same_chain_legs = AsyncMock(  # type: ignore[method-assign]
            side_effect=RuntimeError("lane failed")
        )

        with pytest.raises(RuntimeError, match="lane failed"):
            await runner._execute_multi_chain(
                strategy=_make_strategy(),
                intents=[_IntentStub()],
                start_time=datetime.now(UTC),
            )
