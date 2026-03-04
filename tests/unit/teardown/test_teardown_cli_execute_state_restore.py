"""Tests for teardown CLI state restore behavior."""

from __future__ import annotations

import importlib
import json
from decimal import Decimal
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

teardown_cli_module = importlib.import_module("almanak.framework.cli.teardown")


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create Click test runner."""
    return CliRunner()


def test_build_strategy_id_candidates_includes_all_sources_in_order() -> None:
    """Candidate generation should include configured/runtime/name/class sources."""

    class DummyStrategy:
        STRATEGY_NAME = "meta-strategy"

    strategy = DummyStrategy()
    strategy.strategy_id = "runtime:session"
    strategy.name = "runtime-name"

    candidates = teardown_cli_module._build_strategy_id_candidates(
        strategy,
        DummyStrategy,
        {"strategy_id": "configured:run"},
    )

    assert candidates == [
        "configured:run",
        "configured",
        "runtime:session",
        "runtime",
        "runtime-name",
        "meta-strategy",
        "DummyStrategy",
    ]


def test_build_strategy_id_candidates_deduplicates_while_preserving_order() -> None:
    """Duplicate IDs from different sources should appear once in first-seen order."""

    class DemoStrategy:
        STRATEGY_NAME = "demo_aerodrome_lp"

    strategy = DemoStrategy()
    strategy.strategy_id = "demo_aerodrome_lp"
    strategy.name = "demo_aerodrome_lp"

    candidates = teardown_cli_module._build_strategy_id_candidates(
        strategy,
        DemoStrategy,
        {"strategy_id": "demo_aerodrome_lp"},
    )

    assert candidates == ["demo_aerodrome_lp", "DemoStrategy"]


def test_execute_teardown_restores_state_before_position_detection(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """`execute` should hydrate persisted state before `get_open_positions`."""

    class FakeGatewayClient:
        def __init__(self, _config) -> None:
            self.connected = False
            self.channel = None

        def connect(self) -> None:
            self.connected = True

        def health_check(self) -> bool:
            return True

        def disconnect(self) -> None:
            self.connected = False

    class FakeStrategy:
        STRATEGY_NAME = "demo_aerodrome_lp"
        last_instance: FakeStrategy | None = None

        def __init__(self, config, chain: str, wallet_address: str) -> None:
            self.config = config
            self.chain = chain
            self.wallet_address = wallet_address
            self._has_position = False
            self.events: list[str] = []
            self.state_manager_strategy_ids: list[str] = []
            type(self).last_instance = self

        def supports_teardown(self) -> bool:
            return True

        def set_state_manager(self, _state_manager, strategy_id: str) -> None:
            self.events.append(f"set_state_manager:{strategy_id}")
            self.state_manager_strategy_ids.append(strategy_id)

        def load_state(self) -> bool:
            self.events.append("load_state")
            self._has_position = True
            return True

        def get_open_positions(self):
            self.events.append("get_open_positions")
            positions = []
            if self._has_position:
                positions.append(
                    SimpleNamespace(
                        position_type=SimpleNamespace(value="lp"),
                        protocol="aerodrome",
                        chain=self.chain,
                        position_id="aerodrome-lp-WETH/USDC-base",
                        value_usd=Decimal("1.23"),
                        health_factor=None,
                    )
                )
            return SimpleNamespace(positions=positions)

        def create_market_snapshot(self):
            self.events.append("create_market_snapshot")
            return SimpleNamespace(get_price_oracle_dict=lambda: {})

        def generate_teardown_intents(self, _mode, market=None):
            self.events.append(f"generate_teardown_intents:{'with_market' if market is not None else 'without_market'}")
            return [SimpleNamespace(intent_type=SimpleNamespace(value="LP_CLOSE"))]

    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text("# placeholder\n")

    config_file = tmp_path / "config.json"
    config_file.write_text(
        json.dumps(
            {
                "chain": "base",
                "wallet_address": "0x0000000000000000000000000000000000000001",
                "strategy_id": "demo_aerodrome_lp",
            }
        )
    )

    monkeypatch.setattr(teardown_cli_module, "load_strategy_from_file", lambda _path: (FakeStrategy, None))
    monkeypatch.setattr("almanak.framework.gateway_client.GatewayClient", FakeGatewayClient)

    # Monkeypatch TokenResolver.set_gateway_channel to accept the fake channel
    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.TokenResolver.set_gateway_channel",
        lambda _self, _channel: None,
    )

    result = cli_runner.invoke(
        teardown_cli_module.teardown,
        [
            "execute",
            "-d",
            str(tmp_path),
            "-c",
            str(config_file),
            "--no-gateway",
            "--preview",
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "No open positions found. Nothing to teardown." not in result.output
    assert "Teardown Steps (1):" in result.output

    instance = FakeStrategy.last_instance
    assert instance is not None
    assert instance.state_manager_strategy_ids == ["demo_aerodrome_lp"]
    assert instance.events.index("set_state_manager:demo_aerodrome_lp") < instance.events.index("load_state")
    assert instance.events.index("load_state") < instance.events.index("get_open_positions")


def test_inject_balance_provider_sets_sync_callable(monkeypatch: pytest.MonkeyPatch) -> None:
    """_inject_balance_provider should set _balance_provider on strategy."""
    from unittest.mock import MagicMock

    class FakeBalanceProvider:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def get_balance(self, token):
            return SimpleNamespace(
                balance=Decimal("100"),
                address="0x1",
                decimals=6,
                raw_balance=100_000_000,
                timestamp=0,
                stale=False,
            )

    class FakePriceOracle:
        async def get_aggregated_price(self, token, quote):
            return SimpleNamespace(price=Decimal("1"))

    class FakeStrategy:
        _balance_provider = None

    strategy = FakeStrategy()
    gateway_client = MagicMock()

    # Monkeypatch at the module level where the imports resolve
    monkeypatch.setattr(
        "almanak.framework.data.balance.gateway_provider.GatewayBalanceProvider",
        FakeBalanceProvider,
    )
    monkeypatch.setattr(
        "almanak.framework.data.price.gateway_oracle.GatewayPriceOracle",
        lambda _client: FakePriceOracle(),
    )

    teardown_cli_module._inject_balance_provider(
        strategy=strategy,
        gateway_client=gateway_client,
        chain="arbitrum",
        wallet_address="0xtest",
    )

    # balance_provider should now be a callable (sync wrapper)
    assert strategy._balance_provider is not None
    assert callable(strategy._balance_provider)


def test_inject_balance_provider_skips_if_no_attribute() -> None:
    """_inject_balance_provider should be a no-op for strategies without _balance_provider."""

    class BareStrategy:
        pass

    strategy = BareStrategy()
    # Should not raise
    teardown_cli_module._inject_balance_provider(
        strategy=strategy,
        gateway_client=None,
        chain="arbitrum",
        wallet_address="0xtest",
    )
    assert not hasattr(strategy, "_balance_provider")
