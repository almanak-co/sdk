"""Tests for teardown CLI state restore behavior."""

from __future__ import annotations

import importlib
import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

teardown_cli_module = importlib.import_module("almanak.framework.cli.teardown")


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create Click test runner."""
    return CliRunner()


def test_build_deployment_id_candidates_includes_all_sources_in_order() -> None:
    """Candidate generation should include only explicit deployment IDs."""

    class DummyStrategy:
        STRATEGY_NAME = "meta-strategy"

    strategy = DummyStrategy()
    strategy.deployment_id = "runtime:session"
    strategy.name = "runtime-name"

    candidates = teardown_cli_module._build_deployment_id_candidates(
        strategy,
        {"deployment_id": "configured:run"},
    )

    assert candidates == [
        "configured:run",
        "runtime:session",
    ]


def test_build_deployment_id_candidates_deduplicates_while_preserving_order() -> None:
    """Duplicate IDs from different sources should appear once in first-seen order."""

    class DemoStrategy:
        STRATEGY_NAME = "demo_aerodrome_lp"

    strategy = DemoStrategy()
    strategy.deployment_id = "demo_aerodrome_lp"
    strategy.name = "demo_aerodrome_lp"

    candidates = teardown_cli_module._build_deployment_id_candidates(
        strategy,
        {"deployment_id": "demo_aerodrome_lp"},
    )

    assert candidates == ["demo_aerodrome_lp"]


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
            self.state_manager_deployment_ids: list[str] = []
            type(self).last_instance = self

        def set_state_manager(self, _state_manager, deployment_id: str) -> None:
            self.events.append(f"set_state_manager:{deployment_id}")
            self.state_manager_deployment_ids.append(deployment_id)

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
                "deployment_id": "demo_aerodrome_lp",
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
    assert instance.state_manager_deployment_ids == ["demo_aerodrome_lp"]
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
        lambda _client, default_chain=None: FakePriceOracle(),
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


# ---------------------------------------------------------------------------
# VIB-5520: standalone teardown-execute wires the gateway price oracle so the
# Plan-B break-glass path can warm + validate prices and compile closing intents.
# ---------------------------------------------------------------------------


class _FakeAggOracle:
    """Mimics ``GatewayPriceOracle``: an async ``get_aggregated_price`` that
    returns a price for known tokens and RAISES for unknown ones — exactly how a
    real gateway behaves when it cannot price a token. Lets us prove both that
    the fix makes real prices available AND that TD-17's hard-stop still fires
    when a required token is genuinely unpriceable."""

    def __init__(self, prices: dict[str, Decimal]):
        self._prices = {k.upper(): v for k, v in prices.items()}
        self.calls: list[str] = []

    async def get_aggregated_price(self, token: str, quote: str = "USD", chain: str | None = None):
        self.calls.append(token.upper())
        key = token.upper()
        if key in self._prices:
            return SimpleNamespace(price=self._prices[key])
        raise ValueError(f"Cannot determine price for {token}/{quote} on {chain}")


class _BuilderStrategy:
    """A minimal IntentStrategy-shaped strategy whose ``create_market_snapshot``
    routes through the real ``MarketSnapshotBuilder.for_strategy_runner`` — the
    same builder the runner and the strategy base use. This exercises the actual
    seam the fix targets (``strategy._gateway_client`` / ``strategy._price_oracle``
    feeding ``get_price_oracle_dict()``) instead of a hand-rolled fake snapshot."""

    def __init__(self, config, chain: str, wallet_address: str) -> None:
        self.config = config
        self.chain = chain
        self._chain = chain
        self._wallet_address = wallet_address
        self.wallet_address = wallet_address
        self._balance_provider = None
        self._price_oracle = None
        self._gateway_client = None

    def is_multi_chain(self) -> bool:
        return False

    def get_config(self, key, default=None):
        return default

    def create_market_snapshot(self):
        from almanak.framework.market.builders import MarketSnapshotBuilder

        return MarketSnapshotBuilder.for_strategy_runner(
            strategy=self,
            gateway_client=self._gateway_client,
            chain=self._chain,
            wallet_address=self._wallet_address,
        )


def _instantiate_builder_strategy(monkeypatch: pytest.MonkeyPatch, prices: dict[str, Decimal]) -> object:
    """Run the real ``instantiate_strategy_with_state`` with a gateway oracle
    seeded by ``prices`` and return the wired strategy."""
    from almanak.framework.cli import teardown_helpers as th

    oracle = _FakeAggOracle(prices)

    class _FakeBalanceProvider:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def get_balance(self, token):
            return SimpleNamespace(balance=Decimal("0"), decimals=18)

    monkeypatch.setattr(
        "almanak.framework.data.balance.gateway_provider.GatewayBalanceProvider",
        _FakeBalanceProvider,
    )
    monkeypatch.setattr(
        "almanak.framework.data.price.gateway_oracle.GatewayPriceOracle",
        lambda _client, default_chain=None: oracle,
    )
    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.TokenResolver.set_gateway_channel",
        lambda _self, _channel: None,
    )

    gateway_client = SimpleNamespace(channel=None)
    strategy = th.instantiate_strategy_with_state(
        strategy_class=_BuilderStrategy,
        config_dict={"pool": "WETH/USDC/500"},
        chain="arbitrum",
        wallet_address="0x0000000000000000000000000000000000000001",
        gateway_client=gateway_client,
        inject_balance_provider=teardown_cli_module._inject_balance_provider,
        restore_strategy_state=lambda **_kwargs: None,
    )
    assert strategy._gateway_client is gateway_client
    return strategy


def test_inject_balance_provider_wires_price_oracle(monkeypatch: pytest.MonkeyPatch) -> None:
    """VIB-5520: _inject_balance_provider must wire ``_price_oracle`` (not just
    ``_balance_provider``) so the teardown market snapshot can resolve prices."""

    class FakePriceOracle:
        async def get_aggregated_price(self, token, quote="USD", chain=None):
            return SimpleNamespace(price=Decimal("1"))

    class FakeBalanceProvider:
        def __init__(self, **kwargs):
            pass

        async def get_balance(self, token):
            return SimpleNamespace(balance=Decimal("0"), decimals=18)

    class FakeStrategy:
        _balance_provider = None
        _price_oracle = None

    monkeypatch.setattr(
        "almanak.framework.data.balance.gateway_provider.GatewayBalanceProvider",
        FakeBalanceProvider,
    )
    monkeypatch.setattr(
        "almanak.framework.data.price.gateway_oracle.GatewayPriceOracle",
        lambda _client, default_chain=None: FakePriceOracle(),
    )

    gateway_client = MagicMock()
    gateway_client.channel = None

    strategy = FakeStrategy()
    teardown_cli_module._inject_balance_provider(
        strategy=strategy,
        gateway_client=gateway_client,
        chain="arbitrum",
        wallet_address="0xtest",
    )

    assert callable(strategy._price_oracle), "price oracle must be wired so market.price() works"
    assert callable(strategy._balance_provider)


def test_inject_balance_provider_wires_price_oracle_unconditionally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """VIB-5520: the price-oracle wiring must NOT be gated on a pre-declared
    ``_price_oracle`` attribute. A strategy type that sets ``_balance_provider``
    but does not pre-declare ``_price_oracle`` (the exact VIB-5520 failure class)
    must still get the oracle wired -- otherwise ``market.price()`` silently has
    no oracle and TD-17 validation blocks teardown."""

    class FakePriceOracle:
        async def get_aggregated_price(self, token, quote="USD", chain=None):
            return SimpleNamespace(price=Decimal("1"))

    class FakeBalanceProvider:
        def __init__(self, **kwargs):
            pass

        async def get_balance(self, token):
            return SimpleNamespace(balance=Decimal("0"), decimals=18)

    class FakeStrategyNoOracleAttr:
        # Past the ``_balance_provider`` early-return gate, but deliberately does
        # NOT pre-declare ``_price_oracle``.
        _balance_provider = None

    monkeypatch.setattr(
        "almanak.framework.data.balance.gateway_provider.GatewayBalanceProvider",
        FakeBalanceProvider,
    )
    monkeypatch.setattr(
        "almanak.framework.data.price.gateway_oracle.GatewayPriceOracle",
        lambda _client, default_chain=None: FakePriceOracle(),
    )

    gateway_client = MagicMock()
    gateway_client.channel = None

    strategy = FakeStrategyNoOracleAttr()
    assert not hasattr(strategy, "_price_oracle")

    teardown_cli_module._inject_balance_provider(
        strategy=strategy,
        gateway_client=gateway_client,
        chain="arbitrum",
        wallet_address="0xtest",
    )

    assert hasattr(strategy, "_price_oracle"), "oracle must be set even without a pre-declared attr"
    assert callable(strategy._price_oracle), "price oracle must be wired so market.price() works"
    assert callable(strategy._balance_provider)


def test_instantiate_wires_gateway_and_enables_pricing(monkeypatch: pytest.MonkeyPatch) -> None:
    """VIB-5520 acceptance: after instantiate_strategy_with_state the strategy can
    build a real market snapshot whose price() resolves via the gateway oracle,
    and TD-17's warm_and_validate_oracle returns a complete dict (no raise)."""
    from almanak.framework.intents.vocabulary import Intent
    from almanak.framework.teardown.oracle_warmup import warm_and_validate_oracle

    strategy = _instantiate_builder_strategy(
        monkeypatch,
        prices={"WETH": Decimal("3400"), "USDC": Decimal("1"), "ETH": Decimal("3400")},
    )

    market = strategy.create_market_snapshot()
    assert market.price("WETH", chain="arbitrum") == Decimal("3400")

    intent = Intent.swap(from_token="WETH", to_token="USDC", amount="all", chain="arbitrum")
    oracle = warm_and_validate_oracle(market, [intent], "arbitrum")
    assert oracle is not None
    assert oracle.get("WETH") == Decimal("3400")
    assert oracle.get("USDC") == Decimal("1")


def test_td17_hard_stop_still_fires_when_required_token_unpriceable(monkeypatch: pytest.MonkeyPatch) -> None:
    """VIB-5520 must NOT defeat TD-17: the fix only makes real prices available.
    When the gateway genuinely cannot price a required token, the wired snapshot's
    price() still fails and warm_and_validate_oracle still raises the named
    pre-flight error — refusing to compile closing intents on a blind oracle."""
    from almanak.framework.intents.vocabulary import Intent
    from almanak.framework.teardown.oracle_warmup import (
        TeardownPriceOracleError,
        warm_and_validate_oracle,
    )

    # ARB is genuinely unpriceable (no alias, no stablecoin fallback) — ETH/USDC
    # priced so only ARB is the genuine miss.
    strategy = _instantiate_builder_strategy(
        monkeypatch,
        prices={"ETH": Decimal("3400"), "USDC": Decimal("1")},
    )

    market = strategy.create_market_snapshot()
    intent = Intent.swap(from_token="ARB", to_token="USDC", amount="all", chain="arbitrum")

    with pytest.raises(TeardownPriceOracleError) as exc:
        warm_and_validate_oracle(market, [intent], "arbitrum")
    assert "ARB" in str(exc.value)


def test_instantiate_applies_dataclass_config_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """VIB-5520 secondary: instantiate_strategy_with_state coerces the config the
    same way the runner does, applying the strategy's config dataclass DEFAULTS
    for fields absent from config.json (e.g. uniswap_lp omits force_action /
    position_id). Previously the raw DictConfigWrapper raised AttributeError."""
    from dataclasses import dataclass
    from typing import Generic, TypeVar

    from almanak.framework.cli import teardown_helpers as th

    T = TypeVar("T")

    class _ConfigBase(Generic[T]):
        pass

    @dataclass
    class _LPConfig:
        pool: str = "WETH/USDC/500"
        force_action: str = ""
        position_id: str | None = None

    class _TypedStrategy:
        __orig_bases__ = (_ConfigBase[_LPConfig],)

        def __init__(self, config, chain: str, wallet_address: str) -> None:
            self.config = config
            self.chain = chain
            self._chain = chain
            # Touch the optional fields the way UniswapLPStrategy.__init__ does —
            # this is exactly what raised AttributeError under DictConfigWrapper.
            self.force_action = config.force_action
            self.position_id = config.position_id
            self._balance_provider = None
            self._price_oracle = None
            self._gateway_client = None

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.TokenResolver.set_gateway_channel",
        lambda _self, _channel: None,
    )

    strategy = th.instantiate_strategy_with_state(
        strategy_class=_TypedStrategy,
        config_dict={"pool": "WETH/USDC/3000"},  # force_action / position_id omitted
        chain="arbitrum",
        wallet_address="0x0000000000000000000000000000000000000001",
        gateway_client=SimpleNamespace(channel=None),
        inject_balance_provider=lambda *a, **k: None,
        restore_strategy_state=lambda **_kwargs: None,
    )

    assert strategy.config.pool == "WETH/USDC/3000"
    assert strategy.force_action == ""  # dataclass default applied
    assert strategy.position_id is None  # dataclass default applied


def test_instantiate_falls_back_to_dict_wrapper_without_typed_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """VIB-5489: a strategy with NO typed config dataclass (no ``__orig_bases__``
    generic parameter) must still instantiate under ``teardown execute`` — the
    ``coerce_strategy_config`` path degrades to ``DictConfigWrapper`` exactly as
    the runner does. Guards the no-schema fallback branch of the VIB-5520 fix so
    swapping ``DictConfigWrapper`` for ``coerce_strategy_config`` never regresses
    schemaless strategies (attribute access + ``.get`` must both keep working)."""
    from almanak.framework.cli import teardown_helpers as th
    from almanak.framework.cli._strategy_config import DictConfigWrapper

    class _SchemalessStrategy:
        # No ``__orig_bases__`` generic → coerce_strategy_config resolves no
        # dataclass and returns a DictConfigWrapper around the raw dict.
        def __init__(self, config, chain: str, wallet_address: str) -> None:
            self.config = config
            self.chain = chain
            self._chain = chain
            # Attribute access (the DictConfigWrapper contract) still works for
            # keys that ARE present in config.json.
            self.pool = config.pool
            # ``.get`` with a default is the schemaless way to read an optional
            # field absent from config.json — must not raise.
            self.force_action = config.get("force_action", "")
            self._balance_provider = None
            self._price_oracle = None
            self._gateway_client = None

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.TokenResolver.set_gateway_channel",
        lambda _self, _channel: None,
    )

    strategy = th.instantiate_strategy_with_state(
        strategy_class=_SchemalessStrategy,
        config_dict={"pool": "WETH/USDC/3000"},  # force_action omitted
        chain="arbitrum",
        wallet_address="0x0000000000000000000000000000000000000001",
        gateway_client=SimpleNamespace(channel=None),
        inject_balance_provider=lambda *a, **k: None,
        restore_strategy_state=lambda **_kwargs: None,
    )

    assert isinstance(strategy.config, DictConfigWrapper)  # wrapper fallback taken
    assert strategy.pool == "WETH/USDC/3000"
    assert strategy.force_action == ""  # optional absent field via .get default
