from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli import _run_components as subject
from almanak.framework.cli._run_context import ComponentBundle
from almanak.framework.strategies.metadata import StrategyDataRequirements


def _provider_factories() -> subject._ProviderFactories:
    stack = SimpleNamespace(provider=object(), router=object())
    multi_chain_orchestrator = MagicMock(name="multi_chain_orchestrator")
    multi_chain_orchestrator.from_gateway.return_value = object()
    return subject._ProviderFactories(
        decimal=Decimal,
        balance_provider=MagicMock(name="balance_provider"),
        price_oracle=MagicMock(name="price_oracle"),
        create_ohlcv_stack=MagicMock(name="create_ohlcv_stack", return_value=stack),
        multi_chain_orchestrator=multi_chain_orchestrator,
        get_orca_pool_accounts=MagicMock(name="get_orca_pool_accounts", return_value=[]),
        init_prediction_provider=MagicMock(name="init_prediction_provider"),
        wire_core_providers=MagicMock(name="wire_core_providers"),
        wire_indicators=MagicMock(name="wire_indicators"),
    )


def test_wire_market_data_services_builds_indicator_stack(monkeypatch: pytest.MonkeyPatch) -> None:
    factories = _provider_factories()
    strategy = SimpleNamespace()
    source_policy = object()
    monkeypatch.setattr(subject, "_build_ohlcv_source_policy", MagicMock(return_value=source_policy))

    provider = subject._wire_market_data_services(
        strategy_instance=strategy,
        strategy_config={"pool_address": "0xpool"},
        requirements=StrategyDataRequirements(indicators=True),
        gateway_client="gateway",
        chain="arbitrum",
        price_oracle="price",
        balance_provider="balance",
        factories=factories,
    )

    stack = factories.create_ohlcv_stack.return_value
    assert provider is stack.provider
    assert strategy._ohlcv_router is stack.router
    factories.create_ohlcv_stack.assert_called_once_with(
        gateway_client="gateway",
        chain="arbitrum",
        pool_address="0xpool",
        source_policy=source_policy,
    )
    factories.wire_indicators.assert_called_once_with(strategy, stack.provider, "price", "balance")
    factories.wire_core_providers.assert_not_called()


@pytest.mark.parametrize(
    ("price", "balance", "should_wire"),
    [(True, False, True), (False, True, True), (False, False, False)],
)
def test_wire_market_data_services_without_indicators(
    price: bool,
    balance: bool,
    should_wire: bool,
) -> None:
    factories = _provider_factories()
    strategy = SimpleNamespace()

    provider = subject._wire_market_data_services(
        strategy_instance=strategy,
        strategy_config={},
        requirements=StrategyDataRequirements(price=price, balance=balance),
        gateway_client="gateway",
        chain="base",
        price_oracle="price",
        balance_provider="balance",
        factories=factories,
    )

    assert provider is None
    assert factories.wire_core_providers.called is should_wire
    factories.create_ohlcv_stack.assert_not_called()
    factories.wire_indicators.assert_not_called()


def test_wire_optional_data_services_wires_requested_services(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from almanak.framework.data import funding as funding_module
    from almanak.framework.data import rates as rates_module

    rate_monitor = object()
    funding_provider = object()
    rate_factory = MagicMock(return_value=rate_monitor)
    funding_factory = MagicMock(return_value=funding_provider)
    monkeypatch.setattr(rates_module, "RateMonitor", rate_factory)
    monkeypatch.setattr(funding_module, "GatewayFundingRateProvider", funding_factory)
    strategy = SimpleNamespace(
        _price_oracle=object(),
        _balance_provider=None,
        _indicator_provider=object(),
    )
    rpc_url_getter = MagicMock(return_value="rpc")

    runner = CliRunner()
    with runner.isolation() as (stdout, _stderr):
        subject._wire_optional_data_services(
            strategy_instance=strategy,
            requirements=StrategyDataRequirements(lending_rates=True, funding_rates=True),
            gateway_client="gateway",
            chain="arbitrum",
            rpc_url_getter=rpc_url_getter,
        )

    assert strategy._gateway_client == "gateway"
    assert strategy._rate_monitor is rate_monitor
    assert strategy._funding_rate_provider is funding_provider
    rate_factory.assert_called_once_with(
        chain="arbitrum",
        rpc_url="rpc",
        gateway_client="gateway",
        _internal=True,
    )
    funding_factory.assert_called_once_with(gateway_client="gateway", chain="arbitrum")
    assert stdout.getvalue().decode().strip() == (
        "Injected strategy data services: price, indicators, lending_rates, funding_rates"
    )


def test_wire_optional_data_services_skips_unrequested_services() -> None:
    strategy = SimpleNamespace()
    rpc_url_getter = MagicMock()

    runner = CliRunner()
    with runner.isolation() as (stdout, _stderr):
        subject._wire_optional_data_services(
            strategy_instance=strategy,
            requirements=StrategyDataRequirements(
                price=False,
                balance=False,
                indicators=False,
                lending_rates=False,
                funding_rates=False,
            ),
            gateway_client="gateway",
            chain="arbitrum",
            rpc_url_getter=rpc_url_getter,
        )

    assert strategy._gateway_client == "gateway"
    rpc_url_getter.assert_not_called()
    assert stdout.getvalue().decode().strip() == "Injected strategy data services:"


def test_wire_optional_data_services_logs_supported_failures(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from almanak.framework.data import funding as funding_module
    from almanak.framework.data import rates as rates_module

    monkeypatch.setattr(rates_module, "RateMonitor", MagicMock(side_effect=TypeError("rate unavailable")))
    monkeypatch.setattr(
        funding_module,
        "GatewayFundingRateProvider",
        MagicMock(side_effect=RuntimeError("funding unavailable")),
    )

    with caplog.at_level(logging.DEBUG, logger=subject.logger.name):
        subject._wire_optional_data_services(
            strategy_instance=SimpleNamespace(),
            requirements=StrategyDataRequirements(lending_rates=True, funding_rates=True),
            gateway_client="gateway",
            chain="base",
            rpc_url_getter=lambda: "rpc",
        )

    assert "Rate monitor not available: rate unavailable" in caplog.text
    assert "Funding rate provider init failed for chain=base: funding unavailable" in caplog.text


@pytest.mark.parametrize("error_type", [ImportError, ValueError, RuntimeError])
def test_wire_optional_data_services_suppresses_documented_funding_errors(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[Exception],
) -> None:
    from almanak.framework.data import funding as funding_module

    monkeypatch.setattr(
        funding_module,
        "GatewayFundingRateProvider",
        MagicMock(side_effect=error_type("unavailable")),
    )

    subject._wire_optional_data_services(
        strategy_instance=SimpleNamespace(),
        requirements=StrategyDataRequirements(funding_rates=True),
        gateway_client="gateway",
        chain="base",
        rpc_url_getter=lambda: "rpc",
    )


def test_wire_optional_data_services_propagates_unexpected_funding_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from almanak.framework.data import funding as funding_module

    monkeypatch.setattr(
        funding_module,
        "GatewayFundingRateProvider",
        MagicMock(side_effect=TypeError("bad constructor")),
    )

    with pytest.raises(TypeError, match="bad constructor"):
        subject._wire_optional_data_services(
            strategy_instance=SimpleNamespace(),
            requirements=StrategyDataRequirements(funding_rates=True),
            gateway_client="gateway",
            chain="base",
            rpc_url_getter=lambda: "rpc",
        )


def test_solana_clone_accounts_preserves_config_and_orca_order() -> None:
    get_orca_pool_accounts = MagicMock(return_value=["vault", "tick"])
    config = {
        "pool_address": "pool",
        "pool_a_address": 123,
        "pool_b_address": "pool-b",
    }

    runner = CliRunner()
    with runner.isolation() as (stdout, _stderr):
        accounts = subject._solana_clone_accounts(config, get_orca_pool_accounts)

    assert accounts == ["pool", "pool-b", "vault", "tick"]
    get_orca_pool_accounts.assert_called_once_with(config)
    assert "Pre-cloning 2 Orca pool accounts" in stdout.getvalue().decode()


@pytest.mark.parametrize("config", [{}, {"pool_address": 123}])
def test_solana_clone_accounts_handles_empty_results(config: dict[str, Any]) -> None:
    get_orca_pool_accounts = MagicMock(return_value=[])

    accounts = subject._solana_clone_accounts(config, get_orca_pool_accounts)

    assert accounts == []
    if config:
        get_orca_pool_accounts.assert_called_once_with(config)
    else:
        get_orca_pool_accounts.assert_not_called()


@pytest.mark.parametrize(
    ("chain", "network"),
    [("arbitrum", "anvil"), ("solana", "mainnet")],
)
def test_maybe_start_solana_fork_short_circuits(
    monkeypatch: pytest.MonkeyPatch,
    chain: str,
    network: str,
) -> None:
    cli_config = MagicMock(side_effect=AssertionError("Solana config should not load"))
    monkeypatch.setattr("almanak.config.cli_runtime_config_from_env", cli_config)
    components = ComponentBundle()

    subject._maybe_start_solana_fork(
        runtime_config=SimpleNamespace(chain=chain),
        strategy_config={},
        resolved_network=network,
        components=components,
        factories=_provider_factories(),
    )

    cli_config.assert_not_called()
    assert components.solana_fork_mgr is None


def test_maybe_start_solana_fork_funds_before_exposing_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from almanak.framework.anvil import solana_fork_manager as fork_module

    events: list[str] = []
    manager = MagicMock()
    manager.start = AsyncMock(side_effect=lambda: events.append("start") or True)
    manager.get_rpc_url.return_value = "http://localhost:8899"
    manager.fund_wallet = AsyncMock(side_effect=lambda *_args: events.append("wallet"))
    manager.fund_tokens = AsyncMock(side_effect=lambda *_args: events.append("tokens"))
    manager_factory = MagicMock(return_value=manager)
    monkeypatch.setattr(fork_module, "SolanaForkManager", manager_factory)
    monkeypatch.setattr(
        "almanak.config.cli_runtime_config_from_env",
        MagicMock(return_value=SimpleNamespace(solana_rpc_url="rpc", solana_validator_port=8899)),
    )
    factories = _provider_factories()
    factories.get_orca_pool_accounts.return_value = ["orca"]
    components = ComponentBundle()
    runtime_config = SimpleNamespace(chain="solana", wallet_address="wallet")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        subject._maybe_start_solana_fork(
            runtime_config=runtime_config,
            strategy_config={"pool_address": "pool"},
            resolved_network="anvil",
            components=components,
            factories=factories,
        )
    finally:
        loop.close()
        asyncio.set_event_loop(None)

    assert events == ["start", "wallet", "tokens"]
    assert components.solana_fork_mgr is manager
    manager_factory.assert_called_once_with(
        rpc_url="rpc",
        validator_port=8899,
        clone_accounts=["pool", "orca"],
    )


def test_build_multi_chain_providers_uses_fallback_for_missing_wallet_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from almanak.framework.data.balance import gateway_multichain as balance_module

    multi_balance_provider = object()
    multi_balance_factory = MagicMock(return_value=multi_balance_provider)
    monkeypatch.setattr(balance_module, "MultiChainGatewayBalanceProvider", multi_balance_factory)
    monkeypatch.setattr(subject, "_wire_market_data_services", MagicMock(return_value="ohlcv"))
    monkeypatch.setattr(subject, "_wire_optional_data_services", MagicMock())
    factories = _provider_factories()
    strategy = SimpleNamespace()
    runtime_config = SimpleNamespace(
        execution_address="fallback-wallet",
        max_gas_price_gwei=50,
        rpc_urls={"arbitrum": "rpc"},
    )

    built = subject._build_multi_chain_providers(
        runtime_config=runtime_config,
        strategy_chains=["arbitrum", "base"],
        strategy_config={},
        gateway_client="gateway",
        chain_wallets={"base": "base-wallet"},
        strategy_instance=strategy,
        requirements=StrategyDataRequirements(),
        factories=factories,
    )

    assert built.ohlcv_provider == "ohlcv"
    assert factories.balance_provider.call_args.kwargs["wallet_address"] == "fallback-wallet"
    assert factories.multi_chain_orchestrator.from_gateway.call_args.kwargs["wallet_address"] == "fallback-wallet"
    assert multi_balance_factory.call_args.kwargs["wallet_address"] == "fallback-wallet"
    assert not hasattr(strategy, "set_multi_chain_providers")


@pytest.mark.parametrize("multi_chain", [False, True])
def test_build_orchestrator_and_providers_dispatches_and_finalizes(
    monkeypatch: pytest.MonkeyPatch,
    multi_chain: bool,
) -> None:
    built = subject._BuiltProviders("orchestrator", "price", "balance", "ohlcv")
    build_multi = MagicMock(return_value=built)
    build_single = MagicMock(return_value=built)
    monkeypatch.setattr(subject, "_build_multi_chain_providers", build_multi)
    monkeypatch.setattr(subject, "_build_single_chain_providers", build_single)
    strategy = SimpleNamespace()
    components = ComponentBundle()

    subject._build_orchestrator_and_providers(
        multi_chain=multi_chain,
        runtime_config=SimpleNamespace(),
        strategy_chains=["arbitrum"],
        strategy_config={},
        resolved_network="mainnet",
        gateway_client="gateway",
        chain_wallets={},
        strategy_instance=strategy,
        components=components,
    )

    assert build_multi.called is multi_chain
    assert build_single.called is not multi_chain
    assert strategy._gateway_network == "mainnet"
    assert components.execution_orchestrator == "orchestrator"
    assert components.price_oracle == "price"
    assert components.balance_provider == "balance"
    assert components.ohlcv_provider == "ohlcv"


def test_build_orchestrator_and_providers_does_not_finalize_failed_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        subject,
        "_build_single_chain_providers",
        MagicMock(side_effect=click.ClickException("provider failed")),
    )
    strategy = SimpleNamespace()
    components = ComponentBundle()

    with pytest.raises(click.ClickException, match="provider failed"):
        subject._build_orchestrator_and_providers(
            multi_chain=False,
            runtime_config=SimpleNamespace(),
            strategy_chains=["arbitrum"],
            strategy_config={},
            resolved_network="mainnet",
            gateway_client="gateway",
            chain_wallets={},
            strategy_instance=strategy,
            components=components,
        )

    assert not hasattr(strategy, "_gateway_network")
    assert components.execution_orchestrator is None
    assert components.price_oracle is None
    assert components.balance_provider is None
    assert components.ohlcv_provider is None
