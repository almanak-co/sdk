"""Local-mode configuration sibling.

Phase 0 skeleton: subclass of :class:`BaseConfig` with optional submodels
attached as later phases land them.

* Phase 5a-2 — ``runtime: RuntimeConfig | None``. Optional because the
  runtime config is built lazily from the strategy's decorator metadata
  (chain / chains / protocols), so the bare ``load_config()`` call at the
  Click main group cannot populate it. Consumers attach it after the
  strategy is loaded; ``None`` is the explicit "not yet wired" state.
* Phase 5b — ``connectors``: typed API-key + base-URL submodel populated
  eagerly by ``load_config()`` (the connector layer needs it from the
  moment any connector is instantiated, which can predate strategy
  decorator resolution).
* Phase 5c — ``backtest``: typed paper-trading + pnl-provider submodel
  populated eagerly by ``load_config()`` so providers see the resolved
  config the moment they are constructed (CoinGecko / TheGraph keys,
  the ``ARCHIVE_RPC_URL_<CHAIN>`` cluster as a typed dict, gas-API
  per-chain keys, and the SSL-cert hint for paper-trading subprocesses).

See ``docs/internal/config-service-plan.md`` for the full migration order.
"""

from pydantic import Field

from almanak.config.backtest import BacktestConfig, backtest_config_from_env
from almanak.config.base import BaseConfig
from almanak.config.cli_runtime import CliRuntimeConfig, cli_runtime_config_from_env
from almanak.config.connectors import ConnectorsConfig, connectors_config_from_env
from almanak.config.runtime import RuntimeConfig


class LocalConfig(BaseConfig):
    """Local-mode config (folder-scoped, sqlite-backed).

    ``runtime`` is ``None`` by default. ``load_config()`` does NOT populate
    it automatically — the strategy's decorator metadata (chain / chains /
    protocols) is required and is only known after the strategy module has
    been imported. Consumers (CLI / runner) attach the resolved runtime
    config explicitly via ``cfg.runtime = runtime_config_from_env(...)``
    once the strategy is loaded.

    ``connectors`` is populated by ``connectors_config_from_env`` at
    construction time. The default factory captures whatever env state is
    in effect when the model is built — tests that monkeypatch env vars
    must construct the model AFTER patching (``config_factory(...)`` in
    ``tests/unit/config/conftest.py`` does this correctly because pydantic
    re-runs the factory on every instantiation).

    ``backtest`` mirrors the same pattern: eager default factory means
    the field is available the moment the backtesting layer touches it
    (PnL providers, paper-trading background process, crisis-runner
    date-range guard).

    ``cli`` (Phase 5e) carries the CLI-specific knobs that don't fit
    any other submodel: gateway-wallets discriminator, Safe-mode
    preflight inputs, Solana fork URL/port, Anvil per-chain ports,
    reconciliation / hardcoded-prices toggles, and the legacy unprefixed
    ``GATEWAY_AUTH_TOKEN`` fallback. Eager factory same as the others —
    consumers reach for ``cli_runtime_config_from_env()`` for stateless
    re-reads (truthy boolean toggles flip mid-process during tests),
    or ``load_config().cli`` for boot-time reads.
    """

    runtime: RuntimeConfig | None = None
    connectors: ConnectorsConfig = Field(default_factory=connectors_config_from_env)
    backtest: BacktestConfig = Field(default_factory=backtest_config_from_env)
    cli: CliRuntimeConfig = Field(default_factory=cli_runtime_config_from_env)


__all__ = ["LocalConfig"]
