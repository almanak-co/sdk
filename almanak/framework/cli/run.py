"""CLI command for running strategies locally.

Usage:
    python -m src.cli.run --strategy <name>
    python -m src.cli.run --strategy <name> --config <path>

Example:
    python -m src.cli.run --strategy arbitrum_momentum
    python -m src.cli.run --strategy arbitrum_momentum --once
    python -m src.cli.run --strategy arbitrum_momentum --interval 30 --dry-run

Multi-chain example:
    export ALMANAK_CHAINS=base,arbitrum
    export ALMANAK_BASE_RPC_URL=http://127.0.0.1:8547
    export ALMANAK_ARBITRUM_RPC_URL=http://127.0.0.1:8545
    export ALMANAK_PRIVATE_KEY=0x...
    python -m src.cli.run --strategy leverage_loop_cross_chain --once
"""

import asyncio
import dataclasses
import inspect
import json
import logging
import re
import sys
import uuid
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..strategies.intent_strategy import TokenBalance

import click

from almanak.config.cli_options import gateway_client_options
from almanak.config.cli_runtime import CliRuntimeConfig
from almanak.gateway.data.balance import Web3BalanceProvider
from almanak.gateway.data.price import CoinGeckoPriceSource, PriceAggregator

from ..data.indicators.adx import ADXCalculator
from ..data.indicators.atr import ATRCalculator
from ..data.indicators.bollinger_bands import BollingerBandsCalculator
from ..data.indicators.cci import CCICalculator
from ..data.indicators.ichimoku import IchimokuCalculator
from ..data.indicators.macd import MACDCalculator
from ..data.indicators.moving_averages import MovingAverageCalculator
from ..data.indicators.obv import OBVCalculator
from ..data.indicators.rsi import RSICalculator
from ..data.indicators.stochastic import StochasticCalculator
from ..data.indicators.sync_wrappers import (
    create_sync_adx_func,
    create_sync_atr_func,
    create_sync_bollinger_func,
    create_sync_cci_func,
    create_sync_ema_func,
    create_sync_ichimoku_func,
    create_sync_macd_func,
    create_sync_obv_func,
    create_sync_rsi_func,
    create_sync_sma_func,
    create_sync_stochastic_func,
)
from ..data.interfaces import BalanceProvider as BalanceProviderInterface
from ..data.interfaces import PriceOracle
from ..data.ohlcv.dedup_provider import DedupingOHLCVProvider
from ..data.ohlcv.routing_provider import RoutingOHLCVProvider
from ..execution.config import (
    LocalRuntimeConfig,
    MultiChainRuntimeConfig,
)
from ..execution.multichain import MultiChainOrchestrator
from ..execution.orchestrator import ExecutionOrchestrator
from ..execution.signer.local import LocalKeySigner
from ..execution.simulator import create_simulator
from ..execution.submitter.public import PublicMempoolSubmitter
from ..runner import IterationResult
from ..strategies.intent_strategy import IndicatorProvider

logger = logging.getLogger(__name__)

# Well-known Anvil default account #0 (used when no private key is configured)
ANVIL_DEFAULT_ADDRESS = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
ANVIL_DEFAULT_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"  # gitleaks:allow


# =============================================================================
# Dict Config Wrapper
# =============================================================================


class DictConfigWrapper:
    """Wrapper for dict configs to provide required methods.

    StrategyBase expects config objects to have:
    - to_dict(): Serialize to dictionary
    - update(**kwargs): Update config values

    This wrapper makes plain dicts compatible.
    """

    def __init__(self, data: dict[str, Any]):
        """Initialize with dictionary data."""
        self._data = data
        # Copy all keys as attributes for getattr access
        for key, value in data.items():
            setattr(self, key, value)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return dict(self._data)

    def __getattr__(self, name: str) -> Any:
        """Provide a clearer missing-key error for strategy authors."""
        available_keys = ", ".join(sorted(self._data.keys())) if self._data else "(empty config)"
        raise AttributeError(f"Config key '{name}' not found in DictConfigWrapper. Available keys: {available_keys}")

    def update(self, **kwargs) -> Any:
        """Update config values.

        Returns a result object compatible with StrategyBase expectations.
        """
        from dataclasses import dataclass

        @dataclass
        class UpdateResult:
            success: bool = True
            error: str | None = None
            updated_fields: list[Any] | None = None
            previous_values: dict[Any, Any] | None = None

            def __post_init__(self):
                if self.updated_fields is None:
                    self.updated_fields = []
                if self.previous_values is None:
                    self.previous_values = {}

        previous = {}
        for key, value in kwargs.items():
            if key in self._data:
                previous[key] = self._data[key]
            self._data[key] = value
            setattr(self, key, value)

        return UpdateResult(
            success=True,
            updated_fields=list(kwargs.keys()),
            previous_values=previous,
        )

    def __getitem__(self, key: str) -> Any:
        """Support dict-like access."""
        return self._data[key]

    def get(self, key: str, default: Any = None) -> Any:
        """Support dict-like .get() access."""
        return self._data.get(key, default)


# =============================================================================
# Configuration
# =============================================================================


def find_strategy_dir(strategy_name: str) -> Path | None:
    """Find the strategy directory by name.

    Searches in standard locations:
    - strategies/<name>/
    - strategies/<tier>/<name>/ (for tiered strategies)
    - strategies/tests/<subdir>/<name>/ (for test strategies in subdirectories)
    - src/strategies/<name>/

    Args:
        strategy_name: Name of the strategy

    Returns:
        Path to strategy directory or None if not found
    """
    # Direct paths
    search_paths = [
        Path("strategies") / strategy_name,
        Path("src/strategies") / strategy_name,
    ]

    # Tier directories that contain nested strategies
    tier_dirs = ["poster_child", "production", "incubating", "demo", "alpha_team", "tests", "accounting"]

    # Add tiered paths: strategies/<tier>/<name>
    for tier in tier_dirs:
        search_paths.append(Path("strategies") / tier / strategy_name)

    # Handle demo_ prefix -> demo/<name> directory structure (backward compat)
    if strategy_name.startswith("demo_"):
        subdir_name = strategy_name[5:]  # Remove "demo_" prefix
        search_paths.insert(0, Path("almanak/demo_strategies") / subdir_name)

    # Handle test_ prefix -> tests/<subdir>/<name> for LP and TA strategies
    if strategy_name.startswith("test_"):
        # Strip test_ prefix to get the directory name
        dir_name = strategy_name[5:]  # Remove "test_" prefix

        # Search in tests/ directly: strategies/tests/<dir_name>/
        search_paths.insert(0, Path("strategies/tests") / dir_name)

        # Search in subdirectories of tests/ (e.g., tests/lp/, tests/ta/)
        tests_dir = Path("strategies/tests")
        if tests_dir.exists():
            for subdir in tests_dir.iterdir():
                if subdir.is_dir() and not subdir.name.startswith("_"):
                    # Add with test_ prefix stripped
                    search_paths.insert(0, subdir / dir_name)
                    # Also add with test_ prefix (in case directory has it)
                    search_paths.insert(0, subdir / strategy_name)

    for path in search_paths:
        if path.exists() and path.is_dir():
            return path

    return None


def _warn_missing_token_funding(config: dict[str, Any], config_path: Path) -> None:
    """Emit a warning if the config lacks a ``token_funding`` field."""
    if "token_funding" in config:
        return
    # Skip for intentionally dynamic strategies (e.g. copy_trader)
    if "copy_trading" in config:
        return
    click.echo(
        f'  Warning: {config_path.name} is missing a "token_funding" field. '
        "This will be required in a future release. "
        "Run `almanak strat new` to see the expected format.",
        err=True,
    )


def _deep_merge_configs(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` onto ``base``. Nested dicts merge; scalars / lists replace."""
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge_configs(result[key], value)
        else:
            result[key] = value
    return result


def _apply_env_strategy_config_override(config: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge the hosted-platform ``ALMANAK_STRATEGY_CONFIG`` env override onto ``config``.

    Re-validates the merged dict through ``StrategyConfig`` so env-supplied
    stringly-typed numerics coerce to ``Decimal`` consistently with the
    file-loaded path. Returns ``config`` unchanged when the env var is unset
    or empty. The env read itself lives in ``almanak.config.strategy``
    (config-service boundary, scripts/ci/check_config_boundary.py).
    """
    from pydantic import ValidationError

    from almanak.config.strategy import (
        STRATEGY_CONFIG_OVERRIDE_ENV,
        StrategyConfig,
        StrategyConfigEnvError,
        strategy_config_override_from_env,
    )

    try:
        overrides = strategy_config_override_from_env()
    except StrategyConfigEnvError as e:
        raise click.ClickException(str(e)) from e
    if not overrides:
        return config

    merged = _deep_merge_configs(config, overrides)

    try:
        validated = StrategyConfig.model_validate(merged)
    except ValidationError as e:
        raise click.ClickException(f"{STRATEGY_CONFIG_OVERRIDE_ENV} env override failed schema validation:\n{e}") from e

    click.echo(f"Applied {STRATEGY_CONFIG_OVERRIDE_ENV} env override for top-level fields: {sorted(overrides.keys())}")
    return validated.model_dump(mode="python", exclude_unset=True)


# crap-allowlist: #2098/#2101 adds schema validation around the existing loader
# without increasing its decision complexity.
def load_strategy_config(
    strategy_name: str,
    config_file: str | None = None,
) -> dict[str, Any]:
    """Load strategy configuration from file or defaults.

    #2098 / #2101 wrap the file read in Pydantic schema validation.
    JSON / YAML parse errors and schema mismatches surface as
    ``click.ClickException`` naming the file, instead of bubbling as opaque
    stack traces or — worse — being swallowed by ``except Exception`` further
    upstream in ``_setup_gateway``.

    The validated model is dumped back to ``dict`` (``mode="python"``) so
    downstream callers keep their dict API; Pydantic-coerced typed values
    (e.g. stringly-typed ``"0.005"`` -> ``Decimal("0.005")``) flow through.

    On hosted deployments the ``ALMANAK_STRATEGY_CONFIG`` env var (injected by
    the V2 agent deployer with the user's UI-edited config) is deep-merged on
    top of whatever was loaded from disk via
    ``_apply_env_strategy_config_override``, and the merged dict is
    re-validated against ``StrategyConfig``.

    Args:
        strategy_name: Name of the strategy
        config_file: Optional explicit config file path

    Returns:
        Configuration dictionary (Pydantic-validated; typed values preserved).
    """
    from pydantic import ValidationError

    from almanak.config.strategy import StrategyConfig

    def _parse_file(config_path: Path) -> dict[str, Any]:
        """Read, parse, and schema-validate a config file. Errors name the path."""
        import yaml

        try:
            with open(config_path) as f:
                if config_path.suffix.lower() in [".yaml", ".yml"]:
                    raw = yaml.safe_load(f)
                else:
                    raw = json.load(f)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, yaml.YAMLError) as e:
            raise click.ClickException(f"Failed to read strategy config {config_path}: {e}") from e

        if raw is None:
            # Empty / all-comments YAML — treat as empty config.
            raw = {}
        if not isinstance(raw, dict):
            raise click.ClickException(
                f"Strategy config {config_path} must be a JSON/YAML object, got {type(raw).__name__}."
            )

        try:
            validated = StrategyConfig.model_validate(raw)
        except ValidationError as e:
            raise click.ClickException(f"Strategy config {config_path} failed schema validation:\n{e}") from e

        # ``mode="python"`` preserves Decimal / list types instead of coercing
        # to JSON-friendly strings. ``exclude_unset=True`` preserves the
        # pre-Phase-3 dict shape exactly: only fields actually present in the
        # source file appear in the returned dict. Downstream contracts like
        # ``_warn_missing_token_funding`` (``"token_funding" in config``) and
        # the per-strategy dataclass extension keep working.
        return validated.model_dump(mode="python", exclude_unset=True)

    config: dict[str, Any] | None = None
    # Path of the source file the warning should name, if any.
    loaded_config_path: Path | None = None

    if config_file:
        config_path = Path(config_file)
        if not config_path.exists():
            raise click.ClickException(f"Config file not found: {config_file}")
        config = _parse_file(config_path)
        click.echo(f"Loaded config from: {config_path}")
        loaded_config_path = config_path
    else:
        # Search for config in standard locations
        strategy_dir = find_strategy_dir(strategy_name)
        if strategy_dir:
            for name in ("config.json", "config.yaml", "config.yml"):
                config_path = strategy_dir / name
                if config_path.exists():
                    config = _parse_file(config_path)
                    click.echo(f"Loaded config from: {config_path}")
                    loaded_config_path = config_path
                    break

    if config is None:
        # Minimal default config with generated UUID; still subject to the
        # hosted env override so a hosted run without any file still picks up
        # the platform-supplied config.
        config = {
            "deployment_id": f"{strategy_name}:{uuid.uuid4().hex[:12]}",
        }

    # Apply env override BEFORE the token-funding warning so the warning
    # reflects the config the strategy actually runs with — a hosted override
    # that supplies token_funding shouldn't trigger a false-positive warning,
    # and conversely an override that removes it shouldn't hide a real one.
    config = _apply_env_strategy_config_override(config)
    if loaded_config_path is not None:
        _warn_missing_token_funding(config, loaded_config_path)
    return config


# Re-exported from `chain_resolution` for back-compat. The canonical
# definition lives there so that sweep workers can import it without
# pulling in the heavy CLI module tree (#1703).
from .chain_resolution import get_default_chain  # noqa: E402, F401


def resolve_strategy_chain(
    strategy_class: type,
    strategy_config: dict,
    *,
    env_chain: str | None = None,
    multi_chain: bool = False,
) -> str | None:
    """Resolve the runtime chain for a single-chain strategy.

    Priority (single-chain strategies):
      1. If the strategy declares exactly one supported chain, use that chain —
         ALMANAK_CHAIN and config.json "chain" are both ignored (the strategy
         already knows its only valid chain). ALMANAK_CHAIN emits an INFO log
         when it conflicts so the operator can clean up their .env.
      2. If the strategy supports multiple chains, ALMANAK_CHAIN selects which
         one to run on (env > config.json "chain" > decorator default).

    The env override exists so the same strategy can be retargeted at a
    different supported chain (e.g. by the multi-chain demo smoke harness)
    without rewriting config.json. Production deployments leave the env unset
    and config.json is the source of truth.

    Args:
        strategy_class: The strategy class (used to read decorator metadata).
        strategy_config: The loaded config.json dict.
        env_chain: The ALMANAK_CHAIN env value, already lowercased and trimmed
            (None / empty = no override).
        multi_chain: True for multi-chain strategies — they bypass single-chain
            resolution entirely (chain comes from MultiChainRuntimeConfig).

    Returns:
        Resolved chain name (lowercased), or None for multi-chain when neither
        env nor config supplies one.

    Raises:
        click.ClickException: If env override is set but the chain isn't in the
            strategy's declared supported_chains (only when strategy supports
            multiple chains).
    """
    env = (env_chain or "").strip().lower() or None
    cfg_raw = strategy_config.get("chain")
    cfg_chain = cfg_raw.strip().lower() if isinstance(cfg_raw, str) and cfg_raw.strip() else None

    if not multi_chain:
        supported = [c.lower() for c in get_strategy_chains(strategy_class)]

        if len(supported) == 1:
            # Strategy declares exactly one chain — it knows its own chain.
            # Both ALMANAK_CHAIN and config.json chain are ignored (they have no
            # role when there is exactly one valid chain). Inform the operator
            # if the env var conflicts so they can clean up their .env.
            declared = supported[0]
            if env and env != declared:
                logger.info(
                    "Strategy declares chain=%s; ignoring ALMANAK_CHAIN=%s from environment",
                    declared,
                    env,
                )
            return declared

        if env and supported and env not in supported:
            raise click.ClickException(
                f"ALMANAK_CHAIN={env} conflicts with this strategy's supported chains "
                f"({', '.join(supported)}).\n"
                f"Fix: run with ALMANAK_CHAIN={supported[0]} or unset ALMANAK_CHAIN in your .env"
            )

    resolved = env or cfg_chain
    if not resolved and not multi_chain:
        default = get_default_chain(strategy_class)
        resolved = default.lower() if isinstance(default, str) else None
    return resolved


def create_price_oracle(
    config: LocalRuntimeConfig,
) -> Any:
    """Create a PriceOracle from configuration.

    Args:
        config: Local runtime configuration

    Returns:
        Configured PriceOracle
    """
    coingecko_source = CoinGeckoPriceSource(
        request_timeout=10.0,
        cache_ttl=30,
    )

    aggregator = PriceAggregator(
        sources=[coingecko_source],
    )

    return aggregator


def _price_oracle_supports_chain_kwarg(get_aggregated_price: Any) -> bool:
    """Return True when ``get_aggregated_price(..., chain=...)`` is supported."""
    try:
        parameters = inspect.signature(get_aggregated_price).parameters.values()
    except (TypeError, ValueError):
        return True

    for parameter in parameters:
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            return True
        if parameter.name == "chain":
            return parameter.kind != inspect.Parameter.POSITIONAL_ONLY

    return False


def create_sync_price_oracle_func(
    price_oracle: Any,
) -> Callable[[str, str, str | None], Decimal]:
    """Create a sync callable wrapper for an async PriceOracle.

    Args:
        price_oracle: Async PriceOracle implementation

    Returns:
        Sync callable (token, quote, chain) -> Decimal
    """
    import asyncio

    supports_chain_kwarg = _price_oracle_supports_chain_kwarg(price_oracle.get_aggregated_price)

    def sync_price(token: str, quote: str = "USD", chain: str | None = None) -> Decimal:
        """Fetch price synchronously."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            # We're in an async context - use nest_asyncio
            import nest_asyncio

            nest_asyncio.apply()
            coro = (
                price_oracle.get_aggregated_price(token, quote, chain=chain)
                if supports_chain_kwarg
                else price_oracle.get_aggregated_price(token, quote)
            )
            result = asyncio.get_event_loop().run_until_complete(coro)
        else:
            coro = (
                price_oracle.get_aggregated_price(token, quote, chain=chain)
                if supports_chain_kwarg
                else price_oracle.get_aggregated_price(token, quote)
            )
            result = asyncio.run(coro)

        return result.price

    # VIB-3895: stamp the underlying oracle on the sync wrapper so
    # `_infer_oracle_source` can unwrap and reach the real provider's
    # class identity (e.g. ``GatewayPriceOracle``). Without this,
    # `_infer_oracle_source` only sees ``sync_price`` and returns ""
    # → every ``transaction_ledger.price_inputs_json`` row carries
    # ``oracle_source: "unknown"`` even when the gateway aggregator is
    # correctly fanning out to the real providers.
    sync_price.__wrapped__ = price_oracle  # type: ignore[attr-defined]
    return sync_price


def create_sync_balance_func(
    balance_provider: BalanceProviderInterface,
    price_oracle: PriceOracle,
) -> Callable[[str], "TokenBalance"]:
    """Create a sync callable wrapper for balance provider.

    Args:
        balance_provider: Balance provider implementation
        price_oracle: Price oracle for USD conversion

    Returns:
        Sync callable (token) -> TokenBalance
    """
    import asyncio

    from ..strategies.intent_strategy import TokenBalance

    def sync_balance(token: str) -> TokenBalance:
        """Fetch balance synchronously."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            import nest_asyncio

            nest_asyncio.apply()
            result = asyncio.get_event_loop().run_until_complete(balance_provider.get_balance(token))
            # Get price for USD conversion
            try:
                price_result = asyncio.get_event_loop().run_until_complete(
                    price_oracle.get_aggregated_price(token, "USD")
                )
                balance_usd = result.balance * price_result.price
            except Exception:
                balance_usd = Decimal("0")
        else:
            result = asyncio.run(balance_provider.get_balance(token))
            try:
                price_result = asyncio.run(price_oracle.get_aggregated_price(token, "USD"))
                balance_usd = result.balance * price_result.price
            except Exception:
                balance_usd = Decimal("0")

        return TokenBalance(
            symbol=token,
            balance=result.balance,
            balance_usd=balance_usd,
            address=result.address,
        )

    return sync_balance


## create_sync_rsi_func moved to almanak.framework.data.indicators.sync_wrappers


# VIB-4347: factory relocated to ``almanak.framework.data.ohlcv.factory``.
# This file used to define ``create_routing_ohlcv_provider`` inline, but the
# factory's dependencies (router, providers, adapters) all live under
# ``framework/data/ohlcv/``, so its home in ``cli/`` was historical only.
# Re-exported here unchanged to keep existing imports
# (``from almanak.framework.cli.run import create_routing_ohlcv_provider``)
# working without code change. New call sites that also need the underlying
# sync ``OHLCVRouter`` should call ``create_ohlcv_stack`` instead.
from almanak.framework.data.ohlcv import (  # noqa: F401
    OHLCVStack,
    create_ohlcv_stack,
    create_routing_ohlcv_provider,
)

# _get_orca_pool_accounts now lives in ``cli/_solana_setup.py`` so ``cli/teardown.py``
# can import it without dragging in run.py's full Click command tree (VIB-522).
# Re-exported here to preserve the existing private symbol for ``run_helpers.py``
# and any tests still importing from ``cli/run.py``.
from ._solana_setup import get_orca_pool_accounts as _get_orca_pool_accounts  # noqa: F401


def _current_cli_runtime_config() -> CliRuntimeConfig:
    """Return the boot-loaded CLI config when available.

    `almanak strat run` / `strat test` prime `ctx.obj` after loading the
    strategy-local `.env`. Prefer that boot-surface-owned config slice over a
    fresh submodel loader call. The `load_config()` fallback keeps direct unit
    tests and non-wrapper invocation paths working.
    """
    ctx = click.get_current_context(silent=True)
    while ctx is not None:
        obj = getattr(ctx, "obj", None)
        if obj is not None and hasattr(obj, "cli"):
            return obj.cli
        ctx = ctx.parent

    from almanak.config import load_config

    return load_config().cli


def _validate_safe_mode_preflight(execution_address: str) -> str | None:
    """Validate Safe mode environment consistency between framework and gateway.

    Returns an error message string if validation fails, or None on success.

    Reads through the typed CLI-runtime config so the env-boundary lint stays
    clean during the config-service cutover. Prefer the boot-surface-owned `ctx.obj.cli` slice when
    the strategy wrapper has already called `load_config()`.
    """
    cli_cfg = _current_cli_runtime_config()
    gw_safe_mode = cli_cfg.gateway_safe_mode or ""
    gw_safe_address = cli_cfg.gateway_safe_address or cli_cfg.safe_address

    # Guard 1: Gateway must also be in Safe mode
    if gw_safe_mode not in ("direct", "zodiac"):
        return (
            f"Strategy is in Safe mode (ALMANAK_EXECUTION_MODE="
            f"{cli_cfg.execution_mode}) but gateway Safe mode "
            "is not configured.\n"
            "Set ALMANAK_GATEWAY_SAFE_MODE=direct|zodiac and "
            "ALMANAK_GATEWAY_SAFE_ADDRESS to match."
        )

    # Guard 2: Gateway must have a Safe address
    if not gw_safe_address:
        return "ALMANAK_GATEWAY_SAFE_MODE is set but ALMANAK_GATEWAY_SAFE_ADDRESS is missing."

    # Guard 3: Safe mode type must match (direct vs zodiac)
    framework_exec_mode = cli_cfg.execution_mode or ""
    expected_gw_mode = "zodiac" if framework_exec_mode == "safe_zodiac" else "direct"
    if gw_safe_mode != expected_gw_mode:
        return (
            f"Safe mode type mismatch -- framework execution mode is '{framework_exec_mode}' "
            f"(expects gateway '{expected_gw_mode}') but gateway is '{gw_safe_mode}'."
        )

    # Guard 4: Addresses must match
    if gw_safe_address.lower() != execution_address.lower():
        return (
            f"Safe address mismatch -- framework uses {execution_address} "
            f"but gateway uses {gw_safe_address}. "
            "These must be identical or the gateway will select the wrong signer."
        )

    return None


def _wire_indicators(
    strategy_instance: Any,
    ohlcv_provider: RoutingOHLCVProvider,
    price_oracle: PriceOracle,
    balance_provider: BalanceProviderInterface,
) -> None:
    """Create indicator calculators and inject providers into a strategy instance."""
    # VIB-3783 stop-gap: wrap the routed OHLCV provider in a per-strategy
    # request-coalescing dedup layer. Multiple indicators (e.g. MACD limit=85
    # plus ATR limit=34) for the same token in a single decide() call would
    # otherwise trigger two upstream fetches. The wrapper's cache is cleared
    # per-iteration via IntentStrategy.create_market_snapshot() so subsequent
    # iterations get fresh data.
    deduped_ohlcv_provider = DedupingOHLCVProvider(ohlcv_provider)
    rsi_calculator = RSICalculator(ohlcv_provider=deduped_ohlcv_provider)
    macd_calculator = MACDCalculator(ohlcv_provider=deduped_ohlcv_provider)
    bb_calculator = BollingerBandsCalculator(ohlcv_provider=deduped_ohlcv_provider)
    stoch_calculator = StochasticCalculator(ohlcv_provider=deduped_ohlcv_provider)
    atr_calculator = ATRCalculator(ohlcv_provider=deduped_ohlcv_provider)
    ma_calculator = MovingAverageCalculator(ohlcv_provider=deduped_ohlcv_provider)
    adx_calculator = ADXCalculator(ohlcv_provider=deduped_ohlcv_provider)
    obv_calculator = OBVCalculator(ohlcv_provider=deduped_ohlcv_provider)
    cci_calculator = CCICalculator(ohlcv_provider=deduped_ohlcv_provider)
    ichimoku_calculator = IchimokuCalculator(ohlcv_provider=deduped_ohlcv_provider)

    if hasattr(strategy_instance, "_price_oracle"):
        sync_price_oracle = create_sync_price_oracle_func(price_oracle)
        strategy_instance._price_oracle = sync_price_oracle
        strategy_instance._balance_provider = create_sync_balance_func(balance_provider, price_oracle)
        strategy_instance._rsi_provider = create_sync_rsi_func(rsi_calculator)
        strategy_instance._indicator_provider = IndicatorProvider(
            macd=create_sync_macd_func(macd_calculator),
            bollinger=create_sync_bollinger_func(bb_calculator),
            stochastic=create_sync_stochastic_func(stoch_calculator),
            atr=create_sync_atr_func(atr_calculator, sync_price_oracle),
            sma=create_sync_sma_func(ma_calculator, sync_price_oracle),
            ema=create_sync_ema_func(ma_calculator, sync_price_oracle),
            adx=create_sync_adx_func(adx_calculator),
            obv=create_sync_obv_func(obv_calculator),
            cci=create_sync_cci_func(cci_calculator),
            ichimoku=create_sync_ichimoku_func(ichimoku_calculator),
        )
        # VIB-3783: expose the dedup wrapper so create_market_snapshot() can
        # clear its cache at the start of each iteration.
        strategy_instance._ohlcv_dedup_provider = deduped_ohlcv_provider
        click.echo("  Providers injected into strategy (RSI + full indicator suite incl. ADX/OBV/CCI/Ichimoku)")


def _wire_core_providers(
    strategy_instance: Any,
    price_oracle: PriceOracle,
    balance_provider: BalanceProviderInterface,
) -> None:
    """Wire price oracle and balance provider onto strategy instance without indicator calculators.

    Called when StrategyDataRequirements.indicators=False but price/balance are still needed
    (the common case for non-indicator strategies). _wire_indicators handles all three when
    indicators=True, so this is only needed when indicators=False.
    Each attribute is checked and assigned independently so a strategy declaring only
    price=True does not also receive a balance provider (and vice versa).
    """
    if hasattr(strategy_instance, "_price_oracle"):
        strategy_instance._price_oracle = create_sync_price_oracle_func(price_oracle)
    if hasattr(strategy_instance, "_balance_provider"):
        strategy_instance._balance_provider = create_sync_balance_func(balance_provider, price_oracle)


def _init_prediction_provider(
    strategy_instance: Any,
    chain: str | None = None,
    gateway_client: Any | None = None,
) -> None:
    """Wire the Polymarket prediction provider onto a strategy instance.

    Strategies that declare ``polymarket`` in their ``supported_protocols``
    must abort startup if the provider can't initialize — silently HOLDing
    a polymarket-trading strategy because no gateway-backed client was
    available was the failure mode that drove PM Exp 14 / VIB-3132.

    The helper is strict opt-in: only strategies that explicitly declare
    ``polymarket`` support initialize the provider. Non-polymarket
    strategies (including Polygon strategies) skip initialization entirely
    to avoid irrelevant warnings and confusing operator noise.
    """
    strategy_metadata = getattr(strategy_instance, "STRATEGY_METADATA", None)
    # Use getattr on supported_protocols as well: STRATEGY_METADATA is normally
    # set by the @almanak_strategy decorator, but custom subclasses or partial
    # mocks may construct the metadata directly without that field (Gemini).
    supported_protocols = getattr(strategy_metadata, "supported_protocols", None) or []
    requires_polymarket = "polymarket" in supported_protocols

    # Strict opt-in: only strategies declaring polymarket attempt provider init.
    if not requires_polymarket:
        return

    try:
        from ..connectors.polymarket.gateway_client import GatewayPolymarketClient
        from ..data.prediction_provider import PredictionMarketDataProvider

        if gateway_client is None or not gateway_client.is_connected:
            raise RuntimeError("gateway client not connected")

        clob_client = GatewayPolymarketClient(gateway_client)
        strategy_instance._prediction_provider = PredictionMarketDataProvider(clob_client)  # type: ignore[arg-type]
        click.echo("  Prediction market provider initialized")
    except Exception as e:
        err_kind = type(e).__name__
        if requires_polymarket:
            strategy_name = strategy_metadata.name if strategy_metadata else "<unknown>"
            raise RuntimeError(
                f"Polymarket strategy '{strategy_name}' requires the prediction "
                f"market provider but initialization failed ({err_kind})."
            ) from e
        logger.warning("Prediction market provider not available (%s).", err_kind)


def create_balance_provider(
    config: LocalRuntimeConfig,
) -> BalanceProviderInterface:
    """Create a BalanceProvider from configuration.

    Args:
        config: Local runtime configuration

    Returns:
        Configured BalanceProvider
    """
    return Web3BalanceProvider(
        rpc_url=config.rpc_url,
        wallet_address=config.wallet_address,
        chain=config.chain,
        cache_ttl=5,
    )


# crap-allowlist: #2097 replaces the legacy 5x ``os.environ.get(...) or os.environ.get(legacy)``
# presence-check ladder with a typed :func:`gas_risk_override_presence` call.
# CC stays at 7 (one branch per typed override + the max_value_usd parse) and the
# function is exercised end-to-end by ``almanak strat run`` smokes; targeted unit
# coverage can land alongside a refactor that consolidates the four presence
# checks into a small helper, but the cyclomatic shape is structural to the
# legacy contract rather than something the migration introduced.
def _apply_runtime_gas_risk_overrides(
    tx_risk_config: Any,
    config: LocalRuntimeConfig,
) -> None:
    """Apply explicit env-var gas/risk overrides on top of the chain default.

    Mirrors the legacy presence-check ladder: when the operator has
    explicitly set ``ALMANAK_MAX_GAS_PRICE_GWEI`` (or the legacy
    unprefixed ``MAX_GAS_PRICE_GWEI``), the resolved
    ``LocalRuntimeConfig`` value wins over the chain-specific default.
    Otherwise the chain default selected by ``TransactionRiskConfig.for_chain``
    is preserved verbatim.

    The presence-check is sourced from the typed CLI-runtime config so
    the boundary lint stays clean during the config-service cutover.
    """
    from decimal import Decimal as _Decimal
    from decimal import InvalidOperation as _InvalidOperation

    from almanak.config.cli_runtime import gas_risk_override_presence, max_value_usd_override

    presence = gas_risk_override_presence()
    if presence["max_gas_price_gwei"]:
        tx_risk_config.max_gas_price_gwei = config.max_gas_price_gwei
    if presence["max_gas_cost_native"]:
        tx_risk_config.max_gas_cost_native = config.max_gas_cost_native
    if presence["max_gas_cost_usd"]:
        tx_risk_config.max_gas_cost_usd = config.max_gas_cost_usd
    if presence["max_slippage_bps"]:
        tx_risk_config.max_slippage_bps = config.max_slippage_bps

    # Per-tx USD cap. The CLI path hydrates native_token_price_usd via
    # StrategyRunner before each execute(), so it's safe to enable a default
    # cap here. Other orchestrator callers (gateway, paper trading) leave
    # this off — see TransactionRiskConfig docstring.
    max_value_usd_env = max_value_usd_override()
    if max_value_usd_env:
        try:
            tx_risk_config.max_value_usd = _Decimal(max_value_usd_env)
        except _InvalidOperation as exc:
            raise ValueError(
                "ALMANAK_MAX_VALUE_USD / MAX_VALUE_USD must be a plain decimal "
                "number (no commas, no units). Got: "
                f"{max_value_usd_env!r}"
            ) from exc
    else:
        tx_risk_config.max_value_usd = _Decimal("50000")


def create_execution_orchestrator(
    config: LocalRuntimeConfig,
    simulation_override: bool | None = None,
) -> ExecutionOrchestrator:
    """Create an ExecutionOrchestrator from configuration.

    Args:
        config: Local runtime configuration
        simulation_override: If provided, overrides config.simulation_enabled.
            True = force simulation, False = force no simulation,
            None = use config.simulation_enabled (from env var SIMULATION_ENABLED)

    Returns:
        Configured ExecutionOrchestrator
    """
    signer = LocalKeySigner(private_key=config.private_key)
    submitter = PublicMempoolSubmitter(rpc_url=config.rpc_url)

    # Determine simulation setting: CLI flag > config > env var
    simulation_enabled = simulation_override if simulation_override is not None else config.simulation_enabled

    # Create simulator based on configuration
    # - Auto-skips simulation for local RPCs (Anvil/Hardhat)
    # - Uses Tenderly/Alchemy if credentials configured and enabled
    # - Falls back to DirectSimulator if no credentials
    from ..execution.simulator.config import SimulationConfig

    sim_config = SimulationConfig.from_env()
    sim_config.enabled = simulation_enabled
    simulator = create_simulator(config=sim_config, rpc_url=config.rpc_url)

    from ..execution.orchestrator import TransactionRiskConfig

    tx_risk_config = TransactionRiskConfig.for_chain(config.chain)
    # Override with explicit env var values (env vars take precedence over chain defaults).
    # Route through the typed gas-risk override resolver so the boundary
    # check stays clean during the config-service cutover.
    _apply_runtime_gas_risk_overrides(tx_risk_config, config)

    return ExecutionOrchestrator(
        signer=signer,
        submitter=submitter,
        simulator=simulator,
        chain=config.chain,
        rpc_url=config.rpc_url,
        tx_risk_config=tx_risk_config,
    )


def is_multi_chain_strategy(strategy_class: type, config: dict | None = None) -> bool:
    """Check if a strategy should run in multi-chain mode.

    Multi-chain mode means the strategy executes intents across multiple chains
    simultaneously (e.g., bridge + swap on destination chain). This is different
    from a portable strategy that supports multiple chains but runs on one at a time.

    The signal is a "chains" list with >1 entry in either:
    1. config.json (highest priority)
    2. The strategy's config dataclass defaults (for strategies without config.json)
    3. Legacy SUPPORTED_CHAINS class attribute

    The decorator's supported_chains is NOT used — it's portability metadata,
    not a runtime multi-chain signal.

    Args:
        strategy_class: The strategy class to check
        config: Strategy config dict (from config.json). If provided and contains
            a "chains" list with >1 entry, the strategy is multi-chain.

    Returns:
        True if strategy should use MultiChainOrchestrator
    """
    # Primary signal: config "chains" list with >1 entry
    if config:
        config_chains = config.get("chains")
        if isinstance(config_chains, list) and len(config_chains) > 1:
            return True

    # Check config dataclass defaults (for strategies using Python config classes)
    # e.g., cross_chain_arbitrage has chains: list[str] = ["arbitrum", "optimism", "base"]
    from typing import get_args

    for base in getattr(strategy_class, "__orig_bases__", []):
        args = get_args(base)
        if args and hasattr(args[0], "__dataclass_fields__"):
            config_class = args[0]
            if "chains" in config_class.__dataclass_fields__:
                default = config_class.__dataclass_fields__["chains"].default
                if default is not dataclasses.MISSING and isinstance(default, list) and len(default) > 1:
                    return True
                # Check default_factory
                factory = config_class.__dataclass_fields__["chains"].default_factory
                if factory is not dataclasses.MISSING:
                    try:
                        default_val = factory()
                        if isinstance(default_val, list) and len(default_val) > 1:
                            return True
                    except Exception:
                        pass

    # Legacy: SUPPORTED_CHAINS class attribute (set manually, not by decorator)
    supported_chains = getattr(strategy_class, "SUPPORTED_CHAINS", None)
    if supported_chains and isinstance(supported_chains, list | tuple):
        return len(supported_chains) > 1
    return False


def get_strategy_chains(strategy_class: type) -> list[str]:
    """Get the supported chains for a strategy.

    Args:
        strategy_class: The strategy class

    Returns:
        List of supported chain names
    """
    # Check STRATEGY_METADATA first (set by @almanak_strategy decorator)
    metadata = getattr(strategy_class, "STRATEGY_METADATA", None)
    if metadata and hasattr(metadata, "supported_chains") and metadata.supported_chains:
        return metadata.supported_chains

    # Fall back to legacy SUPPORTED_CHAINS attribute
    return getattr(strategy_class, "SUPPORTED_CHAINS", ["arbitrum"])


def get_strategy_protocols(strategy_class: type) -> dict[str, list[str]]:
    """Get the protocols per chain for a strategy.

    Args:
        strategy_class: The strategy class

    Returns:
        Dict mapping chain to list of protocols
    """
    # Try to get from class attribute
    protocols = getattr(strategy_class, "SUPPORTED_PROTOCOLS", None)
    if protocols:
        return protocols

    # Default protocols per chain
    chains = get_strategy_chains(strategy_class)
    default_protocols = {
        "arbitrum": ["aave_v3", "uniswap_v3", "gmx_v2"],
        "base": ["uniswap_v3", "aave_v3"],
        "optimism": ["aave_v3", "uniswap_v3"],
        "ethereum": ["aave_v3", "uniswap_v3"],
    }
    return {chain: default_protocols.get(chain, ["uniswap_v3"]) for chain in chains}


def create_multi_chain_orchestrator(
    config: MultiChainRuntimeConfig,
) -> MultiChainOrchestrator:
    """Create a MultiChainOrchestrator from configuration.

    Args:
        config: Multi-chain runtime configuration

    Returns:
        Configured MultiChainOrchestrator
    """
    return MultiChainOrchestrator.from_config(config)


def format_iteration_result(result: IterationResult) -> str:
    """Format iteration result for display.

    Args:
        result: Iteration result to format

    Returns:
        Formatted string
    """
    parts = [f"Status: {result.status.value}"]

    if result.intent:
        parts.append(f"Intent: {result.intent.intent_type.value}")

    if result.execution_result and result.execution_result.success:
        parts.append(f"Gas used: {result.execution_result.total_gas_used}")

    if result.error:
        error_text = result.error
        if re.search(r"insufficient (funds|eth|balance)", error_text, re.IGNORECASE):
            error_text = f"{error_text} (hint: fund wallet with native gas token for this chain)"
        parts.append(f"Error: {error_text}")

    parts.append(f"Duration: {result.duration_ms:.0f}ms")

    return " | ".join(parts)


# =============================================================================
# CLI Command
# =============================================================================


@click.command("run")
@click.option(
    "--working-dir",
    "-d",
    type=click.Path(exists=True),
    default=".",
    help="Working directory containing strategy.py. Defaults to current directory.",
)
@click.option(
    "--config",
    "-c",
    "config_file",
    type=click.Path(exists=False),
    default=None,
    help="Path to strategy config JSON file",
)
@click.option(
    "--once",
    is_flag=True,
    default=False,
    help="Run single iteration then exit",
)
@click.option(
    "--interval",
    "-i",
    type=int,
    default=60,
    help="Loop interval in seconds (default: 60)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Execute decide() but don't submit transactions",
)
@click.option(
    "--list",
    "list_all",
    is_flag=True,
    default=False,
    help="List all available strategies and exit",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Enable verbose output (detailed strategy info)",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Enable debug output (includes Web3/HTTP logs)",
)
@click.option(
    "--dashboard",
    is_flag=True,
    default=False,
    help="Launch live dashboard alongside strategy execution",
)
@click.option(
    "--dashboard-port",
    type=int,
    default=8501,
    help="Port to run the dashboard on (default: 8501)",
)
@click.option(
    "--dashboard-mode",
    type=click.Choice(["hosted-parity", "command-center"], case_sensitive=False),
    default="hosted-parity",
    help=(
        "Dashboard layout. 'hosted-parity' (default) mirrors the hosted "
        "platform: one strategy, one gateway, no multi-strategy navigation. "
        "'command-center' opens the repo-wide browser. Standalone mode "
        "(--dashboard with no working dir) always uses Command Center."
    ),
)
@click.option(
    "--simulate-tx/--no-simulate-tx",
    "simulate_tx",
    default=None,
    help="Enable/disable transaction simulation via Tenderly/Alchemy before submission. Default: use SIMULATION_ENABLED env var",
)
@click.option(
    "--network",
    "-n",
    type=click.Choice(["mainnet", "anvil"], case_sensitive=False),
    default=None,
    help="Network environment: 'mainnet' for production RPC, 'anvil' for local fork testing (auto-starts Anvil on a free port). "
    "For paper trading with PnL tracking, use 'almanak strat backtest paper'. Overrides config.json 'network' field.",
)
@gateway_client_options
@click.option(
    "--fresh",
    is_flag=True,
    default=False,
    help="Clear strategy state before running (useful for fresh Anvil forks)",
)
@click.option(
    "--copy-mode",
    type=click.Choice(["live", "shadow", "replay"], case_sensitive=False),
    default=None,
    help="Copy-trading mode override for this run.",
)
@click.option(
    "--copy-shadow",
    is_flag=True,
    default=False,
    help="Enable copy-trading shadow mode (decisioning only, no submissions).",
)
@click.option(
    "--copy-replay-file",
    type=click.Path(exists=False),
    default=None,
    help="Replay file (JSON/JSONL CopySignal fixtures) for copy-trading replay mode.",
)
@click.option(
    "--copy-strict",
    is_flag=True,
    default=False,
    help="Enable strict copy-trading schema + fail-closed validation.",
)
@click.option(
    "--no-gateway",
    "no_gateway",
    is_flag=True,
    default=False,
    help="Do not auto-start a gateway; connect to an existing one.",
)
@click.option(
    "--anvil-port",
    "anvil_ports",
    multiple=True,
    help="Use existing Anvil instance: CHAIN=PORT (e.g., --anvil-port arbitrum=8545). Repeatable.",
)
@click.option(
    "--keep-anvil",
    is_flag=True,
    default=False,
    help="Keep Anvil instances running after gateway shutdown.",
)
@click.option(
    "--wallet",
    type=click.Choice(["default", "isolated"], case_sensitive=False),
    default="default",
    help="Wallet mode for Anvil: 'isolated' derives a unique wallet per strategy for balance isolation.",
)
@click.option(
    "--log-file",
    type=click.Path(dir_okay=False),
    default=None,
    help="Write JSON logs to this file (in addition to console output). Useful for AI agent analysis.",
)
@click.option(
    "--reset-fork",
    "reset_fork",
    is_flag=True,
    default=False,
    help="Reset Anvil fork to latest mainnet block before each iteration (requires --network anvil). "
    "Gives live on-chain state for fork testing.",
)
@click.option(
    "--max-iterations",
    type=int,
    default=None,
    help="Maximum number of iterations to run before exiting cleanly. "
    "Without this flag, continuous mode runs indefinitely.",
)
@click.option(
    "--teardown-after",
    is_flag=True,
    default=False,
    help="After --once iteration, automatically teardown (close all positions). "
    "Useful for CI/testing to avoid accumulating stale positions on-chain.",
)
def run(  # noqa: C901
    working_dir: str,
    config_file: str | None,
    once: bool,
    interval: int,
    dry_run: bool,
    list_all: bool,
    verbose: bool,
    debug: bool,
    dashboard: bool,
    dashboard_port: int,
    dashboard_mode: str,
    simulate_tx: bool | None,
    network: str | None,
    gateway_host: str,
    gateway_port: int,
    fresh: bool = False,
    copy_mode: str | None = None,
    copy_shadow: bool = False,
    copy_replay_file: str | None = None,
    copy_strict: bool = False,
    no_gateway: bool = False,
    anvil_ports: tuple[str, ...] = (),
    keep_anvil: bool = False,
    wallet: str = "default",
    log_file: str | None = None,
    reset_fork: bool = False,
    max_iterations: int | None = None,
    teardown_after: bool = False,
    # Internal-only (not exposed as click flags). Used by `almanak strat test`
    # to drive a force-action lifecycle through this command's setup pipeline
    # without duplicating it. Do not set from the CLI.
    test_actions: list[str] | None = None,
    test_json: bool = False,
) -> None:
    """
    Run a strategy from its working directory.

    By default, a managed gateway is auto-started in the background.
    Use --no-gateway to connect to an existing gateway instead.

    Prerequisites:
        - Environment variables: ALMANAK_PRIVATE_KEY (RPC_URL optional; public RPCs used if unset)
        - For anvil mode: Foundry installed (Anvil is auto-started)

    Examples:

        # Run from strategy directory (auto-starts gateway)
        cd almanak/demo_strategies/uniswap_rsi
        almanak strat run --once

        # Run with explicit working directory
        almanak strat run -d almanak/demo_strategies/uniswap_rsi --once

        # Run on local Anvil fork (auto-starts Anvil + gateway)
        almanak strat run --network anvil --once

        # Run continuously with 30-second intervals
        almanak strat run --interval 30

        # Dry run (no transactions)
        almanak strat run --dry-run --once

        # Fresh start (clear stale state, useful for Anvil forks)
        almanak strat run --fresh --once

        # List registered strategies
        almanak strat run --list
    """
    from .run_helpers import (
        _build_cleanup_fn,
        _build_components_or_exit,
        _configure_logging_and_validate,
        _echo_strategy_runtime_summary,
        _execute_run_mode,
        _instantiate_strategy,
        _load_resume_state,
        _load_strategy_bootstrap,
        _maybe_handle_run_early_exit,
        _maybe_start_dashboard_process,
        _prepare_runtime_bootstrap,
        _print_startup_banner,
        _setup_gateway,
        _stop_dashboard,
        _wire_token_resolver,
    )

    _configure_logging_and_validate(
        verbose=verbose,
        debug=debug,
        log_file=log_file,
        once=once,
        teardown_after=teardown_after,
        max_iterations=max_iterations,
    )

    (
        gateway_client,
        managed_gateway,
        effective_host,
        gateway_port,
        gateway_network,
        session_auth_token,
        isolated_wallet_address,
        _early_strategy_class,
    ) = _setup_gateway(
        working_dir=working_dir,
        config_file=config_file,
        network=network,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        no_gateway=no_gateway,
        anvil_ports=anvil_ports,
        wallet=wallet,
        keep_anvil=keep_anvil,
        reset_fork=reset_fork,
        once=once,
    )

    _wire_token_resolver(gateway_client)

    click.echo()

    if _maybe_handle_run_early_exit(
        list_all=list_all,
        gateway_client=gateway_client,
        working_dir=working_dir,
        dashboard=dashboard,
        dashboard_port=dashboard_port,
        gateway_host=effective_host,
        gateway_port=gateway_port,
        auth_token=session_auth_token,
        dashboard_mode=dashboard_mode,
    ):
        return

    strategy_bootstrap = _load_strategy_bootstrap(
        working_dir=working_dir,
        config_file=config_file,
        copy_mode=copy_mode,
        copy_shadow=copy_shadow,
        copy_replay_file=copy_replay_file,
        copy_strict=copy_strict,
        dry_run=dry_run,
        early_strategy_class=_early_strategy_class,
    )

    runtime_bootstrap = _prepare_runtime_bootstrap(
        strategy_bootstrap=strategy_bootstrap,
        no_gateway=no_gateway,
        gateway_client=gateway_client,
        network=network,
        gateway_network=gateway_network,
        fresh=fresh,
    )

    # Launch the dashboard sidecar AFTER ``deployment_id`` is resolved so
    # hosted-parity mode can scope to it from the first render rather than
    # racing the strategy boot. The dashboard subprocess uses the same
    # ``render_custom_dashboard_safe(...)`` shape the hosted platform uses,
    # closing the local↔hosted divergence operators have been hitting on
    # round-trips through staging.
    dashboard_process = _maybe_start_dashboard_process(
        dashboard=dashboard,
        dashboard_port=dashboard_port,
        gateway_host=effective_host,
        gateway_port=gateway_port,
        auth_token=session_auth_token,
        mode=dashboard_mode.lower(),
        deployment_id=runtime_bootstrap.deployment_id,
        strategy_working_dir=working_dir,
        # Forward the RESOLVED + MUTATED runtime config (post-bootstrap)
        # so the dashboard renders the same values the strategy sees —
        # closes the divergence when --config points outside working_dir
        # or when runtime overrides (copy-trading flags etc.) have been
        # applied (Codex P2 on PR #2372).
        strategy_config=strategy_bootstrap.strategy_config,
    )

    is_resume, existing_state_info = _load_resume_state(
        deployment_id=runtime_bootstrap.deployment_id,
    )

    _print_startup_banner(
        strategy_name=strategy_bootstrap.strategy_name,
        deployment_id=runtime_bootstrap.deployment_id,
        run_id=runtime_bootstrap.run_id,
        is_resume=is_resume,
        existing_state_info=existing_state_info,
        once=once,
        fresh=fresh,
        multi_chain=strategy_bootstrap.multi_chain,
        strategy_chains=strategy_bootstrap.strategy_chains,
        strategy_protocols=strategy_bootstrap.strategy_protocols,
        runtime_config=runtime_bootstrap.runtime_config,
        interval=interval,
        max_iterations=max_iterations,
        effective_dry_run=strategy_bootstrap.effective_dry_run,
        strategy_config=strategy_bootstrap.strategy_config,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        dashboard=dashboard,
    )

    _echo_strategy_runtime_summary(
        strategy_class=strategy_bootstrap.strategy_class,
        multi_chain=strategy_bootstrap.multi_chain,
        strategy_chains=strategy_bootstrap.strategy_chains,
    )

    strategy_instance = _instantiate_strategy(
        strategy_class=strategy_bootstrap.strategy_class,
        strategy_config=strategy_bootstrap.strategy_config,
        runtime_config=runtime_bootstrap.runtime_config,
        multi_chain=strategy_bootstrap.multi_chain,
        strategy_chains=strategy_bootstrap.strategy_chains,
        chain_wallets=runtime_bootstrap.chain_wallets,
    )

    components = _build_components_or_exit(
        strategy_instance=strategy_instance,
        strategy_config=strategy_bootstrap.strategy_config,
        runtime_config=runtime_bootstrap.runtime_config,
        strategy_chains=strategy_bootstrap.strategy_chains,
        multi_chain=strategy_bootstrap.multi_chain,
        resolved_network=runtime_bootstrap.resolved_network,
        gateway_client=gateway_client,
        chain_wallets=runtime_bootstrap.chain_wallets,
        interval=interval,
        effective_dry_run=strategy_bootstrap.effective_dry_run,
        deployment_id=runtime_bootstrap.deployment_id,
        normalized_copy_mode=strategy_bootstrap.normalized_copy_mode,
        copy_replay_file=copy_replay_file,
        copy_shadow=copy_shadow,
        copy_strict=copy_strict,
        config_chain=runtime_bootstrap.config_chain,
        managed_gateway=managed_gateway,
        keep_anvil=keep_anvil,
        dashboard_process=dashboard_process,
    )
    runner = components.runner
    state_manager = components.state_manager

    cleanup_resources = _build_cleanup_fn(
        gateway_client=gateway_client,
        managed_gateway=managed_gateway,
        keep_anvil=keep_anvil,
        components=components,
    )

    exit_code = _execute_run_mode(
        test_actions=test_actions,
        once=once,
        teardown_after=teardown_after,
        test_json=test_json,
        runner=runner,
        strategy_instance=strategy_instance,
        state_manager=state_manager,
        cleanup_fn=cleanup_resources,
        interval=interval,
        max_iterations=max_iterations,
        reset_fork=reset_fork,
        managed_gateway=managed_gateway,
    )

    _stop_dashboard(dashboard_process)
    sys.exit(exit_code)


def _has_placeholder_vault_address(vault_raw: dict) -> bool:
    """Check if the vault config has a placeholder address requiring auto-deploy."""
    addr = vault_raw.get("vault_address") or ""
    return addr.startswith("0x_") or "_DEPLOY_" in addr or "_SET_TO_" in addr


def _auto_deploy_lagoon_vault(
    vault_raw: dict,
    chain: str,
    runtime_config: Any,
    gateway_client: Any,
    execution_orchestrator: Any,
) -> dict:
    """Auto-deploy a Lagoon vault on Anvil and patch vault_raw with real addresses.

    Returns the patched vault_raw dict with real vault_address and valuator_address.
    Exits with sys.exit(1) on failure.
    """
    from ..connectors.lagoon.deployer import LagoonVaultDeployer, VaultDeployParams
    from ..data.tokens import get_token_resolver

    # Resolve underlying token symbol to address
    underlying_symbol = vault_raw.get("underlying_token", "USDC")
    try:
        underlying_address = get_token_resolver().get_address(chain, underlying_symbol)
    except Exception as e:
        click.secho(f"  ERROR: Cannot resolve token '{underlying_symbol}' on {chain}: {e}", fg="red")
        sys.exit(1)

    wallet = runtime_config.wallet_address
    deployer = LagoonVaultDeployer(gateway_client)

    params = VaultDeployParams(
        chain=chain,
        underlying_token_address=underlying_address,
        name="Almanak Anvil Vault",
        symbol="aVLT",
        safe_address=wallet,
        admin_address=wallet,
        fee_receiver_address=wallet,
        deployer_address=wallet,
    )

    # Step 1: Build and execute deploy transaction
    click.echo("  Building vault deploy transaction...")
    try:
        deploy_bundle = deployer.build_deploy_vault_bundle(params)
    except Exception as e:
        click.secho(f"  ERROR: Failed to build deploy transaction: {e}", fg="red")
        sys.exit(1)

    click.echo("  Executing vault deploy transaction...")
    try:
        deploy_result = asyncio.run(execution_orchestrator.execute(deploy_bundle))
    except Exception as e:
        click.secho(f"  ERROR: Vault deploy transaction failed: {e}", fg="red")
        sys.exit(1)

    if not deploy_result.success:
        error_msg = getattr(deploy_result, "error", "Unknown error")
        click.secho(f"  ERROR: Vault deploy transaction reverted: {error_msg}", fg="red")
        sys.exit(1)

    # Step 2: Parse receipt to extract vault address
    receipt = None
    for tx_result in deploy_result.transaction_results:
        if tx_result.receipt:
            receipt = tx_result.receipt
            break

    if receipt is None:
        click.secho("  ERROR: No receipt found for vault deploy transaction", fg="red")
        sys.exit(1)

    parsed = deployer.parse_deploy_receipt(receipt)
    if not parsed.success or not parsed.vault_address:
        click.secho(f"  ERROR: Could not extract vault address: {parsed.error}", fg="red")
        sys.exit(1)

    vault_address = parsed.vault_address
    click.secho(f"  Vault deployed at: {vault_address}", fg="green")

    # Step 3: Approve underlying token for vault
    click.echo("  Approving underlying token for vault...")
    try:
        approve_bundle = deployer.build_post_deploy_bundle(underlying_address, vault_address, wallet)
        approve_result = asyncio.run(execution_orchestrator.execute(approve_bundle))
        if not approve_result.success:
            click.secho("  WARNING: Underlying approval failed (vault may still work)", fg="yellow")
    except Exception as e:
        click.secho(f"  WARNING: Underlying approval failed: {e}", fg="yellow")

    # Patch vault_raw with real addresses
    vault_raw = dict(vault_raw)  # shallow copy to avoid mutating original
    vault_raw["vault_address"] = vault_address
    vault_raw["valuator_address"] = wallet
    click.secho("  Vault config patched with deployed addresses", fg="green")

    return vault_raw


if __name__ == "__main__":
    run()
