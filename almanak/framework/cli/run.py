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
import os
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
from ..data.ohlcv.gateway_data_adapter import GatewayOHLCVDataProvider, GeckoTerminalGatewayDataProvider
from ..data.ohlcv.gateway_provider import GatewayGeckoTerminalOHLCVProvider, GatewayOHLCVProvider
from ..data.ohlcv.ohlcv_router import OHLCVRouter
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
from ..strategies import IntentStrategy
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


def load_strategy_config(
    strategy_name: str,
    config_file: str | None = None,
) -> dict[str, Any]:
    """Load strategy configuration from file or defaults.

    Args:
        strategy_name: Name of the strategy
        config_file: Optional explicit config file path

    Returns:
        Configuration dictionary
    """
    if config_file:
        config_path = Path(config_file)
        if not config_path.exists():
            raise click.ClickException(f"Config file not found: {config_file}")

        if config_path.suffix.lower() in [".yaml", ".yml"]:
            import yaml

            with open(config_path) as f:
                config = yaml.safe_load(f)
        else:
            with open(config_path) as f:
                config = json.load(f)
        click.echo(f"Loaded config from: {config_path}")
        _warn_missing_token_funding(config, config_path)
        return config

    # Search for config in standard locations
    strategy_dir = find_strategy_dir(strategy_name)
    if strategy_dir:
        # Try JSON first
        config_path = strategy_dir / "config.json"
        if config_path.exists():
            with open(config_path) as f:
                config = json.load(f)
            click.echo(f"Loaded config from: {config_path}")
            _warn_missing_token_funding(config, config_path)
            return config

        # Try YAML
        for yaml_name in ["config.yaml", "config.yml"]:
            config_path = strategy_dir / yaml_name
            if config_path.exists():
                import yaml

                with open(config_path) as f:
                    config = yaml.safe_load(f)
                click.echo(f"Loaded config from: {config_path}")
                _warn_missing_token_funding(config, config_path)
                return config

    # Return minimal default config with generated UUID
    return {
        "strategy_id": f"{strategy_name}:{uuid.uuid4().hex[:12]}",
    }


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


def create_routing_ohlcv_provider(
    gateway_client: Any,
    chain: str,
    strategy_config: dict[str, Any],
) -> RoutingOHLCVProvider:
    """Build an OHLCV provider with multi-source routing from strategy context.

    Registers both the gateway/Binance provider and the GeckoTerminal provider
    with an OHLCVRouter, then returns a RoutingOHLCVProvider that implements
    the OHLCVProvider protocol for use by indicators.

    Args:
        gateway_client: Connected GatewayClient instance.
        chain: Chain name (e.g. "base", "arbitrum").
        strategy_config: Parsed strategy config.json dict.

    Returns:
        RoutingOHLCVProvider with multi-source routing enabled.
    """
    gateway_provider = GatewayOHLCVProvider(gateway_client=gateway_client)
    binance_adapter = GatewayOHLCVDataProvider(gateway_provider)

    gecko_provider = GatewayGeckoTerminalOHLCVProvider(gateway_client=gateway_client, chain=chain)
    gecko_adapter = GeckoTerminalGatewayDataProvider(gecko_provider)

    router = OHLCVRouter(default_chain=chain)
    # VIB-3448: OHLCVRouter._PROVIDER_CHAINS["defi_primary"] lists a "defillama"
    # middle tier between Gecko and Binance, but no gateway-backed DeFi Llama
    # OHLCV provider exists yet.  Until one is available, Gecko blips fall straight
    # through to the known-futile CEX path.  Track on VIB-3448 / gateway roadmap.
    router.register_provider(gecko_adapter)
    router.register_provider(binance_adapter)

    pool_address = strategy_config.get("pool_address")
    return RoutingOHLCVProvider(
        router=router,
        chain=chain,
        pool_address=str(pool_address) if pool_address else None,
        closeable_providers=[],
    )


# _get_orca_pool_accounts now lives in ``cli/_solana_setup.py`` so ``cli/teardown.py``
# can import it without dragging in run.py's full Click command tree (VIB-522).
# Re-exported here to preserve the existing private symbol for ``run_helpers.py``
# and any tests still importing from ``cli/run.py``.
from ._solana_setup import get_orca_pool_accounts as _get_orca_pool_accounts  # noqa: F401


def _validate_safe_mode_preflight(execution_address: str) -> str | None:
    """Validate Safe mode environment consistency between framework and gateway.

    Returns an error message string if validation fails, or None on success.
    """
    gw_safe_mode = (os.environ.get("ALMANAK_GATEWAY_SAFE_MODE") or "").lower()
    gw_safe_address = os.environ.get("ALMANAK_GATEWAY_SAFE_ADDRESS") or os.environ.get("ALMANAK_SAFE_ADDRESS")

    # Guard 1: Gateway must also be in Safe mode
    if gw_safe_mode not in ("direct", "zodiac"):
        return (
            f"Strategy is in Safe mode (ALMANAK_EXECUTION_MODE="
            f"{os.environ.get('ALMANAK_EXECUTION_MODE')}) but gateway Safe mode "
            "is not configured.\n"
            "Set ALMANAK_GATEWAY_SAFE_MODE=direct|zodiac and "
            "ALMANAK_GATEWAY_SAFE_ADDRESS to match."
        )

    # Guard 2: Gateway must have a Safe address
    if not gw_safe_address:
        return "ALMANAK_GATEWAY_SAFE_MODE is set but ALMANAK_GATEWAY_SAFE_ADDRESS is missing."

    # Guard 3: Safe mode type must match (direct vs zodiac)
    framework_exec_mode = (os.environ.get("ALMANAK_EXECUTION_MODE") or "").lower()
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
    # Check os.environ directly to distinguish "user set value" from "default used".
    import os

    if os.environ.get("ALMANAK_MAX_GAS_PRICE_GWEI") or os.environ.get("MAX_GAS_PRICE_GWEI"):
        tx_risk_config.max_gas_price_gwei = config.max_gas_price_gwei
    if os.environ.get("ALMANAK_MAX_GAS_COST_NATIVE") or os.environ.get("MAX_GAS_COST_NATIVE"):
        tx_risk_config.max_gas_cost_native = config.max_gas_cost_native
    if os.environ.get("ALMANAK_MAX_GAS_COST_USD") or os.environ.get("MAX_GAS_COST_USD"):
        tx_risk_config.max_gas_cost_usd = config.max_gas_cost_usd
    if os.environ.get("ALMANAK_MAX_SLIPPAGE_BPS") or os.environ.get("MAX_SLIPPAGE_BPS"):
        tx_risk_config.max_slippage_bps = config.max_slippage_bps

    # Per-tx USD cap. The CLI path hydrates native_token_price_usd via
    # StrategyRunner before each execute(), so it's safe to enable a default
    # cap here. Other orchestrator callers (gateway, paper trading) leave
    # this off — see TransactionRiskConfig docstring.
    from decimal import Decimal as _Decimal
    from decimal import InvalidOperation as _InvalidOperation

    max_value_usd_env = os.environ.get("ALMANAK_MAX_VALUE_USD") or os.environ.get("MAX_VALUE_USD")
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
    "--id",
    "strategy_id_override",
    help="Strategy instance ID to resume a previous run.",
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
@click.option(
    "--gateway-host",
    default="localhost",
    envvar="GATEWAY_HOST",
    help="Gateway sidecar hostname",
)
@click.option(
    "--gateway-port",
    default=50051,
    type=int,
    envvar="GATEWAY_PORT",
    help="Gateway sidecar port",
)
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
def run(
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
    strategy_id_override: str | None = None,
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

        # Resume a previous run
        almanak strat run --id abc123 --once

        # Fresh start (clear stale state, useful for Anvil forks)
        almanak strat run --fresh --once

        # List registered strategies
        almanak strat run --list
    """
    # Configure logging + validate setup-stage flag combinations (phase 1 helper).
    import atexit

    from .run_helpers import (
        _build_cleanup_fn,
        _build_components,
        _build_runtime_config,
        _configure_logging_and_validate,
        _detect_state_resume,
        _discover_and_load_config,
        _DryRunVaultEarlyExit,
        _handle_list_all,
        _handle_standalone_dashboard,
        _instantiate_strategy,
        _load_strategy_class,
        _print_startup_banner,
        _resolve_identity,
        _run_continuous,
        _run_once,
        _setup_gateway,
        _start_dashboard_background,
        _stop_dashboard,
        _wire_token_resolver,
    )

    _configure_logging_and_validate(
        verbose=verbose,
        debug=debug,
        log_file=log_file,
        once=once,
        teardown_after=teardown_after,
    )

    # VIB-3761: anchor every local artifact (DB, logs, lock) to the
    # strategy's folder so 10 strategies launched from the same cwd cannot
    # collide on a shared ./almanak_state.db (the April 29 silent-failure
    # root cause). The env var is set ONLY when the operator did not
    # already set it explicitly so test/operator overrides win.
    _resolved_strategy_folder = Path(working_dir).expanduser().resolve()
    if _resolved_strategy_folder.is_dir() and not os.environ.get("ALMANAK_STRATEGY_FOLDER"):
        os.environ["ALMANAK_STRATEGY_FOLDER"] = str(_resolved_strategy_folder)

    # Gateway setup (phase 2 helper): managed auto-start or external connect.
    (
        gateway_client,
        managed_gateway,
        effective_host,
        gateway_port,
        gateway_network,
        _session_auth_token,
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

    # Wire gateway channel into TokenResolver for on-chain token discovery (phase 3 helper).
    _wire_token_resolver(gateway_client)

    click.echo()

    # Handle --list flag (phase 4 helper)
    if _handle_list_all(list_all, gateway_client):
        return

    # If only --dashboard is provided without a working directory, launch dashboard and block (phase 5 helper)
    if _handle_standalone_dashboard(
        working_dir=working_dir,
        dashboard=dashboard,
        dashboard_port=dashboard_port,
        gateway_host=effective_host,
        gateway_port=gateway_port,
    ):
        return

    # Start dashboard as background subprocess (after gateway is healthy, before strategy runs)
    dashboard_process = None
    if dashboard:
        dashboard_process = _start_dashboard_background(
            port=dashboard_port,
            gateway_host=effective_host,
            gateway_port=gateway_port,
        )
        if dashboard_process is not None:
            atexit.register(_stop_dashboard, dashboard_process)

    # Load strategy from working directory (reuse early-loaded class if available) (phase 6 helper)
    strategy_class: type[IntentStrategy[Any]] = _load_strategy_class(working_dir, _early_strategy_class)
    strategy_name = strategy_class.__name__
    click.echo(f"Loaded strategy: {strategy_name}")

    # Use provided instance ID if given
    provided_instance_id = strategy_id_override

    # Preliminary: get strategy chains from decorator (may be refined after config load)
    strategy_chains = get_strategy_chains(strategy_class)
    multi_chain = False  # Determined after config load
    strategy_protocols = get_strategy_protocols(strategy_class)

    # Auto-discover config, load it, and apply copy-trading overrides (phase 7 helper)
    (
        strategy_config,
        multi_chain,
        effective_dry_run,
        config_file,
        normalized_copy_mode,
    ) = _discover_and_load_config(
        working_dir=working_dir,
        config_file=config_file,
        strategy_class=strategy_class,
        copy_mode=copy_mode,
        copy_shadow=copy_shadow,
        copy_replay_file=copy_replay_file,
        copy_strict=copy_strict,
        dry_run=dry_run,
    )
    if multi_chain:
        # Use chains from config if specified, else fall back to decorator
        config_chains = strategy_config.get("chains", [])
        if isinstance(config_chains, list) and len(config_chains) > 1:
            strategy_chains = config_chains

    # --- Three-tier identity model (VIB-2764) ---
    # strategy_name: human/code reference (display name).
    # deployment_id: stable primary key for all DB tables (survives restarts).
    # run_id: per-process ephemeral UUID (forensics only).
    # (deployment_id/run_id resolution itself happens in _resolve_identity below.)
    config_display_name = strategy_config.get("strategy_id", strategy_name)
    if ":" in config_display_name:
        config_display_name = config_display_name.split(":")[0]
    strategy_config["strategy_display_name"] = config_display_name

    # deployment_id is resolved after wallet/chain are known (below).
    # For now, stash the cli_id for the resolver.
    _cli_id_override = provided_instance_id

    # See resolve_strategy_chain(): single-chain strategies always use their
    # declared chain; multi-supported strategies use env > config.json > default.
    env_chain = (os.environ.get("ALMANAK_CHAIN") or "").strip().lower() or None
    config_chain = resolve_strategy_chain(
        strategy_class,
        strategy_config,
        env_chain=env_chain,
        multi_chain=multi_chain,
    )
    _cfg_chain_raw = strategy_config.get("chain")
    _cfg_chain_norm = _cfg_chain_raw.strip().lower() if isinstance(_cfg_chain_raw, str) else ""
    # Only echo the override message when env was actually used to select the
    # chain (i.e. the resolved chain matches env). For single-chain strategies
    # the env is silently ignored and resolve_strategy_chain() returns the
    # strategy's declared chain — no override message should appear in that case.
    if env_chain and config_chain == env_chain and env_chain != _cfg_chain_norm:
        click.echo(f"Chain override: ALMANAK_CHAIN={env_chain} (config.json: {_cfg_chain_raw or 'unset'})")

    # Determine network: CLI flag > config.json > default "mainnet"
    # Priority: --network flag (highest) > config "network" field > "mainnet" (default)
    resolved_network = network or "mainnet"
    if resolved_network == "anvil":
        anvil_port = os.environ.get(f"ANVIL_{(config_chain or 'arbitrum').upper()}_PORT", "8545")
        click.echo(f"Network: ANVIL (local fork at http://127.0.0.1:{anvil_port})")

    # Runtime config wiring (phase 12 helper): sidecar | multi-chain | single-chain.
    runtime_config, chain_wallets = _build_runtime_config(
        no_gateway=no_gateway,
        multi_chain=multi_chain,
        resolved_network=resolved_network,
        config_chain=config_chain,
        strategy_chains=strategy_chains,
        strategy_protocols=strategy_protocols,
        gateway_client=gateway_client,
        strategy_config=strategy_config,
    )

    # Resolve identity + backfill + --fresh state deletion (phase 8 helper).
    identity_info = _resolve_identity(
        strategy_config=strategy_config,
        fresh=fresh,
        multi_chain=multi_chain,
        strategy_chains=strategy_chains,
        config_display_name=config_display_name,
        cli_id_override=_cli_id_override,
        gateway_network=gateway_network,
    )
    run_id = identity_info.run_id
    strategy_id = strategy_config["strategy_id"]

    # Detect RESUME vs FRESH START (phase 9 helper).
    # VIB-3761: canonical local-DB resolver.
    #
    # Hosted mode (AGENT_ID set) keeps state in Postgres via the gateway state
    # manager — there is no local SQLite file to inspect. Calling local_db_path
    # in hosted mode raises LocalPathError by design (see local_paths._ensure_local).
    # Resume semantics for hosted strategies are handled by the gateway against
    # Postgres, not by the runner CLI.
    from almanak.framework.deployment import is_local
    from almanak.framework.local_paths import local_db_path as _local_db_path

    is_resume = False
    existing_state_info: dict[str, Any] | None = None
    if is_local():
        state_db_path = _local_db_path()
        resume_info = _detect_state_resume(state_db_path, strategy_id)
        is_resume = resume_info.is_resume
        existing_state_info = {"version": resume_info.version, "keys": resume_info.state_keys} if is_resume else None

    # Display startup information (phase 10 helper)
    _print_startup_banner(
        strategy_name=strategy_name,
        strategy_id=strategy_id,
        run_id=run_id,
        is_resume=is_resume,
        existing_state_info=existing_state_info,
        once=once,
        fresh=fresh,
        multi_chain=multi_chain,
        strategy_chains=strategy_chains,
        strategy_protocols=strategy_protocols,
        runtime_config=runtime_config,
        interval=interval,
        max_iterations=max_iterations,
        effective_dry_run=effective_dry_run,
        strategy_config=strategy_config,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        dashboard=dashboard,
    )

    # Strategy class already loaded above
    click.echo(f"Strategy class loaded: {strategy_class.__name__}")
    if multi_chain:
        click.echo(f"  Multi-chain: Yes ({len(strategy_chains)} chains)")

    # Instantiate the strategy (phase 11 helper).
    strategy_instance = _instantiate_strategy(
        strategy_class=strategy_class,
        strategy_config=strategy_config,
        runtime_config=runtime_config,
        multi_chain=multi_chain,
        strategy_chains=strategy_chains,
        chain_wallets=chain_wallets,
    )

    # Build components (phase 13 helper): orchestrator + providers + copy-trading +
    # state manager + vault auto-deploy + StrategyRunner. Ordering is load-bearing;
    # see `_build_components` docstring.
    try:
        components = _build_components(
            strategy_instance=strategy_instance,
            strategy_config=strategy_config,
            runtime_config=runtime_config,
            strategy_chains=strategy_chains,
            multi_chain=multi_chain,
            resolved_network=resolved_network,
            gateway_client=gateway_client,
            chain_wallets=chain_wallets,
            interval=interval,
            effective_dry_run=effective_dry_run,
            strategy_id=strategy_id,
            normalized_copy_mode=normalized_copy_mode,
            copy_replay_file=copy_replay_file,
            copy_shadow=copy_shadow,
            copy_strict=copy_strict,
            config_chain=config_chain,
        )
    except _DryRunVaultEarlyExit as early:
        # --dry-run + placeholder vault on Anvil: skip runner construction but
        # still unwind providers/gateway/Solana-fork via cleanup_fn (see #1682).
        import asyncio as _asyncio

        from ._run_context import ComponentBundle as _ComponentBundle

        partial_components = early.components or _ComponentBundle()
        early_cleanup = _build_cleanup_fn(
            gateway_client=gateway_client,
            managed_gateway=managed_gateway,
            keep_anvil=keep_anvil,
            components=partial_components,
        )
        try:
            _asyncio.run(early_cleanup())
        except Exception:  # pragma: no cover - cleanup best-effort
            logger.exception("Cleanup failed during dry-run vault early exit")
        _stop_dashboard(dashboard_process)
        sys.exit(0)
    runner = components.runner
    state_manager = components.state_manager

    # Build cleanup closure (phase 14 helper).
    cleanup_resources = _build_cleanup_fn(
        gateway_client=gateway_client,
        managed_gateway=managed_gateway,
        keep_anvil=keep_anvil,
        components=components,
    )

    # Run strategy (phase 15/16 helpers).
    if test_actions is not None:
        from .run_helpers import _run_test_lifecycle

        exit_code = _run_test_lifecycle(
            runner=runner,
            strategy_instance=strategy_instance,
            state_manager=state_manager,
            cleanup_fn=cleanup_resources,
            actions=test_actions,
            teardown=teardown_after,
            json_output=test_json,
        )
    elif once:
        exit_code = _run_once(
            runner=runner,
            strategy_instance=strategy_instance,
            state_manager=state_manager,
            cleanup_fn=cleanup_resources,
            teardown_after=teardown_after,
        )
    else:
        exit_code = _run_continuous(
            runner=runner,
            strategy_instance=strategy_instance,
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
