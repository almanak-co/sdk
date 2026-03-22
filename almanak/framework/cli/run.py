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

from ..data.balance.gateway_provider import GatewayBalanceProvider
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
from ..data.ohlcv.gateway_data_adapter import GatewayOHLCVDataProvider
from ..data.ohlcv.gateway_provider import GatewayOHLCVProvider
from ..data.ohlcv.ohlcv_router import OHLCVRouter
from ..data.ohlcv.routing_provider import RoutingOHLCVProvider
from ..data.price.gateway_oracle import GatewayPriceOracle
from ..execution.config import (
    GatewayRuntimeConfig,
    LocalRuntimeConfig,
    MissingEnvironmentVariableError,
    MultiChainRuntimeConfig,
)
from ..execution.gas.constants import CHAIN_GAS_PRICE_CAPS_GWEI, DEFAULT_GAS_PRICE_CAP_GWEI
from ..execution.multichain import MultiChainOrchestrator
from ..execution.orchestrator import ExecutionOrchestrator
from ..execution.signer.local import LocalKeySigner
from ..execution.simulator import create_simulator
from ..execution.submitter.public import PublicMempoolSubmitter
from ..runner import IterationResult, IterationStatus, RunnerConfig, StrategyRunner
from ..strategies import IntentStrategy, get_strategy, list_strategies
from ..strategies.intent_strategy import IndicatorProvider
from .intent_debug import load_strategy_from_file

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
    tier_dirs = ["poster_child", "production", "incubating", "demo", "alpha_team", "tests"]

    # Add tiered paths: strategies/<tier>/<name>
    for tier in tier_dirs:
        search_paths.append(Path("strategies") / tier / strategy_name)

    # Handle demo_ prefix -> demo/<name> directory structure (backward compat)
    if strategy_name.startswith("demo_"):
        subdir_name = strategy_name[5:]  # Remove "demo_" prefix
        search_paths.insert(0, Path("strategies/demo") / subdir_name)

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
            return config

        # Try YAML
        for yaml_name in ["config.yaml", "config.yml"]:
            config_path = strategy_dir / yaml_name
            if config_path.exists():
                import yaml

                with open(config_path) as f:
                    config = yaml.safe_load(f)
                click.echo(f"Loaded config from: {config_path}")
                return config

    # Return minimal default config with generated UUID
    return {
        "strategy_id": f"{strategy_name}:{uuid.uuid4().hex[:12]}",
    }


def get_default_chain(strategy_class: type) -> str:
    """Get the default chain for a strategy from decorator metadata.

    Reads STRATEGY_METADATA.default_chain, falling back to supported_chains[0],
    then to "arbitrum" as a last resort.
    """
    metadata = getattr(strategy_class, "STRATEGY_METADATA", None)
    if metadata:
        if metadata.default_chain:
            return metadata.default_chain
        if metadata.supported_chains:
            return metadata.supported_chains[0]
    # Legacy fallback
    supported = getattr(strategy_class, "SUPPORTED_CHAINS", None)
    if supported:
        return supported[0]
    return "arbitrum"


def create_price_oracle(
    config: LocalRuntimeConfig,
) -> PriceOracle:
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


def create_sync_price_oracle_func(
    price_oracle: PriceOracle,
) -> Callable[[str, str], Decimal]:
    """Create a sync callable wrapper for an async PriceOracle.

    Args:
        price_oracle: Async PriceOracle implementation

    Returns:
        Sync callable (token, quote) -> Decimal
    """
    import asyncio

    def sync_price(token: str, quote: str = "USD") -> Decimal:
        """Fetch price synchronously."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            # We're in an async context - use nest_asyncio
            import nest_asyncio

            nest_asyncio.apply()
            result = asyncio.get_event_loop().run_until_complete(price_oracle.get_aggregated_price(token, quote))
        else:
            result = asyncio.run(price_oracle.get_aggregated_price(token, quote))

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

    router = OHLCVRouter(default_chain=chain)
    router.register_provider(binance_adapter)

    pool_address = strategy_config.get("pool_address")
    return RoutingOHLCVProvider(
        router=router,
        chain=chain,
        pool_address=str(pool_address) if pool_address else None,
    )


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
    rsi_calculator = RSICalculator(ohlcv_provider=ohlcv_provider)
    macd_calculator = MACDCalculator(ohlcv_provider=ohlcv_provider)
    bb_calculator = BollingerBandsCalculator(ohlcv_provider=ohlcv_provider)
    stoch_calculator = StochasticCalculator(ohlcv_provider=ohlcv_provider)
    atr_calculator = ATRCalculator(ohlcv_provider=ohlcv_provider)
    ma_calculator = MovingAverageCalculator(ohlcv_provider=ohlcv_provider)
    adx_calculator = ADXCalculator(ohlcv_provider=ohlcv_provider)
    obv_calculator = OBVCalculator(ohlcv_provider=ohlcv_provider)
    cci_calculator = CCICalculator(ohlcv_provider=ohlcv_provider)
    ichimoku_calculator = IchimokuCalculator(ohlcv_provider=ohlcv_provider)

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
        click.echo("  Providers injected into strategy (RSI + full indicator suite incl. ADX/OBV/CCI/Ichimoku)")


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

    return ExecutionOrchestrator(
        signer=signer,
        submitter=submitter,
        simulator=simulator,
        chain=config.chain,
        rpc_url=config.rpc_url,
        tx_risk_config=tx_risk_config,
    )


def is_multi_chain_strategy(strategy_class: type) -> bool:
    """Check if a strategy class supports multiple chains.

    Args:
        strategy_class: The strategy class to check

    Returns:
        True if strategy has SUPPORTED_CHAINS with multiple chains
    """
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
        cd strategies/demo/uniswap_rsi
        almanak strat run --once

        # Run with explicit working directory
        almanak strat run -d strategies/demo/uniswap_rsi --once

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
    # Configure logging using structured logging
    from ..utils.logging import LogFormat, LogLevel, add_file_handler, configure_logging

    # Determine log level: debug > verbose > default (info)
    if debug:
        log_level = LogLevel.DEBUG
    elif verbose:
        log_level = LogLevel.DEBUG  # Verbose shows DEBUG for src.* modules
    else:
        log_level = LogLevel.INFO

    # Use console format for human-readable output
    configure_logging(level=log_level, format=LogFormat.CONSOLE)

    # Add JSON file handler if --log-file is specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        add_file_handler(str(log_path), level=LogLevel.DEBUG)
        click.echo(f"Logging to file: {log_path} (JSON format)")

    # Control third-party logger verbosity based on --debug flag
    # By default, suppress Web3/HTTP noise unless --debug is specified
    if not debug:
        # Suppress third-party debug logs (keep only WARNING+)
        logging.getLogger("web3").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("aiohttp").setLevel(logging.WARNING)
        logging.getLogger("asyncio").setLevel(logging.WARNING)
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
    else:
        # --debug flag: allow all debug logs including third-party
        logging.getLogger("web3").setLevel(logging.DEBUG)
        logging.getLogger("urllib3").setLevel(logging.DEBUG)

    # Validate --teardown-after requires --once
    if teardown_after and not once:
        click.echo("Error: --teardown-after requires --once.", err=True)
        sys.exit(1)

    # Gateway setup: auto-start a managed gateway or connect to an existing one
    import atexit

    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.managed import ManagedGateway, find_available_gateway_port

    from ..gateway_client import GatewayClient, GatewayClientConfig

    # Normalize "localhost" to "127.0.0.1" (gateway binds to 127.0.0.1)
    effective_host = "127.0.0.1" if gateway_host == "localhost" else gateway_host

    managed_gateway: ManagedGateway | None = None
    _early_strategy_class: type[IntentStrategy[Any]] | None = None

    # --wallet isolated requires a managed gateway (derives wallet + funds Anvil fork)
    if wallet == "isolated" and no_gateway:
        raise click.ClickException(
            "--wallet isolated requires a managed gateway (incompatible with --no-gateway). "
            "Remove --no-gateway to let the CLI start its own gateway + Anvil fork."
        )

    if no_gateway:
        if anvil_ports:
            raise click.ClickException("--anvil-port requires a managed gateway (remove --no-gateway).")
        if keep_anvil:
            raise click.ClickException("--keep-anvil requires a managed gateway (remove --no-gateway).")
        # --wallet isolated requires the managed gateway (which auto-funds the derived wallet)
        if wallet == "isolated":
            raise click.ClickException(
                "--wallet isolated requires a managed gateway (remove --no-gateway). "
                "The managed gateway auto-funds the derived wallet on Anvil."
            )

        # --no-gateway: connect to an existing gateway, fail if unavailable
        click.echo(f"Connecting to existing gateway at {effective_host}:{gateway_port}...")
        auth_token = os.environ.get("GATEWAY_AUTH_TOKEN")
        gateway_config = GatewayClientConfig(host=effective_host, port=gateway_port, auth_token=auth_token)
        gateway_client = GatewayClient(gateway_config)
        gateway_client.connect()

        click.echo("Waiting for gateway to become ready...")
        if not gateway_client.wait_for_ready(timeout=60.0, interval=5.0):
            gateway_client.disconnect()
            click.echo()
            click.secho("ERROR: Gateway is not running or not healthy", fg="red", bold=True)
            click.echo()
            click.echo("The gateway sidecar is required for all strategy operations.")
            click.echo("Start the gateway first with:")
            click.echo()
            click.echo("  almanak gateway")
            click.echo()
            raise click.ClickException(f"Gateway not available at {effective_host}:{gateway_port}")

        click.secho(f"Connected to existing gateway at {effective_host}:{gateway_port}", fg="green")
    else:
        # Default: auto-start a managed gateway
        try:
            gateway_port = find_available_gateway_port(effective_host, gateway_port)
        except RuntimeError as e:
            click.echo()
            click.secho(f"ERROR: {e}", fg="red", bold=True)
            click.echo()
            click.echo("Set a specific port with:")
            click.echo()
            click.echo("  almanak strat run --gateway-port <port>")
            click.echo()
            click.echo("Or connect to an existing gateway:")
            click.echo()
            click.echo("  almanak strat run --no-gateway --gateway-port <port>")
            click.echo()
            raise click.ClickException(str(e)) from None

        # Parse --anvil-port values into dict
        external_anvil_ports: dict[str, int] = {}
        for entry in anvil_ports:
            if "=" not in entry:
                raise click.ClickException(
                    f"Invalid --anvil-port format: '{entry}'. Expected CHAIN=PORT (e.g., arbitrum=8545)"
                )
            chain_name, port_str = entry.split("=", 1)
            chain_name = chain_name.strip().lower()
            if not chain_name:
                raise click.ClickException(f"Invalid --anvil-port format: '{entry}'. Chain name cannot be empty.")
            try:
                port = int(port_str)
            except ValueError:
                raise click.ClickException(f"Invalid port in --anvil-port: '{port_str}'") from None
            if not (1 <= port <= 65535):
                raise click.ClickException(f"Invalid port in --anvil-port: '{port_str}'. Expected 1-65535.")
            if chain_name in external_anvil_ports:
                raise click.ClickException(f"Duplicate --anvil-port for chain '{chain_name}'.")
            external_anvil_ports[chain_name] = port

        # Resolve network for the managed gateway (CLI flag only, default mainnet)
        # Auto-infer anvil network when --anvil-port is provided
        if external_anvil_ports and not network:
            network = "anvil"
        gateway_network = network or "mainnet"

        if keep_anvil and gateway_network != "anvil":
            click.echo("Warning: --keep-anvil has no effect without --network anvil or --anvil-port.")

        # Early-load strategy class so decorator metadata is available for chain detection.
        # This must happen before gateway startup so Anvil forks target the correct chain.
        _early_strategy_file = Path(working_dir) / "strategy.py"
        if _early_strategy_file.exists():
            _early_strategy_class, _early_err = load_strategy_from_file(_early_strategy_file)
            if _early_err:
                logger.debug(f"Early strategy load failed (will retry later): {_early_err}")

        # Determine which chains need Anvil forks
        anvil_chains: list[str] = []
        anvil_funding: dict[str, float | int | str] = {}
        if gateway_network == "anvil":
            # Quick-read config for chain info and anvil_funding
            resolved_config_path: Path | None = Path(config_file) if config_file else None
            if resolved_config_path is None:
                for name in ["config.json", "config.yaml", "config.yml"]:
                    candidate = Path(working_dir) / name
                    if candidate.exists():
                        resolved_config_path = candidate
                        break
            if resolved_config_path and resolved_config_path.exists():
                with open(resolved_config_path) as f:
                    if resolved_config_path.suffix.lower() in [".yaml", ".yml"]:
                        import yaml

                        quick_config = yaml.safe_load(f)
                    else:
                        quick_config = json.load(f)
                chain_val = quick_config.get("chain")
                chains_val = quick_config.get("chains")
                if chains_val:
                    anvil_chains = chains_val
                elif chain_val:
                    anvil_chains = [chain_val]
                anvil_funding = quick_config.get("anvil_funding", {})

            # Fall back to decorator metadata if config.json has no chain
            if not anvil_chains and _early_strategy_class:
                decorator_chain = get_default_chain(_early_strategy_class)
                if decorator_chain:
                    anvil_chains = [decorator_chain]

            # Add externally-specified chains to anvil_chains if not already present
            for ext_chain in external_anvil_ports:
                if ext_chain not in anvil_chains:
                    anvil_chains.append(ext_chain)

            # Solana uses solana-test-validator (not Anvil); filter it out so
            # ManagedGateway doesn't try to start a RollingForkManager for it.
            # The Solana fork is started separately below in the runner setup.
            NON_EVM_CHAINS = {"solana"}
            evm_anvil_chains = [c for c in anvil_chains if c.lower() not in NON_EVM_CHAINS]
            solana_anvil = any(c.lower() in NON_EVM_CHAINS for c in anvil_chains)
            anvil_chains = evm_anvil_chains

            if not anvil_chains and not solana_anvil:
                click.echo(
                    "Warning: --network anvil specified but no chain found in config or decorator. "
                    "Gateway will start without Anvil forks."
                )

        # Wallet isolation: derive a unique wallet per strategy on Anvil
        isolated_wallet_address: str | None = None
        if wallet == "isolated" and gateway_network == "anvil":
            from almanak.gateway.managed import derive_isolated_wallet

            master_key = os.environ.get("ALMANAK_PRIVATE_KEY", "")
            if not master_key:
                raise click.ClickException("--wallet isolated requires ALMANAK_PRIVATE_KEY to be set")
            # Use the strategy directory name as the derivation seed
            strategy_seed = Path(working_dir).resolve().name
            derived_key, isolated_wallet_address = derive_isolated_wallet(master_key, strategy_seed)
            # Override the env var so LocalRuntimeConfig.from_env() picks up the derived key
            os.environ["ALMANAK_PRIVATE_KEY"] = derived_key
            click.echo(
                f"Wallet: isolated ({isolated_wallet_address[:10]}...{isolated_wallet_address[-4:]}) "
                f"[derived from strategy '{strategy_seed}']"
            )
        elif wallet == "isolated" and gateway_network != "anvil":
            raise click.ClickException("--wallet isolated is only supported with --network anvil")

        # Validate --reset-fork requires --network anvil
        if reset_fork and gateway_network != "anvil":
            raise click.ClickException("--reset-fork is only supported with --network anvil")
        if reset_fork and once:
            click.echo("Note: --reset-fork has no effect with --once (fork is already fresh at startup)")

        # When using isolated wallets, pass the derived key to the gateway so its
        # signer matches the funded wallet. GatewaySettings reads ALMANAK_GATEWAY_PRIVATE_KEY
        # (not ALMANAK_PRIVATE_KEY), so we must pass it explicitly.
        gateway_private_key = os.environ.get("ALMANAK_PRIVATE_KEY") if isolated_wallet_address else None

        # Ensure gateway knows the strategy's chain for on-chain pricing.
        # For anvil mode, anvil_chains is already populated above.
        # For mainnet, read chain from config or decorator metadata so the MarketService
        # uses the correct Chainlink oracle chain instead of defaulting to arbitrum.
        gateway_chains = anvil_chains
        if not gateway_chains:
            resolved_config_path_gw: Path | None = Path(config_file) if config_file else None
            if resolved_config_path_gw is None:
                for name in ["config.json", "config.yaml", "config.yml"]:
                    candidate = Path(working_dir) / name
                    if candidate.exists():
                        resolved_config_path_gw = candidate
                        break
            if resolved_config_path_gw and resolved_config_path_gw.exists():
                with open(resolved_config_path_gw) as f:
                    if resolved_config_path_gw.suffix.lower() in [".yaml", ".yml"]:
                        import yaml

                        _quick = yaml.safe_load(f)
                    else:
                        _quick = json.load(f)
                    if not isinstance(_quick, dict):
                        _quick = {}
                    _chains_val = _quick.get("chains")
                    _chain_val = _quick.get("chain")
                    if _chains_val:
                        gateway_chains = [_chains_val] if isinstance(_chains_val, str) else list(_chains_val)
                    elif _chain_val:
                        gateway_chains = [_chain_val]

            # Fall back to decorator metadata if config has no chain
            if not gateway_chains and _early_strategy_class:
                decorator_chain = get_default_chain(_early_strategy_class)
                if decorator_chain:
                    gateway_chains = [decorator_chain]

        # Security: generate a random session token for the managed gateway so it
        # is never running without authentication, even on mainnet (VIB-520).
        # For anvil/sepolia we still use allow_insecure for convenience.
        is_test_network = gateway_network in ("anvil", "sepolia")
        session_auth_token = None if is_test_network else uuid.uuid4().hex

        gateway_kwargs: dict[str, Any] = {
            "grpc_host": effective_host,
            "grpc_port": gateway_port,
            "network": gateway_network,
            "allow_insecure": is_test_network,
            "metrics_enabled": False,
            "audit_enabled": False,
            "chains": gateway_chains,
        }
        if session_auth_token:
            gateway_kwargs["auth_token"] = session_auth_token
        if gateway_private_key:
            gateway_kwargs["private_key"] = gateway_private_key
        gateway_settings = GatewaySettings(**gateway_kwargs)

        if anvil_chains:
            click.echo(
                f"Starting managed gateway on {effective_host}:{gateway_port} "
                f"(network={gateway_network}, anvil chains: {', '.join(anvil_chains)})..."
            )
        else:
            click.echo(f"Starting managed gateway on {effective_host}:{gateway_port} (network={gateway_network})...")
        managed_gateway = ManagedGateway(
            gateway_settings,
            anvil_chains=anvil_chains,
            wallet_address=isolated_wallet_address,
            anvil_funding=anvil_funding,
            external_anvil_ports=external_anvil_ports,
            keep_anvil=keep_anvil,
        )
        # Anvil forks need extra startup time (forking from mainnet RPC)
        startup_timeout = 30.0 if anvil_chains else 10.0
        try:
            managed_gateway.start(timeout=startup_timeout)
        except RuntimeError as e:
            click.echo()
            click.secho(f"ERROR: Failed to start managed gateway: {e}", fg="red", bold=True)
            click.echo()
            raise click.ClickException("Managed gateway startup failed") from e

        # Register atexit handler as safety net for sys.exit() paths that skip cleanup
        atexit.register(managed_gateway.stop)

        click.secho(f"Managed gateway started on {effective_host}:{gateway_port}", fg="green")

        # Connect client to the managed gateway (use same session token)
        gateway_config = GatewayClientConfig(host=effective_host, port=gateway_port, auth_token=session_auth_token)
        gateway_client = GatewayClient(gateway_config)
        gateway_client.connect()

        click.echo("Waiting for gateway to become ready...")
        if not gateway_client.wait_for_ready(timeout=60.0, interval=5.0):
            managed_gateway.stop()
            gateway_client.disconnect()
            raise click.ClickException(
                "Managed gateway started but health check failed. Check gateway logs above for details."
            )

    # Wire gateway channel into TokenResolver for on-chain token discovery
    from ..data.tokens import get_token_resolver

    resolver = get_token_resolver()
    resolver.set_gateway_channel(gateway_client.channel)

    click.echo()

    # Handle --list flag
    if list_all:
        available = list_strategies()
        if available:
            click.echo("Registered strategies:")
            for name in sorted(available):
                # Mark multi-chain strategies
                try:
                    strat_class = get_strategy(name)
                    if is_multi_chain_strategy(strat_class):
                        chains = get_strategy_chains(strat_class)
                        click.echo(f"  - {name} [multi-chain: {', '.join(chains)}]")
                    else:
                        click.echo(f"  - {name}")
                except Exception:
                    click.echo(f"  - {name}")
        else:
            click.echo("No strategies registered in the factory.")
        click.echo()
        click.echo("To run a strategy, cd into its directory and run:")
        click.echo("  almanak strat run --once")
        return

    # Dashboard helpers (defined early for dashboard-only mode)
    def start_dashboard_background(
        port: int,
        gw_host: str = "127.0.0.1",
        gw_port: int = 50051,
    ) -> Any:
        """Launch the Streamlit dashboard as a background subprocess.

        Returns the Popen process object, or None if launch failed.
        """
        import socket
        import subprocess

        # Check if port is available
        def is_port_available(p: int) -> bool:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(("localhost", p))
                    return True
                except OSError:
                    return False

        # Try to find an available port if the requested one is in use
        actual_port = port
        if not is_port_available(actual_port):
            click.echo(f"Warning: Dashboard port {actual_port} is already in use.", err=True)
            for alt_port in range(8502, 8510):
                if is_port_available(alt_port):
                    actual_port = alt_port
                    click.echo(f"Using alternative dashboard port: {actual_port}", err=True)
                    break
            else:
                click.echo(
                    f"Error: Could not find an available port for dashboard. "
                    f"Please free up port {port} or specify a different port with --dashboard-port",
                    err=True,
                )
                return None

        project_root = Path(__file__).parent.parent.parent.parent
        dashboard_path = project_root / "almanak" / "framework" / "dashboard" / "app.py"

        # Pass gateway connection info to the dashboard subprocess
        env = os.environ.copy()
        env["GATEWAY_HOST"] = gw_host
        env["GATEWAY_PORT"] = str(gw_port)

        try:
            process = subprocess.Popen(
                [
                    "streamlit",
                    "run",
                    str(dashboard_path),
                    "--server.port",
                    str(actual_port),
                    "--server.headless",
                    "false",
                ],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            click.echo(f"Dashboard started at http://localhost:{actual_port}")
            return process
        except FileNotFoundError:
            click.echo("Error: streamlit not found. Install with: pip install streamlit", err=True)
            return None
        except Exception as e:
            click.echo(f"Error launching dashboard: {e}", err=True)
            return None

    def stop_dashboard(process: Any) -> None:
        """Terminate the background dashboard process (best-effort)."""
        if process is None:
            return
        try:
            process.terminate()
            process.wait(timeout=5)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass

    # If only --dashboard is provided without a working directory, launch dashboard and block
    if dashboard and working_dir == ".":
        click.echo()
        click.echo("=" * 60)
        click.echo("LAUNCHING DASHBOARD (standalone mode)")
        click.echo("=" * 60)
        click.echo("Press Ctrl+C to stop")
        proc = start_dashboard_background(dashboard_port, effective_host, gateway_port)
        if proc is None:
            sys.exit(1)
        try:
            proc.wait()
        except KeyboardInterrupt:
            stop_dashboard(proc)
            click.echo("Dashboard stopped.")
        return

    # Start dashboard as background subprocess (after gateway is healthy, before strategy runs)
    dashboard_process = None
    if dashboard:
        dashboard_process = start_dashboard_background(
            port=dashboard_port,
            gw_host=effective_host,
            gw_port=gateway_port,
        )
        if dashboard_process is not None:
            atexit.register(stop_dashboard, dashboard_process)

    # Load strategy from working directory (reuse early-loaded class if available)
    strategy_file = Path(working_dir) / "strategy.py"
    if not strategy_file.exists():
        click.echo(f"Error: No strategy.py found in {working_dir}", err=True)
        click.echo()
        click.echo("Make sure you're in a strategy directory or use --working-dir:")
        click.echo("  almanak strat run -d strategies/demo/uniswap_rsi --once")
        sys.exit(1)

    if _early_strategy_class is not None:
        loaded_class: type[IntentStrategy[Any]] = _early_strategy_class
    else:
        _loaded, error = load_strategy_from_file(strategy_file)
        if not _loaded:
            click.echo(f"Error loading strategy from {strategy_file}: {error}", err=True)
            sys.exit(1)
        loaded_class = _loaded

    strategy_class: type[IntentStrategy[Any]] = loaded_class
    strategy_name = loaded_class.__name__
    click.echo(f"Loaded strategy: {strategy_name}")

    # Use provided instance ID if given
    provided_instance_id = strategy_id_override

    # Check if strategy is multi-chain
    multi_chain = is_multi_chain_strategy(strategy_class)
    strategy_chains = get_strategy_chains(strategy_class)
    strategy_protocols = get_strategy_protocols(strategy_class)

    # Auto-discover config file from working directory if not explicitly provided
    if not config_file:
        for candidate_name in ["config.json", "config.yaml", "config.yml"]:
            candidate = Path(working_dir) / candidate_name
            if candidate.exists():
                config_file = str(candidate)
                break

    # Load strategy configuration FIRST to get chain (if specified)
    try:
        strategy_config = load_strategy_config(strategy_name, config_file)
    except Exception as e:
        click.echo(f"Error loading strategy config: {e}", err=True)
        sys.exit(1)

    normalized_copy_mode = copy_mode.lower() if copy_mode is not None else None

    # Apply copy-trading runtime overrides from CLI flags.
    if any([normalized_copy_mode, copy_shadow, copy_replay_file, copy_strict]):
        ct_config = strategy_config.get("copy_trading")
        if ct_config is None:
            ct_config = {}
            strategy_config["copy_trading"] = ct_config
        if not isinstance(ct_config, dict):
            raise click.ClickException("copy_trading config must be an object when using copy override flags")

        execution_policy = dict(ct_config.get("execution_policy", {}))
        if normalized_copy_mode is not None:
            execution_policy["copy_mode"] = normalized_copy_mode
        if copy_shadow:
            execution_policy["shadow"] = True
            execution_policy["copy_mode"] = "shadow"
        if copy_replay_file:
            execution_policy["replay_file"] = copy_replay_file
            execution_policy["copy_mode"] = "replay"
        if copy_strict:
            execution_policy["strict"] = True

        ct_config["execution_policy"] = execution_policy

    effective_dry_run = dry_run or copy_shadow or (normalized_copy_mode in {"shadow", "replay"})

    # Ensure strategy_id is always unique per instance.
    # The config's strategy_id (or strategy_name) becomes the display name.
    # A unique runtime ID is always generated unless resuming with --id.
    strategy_id_generated = False
    if provided_instance_id:
        # User provided an ID to resume - use it exactly (format: name:id)
        if ":" in provided_instance_id:
            strategy_config["strategy_id"] = provided_instance_id
            strategy_config["strategy_display_name"] = provided_instance_id.split(":")[0]
        else:
            strategy_config["strategy_id"] = f"{strategy_name}:{provided_instance_id}"
            strategy_config["strategy_display_name"] = strategy_name
    else:
        # Strip any existing UUID suffix from config (load_strategy_config may generate one).
        config_display_name = strategy_config.get("strategy_id", strategy_name)
        if ":" in config_display_name:
            config_display_name = config_display_name.split(":")[0]

        if once:
            # For --once runs, use a stable ID so state persists across reruns.
            strategy_config["strategy_id"] = config_display_name
        else:
            # For continuous runs, generate a unique runtime ID to prevent collisions.
            strategy_config["strategy_id"] = f"{config_display_name}:{uuid.uuid4().hex[:12]}"
            strategy_id_generated = True
        strategy_config["strategy_display_name"] = config_display_name

    # Determine chain: config.json (explicit override) > decorator default_chain > supported_chains[0]
    # Config.json chain wins when present so users can override without editing code.
    config_chain = strategy_config.get("chain")
    if not config_chain and not multi_chain:
        config_chain = get_default_chain(strategy_class)

    # Determine network: CLI flag > config.json > default "mainnet"
    # Priority: --network flag (highest) > config "network" field > "mainnet" (default)
    resolved_network = network or "mainnet"
    if resolved_network == "anvil":
        anvil_port = os.environ.get(f"ANVIL_{(config_chain or 'arbitrum').upper()}_PORT", "8545")
        click.echo(f"Network: ANVIL (local fork at http://127.0.0.1:{anvil_port})")

    # Load runtime configuration from environment
    runtime_config: LocalRuntimeConfig | MultiChainRuntimeConfig | GatewayRuntimeConfig

    # Sidecar deployment mode: --no-gateway without a local private key.
    # The gateway handles all signing and RPC; we only need chain + wallet address.
    if no_gateway and not os.environ.get("ALMANAK_PRIVATE_KEY"):
        if multi_chain:
            raise click.ClickException(
                "Multi-chain sidecar mode is not supported yet; set ALMANAK_PRIVATE_KEY or run a single-chain strategy."
            )
        resolved_chain = config_chain or None
        if not resolved_chain:
            raise click.ClickException(
                "Chain must be specified in config.json or strategy decorator for sidecar deployment mode."
            )

        safe_address = os.environ.get("ALMANAK_SAFE_ADDRESS")
        wallet_address = safe_address or os.environ.get("ALMANAK_EOA_ADDRESS")
        if not wallet_address:
            raise click.ClickException(
                "Sidecar mode (--no-gateway without ALMANAK_PRIVATE_KEY) requires "
                "ALMANAK_SAFE_ADDRESS or ALMANAK_EOA_ADDRESS to be set."
            )

        default_gas_cap = CHAIN_GAS_PRICE_CAPS_GWEI.get(resolved_chain, DEFAULT_GAS_PRICE_CAP_GWEI)
        runtime_config = GatewayRuntimeConfig(
            chain=resolved_chain,
            wallet_address=wallet_address,
            is_safe=bool(safe_address),
            max_gas_price_gwei=default_gas_cap,
        )
        click.echo(f"Sidecar deployment mode: chain={resolved_chain}, wallet={wallet_address}")

    elif multi_chain:
        try:
            runtime_config = MultiChainRuntimeConfig.from_env(
                chains=strategy_chains,
                protocols=strategy_protocols,
                network=resolved_network,
            )
            click.echo(f"Multi-chain config loaded for: {', '.join(strategy_chains)}")
        except MissingEnvironmentVariableError as e:
            if resolved_network == "anvil" and e.var_name.endswith("PRIVATE_KEY"):
                click.echo(f"No ALMANAK_PRIVATE_KEY set. Using default Anvil wallet: {ANVIL_DEFAULT_ADDRESS}")
                if sys.stdin.isatty():
                    if not click.confirm("Continue with this wallet?", default=True):
                        sys.exit(0)
                else:
                    click.echo("(non-interactive, accepting default Anvil wallet)")
                os.environ["ALMANAK_PRIVATE_KEY"] = ANVIL_DEFAULT_PRIVATE_KEY
                try:
                    runtime_config = MultiChainRuntimeConfig.from_env(
                        chains=strategy_chains,
                        protocols=strategy_protocols,
                        network=resolved_network,
                    )
                except Exception as retry_err:
                    click.echo(f"Error loading configuration after setting default key: {retry_err}", err=True)
                    sys.exit(1)
                click.echo(f"Multi-chain config loaded for: {', '.join(strategy_chains)}")
            else:
                if e.var_name.endswith("PRIVATE_KEY"):
                    click.echo("Error: ALMANAK_PRIVATE_KEY is required for mainnet execution.", err=True)
                    click.echo("Set it in your .env file or environment.", err=True)
                else:
                    click.echo(f"Error loading multi-chain configuration: {e}", err=True)
                    click.echo()
                    click.echo("Required environment variables for multi-chain:")
                    click.echo("  ALMANAK_PRIVATE_KEY          - Wallet private key")
                    click.echo()
                    click.echo("RPC access (one of these, or leave empty for free public RPCs):")
                    for chain in strategy_chains:
                        click.echo(f"  ALMANAK_{chain.upper()}_RPC_URL  - Per-chain RPC URL")
                    click.echo("  RPC_URL                      - Generic RPC endpoint URL")
                    click.echo("  ALCHEMY_API_KEY              - Alchemy API key (fallback)")
                sys.exit(1)
        except Exception as e:
            click.echo(f"Error loading multi-chain configuration: {e}", err=True)
            click.echo()
            click.echo("Required environment variables for multi-chain:")
            click.echo("  ALMANAK_PRIVATE_KEY          - Wallet private key")
            click.echo()
            click.echo("RPC access (one of these, or leave empty for free public RPCs):")
            for chain in strategy_chains:
                click.echo(f"  ALMANAK_{chain.upper()}_RPC_URL  - Per-chain RPC URL")
            click.echo("  RPC_URL                      - Generic RPC endpoint URL")
            click.echo("  ALCHEMY_API_KEY              - Alchemy API key (fallback)")
            sys.exit(1)
    else:
        try:
            # Pass chain and network from strategy config for dynamic RPC URL building
            runtime_config = LocalRuntimeConfig.from_env(chain=config_chain, network=resolved_network)
        except MissingEnvironmentVariableError as e:
            if resolved_network == "anvil" and e.var_name.endswith("PRIVATE_KEY"):
                click.echo(f"No ALMANAK_PRIVATE_KEY set. Using default Anvil wallet: {ANVIL_DEFAULT_ADDRESS}")
                if sys.stdin.isatty():
                    if not click.confirm("Continue with this wallet?", default=True):
                        sys.exit(0)
                else:
                    click.echo("(non-interactive, accepting default Anvil wallet)")
                os.environ["ALMANAK_PRIVATE_KEY"] = ANVIL_DEFAULT_PRIVATE_KEY
                try:
                    runtime_config = LocalRuntimeConfig.from_env(chain=config_chain, network=resolved_network)
                except Exception as retry_err:
                    click.echo(f"Error loading configuration after setting default key: {retry_err}", err=True)
                    sys.exit(1)
            else:
                if e.var_name.endswith("PRIVATE_KEY"):
                    click.echo("Error: ALMANAK_PRIVATE_KEY is required for mainnet execution.", err=True)
                    click.echo("Set it in your .env file or environment.", err=True)
                else:
                    click.echo(f"Error loading configuration: {e}", err=True)
                    click.echo()
                    click.echo("Required environment variables:")
                    click.echo("  ALMANAK_PRIVATE_KEY          - Wallet private key")
                    click.echo()
                    click.echo("RPC access (one of these, or leave empty for free public RPCs):")
                    click.echo("  ALMANAK_ARBITRUM_RPC_URL     - Per-chain RPC URL (highest priority)")
                    click.echo("  ALMANAK_RPC_URL              - Generic RPC endpoint URL")
                    click.echo("  RPC_URL                      - Generic RPC endpoint URL")
                    click.echo("  ALCHEMY_API_KEY              - Alchemy API key (fallback)")
                    click.echo()
                    click.echo("Optional environment variables:")
                    click.echo("  ALMANAK_MAX_GAS_PRICE_GWEI - Max gas price (default: 100)")
                    click.echo("  ALMANAK_TX_TIMEOUT_SECONDS - Tx timeout (default: 120)")
                    click.echo("  ALMANAK_SIMULATION_ENABLED - Enable simulation (default: false)")
                sys.exit(1)
        except Exception as e:
            click.echo(f"Error loading configuration: {e}", err=True)
            click.echo()
            click.echo("Required environment variables:")
            click.echo("  ALMANAK_PRIVATE_KEY          - Wallet private key")
            click.echo()
            click.echo("RPC access (one of these, or leave empty for free public RPCs):")
            click.echo("  ALMANAK_ARBITRUM_RPC_URL     - Per-chain RPC URL (highest priority)")
            click.echo("  ALMANAK_RPC_URL              - Generic RPC endpoint URL")
            click.echo("  RPC_URL                      - Generic RPC endpoint URL")
            click.echo("  ALCHEMY_API_KEY              - Alchemy API key (fallback)")
            click.echo()
            click.echo("Optional environment variables:")
            click.echo("  ALMANAK_MAX_GAS_PRICE_GWEI - Max gas price (default: 100)")
            click.echo("  ALMANAK_TX_TIMEOUT_SECONDS - Tx timeout (default: 120)")
            click.echo("  ALMANAK_SIMULATION_ENABLED - Enable simulation (default: false)")
            sys.exit(1)

    # Preflight checks for Safe mode consistency between framework and gateway
    # Only check when the CLI manages the gateway (env vars are local).
    # With --no-gateway the env vars may live on a remote host.
    if runtime_config.is_safe_mode and not no_gateway:
        error = _validate_safe_mode_preflight(runtime_config.execution_address)
        if error:
            click.secho(f"ERROR: {error}", fg="red", err=True)
            sys.exit(1)

    # Ensure chain and wallet_address are set in strategy config
    if "chain" not in strategy_config:
        if multi_chain:
            strategy_config["chain"] = strategy_chains[0]
        else:
            assert isinstance(runtime_config, LocalRuntimeConfig | GatewayRuntimeConfig)
            strategy_config["chain"] = runtime_config.chain
    if "wallet_address" not in strategy_config:
        strategy_config["wallet_address"] = runtime_config.execution_address

    # Handle --fresh flag: clear state for this strategy only
    strategy_id = strategy_config["strategy_id"]
    if fresh:
        state_db_path = Path("./almanak_state.db")
        if state_db_path.exists():
            try:
                import sqlite3

                with sqlite3.connect(str(state_db_path)) as conn:
                    cursor = conn.execute(
                        "DELETE FROM v2_strategy_state WHERE strategy_id = ?",
                        (strategy_id,),
                    )
                    deleted = cursor.rowcount
                    # Also clear teardown requests for this strategy
                    try:
                        teardown_cursor = conn.execute(
                            "DELETE FROM teardown_requests WHERE strategy_id = ?",
                            (strategy_id,),
                        )
                        teardown_deleted = teardown_cursor.rowcount
                    except sqlite3.OperationalError:
                        teardown_deleted = 0  # Table may not exist
                if deleted > 0 or teardown_deleted > 0:
                    parts = []
                    if deleted > 0:
                        parts.append("state")
                    if teardown_deleted > 0:
                        parts.append("teardown requests")
                    click.secho(
                        f"Cleared {' and '.join(parts)} for strategy '{strategy_id}' (--fresh flag)",
                        fg="yellow",
                    )
                else:
                    click.echo(f"No existing state for strategy '{strategy_id}' (--fresh flag)")
            except sqlite3.Error as e:
                click.echo(f"Failed to clear strategy state: {e}", err=True)
        else:
            click.echo("No existing state to clear (--fresh flag)")

    # Check for existing state to determine RESUME vs FRESH START
    is_resume = False
    existing_state_info = None
    try:
        # Quick check of state DB for existing strategy state
        state_db_path = Path("./almanak_state.db")
        if state_db_path.exists():
            import sqlite3

            conn = sqlite3.connect(str(state_db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT strategy_id, version, state_data FROM v2_strategy_state WHERE strategy_id = ? AND is_active = 1",
                (strategy_id,),
            )
            row = cursor.fetchone()
            conn.close()
            if row:
                is_resume = True
                try:
                    state_data = json.loads(row["state_data"]) if row["state_data"] else {}
                    existing_state_info = {
                        "version": row["version"],
                        "keys": list(state_data.keys()) if state_data else [],
                    }
                except Exception:
                    existing_state_info = {"version": row["version"], "keys": []}
    except Exception as e:
        logger.debug(f"Could not check for existing state: {e}")

    # Display startup information
    click.echo("=" * 60)
    click.echo("ALMANAK STRATEGY RUNNER")
    click.echo("=" * 60)
    click.echo(f"Strategy: {strategy_name}")
    click.echo(f"Instance ID: {strategy_id}" + (" (generated)" if strategy_id_generated else ""))
    if is_resume:
        click.secho("Mode: RESUME (existing state found)", fg="yellow", bold=True)
        if existing_state_info:
            click.echo(f"  State version: {existing_state_info['version']}, keys: {existing_state_info['keys']}")
        if once and not fresh:
            click.secho(
                "WARNING: Loading state from a previous run. "
                "If this is unexpected, re-run with --fresh to start clean.",
                fg="red",
                bold=True,
            )
    else:
        click.secho("Mode: FRESH START (no existing state)", fg="green", bold=True)
    if multi_chain:
        click.echo(f"Chains: {', '.join(strategy_chains)}")
        click.echo(f"Protocols: {strategy_protocols}")
    else:
        click.echo(f"Chain: {runtime_config.chain}")  # type: ignore[union-attr]
    safe_mode_str = " (Safe)" if runtime_config.is_safe_mode else ""
    click.echo(f"Wallet: {runtime_config.execution_address}{safe_mode_str}")
    exec_desc = "Single run" if once else f"Continuous (every {interval}s)"
    if max_iterations and not once:
        exec_desc += f", max {max_iterations} iterations"
    click.echo(f"Execution: {exec_desc}")
    click.echo(f"Dry run: {effective_dry_run}")
    if isinstance(strategy_config.get("copy_trading"), dict):
        copy_execution_policy = strategy_config["copy_trading"].get("execution_policy", {})
        copy_mode_label = copy_execution_policy.get("copy_mode", "live")
        click.echo(f"Copy mode: {copy_mode_label}")
        if copy_execution_policy.get("replay_file"):
            click.echo(f"Copy replay file: {copy_execution_policy.get('replay_file')}")
    click.secho(f"Gateway: {gateway_host}:{gateway_port}", fg="cyan")
    if dashboard:
        click.echo("Dashboard: Will launch alongside strategy")
    click.echo("=" * 60)

    # Strategy class already loaded above
    click.echo(f"Strategy class loaded: {strategy_class.__name__}")
    if multi_chain:
        click.echo(f"  Multi-chain: Yes ({len(strategy_chains)} chains)")

    # Create strategy instance
    try:
        # Check if strategy accepts config dict or individual parameters
        if issubclass(strategy_class, IntentStrategy):
            # IntentStrategy requires specific parameters
            primary_chain = strategy_chains[0] if multi_chain else runtime_config.chain  # type: ignore[union-attr]

            # Check if strategy has a config class (generic parameter)
            # Try to get the config type from __orig_bases__
            config_instance: Any = strategy_config
            try:
                from decimal import Decimal
                from typing import get_args, get_type_hints

                bases = getattr(strategy_class, "__orig_bases__", [])
                for base in bases:
                    args = get_args(base)
                    if args and hasattr(args[0], "__dataclass_fields__"):
                        # Found dataclass config type - create instance with defaults
                        config_class = args[0]

                        # Convert numeric values to Decimal where needed
                        type_hints = get_type_hints(config_class)
                        converted_config: dict[str, Any] = {}
                        # Track fields that are NOT in the dataclass (excluding runtime + framework meta-keys)
                        runtime_fields = {"strategy_id", "chain", "wallet_address"}
                        # Meta-keys consumed by the CLI/framework, not by strategy config classes
                        framework_meta_keys = {"anvil_funding", "strategy_display_name"}
                        unknown_fields = []
                        for k, v in strategy_config.items():
                            if k in config_class.__dataclass_fields__:
                                field_type = type_hints.get(k)
                                # Convert int/float/str to Decimal for Decimal fields
                                if field_type == Decimal and isinstance(v, int | float | str):
                                    try:
                                        converted_config[k] = Decimal(str(v))
                                    except Exception:
                                        converted_config[k] = v
                                else:
                                    converted_config[k] = v
                            elif k not in runtime_fields and k not in framework_meta_keys:
                                unknown_fields.append(k)

                        # Use dataclass config, filtering out unknown fields
                        # (runtime fields like strategy_id/chain are handled separately)
                        if unknown_fields:
                            logger.debug(
                                f"Config class {config_class.__name__} ignoring unknown fields: {unknown_fields}"
                            )
                            click.echo(f"  Config class: {config_class.__name__} (ignored: {unknown_fields})")
                        else:
                            click.echo(f"  Config class: {config_class.__name__}")
                        config_instance = config_class(**converted_config) if converted_config else config_class()
                        break
            except Exception as e:
                logger.debug(f"Could not infer config class: {e}")
                # Fall back to using dict or default config
                pass

            # Wrap dict config in DictConfigWrapper for compatibility
            if isinstance(config_instance, dict):
                config_instance = DictConfigWrapper(config_instance)
                click.echo("  Config wrapped in DictConfigWrapper")

            strategy_instance = strategy_class(
                config=config_instance,
                chain=primary_chain,
                wallet_address=runtime_config.execution_address,
            )
        else:
            # Try dict config first, then no config
            try:
                strategy_instance = strategy_class(strategy_config)
            except TypeError:
                strategy_instance = strategy_class()

        click.echo("Strategy instance created successfully")

    except Exception as e:
        click.echo(f"Error creating strategy instance: {e}", err=True)
        sys.exit(1)

    # Track resources that need cleanup
    ohlcv_provider: RoutingOHLCVProvider | None = None
    price_oracle: PriceOracle | None = None
    solana_fork_mgr_ref: Any = None  # Track Solana fork manager for cleanup

    async def cleanup_resources() -> None:
        """Close all resources that have async cleanup methods."""
        nonlocal ohlcv_provider, price_oracle, gateway_client, solana_fork_mgr_ref
        if ohlcv_provider is not None:
            await ohlcv_provider.close()
        if price_oracle is not None and hasattr(price_oracle, "close"):
            await price_oracle.close()
        if gateway_client is not None:
            gateway_client.disconnect()
        if solana_fork_mgr_ref is not None:
            try:
                await solana_fork_mgr_ref.stop()
                click.echo("  Stopped solana-test-validator")
            except Exception as e:
                logger.debug(f"Failed to stop solana-test-validator: {e}")
        if managed_gateway is not None:
            if keep_anvil and managed_gateway._anvil_managers:
                for chain, mgr in managed_gateway._anvil_managers.items():
                    port = mgr.anvil_port
                    pid = mgr._process.pid if mgr._process else "unknown"
                    click.echo(f"Anvil for {chain} still running on port {port} (PID {pid})")
            managed_gateway.stop()

    # Create components
    try:
        click.echo("Initializing components...")

        execution_orchestrator: Any
        if multi_chain:
            # Multi-chain setup - use gateway-backed providers (same pattern as single-chain)
            assert isinstance(runtime_config, MultiChainRuntimeConfig)

            from ..data.balance.gateway_multichain import MultiChainGatewayBalanceProvider
            from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator

            click.echo("  Using gateway-backed providers for multi-chain...")
            price_oracle = GatewayPriceOracle(gateway_client)
            balance_provider = GatewayBalanceProvider(
                client=gateway_client,
                wallet_address=runtime_config.execution_address,
                chain=strategy_chains[0],
            )
            execution_orchestrator = MultiChainOrchestrator.from_gateway(
                gateway_client=gateway_client,
                chains=strategy_chains,
                wallet_address=runtime_config.execution_address,
                max_gas_price_gwei=runtime_config.max_gas_price_gwei,
            )

            # Create multi-chain balance provider for the strategy
            multi_chain_balance_provider = MultiChainGatewayBalanceProvider(
                client=gateway_client,
                wallet_address=runtime_config.execution_address,
                chains=strategy_chains,
            )

            # Set multi-chain providers on strategy if it's an IntentStrategy
            if hasattr(strategy_instance, "set_multi_chain_providers"):
                strategy_instance.set_multi_chain_providers(
                    balance_provider=multi_chain_balance_provider,
                )
                click.echo("  Multi-chain providers set on strategy")

            # Create indicator calculators using routed OHLCV provider (CEX + DEX fallback).
            # NOTE: In multi-chain mode, OHLCV routing is bound to the first chain.
            # For CEX-listed tokens this is fine (Binance data is chain-agnostic).
            # For DeFi-native tokens on secondary chains, GeckoTerminal pool search
            # may resolve to the wrong network. Per-chain providers would require
            # passing chain context through the indicator callables, which is a larger change.
            ohlcv_provider = create_routing_ohlcv_provider(
                gateway_client=gateway_client,
                chain=strategy_chains[0],
                strategy_config=strategy_config,
            )
            _wire_indicators(strategy_instance, ohlcv_provider, price_oracle, balance_provider)

            # Initialize lending rate monitor for multi-chain (uses first chain)
            if hasattr(strategy_instance, "_rate_monitor"):
                try:
                    from ..data.rates import RateMonitor

                    primary_chain = strategy_chains[0]
                    chain_rpc_url = runtime_config.rpc_urls.get(primary_chain)
                    rate_monitor = RateMonitor(chain=primary_chain, rpc_url=chain_rpc_url)
                    strategy_instance._rate_monitor = rate_monitor
                    click.echo(f"  Rate monitor initialized (chain={primary_chain})")
                except Exception as e:
                    logger.debug(f"Rate monitor not available: {e}")

            # Initialize funding rate provider for perpetual venue rates
            if hasattr(strategy_instance, "_funding_rate_provider"):
                try:
                    from ..data.funding import GatewayFundingRateProvider

                    primary_chain = strategy_chains[0]
                    funding_provider = GatewayFundingRateProvider(gateway_client=gateway_client, chain=primary_chain)
                    strategy_instance._funding_rate_provider = funding_provider
                    click.echo(f"  Funding rate provider initialized (chain={primary_chain})")
                except (ImportError, ValueError, RuntimeError) as e:
                    logger.warning(
                        "Funding rate provider init failed for chain=%s: %s",
                        strategy_chains[0],
                        e,
                        exc_info=True,
                    )

            click.echo(f"  Multi-chain orchestrator created for {len(strategy_chains)} chains")
        else:
            # Single-chain setup - always use gateway-backed providers
            assert isinstance(runtime_config, LocalRuntimeConfig | GatewayRuntimeConfig)

            from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator

            click.echo("  Using gateway-backed providers...")
            price_oracle = GatewayPriceOracle(gateway_client)
            balance_provider = GatewayBalanceProvider(
                client=gateway_client,
                wallet_address=runtime_config.execution_address,
                chain=runtime_config.chain,
            )

            # For Solana + --network anvil, start local solana-test-validator
            if runtime_config.chain.lower() == "solana" and resolved_network == "anvil":
                from ..anvil.solana_fork_manager import SolanaForkManager

                solana_rpc_url = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
                solana_fork_mgr = SolanaForkManager(
                    rpc_url=solana_rpc_url,
                    validator_port=int(os.environ.get("SOLANA_VALIDATOR_PORT", "8899")),
                )
                click.echo("  Starting local solana-test-validator...")
                import asyncio as _aio

                started = _aio.get_event_loop().run_until_complete(solana_fork_mgr.start())
                if not started:
                    raise click.ClickException(
                        "Failed to start solana-test-validator. "
                        "Ensure Solana CLI tools are installed: "
                        'sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)"'
                    )
                click.echo(f"  solana-test-validator running at {solana_fork_mgr.get_rpc_url()}")

                # Fund the wallet
                _aio.get_event_loop().run_until_complete(
                    solana_fork_mgr.fund_wallet(runtime_config.wallet_address, Decimal("100"))
                )
                _aio.get_event_loop().run_until_complete(
                    solana_fork_mgr.fund_tokens(
                        runtime_config.wallet_address,
                        {"USDC": Decimal("10000"), "USDT": Decimal("10000")},
                    )
                )
                click.echo("  Wallet funded with 100 SOL + 10K USDC + 10K USDT")
                solana_fork_mgr_ref = solana_fork_mgr

            # All chains (including Solana) use GatewayExecutionOrchestrator
            execution_orchestrator = GatewayExecutionOrchestrator(
                client=gateway_client,
                chain=runtime_config.chain,
                wallet_address=runtime_config.execution_address,
                max_gas_price_gwei=runtime_config.max_gas_price_gwei,
            )
            click.echo("  Gateway-backed providers created")

            # Create indicator calculators using routed OHLCV provider (CEX + DEX fallback)
            ohlcv_provider = create_routing_ohlcv_provider(
                gateway_client=gateway_client,
                chain=runtime_config.chain,
                strategy_config=strategy_config,
            )
            _wire_indicators(strategy_instance, ohlcv_provider, price_oracle, balance_provider)

            # Initialize prediction market provider for Polygon strategies
            if runtime_config.chain.lower() == "polygon" and hasattr(strategy_instance, "_prediction_provider"):
                try:
                    from ..connectors.polymarket import ClobClient, PolymarketConfig
                    from ..data.prediction_provider import PredictionMarketDataProvider

                    pm_config = PolymarketConfig.from_env()
                    clob_client = ClobClient(pm_config)
                    strategy_instance._prediction_provider = PredictionMarketDataProvider(clob_client)
                    click.echo("  Prediction market provider initialized")
                except Exception as e:
                    logger.debug(f"Prediction market provider not available: {e}")

            # Initialize lending rate monitor
            if hasattr(strategy_instance, "_rate_monitor"):
                try:
                    from ..data.rates import RateMonitor

                    rpc_url = getattr(runtime_config, "rpc_url", None)
                    rate_monitor = RateMonitor(chain=runtime_config.chain, rpc_url=rpc_url)
                    strategy_instance._rate_monitor = rate_monitor
                    click.echo(f"  Rate monitor initialized (chain={runtime_config.chain})")
                except Exception as e:
                    logger.debug(f"Rate monitor not available: {e}")

            # Initialize funding rate provider for perpetual venue rates
            if hasattr(strategy_instance, "_funding_rate_provider"):
                try:
                    from ..data.funding import GatewayFundingRateProvider

                    funding_provider = GatewayFundingRateProvider(
                        gateway_client=gateway_client, chain=runtime_config.chain
                    )
                    strategy_instance._funding_rate_provider = funding_provider
                    click.echo(f"  Funding rate provider initialized (chain={runtime_config.chain})")
                except (ImportError, ValueError, RuntimeError) as e:
                    logger.warning(
                        "Funding rate provider init failed for chain=%s: %s",
                        runtime_config.chain,
                        e,
                        exc_info=True,
                    )

            # Initialize copy trading components if configured
            if strategy_config.get("copy_trading"):
                ct_raw = strategy_config["copy_trading"]
                click.echo("  Copy trading config detected, initializing components...")

                from ..connectors.contract_registry import get_default_registry
                from ..data.wallet_activity import WalletActivityProvider
                from ..services.copy_circuit_breaker import CopyCircuitBreaker
                from ..services.copy_intent_builder import CopyIntentBuilder
                from ..services.copy_ledger import CopyLedger
                from ..services.copy_policy_engine import CopyPolicyEngine
                from ..services.copy_signal_engine import CopySignalEngine
                from ..services.copy_trading_models import (
                    CopyTradingConfig,
                    CopyTradingConfigError,
                    CopyTradingConfigV2,
                )
                from ..services.wallet_monitor import WalletMonitor, WalletMonitorConfig
                from ..testing.copy_replay import CopyReplayRunner

                if not isinstance(ct_raw, dict):
                    raise click.ClickException("copy_trading config must be an object")

                ct_config = CopyTradingConfig.from_config(ct_raw)
                ct_v2: CopyTradingConfigV2 | None = None
                strict_requested = bool(
                    copy_strict or ct_raw.get("strict") or ct_raw.get("execution_policy", {}).get("strict")
                )
                try:
                    ct_v2 = CopyTradingConfigV2.from_config(ct_raw)
                except CopyTradingConfigError as e:
                    if strict_requested:
                        raise click.ClickException(f"Invalid copy_trading config in strict mode: {e}") from e
                    click.echo("  Warning: copy_trading strict schema validation failed, using legacy-compatible mode")

                registry = get_default_registry()

                # Group leaders by chain for multi-chain monitoring
                leaders_by_chain: dict[str, list[str]] = {}
                if ct_v2 is not None:
                    for leader in ct_v2.leaders:
                        leader_chain = (leader.chain or runtime_config.chain).lower()
                        leaders_by_chain.setdefault(leader_chain, []).append(leader.address)
                else:
                    for leader_dict in ct_config.leaders:
                        leader_chain = str(leader_dict.get("chain", runtime_config.chain)).lower()
                        leaders_by_chain.setdefault(leader_chain, []).append(leader_dict["address"])

                # Create one WalletMonitor per unique chain
                wallet_monitors: dict[str, WalletMonitor] = {}
                for chain, addresses in leaders_by_chain.items():
                    monitor_config = WalletMonitorConfig(
                        leader_addresses=addresses,
                        chain=chain,
                        poll_interval_seconds=(
                            ct_v2.monitoring.poll_interval_seconds
                            if ct_v2 is not None
                            else ct_config.monitoring.get("poll_interval_seconds", 12)
                        ),
                        lookback_blocks=(
                            ct_v2.monitoring.lookback_blocks
                            if ct_v2 is not None
                            else ct_config.monitoring.get("lookback_blocks", 50)
                        ),
                        confirmation_depth=(
                            ct_v2.monitoring.confirmation_depth
                            if ct_v2 is not None
                            else ct_config.monitoring.get("confirmation_depth", 1)
                        ),
                    )
                    wallet_monitors[chain] = WalletMonitor(config=monitor_config, gateway_client=gateway_client)

                # Create price function for signal engine from gateway price oracle
                copy_price_fn = None
                if price_oracle is not None:
                    sync_price = create_sync_price_oracle_func(price_oracle)

                    def _copy_price_fn(symbol: str, chain: str) -> Decimal | None:
                        try:
                            return sync_price(symbol, "USD")
                        except Exception:
                            return None

                    copy_price_fn = _copy_price_fn

                engine = CopySignalEngine(
                    registry=registry,
                    max_age_seconds=(
                        ct_v2.monitoring.max_signal_age_seconds
                        if ct_v2 is not None
                        else ct_config.monitoring.get("max_signal_age_seconds", 300)
                    ),
                    price_fn=copy_price_fn,
                    strict_token_resolution=strict_requested,
                )

                activity_provider = WalletActivityProvider(
                    signal_engine=engine,
                    wallet_monitors=wallet_monitors,
                )

                # Runtime-inject copy trading attributes (not declared on IntentStrategy)
                strat_any: Any = strategy_instance
                strat_any._wallet_activity_provider = activity_provider
                strat_any._copy_mode = normalized_copy_mode
                strat_any._copy_replay_file = copy_replay_file
                strat_any._copy_strict = strict_requested

                if ct_v2 is not None:
                    strat_any._copy_config_v2 = ct_v2
                    strat_any._copy_policy_engine = CopyPolicyEngine(
                        config=ct_v2,
                        reference_price_fn=copy_price_fn,
                    )
                    strat_any._copy_intent_builder = CopyIntentBuilder(config=ct_v2)
                    strat_any._copy_circuit_breaker = CopyCircuitBreaker.from_copy_config(ct_v2)
                    ledger_db_path = (
                        ct_raw.get("ledger", {}).get("db_path") if isinstance(ct_raw.get("ledger"), dict) else None
                    )
                    strat_any._copy_ledger = CopyLedger(ledger_db_path or "./almanak_copy_ledger.db")

                    exec_policy = ct_v2.execution_policy
                    copy_mode_resolved = str(exec_policy.copy_mode)
                    replay_path = exec_policy.replay_file
                    if normalized_copy_mode is not None:
                        copy_mode_resolved = normalized_copy_mode
                    if copy_replay_file:
                        replay_path = copy_replay_file
                        copy_mode_resolved = "replay"
                    if copy_shadow:
                        copy_mode_resolved = "shadow"

                    strat_any._copy_mode = copy_mode_resolved
                    strat_any._copy_replay_file = replay_path

                    if copy_mode_resolved == "replay" and replay_path:
                        replay_runner = CopyReplayRunner(config=ct_v2)
                        replay_signals = replay_runner.load_signals(replay_path)
                        activity_provider.inject_signals(replay_signals)
                        click.echo(f"  Copy replay loaded: {len(replay_signals)} signal(s) from {replay_path}")

                chains_str = ", ".join(sorted(leaders_by_chain.keys()))
                click.echo(f"  Copy trading initialized: monitoring {len(ct_config.leaders)} leader(s) on {chains_str}")

        # Create state manager - always use gateway-backed state manager
        from ..state.gateway_state_manager import GatewayStateManager

        state_manager = GatewayStateManager(gateway_client)
        click.echo("  Using gateway-backed state manager")

        # Inject state manager into strategy for persistence.
        # State loading is deferred to the async setup phase (run_once_with_cleanup /
        # run_loop_with_cleanup) so that load_state_async() can be awaited properly.
        if hasattr(strategy_instance, "set_state_manager"):
            strategy_instance.set_state_manager(state_manager, strategy_id)

        # Wire vault lifecycle if vault config is present
        vault_lifecycle = None
        if strategy_config.get("vault"):
            from ..connectors.lagoon import LagoonVaultAdapter, LagoonVaultSDK
            from ..vault.config import VaultConfig
            from ..vault.lifecycle import VAULT_STATE_KEY, VaultLifecycleManager

            vault_raw = strategy_config["vault"]

            # Auto-deploy Lagoon vault on Anvil if placeholder address detected
            if resolved_network == "anvil" and _has_placeholder_vault_address(vault_raw):
                if effective_dry_run:
                    click.secho(
                        "  [DRY-RUN] Vault has placeholder address -- skipping auto-deploy",
                        fg="yellow",
                    )
                    click.echo("  Deploy manually or run without --dry-run on Anvil")
                    sys.exit(0)

                click.echo("  Placeholder vault address detected -- auto-deploying on Anvil...")
                vault_raw = _auto_deploy_lagoon_vault(
                    vault_raw,
                    strategy_config.get("chain") or config_chain or "ethereum",
                    runtime_config,
                    gateway_client,
                    execution_orchestrator,
                )

            vault_config = VaultConfig(**vault_raw)
            vault_chain = strategy_config.get("chain", "")
            vault_sdk = LagoonVaultSDK(gateway_client, chain=vault_chain)
            vault_adapter = LagoonVaultAdapter(vault_sdk)

            # Extract initial vault state from persisted strategy state.
            # State loading is deferred to the async phase for IntentStrategy, so we
            # load the raw state here directly from the state manager (safe to use
            # asyncio.run() because we are still in the sync Click command, before any
            # event loop is started).
            initial_vault_state = None
            try:
                import asyncio as _asyncio

                _raw_state_data = _asyncio.run(state_manager.load_state(strategy_id))
                if _raw_state_data and _raw_state_data.state:
                    initial_vault_state = _raw_state_data.state.get(VAULT_STATE_KEY)
            except Exception as _e:  # noqa: BLE001
                logger.debug("Could not load persisted state for vault init (strategy_id=%s): %s", strategy_id, _e)
                # No persisted state — VaultLifecycleManager uses defaults
            # Fallback: also check in-memory strategy state (for StrategyBase subclasses)
            if initial_vault_state is None:
                for attr in ("persistent_state", "state"):
                    store = getattr(strategy_instance, attr, None)
                    if isinstance(store, dict):
                        initial_vault_state = store.get(VAULT_STATE_KEY)
                        break

            # Wire persistence callback: vault state changes are saved into the
            # strategy's persistent_state dict and persisted via the gateway state manager.
            def _persist_vault_state(vault_state_dict: dict) -> None:
                for attr in ("persistent_state", "state"):
                    store = getattr(strategy_instance, attr, None)
                    if isinstance(store, dict):
                        store[VAULT_STATE_KEY] = vault_state_dict
                        if hasattr(strategy_instance, "save_state"):
                            strategy_instance.save_state()
                        return

            vault_lifecycle = VaultLifecycleManager(
                vault_config=vault_config,
                vault_sdk=vault_sdk,
                vault_adapter=vault_adapter,
                execution_orchestrator=execution_orchestrator,
                strategy_id=strategy_id,
                initial_vault_state=initial_vault_state,
                persistence_callback=_persist_vault_state,
            )
            click.echo(
                f"  Vault lifecycle initialized: "
                f"address={vault_config.vault_address}, "
                f"underlying={vault_config.underlying_token}, "
                f"interval={vault_config.settlement_interval_minutes}min"
            )

        # Create runner config
        runner_config = RunnerConfig(
            default_interval_seconds=interval,
            dry_run=effective_dry_run,
            enable_state_persistence=True,
            enable_alerting=False,  # No alert manager configured
        )

        # Create safety components for fail-closed execution
        from ..execution.circuit_breaker import CircuitBreaker
        from ..services.emergency_manager import EmergencyManager
        from ..services.operator_card_generator import OperatorCardGenerator
        from ..services.stuck_detector import StuckDetector

        circuit_breaker = CircuitBreaker(strategy_id=strategy_id)
        stuck_detector = StuckDetector()
        operator_card_generator = OperatorCardGenerator()
        emergency_manager = EmergencyManager()

        # Create runner
        runner = StrategyRunner(
            price_oracle=price_oracle,
            balance_provider=balance_provider,
            execution_orchestrator=execution_orchestrator,
            state_manager=state_manager,  # type: ignore[arg-type]
            config=runner_config,
            vault_lifecycle=vault_lifecycle,
            circuit_breaker=circuit_breaker,
            stuck_detector=stuck_detector,
            operator_card_generator=operator_card_generator,
            emergency_manager=emergency_manager,
        )

        click.echo("Components initialized successfully")

    except Exception as e:
        click.echo(f"Error initializing components: {e}", err=True)
        logger.exception("Component initialization failed")
        sys.exit(1)

    # Run strategy
    if once:
        click.echo()
        click.echo("Running single iteration...")
        click.echo()

        async def run_once_with_cleanup() -> tuple[IterationResult, IterationResult | None]:
            """Run single iteration, optional teardown, and cleanup resources."""
            runner.setup_gateway_integration(strategy_instance)
            try:
                # Restore persisted strategy state (e.g. position_id after restart)
                if hasattr(strategy_instance, "load_state_async"):
                    if await strategy_instance.load_state_async():
                        click.secho("  Strategy state restored from persistence", fg="yellow")
                    else:
                        click.echo("  No previous state found (fresh start)")

                # Restore copy trading cursor state (mirrors run_loop pattern)
                activity_provider = getattr(strategy_instance, "_wallet_activity_provider", None)
                if activity_provider is not None:
                    try:
                        ct_state = await state_manager.load_state(strategy_instance.strategy_id)
                        if ct_state is not None and "copy_trading_state" in ct_state.state:
                            activity_provider.set_state(ct_state.state["copy_trading_state"])
                    except Exception as e:
                        logger.warning(f"Failed to restore copy trading state: {e}")

                result = await runner.run_iteration(strategy_instance)

                # Emit structured iteration summary for JSONL log analysis
                runner._emit_iteration_summary(result, chain=getattr(strategy_instance, "chain", None))

                # --- teardown-after: signal + second iteration ---
                teardown_result = None
                if teardown_after:
                    click.echo()
                    click.echo("Teardown requested -- closing positions...")

                    from almanak.framework.teardown import get_teardown_state_manager
                    from almanak.framework.teardown.models import TeardownMode, TeardownRequest

                    strategy_id = strategy_instance.strategy_id or strategy_instance.STRATEGY_NAME
                    manager = get_teardown_state_manager()
                    manager.create_request(
                        TeardownRequest(
                            strategy_id=strategy_id,
                            mode=TeardownMode.SOFT,
                            reason="--teardown-after flag (CI cleanup)",
                            requested_by="cli",
                        )
                    )

                    teardown_result = await runner.run_iteration(strategy_instance)
                    runner._emit_iteration_summary(teardown_result, chain=getattr(strategy_instance, "chain", None))
                    click.echo(format_iteration_result(teardown_result))
                elif teardown_after:
                    teardown_result = IterationResult(
                        status=IterationStatus.EXECUTION_FAILED,
                        error="--teardown-after requested but strategy does not support teardown",
                    )
                    click.echo(teardown_result.error, err=True)

                # Persist copy trading cursor state
                if activity_provider is not None:
                    try:
                        ct_state = await state_manager.load_state(strategy_instance.strategy_id)
                        if ct_state is None:
                            from almanak.framework.state.state_manager import StateData

                            ct_state = StateData(
                                strategy_id=strategy_instance.strategy_id,
                                version=0,
                                state={},
                            )
                        ct_state.state["copy_trading_state"] = activity_provider.get_state()
                        await state_manager.save_state(ct_state, expected_version=ct_state.version)
                    except Exception as e:
                        logger.warning(f"Failed to persist copy trading state: {e}")

                # Flush any pending state saves before cleanup
                # (run_loop does this automatically, but run_iteration doesn't)
                if hasattr(strategy_instance, "flush_pending_saves"):
                    try:
                        await strategy_instance.flush_pending_saves()
                    except Exception as e:
                        logger.warning(f"Error flushing pending saves: {e}")
                return result, teardown_result
            finally:
                runner.teardown_gateway_integration(strategy_instance.strategy_id)
                await cleanup_resources()

        try:
            result, teardown_result = asyncio.run(run_once_with_cleanup())
            click.echo(format_iteration_result(result))

            # Determine exit code: main iteration + optional teardown
            if teardown_result is not None:
                # With --teardown-after: both iteration and teardown must succeed
                teardown_ok = teardown_result.status == IterationStatus.TEARDOWN
                if result.success and teardown_ok:
                    click.echo()
                    click.echo("Iteration and teardown completed successfully.")
                    stop_dashboard(dashboard_process)
                    sys.exit(0)
                else:
                    click.echo()
                    if not result.success:
                        click.echo(f"Iteration failed: {result.error}")
                    if not teardown_ok:
                        click.echo(f"Teardown failed: {teardown_result.error or teardown_result.status.value}")
                    stop_dashboard(dashboard_process)
                    sys.exit(1)
            elif result.success:
                click.echo()
                click.echo("Iteration completed successfully.")
                stop_dashboard(dashboard_process)
                sys.exit(0)
            else:
                click.echo()
                click.echo(f"Iteration failed: {result.error}")
                stop_dashboard(dashboard_process)
                sys.exit(1)

        except Exception as e:
            click.echo(f"Error running iteration: {e}", err=True)
            logger.exception("Iteration failed")
            stop_dashboard(dashboard_process)
            sys.exit(1)

    else:
        if sys.stdout.isatty():
            click.echo()
            click.echo("Starting continuous execution...")
            click.echo("Press Ctrl+C to stop gracefully.")
            click.echo()

        # Set up signal handlers for graceful shutdown
        runner.setup_signal_handlers()

        def on_iteration(result: IterationResult) -> None:
            """Callback for each iteration."""
            timestamp = result.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            click.echo(f"[{timestamp}] {format_iteration_result(result)}")

        # Build pre-iteration callback for --reset-fork
        pre_iteration_cb: Callable[[], None] | None = None
        if reset_fork and managed_gateway is not None:
            from ..runner.strategy_runner import CriticalCallbackError

            def pre_iteration_cb() -> None:
                click.echo("Resetting Anvil fork to latest block...")
                ok = managed_gateway.reset_anvil_forks()
                if ok:
                    click.echo("Fork reset complete.")
                else:
                    raise CriticalCallbackError(
                        "Anvil fork reset failed. Cannot continue with stale fork state. "
                        "Remove --reset-fork to run without fork resets."
                    )

        async def run_loop_with_cleanup() -> None:
            """Run loop and cleanup resources."""
            try:
                # Restore persisted strategy state (e.g. position_id after restart)
                if hasattr(strategy_instance, "load_state_async"):
                    if await strategy_instance.load_state_async():
                        click.secho("  Strategy state restored from persistence", fg="yellow")
                    else:
                        click.echo("  No previous state found (fresh start)")

                await runner.run_loop(
                    strategy=strategy_instance,
                    interval_seconds=interval,
                    iteration_callback=on_iteration,
                    pre_iteration_callback=pre_iteration_cb,
                    max_iterations=max_iterations,
                )
            finally:
                await cleanup_resources()

        try:
            asyncio.run(run_loop_with_cleanup())
            click.echo()

            # Exit 2 when stopped by signal (SIGTERM/SIGINT) so K8s sees a
            # pod failure and retries.  Check this first so it takes
            # precedence over the max-iterations branch.
            if runner._signal_received:
                click.echo("Runner stopped by signal.")
                stop_dashboard(dashboard_process)
                sys.exit(2)

            # Return a failure exit code when max_iterations is set and every
            # single iteration failed (no successful iterations at all).
            if max_iterations and runner._successful_iterations == 0 and runner._total_iterations > 0:
                click.echo(f"Runner completed {runner._total_iterations} iterations with 0 successes.")
                stop_dashboard(dashboard_process)
                sys.exit(1)

            click.echo("Runner stopped gracefully.")
            stop_dashboard(dashboard_process)
            sys.exit(0)

        except KeyboardInterrupt:
            click.echo()
            click.echo("Shutdown requested. Stopping...")
            runner.request_shutdown()
            # Run cleanup in a new event loop since the previous one was interrupted
            asyncio.run(cleanup_resources())
            stop_dashboard(dashboard_process)
            sys.exit(0)

        except Exception as e:
            click.echo(f"Error in run loop: {e}", err=True)
            logger.exception("Run loop failed")
            stop_dashboard(dashboard_process)
            sys.exit(1)


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
