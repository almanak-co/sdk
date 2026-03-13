"""
Strategy Factory - Auto-registers all available strategies.

This module provides a central registry for all strategy implementations,
allowing strategies to be looked up by name at runtime.

Usage:
    from almanak.framework.strategies import get_strategy, list_strategies, StrategyBase

    # Get a strategy class
    StrategyClass = get_strategy("my_strategy")

    # List all registered strategies
    available = list_strategies()

    # Create a custom strategy
    class MyStrategy(StrategyBase):
        def run(self):
            pass

    # Or use IntentStrategy for simplified authoring
    from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

    @almanak_strategy(name="my_intent_strategy")
    class MyIntentStrategy(IntentStrategy):
        def decide(self, market: MarketSnapshot) -> Optional[Intent]:
            return Intent.hold()
"""

import importlib
import importlib.machinery
import importlib.util
import logging
import os
import sys
from pathlib import Path
from typing import Any, Optional

# Import IntentSequence and DecideResult for multi-intent support
from ..intents import DecideResult, IntentSequence
from .base import (
    ConfigSnapshot,
    NotificationCallback,
    RiskGuard,
    RiskGuardConfig,
    RiskGuardGuidance,
    RiskGuardResult,
    StrategyBase,
)
from .intent_strategy import (
    AaveAvailableBorrowProvider,
    # Protocol Health Metric Providers
    AaveHealthFactorProvider,
    ADXData,
    ATRData,
    BalanceProvider,
    BollingerBandsData,
    CCIData,
    # Chain Health
    ChainHealth,
    ChainHealthStatus,
    ChainNotConfiguredError,
    DataFreshnessPolicy,
    ExecutionResult,
    GmxAvailableLiquidityProvider,
    GmxFundingRateProvider,
    IchimokuData,
    IntentStrategy,
    # Technical Indicator Data Classes
    MACDData,
    MAData,
    MarketSnapshot,
    MultiChainBalanceProvider,
    # Multi-Chain Market Snapshot
    MultiChainMarketSnapshot,
    MultiChainPriceOracle,
    OBVData,
    PriceData,
    PriceOracle,
    RSIData,
    RSIProvider,
    StaleDataError,
    StochasticData,
    StrategyMetadata,
    TokenBalance,
    almanak_strategy,
)
from .multi_step_strategy import MultiStepStrategy, Step

logger = logging.getLogger(__name__)

# Strategy registry - maps strategy names to their classes
STRATEGY_REGISTRY: dict[str, type[Any]] = {}


def _try_import_strategy(module_name: str, file_path: Path | None = None) -> None:
    """Import a strategy module, retrying once on circular import errors.

    Uses spec_from_file_location when a file_path is provided, which allows
    strategy discovery without requiring PYTHONPATH to include the project root.
    Falls back to importlib.import_module if no file_path is given (e.g. when
    the module is already on sys.path).

    Args:
        module_name: Fully qualified module name to import
        file_path: Optional path to the strategy.py file for direct loading
    """

    def _do_import() -> bool:
        if file_path is not None:
            # If the leaf module was already loaded (e.g. by a parent __init__.py
            # that does `from .strategy import ...`), skip re-execution to avoid
            # duplicate registration and double side-effects.
            if module_name in sys.modules:
                return True

            # Ensure the parent package hierarchy exists in sys.modules so that
            # relative imports within the strategy module resolve correctly.
            # Track inserted keys so we can clean up on failure.
            inserted_modules: list[str] = []
            try:
                parts = module_name.split(".")
                for i in range(1, len(parts)):
                    parent = ".".join(parts[:i])
                    if parent not in sys.modules:
                        parent_path = file_path.parents[len(parts) - i - 1]
                        parent_init = parent_path / "__init__.py"
                        if parent_init.exists():
                            parent_spec = importlib.util.spec_from_file_location(parent, parent_init)
                            if parent_spec and parent_spec.loader:
                                parent_mod = importlib.util.module_from_spec(parent_spec)
                                parent_mod.__path__ = [str(parent_path)]
                                sys.modules[parent] = parent_mod
                                inserted_modules.append(parent)
                                parent_spec.loader.exec_module(parent_mod)
                        else:
                            # Create a namespace package placeholder
                            parent_mod = importlib.util.module_from_spec(
                                importlib.machinery.ModuleSpec(parent, None, is_package=True)
                            )
                            parent_mod.__path__ = [str(parent_path)]
                            sys.modules[parent] = parent_mod
                            inserted_modules.append(parent)

                spec = importlib.util.spec_from_file_location(module_name, file_path)
                if spec is None or spec.loader is None:
                    logger.warning(f"Could not create module spec for {module_name} at {file_path}")
                    # Clean up parent entries we inserted before returning
                    for name in reversed(inserted_modules):
                        sys.modules.pop(name, None)
                    return False
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                inserted_modules.append(module_name)
                spec.loader.exec_module(module)
            except Exception:
                # Clean up partially-inserted sys.modules entries to avoid poisoned state
                for name in reversed(inserted_modules):
                    sys.modules.pop(name, None)
                raise
        else:
            importlib.import_module(module_name)
        return True

    try:
        if _do_import():
            logger.debug(f"Imported strategy module: {module_name}")
    except ImportError as e:
        if "circular import" in str(e):
            # Retry once -- Python's import machinery resolves the cycle
            try:
                _do_import()
                logger.debug(f"Imported strategy module on retry: {module_name}")
            except Exception as retry_err:
                logger.warning(f"Failed to import strategy {module_name} (retry failed): {retry_err}")
        else:
            logger.warning(f"Failed to import strategy {module_name}: {e}")
    except Exception as e:
        logger.warning(f"Failed to import strategy {module_name}: {e}")


def _auto_discover_strategies() -> None:
    """Auto-discover and import strategies from the strategies/ directory.

    This function finds all strategy.py files in the strategies/ directory
    and imports them to trigger registration via @almanak_strategy decorator.

    The strategies directory is determined by (in order of priority):
    1. ALMANAK_STRATEGIES_DIR environment variable (relative to cwd or absolute)
    2. ./strategies relative to current working directory

    Supports both:
    - Top-level: strategies/<name>/strategy.py
    - Tiered: strategies/<tier>/<name>/strategy.py (poster_child, production, incubating, demo)
    """
    # Check for ALMANAK_STRATEGIES_DIR env var first
    strategies_dir_env = os.environ.get("ALMANAK_STRATEGIES_DIR")
    if strategies_dir_env:
        strategies_dir = Path(strategies_dir_env)
        # If relative, resolve from current working directory
        if not strategies_dir.is_absolute():
            strategies_dir = Path.cwd() / strategies_dir
    else:
        # Default: ./strategies relative to current working directory
        strategies_dir = Path.cwd() / "strategies"

    if not strategies_dir.exists():
        logger.debug(f"Strategies directory not found: {strategies_dir}")
        return

    # Tier directories that contain nested strategies
    tier_dirs = {"poster_child", "production", "incubating", "demo", "alpha_team", "tests"}

    for strategy_folder in strategies_dir.iterdir():
        if not strategy_folder.is_dir():
            continue

        if strategy_folder.name.startswith("_"):
            continue

        # Check if this is a tier directory with nested strategies
        if strategy_folder.name in tier_dirs:
            for nested_folder in strategy_folder.iterdir():
                if not nested_folder.is_dir():
                    continue
                if nested_folder.name.startswith("_"):
                    continue

                strategy_file = nested_folder / "strategy.py"
                if strategy_file.exists():
                    # Direct nested strategy: tier/<strategy>/strategy.py
                    module_name = f"strategies.{strategy_folder.name}.{nested_folder.name}.strategy"
                    _try_import_strategy(module_name, strategy_file)
                else:
                    # Check for sub-tier (e.g., tests/lp/<strategy>/strategy.py)
                    for sub_nested_folder in nested_folder.iterdir():
                        if not sub_nested_folder.is_dir():
                            continue
                        if sub_nested_folder.name.startswith("_"):
                            continue

                        sub_strategy_file = sub_nested_folder / "strategy.py"
                        if not sub_strategy_file.exists():
                            continue

                        module_name = (
                            f"strategies.{strategy_folder.name}.{nested_folder.name}.{sub_nested_folder.name}.strategy"
                        )
                        _try_import_strategy(module_name, sub_strategy_file)
        else:
            # Top-level strategy (backward compatibility)
            strategy_file = strategy_folder / "strategy.py"
            if not strategy_file.exists():
                continue

            module_name = f"strategies.{strategy_folder.name}.strategy"
            _try_import_strategy(module_name, strategy_file)


# Auto-discover strategies on module load
_auto_discover_strategies()


def register_strategy(name: str, strategy_class: type[Any]) -> None:
    """
    Register a strategy class in the factory.

    Args:
        name: Unique identifier for the strategy
        strategy_class: The strategy class to register
    """
    if name in STRATEGY_REGISTRY:
        raise ValueError(f"Strategy '{name}' is already registered")
    STRATEGY_REGISTRY[name] = strategy_class


def get_strategy(name: str) -> type[Any]:
    """
    Get a strategy class by name.

    Args:
        name: The registered name of the strategy

    Returns:
        The strategy class

    Raises:
        ValueError: If the strategy is not found
    """
    if name not in STRATEGY_REGISTRY:
        available = list(STRATEGY_REGISTRY.keys())
        raise ValueError(f"Unknown strategy: '{name}'. Available strategies: {available}")
    return STRATEGY_REGISTRY[name]


def list_strategies() -> list[str]:
    """
    List all registered strategy names.

    Returns:
        List of registered strategy names
    """
    return list(STRATEGY_REGISTRY.keys())


def unregister_strategy(name: str) -> None:
    """
    Unregister a strategy from the factory.

    Args:
        name: The name of the strategy to unregister

    Raises:
        ValueError: If the strategy is not found
    """
    if name not in STRATEGY_REGISTRY:
        raise ValueError(f"Strategy '{name}' is not registered")
    del STRATEGY_REGISTRY[name]


__all__ = [
    # Registry
    "STRATEGY_REGISTRY",
    "register_strategy",
    "get_strategy",
    "list_strategies",
    "unregister_strategy",
    # Base Strategy
    "StrategyBase",
    "RiskGuard",
    "RiskGuardConfig",
    "RiskGuardGuidance",
    "RiskGuardResult",
    "ConfigSnapshot",
    "NotificationCallback",
    # Intent Strategy
    "IntentStrategy",
    "MarketSnapshot",
    "TokenBalance",
    "PriceData",
    "RSIData",
    "PriceOracle",
    "RSIProvider",
    "BalanceProvider",
    "ExecutionResult",
    "almanak_strategy",
    "StrategyMetadata",
    # Technical Indicator Data Classes
    "MACDData",
    "BollingerBandsData",
    "StochasticData",
    "ATRData",
    "MAData",
    "ADXData",
    "OBVData",
    "CCIData",
    "IchimokuData",
    # Multi-Intent Support
    "IntentSequence",
    "DecideResult",
    # Multi-Chain Market Snapshot
    "MultiChainMarketSnapshot",
    "MultiChainPriceOracle",
    "MultiChainBalanceProvider",
    "ChainNotConfiguredError",
    # Chain Health
    "ChainHealth",
    "ChainHealthStatus",
    "StaleDataError",
    "DataFreshnessPolicy",
    # Protocol Health Metric Providers
    "AaveHealthFactorProvider",
    "AaveAvailableBorrowProvider",
    "GmxAvailableLiquidityProvider",
    "GmxFundingRateProvider",
    # Multi-Step Strategy
    "MultiStepStrategy",
    "Step",
]
