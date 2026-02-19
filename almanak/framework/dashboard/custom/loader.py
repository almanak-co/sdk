"""Custom Dashboard Loader.

Dynamically loads custom dashboard modules with caching and error handling.
"""

import importlib.util
import logging
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

logger = logging.getLogger(__name__)

# Module cache to avoid re-importing
_module_cache: dict[str, ModuleType] = {}


class DashboardLoadError(Exception):
    """Raised when a dashboard module cannot be loaded."""

    pass


class DashboardInterfaceError(Exception):
    """Raised when a dashboard module doesn't implement the required interface."""

    pass


def load_dashboard_module(
    dashboard_path: Path,
    strategy_name: str,
    use_cache: bool = True,
) -> ModuleType:
    """Load a custom dashboard module dynamically.

    Args:
        dashboard_path: Path to the dashboard directory (containing ui.py)
        strategy_name: Name of the strategy (used for module naming)
        use_cache: Whether to use cached modules

    Returns:
        Loaded Python module

    Raises:
        DashboardLoadError: If module cannot be loaded
    """
    ui_path = dashboard_path / "ui.py"
    cache_key = str(ui_path)

    # Check cache first
    if use_cache and cache_key in _module_cache:
        logger.debug(f"Using cached module for {strategy_name}")
        return _module_cache[cache_key]

    # Validate file exists
    if not ui_path.exists():
        raise DashboardLoadError(f"Dashboard module not found: {ui_path}")

    if not ui_path.is_file():
        raise DashboardLoadError(f"Dashboard path is not a file: {ui_path}")

    # Create unique module name to avoid conflicts
    module_name = f"custom_dashboard_{strategy_name}"

    try:
        # Load module spec
        spec = importlib.util.spec_from_file_location(module_name, ui_path)

        if spec is None:
            raise DashboardLoadError(f"Could not create module spec for {ui_path}")

        if spec.loader is None:
            raise DashboardLoadError(f"No loader available for {ui_path}")

        # Create and execute module
        module = importlib.util.module_from_spec(spec)

        # Add to sys.modules temporarily for relative imports
        sys.modules[module_name] = module

        # Execute the module
        spec.loader.exec_module(module)

        # Cache the module
        if use_cache:
            _module_cache[cache_key] = module

        logger.info(f"Loaded custom dashboard module: {strategy_name}")
        return module

    except SyntaxError as e:
        raise DashboardLoadError(f"Syntax error in {ui_path}: {e}") from e
    except ImportError as e:
        raise DashboardLoadError(f"Import error loading {ui_path}: {e}") from e
    except Exception as e:
        raise DashboardLoadError(f"Error loading {ui_path}: {e}") from e


def get_dashboard_render_function(
    module: ModuleType,
) -> Callable:
    """Get the render function from a dashboard module.

    Custom dashboards must implement:
        def render_custom_dashboard(
            strategy_id: str,
            strategy_config: dict,
            api_client: APIClient,
            session_state: dict,
        ) -> None

    Args:
        module: Loaded dashboard module

    Returns:
        The render_custom_dashboard function

    Raises:
        DashboardInterfaceError: If module doesn't implement interface
    """
    # Check for required function
    if not hasattr(module, "render_custom_dashboard"):
        raise DashboardInterfaceError(
            f"Module {module.__name__} does not implement 'render_custom_dashboard' function. "
            "Custom dashboards must define: "
            "def render_custom_dashboard(strategy_id, strategy_config, api_client, session_state)"
        )

    render_func = module.render_custom_dashboard

    if not callable(render_func):
        raise DashboardInterfaceError(f"'render_custom_dashboard' in {module.__name__} is not callable")

    return render_func


def clear_module_cache() -> None:
    """Clear the module cache (useful for development/hot-reload)."""
    global _module_cache
    _module_cache.clear()
    logger.debug("Cleared dashboard module cache")


def invalidate_module(dashboard_path: Path) -> None:
    """Invalidate a specific module in the cache.

    Args:
        dashboard_path: Path to the dashboard directory
    """
    ui_path = dashboard_path / "ui.py"
    cache_key = str(ui_path)

    if cache_key in _module_cache:
        del _module_cache[cache_key]
        logger.debug(f"Invalidated cached module: {cache_key}")
