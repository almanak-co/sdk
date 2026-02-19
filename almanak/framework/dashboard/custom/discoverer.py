"""Custom Dashboard Discoverer.

Scans the strategies directory to find custom dashboards.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class CustomDashboardInfo:
    """Metadata about a discovered custom dashboard."""

    strategy_name: str
    dashboard_path: Path
    display_name: str
    description: str | None = None
    icon: str | None = None

    @property
    def module_path(self) -> Path:
        """Path to the ui.py module."""
        return self.dashboard_path / "ui.py"

    def exists(self) -> bool:
        """Check if the dashboard module exists."""
        return self.module_path.exists()


def discover_custom_dashboards(
    strategies_dir: Path | None = None,
) -> list[CustomDashboardInfo]:
    """Discover all custom dashboards in the strategies directory.

    Scans strategies/{name}/dashboard/ui.py for custom dashboards.

    Args:
        strategies_dir: Path to strategies directory. Defaults to
            project_root/strategies/

    Returns:
        List of CustomDashboardInfo for each discovered dashboard.
        Returns empty list if no dashboards found (never crashes).
    """
    dashboards: list[CustomDashboardInfo] = []

    # Determine strategies directory
    if strategies_dir is None:
        # Default to project_root/strategies/
        project_root = Path(__file__).parent.parent.parent.parent
        strategies_dir = project_root / "strategies"

    # Check if strategies directory exists
    if not strategies_dir.exists():
        logger.debug(f"Strategies directory not found: {strategies_dir}")
        return dashboards

    if not strategies_dir.is_dir():
        logger.warning(f"Strategies path is not a directory: {strategies_dir}")
        return dashboards

    # Scan strategy dashboard paths. Supports both:
    # - strategies/<strategy>/dashboard/ui.py
    # - strategies/<category>/<strategy>/dashboard/ui.py
    seen_paths: set[Path] = set()
    try:
        for ui_file in strategies_dir.rglob("dashboard/ui.py"):
            dashboard_dir = ui_file.parent
            if dashboard_dir in seen_paths:
                continue
            seen_paths.add(dashboard_dir)

            # Strategy directory is always parent of "dashboard"
            strategy_path = dashboard_dir.parent
            if not strategy_path.is_dir():
                continue
            strategy_name = strategy_path.name

            # Try to load metadata if available
            metadata = _load_dashboard_metadata(dashboard_dir)

            dashboard_info = CustomDashboardInfo(
                strategy_name=strategy_name,
                dashboard_path=dashboard_dir,
                display_name=metadata.get("display_name", _format_display_name(strategy_name)),
                description=metadata.get("description"),
                icon=metadata.get("icon", "📊"),
            )

            dashboards.append(dashboard_info)
            logger.debug(f"Discovered custom dashboard: {strategy_name}")

    except PermissionError as e:
        logger.warning(f"Permission denied scanning strategies: {e}")
    except Exception as e:
        logger.error(f"Error discovering custom dashboards: {e}")

    # Sort by display name
    dashboards.sort(key=lambda d: d.display_name)

    return dashboards


def _load_dashboard_metadata(dashboard_dir: Path) -> dict:
    """Load optional metadata.json for a dashboard.

    Args:
        dashboard_dir: Path to dashboard directory

    Returns:
        Metadata dictionary or empty dict if not found
    """
    metadata_file = dashboard_dir / "metadata.json"

    if not metadata_file.exists():
        return {}

    try:
        with open(metadata_file) as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid metadata.json in {dashboard_dir}: {e}")
        return {}
    except Exception as e:
        logger.warning(f"Error loading metadata from {dashboard_dir}: {e}")
        return {}


def _format_display_name(strategy_name: str) -> str:
    """Convert strategy_name to display name.

    Args:
        strategy_name: Snake_case strategy name (e.g., "aave_loop")

    Returns:
        Title case display name (e.g., "Aave Loop")
    """
    # Replace underscores and hyphens with spaces
    name = strategy_name.replace("_", " ").replace("-", " ")
    # Title case
    return name.title()
