"""HTML report templates for backtesting results.

This module provides Jinja2 templates for generating comprehensive HTML reports
from backtest results, including metrics, charts, trade logs, and configuration.

Templates:
    - report.html: Main backtest report template with all sections
"""

from pathlib import Path

# Template directory path for external access
TEMPLATE_DIR = Path(__file__).parent

__all__ = ["TEMPLATE_DIR"]
