"""CLI entry point for Data QA tests.

Usage:
    python -m src.cli.qa_data --chain arbitrum --days 30
    python -m src.cli.qa_data --test cex_spot --skip-plots
    python -m src.cli.qa_data --config custom_config.yaml --output reports/qa

This is a thin wrapper that imports the main CLI from almanak.framework.data.qa.cli.
"""

from ..data.qa.cli import qa_data

if __name__ == "__main__":
    qa_data()
