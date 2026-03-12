"""Standalone server entry point for the BacktestService.

Usage:
    python -m almanak.services.backtest.server
    # or via CLI: almanak backtest-service
"""

from __future__ import annotations

import uvicorn

from almanak.services.backtest.config import BacktestServiceConfig


def run_server(
    host: str | None = None,
    port: int | None = None,
    workers: int | None = None,
    log_level: str | None = None,
) -> None:
    """Start the backtest service with uvicorn."""
    cfg = BacktestServiceConfig.from_env()

    uvicorn.run(
        "almanak.services.backtest.app:app",
        host=host or cfg.host,
        port=port or cfg.port,
        workers=workers or cfg.workers,
        log_level=log_level or cfg.log_level,
    )


if __name__ == "__main__":
    run_server()
