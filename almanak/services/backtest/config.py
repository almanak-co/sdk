"""BacktestService configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class BacktestServiceConfig:
    """Configuration for the standalone backtest service."""

    host: str = "0.0.0.0"
    port: int = 8000
    workers: int = 1
    max_concurrent_backtest_jobs: int = 4
    max_concurrent_paper_sessions: int = 2
    log_level: str = "info"

    @classmethod
    def from_env(cls) -> BacktestServiceConfig:
        """Load config from environment variables."""
        return cls(
            host=os.environ.get("BACKTEST_SERVICE_HOST", "0.0.0.0"),
            port=int(os.environ.get("BACKTEST_SERVICE_PORT", "8000")),
            workers=int(os.environ.get("BACKTEST_SERVICE_WORKERS", "1")),
            max_concurrent_backtest_jobs=int(os.environ.get("BACKTEST_MAX_JOBS", "4")),
            max_concurrent_paper_sessions=int(os.environ.get("BACKTEST_MAX_PAPER_SESSIONS", "2")),
            log_level=os.environ.get("BACKTEST_LOG_LEVEL", "info"),
        )
