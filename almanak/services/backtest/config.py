"""BacktestService configuration."""

from __future__ import annotations

from dataclasses import dataclass

from almanak.config.backtest import backtest_service_config_from_env


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
        cfg = backtest_service_config_from_env()
        return cls(
            host=cfg.host,
            port=cfg.port,
            workers=cfg.workers,
            max_concurrent_backtest_jobs=cfg.max_concurrent_backtest_jobs,
            max_concurrent_paper_sessions=cfg.max_concurrent_paper_sessions,
            log_level=cfg.log_level,
        )
