"""Almanak BacktestService — standalone HTTP API for backtesting.

This service exposes the SDK's PnLBacktester and PaperTrader over HTTP,
allowing external callers (e.g., Edge) to submit backtest jobs, poll results,
manage paper trading sessions, and query fee models.

Usage:
    # Start the service
    almanak backtest-service

    # Or directly with uvicorn
    uvicorn almanak.services.backtest.app:app --host 0.0.0.0 --port 8000
"""
