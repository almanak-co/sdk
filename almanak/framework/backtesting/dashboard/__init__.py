"""Backtest Dashboard - Interactive exploration of backtest results.

This module provides a Streamlit-based interactive dashboard for
exploring and analyzing backtest results from the PnL Backtester
and Paper Trader engines.

Usage:
    Run the dashboard from CLI:
    ```bash
    streamlit run almanak/framework/backtesting/dashboard/app.py
    ```

    Or via the almanak CLI (when available):
    ```bash
    almanak backtest dashboard
    ```

Features:
    - Upload backtest JSON results for analysis
    - View key performance metrics
    - Interactive equity curve visualization
    - Trade log exploration with filtering
    - Position breakdown analysis
"""

from almanak.framework.backtesting.dashboard.app import main

__all__ = ["main"]
