"""Compound V3 PnL Backtest Strategy on Ethereum — Supply Rate Tracker.

Demonstrates PnL backtesting with a lending protocol strategy. Supplies USDC
to Compound V3 on Ethereum when the supply rate exceeds a threshold, holds,
and withdraws when the rate falls below an exit threshold.

First PnL backtest of a lending (supply/withdraw) lifecycle — prior backtest
demos used swap-based strategies (RSI, LP rebalancing).

Example:
    from almanak.demo_strategies.compound_v3_pnl_backtest_ethereum import CompoundV3PnLBacktestStrategy

    strategy = CompoundV3PnLBacktestStrategy(
        chain="ethereum",
        wallet_address="0x...",
        config={
            "supply_token": "USDC",
            "supply_amount": "10000",
            "entry_rate_threshold": "0.03",
            "exit_rate_threshold": "0.01",
        }
    )
"""

from .strategy import CompoundV3PnLBacktestStrategy

__all__ = ["CompoundV3PnLBacktestStrategy"]
