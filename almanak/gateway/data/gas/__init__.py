"""Gateway-owned gas price data providers."""

from .etherscan import fetch_gas_price_at

__all__ = ["fetch_gas_price_at"]
