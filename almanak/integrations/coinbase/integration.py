"""Coinbase market-symbol manifest."""

from almanak.integrations._base import Integration

INTEGRATION = Integration(
    name="coinbase",
    market_symbols={
        ("WETH", "USDC"): "ETH-USD",
        ("WBTC", "USDC"): "BTC-USD",
        ("CBBTC", "USDC"): "CBBTC-USD",
        ("LINK", "USDC"): "LINK-USD",
        ("UNI", "USDC"): "UNI-USD",
        ("AAVE", "USDC"): "AAVE-USD",
        ("ARB", "USDC"): "ARB-USD",
        ("OP", "USDC"): "OP-USD",
    },
)
