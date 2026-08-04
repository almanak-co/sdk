"""Compatibility imports for the Chainlink provider integration.

Chainlink metadata is owned by :mod:`almanak.integrations.chainlink`.  This
module remains import-only so existing SDK consumers can migrate without a
flag-day breaking change; new code must import from the integration package.
"""

from almanak.integrations.chainlink.catalog import (
    CHAINLINK_CHAIN_IDS,
    CHAINLINK_DEVIATION_THRESHOLDS,
    CHAINLINK_HEARTBEATS,
    CHAINLINK_PRICE_FEEDS,
    ETH_DENOMINATED_FEEDS,
    TOKEN_TO_ETH_PAIR,
    TOKEN_TO_PAIR,
)
from almanak.integrations.chainlink.codec import (
    DECIMALS_SELECTOR,
    GET_ROUND_DATA_SELECTOR,
    LATEST_ROUND_DATA_SELECTOR,
)

__all__ = [
    "CHAINLINK_CHAIN_IDS",
    "CHAINLINK_DEVIATION_THRESHOLDS",
    "CHAINLINK_HEARTBEATS",
    "CHAINLINK_PRICE_FEEDS",
    "DECIMALS_SELECTOR",
    "ETH_DENOMINATED_FEEDS",
    "GET_ROUND_DATA_SELECTOR",
    "LATEST_ROUND_DATA_SELECTOR",
    "TOKEN_TO_ETH_PAIR",
    "TOKEN_TO_PAIR",
]
