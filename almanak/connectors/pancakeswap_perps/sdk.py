"""Backwards-compatibility submodule re-exporting the shared Aster perp SDK.

Exists so callers using ``from almanak.connectors.pancakeswap_perps.sdk
import ...`` keep working after the VIB-3044 extraction. Re-exports from the
shared ``_aster_perps_core.sdk`` foundation (not the sibling ``aster_perps``
leaf). New code should import from ``almanak.connectors.aster_perps.sdk``.
"""

from almanak.connectors._aster_perps_core.sdk import *  # noqa: F401,F403 — intentional re-export
from almanak.connectors._aster_perps_core.sdk import (  # noqa: F401 — keep back-compat list explicit
    ASTER_BROKER_RAW,
    EVENT_CLOSE_TRADE_RECEIVED,
    EVENT_CLOSE_TRADE_SUCCESSFUL,
    EVENT_MARKET_PENDING_TRADE,
    EVENT_OPEN_MARKET_TRADE,
    EVENT_PENDING_TRADE_REFUND,
    NATIVE_BNB_ADDRESS,
    PCS_BROKER_ID,
    PRICE_DECIMALS,
    QTY_DECIMALS,
    SELECTOR_CLOSE_TRADE,
    SELECTOR_GET_PENDING_TRADE,
    SELECTOR_GET_POSITION_BY_HASH_V2,
    SELECTOR_GET_POSITIONS_V2,
    SELECTOR_OPEN_MARKET_TRADE,
    SELECTOR_OPEN_MARKET_TRADE_BNB,
    OpenTradeStruct,
    _check_address,  # noqa: F401 — private helper imported by tests/intents/bnb/conftest.py
    encode_close_trade_calldata,
    encode_get_pending_trade_calldata,
    encode_get_position_by_hash_calldata,
    encode_open_market_trade_calldata,
    get_margin_token_address,
    get_pair_base,
    get_router_address,
    slippage_to_limit_price,
    usd_size_to_qty,
)
