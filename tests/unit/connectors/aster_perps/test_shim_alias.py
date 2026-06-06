"""Backwards-compat verification for the pancakeswap_perps shim (VIB-3045).

The shim at ``almanak.connectors.pancakeswap_perps`` must re-export
every previously-public symbol so legacy strategies continue to work unchanged.
These tests lock that surface.
"""

from __future__ import annotations

import warnings

import pytest


# -----------------------------------------------------------------------------
# Symbols that MUST remain importable from pancakeswap_perps after VIB-3045.
# Derived from the package's __all__ at commit d35498cd7 (pre-extraction).
# -----------------------------------------------------------------------------
_LEGACY_PUBLIC_SYMBOLS: tuple[str, ...] = (
    # Event topics
    "EVENT_CLOSE_TRADE_RECEIVED",
    "EVENT_CLOSE_TRADE_SUCCESSFUL",
    "EVENT_MARKET_PENDING_TRADE",
    "EVENT_OPEN_MARKET_TRADE",
    "EVENT_PENDING_TRADE_REFUND",
    # Gas budgets
    "GAS_CLOSE_TRADE",
    "GAS_OPEN_MARKET_TRADE",
    "GAS_OPEN_MARKET_TRADE_BNB",
    # Sentinels / constants
    "NATIVE_BNB_ADDRESS",
    "PCS_BROKER_ID",
    "PRICE_DECIMALS",
    "QTY_DECIMALS",
    # Selectors
    "SELECTOR_CLOSE_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE_BNB",
    # Parser + event dataclasses
    "CloseTradeReceivedEvent",
    "CloseTradeSuccessfulEvent",
    "MarketPendingTradeEvent",
    "OpenMarketTradeEvent",
    "PancakeSwapPerpsReceiptParser",
    "ParsedReceipt",
    "PendingTradeRefundEvent",
    # Adapter
    "OpenTradeStruct",
    "PancakeSwapPerpsAdapter",
    "PancakeSwapPerpsConfig",
    "PancakeSwapPerpsTx",
    "PerpOpenOrderResult",
    # Convenience helpers
    "build_close_transaction",
    "build_open_transaction",
    "encode_close_trade_calldata",
    "encode_get_pending_trade_calldata",
    "encode_get_position_by_hash_calldata",
    "encode_open_market_trade_calldata",
    "get_margin_token_address",
    "get_pair_base",
    "get_router_address",
    "slippage_to_limit_price",
    "usd_size_to_qty",
)


@pytest.fixture(autouse=True)
def _reset_shim_deprecation_flag():
    """Reset the module-level deprecation-emitted flag so each test sees a warning."""
    import almanak.connectors.pancakeswap_perps as shim

    shim._DEPRECATION_EMITTED = False
    yield


def test_every_legacy_symbol_is_importable():
    """Every symbol from the pre-extraction __all__ must resolve through the shim."""
    import almanak.connectors.pancakeswap_perps as shim

    missing = [name for name in _LEGACY_PUBLIC_SYMBOLS if not hasattr(shim, name)]
    assert not missing, f"shim dropped legacy symbols: {missing}"


def test_star_import_exposes_legacy_surface():
    """``from almanak.connectors.pancakeswap_perps import *`` must work."""
    namespace: dict[str, object] = {}
    exec(
        "from almanak.connectors.pancakeswap_perps import *",
        namespace,
    )
    missing = [name for name in _LEGACY_PUBLIC_SYMBOLS if name not in namespace]
    assert not missing, f"star-import missing symbols: {missing}"


def test_addresses_shim_reexports_pancakeswap_perps_table():
    """The address shim must expose only the deprecated protocol address table."""
    import almanak.connectors.pancakeswap_perps.addresses as shim_addresses
    from almanak.connectors.aster_perps.addresses import PANCAKESWAP_PERPS

    assert shim_addresses.__all__ == ["PANCAKESWAP_PERPS"]
    assert shim_addresses.PANCAKESWAP_PERPS is PANCAKESWAP_PERPS


def test_shim_class_names_are_aster_aliases():
    """Legacy class names must be identity-equal to their Aster* canonicals."""
    from almanak.connectors.aster_perps import (
        AsterPerpsAdapter,
        AsterPerpsReceiptParser,
        AsterPerpsTx,
    )
    from almanak.connectors.pancakeswap_perps import (
        PancakeSwapPerpsAdapter,
        PancakeSwapPerpsReceiptParser,
        PancakeSwapPerpsTx,
    )

    assert PancakeSwapPerpsAdapter is AsterPerpsAdapter
    assert PancakeSwapPerpsReceiptParser is AsterPerpsReceiptParser
    assert PancakeSwapPerpsTx is AsterPerpsTx


def test_shim_config_factory_injects_broker_id_2():
    """PancakeSwapPerpsConfig() factory must default broker_id=2 (PCS)."""
    from almanak.connectors.pancakeswap_perps import PancakeSwapPerpsConfig

    cfg = PancakeSwapPerpsConfig()
    assert cfg.broker_id == 2
    assert cfg.chain == "bsc"
    assert cfg.wallet_address is None

    # Overrides still honored
    cfg_custom = PancakeSwapPerpsConfig(wallet_address="0xabc", broker_id=7)
    assert cfg_custom.broker_id == 7
    assert cfg_custom.wallet_address == "0xabc"


def test_shim_emits_deprecation_warning_exactly_once_per_process():
    """Importing the shim module fires exactly one DeprecationWarning per process."""
    import almanak.connectors.pancakeswap_perps as shim

    # First trigger — reset the emitted flag and re-invoke the helper.
    shim._DEPRECATION_EMITTED = False
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        shim._emit_deprecation()
    deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
    assert len(deprecation_warnings) == 1
    assert "aster_perps" in str(deprecation_warnings[0].message)

    # Second call is suppressed.
    with warnings.catch_warnings(record=True) as w2:
        warnings.simplefilter("always")
        shim._emit_deprecation()
    deprecation_warnings2 = [x for x in w2 if issubclass(x.category, DeprecationWarning)]
    assert deprecation_warnings2 == []


def test_shim_submodule_paths_still_resolve():
    """The ReceiptParserRegistry imports parser classes by module path (string).

    Preserving the ``pancakeswap_perps.receipt_parser`` and ``pancakeswap_perps.sdk``
    submodules is load-bearing.
    """
    import almanak.connectors.pancakeswap_perps.receipt_parser as legacy_rp
    import almanak.connectors.pancakeswap_perps.sdk as legacy_sdk

    assert hasattr(legacy_rp, "PancakeSwapPerpsReceiptParser")
    assert hasattr(legacy_sdk, "encode_open_market_trade_calldata")
    assert hasattr(legacy_sdk, "PCS_BROKER_ID")
    assert legacy_sdk.PCS_BROKER_ID == 2
