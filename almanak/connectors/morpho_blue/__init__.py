"""Morpho Blue Connector (isolated lending markets).

This module provides adapters and utilities for interacting with Morpho Blue,
a permissionless lending protocol that allows creating isolated lending markets.

Morpho Blue Features:
- Isolated lending markets with customizable parameters
- Supply assets to earn yield (lending)
- Supply collateral for borrowing
- Borrow against collateral
- Flash loans
- No intermediary tokens (no aTokens)

Supported Chains:
- Ethereum
- Base
- Arbitrum (uses a chain-specific Morpho Blue deployment, not the universal address)
- Polygon (uses a chain-specific Morpho Blue deployment, not the universal address)
- Monad (uses a chain-specific Morpho Blue deployment, not the universal address)

Example:
    from almanak.connectors.morpho_blue import (
        MorphoBlueAdapter,
        MorphoBlueConfig,
        MorphoBlueReceiptParser,
        MorphoBlueSDK,
        create_adapter_with_prices,
    )
    from decimal import Decimal

    # Initialize adapter with prices
    config = MorphoBlueConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    prices = {"wstETH": Decimal("2500"), "USDC": Decimal("1")}
    adapter = create_adapter_with_prices(config, prices)

    # Get market info
    markets = adapter.get_markets()
    print(f"Available markets: {len(markets)}")

    # Build a supply collateral transaction
    result = adapter.supply_collateral(
        market_id="0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
        amount=Decimal("1.0"),
    )

    # Parse transaction receipts
    parser = MorphoBlueReceiptParser()
    events = parser.parse_receipt(receipt)

    # Use SDK for on-chain reads
    sdk = MorphoBlueSDK(chain="ethereum")
    position = sdk.get_position(market_id, user_address)
    print(f"Supply shares: {position.supply_shares}")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        DEFAULT_GAS_ESTIMATES,
        ERC20_APPROVE_SELECTOR,
        MAX_UINT256,
        MORPHO_ACCRUE_INTEREST_SELECTOR,
        MORPHO_BLUE_ADDRESSES,
        MORPHO_BORROW_SELECTOR,
        MORPHO_BUNDLER_ADDRESSES,
        MORPHO_FLASH_LOAN_SELECTOR,
        MORPHO_LIQUIDATE_SELECTOR,
        MORPHO_MARKETS,
        MORPHO_REPAY_SELECTOR,
        MORPHO_SET_AUTHORIZATION_SELECTOR,
        MORPHO_SUPPLY_COLLATERAL_SELECTOR,
        MORPHO_SUPPLY_SELECTOR,
        MORPHO_WITHDRAW_COLLATERAL_SELECTOR,
        MORPHO_WITHDRAW_SELECTOR,
        MorphoBlueAdapter,
        MorphoBlueConfig,
        MorphoBlueHealthFactor,
        MorphoBlueInterestRateMode,
        MorphoBlueMarketParams,
        MorphoBlueMarketState,
        MorphoBluePosition,
        TransactionResult,
        create_adapter_with_prices,
        create_test_adapter,
    )
    from .receipt_parser import (
        EVENT_NAME_TO_TYPE,
        EVENT_TOPICS,
        TOPIC_TO_EVENT,
        AccrueInterestEventData,
        BorrowEventData,
        CreateMarketEventData,
        FlashLoanEventData,
        LiquidateEventData,
        MorphoBlueEvent,
        MorphoBlueEventType,
        MorphoBlueReceiptParser,
        ParseResult,
        RepayEventData,
        SetAuthorizationEventData,
        SupplyCollateralEventData,
        SupplyEventData,
        TransferEventData,
        WithdrawCollateralEventData,
        WithdrawEventData,
    )
    from .sdk import (
        MORPHO_DEPLOYMENT_BLOCKS,
        SUPPORTED_CHAINS,
        MarketNotFoundError,
        MorphoBlueSDK,
        MorphoBlueSDKError,
        PositionNotFoundError,
        RPCError,
        SDKMarketInfo,
        SDKMarketParams,
        SDKMarketState,
        SDKPosition,
        UnsupportedChainError,
    )

__all__ = [
    "AccrueInterestEventData",
    "BorrowEventData",
    "CreateMarketEventData",
    "DEFAULT_GAS_ESTIMATES",
    "ERC20_APPROVE_SELECTOR",
    "EVENT_NAME_TO_TYPE",
    "EVENT_TOPICS",
    "FlashLoanEventData",
    "LiquidateEventData",
    "MAX_UINT256",
    "MORPHO_ACCRUE_INTEREST_SELECTOR",
    "MORPHO_BLUE_ADDRESSES",
    "MORPHO_BORROW_SELECTOR",
    "MORPHO_BUNDLER_ADDRESSES",
    "MORPHO_DEPLOYMENT_BLOCKS",
    "MORPHO_FLASH_LOAN_SELECTOR",
    "MORPHO_LIQUIDATE_SELECTOR",
    "MORPHO_MARKETS",
    "MORPHO_REPAY_SELECTOR",
    "MORPHO_SET_AUTHORIZATION_SELECTOR",
    "MORPHO_SUPPLY_COLLATERAL_SELECTOR",
    "MORPHO_SUPPLY_SELECTOR",
    "MORPHO_WITHDRAW_COLLATERAL_SELECTOR",
    "MORPHO_WITHDRAW_SELECTOR",
    "MarketNotFoundError",
    "MorphoBlueAdapter",
    "MorphoBlueConfig",
    "MorphoBlueEvent",
    "MorphoBlueEventType",
    "MorphoBlueHealthFactor",
    "MorphoBlueInterestRateMode",
    "MorphoBlueMarketParams",
    "MorphoBlueMarketState",
    "MorphoBluePosition",
    "MorphoBlueReceiptParser",
    "MorphoBlueSDK",
    "MorphoBlueSDKError",
    "ParseResult",
    "PositionNotFoundError",
    "RPCError",
    "RepayEventData",
    "SDKMarketInfo",
    "SDKMarketParams",
    "SDKMarketState",
    "SDKPosition",
    "SUPPORTED_CHAINS",
    "SetAuthorizationEventData",
    "SupplyCollateralEventData",
    "SupplyEventData",
    "TOPIC_TO_EVENT",
    "TransactionResult",
    "TransferEventData",
    "UnsupportedChainError",
    "WithdrawCollateralEventData",
    "WithdrawEventData",
    "create_adapter_with_prices",
    "create_test_adapter",
]

_LAZY: dict[str, tuple[str, str]] = {
    "AccrueInterestEventData": (".receipt_parser", "AccrueInterestEventData"),
    "BorrowEventData": (".receipt_parser", "BorrowEventData"),
    "CreateMarketEventData": (".receipt_parser", "CreateMarketEventData"),
    "DEFAULT_GAS_ESTIMATES": (".adapter", "DEFAULT_GAS_ESTIMATES"),
    "ERC20_APPROVE_SELECTOR": (".adapter", "ERC20_APPROVE_SELECTOR"),
    "EVENT_NAME_TO_TYPE": (".receipt_parser", "EVENT_NAME_TO_TYPE"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "FlashLoanEventData": (".receipt_parser", "FlashLoanEventData"),
    "LiquidateEventData": (".receipt_parser", "LiquidateEventData"),
    "MAX_UINT256": (".adapter", "MAX_UINT256"),
    "MORPHO_ACCRUE_INTEREST_SELECTOR": (".adapter", "MORPHO_ACCRUE_INTEREST_SELECTOR"),
    "MORPHO_BLUE_ADDRESSES": (".adapter", "MORPHO_BLUE_ADDRESSES"),
    "MORPHO_BORROW_SELECTOR": (".adapter", "MORPHO_BORROW_SELECTOR"),
    "MORPHO_BUNDLER_ADDRESSES": (".adapter", "MORPHO_BUNDLER_ADDRESSES"),
    "MORPHO_DEPLOYMENT_BLOCKS": (".sdk", "MORPHO_DEPLOYMENT_BLOCKS"),
    "MORPHO_FLASH_LOAN_SELECTOR": (".adapter", "MORPHO_FLASH_LOAN_SELECTOR"),
    "MORPHO_LIQUIDATE_SELECTOR": (".adapter", "MORPHO_LIQUIDATE_SELECTOR"),
    "MORPHO_MARKETS": (".adapter", "MORPHO_MARKETS"),
    "MORPHO_REPAY_SELECTOR": (".adapter", "MORPHO_REPAY_SELECTOR"),
    "MORPHO_SET_AUTHORIZATION_SELECTOR": (".adapter", "MORPHO_SET_AUTHORIZATION_SELECTOR"),
    "MORPHO_SUPPLY_COLLATERAL_SELECTOR": (".adapter", "MORPHO_SUPPLY_COLLATERAL_SELECTOR"),
    "MORPHO_SUPPLY_SELECTOR": (".adapter", "MORPHO_SUPPLY_SELECTOR"),
    "MORPHO_WITHDRAW_COLLATERAL_SELECTOR": (".adapter", "MORPHO_WITHDRAW_COLLATERAL_SELECTOR"),
    "MORPHO_WITHDRAW_SELECTOR": (".adapter", "MORPHO_WITHDRAW_SELECTOR"),
    "MarketNotFoundError": (".sdk", "MarketNotFoundError"),
    "MorphoBlueAdapter": (".adapter", "MorphoBlueAdapter"),
    "MorphoBlueConfig": (".adapter", "MorphoBlueConfig"),
    "MorphoBlueEvent": (".receipt_parser", "MorphoBlueEvent"),
    "MorphoBlueEventType": (".receipt_parser", "MorphoBlueEventType"),
    "MorphoBlueHealthFactor": (".adapter", "MorphoBlueHealthFactor"),
    "MorphoBlueInterestRateMode": (".adapter", "MorphoBlueInterestRateMode"),
    "MorphoBlueMarketParams": (".adapter", "MorphoBlueMarketParams"),
    "MorphoBlueMarketState": (".adapter", "MorphoBlueMarketState"),
    "MorphoBluePosition": (".adapter", "MorphoBluePosition"),
    "MorphoBlueReceiptParser": (".receipt_parser", "MorphoBlueReceiptParser"),
    "MorphoBlueSDK": (".sdk", "MorphoBlueSDK"),
    "MorphoBlueSDKError": (".sdk", "MorphoBlueSDKError"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "PositionNotFoundError": (".sdk", "PositionNotFoundError"),
    "RPCError": (".sdk", "RPCError"),
    "RepayEventData": (".receipt_parser", "RepayEventData"),
    "SDKMarketInfo": (".sdk", "SDKMarketInfo"),
    "SDKMarketParams": (".sdk", "SDKMarketParams"),
    "SDKMarketState": (".sdk", "SDKMarketState"),
    "SDKPosition": (".sdk", "SDKPosition"),
    "SUPPORTED_CHAINS": (".sdk", "SUPPORTED_CHAINS"),
    "SetAuthorizationEventData": (".receipt_parser", "SetAuthorizationEventData"),
    "SupplyCollateralEventData": (".receipt_parser", "SupplyCollateralEventData"),
    "SupplyEventData": (".receipt_parser", "SupplyEventData"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionResult": (".adapter", "TransactionResult"),
    "TransferEventData": (".receipt_parser", "TransferEventData"),
    "UnsupportedChainError": (".sdk", "UnsupportedChainError"),
    "WithdrawCollateralEventData": (".receipt_parser", "WithdrawCollateralEventData"),
    "WithdrawEventData": (".receipt_parser", "WithdrawEventData"),
    "create_adapter_with_prices": (".adapter", "create_adapter_with_prices"),
    "create_test_adapter": (".adapter", "create_test_adapter"),
}

_registered = False


def _register_once() -> None:
    """Fire ``register_connector`` once on first strategy-side access.

    Deferred so importing the connector's gateway-side surface during
    gateway boot does not pull ``framework.intents.vocabulary`` into the
    partially-initialised config-init chain (VIB-4835).
    """
    global _registered
    if _registered:
        return
    _registered = True
    try:
        from almanak.connectors._strategy_base.registry import MatrixEntry, register_connector
        from almanak.framework.intents.vocabulary import IntentType

        from .addresses import MORPHO_BLUE

        register_connector(
            name="morpho_blue",
            intents=(
                IntentType.SUPPLY,
                IntentType.BORROW,
                IntentType.REPAY,
                IntentType.WITHDRAW,
                IntentType.FLASH_LOAN,
            ),
            chains=("ethereum", "base", "arbitrum", "polygon", "monad"),
            # Matrix output is owned by the connector (VIB-4856 / W4).
            # Single ``lending`` row across every chain Morpho Blue ships;
            # flash-loan capability exists on-chain but the matrix has
            # historically scoped ``flash_loan`` to Balancer V2 (the
            # cross-protocol venue).
            matrix_entries=(
                MatrixEntry(
                    matrix_name="morpho_blue",
                    category="lending",
                    chains=frozenset(MORPHO_BLUE.keys()),
                ),
            ),
        )
    except Exception:
        _registered = False
        raise


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    _register_once()
    return value
