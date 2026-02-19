"""Morpho Blue Connector.

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

Example:
    from almanak.framework.connectors.morpho_blue import (
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

from almanak.core.contracts import MORPHO_BLUE_ADDRESS

from .adapter import (
    DEFAULT_GAS_ESTIMATES,
    ERC20_APPROVE_SELECTOR,
    MAX_UINT256,
    MORPHO_ACCRUE_INTEREST_SELECTOR,
    # Constants
    MORPHO_BLUE_ADDRESSES,
    MORPHO_BORROW_SELECTOR,
    MORPHO_BUNDLER_ADDRESSES,
    MORPHO_FLASH_LOAN_SELECTOR,
    MORPHO_LIQUIDATE_SELECTOR,
    MORPHO_MARKETS,
    MORPHO_REPAY_SELECTOR,
    MORPHO_SET_AUTHORIZATION_SELECTOR,
    MORPHO_SUPPLY_COLLATERAL_SELECTOR,
    # Function selectors
    MORPHO_SUPPLY_SELECTOR,
    MORPHO_WITHDRAW_COLLATERAL_SELECTOR,
    MORPHO_WITHDRAW_SELECTOR,
    # Adapter
    MorphoBlueAdapter,
    MorphoBlueConfig,
    MorphoBlueHealthFactor,
    # Enums
    MorphoBlueInterestRateMode,
    # Data classes
    MorphoBlueMarketParams,
    MorphoBlueMarketState,
    MorphoBluePosition,
    TransactionResult,
    # Factory functions
    create_adapter_with_prices,
    create_test_adapter,
)
from .receipt_parser import (
    EVENT_NAME_TO_TYPE,
    # Constants
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    AccrueInterestEventData,
    BorrowEventData,
    CreateMarketEventData,
    FlashLoanEventData,
    LiquidateEventData,
    # Event class
    MorphoBlueEvent,
    MorphoBlueEventType,
    # Parser
    MorphoBlueReceiptParser,
    ParseResult,
    RepayEventData,
    SetAuthorizationEventData,
    SupplyCollateralEventData,
    # Event data classes
    SupplyEventData,
    TransferEventData,
    WithdrawCollateralEventData,
    WithdrawEventData,
)
from .sdk import (
    # Constants
    MORPHO_DEPLOYMENT_BLOCKS,
    SUPPORTED_CHAINS,
    # Exceptions
    MarketNotFoundError,
    # SDK
    MorphoBlueSDK,
    MorphoBlueSDKError,
    PositionNotFoundError,
    RPCError,
    # Data classes
    SDKMarketInfo,
    SDKMarketParams,
    SDKMarketState,
    SDKPosition,
    UnsupportedChainError,
)

__all__ = [
    # Adapter
    "MorphoBlueAdapter",
    "MorphoBlueConfig",
    # Factory functions
    "create_adapter_with_prices",
    "create_test_adapter",
    # SDK
    "MorphoBlueSDK",
    # SDK Data classes
    "SDKPosition",
    "SDKMarketState",
    "SDKMarketParams",
    "SDKMarketInfo",
    # SDK Exceptions
    "MorphoBlueSDKError",
    "MarketNotFoundError",
    "PositionNotFoundError",
    "UnsupportedChainError",
    "RPCError",
    # SDK Constants
    "SUPPORTED_CHAINS",
    "MORPHO_DEPLOYMENT_BLOCKS",
    # Data classes
    "MorphoBlueMarketParams",
    "MorphoBlueMarketState",
    "MorphoBluePosition",
    "MorphoBlueHealthFactor",
    "TransactionResult",
    # Enums
    "MorphoBlueInterestRateMode",
    # Constants
    "MORPHO_BLUE_ADDRESS",
    "MORPHO_BLUE_ADDRESSES",
    "MORPHO_BUNDLER_ADDRESSES",
    "MORPHO_MARKETS",
    "DEFAULT_GAS_ESTIMATES",
    # Function selectors
    "MORPHO_SUPPLY_SELECTOR",
    "MORPHO_WITHDRAW_SELECTOR",
    "MORPHO_BORROW_SELECTOR",
    "MORPHO_REPAY_SELECTOR",
    "MORPHO_SUPPLY_COLLATERAL_SELECTOR",
    "MORPHO_WITHDRAW_COLLATERAL_SELECTOR",
    "MORPHO_LIQUIDATE_SELECTOR",
    "MORPHO_FLASH_LOAN_SELECTOR",
    "MORPHO_SET_AUTHORIZATION_SELECTOR",
    "MORPHO_ACCRUE_INTEREST_SELECTOR",
    "ERC20_APPROVE_SELECTOR",
    "MAX_UINT256",
    # Receipt Parser
    "MorphoBlueReceiptParser",
    "MorphoBlueEvent",
    "MorphoBlueEventType",
    # Event data classes
    "SupplyEventData",
    "WithdrawEventData",
    "BorrowEventData",
    "RepayEventData",
    "SupplyCollateralEventData",
    "WithdrawCollateralEventData",
    "LiquidateEventData",
    "FlashLoanEventData",
    "CreateMarketEventData",
    "SetAuthorizationEventData",
    "AccrueInterestEventData",
    "TransferEventData",
    "ParseResult",
    # Event constants
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
