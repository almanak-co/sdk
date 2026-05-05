"""Almanak Protocol Connectors.

Adapters and connectors for various DeFi protocols. Public names are resolved
lazily via :pep:`562` ``__getattr__`` so that touching a single connector
(e.g. ``from almanak.framework.connectors.polymarket import ClobClient``) does
not eagerly load the 19 other connectors and their adapters / receipt parsers
/ SDKs into memory. Critical for the gateway sidecar where only the connectors
actively used by the deployed strategy should be resident.

Available Connectors:
- Aerodrome: Solidly-based AMM on Base with volatile and stable pools
- Enso: DEX aggregator for optimal routing across multiple DEXs
- GMX v2: Perpetuals trading on GMX v2 (Arbitrum, Avalanche)
- Hyperliquid: Perpetual futures on Hyperliquid (Mainnet, Testnet)
- Aave V3: Lending protocol on multiple chains
- Uniswap V3: DEX for token swaps on multiple chains
- Curve: Stablecoin DEX and LP pools
- Morpho Blue / Spark / Compound V3: Lending protocols
- TraderJoe V2 / PancakeSwap V3: AMMs
- Across / Stargate: Cross-chain bridge adapters
- Polymarket: Prediction market trading via hybrid CLOB + on-chain
- Lido / Ethena / Gimo: Yield/staking primitives
"""

from typing import TYPE_CHECKING

from almanak._lazy import LazySpec, build_lazy_module_dispatch

if TYPE_CHECKING:
    from .aave_v3 import (
        AAVE_STABLE_RATE_MODE,
        AAVE_V3_ORACLE_ADDRESSES,
        AAVE_V3_POOL_ADDRESSES,
        AAVE_V3_POOL_DATA_PROVIDER_ADDRESSES,
        AAVE_VARIABLE_RATE_MODE,
        EMODE_CATEGORIES,
        AaveV3Adapter,
        AaveV3Config,
        AaveV3EModeCategory,
        AaveV3Event,
        AaveV3EventType,
        AaveV3FlashLoanParams,
        AaveV3HealthFactorCalculation,
        AaveV3InterestRateMode,
        AaveV3Position,
        AaveV3ReceiptParser,
        AaveV3ReserveData,
        AaveV3UserAccountData,
        BorrowEventData,
        FlashLoanEventData,
        IsolationModeDebtUpdatedEventData,
        LiquidationCallEventData,
        ParseResult,
        RepayEventData,
        ReserveDataUpdatedEventData,
        SupplyEventData,
        TransactionResult,
        UserEModeSetEventData,
        WithdrawEventData,
    )
    from .aave_v3 import (
        DEFAULT_GAS_ESTIMATES as AAVE_DEFAULT_GAS_ESTIMATES,
    )
    from .aave_v3 import (
        EVENT_TOPICS as AAVE_EVENT_TOPICS,
    )
    from .across import (
        ACROSS_CHAIN_IDS,
        ACROSS_SPOKE_POOL_ADDRESSES,
        ACROSS_SUPPORTED_TOKENS,
        AcrossBridgeAdapter,
        AcrossConfig,
        AcrossError,
        AcrossQuoteError,
        AcrossReceiptParser,
        AcrossStatusError,
        AcrossTransactionError,
    )
    from .aerodrome import (
        AERODROME_ADDRESSES,
        AERODROME_GAS_ESTIMATES,
        AerodromeAdapter,
        AerodromeConfig,
        AerodromeEvent,
        AerodromeEventType,
        AerodromeReceiptParser,
        AerodromeSDK,
        AerodromeSDKError,
    )
    from .aerodrome import (
        EVENT_TOPICS as AERODROME_EVENT_TOPICS,
    )
    from .aerodrome import (
        BurnEventData as AerodromeBurnEventData,
    )
    from .aerodrome import (
        InsufficientLiquidityError as AerodromeInsufficientLiquidityError,
    )
    from .aerodrome import (
        LiquidityResult as AerodromeLiquidityResult,
    )
    from .aerodrome import (
        MintEventData as AerodromeMintEventData,
    )
    from .aerodrome import (
        ParsedLiquidityResult as AerodromeParsedLiquidityResult,
    )
    from .aerodrome import (
        ParsedSwapResult as AerodromeParsedSwapResult,
    )
    from .aerodrome import (
        ParseResult as AerodromeParseResult,
    )
    from .aerodrome import (
        PoolInfo as AerodromePoolInfo,
    )
    from .aerodrome import (
        PoolNotFoundError as AerodromePoolNotFoundError,
    )
    from .aerodrome import (
        PoolType as AerodromePoolType,
    )
    from .aerodrome import (
        SwapEventData as AerodromeSwapEventData,
    )
    from .aerodrome import (
        SwapQuote as AerodromeSwapQuote,
    )
    from .aerodrome import (
        SwapResult as AerodromeSwapResult,
    )
    from .aerodrome import (
        SwapRoute as AerodromeSwapRoute,
    )
    from .aerodrome import (
        SwapType as AerodromeSwapType,
    )
    from .aerodrome import (
        TransactionData as AerodromeTransactionData,
    )
    from .bridge_base import (
        BridgeAdapter,
        BridgeError,
        BridgeQuote,
        BridgeQuoteError,
        BridgeRoute,
        BridgeStatus,
        BridgeStatusEnum,
        BridgeStatusError,
        BridgeTransactionError,
    )
    from .compound_v3 import (
        COMPOUND_V3_COMET_ADDRESSES,
        COMPOUND_V3_MARKETS,
        CompoundV3Adapter,
        CompoundV3Config,
        CompoundV3Event,
        CompoundV3EventType,
        CompoundV3HealthFactor,
        CompoundV3MarketInfo,
        CompoundV3Position,
        CompoundV3ReceiptParser,
    )
    from .compound_v3 import (
        DEFAULT_GAS_ESTIMATES as COMPOUND_V3_DEFAULT_GAS_ESTIMATES,
    )
    from .compound_v3 import (
        EVENT_TOPICS as COMPOUND_V3_EVENT_TOPICS,
    )
    from .compound_v3 import (
        TOPIC_TO_EVENT as COMPOUND_V3_TOPIC_TO_EVENT,
    )
    from .compound_v3 import (
        ParseResult as CompoundV3ParseResult,
    )
    from .compound_v3 import (
        TransactionResult as CompoundV3TransactionResult,
    )
    from .curve import (
        CURVE_ADDRESSES,
        CURVE_GAS_ESTIMATES,
        CURVE_POOLS,
        AddLiquidityEventData,
        CurveAdapter,
        CurveConfig,
        CurveEvent,
        CurveEventType,
        CurveReceiptParser,
        LiquidityResult,
        PoolInfo,
        PoolType,
        RemoveLiquidityEventData,
    )
    from .curve import (
        EVENT_TOPICS as CURVE_EVENT_TOPICS,
    )
    from .curve import (
        TOPIC_TO_EVENT as CURVE_TOPIC_TO_EVENT,
    )
    from .curve import (
        ParseResult as CurveParseResult,
    )
    from .curve import (
        SwapEventData as CurveSwapEventData,
    )
    from .curve import (
        SwapResult as CurveSwapResult,
    )
    from .curve import (
        TransactionData as CurveTransactionData,
    )
    from .enso import (
        EnsoAdapter,
        EnsoAPIError,
        EnsoClient,
        EnsoConfig,
        EnsoConfigError,
        EnsoError,
        EnsoReceiptParser,
        EnsoValidationError,
        PriceImpactExceedsThresholdError,
        RouteParams,
        RouteTransaction,
        RoutingStrategy,
    )
    from .enso import (
        Hop as EnsoHop,
    )
    from .enso import (
        Quote as EnsoQuote,
    )
    from .enso import (
        Transaction as EnsoTransaction,
    )
    from .ethena import (
        DEFAULT_GAS_ESTIMATES as ETHENA_DEFAULT_GAS_ESTIMATES,
    )
    from .ethena import (
        ETHENA_ADDRESSES,
        ETHENA_COOLDOWN_ASSETS_SELECTOR,
        ETHENA_COOLDOWN_SHARES_SELECTOR,
        ETHENA_DEPOSIT_SELECTOR,
        ETHENA_UNSTAKE_SELECTOR,
        EthenaAdapter,
        EthenaConfig,
        EthenaEventType,
        EthenaReceiptParser,
    )
    from .ethena import (
        EVENT_TOPICS as ETHENA_EVENT_TOPICS,
    )
    from .ethena import (
        TOPIC_TO_EVENT as ETHENA_TOPIC_TO_EVENT,
    )
    from .ethena import (
        ParseResult as EthenaParseResult,
    )
    from .ethena import (
        StakeEventData as EthenaStakeEventData,
    )
    from .ethena import (
        TransactionResult as EthenaTransactionResult,
    )
    from .ethena import (
        UnstakeEventData as EthenaUnstakeEventData,
    )
    from .gimo import (
        DEFAULT_GAS_ESTIMATES as GIMO_DEFAULT_GAS_ESTIMATES,
    )
    from .gimo import (
        EVENT_TOPICS as GIMO_EVENT_TOPICS,
    )
    from .gimo import (
        GIMO_ADDRESSES,
        GIMO_STAKE_SELECTOR,
        GIMO_UNSTAKE_SELECTOR,
        GimoAdapter,
        GimoConfig,
        GimoEventType,
        GimoReceiptParser,
    )
    from .gimo import (
        TOPIC_TO_EVENT as GIMO_TOPIC_TO_EVENT,
    )
    from .gimo import (
        ParseResult as GimoParseResult,
    )
    from .gimo import (
        StakeEventData as GimoStakeEventData,
    )
    from .gimo import (
        TransactionResult as GimoTransactionResult,
    )
    from .gimo import (
        UnstakeEventData as GimoUnstakeEventData,
    )
    from .gmx_v2 import (
        DEFAULT_EXECUTION_FEE,
        GMX_V2_ADDRESSES,
        GMX_V2_MARKETS,
        GMXv2Adapter,
        GMXv2Config,
        GMXv2Event,
        GMXv2EventType,
        GMXv2Order,
        GMXv2OrderType,
        GMXv2Position,
        GMXv2PositionSide,
        GMXv2ReceiptParser,
    )
    from .hyperliquid import (
        HYPERLIQUID_API_URLS,
        HYPERLIQUID_ASSETS,
        HYPERLIQUID_CHAIN_IDS,
        HYPERLIQUID_WS_URLS,
        CancelResult,
        EIP712Signer,
        ExternalSigner,
        HyperliquidAdapter,
        HyperliquidConfig,
        HyperliquidMarginMode,
        HyperliquidNetwork,
        HyperliquidOrder,
        HyperliquidOrderSide,
        HyperliquidOrderStatus,
        HyperliquidOrderType,
        HyperliquidPosition,
        HyperliquidPositionSide,
        HyperliquidTimeInForce,
        OrderResult,
    )
    from .lido import (
        DEFAULT_GAS_ESTIMATES as LIDO_DEFAULT_GAS_ESTIMATES,
    )
    from .lido import (
        EVENT_TOPICS as LIDO_EVENT_TOPICS,
    )
    from .lido import (
        LIDO_ADDRESSES,
        LIDO_STAKE_SELECTOR,
        LIDO_UNWRAP_SELECTOR,
        LIDO_WRAP_SELECTOR,
        LidoAdapter,
        LidoConfig,
        LidoEventType,
        LidoReceiptParser,
    )
    from .lido import (
        TOPIC_TO_EVENT as LIDO_TOPIC_TO_EVENT,
    )
    from .lido import (
        ParseResult as LidoParseResult,
    )
    from .lido import (
        StakeEventData as LidoStakeEventData,
    )
    from .lido import (
        TransactionResult as LidoTransactionResult,
    )
    from .lido import (
        UnwrapEventData as LidoUnwrapEventData,
    )
    from .lido import (
        WrapEventData as LidoWrapEventData,
    )
    from .morpho_blue import (
        DEFAULT_GAS_ESTIMATES as MORPHO_DEFAULT_GAS_ESTIMATES,
    )
    from .morpho_blue import (
        EVENT_TOPICS as MORPHO_EVENT_TOPICS,
    )
    from .morpho_blue import (
        MORPHO_BLUE_ADDRESS,
        MORPHO_BLUE_ADDRESSES,
        MORPHO_BUNDLER_ADDRESSES,
        MORPHO_MARKETS,
        AccrueInterestEventData,
        CreateMarketEventData,
        MorphoBlueAdapter,
        MorphoBlueConfig,
        MorphoBlueEvent,
        MorphoBlueEventType,
        MorphoBlueHealthFactor,
        MorphoBlueInterestRateMode,
        MorphoBlueMarketParams,
        MorphoBlueMarketState,
        MorphoBluePosition,
        MorphoBlueReceiptParser,
        SetAuthorizationEventData,
        SupplyCollateralEventData,
        WithdrawCollateralEventData,
    )
    from .morpho_blue import (
        TOPIC_TO_EVENT as MORPHO_TOPIC_TO_EVENT,
    )
    from .morpho_blue import (
        BorrowEventData as MorphoBorrowEventData,
    )
    from .morpho_blue import (
        FlashLoanEventData as MorphoFlashLoanEventData,
    )
    from .morpho_blue import (
        LiquidateEventData as MorphoLiquidateEventData,
    )
    from .morpho_blue import (
        ParseResult as MorphoParseResult,
    )
    from .morpho_blue import (
        RepayEventData as MorphoRepayEventData,
    )
    from .morpho_blue import (
        SupplyEventData as MorphoSupplyEventData,
    )
    from .morpho_blue import (
        TransactionResult as MorphoTransactionResult,
    )
    from .morpho_blue import (
        TransferEventData as MorphoTransferEventData,
    )
    from .morpho_blue import (
        WithdrawEventData as MorphoWithdrawEventData,
    )
    from .pancakeswap_v3 import (
        DEFAULT_GAS_ESTIMATES as PANCAKESWAP_V3_DEFAULT_GAS_ESTIMATES,
    )
    from .pancakeswap_v3 import (
        EVENT_NAME_TO_TYPE as PANCAKESWAP_V3_EVENT_NAME_TO_TYPE,
    )
    from .pancakeswap_v3 import (
        EVENT_TOPICS as PANCAKESWAP_V3_EVENT_TOPICS,
    )
    from .pancakeswap_v3 import (
        EXACT_INPUT_SINGLE_SELECTOR,
        EXACT_OUTPUT_SINGLE_SELECTOR,
        PANCAKESWAP_V3_ADDRESSES,
        PancakeSwapV3Adapter,
        PancakeSwapV3Config,
        PancakeSwapV3EventType,
        PancakeSwapV3ReceiptParser,
    )
    from .pancakeswap_v3 import (
        FEE_TIERS as PANCAKESWAP_V3_FEE_TIERS,
    )
    from .pancakeswap_v3 import (
        TOPIC_TO_EVENT as PANCAKESWAP_V3_TOPIC_TO_EVENT,
    )
    from .pancakeswap_v3 import (
        ParseResult as PancakeSwapV3ParseResult,
    )
    from .pancakeswap_v3 import (
        SwapEventData as PancakeSwapV3SwapEventData,
    )
    from .pancakeswap_v3 import (
        TransactionResult as PancakeSwapV3TransactionResult,
    )
    from .polymarket import (
        ApiCredentials,
        ClobClient,
        CtfSDK,
        GammaMarket,
        OrderBook,
        PolymarketAdapter,
        PolymarketConfig,
        PolymarketReceiptParser,
        PolymarketSDK,
        Position,
        Trade,
    )
    from .polymarket import (
        ApiCredentials as PolymarketApiCredentials,
    )
    from .polymarket import (
        GammaMarket as PolymarketGammaMarket,
    )
    from .polymarket import (
        OrderBook as PolymarketOrderBook,
    )
    from .polymarket import (
        Position as PolymarketPosition,
    )
    from .polymarket import (
        Trade as PolymarketTrade,
    )
    from .spark import (
        DEFAULT_GAS_ESTIMATES as SPARK_DEFAULT_GAS_ESTIMATES,
    )
    from .spark import (
        SPARK_BORROW_SELECTOR,
        SPARK_ORACLE_ADDRESSES,
        SPARK_POOL_ADDRESSES,
        SPARK_POOL_DATA_PROVIDER_ADDRESSES,
        SPARK_REPAY_SELECTOR,
        SPARK_STABLE_RATE_MODE,
        SPARK_SUPPLY_SELECTOR,
        SPARK_VARIABLE_RATE_MODE,
        SPARK_WITHDRAW_SELECTOR,
        SparkAdapter,
        SparkConfig,
        SparkEventType,
        SparkReceiptParser,
    )
    from .spark import (
        BorrowEventData as SparkBorrowEventData,
    )
    from .spark import (
        ParseResult as SparkParseResult,
    )
    from .spark import (
        RepayEventData as SparkRepayEventData,
    )
    from .spark import (
        SupplyEventData as SparkSupplyEventData,
    )
    from .spark import (
        TransactionResult as SparkTransactionResult,
    )
    from .spark import (
        WithdrawEventData as SparkWithdrawEventData,
    )
    from .stargate import (
        STARGATE_CHAIN_IDS,
        STARGATE_POOL_IDS,
        STARGATE_ROUTER_ADDRESSES,
        STARGATE_SUPPORTED_TOKENS,
        StargateBridgeAdapter,
        StargateConfig,
        StargateError,
        StargateQuoteError,
        StargateReceiptParser,
        StargateStatusError,
        StargateTransactionError,
    )
    from .traderjoe_v2 import (
        BIN_ID_OFFSET,
        BIN_STEPS,
        TRADERJOE_V2_ADDRESSES,
        InvalidBinStepError,
        TraderJoeV2Adapter,
        TraderJoeV2Config,
        TraderJoeV2Event,
        TraderJoeV2EventType,
        TraderJoeV2ReceiptParser,
        TraderJoeV2SDK,
        TraderJoeV2SDKError,
    )
    from .traderjoe_v2 import (
        DEFAULT_GAS_ESTIMATES as TRADERJOE_V2_GAS_ESTIMATES,
    )
    from .traderjoe_v2 import (
        EVENT_TOPICS as TRADERJOE_V2_EVENT_TOPICS,
    )
    from .traderjoe_v2 import (
        LiquidityEventData as TraderJoeV2LiquidityEventData,
    )
    from .traderjoe_v2 import (
        LiquidityPosition as TraderJoeV2LiquidityPosition,
    )
    from .traderjoe_v2 import (
        ParsedLiquidityResult as TraderJoeV2ParsedLiquidityResult,
    )
    from .traderjoe_v2 import (
        ParsedSwapResult as TraderJoeV2ParsedSwapResult,
    )
    from .traderjoe_v2 import (
        ParseResult as TraderJoeV2ParseResult,
    )
    from .traderjoe_v2 import (
        PoolInfo as TraderJoeV2PoolInfo,
    )
    from .traderjoe_v2 import (
        PoolNotFoundError as TraderJoeV2PoolNotFoundError,
    )
    from .traderjoe_v2 import (
        SwapEventData as TraderJoeV2SwapEventData,
    )
    from .traderjoe_v2 import (
        SwapQuote as TraderJoeV2SwapQuote,
    )
    from .traderjoe_v2 import (
        SwapResult as TraderJoeV2SwapResult,
    )
    from .traderjoe_v2 import (
        SwapType as TraderJoeV2SwapType,
    )
    from .traderjoe_v2 import (
        TransactionData as TraderJoeV2TransactionData,
    )
    from .uniswap_v3 import (
        DEFAULT_FEE_TIER,
        FEE_TIERS,
        UNISWAP_V3_ADDRESSES,
        UNISWAP_V3_GAS_ESTIMATES,
        SwapQuote,
        SwapResult,
        SwapType,
        UniswapV3Adapter,
        UniswapV3Config,
    )
    from .uniswap_v3 import (
        TransactionData as UniswapTransactionData,
    )


# Maps each public name to (relative subpackage, attribute name on that subpackage).
# Generated to mirror the original eager re-export surface; keep in sync with
# subpackage ``__init__.py`` when names are added or renamed.
_LAZY_IMPORTS: dict[str, LazySpec] = {
    # .aave_v3
    "AAVE_DEFAULT_GAS_ESTIMATES": (".aave_v3", "DEFAULT_GAS_ESTIMATES"),
    "AAVE_EVENT_TOPICS": (".aave_v3", "EVENT_TOPICS"),
    "AAVE_STABLE_RATE_MODE": (".aave_v3", "AAVE_STABLE_RATE_MODE"),
    "AAVE_V3_ORACLE_ADDRESSES": (".aave_v3", "AAVE_V3_ORACLE_ADDRESSES"),
    "AAVE_V3_POOL_ADDRESSES": (".aave_v3", "AAVE_V3_POOL_ADDRESSES"),
    "AAVE_V3_POOL_DATA_PROVIDER_ADDRESSES": (".aave_v3", "AAVE_V3_POOL_DATA_PROVIDER_ADDRESSES"),
    "AAVE_VARIABLE_RATE_MODE": (".aave_v3", "AAVE_VARIABLE_RATE_MODE"),
    "AaveV3Adapter": (".aave_v3", "AaveV3Adapter"),
    "AaveV3Config": (".aave_v3", "AaveV3Config"),
    "AaveV3EModeCategory": (".aave_v3", "AaveV3EModeCategory"),
    "AaveV3Event": (".aave_v3", "AaveV3Event"),
    "AaveV3EventType": (".aave_v3", "AaveV3EventType"),
    "AaveV3FlashLoanParams": (".aave_v3", "AaveV3FlashLoanParams"),
    "AaveV3HealthFactorCalculation": (".aave_v3", "AaveV3HealthFactorCalculation"),
    "AaveV3InterestRateMode": (".aave_v3", "AaveV3InterestRateMode"),
    "AaveV3Position": (".aave_v3", "AaveV3Position"),
    "AaveV3ReceiptParser": (".aave_v3", "AaveV3ReceiptParser"),
    "AaveV3ReserveData": (".aave_v3", "AaveV3ReserveData"),
    "AaveV3UserAccountData": (".aave_v3", "AaveV3UserAccountData"),
    "BorrowEventData": (".aave_v3", "BorrowEventData"),
    "EMODE_CATEGORIES": (".aave_v3", "EMODE_CATEGORIES"),
    "FlashLoanEventData": (".aave_v3", "FlashLoanEventData"),
    "IsolationModeDebtUpdatedEventData": (".aave_v3", "IsolationModeDebtUpdatedEventData"),
    "LiquidationCallEventData": (".aave_v3", "LiquidationCallEventData"),
    "ParseResult": (".aave_v3", "ParseResult"),
    "RepayEventData": (".aave_v3", "RepayEventData"),
    "ReserveDataUpdatedEventData": (".aave_v3", "ReserveDataUpdatedEventData"),
    "SupplyEventData": (".aave_v3", "SupplyEventData"),
    "TransactionResult": (".aave_v3", "TransactionResult"),
    "UserEModeSetEventData": (".aave_v3", "UserEModeSetEventData"),
    "WithdrawEventData": (".aave_v3", "WithdrawEventData"),
    # .across
    "ACROSS_CHAIN_IDS": (".across", "ACROSS_CHAIN_IDS"),
    "ACROSS_SPOKE_POOL_ADDRESSES": (".across", "ACROSS_SPOKE_POOL_ADDRESSES"),
    "ACROSS_SUPPORTED_TOKENS": (".across", "ACROSS_SUPPORTED_TOKENS"),
    "AcrossBridgeAdapter": (".across", "AcrossBridgeAdapter"),
    "AcrossConfig": (".across", "AcrossConfig"),
    "AcrossError": (".across", "AcrossError"),
    "AcrossQuoteError": (".across", "AcrossQuoteError"),
    "AcrossReceiptParser": (".across", "AcrossReceiptParser"),
    "AcrossStatusError": (".across", "AcrossStatusError"),
    "AcrossTransactionError": (".across", "AcrossTransactionError"),
    # .aerodrome
    "AERODROME_ADDRESSES": (".aerodrome", "AERODROME_ADDRESSES"),
    "AERODROME_EVENT_TOPICS": (".aerodrome", "EVENT_TOPICS"),
    "AERODROME_GAS_ESTIMATES": (".aerodrome", "AERODROME_GAS_ESTIMATES"),
    "AerodromeAdapter": (".aerodrome", "AerodromeAdapter"),
    "AerodromeBurnEventData": (".aerodrome", "BurnEventData"),
    "AerodromeConfig": (".aerodrome", "AerodromeConfig"),
    "AerodromeEvent": (".aerodrome", "AerodromeEvent"),
    "AerodromeEventType": (".aerodrome", "AerodromeEventType"),
    "AerodromeInsufficientLiquidityError": (".aerodrome", "InsufficientLiquidityError"),
    "AerodromeLiquidityResult": (".aerodrome", "LiquidityResult"),
    "AerodromeMintEventData": (".aerodrome", "MintEventData"),
    "AerodromeParseResult": (".aerodrome", "ParseResult"),
    "AerodromeParsedLiquidityResult": (".aerodrome", "ParsedLiquidityResult"),
    "AerodromeParsedSwapResult": (".aerodrome", "ParsedSwapResult"),
    "AerodromePoolInfo": (".aerodrome", "PoolInfo"),
    "AerodromePoolNotFoundError": (".aerodrome", "PoolNotFoundError"),
    "AerodromePoolType": (".aerodrome", "PoolType"),
    "AerodromeReceiptParser": (".aerodrome", "AerodromeReceiptParser"),
    "AerodromeSDK": (".aerodrome", "AerodromeSDK"),
    "AerodromeSDKError": (".aerodrome", "AerodromeSDKError"),
    "AerodromeSwapEventData": (".aerodrome", "SwapEventData"),
    "AerodromeSwapQuote": (".aerodrome", "SwapQuote"),
    "AerodromeSwapResult": (".aerodrome", "SwapResult"),
    "AerodromeSwapRoute": (".aerodrome", "SwapRoute"),
    "AerodromeSwapType": (".aerodrome", "SwapType"),
    "AerodromeTransactionData": (".aerodrome", "TransactionData"),
    # .bridge_base
    "BridgeAdapter": (".bridge_base", "BridgeAdapter"),
    "BridgeError": (".bridge_base", "BridgeError"),
    "BridgeQuote": (".bridge_base", "BridgeQuote"),
    "BridgeQuoteError": (".bridge_base", "BridgeQuoteError"),
    "BridgeRoute": (".bridge_base", "BridgeRoute"),
    "BridgeStatus": (".bridge_base", "BridgeStatus"),
    "BridgeStatusEnum": (".bridge_base", "BridgeStatusEnum"),
    "BridgeStatusError": (".bridge_base", "BridgeStatusError"),
    "BridgeTransactionError": (".bridge_base", "BridgeTransactionError"),
    # .compound_v3
    "COMPOUND_V3_COMET_ADDRESSES": (".compound_v3", "COMPOUND_V3_COMET_ADDRESSES"),
    "COMPOUND_V3_DEFAULT_GAS_ESTIMATES": (".compound_v3", "DEFAULT_GAS_ESTIMATES"),
    "COMPOUND_V3_EVENT_TOPICS": (".compound_v3", "EVENT_TOPICS"),
    "COMPOUND_V3_MARKETS": (".compound_v3", "COMPOUND_V3_MARKETS"),
    "COMPOUND_V3_TOPIC_TO_EVENT": (".compound_v3", "TOPIC_TO_EVENT"),
    "CompoundV3Adapter": (".compound_v3", "CompoundV3Adapter"),
    "CompoundV3Config": (".compound_v3", "CompoundV3Config"),
    "CompoundV3Event": (".compound_v3", "CompoundV3Event"),
    "CompoundV3EventType": (".compound_v3", "CompoundV3EventType"),
    "CompoundV3HealthFactor": (".compound_v3", "CompoundV3HealthFactor"),
    "CompoundV3MarketInfo": (".compound_v3", "CompoundV3MarketInfo"),
    "CompoundV3ParseResult": (".compound_v3", "ParseResult"),
    "CompoundV3Position": (".compound_v3", "CompoundV3Position"),
    "CompoundV3ReceiptParser": (".compound_v3", "CompoundV3ReceiptParser"),
    "CompoundV3TransactionResult": (".compound_v3", "TransactionResult"),
    # .curve
    "AddLiquidityEventData": (".curve", "AddLiquidityEventData"),
    "CURVE_ADDRESSES": (".curve", "CURVE_ADDRESSES"),
    "CURVE_EVENT_TOPICS": (".curve", "EVENT_TOPICS"),
    "CURVE_GAS_ESTIMATES": (".curve", "CURVE_GAS_ESTIMATES"),
    "CURVE_POOLS": (".curve", "CURVE_POOLS"),
    "CURVE_TOPIC_TO_EVENT": (".curve", "TOPIC_TO_EVENT"),
    "CurveAdapter": (".curve", "CurveAdapter"),
    "CurveConfig": (".curve", "CurveConfig"),
    "CurveEvent": (".curve", "CurveEvent"),
    "CurveEventType": (".curve", "CurveEventType"),
    "CurveParseResult": (".curve", "ParseResult"),
    "CurveReceiptParser": (".curve", "CurveReceiptParser"),
    "CurveSwapEventData": (".curve", "SwapEventData"),
    "CurveSwapResult": (".curve", "SwapResult"),
    "CurveTransactionData": (".curve", "TransactionData"),
    "LiquidityResult": (".curve", "LiquidityResult"),
    "PoolInfo": (".curve", "PoolInfo"),
    "PoolType": (".curve", "PoolType"),
    "RemoveLiquidityEventData": (".curve", "RemoveLiquidityEventData"),
    # .enso
    "EnsoAPIError": (".enso", "EnsoAPIError"),
    "EnsoAdapter": (".enso", "EnsoAdapter"),
    "EnsoClient": (".enso", "EnsoClient"),
    "EnsoConfig": (".enso", "EnsoConfig"),
    "EnsoConfigError": (".enso", "EnsoConfigError"),
    "EnsoError": (".enso", "EnsoError"),
    "EnsoHop": (".enso", "Hop"),
    "EnsoQuote": (".enso", "Quote"),
    "EnsoReceiptParser": (".enso", "EnsoReceiptParser"),
    "EnsoTransaction": (".enso", "Transaction"),
    "EnsoValidationError": (".enso", "EnsoValidationError"),
    "PriceImpactExceedsThresholdError": (".enso", "PriceImpactExceedsThresholdError"),
    "RouteParams": (".enso", "RouteParams"),
    "RouteTransaction": (".enso", "RouteTransaction"),
    "RoutingStrategy": (".enso", "RoutingStrategy"),
    # .ethena
    "ETHENA_ADDRESSES": (".ethena", "ETHENA_ADDRESSES"),
    "ETHENA_COOLDOWN_ASSETS_SELECTOR": (".ethena", "ETHENA_COOLDOWN_ASSETS_SELECTOR"),
    "ETHENA_COOLDOWN_SHARES_SELECTOR": (".ethena", "ETHENA_COOLDOWN_SHARES_SELECTOR"),
    "ETHENA_DEFAULT_GAS_ESTIMATES": (".ethena", "DEFAULT_GAS_ESTIMATES"),
    "ETHENA_DEPOSIT_SELECTOR": (".ethena", "ETHENA_DEPOSIT_SELECTOR"),
    "ETHENA_EVENT_TOPICS": (".ethena", "EVENT_TOPICS"),
    "ETHENA_TOPIC_TO_EVENT": (".ethena", "TOPIC_TO_EVENT"),
    "ETHENA_UNSTAKE_SELECTOR": (".ethena", "ETHENA_UNSTAKE_SELECTOR"),
    "EthenaAdapter": (".ethena", "EthenaAdapter"),
    "EthenaConfig": (".ethena", "EthenaConfig"),
    "EthenaEventType": (".ethena", "EthenaEventType"),
    "EthenaParseResult": (".ethena", "ParseResult"),
    "EthenaReceiptParser": (".ethena", "EthenaReceiptParser"),
    "EthenaStakeEventData": (".ethena", "StakeEventData"),
    "EthenaTransactionResult": (".ethena", "TransactionResult"),
    "EthenaUnstakeEventData": (".ethena", "UnstakeEventData"),
    # .gimo
    "GIMO_ADDRESSES": (".gimo", "GIMO_ADDRESSES"),
    "GIMO_DEFAULT_GAS_ESTIMATES": (".gimo", "DEFAULT_GAS_ESTIMATES"),
    "GIMO_EVENT_TOPICS": (".gimo", "EVENT_TOPICS"),
    "GIMO_STAKE_SELECTOR": (".gimo", "GIMO_STAKE_SELECTOR"),
    "GIMO_TOPIC_TO_EVENT": (".gimo", "TOPIC_TO_EVENT"),
    "GIMO_UNSTAKE_SELECTOR": (".gimo", "GIMO_UNSTAKE_SELECTOR"),
    "GimoAdapter": (".gimo", "GimoAdapter"),
    "GimoConfig": (".gimo", "GimoConfig"),
    "GimoEventType": (".gimo", "GimoEventType"),
    "GimoParseResult": (".gimo", "ParseResult"),
    "GimoReceiptParser": (".gimo", "GimoReceiptParser"),
    "GimoStakeEventData": (".gimo", "StakeEventData"),
    "GimoTransactionResult": (".gimo", "TransactionResult"),
    "GimoUnstakeEventData": (".gimo", "UnstakeEventData"),
    # .gmx_v2
    "DEFAULT_EXECUTION_FEE": (".gmx_v2", "DEFAULT_EXECUTION_FEE"),
    "GMX_V2_ADDRESSES": (".gmx_v2", "GMX_V2_ADDRESSES"),
    "GMX_V2_MARKETS": (".gmx_v2", "GMX_V2_MARKETS"),
    "GMXv2Adapter": (".gmx_v2", "GMXv2Adapter"),
    "GMXv2Config": (".gmx_v2", "GMXv2Config"),
    "GMXv2Event": (".gmx_v2", "GMXv2Event"),
    "GMXv2EventType": (".gmx_v2", "GMXv2EventType"),
    "GMXv2Order": (".gmx_v2", "GMXv2Order"),
    "GMXv2OrderType": (".gmx_v2", "GMXv2OrderType"),
    "GMXv2Position": (".gmx_v2", "GMXv2Position"),
    "GMXv2PositionSide": (".gmx_v2", "GMXv2PositionSide"),
    "GMXv2ReceiptParser": (".gmx_v2", "GMXv2ReceiptParser"),
    # .hyperliquid
    "CancelResult": (".hyperliquid", "CancelResult"),
    "EIP712Signer": (".hyperliquid", "EIP712Signer"),
    "ExternalSigner": (".hyperliquid", "ExternalSigner"),
    "HYPERLIQUID_API_URLS": (".hyperliquid", "HYPERLIQUID_API_URLS"),
    "HYPERLIQUID_ASSETS": (".hyperliquid", "HYPERLIQUID_ASSETS"),
    "HYPERLIQUID_CHAIN_IDS": (".hyperliquid", "HYPERLIQUID_CHAIN_IDS"),
    "HYPERLIQUID_WS_URLS": (".hyperliquid", "HYPERLIQUID_WS_URLS"),
    "HyperliquidAdapter": (".hyperliquid", "HyperliquidAdapter"),
    "HyperliquidConfig": (".hyperliquid", "HyperliquidConfig"),
    "HyperliquidMarginMode": (".hyperliquid", "HyperliquidMarginMode"),
    "HyperliquidNetwork": (".hyperliquid", "HyperliquidNetwork"),
    "HyperliquidOrder": (".hyperliquid", "HyperliquidOrder"),
    "HyperliquidOrderSide": (".hyperliquid", "HyperliquidOrderSide"),
    "HyperliquidOrderStatus": (".hyperliquid", "HyperliquidOrderStatus"),
    "HyperliquidOrderType": (".hyperliquid", "HyperliquidOrderType"),
    "HyperliquidPosition": (".hyperliquid", "HyperliquidPosition"),
    "HyperliquidPositionSide": (".hyperliquid", "HyperliquidPositionSide"),
    "HyperliquidTimeInForce": (".hyperliquid", "HyperliquidTimeInForce"),
    "OrderResult": (".hyperliquid", "OrderResult"),
    # .lido
    "LIDO_ADDRESSES": (".lido", "LIDO_ADDRESSES"),
    "LIDO_DEFAULT_GAS_ESTIMATES": (".lido", "DEFAULT_GAS_ESTIMATES"),
    "LIDO_EVENT_TOPICS": (".lido", "EVENT_TOPICS"),
    "LIDO_STAKE_SELECTOR": (".lido", "LIDO_STAKE_SELECTOR"),
    "LIDO_TOPIC_TO_EVENT": (".lido", "TOPIC_TO_EVENT"),
    "LIDO_UNWRAP_SELECTOR": (".lido", "LIDO_UNWRAP_SELECTOR"),
    "LIDO_WRAP_SELECTOR": (".lido", "LIDO_WRAP_SELECTOR"),
    "LidoAdapter": (".lido", "LidoAdapter"),
    "LidoConfig": (".lido", "LidoConfig"),
    "LidoEventType": (".lido", "LidoEventType"),
    "LidoParseResult": (".lido", "ParseResult"),
    "LidoReceiptParser": (".lido", "LidoReceiptParser"),
    "LidoStakeEventData": (".lido", "StakeEventData"),
    "LidoTransactionResult": (".lido", "TransactionResult"),
    "LidoUnwrapEventData": (".lido", "UnwrapEventData"),
    "LidoWrapEventData": (".lido", "WrapEventData"),
    # .morpho_blue
    "AccrueInterestEventData": (".morpho_blue", "AccrueInterestEventData"),
    "CreateMarketEventData": (".morpho_blue", "CreateMarketEventData"),
    "MORPHO_BLUE_ADDRESS": (".morpho_blue", "MORPHO_BLUE_ADDRESS"),
    "MORPHO_BLUE_ADDRESSES": (".morpho_blue", "MORPHO_BLUE_ADDRESSES"),
    "MORPHO_BUNDLER_ADDRESSES": (".morpho_blue", "MORPHO_BUNDLER_ADDRESSES"),
    "MORPHO_DEFAULT_GAS_ESTIMATES": (".morpho_blue", "DEFAULT_GAS_ESTIMATES"),
    "MORPHO_EVENT_TOPICS": (".morpho_blue", "EVENT_TOPICS"),
    "MORPHO_MARKETS": (".morpho_blue", "MORPHO_MARKETS"),
    "MORPHO_TOPIC_TO_EVENT": (".morpho_blue", "TOPIC_TO_EVENT"),
    "MorphoBlueAdapter": (".morpho_blue", "MorphoBlueAdapter"),
    "MorphoBlueConfig": (".morpho_blue", "MorphoBlueConfig"),
    "MorphoBlueEvent": (".morpho_blue", "MorphoBlueEvent"),
    "MorphoBlueEventType": (".morpho_blue", "MorphoBlueEventType"),
    "MorphoBlueHealthFactor": (".morpho_blue", "MorphoBlueHealthFactor"),
    "MorphoBlueInterestRateMode": (".morpho_blue", "MorphoBlueInterestRateMode"),
    "MorphoBlueMarketParams": (".morpho_blue", "MorphoBlueMarketParams"),
    "MorphoBlueMarketState": (".morpho_blue", "MorphoBlueMarketState"),
    "MorphoBluePosition": (".morpho_blue", "MorphoBluePosition"),
    "MorphoBlueReceiptParser": (".morpho_blue", "MorphoBlueReceiptParser"),
    "MorphoBorrowEventData": (".morpho_blue", "BorrowEventData"),
    "MorphoFlashLoanEventData": (".morpho_blue", "FlashLoanEventData"),
    "MorphoLiquidateEventData": (".morpho_blue", "LiquidateEventData"),
    "MorphoParseResult": (".morpho_blue", "ParseResult"),
    "MorphoRepayEventData": (".morpho_blue", "RepayEventData"),
    "MorphoSupplyEventData": (".morpho_blue", "SupplyEventData"),
    "MorphoTransactionResult": (".morpho_blue", "TransactionResult"),
    "MorphoTransferEventData": (".morpho_blue", "TransferEventData"),
    "MorphoWithdrawEventData": (".morpho_blue", "WithdrawEventData"),
    "SetAuthorizationEventData": (".morpho_blue", "SetAuthorizationEventData"),
    "SupplyCollateralEventData": (".morpho_blue", "SupplyCollateralEventData"),
    "WithdrawCollateralEventData": (".morpho_blue", "WithdrawCollateralEventData"),
    # .pancakeswap_v3
    "EXACT_INPUT_SINGLE_SELECTOR": (".pancakeswap_v3", "EXACT_INPUT_SINGLE_SELECTOR"),
    "EXACT_OUTPUT_SINGLE_SELECTOR": (".pancakeswap_v3", "EXACT_OUTPUT_SINGLE_SELECTOR"),
    "PANCAKESWAP_V3_ADDRESSES": (".pancakeswap_v3", "PANCAKESWAP_V3_ADDRESSES"),
    "PANCAKESWAP_V3_DEFAULT_GAS_ESTIMATES": (".pancakeswap_v3", "DEFAULT_GAS_ESTIMATES"),
    "PANCAKESWAP_V3_EVENT_NAME_TO_TYPE": (".pancakeswap_v3", "EVENT_NAME_TO_TYPE"),
    "PANCAKESWAP_V3_EVENT_TOPICS": (".pancakeswap_v3", "EVENT_TOPICS"),
    "PANCAKESWAP_V3_FEE_TIERS": (".pancakeswap_v3", "FEE_TIERS"),
    "PANCAKESWAP_V3_TOPIC_TO_EVENT": (".pancakeswap_v3", "TOPIC_TO_EVENT"),
    "PancakeSwapV3Adapter": (".pancakeswap_v3", "PancakeSwapV3Adapter"),
    "PancakeSwapV3Config": (".pancakeswap_v3", "PancakeSwapV3Config"),
    "PancakeSwapV3EventType": (".pancakeswap_v3", "PancakeSwapV3EventType"),
    "PancakeSwapV3ParseResult": (".pancakeswap_v3", "ParseResult"),
    "PancakeSwapV3ReceiptParser": (".pancakeswap_v3", "PancakeSwapV3ReceiptParser"),
    "PancakeSwapV3SwapEventData": (".pancakeswap_v3", "SwapEventData"),
    "PancakeSwapV3TransactionResult": (".pancakeswap_v3", "TransactionResult"),
    # .polymarket
    "ApiCredentials": (".polymarket", "ApiCredentials"),
    "ClobClient": (".polymarket", "ClobClient"),
    "CtfSDK": (".polymarket", "CtfSDK"),
    "GammaMarket": (".polymarket", "GammaMarket"),
    "OrderBook": (".polymarket", "OrderBook"),
    "PolymarketAdapter": (".polymarket", "PolymarketAdapter"),
    "PolymarketApiCredentials": (".polymarket", "ApiCredentials"),
    "PolymarketConfig": (".polymarket", "PolymarketConfig"),
    "PolymarketGammaMarket": (".polymarket", "GammaMarket"),
    "PolymarketOrderBook": (".polymarket", "OrderBook"),
    "PolymarketPosition": (".polymarket", "Position"),
    "PolymarketReceiptParser": (".polymarket", "PolymarketReceiptParser"),
    "PolymarketSDK": (".polymarket", "PolymarketSDK"),
    "PolymarketTrade": (".polymarket", "Trade"),
    "Position": (".polymarket", "Position"),
    "Trade": (".polymarket", "Trade"),
    # .spark
    "SPARK_BORROW_SELECTOR": (".spark", "SPARK_BORROW_SELECTOR"),
    "SPARK_DEFAULT_GAS_ESTIMATES": (".spark", "DEFAULT_GAS_ESTIMATES"),
    "SPARK_ORACLE_ADDRESSES": (".spark", "SPARK_ORACLE_ADDRESSES"),
    "SPARK_POOL_ADDRESSES": (".spark", "SPARK_POOL_ADDRESSES"),
    "SPARK_POOL_DATA_PROVIDER_ADDRESSES": (".spark", "SPARK_POOL_DATA_PROVIDER_ADDRESSES"),
    "SPARK_REPAY_SELECTOR": (".spark", "SPARK_REPAY_SELECTOR"),
    "SPARK_STABLE_RATE_MODE": (".spark", "SPARK_STABLE_RATE_MODE"),
    "SPARK_SUPPLY_SELECTOR": (".spark", "SPARK_SUPPLY_SELECTOR"),
    "SPARK_VARIABLE_RATE_MODE": (".spark", "SPARK_VARIABLE_RATE_MODE"),
    "SPARK_WITHDRAW_SELECTOR": (".spark", "SPARK_WITHDRAW_SELECTOR"),
    "SparkAdapter": (".spark", "SparkAdapter"),
    "SparkBorrowEventData": (".spark", "BorrowEventData"),
    "SparkConfig": (".spark", "SparkConfig"),
    "SparkEventType": (".spark", "SparkEventType"),
    "SparkParseResult": (".spark", "ParseResult"),
    "SparkReceiptParser": (".spark", "SparkReceiptParser"),
    "SparkRepayEventData": (".spark", "RepayEventData"),
    "SparkSupplyEventData": (".spark", "SupplyEventData"),
    "SparkTransactionResult": (".spark", "TransactionResult"),
    "SparkWithdrawEventData": (".spark", "WithdrawEventData"),
    # .stargate
    "STARGATE_CHAIN_IDS": (".stargate", "STARGATE_CHAIN_IDS"),
    "STARGATE_POOL_IDS": (".stargate", "STARGATE_POOL_IDS"),
    "STARGATE_ROUTER_ADDRESSES": (".stargate", "STARGATE_ROUTER_ADDRESSES"),
    "STARGATE_SUPPORTED_TOKENS": (".stargate", "STARGATE_SUPPORTED_TOKENS"),
    "StargateBridgeAdapter": (".stargate", "StargateBridgeAdapter"),
    "StargateConfig": (".stargate", "StargateConfig"),
    "StargateError": (".stargate", "StargateError"),
    "StargateQuoteError": (".stargate", "StargateQuoteError"),
    "StargateReceiptParser": (".stargate", "StargateReceiptParser"),
    "StargateStatusError": (".stargate", "StargateStatusError"),
    "StargateTransactionError": (".stargate", "StargateTransactionError"),
    # .traderjoe_v2
    "BIN_ID_OFFSET": (".traderjoe_v2", "BIN_ID_OFFSET"),
    "BIN_STEPS": (".traderjoe_v2", "BIN_STEPS"),
    "InvalidBinStepError": (".traderjoe_v2", "InvalidBinStepError"),
    "TRADERJOE_V2_ADDRESSES": (".traderjoe_v2", "TRADERJOE_V2_ADDRESSES"),
    "TRADERJOE_V2_EVENT_TOPICS": (".traderjoe_v2", "EVENT_TOPICS"),
    "TRADERJOE_V2_GAS_ESTIMATES": (".traderjoe_v2", "DEFAULT_GAS_ESTIMATES"),
    "TraderJoeV2Adapter": (".traderjoe_v2", "TraderJoeV2Adapter"),
    "TraderJoeV2Config": (".traderjoe_v2", "TraderJoeV2Config"),
    "TraderJoeV2Event": (".traderjoe_v2", "TraderJoeV2Event"),
    "TraderJoeV2EventType": (".traderjoe_v2", "TraderJoeV2EventType"),
    "TraderJoeV2LiquidityEventData": (".traderjoe_v2", "LiquidityEventData"),
    "TraderJoeV2LiquidityPosition": (".traderjoe_v2", "LiquidityPosition"),
    "TraderJoeV2ParseResult": (".traderjoe_v2", "ParseResult"),
    "TraderJoeV2ParsedLiquidityResult": (".traderjoe_v2", "ParsedLiquidityResult"),
    "TraderJoeV2ParsedSwapResult": (".traderjoe_v2", "ParsedSwapResult"),
    "TraderJoeV2PoolInfo": (".traderjoe_v2", "PoolInfo"),
    "TraderJoeV2PoolNotFoundError": (".traderjoe_v2", "PoolNotFoundError"),
    "TraderJoeV2ReceiptParser": (".traderjoe_v2", "TraderJoeV2ReceiptParser"),
    "TraderJoeV2SDK": (".traderjoe_v2", "TraderJoeV2SDK"),
    "TraderJoeV2SDKError": (".traderjoe_v2", "TraderJoeV2SDKError"),
    "TraderJoeV2SwapEventData": (".traderjoe_v2", "SwapEventData"),
    "TraderJoeV2SwapQuote": (".traderjoe_v2", "SwapQuote"),
    "TraderJoeV2SwapResult": (".traderjoe_v2", "SwapResult"),
    "TraderJoeV2SwapType": (".traderjoe_v2", "SwapType"),
    "TraderJoeV2TransactionData": (".traderjoe_v2", "TransactionData"),
    # .uniswap_v3
    "DEFAULT_FEE_TIER": (".uniswap_v3", "DEFAULT_FEE_TIER"),
    "FEE_TIERS": (".uniswap_v3", "FEE_TIERS"),
    "SwapQuote": (".uniswap_v3", "SwapQuote"),
    "SwapResult": (".uniswap_v3", "SwapResult"),
    "SwapType": (".uniswap_v3", "SwapType"),
    "UNISWAP_V3_ADDRESSES": (".uniswap_v3", "UNISWAP_V3_ADDRESSES"),
    "UNISWAP_V3_GAS_ESTIMATES": (".uniswap_v3", "UNISWAP_V3_GAS_ESTIMATES"),
    "UniswapTransactionData": (".uniswap_v3", "TransactionData"),
    "UniswapV3Adapter": (".uniswap_v3", "UniswapV3Adapter"),
    "UniswapV3Config": (".uniswap_v3", "UniswapV3Config"),
}

__all__ = [*sorted(_LAZY_IMPORTS)]

__getattr__, __dir__ = build_lazy_module_dispatch(_LAZY_IMPORTS, package=__name__, namespace=globals())
