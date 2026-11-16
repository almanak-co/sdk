"""Closed shapes of the HyperCore `/info` answers the whitelist admits.

Each model mirrors the API's own field names, so a recorded answer validates as
received. The mainnet recordings in `tests/fixtures/hyperliquid-mainnet-info.json.gz`
(`scripts/record_hyperliquid_info.py`) fix the shapes; a field they do not show is
admitted only where Hyperliquid's documentation shows it, and is marked so. Any other
field, or a missing one, refuses the answer. Amounts and prices stay the API's decimal
strings: exact, never floats.

Ledger deltas are the one open part. The recordings show seven delta types, but the
custody path's own (deposits, class transfers, sends to HyperEVM) cannot be recorded
before an account makes them. A ledger entry's envelope is closed; its delta names its
`type` and carries its other fields as received, for the reconciliation binding to
interpret per type.
"""

from typing import Annotated, Literal

from pydantic import AfterValidator, Field, JsonValue, TypeAdapter

from almanak.models import Address, Hash32, Model, UInt

# A decimal string as the API writes amounts, sizes and prices.
DecimalText = Annotated[str, Field(pattern=r"^-?(0|[1-9][0-9]*)(\.[0-9]+)?$")]
Millis = Annotated[int, Field(strict=True, ge=0, le=2**53 - 1)]
Leverage = Annotated[int, Field(strict=True, ge=1)]
# A market or token name as the API writes it: a perp (`BTC`), a spot pair (`@107`,
# `PURR/USDC`) or a HIP-3 market (`xyz:XYZ100`) in another account's fills.
Name = Annotated[str, Field(min_length=1, max_length=64)]
Label = Annotated[str, Field(max_length=64)]
TokenId = Annotated[str, Field(pattern=r"^0x[0-9a-f]{32}$")]
Cloid = Annotated[str, Field(pattern=r"^0x[0-9a-f]{32}$")]
Side = Literal["A", "B"]


class MarginTier(Model):
    lowerBound: DecimalText
    maxLeverage: Leverage


class MarginTable(Model):
    description: Label
    marginTiers: tuple[MarginTier, ...]


class PerpAsset(Model):
    name: Name
    szDecimals: UInt
    maxLeverage: Leverage
    marginTableId: UInt
    # Present only when true in the recordings.
    isDelisted: bool | None = None
    # The API deprecates onlyIsolated for marginMode; "noCross" is documented, not recorded.
    onlyIsolated: bool | None = None
    marginMode: Literal["strictIsolated", "noCross"] | None = None


class PerpMeta(Model):
    universe: tuple[PerpAsset, ...]
    marginTables: tuple[tuple[UInt, MarginTable], ...]
    collateralToken: UInt


class PerpAssetContext(Model):
    funding: DecimalText
    openInterest: DecimalText
    prevDayPx: DecimalText
    dayNtlVlm: DecimalText
    # A delisted market has no premium, mid or impact prices.
    premium: DecimalText | None
    oraclePx: DecimalText
    markPx: DecimalText
    midPx: DecimalText | None
    impactPxs: tuple[DecimalText, DecimalText] | None
    dayBaseVlm: DecimalText


def aligned(answer: tuple[PerpMeta, tuple[PerpAssetContext, ...]]):
    # The contexts are positional: the n-th belongs to the n-th asset of the universe.
    meta, contexts = answer
    if len(meta.universe) != len(contexts):
        raise ValueError("asset contexts do not align with the perp universe")
    return answer


MetaAndContexts = Annotated[tuple[PerpMeta, tuple[PerpAssetContext, ...]], AfterValidator(aligned)]


class EvmContract(Model):
    address: Address
    evm_extra_wei_decimals: Annotated[int, Field(strict=True, ge=-18, le=18)]


class SpotToken(Model):
    name: Name
    szDecimals: UInt
    weiDecimals: UInt
    index: UInt
    tokenId: TokenId
    isCanonical: bool
    evmContract: EvmContract | None
    fullName: str | None
    deployerTradingFeeShare: DecimalText
    deployerLabel: Label | None = None


class SpotPair(Model):
    tokens: tuple[UInt, UInt]
    name: Name
    index: UInt
    isCanonical: bool


class SpotMeta(Model):
    tokens: tuple[SpotToken, ...]
    universe: tuple[SpotPair, ...]


class BookLevel(Model):
    px: DecimalText
    sz: DecimalText
    n: UInt


class Book(Model):
    coin: Name
    time: Millis
    # Bids, then asks; a delisted market answers two empty sides.
    levels: tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]]


class MarginSummary(Model):
    accountValue: DecimalText
    totalNtlPos: DecimalText
    totalRawUsd: DecimalText
    totalMarginUsed: DecimalText


class CrossLeverage(Model):
    type: Literal["cross"]
    value: Leverage


class IsolatedLeverage(Model):
    # Documented, not recorded.
    type: Literal["isolated"]
    value: Leverage
    rawUsd: DecimalText


PerpLeverage = Annotated[CrossLeverage | IsolatedLeverage, Field(discriminator="type")]


class CumulativeFunding(Model):
    allTime: DecimalText
    sinceOpen: DecimalText
    sinceChange: DecimalText


class Position(Model):
    coin: Name
    szi: DecimalText
    leverage: PerpLeverage
    entryPx: DecimalText
    positionValue: DecimalText
    unrealizedPnl: DecimalText
    returnOnEquity: DecimalText
    liquidationPx: DecimalText | None
    marginUsed: DecimalText
    maxLeverage: Leverage
    cumFunding: CumulativeFunding


class AssetPosition(Model):
    type: Literal["oneWay"]
    position: Position


class PerpAccount(Model):
    marginSummary: MarginSummary
    crossMarginSummary: MarginSummary
    crossMaintenanceMarginUsed: DecimalText
    withdrawable: DecimalText
    assetPositions: tuple[AssetPosition, ...]
    time: Millis


class SpotBalance(Model):
    coin: Name
    token: UInt
    total: DecimalText
    hold: DecimalText
    entryNtl: DecimalText


class SpotAccount(Model):
    balances: tuple[SpotBalance, ...]
    # Recorded for unified accounts only: each token's amount still available after
    # maintenance margin, as (token index, amount).
    tokenToAvailableAfterMaintenance: tuple[tuple[UInt, DecimalText], ...] | None = None


class ActiveAsset(Model):
    # Answers for a market the account holds no position in, so leverage set while
    # flat is observable.
    user: Address
    coin: Name
    leverage: PerpLeverage
    maxTradeSzs: tuple[DecimalText, DecimalText]
    availableToTrade: tuple[DecimalText, DecimalText]
    markPx: DecimalText


class OpenOrder(Model):
    coin: Name
    side: Side
    limitPx: DecimalText
    sz: DecimalText
    oid: UInt
    timestamp: Millis
    origSz: DecimalText
    # Present only on an order placed with a client order ID.
    cloid: Cloid | None = None


class Agent(Model):
    # An approved agent; validUntil is the expiry in milliseconds, null when none was set.
    name: Label
    address: Address
    validUntil: Millis | None


class PlainRole(Model):
    role: Literal["missing", "user", "vault"]


class Principal(Model):
    user: Address


class AgentRole(Model):
    # An approved agent names the account it trades for.
    role: Literal["agent"]
    data: Principal


class Master(Model):
    master: Address


class SubAccountRole(Model):
    # Documented, not recorded.
    role: Literal["subAccount"]
    data: Master


Role = Annotated[PlainRole | AgentRole | SubAccountRole, Field(discriminator="role")]
# Recorded: "default". The others are documented.
Abstraction = Literal["unifiedAccount", "portfolioMargin", "disabled", "default", "dexAbstraction"]


class RateLimit(Model):
    cumVlm: DecimalText
    nRequestsUsed: UInt
    nRequestsCap: UInt
    nRequestsSurplus: UInt


# The order states the API documents; the recordings show filled and canceled.
OrderState = Literal[
    "open",
    "filled",
    "canceled",
    "triggered",
    "rejected",
    "marginCanceled",
    "vaultWithdrawalCanceled",
    "openInterestCapCanceled",
    "selfTradeCanceled",
    "reduceOnlyCanceled",
    "siblingFilledCanceled",
    "delistedCanceled",
    "liquidatedCanceled",
    "scheduledCancel",
    "tickRejected",
    "minTradeNtlRejected",
    "perpMarginRejected",
    "reduceOnlyRejected",
    "badAloPxRejected",
    "iocCancelRejected",
    "badTriggerPxRejected",
    "marketOrderNoLiquidityRejected",
    "positionIncreaseAtOpenInterestCapRejected",
    "positionFlipAtOpenInterestCapRejected",
    "tooAggressiveAtOpenInterestCapRejected",
    "openInterestIncreaseRejected",
    "insufficientSpotBalanceRejected",
    "oracleRejected",
    "perpMaxPositionRejected",
]


class Order(Model):
    coin: Name
    side: Side
    limitPx: DecimalText
    sz: DecimalText
    oid: UInt
    timestamp: Millis
    triggerCondition: Label
    isTrigger: bool
    triggerPx: DecimalText
    # A position's TP/SL orders; empty in the recordings.
    children: tuple["Order", ...]
    isPositionTpsl: bool
    reduceOnly: bool
    orderType: Label
    origSz: DecimalText
    tif: Label | None
    cloid: Cloid | None


class OrderRecord(Model):
    order: Order
    status: OrderState
    statusTimestamp: Millis


class KnownOrder(Model):
    status: Literal["order"]
    order: OrderRecord


class UnknownOrder(Model):
    status: Literal["unknownOid"]


class Liquidation(Model):
    liquidatedUser: Address
    markPx: DecimalText
    method: Label


class Fill(Model):
    coin: Name
    px: DecimalText
    sz: DecimalText
    side: Side
    time: Millis
    startPosition: DecimalText
    dir: Label
    closedPnl: DecimalText
    hash: Hash32
    oid: UInt
    crossed: bool
    # The total fee, inclusive of any builder fee.
    fee: DecimalText
    tid: UInt
    # Present only on a fill of an order placed with a client order ID.
    cloid: Cloid | None = None
    feeToken: Label
    twapId: UInt | None
    # Documented: present only when a builder fee was charged.
    builderFee: DecimalText | None = None
    # Present on both sides of a liquidation.
    liquidation: Liquidation | None = None


class FundingDelta(Model):
    type: Literal["funding"]
    coin: Name
    usdc: DecimalText
    szi: DecimalText
    fundingRate: DecimalText
    nSamples: UInt | None


class FundingPayment(Model):
    time: Millis
    # Zero in the recordings: a funding payment is no transaction of its own.
    hash: Hash32
    delta: FundingDelta


class Candle(Model):
    # One trade candle (D4). `t` and `T` are its open and close in milliseconds, `v` its
    # volume in the market's base unit and `n` its trade count; a minute without trades
    # is a flat candle with `n` zero.
    t: Millis
    T: Millis
    s: Name
    i: Label
    o: DecimalText
    c: DecimalText
    h: DecimalText
    l: DecimalText  # noqa: E741 - the API's field name
    v: DecimalText
    n: UInt


class FundingRate(Model):
    # One market funding rate (D4). The 3 November 2026 recordings show only decimals; a
    # third-party account of early 2023 data reports `NaN` premiums, refused here.
    coin: Name
    fundingRate: DecimalText
    premium: DecimalText
    time: Millis


def typed_delta(delta: dict) -> dict:
    kind = delta.get("type")
    if not isinstance(kind, str) or not kind:
        raise ValueError("a ledger delta must name its type")
    return delta


class LedgerUpdate(Model):
    time: Millis
    hash: Hash32
    delta: Annotated[dict[str, JsonValue], AfterValidator(typed_delta)]


RESPONSES: dict[str, TypeAdapter] = {
    "meta": TypeAdapter(PerpMeta),
    "metaAndAssetCtxs": TypeAdapter(MetaAndContexts),
    "spotMeta": TypeAdapter(SpotMeta),
    "l2Book": TypeAdapter(Book),
    "clearinghouseState": TypeAdapter(PerpAccount),
    "spotClearinghouseState": TypeAdapter(SpotAccount),
    "activeAssetData": TypeAdapter(ActiveAsset),
    "openOrders": TypeAdapter(tuple[OpenOrder, ...]),
    "extraAgents": TypeAdapter(tuple[Agent, ...]),
    "userRole": TypeAdapter(Role),
    "userAbstraction": TypeAdapter(Abstraction),
    "userRateLimit": TypeAdapter(RateLimit),
    "orderStatus": TypeAdapter(Annotated[KnownOrder | UnknownOrder, Field(discriminator="status")]),
    "userFillsByTime": TypeAdapter(tuple[Fill, ...]),
    "userFunding": TypeAdapter(tuple[FundingPayment, ...]),
    "userNonFundingLedgerUpdates": TypeAdapter(tuple[LedgerUpdate, ...]),
    "candleSnapshot": TypeAdapter(tuple[Candle, ...]),
    "fundingHistory": TypeAdapter(tuple[FundingRate, ...]),
}
