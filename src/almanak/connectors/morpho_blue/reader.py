"""Pinned Morpho market shares, collateral and explicitly requested oracle evidence."""

from almanak.connectors.morpho_blue.deployment import immutable_address, read_deployment
from almanak.connectors.morpho_blue.domain import (
    PARAMS_ABI,
    Accounting,
    AccountObservation,
    Market,
    MarketObservation,
    OracleQuote,
    Position,
)
from almanak.connectors.morpho_blue.math import adaptive_rate
from almanak.gateway.ethereum import DataError

MARKET_FIELDS = (
    "supply_assets",
    "supply_shares",
    "borrow_assets",
    "borrow_shares",
    "last_update",
    "fee",
)


def market_accounting(reader, market, block):
    values = reader.call(
        market.core,
        "market(bytes32)",
        block,
        inputs=("bytes32",),
        args=(bytes.fromhex(str(market.market_id)[2:]),),
        outputs=("uint128",) * 6,
    )
    return Accounting(**dict(zip(MARKET_FIELDS, values, strict=True)))


def oracle_scale(reader, market, block, values, loan, collateral):
    decimals = {}
    for key in ("BASE_FEED_1", "BASE_FEED_2", "QUOTE_FEED_1", "QUOTE_FEED_2"):
        feed = immutable_address(values[key])
        decimals[key] = (
            reader.call(feed, "decimals()", block, outputs=("uint8",))[0] if values[key] else 0
        )
    exponent = (
        36
        + loan.decimals
        + decimals["QUOTE_FEED_1"]
        + decimals["QUOTE_FEED_2"]
        - collateral.decimals
        - decimals["BASE_FEED_1"]
        - decimals["BASE_FEED_2"]
    )
    if not 0 <= exponent <= 77 or 10**exponent != values["SCALE_FACTOR"]:
        raise DataError(
            "oracle_scale", "Morpho oracle scale differs from its token and feed precision"
        )


def conversion_price(reader, market, block, loan, collateral):
    # This compiled oracle converts exactly 1e18 shares and multiplies by 1e18.
    # Bind the vault's underlying token instead of inventing a native-asset peg.
    vault = market.collateral_asset.reference
    underlying = reader.call(vault, "asset()", block, outputs=("address",))[0]
    if (
        loan.decimals != 18
        or collateral.decimals != 18
        or underlying != market.loan_asset.reference
    ):
        raise DataError(
            "oracle_scale", "Morpho conversion oracle requires its 18-decimal underlying"
        )
    assets = reader.call(
        vault, "convertToAssets(uint256)", block, inputs=("uint256",), args=(10**18,)
    )[0]
    return assets * 10**18


def price_evidence(reader, market, block, deployment, loan, collateral):
    try:
        expected = None
        if deployment.oracle_kind == "erc4626_conversion":
            expected = conversion_price(reader, market, block, loan, collateral)
        else:
            oracle_scale(reader, market, block, deployment.oracle_inputs, loan, collateral)
        value = reader.call(market.oracle, "price()", block)[0]
        if expected is not None and value != expected:
            raise DataError(
                "oracle_scale", "Morpho conversion price differs from its exact vault input"
            )
        return {"price": OracleQuote(source=market, block=block, price=value), "price_error": None}
    except (DataError, ValueError) as error:
        return {"price": None, "price_error": str(error)}


def observed_rate(reader, market, block, accounting):
    identity = bytes.fromhex(str(market.market_id)[2:])
    target = reader.call(
        market.irm,
        "rateAtTarget(bytes32)",
        block,
        inputs=("bytes32",),
        args=(identity,),
        outputs=("int256",),
    )[0]
    fields = tuple(getattr(accounting, name) for name in MARKET_FIELDS)
    rate = reader.call(
        market.irm,
        "borrowRateView(" + PARAMS_ABI + ",(uint128,uint128,uint128,uint128,uint128,uint128))",
        block,
        inputs=(PARAMS_ABI, "(uint128,uint128,uint128,uint128,uint128,uint128)"),
        args=(market.parameters, fields),
    )[0]
    calculated, _ = adaptive_rate(
        accounting.supply_assets,
        accounting.borrow_assets,
        target,
        block.timestamp - accounting.last_update,
    )
    if calculated != rate:
        raise DataError(
            "accounting", "Morpho observed IRM rate differs from the verified revision math"
        )
    return target, rate


def read_market(reader, market: Market, block) -> MarketObservation:
    deployment = read_deployment(reader, market, block)
    loan = reader.asset(market.loan_asset, block)
    collateral = reader.asset(market.collateral_asset, block)
    accounting = market_accounting(reader, market, block)
    if accounting.last_update == 0 or accounting.last_update > block.timestamp:
        raise DataError("market", "Morpho market is uninitialized or follows its observed block")
    target, rate = observed_rate(reader, market, block, accounting)
    return MarketObservation(
        source=market,
        block=block,
        contracts=deployment.contracts,
        loan_asset=loan,
        collateral_asset=collateral,
        accounting=accounting,
        rate_at_target=target,
        borrow_rate=rate,
        fee_recipient=reader.call(market.core, "feeRecipient()", block, outputs=("address",))[0],
        core_loan_balance=reader.call(
            market.loan_asset.reference,
            "balanceOf(address)",
            block,
            inputs=("address",),
            args=(market.core,),
        )[0],
        **price_evidence(reader, market, block, deployment, loan, collateral),
    )


def read_account(reader, market: MarketObservation, fund) -> AccountObservation:
    source, block = market.source, market.block
    if reader._pinned != block or reader.chain != block.chain or fund.chain != block.chain:
        raise DataError("block_mismatch", "Morpho fund observation requires the same pinned market")
    return AccountObservation(
        market=market, fund=fund, position=read_position(reader, source, fund.address, block)
    )


def read_position(reader, market: Market, holder, block) -> Position:
    """What one address holds in a market at the block: both share kinds and collateral."""
    values = reader.call(
        market.core,
        "position(bytes32,address)",
        block,
        inputs=("bytes32", "address"),
        args=(bytes.fromhex(str(market.market_id)[2:]), holder),
        outputs=("uint256", "uint128", "uint128"),
    )
    return Position(supply_shares=values[0], borrow_shares=values[1], collateral=values[2])
