"""Aave V4 debt ownership, explicit oracle evidence and read-only health projection."""

from typing import Any

from eth_abi import decode

from almanak.connectors.aave_v4.models import (
    BorrowProjection,
    CollateralObservation,
    DebtAllocation,
    OraclePrice,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.safe_fixture import calldata


def position_components(reader, target, block) -> tuple[dict[str, Any], int]:
    args = (target.reserve.reserve_id, target.fund.address)
    drawn, premium, offset, supplied, _ = reader.call(
        target.reserve.spoke,
        "getUserPosition(uint256,address)",
        block,
        inputs=("uint256", "address"),
        args=args,
        outputs=("uint120", "uint120", "int200", "uint120", "uint32"),
    )
    enabled, _ = reader.call(
        target.reserve.spoke,
        "getUserReserveStatus(uint256,address)",
        block,
        inputs=("uint256", "address"),
        args=args,
        outputs=("bool", "bool"),
    )
    return dict(
        drawn_shares=drawn,
        premium_shares=premium,
        premium_offset_ray=offset,
        collateral_enabled=enabled,
    ), supplied


def collateral_observations(reader, config, debt, block):
    result = []
    for name in debt.collateral:
        target = config.target(name)
        components, supplied = position_components(reader, target, block)
        result.append(
            CollateralObservation(
                target=target, enabled=components["collateral_enabled"], supplied_shares=supplied
            )
        )
    return tuple(result)


def snapshot_evidence(reader, config, allocation, block):
    target = config.target(allocation.name)
    components, _ = position_components(reader, target, block)
    if isinstance(allocation, DebtAllocation):
        components["collateral"] = collateral_observations(reader, config, allocation, block)
        try:
            account = reader.call(
                target.reserve.spoke,
                "getUserAccountData(address)",
                block,
                inputs=("address",),
                args=(target.fund.address,),
                outputs=("uint256",) * 7,
            )
            components["health_factor"] = account[2]
        except DataError as error:
            # Debt and funding reads remain usable for risk reduction without prices.
            components["health_error"] = error.kind
    return components


def validate_oracle_sources(reader, config, debt, block):
    (oracle,) = reader.call(debt.reserve.spoke, "ORACLE()", block, outputs=("address",))
    if oracle != debt.oracle:
        raise DataError("source_mismatch", "Spoke oracle differs from the explicit risk source")
    allocations = (*[config.allocation(n) for n in debt.collateral], debt)
    feeds = (*debt.collateral_price_sources, debt.debt_price_source)
    validate_reserve_feeds(reader, oracle, allocations, feeds, block)
    prices = read_oracle_prices(reader, oracle, allocations, block)
    (decimals,) = reader.call(oracle, "decimals()", block, outputs=("uint8",))
    return tuple(
        OraclePrice(
            block=block,
            oracle=oracle,
            feed=feed,
            asset=allocation.reserve.asset,
            decimals=decimals,
            value=price,
        )
        for allocation, feed, price in zip(allocations, feeds, prices, strict=True)
    )


def reserve_source(reader, oracle, reserve_id, block):
    (source,) = reader.call(
        oracle,
        "getReserveSource(uint256)",
        block,
        inputs=("uint256",),
        args=(reserve_id,),
        outputs=("address",),
    )
    return source


def validate_reserve_feeds(reader, oracle, allocations, feeds, block):
    for allocation, feed in zip(allocations, feeds, strict=True):
        if reserve_source(reader, oracle, allocation.reserve.reserve_id, block) != feed:
            raise DataError("source_mismatch", "Aave reserve price feed changed")


def read_oracle_prices(reader, oracle, allocations, block):
    (prices,) = reader.call(
        oracle,
        "getReservesPrices(uint256[])",
        block,
        inputs=("uint256[]",),
        args=([a.reserve.reserve_id for a in allocations],),
        outputs=("uint256[]",),
    )
    if len(prices) != len(allocations) or any(price <= 0 for price in prices):
        raise DataError("unavailable", "borrowing requires measured positive oracle prices")
    return prices


def project_borrow(reader, config, allocation, call, block):
    if not isinstance(allocation, DebtAllocation):
        raise ValueError("health projection requires its configured debt allocation")
    prices = validate_oracle_sources(reader, config, allocation, block)
    health_call = calldata("getUserAccountData(address)", ("address",), (call.fund.address,))
    data = calldata(
        "multicall(bytes[])",
        ("bytes[]",),
        ([bytes.fromhex(call.data[2:]), bytes.fromhex(health_call[2:])],),
    )
    result = reader._rpc(
        "eth_call",
        [
            {"from": call.fund.address, "to": call.to, "data": data},
            {"blockHash": block.hash, "requireCanonical": True},
        ],
    )
    (results,) = decode(("bytes[]",), bytes.fromhex(result[2:]))
    account = decode(("uint256",) * 7, results[1])
    if account[2] < allocation.minimum_health_factor:
        raise DataError("risk_limit", "projected account health is below its configured threshold")
    return BorrowProjection(block=block, health_factor=account[2], prices=prices)
