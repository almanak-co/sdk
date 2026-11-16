"""Built-in Roles predicates retain the pool, coin order and fund receivers, not amounts."""

from eth_abi import encode

from almanak.connectors.curve.authority import capability_codec
from almanak.connectors.curve.models import Capabilities
from almanak.connectors.curve.pool_abi import layout, signature
from almanak.gateway.permission_models import PermissionManifest
from almanak.gateway.roles_conditions import (
    Condition,
    FunctionGrant,
    any_amount,
    bounded,
    combine,
    comparison,
    either,
    equal,
    matches,
)

# Amounts are uint256 on Curve; the uint128 bound is call sanity matching Uniswap V4.
MAXIMUM = 2**128 - 1


def finite_shares():
    return Condition(
        parameter=0,
        operator=1,
        children=(
            comparison(17, encode(("uint256",), (0,))),
            comparison(18, encode(("uint256",), (2**256 - 1,))),
        ),
    )


def nonnegative():
    return either(equal("uint256", 0), finite_shares())


def approval_grant(target, asset):
    return FunctionGrant(
        target=asset.reference,
        signature="approve(address,uint256)",
        condition=matches(equal("address", target.pool.pool), any_amount()),
        explanation=(
            "Coin allowance of any amount, including zero cleanup, to the exact Curve pool only.",
        ),
    )


def swap_grant(target, index):
    return FunctionGrant(
        target=target.pool.pool,
        signature=signature("exchange", layout(target.pool).exchange),
        condition=matches(
            equal(layout(target.pool).index, index),
            either(
                *(
                    equal(layout(target.pool).index, other)
                    for other in range(len(target.assets))
                    if other != index
                )
            ),
            bounded(MAXIMUM),
            finite_shares(),
            *((equal("bool", False),) if layout(target.pool).native_flag else ()),
            equal("address", target.fund.address),
        ),
        explanation=(
            "Exact input coin and another configured output coin in this pool, any nonzero "
            "uint128 input and a nonzero minimum output, paying the Safe.",
        ),
    )


def addition_grant(target):
    return FunctionGrant(
        target=target.pool.pool,
        signature=signature("add_liquidity", layout(target.pool).addition),
        condition=matches(
            matches(
                *(either(equal("uint256", 0), bounded(MAXIMUM)) for _ in target.assets),
                parameter=layout(target.pool).array_parameter,
            ),
            finite_shares(),
            *((equal("bool", False),) if layout(target.pool).native_flag else ()),
            equal("address", target.fund.address),
        ),
        explanation=(
            "Ordered exact-length coin inputs of any uint128 amount pay the configured pool "
            "and mint LP to the Safe, with a nonzero minimum of shares.",
        ),
    )


def proportional_grant(target):
    return FunctionGrant(
        target=target.pool.pool,
        signature=signature("remove_liquidity", layout(target.pool).proportional),
        condition=matches(
            finite_shares(),
            matches(
                *(nonnegative() for _ in target.assets),
                parameter=layout(target.pool).array_parameter,
            ),
            *((equal("bool", False),) if layout(target.pool).native_flag else ()),
            equal("address", target.fund.address),
            *((equal("bool", False),) if layout(target.pool).admin_flag else ()),
        ),
        explanation=(
            "Burn this Safe's pool shares and return every coin to the Safe; "
            "no optional admin fee collection is requested.",
        ),
    )


def single_coin_grant(target):
    return FunctionGrant(
        target=target.pool.pool,
        signature=signature("remove_liquidity_one_coin", layout(target.pool).single_coin),
        condition=matches(
            finite_shares(),
            equal(layout(target.pool).index, target.assets.index(target.single_coin_exit)),
            nonnegative(),
            *((equal("bool", False),) if layout(target.pool).native_flag else ()),
            equal("address", target.fund.address),
        ),
        explanation=(
            "Burn this Safe's pool shares into only the explicitly configured exit coin.",
        ),
    )


def derive(profile, capabilities):
    grants: list[FunctionGrant] = []
    for capability in capabilities.requirements:
        target = capability.target
        if target.fund != profile.fund:
            continue
        for index, asset in enumerate(target.assets):
            grants.extend((approval_grant(target, asset), swap_grant(target, index)))
        if target.liquidity_enabled:
            grants.extend((addition_grant(target), proportional_grant(target)))
            if target.single_coin_exit is not None:
                grants.append(single_coin_grant(target))
    return combine(grants)


class CurvePermissions:
    codec = capability_codec()
    manifest_type = PermissionManifest[Capabilities]
    derive = staticmethod(derive)
    runtime_checks = (
        "Quote minima, slippage, deadline freshness and retained inventory; "
        "pool calls have no on-chain deadline.",
        "Share minimum simulation, embedded LP fee accounting, allowance cleanup and recovery.",
        "Initial fund has no unrelated pool shares; gateway bounds burns to retained owned shares.",
    )

    @staticmethod
    def identities(config, account, reader):
        from almanak.connectors.curve.deployment import identities

        return identities(config, account, reader)
