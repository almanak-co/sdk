"""Install-once V3 NFT grants retain exact call shapes and Safe recipients."""

from almanak.connectors.pancakeswap_v3 import abi
from almanak.connectors.v3_pool_grants import tick_conditions
from almanak.gateway.roles_conditions import (
    Condition,
    FunctionGrant,
    any_amount,
    bounded,
    comparison,
    either,
    equal,
    matches,
)

MAXIMUM = 2**128 - 1
BROAD_SCOPE = (
    "NFT IDs are unrestricted for compatible positions owned or approved to this Safe. "
    "Increases may consume existing manager allowances for other positions. "
    "The gateway enforces the retained strategy NFT, pool, range and inventory."
)


def optional_amount():
    """Zero or a uint128 deposit or minimum: call sanity, not an author amount.

    The bound matches Uniswap V4, whose deposit amounts are uint128.
    """
    return either(equal("uint256", 0), bounded(MAXIMUM))


def token_id():
    return comparison(17, bytes(32))


def grant(target, signature, condition, explanation):
    return FunctionGrant(
        target=target.contracts.position_manager,
        signature=signature,
        condition=condition,
        explanation=(explanation,),
    )


def mint_grant(target):
    return grant(
        target,
        f"mint({abi.MINT_PARAMS})",
        matches(
            matches(
                equal("address", target.pool.currency0.reference),
                equal("address", target.pool.currency1.reference),
                equal("uint24", target.pool.fee),
                *tick_conditions(),
                optional_amount(),
                optional_amount(),
                optional_amount(),
                optional_amount(),
                equal("address", target.fund.address),
                bounded(MAXIMUM),
                parameter=3,
            )
        ),
        "Exact configured pool and Safe recipient, any uint128 deposit amounts. Ticks are "
        "unrestricted; the gateway enforces the configured range or band.",
    )


def increase_grant(target):
    return grant(
        target,
        f"increaseLiquidity({abi.INCREASE_PARAMS})",
        matches(
            matches(
                token_id(),
                optional_amount(),
                optional_amount(),
                optional_amount(),
                optional_amount(),
                bounded(MAXIMUM),
                parameter=3,
            )
        ),
        BROAD_SCOPE + " Any uint128 deposit amounts.",
    )


def collect_condition(target):
    return matches(
        token_id(),
        equal("address", target.fund.address),
        equal("uint128", MAXIMUM),
        equal("uint128", MAXIMUM),
        parameter=3,
    )


def masked_call(signature, types, values, variable_bytes):
    # Roles arrays require one element type tree. Dynamic bitmasks preserve
    # heterogeneous periphery selectors without pretending their tuples match.
    payload = abi.calldata(signature, types, values)
    masks = bytearray(b"\xff" * len(payload))
    for start, length in variable_bytes:
        masks[start : start + length] = bytes(length)
    terms = []
    for start in range(0, len(payload), 15):
        mask = bytes(masks[start : start + 15]).ljust(15, b"\x00")
        value = payload[start : start + 15].ljust(15, b"\x00")
        expected = bytes(a & b for a, b in zip(mask, value, strict=True))
        terms.append(
            Condition(
                parameter=2,
                operator=21,
                value="0x" + (start.to_bytes(2, "big") + mask + expected).hex(),
            )
        )
    # Periphery ABI decoding validates calldata lengths; Roles inspects padded bytes.
    return Condition(parameter=0, operator=1, children=tuple(terms))


def exit_grants(target):
    decrease = masked_call(
        f"decreaseLiquidity({abi.DECREASE_PARAMS})",
        (abi.DECREASE_PARAMS,),
        ((0, 0, 0, 0, 0),),
        ((4, 32), *((4 + index * 32 + 16, 16) for index in range(1, 5))),
    )
    collect = masked_call(
        f"collect({abi.COLLECT_PARAMS})",
        (abi.COLLECT_PARAMS,),
        ((0, target.fund.address, MAXIMUM, MAXIMUM),),
        ((4, 32),),
    )
    burn = masked_call("burn(uint256)", ("uint256",), (0,), ((4, 32),))
    return [
        grant(
            target,
            f"collect({abi.COLLECT_PARAMS})",
            matches(collect_condition(target)),
            BROAD_SCOPE + " Collection pays the Safe.",
        ),
        grant(
            target,
            "multicall(bytes[])",
            matches(
                either(
                    matches(decrease, collect, parameter=4),
                    matches(decrease, collect, burn, parameter=4),
                )
            ),
            BROAD_SCOPE + " Exact decrease/collect or decrease/collect/burn templates; "
            "collection pays the Safe. Decrease amounts and deadline fit uint128. "
            "NFT correlation and nonzero liquidity are gateway checks.",
        ),
    ]


def derive(profile, capability):
    target = capability.target
    approvals = [
        FunctionGrant(
            target=asset.reference,
            signature="approve(address,uint256)",
            condition=matches(equal("address", target.contracts.position_manager), any_amount()),
            explanation=(
                "Allowance of any amount, including zero cleanup, to the configured position "
                "manager only.",
            ),
        )
        for asset in target.assets
    ]
    return [*approvals, mint_grant(target), increase_grant(target), *exit_grants(target)]
