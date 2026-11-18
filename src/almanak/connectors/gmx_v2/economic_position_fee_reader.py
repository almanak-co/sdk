"""Read exact trader discounts and fee factors from the verified handler's referral binding."""

from almanak.connectors.gmx_v2.economic_data import PositionFeePolicies
from almanak.connectors.gmx_v2.economic_position_fees import FeePolicy
from almanak.connectors.gmx_v2.economic_precision import FLOAT
from almanak.connectors.gmx_v2.economic_spot_reader import market_uint
from almanak.connectors.gmx_v2.oracle_reader import stored


def global_uint(reader, market, block, name):
    return stored(reader, block, market.contracts.data_store, "Uint", name)


def trader_pro_discount(reader, target, block):
    store = target.market.contracts.data_store
    tier = stored(
        reader, block, store, "Uint", "PRO_TRADER_TIER", ("address",), (target.fund.address,)
    )
    return (
        stored(reader, block, store, "Uint", "PRO_DISCOUNT_FACTOR", ("uint256",), (tier,))
        if tier
        else 0
    )


def referral_storage(reader, market, block):
    return reader.call(
        market.contracts.order_handler, "referralStorage()", block, outputs=("address",)
    )[0]


def referral_discount(reader, target, block):
    market = target.market
    referral = referral_storage(reader, market, block)
    if int(referral, 16) == 0:
        return False, 0, 0, 0
    code = reader.call(
        referral,
        "traderReferralCodes(address)",
        block,
        inputs=("address",),
        args=(target.fund.address,),
        outputs=("bytes32",),
    )[0]
    if code == bytes(32):
        return False, 0, 0, 0
    return registered_referral(reader, market, block, referral, code)


def registered_referral(reader, market, block, referral, code):
    affiliate = reader.call(
        referral,
        "codeOwners(bytes32)",
        block,
        inputs=("bytes32",),
        args=(code,),
        outputs=("address",),
    )[0]
    tier = reader.call(
        referral,
        "referrerTiers(address)",
        block,
        inputs=("address",),
        args=(affiliate,),
        outputs=("uint256",),
    )[0]
    rebate, share = reader.call(
        referral,
        "tiers(uint256)",
        block,
        inputs=("uint256",),
        args=(tier,),
        outputs=("uint256", "uint256"),
    )
    custom = reader.call(
        referral,
        "referrerDiscountShares(address)",
        block,
        inputs=("address",),
        args=(affiliate,),
        outputs=("uint256",),
    )[0]
    minimum = stored(
        reader,
        block,
        market.contracts.data_store,
        "Uint",
        "MIN_AFFILIATE_REWARD_FACTOR",
        ("uint256",),
        (tier,),
    )
    discount = (rebate * (custom or share) // 10000) * FLOAT // 10000
    return True, rebate * FLOAT // 10000 - discount, discount, minimum


def position_fee_policy(reader, target, block, *, balance_improved):
    market = target.market
    if market.chain != block.chain or target.fund.chain != block.chain:
        raise ValueError("GMX fee policy requires the target's canonical chain")
    enabled, affiliate, discount, minimum = referral_discount(reader, target, block)
    return FeePolicy(
        position_factor=market_uint(
            reader, market, block, "POSITION_FEE_FACTOR", positive=balance_improved
        ),
        position_receiver_factor=global_uint(reader, market, block, "POSITION_FEE_RECEIVER_FACTOR"),
        borrowing_receiver_factor=global_uint(
            reader, market, block, "BORROWING_FEE_RECEIVER_FACTOR"
        ),
        liquidation_factor=market_uint(reader, market, block, "LIQUIDATION_FEE_FACTOR"),
        liquidation_receiver_factor=global_uint(
            reader, market, block, "LIQUIDATION_FEE_RECEIVER_FACTOR"
        ),
        # The native planner fixes uiFeeReceiver to the zero address.
        ui_factor=0,
        referral_enabled=enabled,
        affiliate_factor=affiliate,
        referral_discount_factor=discount,
        minimum_affiliate_factor=minimum,
        pro_discount_factor=trader_pro_discount(reader, target, block),
    )


def position_fee_policies(reader, target, block):
    """Read both POSITION_FEE_FACTOR sides once; settlement selects after pricing impact."""
    improved = position_fee_policy(reader, target, block, balance_improved=True)
    worsened = FeePolicy.model_validate(
        improved.model_dump()
        | {
            "position_factor": market_uint(
                reader, target.market, block, "POSITION_FEE_FACTOR", positive=False
            )
        }
    )
    return PositionFeePolicies(improved=improved, worsened=worsened)
