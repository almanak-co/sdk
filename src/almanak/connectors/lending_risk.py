"""Collateral risk parameters of a lending reserve, for `almanak ax lending-reserve`.

Each connector keeps the same three numbers per collateral asset, in its own units:

- Aave V3 and SparkLend decode them from the reserve's configuration word the
  reserve read already holds: `ltv_bps`, `liquidation_threshold_bps`, and the
  liquidation bonus stored as 10000 + bonus. They are the reserve's ordinary-mode
  values; eMode and isolation are not applied.
- Aave V4 reads the reserve's current dynamic config on the Spoke
  (`getDynamicReserveConfig`) and the Spoke's `getLiquidationConfig`. One
  collateral factor is both the borrow limit and the liquidation threshold, and
  the bonus is the maximum, paid at or below `health_factor_for_max_bonus`.
  The reserve's `collateral_risk_bps` is the risk premium the collateral adds to
  the debt rate, not a collateral factor.
- Compound V3 scales each collateral's 1e18 factors to basis points:
  `borrowCollateralFactor` is the LTV, `liquidateCollateralFactor` the threshold
  and 1 - `liquidationFactor` the share of collateral value an absorbed account
  loses.
- Morpho Blue has one LLTV per market, both the borrow limit and the threshold;
  the liquidation incentive factor is min(1.15, 1 / (1 - 0.3 x (1 - LLTV))) in
  Morpho's rounded-down WAD arithmetic.

For one collateral and a debt of f times the collateral value, the health factor
is LT / f, and the largest f a health floor H admits is min(LTV, LT / H). Ratios
are exact and rounded down to 4 places for presentation, never in the agent's favor.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import ROUND_DOWN, Decimal, localcontext

from almanak.connectors.aave_v4.reader import read_dynamic_reserve_config, read_liquidation_config
from almanak.gateway.ethereum import DataError

BPS = 10_000
WAD = 10**18
WAD_PER_BPS = 10**14
PLACES = Decimal("1e-4")
MORPHO_MAX_INCENTIVE = 115 * 10**16
MORPHO_CURSOR = 3 * 10**17

HEALTH_FACTOR = (
    "sum(collateral_value x liquidation_threshold) / debt_value at the protocol oracle;"
    " liquidatable below 1"
)
BORROW_LIMIT = "debt_value <= sum(collateral_value x ltv)"
PROJECTION = (
    "one collateral, debt = f x collateral_value: health_factor(f) = liquidation_threshold / f;"
    " max f for health floor H = min(ltv, liquidation_threshold / H)"
)


@dataclass(frozen=True)
class RiskQuery:
    """Optional projection inputs: borrow fractions in (0, 1] and a health floor above 1."""

    borrow_fractions: tuple[Decimal, ...] = ()
    health_floor: Decimal | None = None


@dataclass(frozen=True)
class Collateral:
    asset: str
    ltv: Decimal
    threshold: Decimal
    bonus_bps: int
    extra: dict = field(default_factory=dict)


def risk_entry(build: Callable[[], tuple[list[Collateral], dict]], query: RiskQuery | None) -> dict:
    """The risk block, or its refusal beside the rest of the lookup."""
    try:
        collateral, notes = build()
    except DataError as error:
        return {"risk": None, "risk_error": f"{error.kind}: {error}"}
    return {
        "risk": {
            "collateral": [row(entry, query or RiskQuery()) for entry in collateral],
            "health_factor": HEALTH_FACTOR,
            "borrow_limit": BORROW_LIMIT,
            "projection": PROJECTION,
            **notes,
        }
    }


def row(entry: Collateral, query: RiskQuery) -> dict:
    maximum = {"by_ltv": shown(entry.ltv)}
    if query.health_floor is not None:
        maximum["at_floor"] = shown(min(entry.ltv, down(entry.threshold, query.health_floor)))
    projected = [
        {
            "borrow_fraction": str(fraction),
            "health_factor": shown(down(entry.threshold, fraction)),
            "within_borrow_limit": fraction <= entry.ltv,
        }
        for fraction in query.borrow_fractions
    ]
    return {
        "asset": entry.asset,
        "ltv_bps": bps(entry.ltv),
        "liquidation_threshold_bps": bps(entry.threshold),
        "liquidation_bonus_bps": entry.bonus_bps,
        **entry.extra,
        "max_borrow_fraction": maximum,
        **({"projected": projected} if projected else {}),
    }


def down(numerator: Decimal, denominator: Decimal) -> Decimal:
    """The quotient rounded toward zero, so a ratio just under a bound never shows as it."""
    with localcontext(rounding=ROUND_DOWN):
        return numerator / denominator


def shown(value: Decimal) -> str:
    return format(value.quantize(PLACES, rounding=ROUND_DOWN), "f")


def bps(ratio: Decimal) -> int | str:
    scaled = ratio * BPS
    return int(scaled) if scaled == scaled.to_integral_value() else format(scaled, "f")


def ratio(value: int, scale: int) -> Decimal:
    return Decimal(value) / Decimal(scale)


def pool_bonus_bps(stored: int) -> int:
    # Aave V3 and SparkLend store 10000 + bonus; a reserve that is no collateral stores 0.
    return max(stored - BPS, 0)


def comet_factors(borrow: int, liquidate: int, liquidation: int) -> dict:
    return {
        "ltv_bps": borrow // WAD_PER_BPS,
        "liquidation_threshold_bps": liquidate // WAD_PER_BPS,
        "liquidation_bonus_bps": (WAD - liquidation) // WAD_PER_BPS,
    }


def morpho_incentive(lltv: int) -> int:
    """Morpho Blue's liquidation incentive factor in WAD, rounded down as the core does."""
    return min(MORPHO_MAX_INCENTIVE, WAD * WAD // (WAD - MORPHO_CURSOR * (WAD - lltv) // WAD))


def pool_risk(reader, resolved, observation, block):
    config = observation.configuration
    collateral = Collateral(
        asset=str(observation.asset.identifier),
        ltv=ratio(config.ltv_bps, BPS),
        threshold=ratio(config.liquidation_threshold_bps, BPS),
        bonus_bps=pool_bonus_bps(config.liquidation_bonus_bps),
    )
    return [collateral], {
        "liquidation_bonus": "bonus over the repaid debt's value paid in this collateral",
        "scope": "this reserve as collateral, in ordinary mode; read the collateral reserve",
    }


def spoke_risk(reader, resolved, observation, block):
    reserve = resolved.reserve
    key = observation.dynamic_config_key
    config = read_dynamic_reserve_config(reader, reserve.spoke, reserve.reserve_id, key, block)
    liquidation = read_liquidation_config(reader, reserve.spoke, block)
    factor = ratio(config.collateral_factor_bps, BPS)
    collateral = Collateral(
        asset=str(observation.asset.identifier),
        ltv=factor,
        threshold=factor,
        bonus_bps=pool_bonus_bps(config.max_liquidation_bonus_bps),
        extra={"collateral_factor_bps": config.collateral_factor_bps},
    )
    return [collateral], {
        "liquidation_bonus": "maximum bonus, paid at or below health_factor_for_max_bonus",
        "scope": "this reserve as collateral, at its current dynamic config; read the collateral"
        " reserve",
        "dynamic_config_key": key,
        "liquidation": {
            "target_health_factor": shown(ratio(liquidation.target_health_factor, WAD)),
            "health_factor_for_max_bonus": shown(
                ratio(liquidation.health_factor_for_max_bonus, WAD)
            ),
            "bonus_factor_bps": liquidation.bonus_factor_bps,
            "liquidation_fee_bps": config.liquidation_fee_bps,
        },
        "collateral_risk_bps": observation.collateral_risk_bps,
        "collateral_risk_note": "collateral_risk_bps is the risk premium this collateral adds"
        " to the debt rate, not a collateral factor",
    }


def comet_risk(reader, resolved, observation, block):
    collateral = []
    for entry in observation.collaterals:
        factors = comet_factors(
            entry.borrow_factor, entry.liquidate_factor, entry.liquidation_factor
        )
        collateral.append(
            Collateral(
                asset=str(entry.asset.identifier),
                ltv=ratio(entry.borrow_factor, WAD),
                threshold=ratio(entry.liquidate_factor, WAD),
                bonus_bps=factors["liquidation_bonus_bps"],
            )
        )
    return collateral, {
        "liquidation_bonus": "1 - liquidationFactor: the collateral value an absorbed account"
        " loses",
        "scope": "every collateral of the Comet",
    }


def morpho_risk(reader, resolved, observation, block):
    market = resolved.market
    lltv = ratio(market.lltv, WAD)
    incentive = morpho_incentive(market.lltv)
    collateral = Collateral(
        asset=str(market.collateral_asset),
        ltv=lltv,
        threshold=lltv,
        bonus_bps=(incentive - WAD) // WAD_PER_BPS,
        extra={
            "lltv": format(lltv.normalize(), "f"),
            "liquidation_incentive_factor": format(ratio(incentive, WAD).normalize(), "f"),
        },
    )
    return [collateral], {
        "liquidation_bonus": "liquidation incentive factor - 1, rounded down",
        "scope": "the market's one collateral",
    }
