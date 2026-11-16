"""Synthetic adverse paths retain collateral fees and deficits in modeled ownership."""

from dataclasses import replace

import pytest

from almanak.connectors.aave_v3.economic_liquidation import liquidate
from almanak.connectors.aave_v3.economic_math import risk
from almanak.connectors.aave_v3.economic_state import Position
from tests.test_aave_v3_config import parameters as parameters
from tests.test_aave_v3_economic_actions import borrowed


def adverse(parameters):
    positions, markets = borrowed(parameters)
    reserve = markets[0].reserve
    quote = reserve.oracle
    assert quote is not None
    changed = reserve.model_copy(update={"oracle": quote.model_copy(update={"price": 900 * 10**8})})
    return positions, (
        replace(markets[0], sample=markets[0].sample.model_copy(update={"reserve": changed})),
        markets[1],
    )


def test_adverse_price_liquidates_owned_collateral_without_spending_the_funds_proceeds(parameters):
    positions, markets = adverse(parameters)
    assert risk(positions, markets).risk.health_factor_wad < 10**18
    result = liquidate(parameters, positions, markets, positions, 8)
    assert len(result.liquidations) == 1
    assert result.positions[0].supply_scaled == 0
    assert result.positions[1].debt_scaled == 0
    assert result.positions[1].deficit > 0
    assert sum(d.amount for d in result.deficits) == result.positions[1].deficit
    assert (
        result.positions[1].liquidated_debt + result.positions[1].deficit
        == positions[1].debt_scaled
    )
    assert result.positions[1].repaid == 0
    assert result.liquidations[0].collateral_removed.asset == parameters.target("collateral").asset
    assert result.liquidations[0].debt_restored.asset == parameters.target("loan").asset


@pytest.mark.parametrize("fault", ["grace", "liquidity", "paused"])
def test_unsettleable_liquidation_keeps_the_original_obligations_visible(parameters, fault):
    positions, markets = adverse(parameters)
    market = markets[0]
    if fault == "grace":
        sample = market.sample.model_copy(update={"liquidation_grace_until": market.time})
    else:
        reserve = market.reserve
        change = (
            {"available_underlying": 0}
            if fault == "liquidity"
            else {
                "configuration": reserve.configuration.model_copy(
                    update={"raw": reserve.configuration.raw | (1 << 60)}
                ),
            }
        )
        sample = market.sample.model_copy(update={"reserve": reserve.model_copy(update=change)})
    markets = (replace(market, sample=sample), markets[1])
    pool_positions = positions
    if fault == "liquidity":
        target = positions[0].target
        other = target.model_copy(
            update={
                "account": "other",
                "fund": target.fund.model_copy(update={"address": "0x" + "12" * 20}),
            }
        )
        # Another hypothetical fund has consumed this fund's supplied liquidity.
        pool_positions = (*positions, Position(target=other, borrowed=10**18, debt_scaled=10**18))
    result = liquidate(parameters, positions, markets, pool_positions, 8)
    assert result.positions == positions
    assert result.liquidations == result.deficits == ()
    assert result.blocked
