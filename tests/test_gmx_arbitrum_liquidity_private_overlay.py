"""Private GM qualification declares one model without widening public scope."""

import pytest

from almanak.caip import ChainId
from almanak.connectors.gmx_v2.connector import connector
from almanak.connectors.gmx_v2.economic_data import LIQUIDITY_MODEL, MODEL, PERPETUAL_MODEL
from almanak.connectors.support import EconomicAction, RunMode
from tests.qa.catalog import catalog


def test_private_overlay_admits_only_arbitrum_gm_history():
    candidate = connector()
    arbitrum = candidate.support_for(ChainId("eip155:42161"))
    avalanche = candidate.support_for(ChainId("eip155:43114"))

    assert arbitrum.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    assert len(arbitrum.historical) == 1
    history = arbitrum.historical[0]
    assert history.model == LIQUIDITY_MODEL
    assert history.backtest_start_at.isoformat() == "2026-09-08T00:36:52+00:00"
    assert tuple(history.actions) == (
        EconomicAction.OPEN_LIQUIDITY,
        EconomicAction.INCREASE_LIQUIDITY,
        EconomicAction.DECREASE_LIQUIDITY,
        EconomicAction.CLOSE_LIQUIDITY,
        EconomicAction.REVOKE,
    )
    for refused in (MODEL, PERPETUAL_MODEL):
        with pytest.raises(ValueError, match="unsupported"):
            arbitrum.require(RunMode.BACKTEST, model=refused)
    assert avalanche.modes == (RunMode.FORK,)
    assert avalanche.historical == ()


def test_private_overlay_adds_only_three_arbitrum_gm_cases():
    cases = catalog()
    selected = [
        case
        for case in cases
        if case.mode == "backtest" and case.reference == "gmx_v2_arbitrum_liquidity"
    ]

    assert tuple(case.id for case in selected) == (
        "backtest/pinned/gmx_v2_arbitrum_liquidity/economic",
        "backtest/fresh/gmx_v2_arbitrum_liquidity/economic",
        "backtest/native/gmx_v2_arbitrum_liquidity/economic",
    )
    assert all(case.model == LIQUIDITY_MODEL for case in selected)
    assert not any(
        case.model in (MODEL, PERPETUAL_MODEL)
        for case in cases
        if case.connector == "gmx_v2" and case.mode == "backtest"
    )
