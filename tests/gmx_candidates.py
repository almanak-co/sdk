"""Test-local GMX candidate declarations for exercising unfinished economics.

These helpers advertise economic historical support only inside one test
process, for the swap, GM liquidity and perpetual model identities. They do not
change the public connector declaration, do not establish SDK support and do
not count as QA lifecycle coverage.
"""

from dataclasses import replace

import pytest

from almanak.connectors import composition
from almanak.connectors.gmx_v2.connector import connector
from almanak.connectors.gmx_v2.economic_data import LIQUIDITY_MODEL, MODEL, PERPETUAL_MODEL
from almanak.connectors.gmx_v2.economic_model import EconomicModel
from almanak.connectors.support import EconomicAction, HistoricalSupport, RunMode

CANDIDATES = {
    "swap": (MODEL, (EconomicAction.SWAP, EconomicAction.REVOKE)),
    "liquidity": (
        LIQUIDITY_MODEL,
        (
            EconomicAction.OPEN_LIQUIDITY,
            EconomicAction.INCREASE_LIQUIDITY,
            EconomicAction.DECREASE_LIQUIDITY,
            EconomicAction.CLOSE_LIQUIDITY,
            EconomicAction.REVOKE,
        ),
    ),
    "perpetual": (
        PERPETUAL_MODEL,
        (
            EconomicAction.INCREASE_POSITION,
            EconomicAction.DECREASE_POSITION,
            EconomicAction.CLOSE_POSITION,
            EconomicAction.REVOKE_POSITION,
        ),
    ),
}


def candidate_connector(*families):
    """The public GMX connector plus test-local economic support for ``families``."""
    candidate = connector()
    historical = tuple(
        HistoricalSupport(
            model=CANDIDATES[family][0], mode="economic", actions=CANDIDATES[family][1]
        )
        for family in families
    )
    support = tuple(
        entry.model_copy(
            update={"modes": (*entry.modes, RunMode.BACKTEST), "historical": historical}
        )
        for entry in candidate.chain_support
    )
    return replace(candidate, chain_support=support)


def declare_candidate(monkeypatch, *families):
    """Route connector lookup to the candidate for this test; return the GMX driver."""
    candidate = candidate_connector(*families)
    monkeypatch.setattr(composition, "connector_for", lambda config: candidate)
    return EconomicModel


@pytest.fixture
def candidate_model(monkeypatch):
    """Swap and revocation only, exactly the earlier test-local declaration."""
    return declare_candidate(monkeypatch, "swap")


@pytest.fixture
def candidate_liquidity_model(monkeypatch):
    return declare_candidate(monkeypatch, "liquidity")


@pytest.fixture
def candidate_perpetual_model(monkeypatch):
    return declare_candidate(monkeypatch, "perpetual")
