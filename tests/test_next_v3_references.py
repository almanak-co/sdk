"""V3 candidate declaration boundaries and promoted production entries."""

from datetime import datetime

import pytest

from almanak.caip import ChainId
from almanak.connectors.support import ConnectorChainSupport, RunMode
from almanak.connectors.uniswap_v3.support import candidate_support, declaration


def test_candidate_entries_are_validated_declarations():
    from almanak.connectors.uniswap_v3.support import PROMOTED_CHAINS, PROMOTED_MODES

    promoted = set(PROMOTED_CHAINS)
    for entry in candidate_support():
        assert ConnectorChainSupport.model_validate(entry.model_dump()) == entry
        expected = (
            PROMOTED_MODES
            if str(entry.chain) in promoted
            else (
                RunMode.FORK,
                RunMode.BACKTEST,
            )
        )
        assert entry.modes == expected and entry.historical
    assert {entry.chain for entry in candidate_support() if RunMode.PRODUCTION in entry.modes} == {
        ChainId("eip155:1"),
        ChainId("eip155:8453"),
        ChainId("eip155:42161"),
        ChainId("eip155:10"),
        ChainId("eip155:137"),
        ChainId("eip155:43114"),
        ChainId("eip155:56"),
        ChainId("eip155:4663"),
    }
    with pytest.raises(ValueError, match="explicit timezone"):
        declaration("eip155:56", datetime(2026, 9, 8), ())
