"""Complete Avalanche GM valuation is reconstructed from retained raw accounting inputs."""

import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.economic_valuation import market_token_price, pool_value
from almanak.connectors.gmx_v2.economic_valuation_types import ValuationState


@pytest.mark.parametrize("index", range(4))
def test_complete_avalanche_valuation_matches_pinned_reader(index):
    path = Path(
        "docs/internal/reports/evidence/2026-09-09-gmx-avalanche-valuation-proof/avalanche.json"
    )
    case = json.loads(path.read_text())["cases"][index]
    state = ValuationState.model_validate_json(json.dumps(case["state"]))
    result = pool_value(state, maximize=case["maximize"])
    assert result.model_dump(mode="json") == case["calculation"]
    assert list(result.reader_fields()) == case["contract_result"][1]
    assert market_token_price(state, result) == case["contract_result"][0]
