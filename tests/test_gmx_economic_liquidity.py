"""Integer GM quotes are compared with independently captured deployed Reader results."""

import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.economic_liquidity import (
    LiquidityState,
    quote_deposit,
    quote_withdrawal,
    shares_for_usd,
    signed_proportion,
)

EVIDENCE = Path("docs/internal/reports/evidence/2026-09-09-gmx-prepared")


def evidence(chain):
    return json.loads((EVIDENCE / f"{chain}-liquidity-reader-comparison.json").read_text())


def state(chain, action):
    return LiquidityState.model_validate_json(
        json.dumps(evidence(chain)["states"][action]["state"])
    )


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("index", range(8))
def test_deployed_reader_comparison(chain, index):
    case = evidence(chain)["cases"][index]
    sample = state(chain, case["action"])
    expected = case["calculation"]
    if case["action"] == "DEPOSIT":
        result = quote_deposit(sample, expected["long_amount"], expected["short_amount"])
        assert [result.shares] == case["contract_result"]
    else:
        result = quote_withdrawal(sample, expected["shares"])
        assert [result.long_amount, result.short_amount] == case["contract_result"]
    assert result.model_dump(mode="json") == expected


@pytest.mark.parametrize("amounts", [(0, 0), (-1, 1), (1, -1)])
def test_deposit_requires_positive_inventory(amounts):
    with pytest.raises(ValueError, match="nonnegative"):
        quote_deposit(state("arbitrum", "DEPOSIT"), *amounts)


@pytest.mark.parametrize("shares", [0, -1, 10**40])
def test_withdrawal_requires_existing_supply(shares):
    with pytest.raises(ValueError, match="positive shares"):
        quote_withdrawal(state("arbitrum", "WITHDRAWAL"), shares)


def test_initial_share_conversion_is_protocol_rule():
    sample = state("arbitrum", "DEPOSIT").model_copy(
        update={"share_supply": 0, "pool_value_usd_e30": 2 * 10**30}
    )
    assert shares_for_usd(sample, 3 * 10**30) == 5 * 10**18
    sample = sample.model_copy(update={"pool_value_usd_e30": 0})
    assert shares_for_usd(sample, 3 * 10**30) == 3 * 10**18


def test_signed_impact_proportion_truncates_toward_zero():
    assert signed_proportion(-5, 1, 3) == -1
    assert signed_proportion(5, 1, 3) == 1


def test_zero_value_with_outstanding_shares_refused():
    wire = state("arbitrum", "DEPOSIT").model_dump_json()
    values = json.loads(wire)
    values["pool_value_usd_e30"] = 0
    with pytest.raises(ValueError, match="positive pool valuation"):
        LiquidityState.model_validate_json(json.dumps(values))


@pytest.mark.parametrize(
    "operation,action", [(quote_deposit, "WITHDRAWAL"), (quote_withdrawal, "DEPOSIT")]
)
def test_action_specific_valuation_cannot_be_reused(operation, action):
    arguments = (1, 1) if operation == quote_deposit else (1,)
    with pytest.raises(ValueError, match="specific valuation"):
        operation(state("arbitrum", action), *arguments)
