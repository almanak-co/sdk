"""Retained reports and preflight distinguish missing inputs from measured balances."""

import pytest

from almanak.connectors.aave_v3.execution import AaveExecution
from almanak.connectors.aave_v3.reporting import report
from tests.aave_v3_helpers import recorded_v3 as recorded_v3


def test_closed_report_reconciles_borrowing_supply_and_submitter_costs(recorded_v3):
    result = report(recorded_v3)
    collateral, loan = result["positions"]
    assert result["transaction_accounting_complete"]
    assert result["undelivered_outcomes"] == 0
    for position, quantity, incoming, outgoing in (
        (collateral, "realized_supply_delta", "withdraw", "supply"),
        (loan, "realized_borrowing_cost", "repay", "borrow"),
    ):
        effects = position["effects"]
        assert int(position[quantity]["amount"]) == int(effects[incoming]["amount"]) - int(
            effects[outgoing]["amount"]
        )
        assert position["measurement_status"] == "retained"
        assert not position["missing_data"]
    (payer,) = result["gas_payers"]
    assert int(payer["gas_paid"]["amount"]) == sum(o.gas_paid.amount for o in recorded_v3.outcomes)
    assert payer["submitter"] != result["funds"][0]["fund"]
    assert len(set(payer["nonces"])) == len(recorded_v3.outcomes)


def test_report_preserves_missing_and_invalidated_evidence(recorded_v3):
    state = recorded_v3.model_copy(
        update={
            "initialized": False,
            "baseline": (),
            "outcomes": (),
            "closure": (),
            "delivered": 0,
            "integrity_error": "receipt invalidated",
        }
    )
    result = report(state)
    assert result["evidence_status"] == "invalidated"
    assert result["gas_payers"][0]["gas_paid"]["amount"] is None
    for position in result["positions"]:
        assert position["measurement_status"] == "unobserved"
        assert position["missing_data"] == ["no retained observation"]
        assert position["realized_supply_delta"] is None
        assert position["realized_borrowing_cost"] is None


def test_preflight_requires_the_entry_supply_and_all_observations(recorded_v3):
    state = recorded_v3
    tokens, pristine = AaveExecution.preflight(state.config, "fund", state.baseline)
    assert pristine and all(t["funded"] for t in tokens)
    assert [int(t["required_token_base_units"]) for t in tokens] == [
        a.capital for a in state.config.positions
    ]
    tokens, pristine = AaveExecution.preflight(state.config, "fund", ())
    assert not pristine
    assert all(t["observed_token_base_units"] is None and not t["funded"] for t in tokens)


@pytest.mark.parametrize(
    "fault", ["allowance", "supply", "debt", "bitmap", "risk", "emode", "funding"]
)
def test_preflight_refuses_foreign_obligations_or_missing_risk(recorded_v3, fault):
    state = recorded_v3
    first, second = state.baseline
    if fault in ("supply", "debt", "bitmap"):
        field = {
            "supply": "supplied_scaled",
            "debt": "debt_scaled",
            "bitmap": "user_configuration",
        }[fault]
        first = first.model_copy(update={"position": first.position.model_copy(update={field: 1})})
    elif fault == "emode":
        first = first.model_copy(
            update={"pool_risk": first.pool_risk.model_copy(update={"emode_category": 1})}
        )
    else:
        field, value = {
            "allowance": ("allowance", 1),
            "risk": ("pool_risk", None),
            "funding": ("wallet_balance", 0),
        }[fault]
        first = first.model_copy(update={field: value})
    tokens, pristine = AaveExecution.preflight(state.config, "fund", (first, second))
    if fault == "funding":
        assert not tokens[0]["funded"]
    else:
        assert not pristine
