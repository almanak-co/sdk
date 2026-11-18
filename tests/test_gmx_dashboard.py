"""Read-only GMX views keep fund inventory, protocol requests and submitter gas distinct."""

from decimal import Decimal

import pytest

from almanak.connectors.gmx_v2.dashboard import project
from almanak.connectors.gmx_v2.observations import initial_owned
from almanak.connectors.gmx_v2.reporting import report
from almanak.dashboard.history import decimal_text
from almanak.dashboard.models import Context
from tests.gmx_evidence import checkpoint


def test_retained_dashboard_and_report_preserve_request_fund_and_submitter_facts():
    state = checkpoint()
    context, snapshot = project(state, captured_at=state.closure[0].block.timestamp)
    assert Context.model_validate_json(context.model_dump_json()) == context
    target = state.config.target("Main")
    assert all(value.scope.fund == target.fund for value in context.values.values())
    payer = state.config.accounts.binding("fund").profile.submitter
    assert payer != target.fund
    submitter = context.datasets["submitters"].rows[0]
    assert submitter["submitter"] == payer.address
    paid = sum(o.gas_paid.amount for o in state.outcomes if o.kind == "gmx_transaction_outcome")
    assert submitter["amount"] == f"{decimal_text(Decimal(paid) / Decimal(10) ** 18)} ETH"
    requests = context.datasets["requests"].rows
    assert [row["status"] for row in requests] == ["executed", "executed"]
    assert all(row["fee"].endswith(" WETH") for row in requests), [r["fee"] for r in requests]
    assert all(
        row["deadline"] == "Not applicable" or row["deadline"].endswith(" UTC") for row in requests
    )
    assert snapshot.latest_observation == state.closure[0].block.timestamp
    retained = report(state)
    assert retained["unresolved_requests"] == 0
    assert retained["transaction_accounting_complete"]
    assert [o["source"] for o in retained["outcomes"]].count("external_keeper") == 2
    assert retained["transaction_families"] == {"pending": None, "completed": []}


def test_unobserved_inventory_is_configured_and_invalidated_evidence_refuses_projection():
    state = checkpoint()
    initial = state.model_copy(
        update={
            "owned": (initial_owned(state.config, "Main"),),
            "outcomes": (),
            "closure": (),
            "baseline": (),
            "requests": (),
            "initialized": False,
            "delivered": 0,
        }
    )
    context, snapshot = project(initial, captured_at=0)
    assert all(
        value.evidence == "configured" and value.block is None for value in context.values.values()
    )
    assert snapshot.latest_observation is None
    assert context.datasets["submitters"].rows[0]["amount"] == "Unobserved"
    with pytest.raises(ValueError, match="invalidated"):
        project(state.model_copy(update={"integrity_error": "canonical receipt invalidated"}))
