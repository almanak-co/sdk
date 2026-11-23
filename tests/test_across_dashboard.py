"""The Across dashboard: each transfer's amounts, its status and its quote source."""

import pytest

from almanak.connectors.across.connector import AcrossProjection
from almanak.dashboard.models import Ref
from almanak.models import AssetAmount
from almanak.transfers import Arrived, Refunded, Transfer
from tests.across_support import AMOUNT
from tests.test_across_records import (  # noqa: F401
    FORK_ARB,
    FORK_BASE,
    checkpoint,
    config,
    mined_deposit,
)


def ended(transfer, **changes):
    return Transfer.model_validate({**transfer.model_dump(), **changes})


def projected(config, outcome, transfer):  # noqa: F811
    state = checkpoint(config, transfers=(transfer,), outcomes=(outcome,), delivered=1)
    context, snapshot = AcrossProjection.dashboard(state, captured_at=1)
    assert context.positions == () and set(context.datasets) == {"transfers"}
    assert context.values == snapshot.values
    (row,) = context.datasets["transfers"].rows
    return row, snapshot.values


def test_an_open_transfer_shows_what_was_sent_and_offered_and_no_received_amount(
    config,  # noqa: F811
    monkeypatch,
):
    outcome = mined_deposit(monkeypatch, config)
    transfer = outcome.transfer
    row, values = projected(config, outcome, transfer)
    assert (row["status"], row["quote_source"]) == ("open", outcome.operation.quote.source)
    assert row["fill_deadline"].endswith("UTC")
    assert (row["account"], row["destination"]) == ("arb", "base")
    sent, offered = values[row["sent"].ref], values[row["offered"].ref]
    assert (sent.amount, sent.asset, sent.scope.fund.chain) == (
        str(AMOUNT),
        transfer.sent.asset,
        FORK_ARB,
    )
    assert (offered.amount, offered.asset, offered.scope.fund.chain) == (
        str(transfer.offered.amount),
        transfer.offered.asset,
        FORK_BASE,
    )
    assert sent.block == outcome.after.block and "Not a received amount" in offered.reason
    # Missing is not zero: nothing was measured on either chain yet.
    for name in ("received", "returned"):
        assert isinstance(row[name], Ref)
        assert values[row[name].ref].amount is None and values[row[name].ref].block is None
    unfilled, _ = projected(config, outcome, ended(transfer, unfilled=True))
    assert unfilled["status"] == "unfilled"


def test_an_ended_transfer_shows_only_the_amount_its_chain_measured(config, monkeypatch):  # noqa: F811
    outcome = mined_deposit(monkeypatch, config)
    transfer = outcome.transfer
    received = AssetAmount(asset=transfer.offered.asset, amount=transfer.offered.amount - 1)
    row, values = projected(config, outcome, ended(transfer, resolution=Arrived(received=received)))
    assert row["status"] == "arrived"
    assert values[row["received"].ref].amount == str(received.amount)
    assert values[row["received"].ref].scope.account == "base"
    assert values[row["returned"].ref].amount is None
    returned = AssetAmount(asset=transfer.sent.asset, amount=AMOUNT)
    row, values = projected(
        config, outcome, ended(transfer, unfilled=True, resolution=Refunded(returned=returned))
    )
    assert row["status"] == "refunded"
    assert values[row["returned"].ref].amount == str(AMOUNT)
    assert values[row["returned"].ref].scope.account == "arb"
    assert values[row["received"].ref].amount is None


def test_a_transfer_without_its_deposit_names_no_quote_source(config, monkeypatch):  # noqa: F811
    transfer = mined_deposit(monkeypatch, config).transfer
    state = checkpoint(config, transfers=(transfer,))
    context, _ = AcrossProjection.dashboard(state, captured_at=1)
    assert context.datasets["transfers"].rows[0]["quote_source"] == "not retained"
    with pytest.raises(ValueError, match="invalidated Across evidence"):
        AcrossProjection.dashboard(state.model_copy(update={"integrity_error": "x"}))


def test_the_shared_projection_and_export_accept_an_across_checkpoint(
    config,  # noqa: F811
    monkeypatch,
    tmp_path,
):
    from almanak.dashboard.export import export_input
    from almanak.dashboard.projection import project

    outcome = mined_deposit(monkeypatch, config)
    state = checkpoint(config, transfers=(outcome.transfer,), outcomes=(outcome,), delivered=1)
    context, snapshot = project(state, captured_at=1)
    assert snapshot.latest_observation == outcome.after.block.timestamp
    assert context.model_validate_json(context.model_dump_json()) == context
    exported = export_input(tmp_path, state, captured_at=1)
    assert exported is not None and (tmp_path / "input.json").exists()
