"""A test run's coordinator logs each committed transaction; production never does."""

import gzip
import json
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak import runtime as runtime_module
from almanak import transaction_facts
from almanak.connectors.composed.connector import effects as composed_effects
from almanak.connectors.compound_v3.state import Outcome as CompoundOutcome
from almanak.connectors.uniswap_v3.state import Outcome as UniswapV3Outcome
from almanak.connectors.uniswap_v4.connector import effects as uniswap_v4_effects
from almanak.connectors.uniswap_v4.state import Outcome as UniswapV4Outcome

# Real Optimism fork outcomes from `almanak test run` checkpoints: a Compound V3 loop
# composed with a Uniswap V3 trade allocation, and a Uniswap V4 position.
FIXTURE = Path(__file__).parent / "fixtures" / "transaction-events-optimism-outcomes.json.gz"
NOW = datetime(2026, 10, 27, 14, 11, 30, 123456, tzinfo=UTC)
WETH = "eip155:10/erc20:0x4200000000000000000000000000000000000006"
WSTETH = "eip155:10/erc20:0x1f32b1c2345538c0c6f582fcb022739c4a194ebb"
USDC = "eip155:10/erc20:0x0b2c639c533813f4aa9d7837caf62653d097ff85"
GAS = "eip155:10/slip44:60"
TYPES = {
    "compound_approve": CompoundOutcome,
    "compound_supply": CompoundOutcome,
    "compound_borrow": CompoundOutcome,
    "uniswap_v3_swap": UniswapV3Outcome,
    "uniswap_v4_swap": UniswapV4Outcome,
}


@cache
def recorded(name):
    document = json.loads(gzip.decompress(FIXTURE.read_bytes()))
    return TYPES[name].model_validate_json(json.dumps(document[name]))


def facts(name, effects=composed_effects):
    return transaction_facts.facts(recorded(name), effects, NOW)


def test_a_lending_supply_states_the_collateral_it_spent():
    outcome = recorded("compound_supply")
    assert facts("compound_supply") == {
        "event": "transaction_executed",
        "time": "2026-10-27T14:11:30.123456Z",
        "allocation": "collateral",
        "operation": "supply",
        "success": True,
        "tx_hash": outcome.tx_hash,
        "gas_paid": {"asset": GAS, "amount": "5203307339"},
        "changes": [{"asset": WSTETH, "amount": "-801831918680708070"}],
        "revert_reason": None,
    }


def test_a_borrow_states_the_debt_asset_it_received():
    row = facts("compound_borrow")
    assert (row["allocation"], row["operation"]) == ("loan", "borrow")
    assert row["changes"] == [{"asset": WETH, "amount": "250036680000000000"}]
    assert row["gas_paid"] == {"asset": GAS, "amount": "4830234144"}


def test_swaps_state_both_legs_exactly_as_the_wallet_moved():
    v3 = recorded("uniswap_v3_swap")
    row = facts("uniswap_v3_swap")
    assert (row["allocation"], row["operation"], row["tx_hash"]) == ("trade", "swap", v3.tx_hash)
    assert row["changes"] == [
        {"asset": WSTETH, "amount": "801831918680708070"},
        {"asset": WETH, "amount": "-1000000000000000000"},
    ]
    for side, delta in ((v3.after.wallet0, v3.delta0), (v3.after.wallet1, v3.delta1)):
        before = next(w for w in (v3.before.wallet0, v3.before.wallet1) if w.asset == side.asset)
        assert side.amount - before.amount == delta
    row = facts("uniswap_v4_swap", uniswap_v4_effects)
    assert (row["allocation"], row["operation"]) == ("position", "swap")
    assert row["changes"] == [
        {"asset": WETH, "amount": "208431257408809744"},
        {"asset": USDC, "amount": "-487097345"},
    ]


def test_an_approval_moves_no_token():
    row = facts("compound_approve")
    assert row["operation"] == "approve" and row["changes"] == [] and row["success"] is True


@pytest.mark.parametrize(
    ("kind", "amount", "stated"),
    [
        ("approve", 0, "revoke"),
        ("approve", 5, "approve"),
        ("gmx_approval", SimpleNamespace(amount=0), "revoke"),
        ("gmx_exchange_approval", SimpleNamespace(amount=7), "gmx_exchange_approval"),
    ],
)
def test_an_approval_of_zero_is_stated_as_a_revoke(kind, amount, stated):
    outcome = SimpleNamespace(
        operation=SimpleNamespace(kind=kind, amount=amount, target=None),
        success=True,
        tx_hash="0x01",
        gas_paid=None,
        revert_reason=None,
    )
    assert transaction_facts.summary(outcome, None)["operation"] == stated


def test_a_reverted_transaction_states_its_reason_and_no_changes():
    reverted = recorded("compound_borrow").model_copy(
        update={"success": False, "revert_reason": "E" * 400}
    )
    row = transaction_facts.facts(reverted, composed_effects, NOW)
    assert row["success"] is False and row["changes"] == []
    assert row["revert_reason"] == "E" * transaction_facts.REVERT_CHARACTERS


def test_unreadable_effects_are_stated_as_none_rather_than_guessed():
    def unverified(outcome):
        raise ValueError("connector has no verified execution cash effects")

    assert facts("compound_supply", unverified)["changes"] == []
    assert facts("compound_supply", None)["changes"] == []


class Journal:
    def __init__(self, order, failure=None):
        self.order = order
        self.failure = failure

    def update(self, change):
        if self.failure is not None:
            raise self.failure
        self.order.append("committed")


def resolving(order, *, opted_in, failure=None, connector=None):
    """A coordinator reduced to reconciling one receipt into its committed outcome."""
    outcome = recorded("compound_supply")
    runtime = object.__new__(runtime_module.Runtime)
    runtime.gateway = SimpleNamespace(
        receipt=lambda pending: outcome.receipt, reconcile=lambda pending, receipt: outcome
    )
    runtime.journal = Journal(order, failure)
    runtime.connector = connector or SimpleNamespace(execution_effects=composed_effects)
    runtime.clock = lambda: NOW
    runtime.fault = lambda stage, pending: order.append(stage)
    runtime.decision_facts = opted_in
    runtime.replanning = lambda: SimpleNamespace(reset=lambda route: None)
    runtime.route = lambda: None
    return runtime


def logged(capsys):
    lines = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    return [line for line in lines if line.get("event") == "transaction_executed"]


def test_a_test_run_logs_the_transaction_only_after_its_outcome_is_committed(monkeypatch, capsys):
    order = []
    log = transaction_facts.log
    monkeypatch.setattr(
        transaction_facts, "log", lambda *args: order.append("logged") or log(*args)
    )
    assert resolving(order, opted_in=True).resolve_pending(object()) is False
    assert order == ["receipt", "committed", "logged", "commit"]
    [row] = logged(capsys)
    assert row["operation"] == "supply" and row["time"] == "2026-10-27T14:11:30.123456Z"
    assert row["changes"] == [{"asset": WSTETH, "amount": "-801831918680708070"}]


def test_an_uncommitted_outcome_is_never_logged(capsys):
    order = []
    runtime = resolving(order, opted_in=True, failure=RuntimeError("pending changed"))
    with pytest.raises(RuntimeError, match="pending changed"):
        runtime.resolve_pending(object())
    assert logged(capsys) == []


def test_a_production_coordinator_never_logs_transactions(monkeypatch, capsys):
    order = []
    monkeypatch.setattr(transaction_facts, "log", lambda *args: order.append("logged"))
    assert resolving(order, opted_in=False).resolve_pending(object()) is False
    assert order == ["receipt", "committed", "commit"]
    assert logged(capsys) == []


def test_a_failing_log_never_stops_reconciliation(monkeypatch, capsys):
    class Broken:
        @property
        def execution_effects(self):
            raise RuntimeError("connector lookup failed")

    order = []
    assert resolving(order, opted_in=True, connector=Broken()).resolve_pending(object()) is False
    assert order == ["receipt", "committed", "commit"] and logged(capsys) == []

    class Closed:
        def write(self, text):
            raise ValueError("I/O operation on closed file")

        def flush(self):
            raise OSError("broken pipe")

    monkeypatch.setattr(transaction_facts.sys, "stderr", Closed())
    order = []
    assert resolving(order, opted_in=True).resolve_pending(object()) is False
    assert order == ["receipt", "committed", "commit"]
