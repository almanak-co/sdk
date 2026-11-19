"""`almanak ax supply` and `ax withdraw`: the one-process operator action.

Component evidence against captured Aave V4 receipts, without signing or chain access.
The fork proof of the whole command is tests/fork/test_ax_actions.py.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak import ax_actions, operator_runtime
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors.aave_v4.lifecycle import adopt_ownership, commit_ownership
from almanak.gateway.credentials import create_wallet
from almanak.gateway.providers import AlchemyProvider
from almanak.operator_deployment import adopts
from tests.test_operator_runtime import Desk

COMMON = [
    "--connector",
    "aave_v4",
    "--chain",
    "eip155:1",
    "--spoke",
    "0x94e7a5dcbe816e498b89ab752661904e2f56c485",
    "--reserve-id",
    "0",
    "--wallet",
    "wallet",
    "--env-file",
    ".env",
    "--max-gas-price-wei",
    "5000000000",
    "--max-transaction-fee",
    "5000000000000000",
]


@pytest.fixture(autouse=True)
def state_home(tmp_path, monkeypatch):
    """Records and fund claims of these tests stay out of the operator's own state."""
    claims = tmp_path / "home/claims"
    monkeypatch.setattr("almanak.state.claims_root", lambda: claims)
    return claims.parent


@pytest.fixture
def desk(tmp_path, recorded, monkeypatch):
    desk = Desk(tmp_path, recorded)
    monkeypatch.setattr(ax_actions, "ALLOCATION", recorded.config.allocations[0].name)

    def coordinator(*, gateway, create):
        runtime = desk.runtime()
        runtime.close = lambda: None
        return runtime

    monkeypatch.setattr(operator_runtime, "OperatorRuntime", coordinator)
    return desk


def parse(*arguments):
    return parse_validated(build_parser(), ["ax", *arguments])


def test_preview_plans_the_first_step_and_signs_nothing(desk):
    amount = desk.intent("supply").amount.amount
    result = ax_actions.perform(desk.service, "supply", amount, execute=False)
    assert result["executed"] is False and "outcomes" not in result
    assert result["first_step"]["operation"]["kind"] == "approve"
    assert result["position_before"] == {"supplied_shares": "0", "debt_shares": "0"}
    assert desk.gateway.broadcasts == [] and desk.store.load().pending is None


def test_execute_runs_the_action_and_reports_the_position(desk):
    amount = desk.intent("supply").amount.amount
    supplied = ax_actions.perform(desk.service, "supply", amount, execute=True)
    assert [item["operation"] for item in supplied["outcomes"]] == ["approve", "supply"]
    assert all(item["success"] and item["transaction"] for item in supplied["outcomes"])
    assert supplied["executed"] and supplied["completed"] and "refused" not in supplied
    shares = desk.gateway.outcome("supply").shares
    assert supplied["position_after"]["supplied_shares"] == str(shares)

    withdrawn = ax_actions.perform(desk.service, "withdraw", None, execute=True)
    assert withdrawn["position_before"]["supplied_shares"] == str(shares)
    assert withdrawn["position_after"]["supplied_shares"] == "0"


def test_an_action_states_its_amount_exactly_once(recorded, monkeypatch):
    monkeypatch.setattr(ax_actions, "ALLOCATION", recorded.config.allocations[0].name)
    assert ax_actions.intent(recorded.config, "withdraw", None).kind == "withdraw"
    assert ax_actions.intent(recorded.config, "supply", 7).amount.amount == 7
    with pytest.raises(ValueError, match="requires --amount"):
        ax_actions.intent(recorded.config, "supply", None)
    with pytest.raises(ValueError, match="takes no --amount"):
        ax_actions.intent(recorded.config, "withdraw", 7)


def test_adoption_takes_ownership_from_the_observed_position(recorded):
    supply = next(item for item in recorded.outcomes if item.operation.kind == "supply")
    empty = recorded.model_copy(
        update={"owned": adopt_ownership(recorded.owned, recorded.baseline)}
    )
    assert [item.shares for item in empty.owned] == [
        item.position.supplied_shares for item in recorded.baseline
    ]
    adopted = adopt_ownership(empty.owned[:1], (supply.after,))
    assert adopted == commit_ownership(empty.owned[:1], supply)
    assert adopted[0].shares == supply.after.position.supplied_shares > 0


def test_only_an_operator_deployments_first_update_adopts():
    def state(operator, initialized):
        return SimpleNamespace(operator=operator, initialized=initialized)

    assert adopts(state(True, False), state(True, True))
    assert not adopts(state(False, False), state(False, True))
    assert not adopts(state(True, True), state(True, True))


def test_the_configuration_binds_one_wallet_one_market_and_explicit_fee_ceilings(recorded):
    reserve = recorded.config.allocations[0].reserve
    mainnet = reserve.model_copy(
        update={"chain": reserve.chain.model_copy(update={"environment": "mainnet"})}
    )
    document = json.loads(mainnet.model_dump_json().replace("eip155:31337", "eip155:1"))
    reserve = type(reserve).model_validate_json(json.dumps(document))
    config = ax_actions.lending_config(
        reserve,
        {"account": "treasury", "address": "0x" + "Ab" * 20},
        ax_actions.alchemy(reserve.chain.chain_id),
        max_gas_price_wei=5,
        max_transaction_fee=9,
    )
    binding = config.accounts.bindings[0]
    # The binding carries the keystore entry's own name, so any wallet's keystore loads.
    assert binding.name == "treasury" and binding.profile.kind == "eoa"
    assert binding.profile.fund.address == "0x" + "ab" * 20
    assert config.target(ax_actions.ALLOCATION).reserve == reserve
    policy = config.production.policy("treasury")
    assert (policy.max_gas_price_wei, policy.max_transaction_fee.amount) == (5, 9)
    assert policy.total_budget is None
    assert config.production.networks[0].provider == AlchemyProvider(
        kind="alchemy", network="eth-mainnet", api_key_env="ALCHEMY_API_KEY"
    )


def test_a_chain_without_a_provider_binding_is_refused():
    from almanak.caip import ChainId

    with pytest.raises(ValueError, match="no provider binding"):
        ax_actions.alchemy(ChainId("eip155:31337"))


def test_local_gateway_serves_the_service_in_process(tmp_path, recorded):
    desk = Desk(tmp_path, recorded)
    gateway = ax_actions.LocalGateway(desk.service)
    assert gateway.configuration() == recorded.config
    assert gateway.load().deployment_id == desk.store.load().deployment_id
    # A refusal reaches the caller as the service raised it, not as an HTTP status.
    with pytest.raises(ValueError):
        gateway.request("state", unexpected=1)
    gateway.close()


def test_wallet_record_comes_from_wallet_create(tmp_path):
    created = create_wallet(tmp_path / "wallet", "treasury", generate_password=True)
    wallet = ax_actions.read_wallet(tmp_path / "wallet")
    assert (wallet["address"], wallet["account"]) == (created["address"], "treasury")
    assert Path(wallet["password_file"]).name == "keystore-password"
    (tmp_path / "other").mkdir()
    (tmp_path / "other/wallet.json").write_text("{}")
    (tmp_path / "other/wallet.json").chmod(0o600)
    with pytest.raises(ValueError, match="no wallet.json"):
        ax_actions.read_wallet(tmp_path / "other")


def test_supply_requires_an_amount_and_fee_ceilings_and_withdraw_takes_none(capsys):
    supply = parse("supply", *COMMON, "--amount", "5")
    assert (supply.product, supply.amount, supply.execute) == ("operator-action", 5, False)
    withdraw = parse("withdraw", *COMMON, "--execute")
    assert withdraw.amount is None and withdraw.execute
    for arguments in (
        ("supply", *COMMON),
        ("supply", *COMMON, "--amount", "0"),
        ("withdraw", *COMMON, "--amount", "5"),
        ("withdraw", *COMMON[:-2]),
        ("supply", *COMMON, "--amount", "5", "--config", "market.json"),
    ):
        with pytest.raises(SystemExit):
            parse(*arguments)
    capsys.readouterr()


def test_an_action_never_reads_through_the_session_read_gateway(monkeypatch):
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_AUTH_FILE", "read-token")
    assert not hasattr(parse("withdraw", *COMMON), "gateway_url")


class Service:
    def __init__(self, config, database, backend, **options):
        self.options = options
        database.touch()


class Stored:
    """Stands in for the journal a record holds; `pending` is what its checkpoint says."""

    pending = None

    def __init__(self, path):
        self.path = path

    def load(self):
        if isinstance(Stored.pending, Exception):
            raise Stored.pending
        return SimpleNamespace(pending=Stored.pending)


@pytest.fixture
def stored(monkeypatch):
    monkeypatch.setattr("almanak.gateway.service.GatewayService", Service)
    monkeypatch.setattr("almanak.state.Journal", Stored)
    monkeypatch.setattr(Stored, "pending", None)
    return Stored


def records(state_home):
    root = state_home / "actions"
    return sorted(path for path in root.iterdir() if path.is_dir()) if root.is_dir() else []


def test_the_private_record_is_removed_when_nothing_is_in_flight(monkeypatch, stored, state_home):
    seen = {}

    def perform(service, action, amount, *, execute, create, interrupt):
        seen.update(service.options, create=create, records=len(records(state_home)))
        return {"action": action}

    monkeypatch.setattr(ax_actions, "perform", perform)
    assert ax_actions.run("config", None, "withdraw", None, execute=False) == {"action": "withdraw"}
    assert seen == {"lease_seconds": 30, "operator_actions": True, "create": "config", "records": 1}
    assert records(state_home) == []

    monkeypatch.setattr(ax_actions, "perform", lambda *a, **k: (_ for _ in ()).throw(ValueError))
    with pytest.raises(ValueError):
        ax_actions.run("config", None, "withdraw", None, execute=True)
    assert records(state_home) == []


def test_a_service_that_cannot_start_leaves_no_record(monkeypatch, state_home):
    def refuse(*arguments, **options):
        raise ValueError("refused")

    monkeypatch.setattr("almanak.gateway.service.GatewayService", refuse)
    with pytest.raises(ValueError, match="refused"):
        ax_actions.run("config", None, "withdraw", None, execute=False)
    assert records(state_home) == []


@pytest.mark.parametrize("pending", [object(), RuntimeError("unreadable")])
def test_a_signed_step_in_flight_keeps_its_record(monkeypatch, stored, state_home, capsys, pending):
    # A record that cannot be read is kept too: it may hold a signed step.
    monkeypatch.setattr(stored, "pending", pending)
    monkeypatch.setattr(ax_actions, "perform", lambda *a, **k: (_ for _ in ()).throw(OSError))
    with pytest.raises(OSError):
        ax_actions.run("config", None, "supply", 5, execute=True)
    (record,) = records(state_home)
    assert json.loads(capsys.readouterr().out) == {"event": "record_kept", "path": str(record)}


def test_a_result_returned_with_a_signed_step_pending_keeps_its_record(
    monkeypatch, stored, state_home, capsys
):
    monkeypatch.setattr(stored, "pending", object())
    monkeypatch.setattr(ax_actions, "perform", lambda *a, **k: {"completed": False})
    assert ax_actions.run("config", None, "supply", 5, execute=True) == {"completed": False}
    (record,) = records(state_home)
    assert json.loads(capsys.readouterr().out)["path"] == str(record)


def claim(state_home, database):
    claims = state_home / "claims"
    claims.mkdir(parents=True, exist_ok=True)
    path = claims / (database.parent.name + ".claim")
    path.write_text(json.dumps({"deployment_id": "d", "database": str(database.resolve())}))
    return path


def abandoned(state_home, name):
    """The record of a command that was killed: unlocked, with its fund still claimed."""
    record = state_home / "actions" / name
    record.mkdir(parents=True)
    (record / "lock").touch()
    (record / "state.sqlite").touch()
    return record, claim(state_home, record / "state.sqlite")


def test_a_killed_commands_record_and_claim_are_cleared_unless_a_step_was_signed(
    monkeypatch, stored, state_home
):
    import fcntl

    record, claimed = abandoned(state_home, "killed")
    running, running_claim = abandoned(state_home, "running")
    (state_home / "actions/unrelated").mkdir()
    with (running / "lock").open() as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        ax_actions.clear_finished_records()
        # Another command's live record is left alone, with its claim.
        assert running.exists() and running_claim.exists()
    assert not record.exists() and not claimed.exists()
    assert (state_home / "actions/unrelated").exists()

    signed, signed_claim = abandoned(state_home, "signed")
    monkeypatch.setattr(stored, "pending", object())
    ax_actions.clear_finished_records()
    assert signed.exists() and signed_claim.exists()


def test_a_record_without_a_journal_holds_no_signed_step(tmp_path):
    assert ax_actions.in_flight(tmp_path) is False


def outcomes_of(recorded, *kinds):
    return [next(o for o in recorded.outcomes if o.operation.kind == kind) for kind in kinds]


def finished(recorded, outcomes, refused=None):
    return ax_actions.executed_result(recorded, outcomes, refused)


def test_only_the_finished_request_completes_the_action(recorded):
    from almanak.gateway.ethereum import DataError

    assert finished(recorded, outcomes_of(recorded, "approve", "supply"))["completed"] is True
    (approve,) = outcomes_of(recorded, "approve")
    # An approval can succeed before the supply is refused: sent, but not completed.
    partial = finished(recorded, [approve], DataError("gas_fee_quote", "above the ceiling"))
    assert partial["executed"] and not partial["completed"]
    assert partial["refused"] == {"kind": "gas_fee_quote", "message": "above the ceiling"}
    # A step refused at signing leaves no outcome at all.
    unsent = finished(recorded, [], DataError("deadline_margin", "too late"))
    assert (unsent["executed"], unsent["completed"]) == (False, False)
    assert unsent["refused"]["kind"] == "deadline_margin"


def test_a_withdrawal_is_complete_only_once_no_allowance_remains(recorded):
    (withdraw,) = outcomes_of(recorded, "withdraw")
    assert finished(recorded, [withdraw])["completed"] is True
    residual = withdraw.model_copy(
        update={"after": withdraw.after.model_copy(update={"allowance": 5})}
    )
    # The withdrawal succeeded, but its revocation has not run yet.
    assert finished(recorded, [residual])["completed"] is False
    revoke = SimpleNamespace(
        operation=SimpleNamespace(kind="revoke"),
        success=True,
        tx_hash="0x1",
        strategy_result=lambda: SimpleNamespace(complete=True),
    )
    # The final step of a withdrawal can be the revocation, and it completes it.
    assert finished(recorded, [residual, revoke])["completed"] is True


def test_a_fee_refusal_on_a_later_step_still_reports_the_steps_sent(desk):
    from almanak.gateway.ethereum import DataError

    prepare = desk.gateway.prepare

    def priced_out(state, intent, *arguments, **options):
        if desk.kinds() == ["approve"]:
            raise DataError("gas_fee_quote", "fee quote is above the ceiling")
        return prepare(state, intent, *arguments, **options)

    desk.gateway.prepare = priced_out
    amount = desk.intent("supply").amount.amount
    result = ax_actions.perform(desk.service, "supply", amount, execute=True)
    assert [item["operation"] for item in result["outcomes"]] == ["approve"]
    assert result["outcomes"][0]["transaction"]
    assert result["executed"] and not result["completed"]
    assert result["refused"] == {
        "kind": "gas_fee_quote",
        "message": "fee quote is above the ceiling",
    }


def test_an_unexpected_failure_reports_the_steps_already_sent(desk, capsys):
    prepare = desk.gateway.prepare

    def broken(state, intent, *arguments, **options):
        if desk.kinds() == ["approve"]:
            raise OSError("disk")
        return prepare(state, intent, *arguments, **options)

    desk.gateway.prepare = broken
    amount = desk.intent("supply").amount.amount
    with pytest.raises(OSError):
        ax_actions.perform(desk.service, "supply", amount, execute=True)
    event = json.loads(capsys.readouterr().out)
    assert event["event"] == "steps_sent"
    assert [item["operation"] for item in event["outcomes"]] == ["approve"]


def test_a_refusal_is_raised_when_the_record_cannot_be_read_afterwards():
    from almanak.gateway.ethereum import DataError

    refusal = DataError("gas_price", "above the ceiling")

    def unreadable():
        raise OSError("disk")

    def refuse(interrupt):
        raise refusal

    runtime = SimpleNamespace(execute=refuse, journal=SimpleNamespace(load=unreadable))
    with pytest.raises(DataError) as raised:
        ax_actions.executed(runtime, 0, lambda: None)
    assert raised.value is refusal
    runtime.execute = lambda interrupt: None
    with pytest.raises(OSError):
        ax_actions.executed(runtime, 0, lambda: None)
    # Nothing can be reported from an unreadable record either.
    ax_actions.report_sent(runtime, 0)


def test_local_gateway_reports_storage_failures_as_the_loopback_server_does():
    import sqlite3

    from almanak.gateway.ethereum import DataError

    def locked(method, request):
        raise sqlite3.OperationalError("database is locked")

    request_type = SimpleNamespace(model_validate_json=lambda body: body)
    gateway = ax_actions.LocalGateway(SimpleNamespace(request_type=request_type, dispatch=locked))
    # The inherited lease renewal retries exactly this kind and no other.
    with pytest.raises(DataError) as raised:
        gateway.request("renew")
    assert raised.value.kind == "gateway_unavailable"
    # A release that fails does not replace the result of the action that ran.
    gateway.coordinator, gateway.deployment_id = "c", "d"
    gateway.close()
    assert gateway.coordinator is None


@pytest.mark.parametrize(
    ("execute", "completed", "status"),
    [(False, None, 0), (True, True, 0), (True, False, 1)],
)
def test_exit_status_follows_completion_only_when_executing(
    monkeypatch, capsys, execute, completed, status
):
    monkeypatch.setattr("almanak.ax.lending_request", lambda args: SimpleNamespace(chain="c"))
    monkeypatch.setattr(ax_actions, "read_wallet", lambda directory: {})
    monkeypatch.setattr(ax_actions, "alchemy", lambda chain: None)
    monkeypatch.setattr("almanak.gateway.providers.provider_endpoint", lambda *a: "endpoint")
    monkeypatch.setattr(ax_actions, "action_config", lambda *a: "config")
    backend = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(ax_actions, "production_backend", lambda *a: backend)
    result = {"executed": execute} if completed is None else {"completed": completed}
    monkeypatch.setattr(ax_actions, "run", lambda *a, **k: result)
    args = SimpleNamespace(
        wallet=None, env_file=None, ax_command="supply", amount=1, execute=execute
    )
    assert ax_actions.command(args) == status
    assert json.loads(capsys.readouterr().out) == result


def test_a_plan_refused_in_preview_names_its_refusal(desk):
    from almanak.gateway.ethereum import DataError

    runtime = desk.runtime()
    runtime.submit(desk.intent("supply"))

    def refuse(state):
        runtime.count_replan(
            DataError("simulation_reverted", "transfer exceeds balance"), "prepare"
        )

    runtime.prepare_decision = refuse
    runtime.replanning = lambda: SimpleNamespace(attempts={}, stages={}, delay=0)
    runtime.route = lambda: None
    with pytest.raises(DataError, match="refused during planning: transfer exceeds") as raised:
        runtime.preview()
    assert raised.value.kind == "simulation_reverted"
    runtime.refused = None
    runtime.prepare_decision = lambda state: None
    with pytest.raises(DataError) as unnamed:
        runtime.preview()
    assert unnamed.value.kind == "plan_refused"


def raised_by(number):
    import os
    import signal

    previous = {n: signal.getsignal(n) for n in (signal.SIGINT, signal.SIGTERM)}
    steps = []
    with ax_actions.interruptible() as check:
        check()
        os.kill(os.getpid(), number)
        # The signal is only recorded: the step it arrived in runs to its end.
        steps.append("finished")
        with pytest.raises(BaseException) as raised:
            check()
    assert steps == ["finished"]
    assert {n: signal.getsignal(n) for n in previous} == previous
    return raised.value


def test_signals_are_recorded_and_raised_only_at_a_check():
    import signal

    stopped = raised_by(signal.SIGTERM)
    assert isinstance(stopped, ax_actions.Terminated)
    assert stopped.code == 128 + signal.SIGTERM
    assert isinstance(raised_by(signal.SIGINT), KeyboardInterrupt)
    # A closed terminal stops the action the same way, with its record reported.
    assert raised_by(signal.SIGHUP).code == 128 + signal.SIGHUP


def test_a_signal_after_the_last_check_does_not_discard_the_result(monkeypatch, stored, state_home):
    import os
    import signal

    def perform(service, action, amount, **options):
        os.kill(os.getpid(), signal.SIGTERM)
        return {"action": action, "completed": True}

    monkeypatch.setattr(ax_actions, "perform", perform)
    result = ax_actions.run("config", None, "withdraw", None, execute=True)
    assert result == {"action": "withdraw", "completed": True}
    assert records(state_home) == []


def test_a_signal_between_steps_stops_the_action_and_reports_its_kept_record(
    monkeypatch, stored, state_home, capsys
):
    import os
    import signal

    def perform(service, action, amount, *, interrupt, **options):
        os.kill(os.getpid(), signal.SIGTERM)
        interrupt()
        raise AssertionError("the check did not stop the action")

    def inspected(self):
        # A second signal while the record is inspected cannot skip the report either.
        os.kill(os.getpid(), signal.SIGTERM)
        return SimpleNamespace(pending=object())

    monkeypatch.setattr(stored, "load", inspected)
    monkeypatch.setattr(ax_actions, "perform", perform)
    with pytest.raises(ax_actions.Terminated):
        ax_actions.run("config", None, "supply", 5, execute=True)
    (record,) = records(state_home)
    assert json.loads(capsys.readouterr().out) == {"event": "record_kept", "path": str(record)}


def test_a_stop_requested_while_waiting_is_honoured_before_the_next_step(desk):
    runtime = desk.runtime()
    runtime.submit(desk.intent("supply"))
    requested = []
    sent_at_request = []

    def wait(state):
        # The stop arrives during the wait that follows the first broadcast.
        if desk.gateway.broadcasts and not requested:
            requested.append(True)
            sent_at_request.append(list(desk.gateway.broadcasts))

    def interrupt():
        if requested:
            raise ax_actions.Terminated(143)

    runtime.wait = wait
    with pytest.raises(ax_actions.Terminated):
        runtime.execute(interrupt)
    # Nothing was sent after the stop was requested, and the request stays recorded.
    assert sent_at_request and desk.gateway.broadcasts == sent_at_request[0]
    assert desk.store.load().decision is not None


def test_execution_checks_for_an_interrupt_before_its_first_step(desk):
    runtime = desk.runtime()
    runtime.submit(desk.intent("supply"))

    def interrupt():
        raise ax_actions.Terminated(143)

    with pytest.raises(ax_actions.Terminated):
        runtime.execute(interrupt)
    assert desk.gateway.broadcasts == [] and desk.store.load().plan is None


def test_sigterm_handling_is_left_alone_off_the_main_thread():
    import signal
    import threading

    seen = []

    def body():
        with ax_actions.interruptible() as check:
            check()
            seen.append(signal.getsignal(signal.SIGTERM))

    worker = threading.Thread(target=body)
    worker.start()
    worker.join()
    assert seen == [signal.getsignal(signal.SIGTERM)]


def adoption(desk, change=lambda owned: owned):
    state = desk.store.load()
    observed = desk.gateway.observe()
    owned = change(adopt_ownership(state.owned, observed))
    update = {"initialized": True, "baseline": observed, "owned": owned}
    return desk.journal.update(lambda current: current.model_copy(update=update))


def inflated(owned):
    return (owned[0].model_copy(update={"shares": owned[0].shares + 1}), *owned[1:])


def test_the_gateway_refuses_adopted_ownership_its_baseline_does_not_state(desk):
    with pytest.raises(ValueError, match="ownership"):
        adoption(desk, inflated)
    assert not desk.store.load().initialized
    assert adoption(desk).initialized
    # Adoption is the first update only; afterwards ownership follows execution evidence.
    state = desk.store.load()
    with pytest.raises(ValueError, match="ownership"):
        desk.journal.update(
            lambda current: current.model_copy(update={"owned": inflated(state.owned)})
        )


def test_an_interrupted_action_is_not_described_as_resumable():
    from almanak.cli import interrupted_message

    assert "read the position" in interrupted_message(SimpleNamespace(product="operator-action"))
    assert "resume" in interrupted_message(SimpleNamespace(product="run"))


def test_a_record_is_kept_while_its_claim_cannot_be_released(stored, state_home):
    import fcntl

    record, claimed = abandoned(state_home, "killed")
    with claimed.open() as contended:
        # Another command for the same wallet is reading the claim's owner.
        fcntl.flock(contended, fcntl.LOCK_EX)
        ax_actions.clear_finished_records()
        assert record.exists() and claimed.exists()
    # The record is what lets the next command release the claim.
    ax_actions.clear_finished_records()
    assert not record.exists() and not claimed.exists()


def test_a_new_record_is_never_visible_to_a_sweep_before_it_is_locked(state_home, monkeypatch):
    swept = []
    flock = ax_actions.fcntl.flock

    def sweeping(handle, operation):
        flock(handle, operation)
        if Path(handle.name).name == "lock" and not swept:
            swept.append(True)
            # A sweep in this process would block on the root lock held by creation.
            root_lock = (state_home / "actions/.lock").open()
            with pytest.raises(OSError):
                flock(root_lock, ax_actions.fcntl.LOCK_EX | ax_actions.fcntl.LOCK_NB)
            root_lock.close()

    monkeypatch.setattr(ax_actions.fcntl, "flock", sweeping)
    directory, lock = ax_actions.new_record()
    assert swept and directory.is_dir()
    lock.close()


def test_a_signed_step_without_an_outcome_is_reported_as_unresolved(recorded, capsys):
    (approve,) = outcomes_of(recorded, "approve")
    pending = SimpleNamespace(operation=SimpleNamespace(kind="supply"), tx_hash="0xabc")
    state = SimpleNamespace(owned=recorded.owned, outcomes=(approve,), pending=pending)
    expected = {"operation": "supply", "transaction": "0xabc"}
    result = ax_actions.executed_result(state, [approve], None)
    # It is not an outcome: nothing says yet whether it was mined.
    assert result["unresolved"] == expected and len(result["outcomes"]) == 1
    assert "unresolved" not in finished(recorded, [approve])

    runtime = SimpleNamespace(journal=SimpleNamespace(load=lambda: state))
    ax_actions.report_sent(runtime, 1)
    assert json.loads(capsys.readouterr().out) == {
        "event": "steps_sent",
        "outcomes": [],
        "unresolved": expected,
    }


def test_every_directory_a_new_record_creates_is_synced_through_its_parent(state_home, monkeypatch):
    synced = []
    monkeypatch.setattr(ax_actions, "sync_directory", synced.append)
    directory, lock = ax_actions.new_record()
    lock.close()
    root = state_home / "actions"
    # The records directory and its new ancestor, then the record's own entry.
    assert synced == [state_home, state_home.parent, root]
    synced.clear()
    ax_actions.new_record()[1].close()
    assert synced == [root]
